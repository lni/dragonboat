// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Nodehost is dragonboat's nodehost server program.
*/
package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/drummer"
	"github.com/lni/dragonboat/drummer/client"
	"github.com/lni/dragonboat/internal/utils/envutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/plugin/rpc"
)

const (
	nodehostJSONFilename = "dragonboat-nodehost.json"
)

var (
	plog = logger.GetLogger("nodehost")
)

type nodehostConfig struct {
	MasterAddresses   string
	RaftAddress       string
	APIAddress        string
	WALDirectory      string
	NodeHostDirectory string
	RTTMillisecond    uint64
	MutualTLS         bool
	CAFile            string
	CertFile          string
	KeyFile           string
}

func validateNodeHostConfig(cc nodehostConfig) bool {
	good := true
	if len(cc.MasterAddresses) == 0 {
		plog.Errorf("empty DrummerAddresses field")
		good = false
	}
	if len(cc.RaftAddress) == 0 {
		plog.Errorf("empty RaftAddress field")
		good = false
	}
	if len(cc.APIAddress) == 0 {
		plog.Errorf("empty APIAddress field")
		good = false
	}
	if cc.APIAddress == cc.RaftAddress {
		plog.Errorf("APIAddress and RaftAddress can not be the same")
		good = false
	}
	if len(cc.WALDirectory) == 0 {
		plog.Errorf("empty WALDirectory field")
		good = false
	}
	if len(cc.NodeHostDirectory) == 0 {
		plog.Errorf("empty NodeHostDirectory field")
		good = false
	}
	if cc.RTTMillisecond == 0 {
		plog.Errorf("RTTMillisecond is 0")
		good = false
	}
	if cc.RTTMillisecond < 20 || cc.RTTMillisecond > 200 {
		plog.Warningf("cc.RTTMillisecond < 20 || cc.RTTMillisecond > 200")
	}
	if cc.MutualTLS {
		if len(cc.CAFile) == 0 || len(cc.CertFile) == 0 || len(cc.KeyFile) == 0 {
			plog.Errorf("Missing CAFile, CertFile or KeyFile settings")
			good = false
		}
	}

	return good
}

func getNodeHostConfig() nodehostConfig {
	fpList := make([]string, 0)
	for _, dir := range envutil.GetConfigDirs() {
		fp := filepath.Join(dir, nodehostJSONFilename)
		fpList = append(fpList, fp)
	}

	return getNodeHostConfigFromFiles(fpList)
}

func getNodeHostConfigFromFile(fp string) nodehostConfig {
	return getNodeHostConfigFromFiles([]string{fp})
}

func getNodeHostConfigFromFiles(fpList []string) nodehostConfig {
	for _, fp := range fpList {
		f, err := os.Open(filepath.Clean(fp))
		if err != nil {
			continue
		}
		data, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		if err = f.Close(); err != nil {
			panic(err)
		}
		if err != nil {
			panic(err)
		}
		var cc nodehostConfig
		err = json.Unmarshal(data, &cc)
		if err != nil {
			plog.Panicf("failed to parse the configuration JSON file, %v", err)
		}
		plog.Infof("using nodehost config file found at %s", fp)
		return cc
	}
	plog.Panicf("couldn't find any nodehost config file")
	// should never reach here
	return nodehostConfig{}
}

func main() {
	fp := flag.String("config", "", "full path of the JSON nodehost config file")
	flag.Parse()
	var cc nodehostConfig
	if len(*fp) > 0 {
		cc = getNodeHostConfigFromFile(*fp)
	} else {
		cc = getNodeHostConfig()
	}
	plog.Infof("Drummer servers: %s", cc.MasterAddresses)
	plog.Infof("Raft address: %s", cc.RaftAddress)
	plog.Infof("Public API address: %s", cc.APIAddress)
	plog.Infof("Low Latency directory: %s", cc.WALDirectory)
	plog.Infof("NodeHost directory: %s", cc.NodeHostDirectory)
	plog.Infof("RTT millisecond: %d", cc.RTTMillisecond)
	plog.Infof("Mutual TLS: %t", cc.MutualTLS)
	plog.Infof("CAFile path: %s", cc.CAFile)
	plog.Infof("CertFile path: %s", cc.CertFile)
	plog.Infof("KeyFile path: %s", cc.KeyFile)
	if !validateNodeHostConfig(cc) {
		plog.Panicf("invalid nodehost config file")
	}
	if !config.IsValidAddress(cc.APIAddress) {
		plog.Panicf("invalid address %s in the APIAddress field", cc.APIAddress)
	}
	if !config.IsValidAddress(cc.RaftAddress) {
		plog.Panicf("invalid address %s in the RaftAddress field", cc.RaftAddress)
	}
	masterServers := strings.Split(cc.MasterAddresses, ",")
	for _, v := range masterServers {
		if !config.IsValidAddress(v) {
			plog.Panicf("invalid address %s in the DrummerAddresses field", v)
		}
	}
	// prepare for config and new the nodehost instance
	nhc := config.NodeHostConfig{
		WALDir:         cc.WALDirectory,
		NodeHostDir:    cc.NodeHostDirectory,
		RTTMillisecond: cc.RTTMillisecond,
		RaftAddress:    cc.RaftAddress,
		APIAddress:     cc.APIAddress,
		MasterServers:  masterServers,
		MutualTLS:      cc.MutualTLS,
		CAFile:         cc.CAFile,
		CertFile:       cc.CertFile,
		KeyFile:        cc.KeyFile,
		RaftRPCFactory: rpc.NewRaftGRPC,
	}
	nh := dragonboat.NewNodeHostWithMasterClientFactory(nhc,
		client.NewDrummerClient)
	nhAPIServer := drummer.NewNodehostAPI(nhc.APIAddress, nh)
	// handle SIGINT
	serverStopper := syncutil.NewStopper()
	serverStopper.RunWorker(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		for {
			select {
			case sig := <-c:
				if sig != syscall.SIGINT {
					continue
				}
				plog.Infof("SIGINT/CTRL+C received")
				nhAPIServer.Stop()
				nh.Stop()
				return
			case <-serverStopper.ShouldStop():
				return
			}
		}
	})
	// don't exit the main until SIGINT is received
	serverStopper.Wait()
}
