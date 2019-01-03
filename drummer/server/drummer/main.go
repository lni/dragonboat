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
Drummer is dragonboat's drummer server program.
*/
package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/drummer"
	pb "github.com/lni/dragonboat/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/plugin/rpc"
)

var (
	plog = logger.GetLogger("server/drummer")
)

func validateDrummerConfig(cc pb.Config) bool {
	good := true
	if len(cc.RaftClusterAddresses) == 0 {
		plog.Errorf("empty RaftClusterAddresses field")
		good = false
	}
	if len(cc.DrummerAddress) == 0 {
		plog.Errorf("empty DrummerAddress field")
		good = false
	}
	if cc.DrummerNodeID == 0 {
		plog.Errorf("DrummerNodeID field is not allowed to be 0")
		good = false
	}
	if cc.ElectionRTT == 0 {
		plog.Errorf("ElectionRTT field is 0")
		good = false
	}
	if cc.HeartbeatRTT == 0 {
		plog.Errorf("HeartbeatRTT field is 0")
		good = false
	}
	if cc.HeartbeatRTT >= cc.ElectionRTT {
		plog.Errorf("HeartbeatRTT must be significantly smaller than ElectionRTT")
		good = false
	}
	if len(cc.DrummerNodeHostDirectory) == 0 {
		plog.Errorf("empty DrummerNodeHostDirectory field")
		good = false
	}
	if len(cc.DrummerWALDirectory) == 0 {
		plog.Errorf("empty DrummerWALDirectory field")
		good = false
	}

	return good
}

func main() {
	fp := flag.String("config", "", "full path of the Drummer JSON config file")
	flag.Parse()

	var cc pb.Config
	if len(*fp) > 0 {
		cc = drummer.GetClusterConfigFromFile(*fp)
	} else {
		cc = drummer.GetClusterConfig()
	}
	if !validateDrummerConfig(cc) {
		plog.Panicf("invalid drummer config file")
	}
	// log the details
	plog.Infof("Drummer cluster: %s", cc.RaftClusterAddresses)
	plog.Infof("Drummer node ID: %d", cc.DrummerNodeID)
	plog.Infof("Drummer address: %s", cc.DrummerAddress)
	plog.Infof("WAL dir: %s", cc.DrummerWALDirectory)
	plog.Infof("Nodehost dir: %s", cc.DrummerNodeHostDirectory)
	plog.Infof("Mutual TLS: %t", cc.MutualTLS)
	plog.Infof("CA file path: %s", cc.CAFile)
	plog.Infof("Cert file path: %s", cc.CertFile)
	plog.Infof("Key file path: %s", cc.KeyFile)
	// configurations for raft
	rc := config.Config{
		ElectionRTT:        20,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		ClusterID:          0,
		NodeID:             cc.DrummerNodeID,
		SnapshotEntries:    cc.SnapshotEntries,
		CompactionOverhead: cc.CompactionOverhead,
	}
	if !config.IsValidAddress(cc.DrummerAddress) {
		plog.Panicf("invalid drummer addressi %s", cc.DrummerAddress)
	}
	peers := make(map[uint64]string)
	for idx, v := range strings.Split(cc.RaftClusterAddresses, ",") {
		if !config.IsValidAddress(v) {
			plog.Panicf("invalid address %s in the RaftClusterAddresses field", v)
		}
		peers[uint64(idx+1)] = v
	}
	nhc := config.NodeHostConfig{
		WALDir:         cc.DrummerWALDirectory,
		NodeHostDir:    cc.DrummerNodeHostDirectory,
		RTTMillisecond: 50,
		RaftAddress:    peers[cc.DrummerNodeID],
		MutualTLS:      cc.MutualTLS,
		CAFile:         cc.CAFile,
		CertFile:       cc.CertFile,
		KeyFile:        cc.KeyFile,
		RaftRPCFactory: rpc.NewRaftGRPC,
	}
	// drummer doesn't have NodeHostAPI instance
	nh := dragonboat.NewNodeHost(nhc)
	// create the drummer cluster
	if err := nh.StartCluster(peers, false, drummer.NewDB, rc); err != nil {
		panic(err)
	}

	// start the grpc server for drummer api
	grpcServerStopper := syncutil.NewStopper()
	grpcHost := cc.DrummerAddress
	drummerServer := drummer.NewDrummer(nh, grpcHost)
	drummerServer.Start()
	// handle CTRL+C signal
	drummerServerStopper := syncutil.NewStopper()
	drummerServerStopper.RunWorker(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, syscall.SIGUSR2)
		for {
			select {
			case sig := <-c:
				if sig != syscall.SIGINT {
					continue
				}

				plog.Infof("Ctrl+C pressed")
				drummerServer.Stop()
				nh.Stop()
				grpcServerStopper.Stop()
				return
			case <-drummerServerStopper.ShouldStop():
				return
			}
		}
	})

	// don't exit the main until SIGINT is received
	drummerServerStopper.Wait()
}
