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

package drummer

import (
	"context"
	"time"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/internal/drummer/client"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
)

var (
	persistentLogReportCycle uint64 = settings.Soft.PersisentLogReportCycle
)

// NodeHostClient is a NodeHost drummer client.
type NodeHostClient struct {
	nh              *dragonboat.NodeHost
	masterClient    *client.DrummerClient
	masterServers   []string
	reporterStopper *syncutil.Stopper
	stopper         *syncutil.Stopper
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewNodeHostClient creates and returns a new NodeHostClient instance.
func NewNodeHostClient(nh *dragonboat.NodeHost,
	drummerServers []string) *NodeHostClient {
	if len(drummerServers) == 0 {
		plog.Panicf("drummer server address not specified")
	}
	servers := make([]string, 0)
	servers = append(servers, drummerServers...)
	masterClient := client.NewDrummerClient(nh)
	ctx, cancel := context.WithCancel(context.Background())
	stopper := syncutil.NewStopper()
	reporterStopper := syncutil.NewStopper()
	dnh := &NodeHostClient{
		nh:              nh,
		masterClient:    masterClient,
		masterServers:   servers,
		stopper:         stopper,
		reporterStopper: reporterStopper,
		ctx:             ctx,
		cancel:          cancel,
	}
	reporterStopper.RunWorker(func() {
		dnh.reportWorker(ctx)
	})
	stopper.RunWorker(func() {
		dnh.masterRequestWorker(ctx)
	})
	return dnh
}

// Stop stops the NodeHostClient instance.
func (dnh *NodeHostClient) Stop() {
	dnh.cancel()
	plog.Debugf("%s is going to stop the dnh stopper", dnh.nh.RaftAddress())
	if dnh.reporterStopper != nil {
		dnh.reporterStopper.Stop()
	}
	dnh.stopper.Stop()
	if dnh.masterClient != nil {
		dnh.masterClient.Stop()
	}
}

// StopNodeHostInfoReporter stop the info reporter part of the client.
func (dnh *NodeHostClient) StopNodeHostInfoReporter() {
	dnh.reporterStopper.Stop()
	dnh.reporterStopper = nil
}

func (dnh *NodeHostClient) masterRequestWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := dnh.masterClient.HandleMasterRequests(ctx); err == context.Canceled {
				return
			}
		case <-ctx.Done():
			return
		case <-dnh.stopper.ShouldStop():
			return
		}
	}
}

func (dnh *NodeHostClient) reportWorker(ctx context.Context) {
	interval := time.Duration(NodeHostInfoReportSecond) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	count := uint64(0)
	for {
		select {
		case <-ticker.C:
			count++
			includePlog := false
			if count == 1 || count%persistentLogReportCycle == 0 {
				includePlog = true
			}
			if err := dnh.reportNodeHostInfo(ctx, includePlog); err == context.Canceled {
				return
			}
		case <-ctx.Done():
			return
		case <-dnh.reporterStopper.ShouldStop():
			return
		}
	}
}

func (dnh *NodeHostClient) reportNodeHostInfo(ctx context.Context,
	plogIncluded bool) error {
	nhi := dnh.nh.GetNodeHostInfo()
	if !plogIncluded {
		nhi.LogInfo = []raftio.NodeInfo{}
	}
	nhi.LogInfoIncluded = plogIncluded
	servers := make([]string, 0)
	for _, v := range dnh.masterServers {
		servers = append(servers, v)
	}
	timeoutSecond := time.Duration(NodeHostInfoReportSecond * 2)
	rctx, cancel := context.WithTimeout(dnh.ctx, timeoutSecond*time.Second)
	defer cancel()
	random.ShuffleStringList(servers)
	for _, url := range servers {
		err := dnh.masterClient.SendNodeHostInfo(rctx, url, *nhi)
		if err != nil {
			if err == context.Canceled {
				return err
			}
			plog.Warningf("%s failed to send node host info to %s, %v",
				dnh.nh.RaftAddress(), url, err)
		} else {
			break
		}
	}
	return nil
}
