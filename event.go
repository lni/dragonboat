// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package dragonboat

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/raftio"
)

// WriteHealthMetrics writes all health metrics in Prometheus format to the
// specified writer. This function is typically called by the metrics http
// handler.
func WriteHealthMetrics(w io.Writer) {
	metrics.WritePrometheus(w, false)
}

type raftEventListener struct {
	clusterID           uint64
	nodeID              uint64
	leaderID            *uint64
	metrics             bool
	workCh              chan struct{}
	mu                  sync.Mutex
	notifications       []raftio.LeaderInfo
	stopper             *syncutil.Stopper
	isLeader            *metrics.Gauge
	campaignLaunched    *metrics.Counter
	campaignSkipped     *metrics.Counter
	snapshotRejected    *metrics.Counter
	replicationRejected *metrics.Counter
	proposalDropped     *metrics.Counter
	readIndexDropped    *metrics.Counter
	userListener        raftio.IRaftEventListener
}

func newRaftEventListener(clusterID uint64, nodeID uint64,
	leaderID *uint64, useMetrics bool,
	userListener raftio.IRaftEventListener) *raftEventListener {
	el := &raftEventListener{
		clusterID:     clusterID,
		nodeID:        nodeID,
		leaderID:      leaderID,
		metrics:       useMetrics,
		userListener:  userListener,
		stopper:       syncutil.NewStopper(),
		workCh:        make(chan struct{}, 1),
		notifications: make([]raftio.LeaderInfo, 0),
	}
	if userListener != nil {
		el.stopper.RunWorker(func() {
			for {
				select {
				case <-el.stopper.ShouldStop():
					return
				case <-el.workCh:
					for {
						v, ok := el.getLeaderInfo()
						if !ok {
							break
						}
						el.userListener.LeaderUpdated(v)
					}
				}
			}
		})
	}
	if useMetrics {
		label := fmt.Sprintf(`{clusterid="%d",nodeid="%d"}`, clusterID, nodeID)
		name := fmt.Sprintf(`raft_node_campaign_launched%s`, label)
		campaignLaunched := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_campaign_skipped%s`, label)
		campaignSkipped := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_snapshot_rejected%s`, label)
		snapshotRejected := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_replication_rejected%s`, label)
		replicationRejected := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_proposal_dropped%s`, label)
		proposalDropped := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_read_index_dropped%s`, label)
		readIndexDropped := metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`raft_node_is_leader%s`, label)
		isLeader := metrics.GetOrCreateGauge(name, func() float64 {
			if atomic.LoadUint64(leaderID) == nodeID {
				return 1.0
			}
			return 0.0
		})

		el.isLeader = isLeader
		el.campaignLaunched = campaignLaunched
		el.campaignSkipped = campaignSkipped
		el.snapshotRejected = snapshotRejected
		el.replicationRejected = replicationRejected
		el.proposalDropped = proposalDropped
		el.readIndexDropped = readIndexDropped
	}
	return el
}

func (e *raftEventListener) stop() {
	e.stopper.Stop()
}

func (e *raftEventListener) LeaderUpdated(info server.LeaderInfo) {
	atomic.StoreUint64(e.leaderID, info.LeaderID)
	if e.userListener != nil {
		ui := raftio.LeaderInfo{
			ClusterID: info.ClusterID,
			NodeID:    info.NodeID,
			Term:      info.Term,
			LeaderID:  info.LeaderID,
		}
		e.addLeaderInfo(ui)
		select {
		case e.workCh <- struct{}{}:
		default:
		}
	}
}

func (e *raftEventListener) CampaignLaunched(info server.CampaignInfo) {
	if e.metrics {
		e.campaignLaunched.Add(1)
	}
}

func (e *raftEventListener) CampaignSkipped(info server.CampaignInfo) {
	if e.metrics {
		e.campaignSkipped.Add(1)
	}
}

func (e *raftEventListener) SnapshotRejected(info server.SnapshotInfo) {
	if e.metrics {
		e.snapshotRejected.Add(1)
	}
}

func (e *raftEventListener) ReplicationRejected(info server.ReplicationInfo) {
	if e.metrics {
		e.replicationRejected.Add(1)
	}
}

func (e *raftEventListener) ProposalDropped(info server.ProposalInfo) {
	if e.metrics {
		e.proposalDropped.Add(len(info.Entries))
	}
}

func (e *raftEventListener) ReadIndexDropped(info server.ReadIndexInfo) {
	if e.metrics {
		e.readIndexDropped.Add(1)
	}
}

func (e *raftEventListener) addLeaderInfo(li raftio.LeaderInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notifications = append(e.notifications, li)
}

func (e *raftEventListener) getLeaderInfo() (raftio.LeaderInfo, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.notifications) > 0 {
		v := e.notifications[0]
		e.notifications = e.notifications[1:]
		return v, true
	}
	return raftio.LeaderInfo{}, false
}
