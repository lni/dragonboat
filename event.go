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
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/lni/dragonboat/v3/internal/server"
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
	termValue           uint64
	leaderID            *uint64
	metrics             bool
	queue               *leaderInfoQueue
	hasLeader           *metrics.Gauge
	term                *metrics.Gauge
	campaignLaunched    *metrics.Counter
	campaignSkipped     *metrics.Counter
	snapshotRejected    *metrics.Counter
	replicationRejected *metrics.Counter
	proposalDropped     *metrics.Counter
	readIndexDropped    *metrics.Counter
}

func newRaftEventListener(clusterID uint64, nodeID uint64,
	leaderID *uint64, useMetrics bool,
	queue *leaderInfoQueue) *raftEventListener {
	el := &raftEventListener{
		clusterID: clusterID,
		nodeID:    nodeID,
		leaderID:  leaderID,
		metrics:   useMetrics,
		queue:     queue,
	}
	if useMetrics {
		label := fmt.Sprintf(`{clusterid="%d",nodeid="%d"}`, clusterID, nodeID)
		name := fmt.Sprintf(`dragonboat_raftnode_campaign_launched_total%s`, label)
		el.campaignLaunched = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_campaign_skipped_total%s`, label)
		el.campaignSkipped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_snapshot_rejected_total%s`, label)
		el.snapshotRejected = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_replication_rejected_total%s`, label)
		el.replicationRejected = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_proposal_dropped_total%s`, label)
		el.proposalDropped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_read_index_dropped_total%s`, label)
		el.readIndexDropped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_has_leader%s`, label)
		el.hasLeader = metrics.GetOrCreateGauge(name, func() float64 {
			if atomic.LoadUint64(leaderID) == raftio.NoLeader {
				return 0.0
			}
			return 1.0
		})
		name = fmt.Sprintf(`dragonboat_raftnode_term%s`, label)
		el.term = metrics.GetOrCreateGauge(name, func() float64 {
			return float64(atomic.LoadUint64(&el.termValue))
		})
	}
	return el
}

func (e *raftEventListener) stop() {
}

func (e *raftEventListener) LeaderUpdated(info server.LeaderInfo) {
	atomic.StoreUint64(e.leaderID, info.LeaderID)
	atomic.StoreUint64(&e.termValue, info.Term)
	if e.queue != nil {
		ui := raftio.LeaderInfo{
			ClusterID: info.ClusterID,
			NodeID:    info.NodeID,
			Term:      info.Term,
			LeaderID:  info.LeaderID,
		}
		e.queue.addLeaderInfo(ui)
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
