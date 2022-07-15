// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/raftio"
)

// WriteHealthMetrics writes all health metrics in Prometheus format to the
// specified writer. This function is typically called by the metrics http
// handler.
func WriteHealthMetrics(w io.Writer) {
	metrics.WritePrometheus(w, false)
}

type raftEventListener struct {
	readIndexDropped    *metrics.Counter
	proposalDropped     *metrics.Counter
	replicationRejected *metrics.Counter
	snapshotRejected    *metrics.Counter
	queue               *leaderInfoQueue
	hasLeader           *metrics.Gauge
	term                *metrics.Gauge
	campaignLaunched    *metrics.Counter
	campaignSkipped     *metrics.Counter
	leaderID            uint64
	termValue           uint64
	replicaID           uint64
	shardID             uint64
	metrics             bool
}

var _ server.IRaftEventListener = (*raftEventListener)(nil)

func newRaftEventListener(shardID uint64, replicaID uint64,
	useMetrics bool, queue *leaderInfoQueue) *raftEventListener {
	el := &raftEventListener{
		shardID:   shardID,
		replicaID: replicaID,
		metrics:   useMetrics,
		queue:     queue,
	}
	if useMetrics {
		label := fmt.Sprintf(`{shardid="%d",replicaid="%d"}`, shardID, replicaID)
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
			if atomic.LoadUint64(&el.leaderID) == raftio.NoLeader {
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

func (e *raftEventListener) close() {
}

func (e *raftEventListener) LeaderUpdated(info server.LeaderInfo) {
	atomic.StoreUint64(&e.leaderID, info.LeaderID)
	atomic.StoreUint64(&e.termValue, info.Term)
	if e.queue != nil {
		ui := raftio.LeaderInfo{
			ShardID:   info.ShardID,
			ReplicaID: info.ReplicaID,
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

type sysEventListener struct {
	stopc  chan struct{}
	events chan server.SystemEvent
	ul     raftio.ISystemEventListener
}

func newSysEventListener(l raftio.ISystemEventListener,
	stopc chan struct{}) *sysEventListener {
	return &sysEventListener{
		stopc:  stopc,
		events: make(chan server.SystemEvent),
		ul:     l,
	}
}

func (l *sysEventListener) Publish(e server.SystemEvent) {
	if l.ul == nil {
		return
	}
	select {
	case l.events <- e:
	case <-l.stopc:
		return
	}
}

func (l *sysEventListener) handle(e server.SystemEvent) {
	if l.ul == nil {
		return
	}
	switch e.Type {
	case server.NodeHostShuttingDown:
		l.ul.NodeHostShuttingDown()
	case server.NodeReady:
		l.ul.NodeReady(getNodeInfo(e))
	case server.NodeUnloaded:
		l.ul.NodeUnloaded(getNodeInfo(e))
	case server.NodeDeleted:
		l.ul.NodeDeleted(getNodeInfo(e))
	case server.MembershipChanged:
		l.ul.MembershipChanged(getNodeInfo(e))
	case server.ConnectionEstablished:
		l.ul.ConnectionEstablished(getConnectionInfo(e))
	case server.ConnectionFailed:
		l.ul.ConnectionFailed(getConnectionInfo(e))
	case server.SendSnapshotStarted:
		l.ul.SendSnapshotStarted(getSnapshotInfo(e))
	case server.SendSnapshotCompleted:
		l.ul.SendSnapshotCompleted(getSnapshotInfo(e))
	case server.SendSnapshotAborted:
		l.ul.SendSnapshotAborted(getSnapshotInfo(e))
	case server.SnapshotReceived:
		l.ul.SnapshotReceived(getSnapshotInfo(e))
	case server.SnapshotRecovered:
		l.ul.SnapshotRecovered(getSnapshotInfo(e))
	case server.SnapshotCreated:
		l.ul.SnapshotCreated(getSnapshotInfo(e))
	case server.SnapshotCompacted:
		l.ul.SnapshotCompacted(getSnapshotInfo(e))
	case server.LogCompacted:
		l.ul.LogCompacted(getEntryInfo(e))
	case server.LogDBCompacted:
		l.ul.LogDBCompacted(getEntryInfo(e))
	default:
		panic("unknown event type")
	}
}

func getSnapshotInfo(e server.SystemEvent) raftio.SnapshotInfo {
	return raftio.SnapshotInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
		From:      e.From,
		Index:     e.Index,
	}
}

func getNodeInfo(e server.SystemEvent) raftio.NodeInfo {
	return raftio.NodeInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
	}
}

func getEntryInfo(e server.SystemEvent) raftio.EntryInfo {
	return raftio.EntryInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
		Index:     e.Index,
	}
}

func getConnectionInfo(e server.SystemEvent) raftio.ConnectionInfo {
	return raftio.ConnectionInfo{
		Address:            e.Address,
		SnapshotConnection: e.SnapshotConnection,
	}
}
