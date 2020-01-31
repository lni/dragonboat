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

var _ server.IRaftEventListener = &raftEventListener{}

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

type sysEventListener struct {
	stopc        chan struct{}
	events       chan server.SystemEvent
	userListener raftio.ISystemEventListener
}

func newSysEventListener(l raftio.ISystemEventListener,
	stopc chan struct{}) *sysEventListener {
	return &sysEventListener{
		stopc:        stopc,
		events:       make(chan server.SystemEvent),
		userListener: l,
	}
}

func (l *sysEventListener) Publish(e server.SystemEvent) {
	if l.userListener == nil {
		return
	}
	select {
	case l.events <- e:
	case <-l.stopc:
		return
	}
}

func (l *sysEventListener) handle(e server.SystemEvent) {
	switch e.Type {
	case server.NodeHostShuttingDown:
		l.handleNodeHostShuttingDown(e)
	case server.NodeReady:
		l.handleNodeReady(e)
	case server.NodeUnloaded:
		l.handleNodeUnloaded(e)
	case server.MembershipChanged:
		l.handleMembershipChanged(e)
	case server.ConnectionEstablished:
		l.handleConnectionEstablished(e)
	case server.ConnectionFailed:
		l.handleConnectionFailed(e)
	case server.SendSnapshotStarted:
		l.handleSendSnapshotStarted(e)
	case server.SendSnapshotCompleted:
		l.handleSendSnapshotCompleted(e)
	case server.SendSnapshotAborted:
		l.handleSendSnapshotAborted(e)
	case server.SnapshotReceived:
		l.handleSnapshotReceived(e)
	case server.SnapshotRecovered:
		l.handleSnapshotRecovered(e)
	case server.SnapshotCreated:
		l.handleSnapshotCreated(e)
	case server.SnapshotCompacted:
		l.handleSnapshotCompacted(e)
	case server.LogCompacted:
		l.handleLogCompacted(e)
	case server.LogDBCompacted:
		l.handleLogDBCompacted(e)
	default:
		panic("unknown event type")
	}
}

func getSnapshotInfo(e server.SystemEvent) raftio.SnapshotInfo {
	return raftio.SnapshotInfo{
		ClusterID: e.ClusterID,
		NodeID:    e.NodeID,
		From:      e.From,
		Index:     e.Index,
	}
}

func getNodeInfo(e server.SystemEvent) raftio.NodeInfo {
	return raftio.NodeInfo{
		ClusterID: e.ClusterID,
		NodeID:    e.NodeID,
	}
}

func getEntryInfo(e server.SystemEvent) raftio.EntryInfo {
	return raftio.EntryInfo{
		ClusterID: e.ClusterID,
		NodeID:    e.NodeID,
		Index:     e.Index,
	}
}

func getConnectionInfo(e server.SystemEvent) raftio.ConnectionInfo {
	return raftio.ConnectionInfo{
		Address:            e.Address,
		SnapshotConnection: e.SnapshotConnection,
	}
}

func (l *sysEventListener) handleNodeHostShuttingDown(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.NodeHostShuttingDown()
	}
}

func (l *sysEventListener) handleNodeReady(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.NodeReady(getNodeInfo(e))
	}
}

func (l *sysEventListener) handleNodeUnloaded(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.NodeUnloaded(getNodeInfo(e))
	}
}

func (l *sysEventListener) handleMembershipChanged(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.MembershipChanged(getNodeInfo(e))
	}
}

func (l *sysEventListener) handleConnectionEstablished(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.ConnectionEstablished(getConnectionInfo(e))
	}
}

func (l *sysEventListener) handleConnectionFailed(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.ConnectionFailed(getConnectionInfo(e))
	}
}

func (l *sysEventListener) handleSendSnapshotStarted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SendSnapshotStarted(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSendSnapshotCompleted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SendSnapshotCompleted(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSendSnapshotAborted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SendSnapshotAborted(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSnapshotReceived(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SnapshotReceived(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSnapshotRecovered(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SnapshotRecovered(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSnapshotCreated(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SnapshotCreated(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleSnapshotCompacted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.SnapshotCompacted(getSnapshotInfo(e))
	}
}

func (l *sysEventListener) handleLogCompacted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.LogCompacted(getEntryInfo(e))
	}
}

func (l *sysEventListener) handleLogDBCompacted(e server.SystemEvent) {
	if l.userListener != nil {
		l.userListener.LogDBCompacted(getEntryInfo(e))
	}
}
