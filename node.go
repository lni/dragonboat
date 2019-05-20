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

package dragonboat

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/raft"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	snapshotCommitCSlots = uint64(3)
)

var (
	incomingProposalsMaxLen = settings.Soft.IncomingProposalQueueLength
	incomingReadIndexMaxLen = settings.Soft.IncomingReadIndexQueueLength
	lazyFreeCycle           = settings.Soft.LazyFreeCycle
	logUnreachable          = true
)

type node struct {
	readReqCount         uint64
	leaderID             uint64
	raftAddress          string
	config               config.Config
	confChangeC          <-chan *RequestState
	commitC              chan<- rsm.Commit
	mq                   *server.MessageQueue
	lastApplied          uint64
	confirmedLastApplied uint64
	publishedIndex       uint64
	commitReady          func(uint64)
	sendRaftMessage      func(pb.Message)
	sm                   *rsm.StateMachine
	incomingProposals    *entryQueue
	incomingReadIndexes  *readIndexQueue
	pendingProposals     *pendingProposal
	pendingReadIndexes   *pendingReadIndex
	pendingConfigChange  *pendingConfigChange
	raftMu               sync.Mutex
	node                 *raft.Peer
	logreader            *logdb.LogReader
	logdb                raftio.ILogDB
	snapshotter          *snapshotter
	nodeRegistry         transport.INodeRegistry
	stopc                chan struct{}
	clusterInfo          atomic.Value
	tickCount            uint64
	expireNotified       uint64
	rateLimited          bool
	closeOnce            sync.Once
	ss                   *snapshotState
	snapshotLock         *syncutil.Lock
	initializedMu        struct {
		sync.Mutex
		initialized bool
	}
	quiesceManager
}

func newNode(raftAddress string,
	peers map[uint64]string,
	initialMember bool,
	snapshotter *snapshotter,
	dataStore rsm.IManagedStateMachine,
	commitReady func(uint64),
	sendMessage func(pb.Message),
	mq *server.MessageQueue,
	stopc chan struct{},
	nodeRegistry transport.INodeRegistry,
	requestStatePool *sync.Pool,
	config config.Config,
	tickMillisecond uint64,
	ldb raftio.ILogDB) *node {
	proposals := newEntryQueue(incomingProposalsMaxLen, lazyFreeCycle)
	readIndexes := newReadIndexQueue(incomingReadIndexMaxLen)
	confChangeC := make(chan *RequestState, 1)
	pp := newPendingProposal(requestStatePool,
		proposals, config.ClusterID, config.NodeID, raftAddress, tickMillisecond)
	pscr := newPendingReadIndex(requestStatePool, readIndexes, tickMillisecond)
	pcc := newPendingConfigChange(confChangeC, tickMillisecond)
	lr := logdb.NewLogReader(config.ClusterID, config.NodeID, ldb)
	rc := &node{
		config:              config,
		raftAddress:         raftAddress,
		incomingProposals:   proposals,
		incomingReadIndexes: readIndexes,
		confChangeC:         confChangeC,
		commitReady:         commitReady,
		stopc:               stopc,
		pendingProposals:    pp,
		pendingReadIndexes:  pscr,
		pendingConfigChange: pcc,
		nodeRegistry:        nodeRegistry,
		snapshotter:         snapshotter,
		logreader:           lr,
		sendRaftMessage:     sendMessage,
		mq:                  mq,
		logdb:               ldb,
		snapshotLock:        syncutil.NewLock(),
		ss:                  &snapshotState{},
		quiesceManager: quiesceManager{
			electionTick: config.ElectionRTT * 2,
			enabled:      config.Quiesce,
			clusterID:    config.ClusterID,
			nodeID:       config.NodeID,
		},
	}
	nodeProxy := newNodeProxy(rc)
	ordered := config.OrderedConfigChange
	sm := rsm.NewStateMachine(dataStore, snapshotter, ordered, nodeProxy)
	rc.commitC = sm.CommitC()
	rc.sm = sm
	rc.startRaft(config, rc.logreader, peers, initialMember)
	return rc
}

func (rc *node) startRaft(cc config.Config,
	logdb raft.ILogDB, peers map[uint64]string, initial bool) {
	// replay the log when restarting a peer,
	newNode := rc.replayLog(cc.ClusterID, cc.NodeID)
	pas := make([]raft.PeerAddress, 0)
	for k, v := range peers {
		pas = append(pas, raft.PeerAddress{NodeID: k, Address: v})
	}
	node, err := raft.LaunchPeer(&cc, logdb, pas, initial, newNode)
	if err != nil {
		panic(err)
	}
	rc.node = node
}

func (rc *node) close() {
	rc.requestRemoval()
	rc.pendingReadIndexes.close()
	rc.pendingProposals.close()
	rc.pendingConfigChange.close()
}

func (rc *node) stopped() bool {
	select {
	case <-rc.stopc:
		return true
	default:
	}
	return false
}

func (rc *node) requestRemoval() {
	rc.closeOnce.Do(func() {
		close(rc.stopc)
	})
	plog.Infof("%s called requestRemoval()", rc.describe())
}

func (rc *node) shouldStop() <-chan struct{} {
	return rc.stopc
}

func (rc *node) concurrentSnapshot() bool {
	return rc.sm.ConcurrentSnapshot()
}

func (rc *node) proposeSession(session *client.Session,
	handler ICompleteHandler, timeout time.Duration) (*RequestState, error) {
	if !session.ValidForSessionOp(rc.clusterID) {
		return nil, ErrInvalidSession
	}
	return rc.pendingProposals.propose(session, nil, handler, timeout)
}

func (rc *node) propose(session *client.Session,
	cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	if !session.ValidForProposal(rc.clusterID) {
		return nil, ErrInvalidSession
	}
	return rc.pendingProposals.propose(session, cmd, handler, timeout)
}

func (rc *node) read(handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	rs, err := rc.pendingReadIndexes.read(handler, timeout)
	if err == nil {
		rs.node = rc
	}
	return rs, err
}

func (rc *node) requestLeaderTransfer(nodeID uint64) {
	rc.node.RequestLeaderTransfer(nodeID)
}

func (rc *node) requestConfigChange(cct pb.ConfigChangeType,
	nodeID uint64, addr string, orderID uint64,
	timeout time.Duration) (*RequestState, error) {
	cc := pb.ConfigChange{
		Type:           cct,
		NodeID:         nodeID,
		ConfigChangeId: orderID,
		Address:        addr,
	}
	return rc.pendingConfigChange.request(cc, timeout)
}

func (rc *node) requestDeleteNodeWithOrderID(nodeID uint64,
	orderID uint64, timeout time.Duration) (*RequestState, error) {
	return rc.requestConfigChange(pb.RemoveNode,
		nodeID, "", orderID, timeout)
}

func (rc *node) requestDeleteNode(nodeID uint64,
	timeout time.Duration) (*RequestState, error) {
	return rc.requestDeleteNodeWithOrderID(nodeID, 0, timeout)
}

func (rc *node) requestAddNodeWithOrderID(nodeID uint64,
	addr string, orderID uint64, timeout time.Duration) (*RequestState, error) {
	return rc.requestConfigChange(pb.AddNode,
		nodeID, addr, orderID, timeout)
}

func (rc *node) requestAddNode(nodeID uint64,
	addr string, timeout time.Duration) (*RequestState, error) {
	return rc.requestAddNodeWithOrderID(nodeID, addr, 0, timeout)
}

func (rc *node) requestAddObserverWithOrderID(nodeID uint64,
	addr string, orderID uint64, timeout time.Duration) (*RequestState, error) {
	return rc.requestConfigChange(pb.AddObserver,
		nodeID, addr, orderID, timeout)
}

func (rc *node) getLeaderID() (uint64, bool) {
	v := rc.node.GetLeaderID()
	return v, v != raft.NoLeader
}

func (rc *node) notifyOffloaded(from rsm.From) {
	rc.sm.Offloaded(from)
}

func (rc *node) notifyLoaded(from rsm.From) {
	rc.sm.Loaded(from)
}

func (rc *node) entriesToApply(ents []pb.Entry) (nents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	if rc.stopped() {
		return
	}
	lastIdx := ents[len(ents)-1].Index
	if lastIdx < rc.publishedIndex {
		plog.Panicf("%s got entries [%d-%d] older than current state %d",
			rc.describe(), ents[0].Index, lastIdx, rc.publishedIndex)
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.publishedIndex+1 {
		plog.Panicf("%s has hole in to be applied logs, found: %d, want: %d",
			rc.describe(), firstIdx, rc.publishedIndex+1)
	}
	// filter redundant entries that have been previously published
	if rc.publishedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.publishedIndex-firstIdx+1:]
	}
	return
}

func (rc *node) publishCommitRec(rec rsm.Commit) bool {
	if rc.stopped() {
		return false
	}
	select {
	case rc.commitC <- rec:
		rc.commitReady(rc.clusterID)
	case <-rc.stopc:
		return false
	}
	return true
}

func (rc *node) publishEntries(ents []pb.Entry) bool {
	if len(ents) == 0 {
		return true
	}
	rec := rsm.Commit{Entries: ents}
	if !rc.publishCommitRec(rec) {
		return false
	}
	rc.publishedIndex = ents[len(ents)-1].Index
	return true
}

func (rc *node) publishTakeSnapshotRequest() bool {
	rec := rsm.Commit{SnapshotRequested: true}
	return rc.publishCommitRec(rec)
}

func (rc *node) publishSnapshot(snapshot pb.Snapshot,
	lastApplied uint64) bool {
	if pb.IsEmptySnapshot(snapshot) {
		return true
	}
	if snapshot.Index < rc.publishedIndex ||
		snapshot.Index < rc.ss.getSnapshotIndex() ||
		snapshot.Index < lastApplied {
		panic("got a snapshot older than current applied state")
	}
	rec := rsm.Commit{
		SnapshotAvailable: true,
		Index:             snapshot.Index,
	}
	if !rc.publishCommitRec(rec) {
		return false
	}
	rc.ss.setSnapshotIndex(snapshot.Index)
	rc.publishedIndex = snapshot.Index
	return true
}

func (rc *node) replayLog(clusterID uint64, nodeID uint64) bool {
	plog.Infof("%s is replaying logs", rc.describe())
	snapshot, err := rc.snapshotter.GetMostRecentSnapshot()
	if err != nil && err != ErrNoSnapshot {
		panic(err)
	}
	if snapshot.Index > 0 {
		if err = rc.logreader.ApplySnapshot(snapshot); err != nil {
			plog.Panicf("failed to apply snapshot %v", err)
		}
	}
	rs, err := rc.logdb.ReadRaftState(clusterID, nodeID, snapshot.Index)
	if err == raftio.ErrNoSavedLog {
		return true
	}
	if err != nil {
		panic(err)
	}
	if rs.State != nil {
		plog.Infof("%s logdb ents sz %d commit %d term %d",
			rc.describe(), rs.EntryCount, rs.State.Commit, rs.State.Term)
		rc.logreader.SetState(*rs.State)
	}
	rc.logreader.SetRange(rs.FirstIndex, rs.EntryCount)
	newNode := true
	if snapshot.Index > 0 || rs.EntryCount > 0 || rs.State != nil {
		newNode = false
	}
	return newNode
}

func (rc *node) saveSnapshotRequired(lastApplied uint64) bool {
	if rc.config.SnapshotEntries == 0 {
		return false
	}
	si := rc.ss.getSnapshotIndex()
	if rc.publishedIndex <= rc.config.SnapshotEntries+si ||
		lastApplied <= rc.config.SnapshotEntries+si ||
		lastApplied <= rc.config.SnapshotEntries+rc.ss.getReqSnapshotIndex() {
		return false
	}
	plog.Infof("snapshot at index %d requested on %s", lastApplied, rc.describe())
	rc.ss.setReqSnapshotIndex(lastApplied)
	return true
}

func isSoftSnapshotError(err error) bool {
	return err == raft.ErrCompacted || err == raft.ErrSnapshotOutOfDate
}

func (rc *node) saveSnapshot() {
	// this is suppose to be called in snapshot worker thread.
	// calling this rc.sm.GetLastApplied() won't block the raft sm.
	if rc.sm.GetLastApplied() <= rc.ss.getSnapshotIndex() {
		// a snapshot has been published to the sm but not applied yet
		// or the snapshot has been applied and there is no further progress
		return
	}
	ss, ssenv, err := rc.sm.SaveSnapshot()
	if err != nil {
		if err == sm.ErrSnapshotStopped {
			ssenv.MustRemoveTempDir()
			plog.Infof("%s aborted SaveSnapshot", rc.describe())
			return
		} else if isSoftSnapshotError(err) {
			return
		}
		panic(err)
	}
	plog.Infof("%s snapshotted, index %d, term %d, file count %d",
		rc.describe(), ss.Index, ss.Term, len(ss.Files))
	if err := rc.snapshotter.Commit(*ss); err != nil {
		if err == errSnapshotOutOfDate {
			plog.Warningf("snapshot aborted on %s, idx %d", rc.describe(), ss.Index)
			ssenv.MustRemoveTempDir()
			return
		}
		// this can only happen in monkey test
		if err == sm.ErrSnapshotStopped {
			return
		}
		panic(err)
	}
	if !ss.Validate() {
		plog.Panicf("invalid snapshot %v", ss)
	}

	if err = rc.logreader.CreateSnapshot(*ss); err != nil {
		if !isSoftSnapshotError(err) {
			panic(err)
		} else {
			return
		}
	}
	if ss.Index > rc.config.CompactionOverhead {
		rc.ss.setCompactLogTo(ss.Index - rc.config.CompactionOverhead)
	}
	rc.ss.setSnapshotIndex(ss.Index)
}

func (rc *node) recoverFromSnapshot(rec rsm.Commit) (uint64, bool) {
	rc.snapshotLock.Lock()
	defer rc.snapshotLock.Unlock()
	index, err := rc.sm.RecoverFromSnapshot(rec)
	if err == sm.ErrSnapshotStopped {
		plog.Infof("%s aborted its RecoverFromSnapshot", rc.describe())
		return 0, true
	}
	if err != nil {
		panic(err)
	}
	return index, false
}

func (rc *node) saveSnapshotDone() {
	rc.ss.notifySnapshotStatus(true, false, false, 0)
	rc.commitReady(rc.clusterID)
}

func (rc *node) initialSnapshotDone(index uint64) {
	rc.ss.notifySnapshotStatus(false, true, true, index)
	rc.commitReady(rc.clusterID)
}

func (rc *node) recoverFromSnapshotDone() {
	rc.ss.notifySnapshotStatus(false, true, false, 0)
	rc.commitReady(rc.clusterID)
}

func (rc *node) handleCommit(batch []rsm.Commit,
	entries []sm.Entry) (rsm.Commit, bool) {
	return rc.sm.Handle(batch, entries)
}

func (rc *node) removeSnapshotFlagFile(index uint64) error {
	return rc.snapshotter.removeFlagFile(index)
}

func (rc *node) compactLog() error {
	compactTo := rc.ss.getCompactLogTo()
	if compactTo == 0 {
		return nil
	}
	if rc.snapshotLock.TryLock() {
		defer rc.snapshotLock.Unlock()
		if err := rc.logreader.Compact(compactTo); err != nil {
			if err != raft.ErrCompacted {
				return err
			}
		}
		if err := rc.snapshotter.Compaction(rc.clusterID,
			rc.nodeID, compactTo); err != nil {
			return err
		}
		if err := rc.logdb.RemoveEntriesTo(rc.clusterID,
			rc.nodeID, compactTo); err != nil {
			return err
		}
		plog.Infof("%s compacted log at index %d", rc.describe(), compactTo)
	}
	return nil
}

func isFreeOrderMessage(m pb.Message) bool {
	return m.Type == pb.Replicate || m.Type == pb.Ping
}

func (rc *node) sendEnterQuiesceMessages() {
	nodes, _, _, _ := rc.sm.GetMembership()
	for nodeID := range nodes {
		if nodeID != rc.nodeID {
			msg := pb.Message{
				Type:      pb.Quiesce,
				From:      rc.nodeID,
				To:        nodeID,
				ClusterId: rc.clusterID,
			}
			rc.sendRaftMessage(msg)
		}
	}
}

func (rc *node) sendMessages(msgs []pb.Message) {
	for _, msg := range msgs {
		if !isFreeOrderMessage(msg) {
			msg.ClusterId = rc.clusterID
			rc.sendRaftMessage(msg)
		}
	}
}

func (rc *node) sendAppendMessages(ud pb.Update) {
	msgs := ud.Messages
	for _, msg := range msgs {
		if isFreeOrderMessage(msg) {
			msg.ClusterId = rc.clusterID
			rc.sendRaftMessage(msg)
		}
	}
}

func (rc *node) getUpdate() (pb.Update, bool) {
	moreEntriesToApply := rc.canHaveMoreEntriesToApply()
	if rc.node.HasUpdate(moreEntriesToApply) ||
		rc.confirmedLastApplied != rc.lastApplied {
		if rc.lastApplied < rc.confirmedLastApplied {
			plog.Panicf("last applied value moving backwards, %d, now %d",
				rc.confirmedLastApplied, rc.lastApplied)
		}
		ud := rc.node.GetUpdate(moreEntriesToApply, rc.lastApplied)
		for idx := range ud.Messages {
			ud.Messages[idx].ClusterId = rc.clusterID
		}
		rc.confirmedLastApplied = rc.lastApplied
		return ud, true
	}
	return pb.Update{}, false
}

func (rc *node) processReadyToRead(ud pb.Update) {
	if len(ud.ReadyToReads) > 0 {
		rc.pendingReadIndexes.addReadyToRead(ud.ReadyToReads)
		rc.pendingReadIndexes.applied(ud.LastApplied)
	}
}

func (rc *node) processSnapshot(ud pb.Update) bool {
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		if rc.stopped() {
			return false
		}
		err := rc.logreader.ApplySnapshot(ud.Snapshot)
		if err != nil && !isSoftSnapshotError(err) {
			panic(err)
		}
		plog.Infof("%s, snapshot %d is ready to be published", rc.describe(),
			ud.Snapshot.Index)
		if !rc.publishSnapshot(ud.Snapshot, ud.LastApplied) {
			return false
		}
	}
	return true
}

func (rc *node) applyRaftUpdates(ud pb.Update) bool {
	toApply := rc.entriesToApply(ud.CommittedEntries)
	if ok := rc.publishEntries(toApply); !ok {
		return false
	}
	return true
}

func (rc *node) processRaftUpdate(ud pb.Update) bool {
	rc.logreader.Append(ud.EntriesToSave)
	rc.sendMessages(ud.Messages)
	if err := rc.compactLog(); err != nil {
		panic(err)
	}
	if required := rc.saveSnapshotRequired(ud.LastApplied); required {
		return rc.publishTakeSnapshotRequest()
	}
	return true
}

func (rc *node) commitRaftUpdate(ud pb.Update) {
	rc.raftMu.Lock()
	rc.node.Commit(ud)
	rc.raftMu.Unlock()
}

func (rc *node) canHaveMoreEntriesToApply() bool {
	return uint64(cap(rc.commitC)-len(rc.commitC)) > snapshotCommitCSlots
}

func (rc *node) updateBatchedLastApplied() uint64 {
	rc.lastApplied = rc.sm.GetBatchedLastApplied()
	rc.node.NotifyRaftLastApplied(rc.lastApplied)
	return rc.lastApplied
}

func (rc *node) stepNode() (pb.Update, bool) {
	rc.raftMu.Lock()
	defer rc.raftMu.Unlock()
	if rc.initialized() {
		if rc.handleEvents() {
			if rc.newQuiesceState() {
				rc.sendEnterQuiesceMessages()
			}
			return rc.getUpdate()
		}
	}
	return pb.Update{}, false
}

func (rc *node) handleEvents() bool {
	hasEvent := false
	lastApplied := rc.updateBatchedLastApplied()
	if lastApplied != rc.confirmedLastApplied {
		hasEvent = true
	}
	if rc.handleReadIndexRequests() {
		hasEvent = true
	}
	if rc.handleReceivedMessages() {
		hasEvent = true
	}
	if rc.handleConfigChangeMessage() {
		hasEvent = true
	}
	if rc.handleProposals() {
		hasEvent = true
	}
	if hasEvent {
		if rc.expireNotified != rc.tickCount {
			rc.pendingProposals.gc()
			rc.pendingConfigChange.gc()
			rc.expireNotified = rc.tickCount
		}
		rc.pendingReadIndexes.applied(lastApplied)
	}
	return hasEvent
}

func (rc *node) handleProposals() bool {
	rateLimited := rc.node.RateLimited()
	if rc.rateLimited != rateLimited {
		rc.rateLimited = rateLimited
		plog.Infof("%s new rate limit state is %t", rc.describe(), rateLimited)
	}
	entries := rc.incomingProposals.get(rc.rateLimited)
	if len(entries) > 0 {
		rc.node.ProposeEntries(entries)
		return true
	}
	return false
}

func (rc *node) handleReadIndexRequests() bool {
	reqs := rc.incomingReadIndexes.get()
	if len(reqs) > 0 {
		rc.recordActivity(pb.ReadIndex)
		ctx := rc.pendingReadIndexes.peepNextCtx()
		rc.pendingReadIndexes.addPendingRead(ctx, reqs)
		rc.increaseReadReqCount()
		return true
	}
	return false
}

func (rc *node) handleConfigChangeMessage() bool {
	if len(rc.confChangeC) == 0 {
		return false
	}
	select {
	case req, ok := <-rc.confChangeC:
		if !ok {
			rc.confChangeC = nil
		} else {
			rc.recordActivity(pb.ConfigChangeEvent)
			var cc pb.ConfigChange
			if err := cc.Unmarshal(req.data); err != nil {
				panic(err)
			}
			rc.node.ProposeConfigChange(cc, req.key)
		}
	case <-rc.stopc:
		return false
	default:
		return false
	}
	return true
}

func (rc *node) isBusySnapshotting() bool {
	snapshotting := rc.ss.takingSnapshot() || rc.ss.recoveringFromSnapshot()
	return snapshotting && rc.sm.CommitChanBusy()
}

func (rc *node) handleLocalTickMessage(count uint64) {
	if count > rc.config.ElectionRTT {
		count = rc.config.ElectionRTT
	}
	for i := uint64(0); i < count; i++ {
		rc.tick()
	}
}

func (rc *node) tryRecordNodeActivity(m pb.Message) {
	if (m.Type == pb.Heartbeat ||
		m.Type == pb.HeartbeatResp) &&
		m.Hint > 0 {
		rc.recordActivity(pb.ReadIndex)
	} else {
		rc.recordActivity(m.Type)
	}
}

func (rc *node) handleReceivedMessages() bool {
	hasEvent := false
	ltCount := uint64(0)
	scCount := rc.getReadReqCount()
	busy := rc.isBusySnapshotting()
	msgs := rc.mq.Get()
	for _, m := range msgs {
		hasEvent = true
		if m.Type == pb.LocalTick {
			ltCount++
			continue
		}
		if m.Type == pb.Replicate && busy {
			continue
		}
		if done := rc.handleMessage(m); !done {
			if m.ClusterId != rc.clusterID {
				plog.Panicf("received message for cluster %d on %d",
					m.ClusterId, rc.clusterID)
			}
			rc.tryRecordNodeActivity(m)
			rc.node.Handle(m)
		}
	}
	if scCount > 0 {
		rc.batchedReadIndex()
	}
	if lazyFreeCycle > 0 {
		for i := range msgs {
			msgs[i].Entries = nil
		}
	}
	rc.handleLocalTickMessage(ltCount)
	return hasEvent
}

func (rc *node) handleMessage(m pb.Message) bool {
	switch m.Type {
	case pb.Quiesce:
		rc.tryEnterQuiesce()
	case pb.LocalTick:
		rc.tick()
	case pb.SnapshotStatus:
		plog.Debugf("%s ReportSnapshot from %d, rejected %t",
			rc.describe(), m.From, m.Reject)
		rc.node.ReportSnapshotStatus(m.From, m.Reject)
	case pb.Unreachable:
		if logUnreachable {
			plog.Debugf("%s report unreachable from %s",
				rc.describe(), raft.NodeID(m.From))
		}
		rc.node.ReportUnreachableNode(m.From)
	default:
		return false
	}
	return true
}

func (rc *node) applyUpdate(entry pb.Entry,
	result uint64, rejected bool, ignored bool, notifyReadClient bool) {
	if notifyReadClient {
		rc.pendingReadIndexes.applied(entry.Index)
	}
	if !ignored {
		if entry.Key == 0 {
			plog.Panicf("key is 0")
		}
		rc.pendingProposals.applied(entry.ClientID,
			entry.SeriesID, entry.Key, result, rejected)
	}
}

func (rc *node) applyConfigChange(cc pb.ConfigChange) {
	rc.raftMu.Lock()
	defer rc.raftMu.Unlock()
	rc.node.ApplyConfigChange(cc)
	switch cc.Type {
	case pb.AddNode:
		rc.nodeRegistry.AddNode(rc.clusterID, cc.NodeID, string(cc.Address))
	case pb.AddObserver:
		rc.nodeRegistry.AddNode(rc.clusterID, cc.NodeID, string(cc.Address))
	case pb.RemoveNode:
		if cc.NodeID == rc.nodeID {
			plog.Infof("%s applied ConfChange Remove for itself", rc.describe())
			rc.nodeRegistry.RemoveCluster(rc.clusterID)
			rc.requestRemoval()
		} else {
			rc.nodeRegistry.RemoveNode(rc.clusterID, cc.NodeID)
		}
	default:
		panic("unknown config change type")
	}
}

func (rc *node) restoreRemotes(snapshot pb.Snapshot) {
	if snapshot.Membership.ConfigChangeId == 0 {
		panic("invalid snapshot.Metadata.Membership.ConfChangeId")
	}
	rc.raftMu.Lock()
	defer rc.raftMu.Unlock()
	for nid, addr := range snapshot.Membership.Addresses {
		rc.nodeRegistry.AddNode(rc.clusterID, nid, addr)
	}
	for nid, addr := range snapshot.Membership.Observers {
		rc.nodeRegistry.AddNode(rc.clusterID, nid, addr)
	}
	for nid := range snapshot.Membership.Removed {
		if nid == rc.nodeID {
			rc.nodeRegistry.RemoveCluster(rc.clusterID)
			rc.requestRemoval()
		}
	}
	rc.node.RestoreRemotes(snapshot)
	rc.captureClusterConfig()
}

func (rc *node) setInitialStatus(index uint64) {
	if rc.initialized() {
		panic("setInitialStatus called twice")
	}
	plog.Infof("%s initial index set to %d", rc.describe(), index)
	rc.ss.setSnapshotIndex(index)
	rc.publishedIndex = index
	rc.setInitialized()
}

func (rc *node) batchedReadIndex() {
	ctx := rc.pendingReadIndexes.nextCtx()
	rc.node.ReadIndex(ctx)
}

func (rc *node) tick() {
	if rc.node == nil {
		panic("rc node is still nil")
	}
	rc.tickCount++
	if rc.tickCount%rc.electionTick == 0 {
		rc.leaderID = rc.node.LocalStatus().LeaderID
	}
	rc.increaseQuiesceTick()
	if rc.quiesced() {
		rc.node.QuiescedTick()
	} else {
		rc.node.Tick()
	}
	rc.pendingProposals.increaseTick()
	rc.pendingReadIndexes.increaseTick()
	rc.pendingConfigChange.increaseTick()
}

func (rc *node) captureClusterConfig() {
	// this can only be called when RSM is not stepping any updates
	// currently it is called from a RSM step function and from
	// ApplySnapshot
	nodes, _, _, index := rc.sm.GetMembership()
	if len(nodes) == 0 {
		plog.Panicf("empty nodes")
	}
	plog.Infof("%s called captureClusterConfig, nodes %v",
		rc.describe(), nodes)
	ci := &ClusterInfo{
		ClusterID:         rc.clusterID,
		NodeID:            rc.nodeID,
		IsLeader:          rc.isLeader(),
		ConfigChangeIndex: index,
		Nodes:             nodes,
	}
	rc.clusterInfo.Store(ci)
}

func (rc *node) getClusterInfo() *ClusterInfo {
	v := rc.clusterInfo.Load()
	if v == nil {
		return &ClusterInfo{
			ClusterID: rc.clusterID,
			NodeID:    rc.nodeID,
			Pending:   true,
		}
	}
	ci := v.(*ClusterInfo)
	return &ClusterInfo{
		ClusterID:         ci.ClusterID,
		NodeID:            ci.NodeID,
		IsLeader:          rc.isLeader(),
		ConfigChangeIndex: ci.ConfigChangeIndex,
		Nodes:             ci.Nodes,
	}
}

func (rc *node) describe() string {
	return logutil.DescribeNode(rc.clusterID, rc.nodeID)
}

func (rc *node) isLeader() bool {
	if rc.node != nil {
		leaderID := rc.node.GetLeaderID()
		return rc.nodeID == leaderID
	}
	return false
}

func (rc *node) isFollower() bool {
	if rc.node != nil {
		leaderID := rc.node.GetLeaderID()
		if leaderID != rc.nodeID && leaderID != raft.NoLeader {
			return true
		}
	}
	return false
}

func (rc *node) getStateMachineHash() uint64 {
	return rc.sm.GetHash()
}

func (rc *node) getSessionHash() uint64 {
	return rc.sm.GetSessionHash()
}

func (rc *node) getMembershipHash() uint64 {
	return rc.sm.GetMembershipHash()
}

func (rc *node) dumpRaftInfoToLog() {
	if rc.node != nil {
		addrMap := make(map[uint64]string)
		nodes, _, _, _ := rc.sm.GetMembership()
		for nodeID := range nodes {
			if nodeID == rc.nodeID {
				addrMap[nodeID] = rc.raftAddress
			} else {
				v, _, err := rc.nodeRegistry.Resolve(rc.clusterID, nodeID)
				if err == nil {
					addrMap[nodeID] = v
				}
			}
		}
		rc.node.DumpRaftInfoToLog(addrMap)
	}
}

func (rc *node) increaseReadReqCount() {
	atomic.AddUint64(&rc.readReqCount, 1)
}

func (rc *node) getReadReqCount() uint64 {
	return atomic.SwapUint64(&rc.readReqCount, 0)
}

func (rc *node) initialized() bool {
	rc.initializedMu.Lock()
	defer rc.initializedMu.Unlock()
	return rc.initializedMu.initialized
}

func (rc *node) setInitialized() {
	rc.initializedMu.Lock()
	defer rc.initializedMu.Unlock()
	rc.initializedMu.initialized = true
}
