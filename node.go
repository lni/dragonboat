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
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	incomingProposalsMaxLen = settings.Soft.IncomingProposalQueueLength
	incomingReadIndexMaxLen = settings.Soft.IncomingReadIndexQueueLength
	syncTaskInterval        = settings.Soft.SyncTaskInterval
	lazyFreeCycle           = settings.Soft.LazyFreeCycle
)

type engine interface {
	setNodeReady(clusterID uint64)
	setTaskReady(clusterID uint64)
	setStreamReady(clusterID uint64)
	setRequestedSnapshotReady(clusterID uint64)
	setAvailableSnapshotReady(clusterID uint64)
}

type node struct {
	readReqCount          uint64
	leaderID              uint64
	instanceID            uint64
	raftAddress           string
	config                config.Config
	confChangeC           <-chan configChangeRequest
	snapshotC             <-chan rsm.SnapshotRequest
	taskQ                 *rsm.TaskQueue
	mq                    *server.MessageQueue
	smAppliedIndex        uint64
	confirmedIndex        uint64
	pushedIndex           uint64
	engine                engine
	getStreamConnection   func(uint64, uint64) pb.IChunkSink
	handleSnapshotStatus  func(uint64, uint64, bool)
	sendRaftMessage       func(pb.Message)
	sm                    *rsm.StateMachine
	smType                pb.StateMachineType
	incomingProposals     *entryQueue
	incomingReadIndexes   *readIndexQueue
	pendingProposals      *pendingProposal
	pendingReadIndexes    *pendingReadIndex
	pendingConfigChange   *pendingConfigChange
	pendingSnapshot       *pendingSnapshot
	pendingLeaderTransfer *pendingLeaderTransfer
	raftMu                sync.Mutex
	p                     *raft.Peer
	logreader             *logdb.LogReader
	logdb                 raftio.ILogDB
	snapshotter           *snapshotter
	nodeRegistry          transport.INodeRegistry
	stopc                 chan struct{}
	clusterInfo           atomic.Value
	tickCount             uint64
	expireNotified        uint64
	tickMillisecond       uint64
	syncTask              *task
	rateLimited           bool
	new                   bool
	closeOnce             sync.Once
	ss                    *snapshotState
	snapshotLock          *syncutil.Lock
	raftEvents            *raftEventListener
	initializedMu         struct {
		sync.Mutex
		initialized bool
	}
	quiesceManager
}

var instanceID uint64

func newNode(raftAddress string,
	peers map[uint64]string,
	initialMember bool,
	snapshotter *snapshotter,
	dataStore rsm.IManagedStateMachine,
	smType pb.StateMachineType,
	engine engine,
	liQueue *leaderInfoQueue,
	getStreamConnection func(uint64, uint64) pb.IChunkSink,
	handleSnapshotStatus func(uint64, uint64, bool),
	sendMessage func(pb.Message),
	mq *server.MessageQueue,
	stopc chan struct{},
	nodeRegistry transport.INodeRegistry,
	requestStatePool *sync.Pool,
	config config.Config,
	useMetrics bool,
	tickMillisecond uint64,
	ldb raftio.ILogDB) (*node, error) {
	proposals := newEntryQueue(incomingProposalsMaxLen, lazyFreeCycle)
	readIndexes := newReadIndexQueue(incomingReadIndexMaxLen)
	confChangeC := make(chan configChangeRequest, 1)
	snapshotC := make(chan rsm.SnapshotRequest, 1)
	leaderTransfer := newPendingLeaderTransfer()
	pp := newPendingProposal(requestStatePool,
		proposals, config.ClusterID, config.NodeID, raftAddress, tickMillisecond)
	pscr := newPendingReadIndex(requestStatePool, readIndexes, tickMillisecond)
	pcc := newPendingConfigChange(confChangeC, tickMillisecond)
	ps := newPendingSnapshot(snapshotC, tickMillisecond)
	lr := logdb.NewLogReader(config.ClusterID, config.NodeID, ldb)
	rn := &node{
		instanceID:            atomic.AddUint64(&instanceID, 1),
		tickMillisecond:       tickMillisecond,
		config:                config,
		raftAddress:           raftAddress,
		incomingProposals:     proposals,
		incomingReadIndexes:   readIndexes,
		confChangeC:           confChangeC,
		snapshotC:             snapshotC,
		engine:                engine,
		getStreamConnection:   getStreamConnection,
		handleSnapshotStatus:  handleSnapshotStatus,
		stopc:                 stopc,
		pendingProposals:      pp,
		pendingReadIndexes:    pscr,
		pendingConfigChange:   pcc,
		pendingSnapshot:       ps,
		pendingLeaderTransfer: leaderTransfer,
		nodeRegistry:          nodeRegistry,
		snapshotter:           snapshotter,
		logreader:             lr,
		sendRaftMessage:       sendMessage,
		mq:                    mq,
		logdb:                 ldb,
		snapshotLock:          syncutil.NewLock(),
		ss:                    &snapshotState{},
		syncTask:              newTask(syncTaskInterval),
		smType:                smType,
		quiesceManager: quiesceManager{
			electionTick: config.ElectionRTT * 2,
			enabled:      config.Quiesce,
			clusterID:    config.ClusterID,
			nodeID:       config.NodeID,
		},
	}
	ordered := config.OrderedConfigChange
	sm := rsm.NewStateMachine(dataStore, snapshotter, ordered, rn)
	rn.taskQ = sm.TaskQ()
	rn.sm = sm
	rn.raftEvents = newRaftEventListener(config.ClusterID,
		config.NodeID, &rn.leaderID, useMetrics, liQueue)
	new, err := rn.startRaft(config, lr, peers, initialMember)
	if err != nil {
		return nil, err
	}
	rn.new = new
	return rn, nil
}

func (n *node) NodeID() uint64 {
	return n.nodeID
}

func (n *node) ClusterID() uint64 {
	return n.clusterID
}

func (n *node) ShouldStop() <-chan struct{} {
	return n.stopc
}

func (n *node) NodeReady() {
	n.engine.setNodeReady(n.clusterID)
}

func (n *node) ApplyUpdate(entry pb.Entry,
	result sm.Result, rejected bool, ignored bool, notifyReadClient bool) {
	if notifyReadClient {
		n.pendingReadIndexes.applied(entry.Index)
	}
	if !ignored {
		if entry.Key == 0 {
			panic("key is 0")
		}
		n.pendingProposals.applied(entry.ClientID,
			entry.SeriesID, entry.Key, result, rejected)
	}
}

func (n *node) ApplyConfigChange(cc pb.ConfigChange) {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	n.p.ApplyConfigChange(cc)
	switch cc.Type {
	case pb.AddNode:
		n.nodeRegistry.AddNode(n.clusterID, cc.NodeID, string(cc.Address))
	case pb.AddObserver:
		n.nodeRegistry.AddNode(n.clusterID, cc.NodeID, string(cc.Address))
	case pb.RemoveNode:
		if cc.NodeID == n.nodeID {
			plog.Infof("%s applied ConfChange Remove for itself", n.id())
			n.nodeRegistry.RemoveCluster(n.clusterID)
			n.requestRemoval()
		} else {
			n.nodeRegistry.RemoveNode(n.clusterID, cc.NodeID)
		}
	default:
		panic("unknown config change type")
	}
}

func (n *node) RestoreRemotes(snapshot pb.Snapshot) {
	if snapshot.Membership.ConfigChangeId == 0 {
		panic("invalid snapshot.Metadata.Membership.ConfChangeId")
	}
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	for nid, addr := range snapshot.Membership.Addresses {
		n.nodeRegistry.AddNode(n.clusterID, nid, addr)
	}
	for nid, addr := range snapshot.Membership.Observers {
		n.nodeRegistry.AddNode(n.clusterID, nid, addr)
	}
	for nid := range snapshot.Membership.Removed {
		if nid == n.nodeID {
			n.nodeRegistry.RemoveCluster(n.clusterID)
			n.requestRemoval()
		}
	}
	plog.Infof("%s is restoring remotes %+v", n.id(), snapshot.Membership)
	n.p.RestoreRemotes(snapshot)
	n.captureClusterState()
}

func (n *node) ConfigChangeProcessed(key uint64, accepted bool) {
	if accepted {
		n.pendingConfigChange.apply(key, false)
		n.captureClusterState()
	} else {
		n.p.RejectConfigChange()
		n.pendingConfigChange.apply(key, true)
	}
}

func (n *node) startRaft(cc config.Config,
	logdb raft.ILogDB, peers map[uint64]string, initial bool) (bool, error) {
	newNode, err := n.replayLog(cc.ClusterID, cc.NodeID)
	if err != nil {
		return false, err
	}
	pas := make([]raft.PeerAddress, 0)
	for k, v := range peers {
		pas = append(pas, raft.PeerAddress{NodeID: k, Address: v})
	}
	n.p = raft.Launch(&cc, logdb, n.raftEvents, pas, initial, newNode)
	return newNode, nil
}

func (n *node) close() {
	n.requestRemoval()
	n.raftEvents.stop()
	n.pendingReadIndexes.close()
	n.pendingProposals.close()
	n.pendingConfigChange.close()
	n.pendingSnapshot.close()
}

func (n *node) stopped() bool {
	select {
	case <-n.stopc:
		return true
	default:
	}
	return false
}

func (n *node) requestRemoval() {
	n.closeOnce.Do(func() {
		close(n.stopc)
	})
	plog.Infof("%s called requestRemoval()", n.id())
}

func (n *node) shouldStop() <-chan struct{} {
	return n.stopc
}

func (n *node) concurrentSnapshot() bool {
	return n.sm.ConcurrentSnapshot()
}

func (n *node) supportClientSession() bool {
	return !n.OnDiskStateMachine()
}

func (n *node) OnDiskStateMachine() bool {
	return n.sm.OnDiskStateMachine()
}

func (n *node) proposeSession(session *client.Session,
	handler ICompleteHandler, timeout time.Duration) (*RequestState, error) {
	if !session.ValidForSessionOp(n.clusterID) {
		return nil, ErrInvalidSession
	}
	return n.pendingProposals.propose(session, nil, handler, timeout)
}

func (n *node) payloadTooBig(sz int) bool {
	if n.config.MaxInMemLogSize == 0 {
		return false
	}
	return uint64(sz+settings.EntryNonCmdFieldsSize) > n.config.MaxInMemLogSize
}

func (n *node) propose(session *client.Session,
	cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	if !session.ValidForProposal(n.clusterID) {
		return nil, ErrInvalidSession
	}
	if n.payloadTooBig(len(cmd)) {
		return nil, ErrPayloadTooBig
	}
	return n.pendingProposals.propose(session, cmd, handler, timeout)
}

func (n *node) read(handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	rs, err := n.pendingReadIndexes.read(handler, timeout)
	if err == nil {
		rs.node = n
	}
	return rs, err
}

func (n *node) requestLeaderTransfer(nodeID uint64) error {
	return n.pendingLeaderTransfer.request(nodeID)
}

func (n *node) requestSnapshot(opt SnapshotOption,
	timeout time.Duration) (*RequestState, error) {
	st := rsm.UserRequestedSnapshot
	if opt.Exported {
		plog.Infof("export snapshot called on %s", n.id())
		st = rsm.ExportedSnapshot
		exist, err := fileutil.Exist(opt.ExportPath)
		if err != nil {
			return nil, err
		}
		if !exist {
			return nil, ErrDirNotExist
		}
	} else {
		if len(opt.ExportPath) > 0 {
			plog.Warningf("opt.ExportPath set when not exporting a snapshot")
			opt.ExportPath = ""
		}
	}
	return n.pendingSnapshot.request(st, opt.ExportPath, timeout)
}

func (n *node) reportIgnoredSnapshotRequest(key uint64) {
	n.pendingSnapshot.apply(key, true, 0)
}

func (n *node) requestConfigChange(cct pb.ConfigChangeType,
	nodeID uint64, addr string, orderID uint64,
	timeout time.Duration) (*RequestState, error) {
	cc := pb.ConfigChange{
		Type:           cct,
		NodeID:         nodeID,
		ConfigChangeId: orderID,
		Address:        addr,
	}
	return n.pendingConfigChange.request(cc, timeout)
}

func (n *node) requestDeleteNodeWithOrderID(nodeID uint64,
	orderID uint64, timeout time.Duration) (*RequestState, error) {
	return n.requestConfigChange(pb.RemoveNode,
		nodeID, "", orderID, timeout)
}

func (n *node) requestAddNodeWithOrderID(nodeID uint64,
	addr string, orderID uint64, timeout time.Duration) (*RequestState, error) {
	return n.requestConfigChange(pb.AddNode,
		nodeID, addr, orderID, timeout)
}

func (n *node) requestAddObserverWithOrderID(nodeID uint64,
	addr string, orderID uint64, timeout time.Duration) (*RequestState, error) {
	return n.requestConfigChange(pb.AddObserver,
		nodeID, addr, orderID, timeout)
}

func (n *node) getLeaderID() (uint64, bool) {
	v := atomic.LoadUint64(&n.leaderID)
	return v, v != raft.NoLeader
}

func (n *node) notifyOffloaded(from rsm.From) {
	n.sm.Offloaded(from)
}

func (n *node) notifyLoaded(from rsm.From) {
	n.sm.Loaded(from)
}

func (n *node) entriesToApply(ents []pb.Entry) (newents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	if n.stopped() {
		return
	}
	lastIdx := ents[len(ents)-1].Index
	if lastIdx < n.pushedIndex {
		plog.Panicf("%s got entries [%d-%d] older than current state %d",
			n.id(), ents[0].Index, lastIdx, n.pushedIndex)
	}
	firstIdx := ents[0].Index
	if firstIdx > n.pushedIndex+1 {
		plog.Panicf("%s has hole in to be applied logs, found: %d, want: %d",
			n.id(), firstIdx, n.pushedIndex+1)
	}
	if n.pushedIndex-firstIdx+1 < uint64(len(ents)) {
		newents = ents[n.pushedIndex-firstIdx+1:]
	}
	return
}

func (n *node) pushTask(rec rsm.Task) bool {
	n.taskQ.Add(rec)
	n.engine.setTaskReady(n.clusterID)
	return !n.stopped()
}

func (n *node) pushEntries(ents []pb.Entry) bool {
	if len(ents) == 0 {
		return true
	}
	rec := rsm.Task{Entries: ents}
	if !n.pushTask(rec) {
		return false
	}
	n.pushedIndex = ents[len(ents)-1].Index
	return true
}

func (n *node) pushStreamSnapshotRequest(clusterID uint64, nodeID uint64) bool {
	rec := rsm.Task{
		ClusterID:      clusterID,
		NodeID:         nodeID,
		StreamSnapshot: true,
	}
	return n.pushTask(rec)
}

func (n *node) pushTakeSnapshotRequest(req rsm.SnapshotRequest) bool {
	rec := rsm.Task{SnapshotRequested: true, SnapshotRequest: req}
	return n.pushTask(rec)
}

func (n *node) pushSnapshot(snapshot pb.Snapshot,
	lastApplied uint64) bool {
	if pb.IsEmptySnapshot(snapshot) {
		return true
	}
	if snapshot.Index < n.pushedIndex ||
		snapshot.Index < n.ss.getSnapshotIndex() ||
		snapshot.Index < lastApplied {
		panic("got a snapshot older than current applied state")
	}
	rec := rsm.Task{
		SnapshotAvailable: true,
		Index:             snapshot.Index,
	}
	if !n.pushTask(rec) {
		return false
	}
	n.ss.setSnapshotIndex(snapshot.Index)
	n.pushedIndex = snapshot.Index
	return true
}

func (n *node) replayLog(clusterID uint64, nodeID uint64) (bool, error) {
	plog.Infof("%s is replaying logs", n.id())
	snapshot, err := n.snapshotter.GetMostRecentSnapshot()
	if err != nil && err != ErrNoSnapshot {
		return false, err
	}
	if snapshot.Index > 0 {
		if err = n.logreader.ApplySnapshot(snapshot); err != nil {
			plog.Errorf("failed to apply snapshot %v", err)
			return false, err
		}
	}
	rs, err := n.logdb.ReadRaftState(clusterID, nodeID, snapshot.Index)
	if err == raftio.ErrNoSavedLog {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if rs.State != nil {
		plog.Infof("%s logdb entries size %d commit %d term %d",
			n.id(), rs.EntryCount, rs.State.Commit, rs.State.Term)
		n.logreader.SetState(*rs.State)
	}
	n.logreader.SetRange(rs.FirstIndex, rs.EntryCount)
	newNode := true
	if snapshot.Index > 0 || rs.EntryCount > 0 || rs.State != nil {
		newNode = false
	}
	return newNode, nil
}

func (n *node) saveSnapshotRequired(lastApplied uint64) bool {
	if n.config.SnapshotEntries == 0 {
		return false
	}
	si := n.ss.getSnapshotIndex()
	if n.pushedIndex <= n.config.SnapshotEntries+si ||
		lastApplied <= n.config.SnapshotEntries+si ||
		lastApplied <= n.config.SnapshotEntries+n.ss.getReqSnapshotIndex() {
		return false
	}
	if n.isBusySnapshotting() {
		return false
	}
	plog.Infof("snapshot at index %d requested on %s", lastApplied, n.id())
	n.ss.setReqSnapshotIndex(lastApplied)
	return true
}

func isSoftSnapshotError(err error) bool {
	return err == raft.ErrCompacted || err == raft.ErrSnapshotOutOfDate
}

func (n *node) saveSnapshot(rec rsm.Task) error {
	index, err := n.doSaveSnapshot(rec.SnapshotRequest)
	if err != nil {
		return err
	}
	n.pendingSnapshot.apply(rec.SnapshotRequest.Key, index == 0, index)
	return nil
}

func (n *node) doSaveSnapshot(req rsm.SnapshotRequest) (uint64, error) {
	n.snapshotLock.Lock()
	defer n.snapshotLock.Unlock()
	if !req.IsExportedSnapshot() &&
		n.sm.GetLastApplied() <= n.ss.getSnapshotIndex() {
		// a snapshot has been pushed to the sm but not applied yet
		// or the snapshot has been applied and there is no further progress
		return 0, nil
	}
	ss, ssenv, err := n.sm.SaveSnapshot(req)
	if err != nil {
		if err == sm.ErrSnapshotStopped {
			ssenv.MustRemoveTempDir()
			plog.Infof("%s aborted SaveSnapshot", n.id())
			return 0, nil
		} else if isSoftSnapshotError(err) || err == rsm.ErrTestKnobReturn {
			return 0, nil
		}
		plog.Errorf("%s SaveSnapshot failed %v", n.id(), err)
		return 0, err
	}
	if tests.ReadyToReturnTestKnob(n.stopc, true, "snapshotter.Commit") {
		return 0, nil
	}
	plog.Infof("%s snapshotted, index %d, term %d, file count %d",
		n.id(), ss.Index, ss.Term, len(ss.Files))
	if err := n.snapshotter.Commit(*ss, req); err != nil {
		plog.Errorf("%s Commit failed %v", n.id(), err)
		if err == errSnapshotOutOfDate {
			ssenv.MustRemoveTempDir()
			return 0, nil
		}
		// this can only happen in monkey test
		if err == sm.ErrSnapshotStopped {
			return 0, nil
		}
		return 0, err
	}
	if req.IsExportedSnapshot() {
		return ss.Index, nil
	}
	if !ss.Validate() {
		plog.Panicf("%s generated invalid snapshot %v", n.id(), ss)
	}
	if err = n.logreader.CreateSnapshot(*ss); err != nil {
		plog.Errorf("%s CreateSnapshot failed %v", n.id(), err)
		if !isSoftSnapshotError(err) {
			return 0, err
		}
		return 0, nil
	}
	if ss.Index > n.config.CompactionOverhead {
		n.ss.setCompactLogTo(ss.Index - n.config.CompactionOverhead)
		if err := n.snapshotter.Compact(ss.Index); err != nil {
			plog.Errorf("%s snapshotter.Compact failed %v", n.id(), err)
			return 0, err
		}
	}
	n.ss.setSnapshotIndex(ss.Index)
	return ss.Index, nil
}

func (n *node) streamSnapshot(sink pb.IChunkSink) error {
	if sink != nil {
		target := sink.ToNodeID
		plog.Infof("%s called streamSnapshot to node %d", n.id(), target)
		if err := n.doStreamSnapshot(sink); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) doStreamSnapshot(sink pb.IChunkSink) error {
	if err := n.sm.StreamSnapshot(sink); err != nil {
		plog.Errorf("%s StreamSnapshot failed %v", n.id(), err)
		if err != sm.ErrSnapshotStopped &&
			err != sm.ErrSnapshotStreaming &&
			err != rsm.ErrTestKnobReturn {
			return err
		}
	}
	return nil
}

func (n *node) recoverFromSnapshot(rec rsm.Task) (uint64, error) {
	n.snapshotLock.Lock()
	defer n.snapshotLock.Unlock()
	var index uint64
	var err error
	if rec.InitialSnapshot && n.OnDiskStateMachine() {
		plog.Infof("all disk SM %s beng initialized", n.id())
		idx, err := n.sm.OpenOnDiskStateMachine()
		if err == sm.ErrSnapshotStopped || err == sm.ErrOpenStopped {
			plog.Infof("%s aborted OpenOnDiskStateMachine", n.id())
			return 0, sm.ErrOpenStopped
		}
		if err != nil {
			plog.Errorf("OpenOnDiskStateMachine failed %v, %s", err, n.id())
			return 0, err
		}
		if idx > 0 && rec.NewNode {
			plog.Panicf("Open returned index %d (>0) on new node %s", idx, n.id())
		}
	}
	index, err = n.sm.RecoverFromSnapshot(rec)
	if err == sm.ErrSnapshotStopped {
		plog.Infof("%s aborted its RecoverFromSnapshot", n.id())
		return 0, err
	}
	if err != nil && err != raft.ErrSnapshotOutOfDate {
		plog.Errorf("%s RecoverFromSnapshot failed %v", n.id(), err)
		return 0, err
	}
	if index > 0 {
		if n.OnDiskStateMachine() {
			if err := n.sm.Sync(); err != nil {
				plog.Errorf("sm.Sync failed %v", err)
				return 0, err
			}
			if err := n.snapshotter.Shrink(index); err != nil {
				plog.Errorf("snapshotter.Shrink snapshot failed %v", err)
				return 0, err
			}
		}
		if err := n.snapshotter.Compact(index); err != nil {
			plog.Errorf("snapshotter.Compact failed %v", err)
			return 0, err
		}
	}
	return index, nil
}

func (n *node) streamSnapshotDone() {
	n.ss.notifySnapshotStatus(false, false, true, false, 0)
	n.engine.setTaskReady(n.clusterID)
}

func (n *node) saveSnapshotDone() {
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.engine.setTaskReady(n.clusterID)
}

func (n *node) recoverFromSnapshotDone(index uint64) {
	if !n.initialized() {
		n.initialSnapshotDone(index)
	} else {
		n.doRecoverFromSnapshotDone()
	}
}

func (n *node) initialSnapshotDone(index uint64) {
	n.ss.notifySnapshotStatus(false, true, false, true, index)
	n.engine.setTaskReady(n.clusterID)
}

func (n *node) doRecoverFromSnapshotDone() {
	n.ss.notifySnapshotStatus(false, true, false, false, 0)
	n.engine.setTaskReady(n.clusterID)
}

func (n *node) handleTask(ts []rsm.Task, es []sm.Entry) (rsm.Task, error) {
	return n.sm.Handle(ts, es)
}

func (n *node) removeSnapshotFlagFile(index uint64) error {
	return n.snapshotter.removeFlagFile(index)
}

func (n *node) runSyncTask() (bool, error) {
	if !n.sm.OnDiskStateMachine() {
		return true, nil
	}
	if n.syncTask == nil ||
		!n.syncTask.timeToRun(n.millisecondSinceStart()) {
		return true, nil
	}
	plog.Infof("%s is running sync task", n.id())
	if !n.sm.TaskChanBusy() {
		task := rsm.Task{PeriodicSync: true}
		if !n.pushTask(task) {
			return false, nil
		}
	}
	syncedIndex := n.sm.GetSyncedIndex()
	if err := n.shrinkSnapshots(syncedIndex); err != nil {
		return false, err
	}
	return true, nil
}

func (n *node) shrinkSnapshots(index uint64) error {
	if n.snapshotLock.TryLock() {
		defer n.snapshotLock.Unlock()
		if !n.sm.OnDiskStateMachine() {
			panic("trying to shrink snapshots on non all disk SMs")
		}
		plog.Infof("%s will shrink snapshots up to %d", n.id(), index)
		if err := n.snapshotter.Shrink(index); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) compactSnapshots(index uint64) error {
	if n.snapshotLock.TryLock() {
		defer n.snapshotLock.Unlock()
		if err := n.snapshotter.Compact(index); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) compactLog() error {
	if n.ss.hasCompactLogTo() {
		compactTo := n.ss.getCompactLogTo()
		if compactTo == 0 {
			panic("racy compact log to value?")
		}
		if err := n.logreader.Compact(compactTo); err != nil {
			if err != raft.ErrCompacted {
				return err
			}
		}
		if err := n.logdb.RemoveEntriesTo(n.clusterID,
			n.nodeID, compactTo); err != nil {
			return err
		}
		plog.Infof("%s compacted log up to index %d", n.id(), compactTo)
	}
	return nil
}

func isFreeOrderMessage(m pb.Message) bool {
	return m.Type == pb.Replicate || m.Type == pb.Ping
}

func (n *node) sendEnterQuiesceMessages() {
	nodes, _, _, _ := n.sm.GetMembership()
	for nodeID := range nodes {
		if nodeID != n.nodeID {
			msg := pb.Message{
				Type:      pb.Quiesce,
				From:      n.nodeID,
				To:        nodeID,
				ClusterId: n.clusterID,
			}
			n.sendRaftMessage(msg)
		}
	}
}

func (n *node) sendMessages(msgs []pb.Message) {
	for _, msg := range msgs {
		if !isFreeOrderMessage(msg) {
			msg.ClusterId = n.clusterID
			n.sendRaftMessage(msg)
		}
	}
}

func (n *node) sendReplicateMessages(ud pb.Update) {
	msgs := ud.Messages
	for _, msg := range msgs {
		if isFreeOrderMessage(msg) {
			msg.ClusterId = n.clusterID
			n.sendRaftMessage(msg)
		}
	}
}

func (n *node) getUpdate() (pb.Update, bool) {
	moreEntriesToApply := n.canHaveMoreEntriesToApply()
	if n.p.HasUpdate(moreEntriesToApply) ||
		n.confirmedIndex != n.smAppliedIndex {
		if n.smAppliedIndex < n.confirmedIndex {
			plog.Panicf("last applied value moving backwards, %d, now %d",
				n.confirmedIndex, n.smAppliedIndex)
		}
		ud := n.p.GetUpdate(moreEntriesToApply, n.smAppliedIndex)
		for idx := range ud.Messages {
			ud.Messages[idx].ClusterId = n.clusterID
		}
		n.confirmedIndex = n.smAppliedIndex
		return ud, true
	}
	return pb.Update{}, false
}

func (n *node) processDroppedReadIndexes(ud pb.Update) {
	for _, sysctx := range ud.DroppedReadIndexes {
		n.pendingReadIndexes.dropped(sysctx)
	}
}

func (n *node) processDroppedEntries(ud pb.Update) {
	for _, e := range ud.DroppedEntries {
		if e.Type == pb.ApplicationEntry {
			n.pendingProposals.dropped(e.ClientID, e.SeriesID, e.Key)
		} else if e.Type == pb.ConfigChangeEntry {
			n.pendingConfigChange.dropped(e.Key)
		} else {
			panic("unknown dropped entry type")
		}
	}
}

func (n *node) processReadyToRead(ud pb.Update) {
	if len(ud.ReadyToReads) > 0 {
		n.pendingReadIndexes.addReadyToRead(ud.ReadyToReads)
		n.pendingReadIndexes.applied(ud.LastApplied)
	}
}

func (n *node) processSnapshot(ud pb.Update) (bool, error) {
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		if n.stopped() {
			return false, nil
		}
		err := n.logreader.ApplySnapshot(ud.Snapshot)
		if err != nil && !isSoftSnapshotError(err) {
			return false, err
		}
		plog.Infof("%s, snapshot %d ready to be pushed", n.id(),
			ud.Snapshot.Index)
		if !n.pushSnapshot(ud.Snapshot, ud.LastApplied) {
			return false, nil
		}
	}
	return true, nil
}

func (n *node) applyRaftUpdates(ud pb.Update) bool {
	toApply := n.entriesToApply(ud.CommittedEntries)
	if ok := n.pushEntries(toApply); !ok {
		return false
	}
	return true
}

func (n *node) processRaftUpdate(ud pb.Update) (bool, error) {
	if err := n.logreader.Append(ud.EntriesToSave); err != nil {
		return false, err
	}
	n.sendMessages(ud.Messages)
	if err := n.compactLog(); err != nil {
		return false, err
	}
	cont, err := n.runSyncTask()
	if err != nil {
		return false, err
	}
	if !cont {
		return false, nil
	}
	if n.saveSnapshotRequired(ud.LastApplied) {
		return n.pushTakeSnapshotRequest(rsm.SnapshotRequest{}), nil
	}
	return true, nil
}

func (n *node) commitRaftUpdate(ud pb.Update) {
	n.raftMu.Lock()
	n.p.Commit(ud)
	n.raftMu.Unlock()
}

func (n *node) canHaveMoreEntriesToApply() bool {
	return n.taskQ.MoreEntryToApply()
}

func (n *node) hasEntryToApply() bool {
	return n.p.HasEntryToApply()
}

func (n *node) updateBatchedLastApplied() uint64 {
	n.smAppliedIndex = n.sm.GetBatchedLastApplied()
	n.p.NotifyRaftLastApplied(n.smAppliedIndex)
	return n.smAppliedIndex
}

func (n *node) stepNode() (pb.Update, bool) {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	if n.initialized() {
		if n.handleEvents() {
			if n.newQuiesceState() {
				n.sendEnterQuiesceMessages()
			}
			return n.getUpdate()
		}
	}
	return pb.Update{}, false
}

func (n *node) handleEvents() bool {
	hasEvent := false
	lastApplied := n.updateBatchedLastApplied()
	if lastApplied != n.confirmedIndex {
		hasEvent = true
	}
	if n.hasEntryToApply() {
		hasEvent = true
	}
	if n.handleReadIndexRequests() {
		hasEvent = true
	}
	if n.handleReceivedMessages() {
		hasEvent = true
	}
	if n.handleConfigChangeMessage() {
		hasEvent = true
	}
	if n.handleProposals() {
		hasEvent = true
	}
	if n.handleLeaderTransferRequest() {
		hasEvent = true
	}
	if n.handleSnapshotRequest(lastApplied) {
		hasEvent = true
	}
	if hasEvent {
		if n.expireNotified != n.tickCount {
			n.pendingProposals.gc()
			n.pendingConfigChange.gc()
			n.pendingSnapshot.gc()
			n.expireNotified = n.tickCount
		}
		n.pendingReadIndexes.applied(lastApplied)
	}
	return hasEvent
}

func (n *node) handleLeaderTransferRequest() bool {
	target, ok := n.pendingLeaderTransfer.get()
	if ok {
		n.p.RequestLeaderTransfer(target)
	}
	return ok
}

func (n *node) handleSnapshotRequest(lastApplied uint64) bool {
	var req rsm.SnapshotRequest
	select {
	case req = <-n.snapshotC:
	default:
		return false
	}
	si := n.ss.getReqSnapshotIndex()
	plog.Infof("req: %+v", req)
	if !req.IsExportedSnapshot() && lastApplied == si {
		n.reportIgnoredSnapshotRequest(req.Key)
		return false
	}
	n.ss.setReqSnapshotIndex(lastApplied)
	n.pushTakeSnapshotRequest(req)
	return true
}

func (n *node) handleProposals() bool {
	rateLimited := n.p.RateLimited()
	if n.rateLimited != rateLimited {
		n.rateLimited = rateLimited
		plog.Infof("%s new rate limit state is %t", n.id(), rateLimited)
	}
	entries := n.incomingProposals.get(n.rateLimited)
	if len(entries) > 0 {
		n.p.ProposeEntries(entries)
		return true
	}
	return false
}

func (n *node) handleReadIndexRequests() bool {
	reqs := n.incomingReadIndexes.get()
	if len(reqs) > 0 {
		n.recordActivity(pb.ReadIndex)
		ctx := n.pendingReadIndexes.peepNextCtx()
		n.pendingReadIndexes.addPendingRead(ctx, reqs)
		n.increaseReadReqCount()
		return true
	}
	return false
}

func (n *node) handleConfigChangeMessage() bool {
	if len(n.confChangeC) == 0 {
		return false
	}
	select {
	case req, ok := <-n.confChangeC:
		if !ok {
			n.confChangeC = nil
		} else {
			n.recordActivity(pb.ConfigChangeEvent)
			var cc pb.ConfigChange
			if err := cc.Unmarshal(req.data); err != nil {
				panic(err)
			}
			n.p.ProposeConfigChange(cc, req.key)
		}
	case <-n.stopc:
		return false
	default:
		return false
	}
	return true
}

func (n *node) isBusySnapshotting() bool {
	snapshotting := n.ss.recoveringFromSnapshot()
	if !n.concurrentSnapshot() {
		snapshotting = snapshotting || n.ss.takingSnapshot()
	}
	return snapshotting && n.sm.TaskChanBusy()
}

func (n *node) handleLocalTickMessage(count uint64) {
	if count > n.config.ElectionRTT {
		count = n.config.ElectionRTT
	}
	for i := uint64(0); i < count; i++ {
		n.tick()
	}
}

func (n *node) tryRecordNodeActivity(m pb.Message) {
	if (m.Type == pb.Heartbeat ||
		m.Type == pb.HeartbeatResp) &&
		m.Hint > 0 {
		n.recordActivity(pb.ReadIndex)
	} else {
		n.recordActivity(m.Type)
	}
}

func (n *node) handleReceivedMessages() bool {
	hasEvent := false
	ltCount := uint64(0)
	scCount := n.getReadReqCount()
	busy := n.isBusySnapshotting()
	msgs := n.mq.Get()
	for _, m := range msgs {
		hasEvent = true
		if m.Type == pb.LocalTick {
			ltCount++
			continue
		}
		if m.Type == pb.Replicate && busy {
			continue
		}
		if done := n.handleMessage(m); !done {
			if m.ClusterId != n.clusterID {
				plog.Panicf("received message for cluster %d on %d",
					m.ClusterId, n.clusterID)
			}
			n.tryRecordNodeActivity(m)
			n.p.Handle(m)
		}
	}
	if scCount > 0 {
		n.batchedReadIndex()
	}
	if lazyFreeCycle > 0 {
		for i := range msgs {
			msgs[i].Entries = nil
		}
	}
	n.handleLocalTickMessage(ltCount)
	return hasEvent
}

func (n *node) handleMessage(m pb.Message) bool {
	switch m.Type {
	case pb.Quiesce:
		n.tryEnterQuiesce()
	case pb.SnapshotStatus:
		plog.Debugf("%s got ReportSnapshot from %d, rejected %t",
			n.id(), m.From, m.Reject)
		n.p.ReportSnapshotStatus(m.From, m.Reject)
	case pb.Unreachable:
		n.p.ReportUnreachableNode(m.From)
	default:
		return false
	}
	return true
}

func (n *node) setInitialStatus(index uint64) {
	if n.initialized() {
		panic("setInitialStatus called twice")
	}
	plog.Infof("%s initial index set to %d", n.id(), index)
	n.ss.setSnapshotIndex(index)
	n.pushedIndex = index
	n.setInitialized()
}

func (n *node) handleSnapshotTask(task rsm.Task) {
	if n.ss.recoveringFromSnapshot() {
		plog.Panicf("recovering from snapshot again")
	}
	if task.SnapshotAvailable {
		plog.Infof("check incoming snapshot, %s", n.id())
		n.reportAvailableSnapshot(task)
	} else if task.SnapshotRequested {
		plog.Infof("reportRequestedSnapshot, %s", n.id())
		if n.ss.takingSnapshot() {
			plog.Infof("task.SnapshotRequested ignored on %s", n.id())
			n.reportIgnoredSnapshotRequest(task.SnapshotRequest.Key)
			return
		}
		n.reportRequestedSnapshot(task)
	} else if task.StreamSnapshot {
		if !n.canStreamSnapshot() {
			n.reportSnapshotStatus(task.ClusterID, task.NodeID, true)
			return
		}
		n.reportStreamSnapshot(task)
	} else {
		panic("unknown returned task rec type")
	}
}

func (n *node) reportSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	n.handleSnapshotStatus(clusterID, nodeID, failed)
}

func (n *node) reportStreamSnapshot(rec rsm.Task) {
	n.ss.setStreamingSnapshot()
	getSinkFn := func() pb.IChunkSink {
		conn := n.getStreamConnection(rec.ClusterID, rec.NodeID)
		if conn == nil {
			plog.Errorf("failed to get connection to %s",
				logutil.DescribeNode(rec.ClusterID, rec.NodeID))
			return nil
		}
		return conn
	}
	n.ss.setStreamSnapshotReq(rec, getSinkFn)
	n.engine.setStreamReady(n.clusterID)
}

func (n *node) canStreamSnapshot() bool {
	if n.ss.streamingSnapshot() {
		plog.Infof("task.StreamSnapshot ignored on %s", n.id())
		return false
	}
	if !n.sm.ReadyToStreamSnapshot() {
		plog.Infof("not ready to stream snapshot %s", n.id())
		return false
	}
	return true
}

func (n *node) reportRequestedSnapshot(rec rsm.Task) {
	n.ss.setTakingSnapshot()
	n.ss.setSaveSnapshotReq(rec)
	n.engine.setRequestedSnapshotReady(n.clusterID)
}

func (n *node) reportAvailableSnapshot(rec rsm.Task) {
	n.ss.setRecoveringFromSnapshot()
	n.ss.setRecoverFromSnapshotReq(rec)
	n.engine.setAvailableSnapshotReady(n.clusterID)
}

func (n *node) processSnapshotStatusTransition() bool {
	if n.processTakeSnapshotStatus() {
		return true
	}
	if n.processStreamSnapshotStatus() {
		return true
	}
	if n.processRecoverSnapshotStatus() {
		return true
	}
	if task, ok := n.getUninitializedNodeTask(); ok {
		n.ss.setRecoveringFromSnapshot()
		n.reportAvailableSnapshot(task)
		return true
	}
	return false
}

func (n *node) getUninitializedNodeTask() (rsm.Task, bool) {
	if !n.initialized() {
		plog.Infof("check initial snapshot, %s", n.id())
		return rsm.Task{
			SnapshotAvailable: true,
			InitialSnapshot:   true,
			NewNode:           n.new,
		}, true
	}
	return rsm.Task{}, false
}

func (n *node) processRecoverSnapshotStatus() bool {
	if n.ss.recoveringFromSnapshot() {
		rec, ok := n.ss.getRecoverCompleted()
		if !ok {
			return true
		}
		plog.Infof("%s got completed snapshot rec (1) %+v", n.id(), rec)
		if rec.SnapshotRequested {
			panic("got a completed.SnapshotRequested")
		}
		if rec.InitialSnapshot {
			plog.Infof("%s handled initial snapshot, %d", n.id(), rec.Index)
			n.setInitialStatus(rec.Index)
		}
		n.ss.clearRecoveringFromSnapshot()
	}
	return false
}

func (n *node) processTakeSnapshotStatus() bool {
	if n.ss.takingSnapshot() {
		rec, ok := n.ss.getSaveSnapshotCompleted()
		if !ok {
			return !n.concurrentSnapshot()
		}
		plog.Infof("%s got completed snapshot rec (2) %+v", n.id(), rec)
		if rec.SnapshotRequested && !n.initialized() {
			plog.Panicf("%s taking snapshot when uninitialized", n.id())
		}
		n.ss.clearTakingSnapshot()
	}
	return false
}

func (n *node) processStreamSnapshotStatus() bool {
	if n.ss.streamingSnapshot() {
		if !n.OnDiskStateMachine() {
			panic("non-on disk sm is streaming snapshot")
		}
		rec, ok := n.ss.getStreamSnapshotCompleted()
		if !ok {
			return false
		}
		plog.Infof("%s got completed streaming rec (3) %+v", n.id(), rec)
		n.ss.clearStreamingSnapshot()
	}
	return false
}

func (n *node) batchedReadIndex() {
	ctx := n.pendingReadIndexes.nextCtx()
	n.p.ReadIndex(ctx)
}

func (n *node) tick() {
	if n.p == nil {
		panic("rc node is still nil")
	}
	n.tickCount++
	n.increaseQuiesceTick()
	if n.quiesced() {
		n.p.QuiescedTick()
	} else {
		n.p.Tick()
	}
	n.pendingSnapshot.tick()
	n.pendingProposals.tick()
	n.pendingReadIndexes.tick()
	n.pendingConfigChange.tick()
}

func (n *node) captureClusterState() {
	nodes, observers, _, index := n.sm.GetMembership()
	if len(nodes) == 0 {
		plog.Panicf("empty nodes %s", n.id())
	}
	_, isObserver := observers[n.nodeID]
	plog.Infof("%s called captureClusterState, nodes %v, observers %v",
		n.id(), nodes, observers)
	ci := &ClusterInfo{
		ClusterID:         n.clusterID,
		NodeID:            n.nodeID,
		IsLeader:          n.isLeader(),
		IsObserver:        isObserver,
		ConfigChangeIndex: index,
		Nodes:             nodes,
	}
	n.clusterInfo.Store(ci)
}

func (n *node) getStateMachineType() sm.Type {
	if n.smType == pb.RegularStateMachine {
		return sm.RegularStateMachine
	} else if n.smType == pb.ConcurrentStateMachine {
		return sm.ConcurrentStateMachine
	} else if n.smType == pb.OnDiskStateMachine {
		return sm.OnDiskStateMachine
	}
	panic("unknown type")
}

func (n *node) getClusterInfo() *ClusterInfo {
	v := n.clusterInfo.Load()
	if v == nil {
		return &ClusterInfo{
			ClusterID:        n.clusterID,
			NodeID:           n.nodeID,
			Pending:          true,
			StateMachineType: n.getStateMachineType(),
		}
	}
	ci := v.(*ClusterInfo)
	return &ClusterInfo{
		ClusterID:         ci.ClusterID,
		NodeID:            ci.NodeID,
		IsLeader:          n.isLeader(),
		IsObserver:        ci.IsObserver,
		ConfigChangeIndex: ci.ConfigChangeIndex,
		Nodes:             ci.Nodes,
		StateMachineType:  n.getStateMachineType(),
	}
}

func (n *node) id() string {
	return logutil.DescribeNode(n.clusterID, n.nodeID)
}

func (n *node) isLeader() bool {
	if n.p != nil {
		leaderID, _ := n.getLeaderID()
		return n.nodeID == leaderID
	}
	return false
}

func (n *node) isFollower() bool {
	if n.p != nil {
		leaderID, _ := n.getLeaderID()
		if leaderID != n.nodeID && leaderID != raft.NoLeader {
			return true
		}
	}
	return false
}

func (n *node) increaseReadReqCount() {
	atomic.AddUint64(&n.readReqCount, 1)
}

func (n *node) getReadReqCount() uint64 {
	return atomic.SwapUint64(&n.readReqCount, 0)
}

func (n *node) initialized() bool {
	n.initializedMu.Lock()
	defer n.initializedMu.Unlock()
	return n.initializedMu.initialized
}

func (n *node) setInitialized() {
	n.initializedMu.Lock()
	defer n.initializedMu.Unlock()
	n.initializedMu.initialized = true
}

func (n *node) millisecondSinceStart() uint64 {
	return n.tickMillisecond * n.tickCount
}
