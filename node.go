// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/transport"
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

type pipeline interface {
	setCloseReady(*node)
	setStepReady(clusterID uint64)
	setCommitReady(clusterID uint64)
	setApplyReady(clusterID uint64)
	setStreamReady(clusterID uint64)
	setSaveReady(clusterID uint64)
	setRecoverReady(clusterID uint64)
}

type logDBMetrics struct {
	busy int32
}

func (l *logDBMetrics) update(busy bool) {
	v := int32(0)
	if busy {
		v = int32(1)
	}
	atomic.StoreInt32(&l.busy, v)
}

func (l *logDBMetrics) isBusy() bool {
	return atomic.LoadInt32(&l.busy) != 0
}

type node struct {
	clusterID             uint64
	nodeID                uint64
	leaderID              uint64
	instanceID            uint64
	raftAddress           string
	config                config.Config
	configChangeC         <-chan configChangeRequest
	snapshotC             <-chan rsm.SSRequest
	toApplyQ              *rsm.TaskQueue
	toCommitQ             *rsm.TaskQueue
	mq                    *server.MessageQueue
	appliedIndex          uint64
	confirmedIndex        uint64
	pushedIndex           uint64
	pipeline              pipeline
	getStreamSink         func(uint64, uint64) *transport.Sink
	handleSnapshotStatus  func(uint64, uint64, bool)
	sendRaftMessage       func(pb.Message)
	validateTarget        func(string) bool
	sm                    *rsm.StateMachine
	incomingProposals     *entryQueue
	incomingReadIndexes   *readIndexQueue
	pendingProposals      *pendingProposal
	pendingReadIndexes    *pendingReadIndex
	pendingConfigChange   *pendingConfigChange
	pendingSnapshot       *pendingSnapshot
	pendingLeaderTransfer *pendingLeaderTransfer
	raftMu                sync.Mutex
	p                     *raft.Peer
	logReader             *logdb.LogReader
	logdb                 raftio.ILogDB
	snapshotter           *snapshotter
	nodeRegistry          transport.INodeRegistry
	stopC                 chan struct{}
	clusterInfo           atomic.Value
	currentTick           uint64
	gcTick                uint64
	tickMillisecond       uint64
	syncTask              *task
	notifyCommit          bool
	rateLimited           bool
	logDBLimited          bool
	new                   bool
	closeOnce             sync.Once
	ss                    *snapshotState
	qs                    *quiesceState
	snapshotLock          *syncutil.Lock
	raftEvents            *raftEventListener
	sysEvents             *sysEventListener
	metrics               *logDBMetrics
	initializedC          chan struct{}
	initializedFlag       uint64
}

var _ rsm.INode = (*node)(nil)

var instanceID uint64

func newNode(peers map[uint64]string,
	initialMember bool,
	config config.Config,
	nhConfig config.NodeHostConfig,
	createSM rsm.ManagedStateMachineFactory,
	snapshotter *snapshotter,
	pipeline pipeline,
	liQueue *leaderInfoQueue,
	getStreamSink func(uint64, uint64) *transport.Sink,
	handleSnapshotStatus func(uint64, uint64, bool),
	sendMessage func(pb.Message),
	nodeRegistry transport.INodeRegistry,
	pool *sync.Pool,
	ldb raftio.ILogDB,
	metrics *logDBMetrics,
	sysEvents *sysEventListener) (*node, error) {
	notifyCommit := nhConfig.NotifyCommit
	proposals := newEntryQueue(incomingProposalsMaxLen, lazyFreeCycle)
	readIndexes := newReadIndexQueue(incomingReadIndexMaxLen)
	configChangeC := make(chan configChangeRequest, 1)
	snapshotC := make(chan rsm.SSRequest, 1)
	stopC := make(chan struct{})
	mq := server.NewMessageQueue(receiveQueueLen,
		false, lazyFreeCycle, nhConfig.MaxReceiveQueueSize)
	rn := &node{
		clusterID:             config.ClusterID,
		nodeID:                config.NodeID,
		raftAddress:           nhConfig.RaftAddress,
		instanceID:            atomic.AddUint64(&instanceID, 1),
		tickMillisecond:       nhConfig.RTTMillisecond,
		config:                config,
		incomingProposals:     proposals,
		incomingReadIndexes:   readIndexes,
		configChangeC:         configChangeC,
		snapshotC:             snapshotC,
		pipeline:              pipeline,
		getStreamSink:         getStreamSink,
		handleSnapshotStatus:  handleSnapshotStatus,
		stopC:                 stopC,
		pendingProposals:      newPendingProposal(config, notifyCommit, pool, proposals),
		pendingReadIndexes:    newPendingReadIndex(pool, readIndexes),
		pendingConfigChange:   newPendingConfigChange(configChangeC, notifyCommit),
		pendingSnapshot:       newPendingSnapshot(snapshotC),
		pendingLeaderTransfer: newPendingLeaderTransfer(),
		nodeRegistry:          nodeRegistry,
		snapshotter:           snapshotter,
		logReader:             logdb.NewLogReader(config.ClusterID, config.NodeID, ldb),
		sendRaftMessage:       sendMessage,
		mq:                    mq,
		logdb:                 ldb,
		snapshotLock:          syncutil.NewLock(),
		syncTask:              newTask(syncTaskInterval),
		sysEvents:             sysEvents,
		notifyCommit:          notifyCommit,
		metrics:               metrics,
		initializedC:          make(chan struct{}),
		ss:                    &snapshotState{},
		validateTarget:        nhConfig.GetTargetValidator(),
		qs: &quiesceState{
			electionTick: config.ElectionRTT * 2,
			enabled:      config.Quiesce,
			clusterID:    config.ClusterID,
			nodeID:       config.NodeID,
		},
	}
	ds := createSM(config.ClusterID, config.NodeID, stopC)
	sm := rsm.NewStateMachine(ds, snapshotter, config, rn, snapshotter.fs)
	if notifyCommit {
		rn.toCommitQ = rsm.NewTaskQueue()
	}
	rn.toApplyQ = sm.TaskQ()
	rn.sm = sm
	rn.raftEvents = newRaftEventListener(config.ClusterID,
		config.NodeID, &rn.leaderID, nhConfig.EnableMetrics, liQueue)
	new, err := rn.startRaft(config, peers, initialMember)
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
	return n.stopC
}

func (n *node) StepReady() {
	n.pipeline.setStepReady(n.clusterID)
}

func (n *node) applyReady() {
	n.pipeline.setApplyReady(n.clusterID)
}

func (n *node) commitReady() {
	n.pipeline.setCommitReady(n.clusterID)
}

func (n *node) ApplyUpdate(e pb.Entry,
	result sm.Result, rejected bool, ignored bool, notifyRead bool) {
	if n.isWitness() {
		return
	}
	if notifyRead {
		n.pendingReadIndexes.applied(e.Index)
	}
	if !ignored {
		if e.Key == 0 {
			plog.Panicf("key is 0")
		}
		n.pendingProposals.applied(e.ClientID, e.SeriesID, e.Key, result, rejected)
	}
}

func (n *node) ApplyConfigChange(cc pb.ConfigChange,
	key uint64, rejected bool) {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	if !rejected {
		n.applyConfigChange(cc)
	}
	n.configChangeProcessed(key, rejected)
}

func (n *node) applyConfigChange(cc pb.ConfigChange) {
	n.p.ApplyConfigChange(cc)
	switch cc.Type {
	case pb.AddNode, pb.AddObserver, pb.AddWitness:
		n.nodeRegistry.Add(n.clusterID, cc.NodeID, cc.Address)
	case pb.RemoveNode:
		if cc.NodeID == n.nodeID {
			plog.Infof("%s applied ConfChange Remove for itself", n.id())
			n.nodeRegistry.RemoveCluster(n.clusterID)
			n.requestRemoval()
		} else {
			n.nodeRegistry.Remove(n.clusterID, cc.NodeID)
		}
	default:
		plog.Panicf("unknown config change type, %s", cc.Type)
	}
}

func (n *node) configChangeProcessed(key uint64, rejected bool) {
	if n.isWitness() {
		return
	}
	if rejected {
		n.p.RejectConfigChange()
	} else {
		n.notifyConfigChange()
	}
	n.pendingConfigChange.apply(key, rejected)
}

func (n *node) RestoreRemotes(snapshot pb.Snapshot) {
	if snapshot.Membership.ConfigChangeId == 0 {
		plog.Panicf("invalid ConfChangeId")
	}
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	for nid, addr := range snapshot.Membership.Addresses {
		n.nodeRegistry.Add(n.clusterID, nid, addr)
	}
	for nid, addr := range snapshot.Membership.Observers {
		n.nodeRegistry.Add(n.clusterID, nid, addr)
	}
	for nid, addr := range snapshot.Membership.Witnesses {
		n.nodeRegistry.Add(n.clusterID, nid, addr)
	}
	for nid := range snapshot.Membership.Removed {
		if nid == n.nodeID {
			n.nodeRegistry.RemoveCluster(n.clusterID)
			n.requestRemoval()
		}
	}
	plog.Debugf("%s is restoring remotes", n.id())
	n.p.RestoreRemotes(snapshot)
	n.notifyConfigChange()
}

func (n *node) startRaft(cfg config.Config,
	peers map[uint64]string, initial bool) (bool, error) {
	newNode, err := n.replayLog(cfg.ClusterID, cfg.NodeID)
	if err != nil {
		return false, err
	}
	pas := make([]raft.PeerAddress, 0)
	for k, v := range peers {
		pas = append(pas, raft.PeerAddress{NodeID: k, Address: v})
	}
	n.p = raft.Launch(cfg, n.logReader, n.raftEvents, pas, initial, newNode)
	return newNode, nil
}

func (n *node) close() {
	n.requestRemoval()
	n.raftEvents.stop()
	n.mq.Close()
	n.pendingReadIndexes.close()
	n.pendingProposals.close()
	n.pendingConfigChange.close()
	n.pendingSnapshot.close()
}

func (n *node) stopped() bool {
	select {
	case <-n.stopC:
		return true
	default:
	}
	return false
}

func (n *node) requestRemoval() {
	n.closeOnce.Do(func() {
		close(n.stopC)
	})
	plog.Debugf("%s called requestRemoval()", n.id())
}

func (n *node) concurrentSnapshot() bool {
	return n.sm.Concurrent()
}

func (n *node) supportClientSession() bool {
	return !n.OnDiskStateMachine()
}

func (n *node) isWitness() bool {
	return n.config.IsWitness
}

func (n *node) OnDiskStateMachine() bool {
	return n.sm.OnDiskStateMachine()
}

func (n *node) proposeSession(session *client.Session,
	timeout uint64) (*RequestState, error) {
	if !n.initialized() {
		return nil, ErrClusterNotReady
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	if !session.ValidForSessionOp(n.clusterID) {
		return nil, ErrInvalidSession
	}
	return n.pendingProposals.propose(session, nil, timeout)
}

func (n *node) payloadTooBig(sz int) bool {
	if n.config.MaxInMemLogSize == 0 {
		return false
	}
	return uint64(sz+settings.EntryNonCmdFieldsSize) > n.config.MaxInMemLogSize
}

func (n *node) propose(session *client.Session,
	cmd []byte, timeout uint64) (*RequestState, error) {
	if !n.initialized() {
		return nil, ErrClusterNotReady
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	if !session.ValidForProposal(n.clusterID) {
		return nil, ErrInvalidSession
	}
	if n.payloadTooBig(len(cmd)) {
		return nil, ErrPayloadTooBig
	}
	return n.pendingProposals.propose(session, cmd, timeout)
}

func (n *node) read(timeout uint64) (*RequestState, error) {
	if !n.initialized() {
		return nil, ErrClusterNotReady
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	rs, err := n.pendingReadIndexes.read(timeout)
	if err == nil {
		rs.node = n
	}
	return rs, err
}

func (n *node) requestLeaderTransfer(nodeID uint64) error {
	if !n.initialized() {
		return ErrClusterNotReady
	}
	if n.isWitness() {
		return ErrInvalidOperation
	}
	return n.pendingLeaderTransfer.request(nodeID)
}

func (n *node) requestSnapshot(opt SnapshotOption,
	timeout uint64) (*RequestState, error) {
	if !n.initialized() {
		return nil, ErrClusterNotReady
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	st := rsm.UserRequested
	if opt.Exported {
		plog.Debugf("%s called export snapshot", n.id())
		st = rsm.Exported
		exist, err := fileutil.Exist(opt.ExportPath, n.snapshotter.fs)
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
	return n.pendingSnapshot.request(st,
		opt.ExportPath,
		opt.OverrideCompactionOverhead,
		opt.CompactionOverhead,
		timeout)
}

func (n *node) reportIgnoredSnapshotRequest(key uint64) {
	n.pendingSnapshot.apply(key, true, false, 0)
}

func (n *node) requestConfigChange(cct pb.ConfigChangeType,
	nodeID uint64, target string, orderID uint64,
	timeout uint64) (*RequestState, error) {
	if !n.initialized() {
		return nil, ErrClusterNotReady
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	if cct != pb.RemoveNode && !n.validateTarget(target) {
		return nil, ErrInvalidAddress
	}
	cc := pb.ConfigChange{
		Type:           cct,
		NodeID:         nodeID,
		ConfigChangeId: orderID,
		Address:        target,
	}
	return n.pendingConfigChange.request(cc, timeout)
}

func (n *node) requestDeleteNodeWithOrderID(nodeID uint64,
	order uint64, timeout uint64) (*RequestState, error) {
	return n.requestConfigChange(pb.RemoveNode, nodeID, "", order, timeout)
}

func (n *node) requestAddNodeWithOrderID(nodeID uint64,
	target string, order uint64, timeout uint64) (*RequestState, error) {
	return n.requestConfigChange(pb.AddNode, nodeID, target, order, timeout)
}

func (n *node) requestAddObserverWithOrderID(nodeID uint64,
	target string, order uint64, timeout uint64) (*RequestState, error) {
	return n.requestConfigChange(pb.AddObserver, nodeID, target, order, timeout)
}

func (n *node) requestAddWitnessWithOrderID(nodeID uint64,
	target string, order uint64, timeout uint64) (*RequestState, error) {
	return n.requestConfigChange(pb.AddWitness, nodeID, target, order, timeout)
}

func (n *node) getLeaderID() (uint64, bool) {
	v := atomic.LoadUint64(&n.leaderID)
	return v, v != raft.NoLeader
}

func (n *node) destroy() {
	n.sm.Close()
}

func (n *node) offloaded() {
	if n.sm.Offloaded() {
		n.pipeline.setCloseReady(n)
		n.sysEvents.Publish(server.SystemEvent{
			Type:      server.NodeUnloaded,
			ClusterID: n.clusterID,
			NodeID:    n.nodeID,
		})
	}
}

func (n *node) loaded() {
	n.sm.Loaded()
}

func (n *node) entriesToApply(ents []pb.Entry) []pb.Entry {
	if len(ents) == 0 {
		return nil
	}
	if ents[len(ents)-1].Index < n.pushedIndex {
		plog.Panicf("%s got entries [%d-%d] older than current state %d",
			n.id(), ents[0].Index, ents[len(ents)-1].Index, n.pushedIndex)
	}
	firstIndex := ents[0].Index
	if firstIndex > n.pushedIndex+1 {
		plog.Panicf("%s has hole in to be applied logs, found: %d, want: %d",
			n.id(), firstIndex, n.pushedIndex+1)
	}
	if n.pushedIndex-firstIndex+1 < uint64(len(ents)) {
		return ents[n.pushedIndex-firstIndex+1:]
	}
	return nil
}

func (n *node) pushTask(rec rsm.Task, notify bool) {
	if n.notifyCommit {
		n.toCommitQ.Add(rec)
		if notify {
			n.commitReady()
		}
	} else {
		n.toApplyQ.Add(rec)
		if notify {
			n.applyReady()
		}
	}
}

func (n *node) pushEntries(ents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	n.pushTask(rsm.Task{Entries: ents}, false)
	n.pushedIndex = ents[len(ents)-1].Index
}

func (n *node) pushStreamSnapshotRequest(clusterID uint64, nodeID uint64) {
	n.pushTask(rsm.Task{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Stream:    true,
	}, true)
}

func (n *node) pushTakeSnapshotRequest(req rsm.SSRequest) {
	n.pushTask(rsm.Task{
		Save:      true,
		SSRequest: req,
	}, true)
}

func (n *node) pushSnapshot(snapshot pb.Snapshot, applied uint64) {
	if pb.IsEmptySnapshot(snapshot) {
		return
	}
	if snapshot.Index < n.pushedIndex ||
		snapshot.Index < n.ss.getIndex() ||
		snapshot.Index < applied {
		plog.Panicf("out of date snapshot, index %d, pushed %d, applied %d, ss %d",
			snapshot.Index, n.pushedIndex, applied, n.ss.getIndex())
	}
	n.pushTask(rsm.Task{
		Recover: true,
		Index:   snapshot.Index,
	}, true)
	n.ss.setIndex(snapshot.Index)
	n.pushedIndex = snapshot.Index
}

func (n *node) replayLog(clusterID uint64, nodeID uint64) (bool, error) {
	plog.Infof("%s replaying raft logs", n.id())
	snapshot, err := n.snapshotter.GetMostRecentSnapshot()
	if err != nil && err != ErrNoSnapshot {
		return false, err
	}
	if snapshot.Index > 0 {
		if err = n.logReader.ApplySnapshot(snapshot); err != nil {
			plog.Errorf("failed to apply snapshot, %v", err)
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
	hasRaftState := !reflect.DeepEqual(rs.State, pb.State{})
	if hasRaftState {
		plog.Infof("%s has logdb entries size %d commit %d term %d",
			n.id(), rs.EntryCount, rs.State.Commit, rs.State.Term)
		n.logReader.SetState(rs.State)
	}
	n.logReader.SetRange(rs.FirstIndex, rs.EntryCount)
	return !(snapshot.Index > 0 || rs.EntryCount > 0 || hasRaftState), nil
}

func (n *node) saveSnapshotRequired(applied uint64) bool {
	if n.config.SnapshotEntries == 0 {
		return false
	}
	index := n.ss.getIndex()
	if n.pushedIndex <= n.config.SnapshotEntries+index ||
		applied <= n.config.SnapshotEntries+index ||
		applied <= n.config.SnapshotEntries+n.ss.getReqIndex() {
		return false
	}
	if n.isBusySnapshotting() {
		return false
	}
	plog.Debugf("%s requested to create %s", n.id(), n.ssid(applied))
	n.ss.setReqIndex(applied)
	return true
}

func isSoftSnapshotError(err error) bool {
	return err == raft.ErrCompacted || err == raft.ErrSnapshotOutOfDate
}

func saveAborted(err error) bool {
	return err == sm.ErrSnapshotStopped || err == sm.ErrSnapshotAborted
}

func snapshotCommitAborted(err error) bool {
	return err == errSnapshotOutOfDate
}

func streamAborted(err error) bool {
	return saveAborted(err) || err == sm.ErrSnapshotStreaming
}

func openAborted(err error) bool {
	return err == sm.ErrOpenStopped
}

func recoverAborted(err error) bool {
	return err == sm.ErrSnapshotStopped || err == raft.ErrSnapshotOutOfDate
}

func (n *node) save(rec rsm.Task) error {
	index, err := n.doSave(rec.SSRequest)
	if err != nil {
		return err
	}
	n.pendingSnapshot.apply(rec.SSRequest.Key, index == 0, false, index)
	n.sysEvents.Publish(server.SystemEvent{
		Type:      server.SnapshotCreated,
		ClusterID: n.clusterID,
		NodeID:    n.nodeID,
	})
	return nil
}

func (n *node) doSave(req rsm.SSRequest) (uint64, error) {
	n.snapshotLock.Lock()
	defer n.snapshotLock.Unlock()
	if !req.Exported() && n.sm.GetLastApplied() <= n.ss.getIndex() {
		// a snapshot has been pushed to the sm but not applied yet
		// or the snapshot has been applied and there is no further progress
		return 0, nil
	}
	ss, ssenv, err := n.sm.Save(req)
	if err != nil {
		if saveAborted(err) {
			plog.Warningf("%s save snapshot aborted, %v", n.id(), err)
			ssenv.MustRemoveTempDir()
			n.pendingSnapshot.apply(req.Key, false, true, 0)
			return 0, nil
		} else if isSoftSnapshotError(err) {
			// e.g. trying to save a snapshot at the same index twice
			return 0, nil
		}
		plog.Errorf("%s save snapshot failed %v", n.id(), err)
		return 0, err
	}
	plog.Infof("%s saved snapshot, index %s, term %d, file count %d",
		n.id(), n.ssid(ss.Index), ss.Term, len(ss.Files))
	if err := n.snapshotter.commit(ss, req); err != nil {
		plog.Errorf("%s commit snapshot failed %v", n.id(), err)
		if snapshotCommitAborted(err) || saveAborted(err) {
			// saveAborted() will only be true in monkey test
			// commit abort happens when the final dir already exists, probably due to
			// incoming snapshot
			ssenv.MustRemoveTempDir()
			return 0, nil
		}
		return 0, err
	}
	if req.Exported() {
		return ss.Index, nil
	}
	if !ss.Validate(n.snapshotter.fs) {
		plog.Panicf("%s generated invalid snapshot %v", n.id(), ss)
	}
	if err = n.logReader.CreateSnapshot(ss); err != nil {
		plog.Errorf("%s create snapshot record failed %v", n.id(), err)
		if isSoftSnapshotError(err) {
			return 0, nil
		}
		return 0, err
	}
	if err := n.compact(req, ss.Index); err != nil {
		return 0, err
	}
	n.ss.setIndex(ss.Index)
	return ss.Index, nil
}

func (n *node) compact(req rsm.SSRequest, index uint64) error {
	if overhead := n.compactionOverhead(req); index > overhead {
		n.ss.setCompactLogTo(index - overhead)
	}
	return n.compactSnapshots(index)
}

func (n *node) compactionOverhead(req rsm.SSRequest) uint64 {
	if req.OverrideCompaction {
		return req.CompactionOverhead
	}
	return n.config.CompactionOverhead
}

func (n *node) stream(sink pb.IChunkSink) error {
	if sink != nil {
		plog.Infof("%s requested to stream to %d", n.id(), sink.ToNodeID())
		if err := n.sm.Stream(sink); err != nil {
			if !streamAborted(err) {
				plog.Errorf("%s failed to stream, %v", n.id(), err)
				return err
			}
		}
	}
	return nil
}

func (n *node) recover(rec rsm.Task) (uint64, error) {
	n.snapshotLock.Lock()
	defer n.snapshotLock.Unlock()
	if rec.Initial && n.OnDiskStateMachine() {
		plog.Debugf("%s on disk SM is beng initialized", n.id())
		idx, err := n.sm.OpenOnDiskStateMachine()
		if err != nil {
			if openAborted(err) {
				plog.Warningf("%s aborted OpenOnDiskStateMachine", n.id())
				return 0, nil
			}
			plog.Errorf("%s failed to OpenOnDiskStateMachine, %v", n.id(), err)
			return 0, err
		}
		if idx > 0 && rec.NewNode {
			plog.Panicf("%s new node at non-zero index %d", n.id(), idx)
		}
	}
	index, err := n.sm.Recover(rec)
	if err != nil {
		plog.Errorf("%s failed to recover, %v", n.id(), err)
		if recoverAborted(err) {
			plog.Warningf("%s aborted recovery", n.id())
			return 0, nil
		}
		return 0, err
	}
	if index > 0 {
		plog.Infof("%s recovered from %s", n.id(), n.ssid(index))
		if n.OnDiskStateMachine() {
			if err := n.sm.Sync(); err != nil {
				plog.Errorf("%s failed to sync, %v", n.id(), err)
				return 0, err
			}
			if err := n.snapshotter.shrink(index); err != nil {
				plog.Errorf("%s failed to shrink, %v", n.id(), err)
				return 0, err
			}
		}
		if err := n.compactSnapshots(index); err != nil {
			plog.Errorf("%s failed to compact snapshots, %v", n.id(), err)
			return 0, err
		}
	}
	n.sysEvents.Publish(server.SystemEvent{
		Type:      server.SnapshotRecovered,
		ClusterID: n.clusterID,
		NodeID:    n.nodeID,
		Index:     index,
	})
	return index, nil
}

func (n *node) streamDone() {
	n.ss.notifySnapshotStatus(false, false, true, false, 0)
	n.applyReady()
}

func (n *node) saveDone() {
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.applyReady()
}

func (n *node) recoverDone(index uint64) {
	if !n.initialized() {
		n.initialSnapshotDone(index)
	} else {
		n.recoverFromSnapshotDone()
	}
}

func (n *node) initialSnapshotDone(index uint64) {
	n.ss.notifySnapshotStatus(false, true, false, true, index)
	n.applyReady()
}

func (n *node) recoverFromSnapshotDone() {
	n.ss.notifySnapshotStatus(false, true, false, false, 0)
	n.applyReady()
}

func (n *node) handleTask(ts []rsm.Task, es []sm.Entry) (rsm.Task, error) {
	return n.sm.Handle(ts, es)
}

func (n *node) removeSnapshotFlagFile(index uint64) error {
	return n.snapshotter.removeFlagFile(index)
}

func (n *node) runSyncTask() error {
	if !n.sm.OnDiskStateMachine() {
		return nil
	}
	if n.syncTask == nil ||
		!n.syncTask.timeToRun(n.millisecondSinceStart()) {
		return nil
	}
	if !n.sm.TaskChanBusy() {
		n.pushTask(rsm.Task{PeriodicSync: true}, true)
	}
	return n.shrink(n.sm.GetSyncedIndex())
}

func (n *node) shrink(index uint64) error {
	if n.snapshotLock.TryLock() {
		defer n.snapshotLock.Unlock()
		if !n.sm.OnDiskStateMachine() {
			plog.Panicf("trying to shrink snapshots on non all disk SMs")
		}
		plog.Debugf("%s will shrink snapshots up to %d", n.id(), index)
		if err := n.snapshotter.shrink(index); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) compactSnapshots(index uint64) error {
	if err := n.snapshotter.compact(index); err != nil {
		return err
	}
	n.sysEvents.Publish(server.SystemEvent{
		Type:      server.SnapshotCompacted,
		ClusterID: n.clusterID,
		NodeID:    n.nodeID,
		Index:     index,
	})
	return nil
}

func (n *node) removeLog() error {
	if n.ss.hasCompactLogTo() {
		compactTo := n.ss.getCompactLogTo()
		if compactTo == 0 {
			plog.Panicf("racy compact log to value?")
		}
		if err := n.logReader.Compact(compactTo); err != nil {
			if err != raft.ErrCompacted {
				return err
			}
		}
		if err := n.logdb.RemoveEntriesTo(n.clusterID,
			n.nodeID, compactTo); err != nil {
			return err
		}
		plog.Debugf("%s compacted log up to index %d", n.id(), compactTo)
		n.ss.setCompactedTo(compactTo)
		n.sysEvents.Publish(server.SystemEvent{
			Type:      server.LogCompacted,
			ClusterID: n.clusterID,
			NodeID:    n.nodeID,
			Index:     compactTo,
		})
		if !n.config.DisableAutoCompactions {
			if _, err := n.requestCompaction(); err == nil {
				plog.Debugf("auto compaction for %s up to index %d", n.id(), compactTo)
			}
		}
	}
	return nil
}

func (n *node) requestCompaction() (*SysOpState, error) {
	if compactTo := n.ss.getCompactedTo(); compactTo > 0 {
		done, err := n.logdb.CompactEntriesTo(n.clusterID, n.nodeID, compactTo)
		if err != nil {
			return nil, err
		}
		n.sysEvents.Publish(server.SystemEvent{
			Type:      server.LogDBCompacted,
			ClusterID: n.clusterID,
			NodeID:    n.nodeID,
			Index:     compactTo,
		})
		return &SysOpState{completedC: done}, nil
	}
	return nil, ErrRejected
}

func isFreeOrderMessage(m pb.Message) bool {
	return m.Type == pb.Replicate || m.Type == pb.Ping
}

func (n *node) sendEnterQuiesceMessages() {
	for nodeID := range n.sm.GetMembership().Addresses {
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
	for _, msg := range ud.Messages {
		if isFreeOrderMessage(msg) {
			msg.ClusterId = n.clusterID
			n.sendRaftMessage(msg)
		}
	}
}

func (n *node) getUpdate() (pb.Update, bool) {
	moreEntries := n.moreEntriesToApply()
	if n.p.HasUpdate(moreEntries) ||
		n.confirmedIndex != n.appliedIndex ||
		n.ss.hasCompactLogTo() || n.ss.hasCompactedTo() {
		if n.appliedIndex < n.confirmedIndex {
			plog.Panicf("applied index moving backwards, %d, now %d",
				n.confirmedIndex, n.appliedIndex)
		}
		ud := n.p.GetUpdate(moreEntries, n.appliedIndex)
		n.confirmedIndex = n.appliedIndex
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
		if e.IsProposal() {
			n.pendingProposals.dropped(e.ClientID, e.SeriesID, e.Key)
		} else if e.Type == pb.ConfigChangeEntry {
			n.pendingConfigChange.dropped(e.Key)
		} else {
			plog.Panicf("unknown entry type %s", e.Type)
		}
	}
}

func (n *node) notifyCommittedEntries() {
	tasks := n.toCommitQ.GetAll()
	for _, t := range tasks {
		for _, e := range t.Entries {
			if e.IsProposal() {
				n.pendingProposals.committed(e.ClientID, e.SeriesID, e.Key)
			} else if e.Type == pb.ConfigChangeEntry {
				n.pendingConfigChange.committed(e.Key)
			} else {
				plog.Panicf("unknown entry type %s", e.Type)
			}
		}
		n.toApplyQ.Add(t)
	}
	if len(tasks) > 0 {
		n.applyReady()
	}
}

func (n *node) processReadyToRead(ud pb.Update) {
	if len(ud.ReadyToReads) > 0 {
		n.pendingReadIndexes.addReady(ud.ReadyToReads)
		n.pendingReadIndexes.applied(ud.LastApplied)
	}
}

func (n *node) processSnapshot(ud pb.Update) error {
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		err := n.logReader.ApplySnapshot(ud.Snapshot)
		if err != nil && !isSoftSnapshotError(err) {
			return err
		}
		plog.Debugf("%s, push snapshot %d", n.id(), ud.Snapshot.Index)
		n.pushSnapshot(ud.Snapshot, ud.LastApplied)
	}
	return nil
}

func (n *node) applyRaftUpdates(ud pb.Update) {
	n.pushEntries(n.entriesToApply(ud.CommittedEntries))
}

func (n *node) processRaftUpdate(ud pb.Update) error {
	if err := n.logReader.Append(ud.EntriesToSave); err != nil {
		return err
	}
	n.sendMessages(ud.Messages)
	if err := n.removeLog(); err != nil {
		return err
	}
	if err := n.runSyncTask(); err != nil {
		return err
	}
	if n.saveSnapshotRequired(ud.LastApplied) {
		n.pushTakeSnapshotRequest(rsm.SSRequest{})
	}
	return nil
}

func (n *node) commitRaftUpdate(ud pb.Update) {
	n.raftMu.Lock()
	n.p.Commit(ud)
	n.raftMu.Unlock()
}

func (n *node) moreEntriesToApply() bool {
	return n.toApplyQ.MoreEntryToApply()
}

func (n *node) hasEntryToApply() bool {
	return n.p.HasEntryToApply()
}

func (n *node) updateAppliedIndex() uint64 {
	n.appliedIndex = n.sm.GetLastApplied()
	n.p.NotifyRaftLastApplied(n.appliedIndex)
	return n.appliedIndex
}

func (n *node) stepNode() (pb.Update, bool) {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	if n.initialized() {
		if n.handleEvents() {
			if n.qs.newQuiesceState() {
				n.sendEnterQuiesceMessages()
			}
			return n.getUpdate()
		}
	}
	return pb.Update{}, false
}

func (n *node) handleEvents() bool {
	hasEvent := false
	lastApplied := n.updateAppliedIndex()
	if lastApplied != n.confirmedIndex {
		hasEvent = true
	}
	if n.hasEntryToApply() {
		hasEvent = true
	}
	if n.handleReadIndex() {
		hasEvent = true
	}
	if n.handleReceivedMessages() {
		hasEvent = true
	}
	if n.handleConfigChange() {
		hasEvent = true
	}
	if n.handleProposals() {
		hasEvent = true
	}
	if n.handleLeaderTransfer() {
		hasEvent = true
	}
	if n.handleSnapshot(lastApplied) {
		hasEvent = true
	}
	if n.handleCompaction() {
		hasEvent = true
	}
	n.gc()
	if hasEvent {
		n.pendingReadIndexes.applied(lastApplied)
	}
	return hasEvent
}

func (n *node) gc() {
	if n.gcTick != n.currentTick {
		n.pendingProposals.gc()
		n.pendingConfigChange.gc()
		n.pendingSnapshot.gc()
		n.gcTick = n.currentTick
	}
}

func (n *node) handleCompaction() bool {
	return n.ss.hasCompactedTo() || n.ss.hasCompactLogTo()
}

func (n *node) handleLeaderTransfer() bool {
	target, ok := n.pendingLeaderTransfer.get()
	if ok {
		n.p.RequestLeaderTransfer(target)
	}
	return ok
}

func (n *node) handleSnapshot(lastApplied uint64) bool {
	var req rsm.SSRequest
	select {
	case req = <-n.snapshotC:
	default:
		return false
	}
	if !req.Exported() && lastApplied == n.ss.getReqIndex() {
		n.reportIgnoredSnapshotRequest(req.Key)
		return false
	}
	n.ss.setReqIndex(lastApplied)
	n.pushTakeSnapshotRequest(req)
	return true
}

func (n *node) handleProposals() bool {
	rateLimited := n.p.RateLimited()
	if n.rateLimited != rateLimited {
		n.rateLimited = rateLimited
		plog.Infof("%s new rate limit state is %t", n.id(), rateLimited)
	}
	logDBBusy := n.logDBBusy()
	if n.logDBLimited != logDBBusy {
		n.logDBLimited = logDBBusy
		plog.Infof("%s new LogDB busy state is %t", n.id(), logDBBusy)
	}
	paused := logDBBusy || n.rateLimited
	if entries := n.incomingProposals.get(paused); len(entries) > 0 {
		n.p.ProposeEntries(entries)
		return true
	}
	return false
}

func (n *node) handleReadIndex() bool {
	if reqs := n.incomingReadIndexes.get(); len(reqs) > 0 {
		n.qs.record(pb.ReadIndex)
		ctx := n.pendingReadIndexes.nextCtx()
		n.pendingReadIndexes.add(ctx, reqs)
		n.p.ReadIndex(ctx)
		return true
	}
	return false
}

func (n *node) handleConfigChange() bool {
	if len(n.configChangeC) == 0 {
		return false
	}
	select {
	case req, ok := <-n.configChangeC:
		if !ok {
			n.configChangeC = nil
		} else {
			n.qs.record(pb.ConfigChangeEvent)
			var cc pb.ConfigChange
			if err := cc.Unmarshal(req.data); err != nil {
				panic(err)
			}
			n.p.ProposeConfigChange(cc, req.key)
		}
	default:
		return false
	}
	return true
}

func (n *node) isBusySnapshotting() bool {
	snapshotting := n.ss.recovering()
	if !n.concurrentSnapshot() {
		snapshotting = snapshotting || n.ss.saving()
	}
	return snapshotting && n.sm.TaskChanBusy()
}

func (n *node) recordMessage(m pb.Message) {
	if (m.Type == pb.Heartbeat || m.Type == pb.HeartbeatResp) && m.Hint > 0 {
		n.qs.record(pb.ReadIndex)
	} else {
		n.qs.record(m.Type)
	}
}

func (n *node) handleReceivedMessages() bool {
	count := uint64(0)
	busy := n.isBusySnapshotting()
	msgs := n.mq.Get()
	for _, m := range msgs {
		if m.Type == pb.LocalTick {
			count++
		} else if m.Type == pb.Replicate && busy {
			continue
		}
		if done := n.handleMessage(m); !done {
			n.recordMessage(m)
			n.p.Handle(m)
		}
	}
	if count > n.config.ElectionRTT/2 {
		plog.Warningf("%s had %d LocalTick msgs in one batch", n.id(), count)
	}
	if lazyFreeCycle > 0 {
		for i := range msgs {
			msgs[i].Entries = nil
		}
	}
	return len(msgs) > 0
}

func (n *node) handleMessage(m pb.Message) bool {
	switch m.Type {
	case pb.LocalTick:
		n.tick(m.Hint)
	case pb.Quiesce:
		n.qs.tryEnterQuiesce()
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
		plog.Panicf("setInitialStatus called twice")
	}
	plog.Infof("%s initial index set to %d", n.id(), index)
	n.ss.setIndex(index)
	n.pushedIndex = index
	n.setInitialized()
}

func (n *node) handleSnapshotTask(task rsm.Task) {
	if n.ss.recovering() {
		plog.Panicf("%s recovering from snapshot again on %s",
			n.id(), n.getRaftAddress())
	}
	if task.Recover {
		n.reportRecoverSnapshot(task)
	} else if task.Save {
		if n.ss.saving() {
			plog.Warningf("%s taking snapshot, ignored new snapshot req", n.id())
			n.reportIgnoredSnapshotRequest(task.SSRequest.Key)
			return
		}
		n.reportSaveSnapshot(task)
	} else if task.Stream {
		if !n.canStream() {
			n.reportSnapshotStatus(task.ClusterID, task.NodeID, true)
			return
		}
		n.reportStreamSnapshot(task)
	} else {
		plog.Panicf("unknown task type %+v", task)
	}
}

func (n *node) reportSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	n.handleSnapshotStatus(clusterID, nodeID, failed)
}

func (n *node) reportStreamSnapshot(rec rsm.Task) {
	n.ss.setStreaming()
	getSinkFn := func() pb.IChunkSink {
		conn := n.getStreamSink(rec.ClusterID, rec.NodeID)
		if conn == nil {
			plog.Errorf("failed to connect to %s", dn(rec.ClusterID, rec.NodeID))
			return nil
		}
		return conn
	}
	n.ss.setStreamReq(rec, getSinkFn)
	n.pipeline.setStreamReady(n.clusterID)
}

func (n *node) canStream() bool {
	if n.ss.streaming() {
		plog.Warningf("%s ignored task.StreamSnapshot", n.id())
		return false
	}
	if !n.sm.ReadyToStream() {
		plog.Warningf("%s is not ready to stream snapshot", n.id())
		return false
	}
	return true
}

func (n *node) reportSaveSnapshot(rec rsm.Task) {
	n.ss.setSaving()
	n.ss.setSaveReq(rec)
	n.pipeline.setSaveReady(n.clusterID)
}

func (n *node) reportRecoverSnapshot(rec rsm.Task) {
	n.ss.setRecovering()
	n.ss.setRecoverReq(rec)
	n.pipeline.setRecoverReady(n.clusterID)
}

// returns a boolean flag indicating whether to skip task handling for the
// current node
func (n *node) processStatusTransition() bool {
	if n.processSaveStatus() {
		return true
	}
	if n.processStreamStatus() {
		return true
	}
	if n.processRecoverStatus() {
		return true
	}
	if n.processUninitializedNodeStatus() {
		return true
	}
	return false
}

func (n *node) processUninitializedNodeStatus() bool {
	if !n.initialized() {
		plog.Debugf("%s checking initial snapshot", n.id())
		n.ss.setRecovering()
		n.reportRecoverSnapshot(rsm.Task{
			Recover: true,
			Initial: true,
			NewNode: n.new,
		})
		return true
	}
	return false
}

func (n *node) processRecoverStatus() bool {
	if n.ss.recovering() {
		rec, ok := n.ss.getRecoverCompleted()
		if !ok {
			return true
		}
		if rec.Save {
			plog.Panicf("got a completed.SnapshotRequested")
		}
		if rec.Initial {
			plog.Infof("%s initialized using %s", n.id(), n.ssid(rec.Index))
			n.setInitialStatus(rec.Index)
			n.sysEvents.Publish(server.SystemEvent{
				Type:      server.NodeReady,
				ClusterID: n.clusterID,
				NodeID:    n.nodeID,
			})
		}
		n.ss.clearRecovering()
	}
	return false
}

func (n *node) processSaveStatus() bool {
	if n.ss.saving() {
		rec, ok := n.ss.getSaveCompleted()
		if !ok {
			return !n.concurrentSnapshot()
		}
		if rec.Save && !n.initialized() {
			plog.Panicf("%s taking snapshot when uninitialized", n.id())
		}
		n.ss.clearSaving()
	}
	return false
}

func (n *node) processStreamStatus() bool {
	if n.ss.streaming() {
		if !n.OnDiskStateMachine() {
			plog.Panicf("non-on disk sm is streaming snapshot")
		}
		if _, ok := n.ss.getStreamCompleted(); !ok {
			return false
		}
		n.ss.clearStreaming()
	}
	return false
}

func (n *node) tick(tick uint64) {
	if n.p == nil {
		plog.Panicf("raft node is nil")
	}
	n.currentTick++
	n.qs.tick()
	n.mq.Tick()
	if n.qs.quiesced() {
		n.p.QuiescedTick()
	} else {
		n.p.Tick()
	}
	n.pendingSnapshot.tick(tick)
	n.pendingProposals.tick(tick)
	n.pendingReadIndexes.tick(tick)
	n.pendingConfigChange.tick(tick)
}

func (n *node) notifyConfigChange() {
	m := n.sm.GetMembership()
	if len(m.Addresses) == 0 {
		plog.Panicf("empty nodes %s", n.id())
	}
	_, isObserver := m.Observers[n.nodeID]
	_, isWitness := m.Witnesses[n.nodeID]
	ci := &ClusterInfo{
		ClusterID:         n.clusterID,
		NodeID:            n.nodeID,
		IsLeader:          n.isLeader(),
		IsObserver:        isObserver,
		IsWitness:         isWitness,
		ConfigChangeIndex: m.ConfigChangeId,
		Nodes:             m.Addresses,
	}
	n.clusterInfo.Store(ci)
	n.sysEvents.Publish(server.SystemEvent{
		Type:      server.MembershipChanged,
		ClusterID: n.clusterID,
		NodeID:    n.nodeID,
	})
}

func (n *node) getClusterInfo() *ClusterInfo {
	v := n.clusterInfo.Load()
	if v == nil {
		return &ClusterInfo{
			ClusterID:        n.clusterID,
			NodeID:           n.nodeID,
			Pending:          true,
			StateMachineType: sm.Type(n.sm.Type()),
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
		StateMachineType:  sm.Type(n.sm.Type()),
	}
}

func (n *node) logDBBusy() bool {
	if n.metrics == nil {
		// only happens in tests
		return false
	}
	return n.metrics.isBusy()
}

func (n *node) id() string {
	return dn(n.clusterID, n.nodeID)
}

func (n *node) ssid(index uint64) string {
	return logutil.DescribeSS(n.clusterID, n.nodeID, index)
}

func (n *node) isLeader() bool {
	if n.p != nil {
		leaderID, ok := n.getLeaderID()
		return ok && n.nodeID == leaderID
	}
	return false
}

func (n *node) isFollower() bool {
	if n.p != nil {
		leaderID, ok := n.getLeaderID()
		return ok && n.nodeID != leaderID
	}
	return false
}

func (n *node) initialized() bool {
	if atomic.LoadUint64(&n.initializedFlag) != 0 {
		return true
	}
	select {
	case <-n.initializedC:
		atomic.StoreUint64(&n.initializedFlag, 1)
		return true
	default:
	}
	return false
}

func (n *node) setInitialized() {
	close(n.initializedC)
}

func (n *node) millisecondSinceStart() uint64 {
	return n.tickMillisecond * n.currentTick
}

func (n *node) getRaftAddress() string {
	return n.raftAddress
}
