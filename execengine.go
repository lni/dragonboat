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
	"time"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

var (
	workerCount         = settings.Hard.StepEngineWorkerCount
	commitWorkerCount   = settings.Soft.StepEngineCommitWorkerCount
	snapshotWorkerCount = settings.Soft.StepEngineSnapshotWorkerCount
	nodeReloadInterval  = time.Millisecond * time.Duration(settings.Soft.NodeReloadMillisecond)
	commitBatchSize     = settings.Soft.CommitBatchSize
)

type nodeLoader interface {
	getClusterSetIndex() uint64
	forEachClusterRun(bf func() bool,
		af func() bool, f func(uint64, *node) bool)
}

type workReady struct {
	partitioner  server.IPartitioner
	count        uint64
	readyMapList []*readyCluster
	readyChList  []chan uint64
}

func newWorkReady(count uint64) *workReady {
	wr := &workReady{
		partitioner:  server.NewFixedPartitioner(count),
		count:        count,
		readyMapList: make([]*readyCluster, count),
		readyChList:  make([]chan uint64, count),
	}
	for i := uint64(0); i < count; i++ {
		wr.readyChList[i] = make(chan uint64, 1)
		wr.readyMapList[i] = newReadyCluster()
	}
	return wr
}

func (wr *workReady) getPartitioner() server.IPartitioner {
	return wr.partitioner
}

func (wr *workReady) clusterReady(clusterID uint64) {
	idx := wr.partitioner.GetPartitionID(clusterID)
	readyMap := wr.readyMapList[idx]
	readyMap.setClusterReady(clusterID)
	select {
	case wr.readyChList[idx] <- clusterID:
	default:
	}
}

func (wr *workReady) waitCh(workerID uint64) chan uint64 {
	return wr.readyChList[workerID-1]
}

func (wr *workReady) getReadyMap(workerID uint64) map[uint64]struct{} {
	readyMap := wr.readyMapList[workerID-1]
	return readyMap.getReadyClusters()
}

type sendLocalMessageFunc func(clusterID uint64, nodeID uint64)

type execEngine struct {
	nodeStopper                *syncutil.Stopper
	commitStopper              *syncutil.Stopper
	snapshotStopper            *syncutil.Stopper
	nh                         nodeLoader
	ctx                        *server.Context
	logdb                      raftio.ILogDB
	ctxs                       []raftio.IContext
	profilers                  []*profiler
	nodeWorkReady              *workReady
	commitWorkReady            *workReady
	snapshotWorkReady          *workReady
	requestedSnapshotWorkReady *workReady
	streamSnapshotWorkReady    *workReady
	sendLocalMsg               sendLocalMessageFunc
}

func newExecEngine(nh nodeLoader, ctx *server.Context,
	logdb raftio.ILogDB, sendLocalMsg sendLocalMessageFunc) *execEngine {
	s := &execEngine{
		nh:                         nh,
		ctx:                        ctx,
		logdb:                      logdb,
		nodeStopper:                syncutil.NewStopper(),
		commitStopper:              syncutil.NewStopper(),
		snapshotStopper:            syncutil.NewStopper(),
		nodeWorkReady:              newWorkReady(workerCount),
		commitWorkReady:            newWorkReady(commitWorkerCount),
		snapshotWorkReady:          newWorkReady(snapshotWorkerCount),
		requestedSnapshotWorkReady: newWorkReady(snapshotWorkerCount),
		streamSnapshotWorkReady:    newWorkReady(snapshotWorkerCount),
		ctxs:                       make([]raftio.IContext, workerCount),
		profilers:                  make([]*profiler, workerCount),
		sendLocalMsg:               sendLocalMsg,
	}
	sampleRatio := int64(delaySampleRatio / 10)
	for i := uint64(1); i <= workerCount; i++ {
		workerID := i
		s.ctxs[i-1] = logdb.GetLogDBThreadContext()
		s.profilers[i-1] = newProfiler(sampleRatio)
		s.nodeStopper.RunWorker(func() {
			s.nodeWorkerMain(workerID)
		})
	}
	for i := uint64(1); i <= commitWorkerCount; i++ {
		commitWorkerID := i
		s.commitStopper.RunWorker(func() {
			s.commitWorkerMain(commitWorkerID)
		})
	}
	for i := uint64(1); i <= snapshotWorkerCount; i++ {
		snapshotWorkerID := i
		s.snapshotStopper.RunWorker(func() {
			s.snapshotWorkerMain(snapshotWorkerID)
		})
	}
	return s
}

func (s *execEngine) stop() {
	s.nodeStopper.Stop()
	s.commitStopper.Stop()
	s.snapshotStopper.Stop()
	for _, ctx := range s.ctxs {
		if ctx != nil {
			ctx.Destroy()
		}
	}
	s.logProfileStats()
}

func (s *execEngine) logProfileStats() {
	for _, p := range s.profilers {
		if p.ratio == 0 {
			continue
		}
		plog.Infof("prop %d,%dμs step %d,%dμs save %d,%dμs ec %d,%d cs %d,%dμs exec %d,%dμs %d",
			p.propose.median(), p.propose.p999(),
			p.step.median(), p.step.p999(),
			p.save.median(), p.save.p999(),
			p.ec.median(), p.ec.p999(),
			p.cs.median(), p.cs.p999(),
			p.exec.median(), p.exec.p999(),
			p.sampleCount)
	}
}

func (s *execEngine) snapshotWorkerClosed(nodes map[uint64]*node) bool {
	select {
	case <-s.snapshotStopper.ShouldStop():
		s.offloadNodeMap(nodes, rsm.FromSnapshotWorker)
		return true
	default:
	}
	return false
}

func (s *execEngine) snapshotWorkerMain(workerID uint64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nodes := make(map[uint64]*node)
	cci := uint64(0)
	for {
		select {
		case <-s.snapshotStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromSnapshotWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadSnapshotNodes(workerID, cci, nodes)
			for _, node := range nodes {
				s.recoverFromSnapshot(node.clusterID, nodes)
				s.saveSnapshot(node.clusterID, nodes)
				if s.snapshotWorkerClosed(nodes) {
					return
				}
			}
		case <-s.snapshotWorkReady.waitCh(workerID):
			clusterIDMap := s.snapshotWorkReady.getReadyMap(workerID)
			for clusterID := range clusterIDMap {
				nodes, cci = s.loadSnapshotNodes(workerID, cci, nodes)
				s.recoverFromSnapshot(clusterID, nodes)
				if s.snapshotWorkerClosed(nodes) {
					return
				}
			}
		case <-s.requestedSnapshotWorkReady.waitCh(workerID):
			clusterIDMap := s.requestedSnapshotWorkReady.getReadyMap(workerID)
			for clusterID := range clusterIDMap {
				nodes, cci = s.loadSnapshotNodes(workerID, cci, nodes)
				s.saveSnapshot(clusterID, nodes)
				if s.snapshotWorkerClosed(nodes) {
					return
				}
			}
		case <-s.streamSnapshotWorkReady.waitCh(workerID):
			clusterIDMap := s.streamSnapshotWorkReady.getReadyMap(workerID)
			for clusterID := range clusterIDMap {
				nodes, cci = s.loadSnapshotNodes(workerID, cci, nodes)
				s.streamSnapshot(clusterID, nodes)
				if s.snapshotWorkerClosed(nodes) {
					return
				}
			}
		}
	}
}

func (s *execEngine) loadSnapshotNodes(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.loadBucketNodes(workerID, cci, nodes,
		s.snapshotWorkReady.getPartitioner(), rsm.FromSnapshotWorker)
}

func (s *execEngine) saveSnapshot(clusterID uint64, nodes map[uint64]*node) {
	node, ok := nodes[clusterID]
	if !ok {
		return
	}
	rec, ok := node.ss.getSaveSnapshotReq()
	if !ok {
		return
	}
	plog.Infof("%s called saveSnapshot", node.describe())
	node.saveSnapshot(rec)
	node.saveSnapshotDone()
}

func (s *execEngine) streamSnapshot(clusterID uint64, nodes map[uint64]*node) {
	node, ok := nodes[clusterID]
	if !ok {
		return
	}
	_, getSinkFn, ok := node.ss.getStreamSnapshotReq()
	if !ok {
		return
	}
	sink := getSinkFn()
	if sink != nil {
		plog.Infof("%s called streamSnapshot to node %d",
			node.describe(), sink.ToNodeID())
		node.streamSnapshot(sink)
	}
	node.streamSnapshotDone()
}

func (s *execEngine) recoverFromSnapshot(clusterID uint64,
	nodes map[uint64]*node) {
	node, ok := nodes[clusterID]
	if !ok {
		return
	}
	commitRec, ok := node.ss.getRecoverFromSnapshotReq()
	if !ok {
		return
	}
	plog.Infof("%s called recoverFromSnapshot", node.describe())
	index, stopped := node.recoverFromSnapshot(commitRec)
	if stopped {
		// keep the paused for snapshot flag to make sure it won't be touched
		// by commit worker
		return
	}
	if !node.initialized() {
		node.initialSnapshotDone(index)
	} else {
		node.recoverFromSnapshotDone()
	}
}

func (s *execEngine) commitWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	batch := make([]rsm.Task, 0, commitBatchSize)
	entries := make([]sm.Entry, 0, commitBatchSize)
	cci := uint64(0)
	for {
		select {
		case <-s.commitStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromCommitWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadSMs(workerID, cci, nodes)
			s.execSMs(workerID, make(map[uint64]struct{}), nodes, batch, entries)
			batch = make([]rsm.Task, 0, commitBatchSize)
			entries = make([]sm.Entry, 0, commitBatchSize)
		case <-s.commitWorkReady.waitCh(workerID):
			clusterIDMap := s.commitWorkReady.getReadyMap(workerID)
			s.execSMs(workerID, clusterIDMap, nodes, batch, entries)
		}
	}
}

func (s *execEngine) loadSMs(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.loadBucketNodes(workerID, cci, nodes,
		s.commitWorkReady.getPartitioner(), rsm.FromCommitWorker)
}

func processUninitializedNode(node *node) *rsm.Task {
	if !node.initialized() {
		plog.Infof("check initial snapshot, %s", node.describe())
		node.ss.setRecoveringFromSnapshot()
		return &rsm.Task{
			SnapshotAvailable: true,
			InitialSnapshot:   true,
		}
	}
	return nil
}

// Returns a boolean flag indicating whether this node should be skipped for
// further execSMs processing.
func processRecoveringNode(node *node) bool {
	if node.ss.recoveringFromSnapshot() {
		completed, ok := node.ss.getRecoverCompleted()
		if !ok {
			return true
		}
		plog.Infof("%s received completed snapshot rec (1) %+v",
			node.describe(), completed)
		if completed.SnapshotRequested {
			panic("got a completed.SnapshotRequested")
		}
		if completed.InitialSnapshot {
			plog.Infof("%s handled initial snapshot, index %d",
				node.describe(), completed.Index)
			node.setInitialStatus(completed.Index)
		}
		node.ss.clearRecoveringFromSnapshot()
	}
	return false
}

func processTakingSnapshotNode(node *node) bool {
	if node.ss.takingSnapshot() {
		completed, ok := node.ss.getSaveSnapshotCompleted()
		if !ok {
			return !node.concurrentSnapshot()
		}
		plog.Infof("%s received completed snapshot rec (2) %+v",
			node.describe(), completed)
		if completed.SnapshotRequested && !node.initialized() {
			plog.Panicf("%s taking a snapshot on uninitialized node",
				node.describe())
		}
		node.ss.clearTakingSnapshot()
	}
	return false
}

func processStreamingSnapshotNode(node *node) bool {
	if node.ss.streamingSnapshot() {
		completed, ok := node.ss.getStreamSnapshotCompleted()
		if !ok {
			return false
		}
		plog.Infof("%s received completed streaming snapshot rec %+v",
			node.describe(), completed)
		node.ss.clearStreamingSnapshot()
	}
	return false
}

// T: take snapshot
// R: recover from snapshot
// existing op, new op, action
// T, T, ignore the new op
// T, R, R is queued as node state, will be handled when T is done
// R, R, won't happen, when in R state, execSMs will not process the node
// R, T, won't happen, when in R state, execSMs will not process the node

func (s *execEngine) execSMs(workerID uint64,
	idmap map[uint64]struct{},
	nodes map[uint64]*node, batch []rsm.Task, entries []sm.Entry) {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	var p *profiler
	if workerCount == commitWorkerCount {
		p = s.profilers[workerID-1]
		p.newCommitIteration()
		p.exec.start()
	}
	for clusterID := range idmap {
		node, ok := nodes[clusterID]
		if !ok || node.stopped() {
			continue
		}
		if processTakingSnapshotNode(node) {
			continue
		}
		if processStreamingSnapshotNode(node) {
			continue
		}
		if processRecoveringNode(node) {
			continue
		}
		if rec := processUninitializedNode(node); rec != nil {
			s.reportAvailableSnapshot(node, *rec)
			continue
		}
		commit, snapshotRequired := node.handleTask(batch, entries)
		// batched last applied might updated, give the node work a chance to run
		s.setNodeReady(node.clusterID)
		if snapshotRequired {
			if node.ss.recoveringFromSnapshot() {
				plog.Panicf("recovering from snapshot again")
			}
			if commit.SnapshotAvailable {
				plog.Infof("check incoming snapshot, %s", node.describe())
				node.ss.setRecoveringFromSnapshot()
				s.reportAvailableSnapshot(node, commit)
			} else if commit.SnapshotRequested {
				plog.Infof("reportRequestedSnapshot, %s", node.describe())
				if !node.ss.takingSnapshot() {
					node.ss.setTakingSnapshot()
				} else {
					plog.Infof("commit.SnapshotRequested ignored on %s", node.describe())
					node.reportIgnoredSnapshotRequest(commit.SnapshotRequest.Key)
					continue
				}
				s.reportRequestedSnapshot(node, commit)
			} else if commit.StreamSnapshot {
				if !node.ss.streamingSnapshot() {
					node.ss.setStreamingSnapshot()
				} else {
					plog.Infof("commit.StreamSnapshot ignored on %s", node.describe())
					s.reportSnapshotStatus(commit.ClusterID, commit.NodeID, true)
					continue
				}
				s.reportStreamSnapshot(node, commit)
			} else {
				panic("unknown returned commit rec type")
			}
		}
	}
	if p != nil {
		p.exec.end()
	}
}

func (s *execEngine) reportSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	nh, ok := s.nh.(*NodeHost)
	if !ok {
		panic("failed to get nh")
	}
	nh.msgHandler.HandleSnapshotStatus(clusterID, nodeID, failed)
}

func (s *execEngine) reportStreamSnapshot(node *node, rec rsm.Task) {
	nh, ok := s.nh.(*NodeHost)
	if !ok {
		panic("failed to get nh")
	}
	getSinkFn := func() pb.IChunkSink {
		conn := nh.transport.GetStreamConnection(rec.ClusterID, rec.NodeID)
		if conn == nil {
			plog.Errorf("failed to get connection to %s",
				logutil.DescribeNode(rec.ClusterID, rec.NodeID))
			return nil
		}
		return conn
	}
	node.ss.setStreamSnapshotReq(rec, getSinkFn)
	s.streamSnapshotWorkReady.clusterReady(node.clusterID)
}

func (s *execEngine) reportRequestedSnapshot(node *node, rec rsm.Task) {
	node.ss.setSaveSnapshotReq(rec)
	s.requestedSnapshotWorkReady.clusterReady(node.clusterID)
}

func (s *execEngine) reportAvailableSnapshot(node *node, rec rsm.Task) {
	node.ss.setRecoverFromSnapshotReq(rec)
	s.snapshotWorkReady.clusterReady(node.clusterID)
}

func (s *execEngine) nodeWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	cci := uint64(0)
	stopC := s.nodeStopper.ShouldStop()
	for {
		select {
		case <-stopC:
			s.offloadNodeMap(nodes, rsm.FromStepWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadNodes(workerID, cci, nodes)
			s.execNodes(workerID, make(map[uint64]struct{}), nodes, stopC)
		case <-s.nodeWorkReady.waitCh(workerID):
			clusterIDMap := s.nodeWorkReady.getReadyMap(workerID)
			s.execNodes(workerID, clusterIDMap, nodes, stopC)
		}
	}
}

func (s *execEngine) loadNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.loadBucketNodes(workerID, cci, nodes,
		s.nodeWorkReady.getPartitioner(), rsm.FromStepWorker)
}

func (s *execEngine) loadBucketNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node, partitioner server.IPartitioner,
	from rsm.From) (map[uint64]*node, uint64) {
	bucket := workerID - 1
	newCCI := s.nh.getClusterSetIndex()
	if newCCI != cci {
		newNodes := make(map[uint64]*node)
		s.nh.forEachClusterRun(nil,
			func() bool {
				for cid, node := range nodes {
					_, ok := newNodes[cid]
					if !ok || node.stopped() {
						node.notifyOffloaded(from)
					}
				}
				return true
			},
			func(cid uint64, v *node) bool {
				if partitioner.GetPartitionID(cid) == bucket {
					v.notifyLoaded(from)
					newNodes[cid] = v
				}
				return true
			})
		return newNodes, newCCI
	}
	return nodes, cci
}

func (s *execEngine) execNodes(workerID uint64,
	clusterIDMap map[uint64]struct{},
	nodes map[uint64]*node, stopC chan struct{}) {
	if len(nodes) == 0 {
		return
	}
	if readyToReturnTestKnob(stopC, "") {
		return
	}
	nodeCtx := s.ctxs[workerID-1]
	nodeCtx.Reset()
	p := s.profilers[workerID-1]
	p.newIteration()
	p.step.start()
	if len(clusterIDMap) == 0 {
		for cid := range nodes {
			clusterIDMap[cid] = struct{}{}
		}
	}
	hasSnapshot := false
	nodeUpdates := nodeCtx.GetUpdates()
	for cid := range clusterIDMap {
		node, ok := nodes[cid]
		if !ok {
			continue
		}
		ud, hasUpdate := node.stepNode()
		if hasUpdate {
			if !pb.IsEmptySnapshot(ud.Snapshot) {
				hasSnapshot = true
			}
			nodeUpdates = append(nodeUpdates, ud)
		}
	}
	if !hasSnapshot {
		s.applySnapshotAndUpdate(nodeUpdates, nodes)
	}
	if readyToReturnTestKnob(stopC, "sending append msg") {
		return
	}
	// see raft thesis section 10.2.1 on details why we send Relicate message
	// before those entries are persisted to disk
	for _, ud := range nodeUpdates {
		node := nodes[ud.ClusterID]
		node.sendReplicateMessages(ud)
		node.processReadyToRead(ud)
	}
	p.step.end()
	p.recordEntryCount(nodeUpdates)
	if readyToReturnTestKnob(stopC, "saving raft state") {
		return
	}
	p.save.start()
	if err := s.logdb.SaveRaftState(nodeUpdates, nodeCtx); err != nil {
		panic(err)
	}
	p.save.end()
	if readyToReturnTestKnob(stopC, "saving snapshots") {
		return
	}
	if hasSnapshot {
		if err := s.onSnapshotSaved(nodeUpdates, nodes); err != nil {
			panic(err)
		}
		if readyToReturnTestKnob(stopC, "applying updates") {
			return
		}
		s.applySnapshotAndUpdate(nodeUpdates, nodes)
	}
	if readyToReturnTestKnob(stopC, "processing raft updates") {
		return
	}
	p.cs.start()
	for _, ud := range nodeUpdates {
		node := nodes[ud.ClusterID]
		if !node.processRaftUpdate(ud) {
			plog.Infof("process update failed, %s is ready to exit",
				node.describe())
		}
		s.processRaftUpdate(ud)
		if readyToReturnTestKnob(stopC, "committing updates") {
			return
		}
		node.commitRaftUpdate(ud)
	}
	p.cs.end()
	if lazyFreeCycle > 0 {
		resetNodeUpdate(nodeUpdates)
	}
}

func resetNodeUpdate(nodeUpdates []pb.Update) {
	for i := range nodeUpdates {
		nodeUpdates[i].EntriesToSave = nil
		nodeUpdates[i].CommittedEntries = nil
		for j := range nodeUpdates[i].Messages {
			nodeUpdates[i].Messages[j].Entries = nil
		}
	}
}

func (s *execEngine) processRaftUpdate(ud pb.Update) {
	if ud.MoreCommittedEntries {
		s.sendLocalMsg(ud.ClusterID, ud.NodeID)
	}
}

func (s *execEngine) applySnapshotAndUpdate(updates []pb.Update,
	nodes map[uint64]*node) {
	for _, ud := range updates {
		node := nodes[ud.ClusterID]
		if !node.processSnapshot(ud) || !node.applyRaftUpdates(ud) {
			plog.Infof("raft update and snapshot not published, %s stopped",
				node.describe())
		}
	}
}

func (s *execEngine) onSnapshotSaved(updates []pb.Update,
	nodes map[uint64]*node) error {
	for _, ud := range updates {
		node := nodes[ud.ClusterID]
		if !pb.IsEmptySnapshot(ud.Snapshot) {
			if err := node.removeSnapshotFlagFile(ud.Snapshot.Index); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *execEngine) setNodeReady(clusterID uint64) {
	s.nodeWorkReady.clusterReady(clusterID)
}

func (s *execEngine) ProposeDelay(clusterID uint64, startTime time.Time) {
	p := s.nodeWorkReady.getPartitioner()
	idx := p.GetPartitionID(clusterID)
	profiler := s.profilers[idx]
	profiler.propose.record(startTime)
}

func (s *execEngine) SetCommitReady(clusterID uint64) {
	s.commitWorkReady.clusterReady(clusterID)
}

func (s *execEngine) offloadNodeMap(nodes map[uint64]*node,
	from rsm.From) {
	for _, node := range nodes {
		node.notifyOffloaded(from)
	}
}
