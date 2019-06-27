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
	"time"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	workerCount         = settings.Hard.StepEngineWorkerCount
	taskWorkerCount     = settings.Soft.StepEngineTaskWorkerCount
	snapshotWorkerCount = settings.Soft.StepEngineSnapshotWorkerCount
	reloadTime          = settings.Soft.NodeReloadMillisecond
	nodeReloadInterval  = time.Millisecond * time.Duration(reloadTime)
	taskBatchSize       = settings.Soft.TaskBatchSize
)

type nodeLoader interface {
	getClusterSetIndex() uint64
	forEachClusterRun(bf func() bool,
		af func() bool, f func(uint64, *node) bool)
}

type nodeType struct {
	workerID uint64
	from     rsm.From
}

type loadedNodes struct {
	mu    sync.Mutex
	nodes map[nodeType]map[uint64]*node
}

func newLoadedNodes() *loadedNodes {
	return &loadedNodes{
		nodes: make(map[nodeType]map[uint64]*node),
	}
}

func (l *loadedNodes) loaded(clusterID uint64, nodeID uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.nodes {
		n, ok := m[clusterID]
		if ok && n.nodeID == nodeID {
			return true
		}
	}
	return false
}

func (l *loadedNodes) update(workerID uint64,
	from rsm.From, nodes map[uint64]*node) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nt := nodeType{workerID: workerID, from: from}
	l.nodes[nt] = nodes
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

type execEngine struct {
	nodeStopper                *syncutil.Stopper
	taskStopper                *syncutil.Stopper
	snapshotStopper            *syncutil.Stopper
	nh                         nodeLoader
	loaded                     *loadedNodes
	ctx                        *server.Context
	logdb                      raftio.ILogDB
	ctxs                       []raftio.IContext
	profilers                  []*profiler
	nodeWorkReady              *workReady
	taskWorkReady              *workReady
	snapshotWorkReady          *workReady
	requestedSnapshotWorkReady *workReady
	streamSnapshotWorkReady    *workReady
}

func newExecEngine(nh nodeLoader,
	ctx *server.Context, logdb raftio.ILogDB) *execEngine {
	s := &execEngine{
		nh:                         nh,
		ctx:                        ctx,
		logdb:                      logdb,
		loaded:                     newLoadedNodes(),
		nodeStopper:                syncutil.NewStopper(),
		taskStopper:                syncutil.NewStopper(),
		snapshotStopper:            syncutil.NewStopper(),
		nodeWorkReady:              newWorkReady(workerCount),
		taskWorkReady:              newWorkReady(taskWorkerCount),
		snapshotWorkReady:          newWorkReady(snapshotWorkerCount),
		requestedSnapshotWorkReady: newWorkReady(snapshotWorkerCount),
		streamSnapshotWorkReady:    newWorkReady(snapshotWorkerCount),
		ctxs:                       make([]raftio.IContext, workerCount),
		profilers:                  make([]*profiler, workerCount),
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
	for i := uint64(1); i <= taskWorkerCount; i++ {
		taskWorkerID := i
		s.taskStopper.RunWorker(func() {
			s.taskWorkerMain(taskWorkerID)
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
	s.taskStopper.Stop()
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

func (s *execEngine) nodeLoaded(clusterID uint64, nodeID uint64) bool {
	return s.loaded.loaded(clusterID, nodeID)
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
	result, cci := s.loadBucketNodes(workerID, cci, nodes,
		s.snapshotWorkReady.getPartitioner(), rsm.FromSnapshotWorker)
	s.loaded.update(workerID, rsm.FromSnapshotWorker, result)
	return result, cci
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
	plog.Infof("%s called saveSnapshot", node.id())
	if err := node.saveSnapshot(rec); err != nil {
		panic(err)
	} else {
		node.saveSnapshotDone()
	}
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
	if err := node.streamSnapshot(sink); err != nil {
		panic(err)
	} else {
		node.streamSnapshotDone()
	}
}

func (s *execEngine) recoverFromSnapshot(clusterID uint64,
	nodes map[uint64]*node) {
	node, ok := nodes[clusterID]
	if !ok {
		return
	}
	rec, ok := node.ss.getRecoverFromSnapshotReq()
	if !ok {
		return
	}
	plog.Infof("%s called recoverFromSnapshot", node.id())
	if index, err := node.recoverFromSnapshot(rec); err != nil {
		if err != sm.ErrOpenStopped && err != sm.ErrSnapshotStopped {
			panic(err)
		}
	} else {
		node.recoverFromSnapshotDone(index)
	}
}

func (s *execEngine) taskWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	batch := make([]rsm.Task, 0, taskBatchSize)
	entries := make([]sm.Entry, 0, taskBatchSize)
	cci := uint64(0)
	for {
		select {
		case <-s.taskStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromCommitWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadSMs(workerID, cci, nodes)
			s.execSMs(workerID, make(map[uint64]struct{}), nodes, batch, entries)
			batch = make([]rsm.Task, 0, taskBatchSize)
			entries = make([]sm.Entry, 0, taskBatchSize)
		case <-s.taskWorkReady.waitCh(workerID):
			clusterIDMap := s.taskWorkReady.getReadyMap(workerID)
			s.execSMs(workerID, clusterIDMap, nodes, batch, entries)
		}
	}
}

func (s *execEngine) loadSMs(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	result, cci := s.loadBucketNodes(workerID, cci, nodes,
		s.taskWorkReady.getPartitioner(), rsm.FromCommitWorker)
	s.loaded.update(workerID, rsm.FromCommitWorker, result)
	return result, cci
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
	if workerCount == taskWorkerCount {
		p = s.profilers[workerID-1]
		p.newCommitIteration()
		p.exec.start()
	}
	for clusterID := range idmap {
		node, ok := nodes[clusterID]
		if !ok || node.stopped() {
			continue
		}
		if node.processSnapshotStatusTransition() {
			continue
		}
		task, err := node.handleTask(batch, entries)
		if err != nil {
			panic(err)
		}
		if task.IsSnapshotTask() {
			node.handleSnapshotTask(task)
		}
	}
	if p != nil {
		p.exec.end()
	}
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
	result, cci := s.loadBucketNodes(workerID, cci, nodes,
		s.nodeWorkReady.getPartitioner(), rsm.FromStepWorker)
	s.loaded.update(workerID, rsm.FromStepWorker, result)
	return result, cci
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
					nv, ok := newNodes[cid]
					if !ok {
						node.notifyOffloaded(from)
					} else {
						if nv.instanceID != node.instanceID {
							node.notifyOffloaded(from)
						}
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
	if tests.ReadyToReturnTestKnob(stopC, false, "") {
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
	if tests.ReadyToReturnTestKnob(stopC, false, "sending append msg") {
		return
	}
	// see raft thesis section 10.2.1 on details why we send Relicate message
	// before those entries are persisted to disk
	for _, ud := range nodeUpdates {
		node := nodes[ud.ClusterID]
		node.sendReplicateMessages(ud)
		node.processReadyToRead(ud)
		node.processDroppedEntries(ud)
		node.processDroppedReadIndexes(ud)
	}
	p.step.end()
	p.recordEntryCount(nodeUpdates)
	if tests.ReadyToReturnTestKnob(stopC, false, "saving raft state") {
		return
	}
	p.save.start()
	if err := s.logdb.SaveRaftState(nodeUpdates, nodeCtx); err != nil {
		panic(err)
	}
	p.save.end()
	if tests.ReadyToReturnTestKnob(stopC, false, "saving snapshots") {
		return
	}
	if hasSnapshot {
		if err := s.onSnapshotSaved(nodeUpdates, nodes); err != nil {
			panic(err)
		}
		if tests.ReadyToReturnTestKnob(stopC, false, "applying updates") {
			return
		}
		s.applySnapshotAndUpdate(nodeUpdates, nodes)
	}
	if tests.ReadyToReturnTestKnob(stopC, false, "processing raft updates") {
		return
	}
	p.cs.start()
	for _, ud := range nodeUpdates {
		node := nodes[ud.ClusterID]
		cont, err := node.processRaftUpdate(ud)
		if err != nil {
			panic(err)
		}
		if !cont {
			plog.Infof("process update failed, %s is ready to exit", node.id())
		}
		s.processMoreCommittedEntries(ud)
		if tests.ReadyToReturnTestKnob(stopC, false, "committing updates") {
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

func (s *execEngine) processMoreCommittedEntries(ud pb.Update) {
	if ud.MoreCommittedEntries {
		s.setNodeReady(ud.ClusterID)
	}
}

func (s *execEngine) applySnapshotAndUpdate(updates []pb.Update,
	nodes map[uint64]*node) {
	for _, ud := range updates {
		node := nodes[ud.ClusterID]
		cont, err := node.processSnapshot(ud)
		if err != nil {
			panic(err)
		}
		if !cont || !node.applyRaftUpdates(ud) {
			plog.Infof("raft update and snapshot not published, %s stopped",
				node.id())
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
			if err := node.compactSnapshots(ud.Snapshot.Index); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *execEngine) setNodeReady(clusterID uint64) {
	s.nodeWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setTaskReady(clusterID uint64) {
	s.taskWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setStreamReady(clusterID uint64) {
	s.streamSnapshotWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setRequestedSnapshotReady(clusterID uint64) {
	s.requestedSnapshotWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setAvailableSnapshotReady(clusterID uint64) {
	s.snapshotWorkReady.clusterReady(clusterID)
}

func (s *execEngine) proposeDelay(clusterID uint64, startTime time.Time) {
	p := s.nodeWorkReady.getPartitioner()
	idx := p.GetPartitionID(clusterID)
	profiler := s.profilers[idx]
	profiler.propose.record(startTime)
}

func (s *execEngine) offloadNodeMap(nodes map[uint64]*node,
	from rsm.From) {
	for _, node := range nodes {
		node.notifyOffloaded(from)
	}
}
