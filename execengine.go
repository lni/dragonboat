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
	"reflect"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/syncutil"
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

type tsn struct {
	task rsm.Task
	sink getSink
	node *node
}

type ssWorker struct {
	workerID   uint64
	stopper    *syncutil.Stopper
	requestC   chan tsn
	completedC chan struct{}
}

func newSSWorker(workerID uint64, stopper *syncutil.Stopper) *ssWorker {
	w := &ssWorker{
		workerID:   workerID,
		stopper:    stopper,
		requestC:   make(chan tsn, 1),
		completedC: make(chan struct{}, 1),
	}
	stopper.RunWorker(func() {
		w.workerMain()
	})
	return w
}

func (w *ssWorker) workerMain() {
	for {
		select {
		case <-w.stopper.ShouldStop():
			return
		case req := <-w.requestC:
			if req.node == nil {
				panic("req.node == nil")
			}
			w.handle(req)
			w.completed()
		}
	}
}

func (w *ssWorker) completed() {
	select {
	case w.completedC <- struct{}{}:
	}
}

func (w *ssWorker) handle(req tsn) {
	if !req.task.IsSnapshotTask() {
		panic("not a snapshot task")
	}
	if req.task.SnapshotAvailable {
		w.recoverFromSnapshot(req)
	} else if req.task.SnapshotRequested {
		w.saveSnapshot(req)
	} else if req.task.StreamSnapshot {
		w.streamSnapshot(req)
	} else {
		panic("unknown snapshot task type")
	}
}

func (w *ssWorker) recoverFromSnapshot(rec tsn) {
	if index, err := rec.node.recoverFromSnapshot(rec.task); err != nil {
		if err != sm.ErrOpenStopped && err != sm.ErrSnapshotStopped {
			panic(err)
		}
	} else {
		rec.node.recoverFromSnapshotDone(index)
	}
}

func (w *ssWorker) saveSnapshot(rec tsn) {
	if err := rec.node.saveSnapshot(rec.task); err != nil {
		panic(err)
	} else {
		rec.node.saveSnapshotDone()
	}
}

func (w *ssWorker) streamSnapshot(rec tsn) {
	if err := rec.node.streamSnapshot(rec.sink()); err != nil {
		panic(err)
	} else {
		rec.node.streamSnapshotDone()
	}
}

type workerPool struct {
	nh            nodeLoader
	loaded        *loadedNodes
	saveReady     *workReady
	recoverReady  *workReady
	streamReady   *workReady
	workers       []*ssWorker
	busy          map[uint64]*node    // map of workerID -> *node
	saving        map[uint64]struct{} // map of clusterID
	recovering    map[uint64]struct{} // map of clusterID
	streaming     map[uint64]uint64   // map of clusterID -> target count
	pending       []tsn
	workerStopper *syncutil.Stopper
	poolStopper   *syncutil.Stopper
}

func newWorkerPool(nh nodeLoader, loaded *loadedNodes) *workerPool {
	w := &workerPool{
		nh:            nh,
		loaded:        loaded,
		saveReady:     newWorkReady(1),
		recoverReady:  newWorkReady(1),
		streamReady:   newWorkReady(1),
		workers:       make([]*ssWorker, snapshotWorkerCount),
		busy:          make(map[uint64]*node, snapshotWorkerCount),
		saving:        make(map[uint64]struct{}, snapshotWorkerCount),
		recovering:    make(map[uint64]struct{}, snapshotWorkerCount),
		streaming:     make(map[uint64]uint64, snapshotWorkerCount),
		pending:       make([]tsn, 0),
		workerStopper: syncutil.NewStopper(),
		poolStopper:   syncutil.NewStopper(),
	}
	for workerID := uint64(0); workerID < snapshotWorkerCount; workerID++ {
		w.workers[workerID] = newSSWorker(workerID, w.workerStopper)
	}
	w.poolStopper.RunWorker(func() {
		w.workerPoolMain()
	})
	return w
}

func (p *workerPool) stop() {
	p.poolStopper.Stop()
}

func (p *workerPool) getWorker() *ssWorker {
	for _, w := range p.workers {
		if _, busy := p.busy[w.workerID]; !busy {
			return w
		}
	}
	return nil
}

func (p *workerPool) workerPoolMain() {
	// TODO:
	// convert the selects below using reflect.Select
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	nodes := make(map[uint64]*node)
	cci := uint64(0)
	busyUpdated := false
	for {
		toSchedule := false
		// 0 - pool stopper stopc
		// 1 - p.saveReady.waitCh(1)
		// 2 - p.recoverReady.waitCh(1)
		// 3 - p.streamReady.waitCh(1)
		// 4 - worker completedC
		// 4 + len(workers) - ticker.C
		cases := make([]reflect.SelectCase, len(p.workers)+5)
		cases[0] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.poolStopper.ShouldStop()),
		}
		cases[1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.saveReady.waitCh(1)),
		}
		cases[2] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.recoverReady.waitCh(1)),
		}
		cases[3] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.streamReady.waitCh(1)),
		}
		for idx, w := range p.workers {
			cases[4+idx] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.completedC),
			}
		}
		cases[4+len(p.workers)] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ticker.C),
		}
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			p.workerStopper.Stop()
			for _, node := range nodes {
				node.notifyOffloaded(rsm.FromSnapshotWorker)
			}
			return
		} else if chosen == 1 {
			cci, nodes = p.loadNodes(cci, nodes)
			clusters := p.saveReady.getReadyMap(1)
			for cid := range clusters {
				n, ok := nodes[cid]
				if ok {
					req, ok := n.ss.getSaveSnapshotReq()
					if !ok {
						continue
					}
					req.ClusterID = cid
					tr := tsn{task: req}
					p.pending = append(p.pending, tr)
					toSchedule = true
				}
			}
		} else if chosen == 2 {
			cci, nodes = p.loadNodes(cci, nodes)
			clusters := p.recoverReady.getReadyMap(1)
			for cid := range clusters {
				n, ok := nodes[cid]
				if ok {
					if n.clusterID != cid {
						plog.Panicf("n.clusterID != cid")
					}
					req, ok := n.ss.getRecoverFromSnapshotReq()
					if !ok {
						continue
					}
					req.ClusterID = cid
					tr := tsn{task: req}
					p.pending = append(p.pending, tr)
					toSchedule = true
				}
			}
		} else if chosen == 3 {
			cci, nodes = p.loadNodes(cci, nodes)
			clusters := p.streamReady.getReadyMap(1)
			for cid := range clusters {
				n, ok := nodes[cid]
				if ok {
					req, sinkFn, ok := n.ss.getStreamSnapshotReq()
					if !ok {
						continue
					}
					req.ClusterID = cid
					tr := tsn{task: req, sink: sinkFn}
					p.pending = append(p.pending, tr)
					toSchedule = true
				}
			}
		} else if chosen >= 4 && chosen <= 4+len(p.workers)-1 {
			workerID := uint64(chosen - 4)
			w := p.workers[workerID]
			if w.workerID != workerID {
				panic("w.workerID != workerID")
			}
			_, ok := p.busy[workerID]
			if !ok {
				plog.Panicf("not in busy state")
			}
			p.completed(workerID)
			busyUpdated = true
			cci, nodes = p.loadNodes(0, nodes)
			toSchedule = true
		} else if chosen == len(cases)-1 {
			if busyUpdated {
				cci, nodes = p.loadNodes(0, nodes)
				busyUpdated = false
			} else {
				cci, nodes = p.loadNodes(cci, nodes)
			}
		} else {
			plog.Panicf("chosen %d, unexpected case", chosen)
		}
		if toSchedule {
			if cci == 0 || len(nodes) == 0 {
				cci, nodes = p.loadNodes(cci, nodes)
			}
			p.schedule(nodes)
		}
	}
}

func (p *workerPool) loadNodes(cci uint64,
	nodes map[uint64]*node) (uint64, map[uint64]*node) {
	newCCI, newNodes := p.doLoadNodes(cci, nodes)
	p.loaded.update(1, rsm.FromSnapshotWorker, newNodes)
	return newCCI, newNodes
}

func (p *workerPool) doLoadNodes(cci uint64,
	nodes map[uint64]*node) (uint64, map[uint64]*node) {
	newCCI := p.nh.getClusterSetIndex()
	if newCCI != cci {
		newNodes := make(map[uint64]*node)
		for _, n := range p.busy {
			newNodes[n.clusterID] = n
		}
		p.nh.forEachClusterRun(nil,
			func() bool {
				for cid, node := range nodes {
					_, ok := newNodes[cid]
					if !ok {
						node.notifyOffloaded(rsm.FromSnapshotWorker)
					}
					// TODO:
					// check instanceID change issue
				}
				return true
			},
			func(cid uint64, v *node) bool {
				v.notifyLoaded(rsm.FromSnapshotWorker)
				_, ok := newNodes[cid]
				if !ok {
					newNodes[cid] = v
				}
				return true
			})
		return newCCI, newNodes
	}
	return cci, nodes
}

func (p *workerPool) completed(workerID uint64) {
	n, ok := p.busy[workerID]
	if !ok {
		plog.Panicf("worker %d is not busy", workerID)
	}
	delete(p.busy, workerID)
	_, ok1 := p.saving[n.clusterID]
	if ok1 {
		delete(p.saving, n.clusterID)
	}
	_, ok2 := p.recovering[n.clusterID]
	if ok2 {
		delete(p.recovering, n.clusterID)
	}
	count, ok3 := p.streaming[n.clusterID]
	if ok3 {
		if count == 0 {
			plog.Panicf("node is not streaming, but it just completed streaming")
		} else if count == 1 {
			delete(p.streaming, n.clusterID)
		} else {
			p.streaming[n.clusterID] = count - 1
		}
	}
	if !ok1 && !ok2 && !ok3 {
		plog.Panicf("not sure what got completed")
	}
}

func (p *workerPool) inProgress(clusterID uint64) bool {
	_, ok1 := p.saving[clusterID]
	_, ok2 := p.recovering[clusterID]
	_, ok3 := p.streaming[clusterID]
	return ok1 || ok2 || ok3
}

func (p *workerPool) canStream(clusterID uint64) bool {
	_, ok := p.saving[clusterID]
	if ok {
		return false
	}
	_, ok = p.recovering[clusterID]
	if ok {
		return false
	}
	return true
}

func (p *workerPool) canSave(clusterID uint64) bool {
	return !p.inProgress(clusterID)
}

func (p *workerPool) canRecover(clusterID uint64) bool {
	return !p.inProgress(clusterID)
}

func (p *workerPool) canSchedule(rec tsn) bool {
	if rec.task.SnapshotAvailable {
		v := p.canRecover(rec.task.ClusterID)
		return v
	} else if rec.task.SnapshotRequested {
		return p.canSave(rec.task.ClusterID)
	} else if rec.task.StreamSnapshot {
		return p.canStream(rec.task.ClusterID)
	}
	plog.Panicf("unknown task type %v", rec.task)
	return false
}

func (p workerPool) setBusy(node *node, workerID uint64) {
	_, ok := p.busy[workerID]
	if ok {
		plog.Panicf("trying to use a busy worker")
	}
	p.busy[workerID] = node
}

func (p *workerPool) startStreaming(node *node, workerID uint64) {
	count, ok := p.streaming[node.clusterID]
	if !ok {
		p.streaming[node.clusterID] = 1
	} else {
		p.streaming[node.clusterID] = count + 1
	}
}

func (p *workerPool) startSaving(node *node, workerID uint64) {
	_, ok := p.saving[node.clusterID]
	if ok {
		plog.Panicf("%s trying to start saving again", node.id())
	}
	p.saving[node.clusterID] = struct{}{}
}

func (p *workerPool) startRecovering(node *node, workerID uint64) {
	_, ok := p.recovering[node.clusterID]
	if ok {
		plog.Panicf("%s trying to start recovering again", node.id())
	}
	p.recovering[node.clusterID] = struct{}{}
}

func (p *workerPool) start(rec tsn, node *node, workerID uint64) {
	p.setBusy(node, workerID)
	if rec.task.SnapshotAvailable {
		p.startRecovering(node, workerID)
	} else if rec.task.SnapshotRequested {
		p.startSaving(node, workerID)
	} else if rec.task.StreamSnapshot {
		p.startStreaming(node, workerID)
	} else {
		plog.Panicf("unknown task type %v", rec.task)
	}
}

func (p *workerPool) schedule(nodes map[uint64]*node) {
	for {
		if !p.scheduleWorker(nodes) {
			return
		}
	}
}

func (p *workerPool) scheduleWorker(nodes map[uint64]*node) bool {
	if len(p.pending) == 0 {
		return false
	}
	w := p.getWorker()
	if w == nil {
		return false
	}
	for idx, ct := range p.pending {
		n, ok := nodes[ct.task.ClusterID]
		if !ok {
			p.removeFromPending(idx)
			return true
		}
		if n.clusterID != ct.task.ClusterID {
			plog.Panicf("n.clusterID != ct.task.ClusterID")
		}
		if p.canSchedule(ct) {
			p.scheduleTask(ct, n, w)
			p.removeFromPending(idx)
			return true
		}
	}
	return false
}

func (p *workerPool) removeFromPending(idx int) {
	sz := len(p.pending)
	copy(p.pending[idx:], p.pending[idx+1:])
	p.pending = p.pending[:sz-1]
	if len(p.pending) != sz-1 {
		panic("len(p.pending) != sz - 1")
	}
}

func (p *workerPool) scheduleTask(rec tsn, n *node, w *ssWorker) {
	p.start(rec, n, w.workerID)
	req := tsn{task: rec.task, sink: rec.sink, node: n}
	select {
	case w.requestC <- req:
	default:
		panic("worker busy")
	}
}

type execEngine struct {
	nodeStopper   *syncutil.Stopper
	taskStopper   *syncutil.Stopper
	nh            nodeLoader
	loaded        *loadedNodes
	ctx           *server.Context
	logdb         raftio.ILogDB
	ctxs          []raftio.IContext
	profilers     []*profiler
	nodeWorkReady *workReady
	taskWorkReady *workReady
	wp            *workerPool
}

func newExecEngine(nh nodeLoader,
	ctx *server.Context, logdb raftio.ILogDB) *execEngine {
	loaded := newLoadedNodes()
	s := &execEngine{
		nh:            nh,
		ctx:           ctx,
		logdb:         logdb,
		loaded:        loaded,
		nodeStopper:   syncutil.NewStopper(),
		taskStopper:   syncutil.NewStopper(),
		nodeWorkReady: newWorkReady(workerCount),
		taskWorkReady: newWorkReady(taskWorkerCount),
		ctxs:          make([]raftio.IContext, workerCount),
		profilers:     make([]*profiler, workerCount),
		wp:            newWorkerPool(nh, loaded),
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
	return s
}

func (s *execEngine) stop() {
	s.nodeStopper.Stop()
	s.taskStopper.Stop()
	s.wp.stop()
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
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = s.loadSMs(workerID, cci, nodes)
			}
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
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = s.loadNodes(workerID, cci, nodes)
			}
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
	nodeUpdates := nodeCtx.GetUpdates()
	for cid := range clusterIDMap {
		node, ok := nodes[cid]
		if !ok {
			continue
		}
		ud, hasUpdate := node.stepNode()
		if hasUpdate {
			nodeUpdates = append(nodeUpdates, ud)
		}
	}
	s.applySnapshotAndUpdate(nodeUpdates, nodes, true)
	if tests.ReadyToReturnTestKnob(stopC, false, "sending append msg") {
		return
	}
	// see raft thesis section 10.2.1 on details why we send Replicate message
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
	if err := s.onSnapshotSaved(nodeUpdates, nodes); err != nil {
		panic(err)
	}
	if tests.ReadyToReturnTestKnob(stopC, false, "applying updates") {
		return
	}
	s.applySnapshotAndUpdate(nodeUpdates, nodes, false)
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
	nodes map[uint64]*node, fastApply bool) {
	for _, ud := range updates {
		if ud.FastApply != fastApply {
			continue
		}
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
	s.wp.streamReady.clusterReady(clusterID)
}

func (s *execEngine) setRequestedSnapshotReady(clusterID uint64) {
	s.wp.saveReady.clusterReady(clusterID)
}

func (s *execEngine) setAvailableSnapshotReady(clusterID uint64) {
	s.wp.recoverReady.clusterReady(clusterID)
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
