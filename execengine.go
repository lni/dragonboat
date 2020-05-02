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
	stepWorkerCount     = settings.Hard.StepEngineWorkerCount
	commitWorkerCount   = settings.Soft.StepEngineCommitWorkerCount
	applyWorkerCount    = settings.Soft.StepEngineTaskWorkerCount
	snapshotWorkerCount = settings.Soft.StepEngineSnapshotWorkerCount
	reloadTime          = settings.Soft.NodeReloadMillisecond
	nodeReloadInterval  = time.Millisecond * time.Duration(reloadTime)
	taskBatchSize       = settings.Soft.TaskBatchSize
)

type nodeLoader interface {
	id() string
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
	return l.get(clusterID, nodeID) != nil
}

func (l *loadedNodes) get(clusterID uint64, nodeID uint64) *node {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.nodes {
		n, ok := m[clusterID]
		if ok && n.nodeID == nodeID {
			return n
		}
	}
	return nil
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

type jobType uint64

const (
	snapshotAvailable jobType = iota
	snapshotRequested
	streamSnapshot
)

var jobTypeNames = [...]string{
	"snapshotAvailable",
	"snapshotRequested",
	"streamSnapshot",
}

func (jt jobType) String() string {
	return jobTypeNames[uint64(jt)]
}

type pendingJob struct {
	jt        jobType
	clusterID uint64
}

type job struct {
	task rsm.Task
	node *node
	sink getSink
}

type ssWorker struct {
	workerID   uint64
	stopper    *syncutil.Stopper
	requestC   chan job
	completedC chan struct{}
}

func newSSWorker(workerID uint64, stopper *syncutil.Stopper) *ssWorker {
	w := &ssWorker{
		workerID:   workerID,
		stopper:    stopper,
		requestC:   make(chan job, 1),
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
		case job := <-w.requestC:
			if job.node == nil {
				panic("req.node == nil")
			}
			w.handle(job)
			w.completed()
		}
	}
}

func (w *ssWorker) completed() {
	w.completedC <- struct{}{}
}

func (w *ssWorker) handle(j job) {
	if j.task.SnapshotAvailable {
		w.recoverFromSnapshot(j)
	} else if j.task.SnapshotRequested {
		w.saveSnapshot(j)
	} else if j.task.StreamSnapshot {
		w.streamSnapshot(j)
	} else {
		panic("unknown snapshot task type")
	}
}

func (w *ssWorker) recoverFromSnapshot(j job) {
	if index, err := j.node.recoverFromSnapshot(j.task); err != nil {
		if err != sm.ErrOpenStopped && err != sm.ErrSnapshotStopped {
			panic(err)
		}
	} else {
		j.node.recoverFromSnapshotDone(index)
	}
}

func (w *ssWorker) saveSnapshot(j job) {
	if err := j.node.saveSnapshot(j.task); err != nil {
		panic(err)
	} else {
		j.node.saveSnapshotDone()
	}
}

func (w *ssWorker) streamSnapshot(j job) {
	if err := j.node.streamSnapshot(j.sink()); err != nil {
		panic(err)
	} else {
		j.node.streamSnapshotDone()
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
	pending       []pendingJob
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
		pending:       make([]pendingJob, 0),
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
			clusters := p.saveReady.getReadyMap(1)
			for cid := range clusters {
				pj := pendingJob{clusterID: cid, jt: snapshotRequested}
				plog.Infof("%s snapshotRequested for %d", p.nh.id(), cid)
				p.pending = append(p.pending, pj)
				toSchedule = true
			}
		} else if chosen == 2 {
			clusters := p.recoverReady.getReadyMap(1)
			for cid := range clusters {
				pj := pendingJob{clusterID: cid, jt: snapshotAvailable}
				plog.Infof("%s snapshotAvailable for %d", p.nh.id(), cid)
				p.pending = append(p.pending, pj)
				toSchedule = true
			}
		} else if chosen == 3 {
			clusters := p.streamReady.getReadyMap(1)
			for cid := range clusters {
				pj := pendingJob{clusterID: cid, jt: streamSnapshot}
				plog.Infof("%s streamSnapshot for %d", p.nh.id(), cid)
				p.pending = append(p.pending, pj)
				toSchedule = true
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
			cci, nodes = p.loadNodes(0, nodes)
			p.schedule(nodes)
		}
	}
}

func (p *workerPool) loadNodes(cci uint64,
	nodes map[uint64]*node) (uint64, map[uint64]*node) {
	newCCI, newNodes, offloaded := p.doLoadNodes(cci, nodes)
	p.loaded.update(1, rsm.FromSnapshotWorker, newNodes)
	for _, n := range offloaded {
		n.notifyOffloaded(rsm.FromSnapshotWorker)
	}
	return newCCI, newNodes
}

func (p *workerPool) doLoadNodes(cci uint64,
	nodes map[uint64]*node) (uint64, map[uint64]*node, []*node) {
	newCCI := p.nh.getClusterSetIndex()
	var offloaded []*node
	if newCCI != cci {
		newNodes := make(map[uint64]*node)
		// busy nodes should never be offloaded
		for _, n := range p.busy {
			newNodes[n.clusterID] = n
		}
		p.nh.forEachClusterRun(nil,
			func() bool {
				for cid, node := range nodes {
					_, ok := newNodes[cid]
					if !ok {
						offloaded = append(offloaded, node)
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
		return newCCI, newNodes, offloaded
	}
	return cci, nodes, offloaded
}

func (p *workerPool) completed(workerID uint64) {
	n, ok := p.busy[workerID]
	if !ok {
		plog.Panicf("worker %d is not busy", workerID)
	}
	delete(p.busy, workerID)
	_, ok1 := p.saving[n.clusterID]
	if ok1 {
		plog.Infof("%s completed snapshotRequested", n.id())
		delete(p.saving, n.clusterID)
	}
	_, ok2 := p.recovering[n.clusterID]
	if ok2 {
		plog.Infof("%s completed snapshotAvailable", n.id())
		delete(p.recovering, n.clusterID)
	}
	count, ok3 := p.streaming[n.clusterID]
	if ok3 {
		plog.Infof("%s completed streamSnapshot", n.id())
		if count == 0 {
			plog.Panicf("node completed streaming when not streaming")
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
	return !ok
}

func (p *workerPool) canSave(clusterID uint64) bool {
	return !p.inProgress(clusterID)
}

func (p *workerPool) canRecover(clusterID uint64) bool {
	return !p.inProgress(clusterID)
}

func (p *workerPool) canSchedule(j pendingJob) bool {
	switch j.jt {
	case snapshotAvailable:
		return p.canRecover(j.clusterID)
	case snapshotRequested:
		return p.canSave(j.clusterID)
	case streamSnapshot:
		return p.canStream(j.clusterID)
	default:
		plog.Panicf("unknown job type %d", j.jt)
	}
	panic("not suppose to reach here")
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

func (p *workerPool) start(jt jobType, node *node, workerID uint64) {
	p.setBusy(node, workerID)
	switch jt {
	case snapshotAvailable:
		p.startRecovering(node, workerID)
	case snapshotRequested:
		p.startSaving(node, workerID)
	case streamSnapshot:
		p.startStreaming(node, workerID)
	default:
		plog.Panicf("unknown task type %d", jt)
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
		plog.Infof("%s no more worker", p.nh.id())
		return false
	}
	for idx, pj := range p.pending {
		n, ok := nodes[pj.clusterID]
		if !ok {
			p.removeFromPending(idx)
			return true
		}
		if p.canSchedule(pj) {
			if p.scheduleTask(pj.jt, n, w) {
				plog.Infof("%s scheduled for %s", n.id(), pj.jt)
				p.removeFromPending(idx)
				return true
			}
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

func (p *workerPool) scheduleTask(jt jobType, n *node, w *ssWorker) bool {
	var j job
	switch jt {
	case snapshotRequested:
		req, ok := n.ss.getSaveSnapshotReq()
		if !ok {
			return false
		}
		j = job{task: req, node: n}
	case snapshotAvailable:
		req, ok := n.ss.getRecoverFromSnapshotReq()
		if !ok {
			return false
		}
		j = job{task: req, node: n}
	case streamSnapshot:
		req, sinkFn, ok := n.ss.getStreamSnapshotReq()
		if !ok {
			return false
		}
		j = job{task: req, node: n, sink: sinkFn}
	default:
		panic("unknown job type")
	}
	p.start(jt, n, w.workerID)
	select {
	case w.requestC <- j:
	default:
		panic("worker busy")
	}
	return true
}

type execEngine struct {
	nodeStopper     *syncutil.Stopper
	commitStopper   *syncutil.Stopper
	taskStopper     *syncutil.Stopper
	nh              nodeLoader
	loaded          *loadedNodes
	ctx             *server.Context
	logdb           raftio.ILogDB
	ctxs            []raftio.IContext
	profilers       []*profiler
	stepWorkReady   *workReady
	commitWorkReady *workReady
	applyWorkReady  *workReady
	wp              *workerPool
	notifyCommit    bool
}

func newExecEngine(nh nodeLoader, notifyCommit bool,
	ctx *server.Context, logdb raftio.ILogDB) *execEngine {
	loaded := newLoadedNodes()
	s := &execEngine{
		nh:              nh,
		ctx:             ctx,
		logdb:           logdb,
		loaded:          loaded,
		nodeStopper:     syncutil.NewStopper(),
		commitStopper:   syncutil.NewStopper(),
		taskStopper:     syncutil.NewStopper(),
		stepWorkReady:   newWorkReady(stepWorkerCount),
		commitWorkReady: newWorkReady(commitWorkerCount),
		applyWorkReady:  newWorkReady(applyWorkerCount),
		ctxs:            make([]raftio.IContext, stepWorkerCount),
		profilers:       make([]*profiler, stepWorkerCount),
		wp:              newWorkerPool(nh, loaded),
		notifyCommit:    notifyCommit,
	}
	sampleRatio := int64(delaySampleRatio / 10)
	for i := uint64(1); i <= stepWorkerCount; i++ {
		workerID := i
		s.ctxs[i-1] = logdb.GetLogDBThreadContext()
		s.profilers[i-1] = newProfiler(sampleRatio)
		s.nodeStopper.RunWorker(func() {
			s.stepWorkerMain(workerID)
		})
	}
	if notifyCommit {
		for i := uint64(1); i <= commitWorkerCount; i++ {
			commitWorkerID := i
			s.commitStopper.RunWorker(func() {
				s.commitWorkerMain(commitWorkerID)
			})
		}
	}
	for i := uint64(1); i <= applyWorkerCount; i++ {
		applyWorkerID := i
		s.taskStopper.RunWorker(func() {
			s.applyWorkerMain(applyWorkerID)
		})
	}
	return s
}

func (s *execEngine) stop() {
	s.nodeStopper.Stop()
	s.commitStopper.Stop()
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
	return s.loaded.get(clusterID, nodeID) != nil
}

func (s *execEngine) destroyedC(clusterID uint64, nodeID uint64) <-chan struct{} {
	if n := s.loaded.get(clusterID, nodeID); n != nil {
		return n.sm.DestroyedC()
	}
	return nil
}

func (s *execEngine) load(workerID uint64,
	cci uint64, nodes map[uint64]*node,
	from rsm.From, ready *workReady) (map[uint64]*node, uint64) {
	result, offloaded, cci := s.loadBucketNodes(workerID, cci, nodes,
		ready.getPartitioner(), from)
	s.loaded.update(workerID, from, result)
	for _, n := range offloaded {
		n.notifyOffloaded(from)
	}
	return result, cci
}

func (s *execEngine) commitWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	cci := uint64(0)
	for {
		select {
		case <-s.commitStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromCommitWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadCommitNodes(workerID, cci, nodes)
			s.processCommits(workerID, make(map[uint64]struct{}), nodes)
		case <-s.commitWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = s.loadCommitNodes(workerID, cci, nodes)
			}
			clusterIDMap := s.commitWorkReady.getReadyMap(workerID)
			s.processCommits(workerID, clusterIDMap, nodes)
		}
	}
}

func (s *execEngine) loadCommitNodes(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.load(workerID, cci, nodes, rsm.FromCommitWorker, s.commitWorkReady)
}

func (s *execEngine) processCommits(workerID uint64,
	idmap map[uint64]struct{}, nodes map[uint64]*node) {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	for clusterID := range idmap {
		node, ok := nodes[clusterID]
		if !ok || node.stopped() {
			continue
		}
		node.notifyCommittedEntries()
	}
}

func (s *execEngine) applyWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	batch := make([]rsm.Task, 0, taskBatchSize)
	entries := make([]sm.Entry, 0, taskBatchSize)
	cci := uint64(0)
	for {
		select {
		case <-s.taskStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromApplyWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadApplyNodes(workerID, cci, nodes)
			s.processApplies(workerID, make(map[uint64]struct{}), nodes, batch, entries)
			batch = make([]rsm.Task, 0, taskBatchSize)
			entries = make([]sm.Entry, 0, taskBatchSize)
		case <-s.applyWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = s.loadApplyNodes(workerID, cci, nodes)
			}
			clusterIDMap := s.applyWorkReady.getReadyMap(workerID)
			s.processApplies(workerID, clusterIDMap, nodes, batch, entries)
		}
	}
}

func (s *execEngine) loadApplyNodes(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.load(workerID, cci, nodes, rsm.FromApplyWorker, s.applyWorkReady)
}

// T: take snapshot
// R: recover from snapshot
// existing op, new op, action
// T, T, ignore the new op
// T, R, R is queued as node state, will be handled when T is done
// R, R, won't happen, when in R state, processApplies will not process the node
// R, T, won't happen, when in R state, processApplies will not process the node

func (s *execEngine) processApplies(workerID uint64,
	idmap map[uint64]struct{},
	nodes map[uint64]*node, batch []rsm.Task, entries []sm.Entry) {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	var p *profiler
	if stepWorkerCount == applyWorkerCount {
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

func (s *execEngine) stepWorkerMain(workerID uint64) {
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
			nodes, cci = s.loadStepNodes(workerID, cci, nodes)
			s.processSteps(workerID, make(map[uint64]struct{}), nodes, stopC)
		case <-s.stepWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = s.loadStepNodes(workerID, cci, nodes)
			}
			clusterIDMap := s.stepWorkReady.getReadyMap(workerID)
			s.processSteps(workerID, clusterIDMap, nodes, stopC)
		}
	}
}

func (s *execEngine) loadStepNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.load(workerID, cci, nodes, rsm.FromStepWorker, s.stepWorkReady)
}

func (s *execEngine) loadBucketNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node, partitioner server.IPartitioner,
	from rsm.From) (map[uint64]*node, []*node, uint64) {
	bucket := workerID - 1
	newCCI := s.nh.getClusterSetIndex()
	var offloaded []*node
	if newCCI != cci {
		newNodes := make(map[uint64]*node)
		s.nh.forEachClusterRun(nil,
			func() bool {
				for cid, node := range nodes {
					nv, ok := newNodes[cid]
					if !ok {
						offloaded = append(offloaded, node)
					} else {
						if nv.instanceID != node.instanceID {
							offloaded = append(offloaded, node)
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
		return newNodes, offloaded, newCCI
	}
	return nodes, offloaded, cci
}

func (s *execEngine) processSteps(workerID uint64,
	clusterIDMap map[uint64]struct{},
	nodes map[uint64]*node, stopC chan struct{}) {
	if len(nodes) == 0 {
		return
	}
	if tests.ReadyToReturnTestKnob(stopC, "") {
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
	if tests.ReadyToReturnTestKnob(stopC, "sending append msg") {
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
	if tests.ReadyToReturnTestKnob(stopC, "saving raft state") {
		return
	}
	p.save.start()
	if err := s.logdb.SaveRaftState(nodeUpdates, nodeCtx); err != nil {
		panic(err)
	}
	p.save.end()
	if tests.ReadyToReturnTestKnob(stopC, "saving snapshots") {
		return
	}
	if err := s.onSnapshotSaved(nodeUpdates, nodes); err != nil {
		panic(err)
	}
	if tests.ReadyToReturnTestKnob(stopC, "applying updates") {
		return
	}
	s.applySnapshotAndUpdate(nodeUpdates, nodes, false)
	if tests.ReadyToReturnTestKnob(stopC, "processing raft updates") {
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
		if tests.ReadyToReturnTestKnob(stopC, "committing updates") {
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
		s.setStepReady(ud.ClusterID)
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
			plog.Infof("raft update not applied, %s stopped", node.id())
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

func (s *execEngine) setStepReady(clusterID uint64) {
	s.stepWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setCommitReady(clusterID uint64) {
	s.commitWorkReady.clusterReady(clusterID)
}

func (s *execEngine) setApplyReady(clusterID uint64) {
	s.applyWorkReady.clusterReady(clusterID)
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
	p := s.stepWorkReady.getPartitioner()
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
