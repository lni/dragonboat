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
	"time"

	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	reloadTime           = settings.Soft.NodeReloadMillisecond
	timedCloseWaitSecond = settings.Soft.CloseWorkerTimedWaitSecond
	timedCloseWait       = time.Second * time.Duration(timedCloseWaitSecond)
	nodeReloadInterval   = time.Millisecond * time.Duration(reloadTime)
	taskBatchSize        = settings.Soft.TaskBatchSize
)

type bitmap struct {
	v uint64
}

func (b *bitmap) contains(v uint64) bool {
	if v >= 64 {
		panic("invalid v")
	}
	return b.v&(1<<v) > 0
}

func (b *bitmap) add(v uint64) {
	if v >= 64 {
		panic("invalid v")
	}
	b.v = b.v | (1 << v)
}

type from uint64

const (
	fromStepWorker from = iota
	fromCommitWorker
	fromApplyWorker
	fromWorkerPool
	fromWorker
)

type nodeLoader interface {
	describe() string
	getShardSetIndex() uint64
	forEachShard(f func(uint64, *node) bool) uint64
}

type nodeType struct {
	workerID uint64
	from     from
}

type loadedNodes struct {
	nodes map[nodeType]map[uint64]*node
	mu    sync.Mutex
}

func newLoadedNodes() *loadedNodes {
	return &loadedNodes{
		nodes: make(map[nodeType]map[uint64]*node),
	}
}

func (l *loadedNodes) get(shardID uint64, replicaID uint64) *node {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.nodes {
		if n, ok := m[shardID]; ok && n.replicaID == replicaID {
			return n
		}
	}
	return nil
}

func (l *loadedNodes) update(workerID uint64,
	from from, nodes map[uint64]*node) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nt := nodeType{workerID: workerID, from: from}
	l.nodes[nt] = nodes
}

// nodes is a map of workerID -> *node
func (l *loadedNodes) updateFromBusySSNodes(nodes map[uint64]*node) {
	l.updateFromLoadedSSNodes(fromWorker, nodes)
}

// nodes is a map of shardID -> *node
func (l *loadedNodes) updateFromLoadedSSNodes(from from,
	nodes map[uint64]*node) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nt := nodeType{workerID: 0, from: from}
	nm := make(map[uint64]*node, len(nodes))
	for _, n := range nodes {
		nm[n.shardID] = n
	}
	l.nodes[nt] = nm
}

type workReady struct {
	partitioner server.IPartitioner
	maps        []*readyShard
	channels    []chan struct{}
	count       uint64
}

func newWorkReady(count uint64) *workReady {
	wr := &workReady{
		partitioner: server.NewFixedPartitioner(count),
		count:       count,
		maps:        make([]*readyShard, count),
		channels:    make([]chan struct{}, count),
	}
	for i := uint64(0); i < count; i++ {
		wr.channels[i] = make(chan struct{}, 1)
		wr.maps[i] = newReadyShard()
	}
	return wr
}

func (wr *workReady) getPartitioner() server.IPartitioner {
	return wr.partitioner
}

func (wr *workReady) notify(idx uint64) {
	select {
	case wr.channels[idx] <- struct{}{}:
	default:
	}
}

func (wr *workReady) shardReadyByUpdates(updates []pb.Update) {
	var notified bitmap
	for _, ud := range updates {
		if len(ud.CommittedEntries) > 0 {
			idx := wr.partitioner.GetPartitionID(ud.ShardID)
			readyMap := wr.maps[idx]
			readyMap.setShardReady(ud.ShardID)
		}
	}
	for _, ud := range updates {
		if len(ud.CommittedEntries) > 0 {
			idx := wr.partitioner.GetPartitionID(ud.ShardID)
			if !notified.contains(idx) {
				notified.add(idx)
				wr.notify(idx)
			}
		}
	}
}

func (wr *workReady) shardReadyByMessageBatch(mb pb.MessageBatch) {
	var notified bitmap
	for _, req := range mb.Requests {
		idx := wr.partitioner.GetPartitionID(req.ShardID)
		readyMap := wr.maps[idx]
		readyMap.setShardReady(req.ShardID)
	}
	for _, req := range mb.Requests {
		idx := wr.partitioner.GetPartitionID(req.ShardID)
		if !notified.contains(idx) {
			notified.add(idx)
			wr.notify(idx)
		}
	}
}

func (wr *workReady) allShardsReady(nodes []*node) {
	var notified bitmap
	for _, n := range nodes {
		idx := wr.partitioner.GetPartitionID(n.shardID)
		readyMap := wr.maps[idx]
		readyMap.setShardReady(n.shardID)
	}
	for _, n := range nodes {
		idx := wr.partitioner.GetPartitionID(n.shardID)
		if !notified.contains(idx) {
			notified.add(idx)
			wr.notify(idx)
		}
	}
}

func (wr *workReady) shardReady(shardID uint64) {
	idx := wr.partitioner.GetPartitionID(shardID)
	readyMap := wr.maps[idx]
	readyMap.setShardReady(shardID)
	wr.notify(idx)
}

func (wr *workReady) waitCh(workerID uint64) chan struct{} {
	return wr.channels[workerID-1]
}

func (wr *workReady) getReadyMap(workerID uint64) map[uint64]struct{} {
	readyMap := wr.maps[workerID-1]
	return readyMap.getReadyShards()
}

type job struct {
	node       *node
	sink       getSink
	task       rsm.Task
	instanceID uint64
	shardID    uint64
}

type ssWorker struct {
	stopper    *syncutil.Stopper
	requestC   chan job
	completedC chan struct{}
	workerID   uint64
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
			if err := w.handle(job); err != nil {
				panicNow(err)
			}
			w.completed()
		}
	}
}

func (w *ssWorker) completed() {
	w.completedC <- struct{}{}
}

func (w *ssWorker) handle(j job) error {
	if j.task.Recover {
		return w.recover(j)
	} else if j.task.Save {
		return w.save(j)
	} else if j.task.Stream {
		return w.stream(j)
	}
	panic("unknown snapshot task type")
}

func (w *ssWorker) recover(j job) error {
	var err error
	var index uint64
	if index, err = j.node.recover(j.task); err != nil {
		return err
	}
	j.node.recoverDone(index)
	return nil
}

func (w *ssWorker) save(j job) error {
	if err := j.node.save(j.task); err != nil {
		return err
	}
	j.node.saveDone()
	return nil
}

func (w *ssWorker) stream(j job) error {
	if err := j.node.stream(j.sink()); err != nil {
		return err
	}
	j.node.streamDone()
	return nil
}

type workerPool struct {
	nh            nodeLoader
	saving        map[uint64]struct{}
	cciReady      *workReady
	saveReady     *workReady
	recoverReady  *workReady
	streamReady   *workReady
	workerStopper *syncutil.Stopper
	busy          map[uint64]*node
	loaded        *loadedNodes
	recovering    map[uint64]struct{}
	streaming     map[uint64]uint64
	nodes         map[uint64]*node
	poolStopper   *syncutil.Stopper
	pending       []job
	workers       []*ssWorker
	cci           uint64
}

func newWorkerPool(nh nodeLoader,
	snapshotWorkerCount uint64, loaded *loadedNodes) *workerPool {
	w := &workerPool{
		nh:            nh,
		loaded:        loaded,
		cciReady:      newWorkReady(1),
		saveReady:     newWorkReady(1),
		recoverReady:  newWorkReady(1),
		streamReady:   newWorkReady(1),
		nodes:         make(map[uint64]*node),
		workers:       make([]*ssWorker, snapshotWorkerCount),
		busy:          make(map[uint64]*node, snapshotWorkerCount),
		saving:        make(map[uint64]struct{}, snapshotWorkerCount),
		recovering:    make(map[uint64]struct{}, snapshotWorkerCount),
		streaming:     make(map[uint64]uint64, snapshotWorkerCount),
		pending:       make([]job, 0),
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

func (p *workerPool) close() error {
	p.poolStopper.Stop()
	return nil
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
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	cases := make([]reflect.SelectCase, len(p.workers)+6)
	for {
		toSchedule := false
		// 0 - pool stopper stopc
		// 1 - p.saveReady.waitCh(1)
		// 2 - p.recoverReady.waitCh(1)
		// 3 - p.streamReady.waitCh(1)
		// 4 - p.cciReady.waitCh(1)
		// 5 - worker completedC
		// 5 + len(workers) - ticker.C
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
		cases[4] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.cciReady.waitCh(1)),
		}
		for idx, w := range p.workers {
			cases[5+idx] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.completedC),
			}
		}
		cases[5+len(p.workers)] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ticker.C),
		}
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			p.workerStopper.Stop()
			p.unloadNodes()
			return
		} else if chosen == 1 {
			shards := p.saveReady.getReadyMap(1)
			p.loadNodes()
			for cid := range shards {
				if j, ok := p.getSaveJob(cid); ok {
					plog.Debugf("%s saveRequested for %d", p.nh.describe(), cid)
					p.pending = append(p.pending, j)
					toSchedule = true
				}
			}
		} else if chosen == 2 {
			shards := p.recoverReady.getReadyMap(1)
			p.loadNodes()
			for cid := range shards {
				if j, ok := p.getRecoverJob(cid); ok {
					plog.Debugf("%s recoverRequested for %d", p.nh.describe(), cid)
					p.pending = append(p.pending, j)
					toSchedule = true
				}
			}
		} else if chosen == 3 {
			shards := p.streamReady.getReadyMap(1)
			p.loadNodes()
			for cid := range shards {
				if j, ok := p.getStreamJob(cid); ok {
					plog.Debugf("%s streamRequested for %d", p.nh.describe(), cid)
					p.pending = append(p.pending, j)
					toSchedule = true
				}
			}
		} else if chosen == 4 {
			p.loadNodes()
		} else if chosen >= 5 && chosen <= 5+len(p.workers)-1 {
			workerID := uint64(chosen - 5)
			p.completed(workerID)
			toSchedule = true
		} else if chosen == len(cases)-1 {
			p.loadNodes()
		} else {
			plog.Panicf("chosen %d, unexpected case", chosen)
		}
		if toSchedule {
			p.loadNodes()
			p.schedule()
		}
	}
}

func (p *workerPool) unloadNodes() {
	for _, n := range p.nodes {
		n.offloaded()
	}
	for _, n := range p.busy {
		n.offloaded()
	}
}

func (p *workerPool) updateLoadedBusyNodes() {
	p.loaded.updateFromBusySSNodes(p.busy)
}

func (p *workerPool) loadNodes() {
	if p.nh.getShardSetIndex() != p.cci {
		newNodes := make(map[uint64]*node)
		loaded := make([]*node, 0)
		p.cci = p.nh.forEachShard(func(cid uint64, n *node) bool {
			if on, ok := p.nodes[cid]; ok {
				if on.instanceID != n.instanceID {
					plog.Panicf("%s from two incarnations found", n.id())
				}
				newNodes[cid] = on
			} else {
				loaded = append(loaded, n)
				newNodes[cid] = n
			}
			return true
		})
		p.loaded.updateFromLoadedSSNodes(fromWorkerPool, newNodes)
		for cid, n := range p.nodes {
			if _, ok := newNodes[cid]; !ok {
				n.offloaded()
			}
		}
		for _, n := range loaded {
			n.loaded()
		}
		p.nodes = newNodes
	}
}

func (p *workerPool) completed(workerID uint64) {
	count := 0
	n, ok := p.busy[workerID]
	if !ok {
		plog.Panicf("worker %d is not busy", workerID)
	}
	if _, ok := p.saving[n.shardID]; ok {
		plog.Debugf("%s completed saveRequested", n.id())
		delete(p.saving, n.shardID)
		count++
	}
	if _, ok := p.recovering[n.shardID]; ok {
		plog.Debugf("%s completed recoverRequested", n.id())
		delete(p.recovering, n.shardID)
		count++
	}
	if sc, ok := p.streaming[n.shardID]; ok {
		plog.Debugf("%s completed streamRequested", n.id())
		if sc == 0 {
			plog.Panicf("node completed streaming when not streaming")
		} else if sc == 1 {
			delete(p.streaming, n.shardID)
		} else {
			p.streaming[n.shardID] = sc - 1
		}
		count++
	}
	if count == 0 {
		plog.Panicf("not sure what got completed")
	}
	if count > 1 {
		plog.Panicf("completed more than one type of snapshot op")
	}
	p.setIdle(workerID)
}

func (p *workerPool) inProgress(shardID uint64) bool {
	_, ok1 := p.saving[shardID]
	_, ok2 := p.recovering[shardID]
	_, ok3 := p.streaming[shardID]
	return ok1 || ok2 || ok3
}

func (p *workerPool) canStream(shardID uint64) bool {
	if _, ok := p.saving[shardID]; ok {
		return false
	}
	_, ok := p.recovering[shardID]
	return !ok
}

func (p *workerPool) canSave(shardID uint64) bool {
	return !p.inProgress(shardID)
}

func (p *workerPool) canRecover(shardID uint64) bool {
	return !p.inProgress(shardID)
}

func (p *workerPool) canSchedule(j job) bool {
	if j.task.Recover {
		return p.canRecover(j.shardID)
	} else if j.task.Save {
		return p.canSave(j.shardID)
	} else if j.task.Stream {
		return p.canStream(j.shardID)
	} else {
		plog.Panicf("unknown task type %+v", j.task)
	}
	panic("not suppose to reach here")
}

func (p *workerPool) setIdle(workerID uint64) {
	n, ok := p.busy[workerID]
	if !ok {
		plog.Panicf("worker %d is not busy", workerID)
	}
	delete(p.busy, workerID)
	p.updateLoadedBusyNodes()
	n.offloaded()
}

func (p *workerPool) setBusy(n *node, workerID uint64) {
	if _, ok := p.busy[workerID]; ok {
		plog.Panicf("trying to use a busy worker")
	}
	n.loaded()
	p.busy[workerID] = n
	p.updateLoadedBusyNodes()
}

func (p *workerPool) startStreaming(n *node) {
	if count, ok := p.streaming[n.shardID]; !ok {
		p.streaming[n.shardID] = 1
	} else {
		p.streaming[n.shardID] = count + 1
	}
}

func (p *workerPool) startSaving(n *node) {
	if _, ok := p.saving[n.shardID]; ok {
		plog.Panicf("%s trying to start saving again", n.id())
	}
	p.saving[n.shardID] = struct{}{}
}

func (p *workerPool) startRecovering(n *node) {
	if _, ok := p.recovering[n.shardID]; ok {
		plog.Panicf("%s trying to start recovering again", n.id())
	}
	p.recovering[n.shardID] = struct{}{}
}

func (p *workerPool) start(j job, n *node, workerID uint64) {
	p.setBusy(n, workerID)
	if j.task.Recover {
		p.startRecovering(n)
	} else if j.task.Save {
		p.startSaving(n)
	} else if j.task.Stream {
		p.startStreaming(n)
	} else {
		plog.Panicf("unknown task type %+v", j.task)
	}
}

func (p *workerPool) schedule() {
	for {
		if !p.scheduleWorker() {
			return
		}
	}
}

func (p *workerPool) scheduleWorker() bool {
	if len(p.pending) == 0 {
		return false
	}
	w := p.getWorker()
	if w == nil {
		plog.Debugf("%s no more worker", p.nh.describe())
		return false
	}
	for idx, j := range p.pending {
		n, ok := p.nodes[j.shardID]
		if !ok {
			p.removeFromPending(idx)
			return true
		}
		if p.canSchedule(j) {
			p.scheduleTask(j, n, w)
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
}

func (p *workerPool) getSaveJob(shardID uint64) (job, bool) {
	n, ok := p.nodes[shardID]
	if !ok {
		return job{}, false
	}
	req, ok := n.ss.getSaveReq()
	if !ok {
		return job{}, false
	}
	return job{
		task:       req,
		node:       n,
		instanceID: n.instanceID,
		shardID:    shardID,
	}, true
}

func (p *workerPool) getRecoverJob(shardID uint64) (job, bool) {
	n, ok := p.nodes[shardID]
	if !ok {
		return job{}, false
	}
	req, ok := n.ss.getRecoverReq()
	if !ok {
		return job{}, false
	}
	return job{
		task:       req,
		node:       n,
		instanceID: n.instanceID,
		shardID:    shardID,
	}, true
}

func (p *workerPool) getStreamJob(shardID uint64) (job, bool) {
	n, ok := p.nodes[shardID]
	if !ok {
		return job{}, false
	}
	req, sinkFn, ok := n.ss.getStreamReq()
	if !ok {
		return job{}, false
	}
	return job{
		task:       req,
		node:       n,
		sink:       sinkFn,
		instanceID: n.instanceID,
		shardID:    shardID,
	}, true
}

func (p *workerPool) scheduleTask(j job, n *node, w *ssWorker) {
	if n.instanceID == j.instanceID {
		p.start(j, n, w.workerID)
		select {
		case w.requestC <- j:
		default:
			panic("worker received multiple jobs")
		}
	}
}

type closeReq struct {
	node *node
}

type closeWorker struct {
	stopper    *syncutil.Stopper
	requestC   chan closeReq
	completedC chan struct{}
	workerID   uint64
}

func newCloseWorker(workerID uint64, stopper *syncutil.Stopper) *closeWorker {
	w := &closeWorker{
		workerID:   workerID,
		stopper:    stopper,
		requestC:   make(chan closeReq, 1),
		completedC: make(chan struct{}, 1),
	}
	stopper.RunWorker(func() {
		w.workerMain()
	})
	return w
}

func (w *closeWorker) workerMain() {
	for {
		select {
		case <-w.stopper.ShouldStop():
			return
		case req := <-w.requestC:
			if err := w.handle(req); err != nil {
				panicNow(err)
			}
			w.completed()
		}
	}
}

func (w *closeWorker) completed() {
	w.completedC <- struct{}{}
}

func (w *closeWorker) handle(req closeReq) error {
	if req.node.destroyed() {
		return nil
	}
	return req.node.destroy()
}

type closeWorkerPool struct {
	ready         chan closeReq
	busy          map[uint64]uint64
	processing    map[uint64]struct{}
	workerStopper *syncutil.Stopper
	poolStopper   *syncutil.Stopper
	workers       []*closeWorker
	pending       []*node
}

func newCloseWorkerPool(closeWorkerCount uint64) *closeWorkerPool {
	w := &closeWorkerPool{
		workers:       make([]*closeWorker, closeWorkerCount),
		ready:         make(chan closeReq, 1),
		busy:          make(map[uint64]uint64, closeWorkerCount),
		processing:    make(map[uint64]struct{}, closeWorkerCount),
		pending:       make([]*node, 0),
		workerStopper: syncutil.NewStopper(),
		poolStopper:   syncutil.NewStopper(),
	}

	for workerID := uint64(0); workerID < closeWorkerCount; workerID++ {
		w.workers[workerID] = newCloseWorker(workerID, w.workerStopper)
	}
	w.poolStopper.RunWorker(func() {
		w.workerPoolMain()
	})
	return w
}

func (p *closeWorkerPool) close() error {
	p.poolStopper.Stop()
	return nil
}

func (p *closeWorkerPool) workerPoolMain() {
	cases := make([]reflect.SelectCase, len(p.workers)+2)
	for {
		// 0 - pool stopper stopc
		// 1 - node ready for destroy
		cases[0] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.poolStopper.ShouldStop()),
		}
		cases[1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.ready),
		}
		for idx, w := range p.workers {
			cases[2+idx] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.completedC),
			}
		}
		chosen, v, _ := reflect.Select(cases)
		if chosen == 0 {
			p.timedWait()
			return
		} else if chosen == 1 {
			node := v.Interface().(closeReq).node
			p.pending = append(p.pending, node)
		} else if chosen > 1 && chosen < len(p.workers)+2 {
			workerID := uint64(chosen - 2)
			p.completed(workerID)
		} else {
			plog.Panicf("chosen %d, unknown case", chosen)
		}
		p.schedule()
	}
}

func (p *closeWorkerPool) timedWait() {
	timer := time.NewTimer(timedCloseWait)
	timeout := false
	defer timer.Stop()
	defer p.workerStopper.Stop()
	defer func() {
		if timeout {
			plog.Infof("timedWait ready to exit, busy %d, pending %d",
				len(p.busy), len(p.pending))
		}
	}()
	// p.ready is buffered, don't ignore that buffered close req
	select {
	case v := <-p.ready:
		p.pending = append(p.pending, v.node)
	default:
	}
	p.schedule()
	cases := make([]reflect.SelectCase, len(p.workers)+1)
	for !p.isIdle() {
		cases[0] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(timer.C),
		}
		for idx, w := range p.workers {
			cases[1+idx] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.completedC),
			}
		}
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			timeout = true
			return
		} else if chosen > 0 && chosen < len(p.workers)+1 {
			select {
			case <-timer.C:
				timeout = true
				return
			default:
			}
			workerID := uint64(chosen - 1)
			p.completed(workerID)
			p.schedule()
		} else {
			plog.Panicf("chosen %d, unknown case", chosen)
		}
	}
}

func (p *closeWorkerPool) isIdle() bool {
	return len(p.busy) == 0 && len(p.pending) == 0
}

func (p *closeWorkerPool) completed(workerID uint64) {
	shardID, ok := p.busy[workerID]
	if !ok {
		plog.Panicf("close worker %d is not in busy state", workerID)
	}
	if _, ok := p.processing[shardID]; !ok {
		plog.Panicf("shard %d is not being processed", shardID)
	}
	delete(p.processing, shardID)
	delete(p.busy, workerID)
}

func (p *closeWorkerPool) setBusy(workerID uint64, shardID uint64) {
	p.processing[shardID] = struct{}{}
	p.busy[workerID] = shardID
}

func (p *closeWorkerPool) getWorker() *closeWorker {
	for _, w := range p.workers {
		if _, busy := p.busy[w.workerID]; !busy {
			return w
		}
	}
	return nil
}

func (p *closeWorkerPool) schedule() {
	for {
		if !p.scheduleWorker() {
			return
		}
	}
}

func (p *closeWorkerPool) canSchedule(n *node) bool {
	_, ok := p.processing[n.shardID]
	return !ok
}

func (p *closeWorkerPool) scheduleWorker() bool {
	w := p.getWorker()
	if w == nil {
		return false
	}

	for i := 0; i < len(p.pending); i++ {
		node := p.pending[0]
		p.removeFromPending(0)
		if p.canSchedule(node) {
			p.scheduleReq(node, w)
			return true
		} else {
			p.pending = append(p.pending, node)
		}
	}

	return false
}

func (p *closeWorkerPool) scheduleReq(n *node, w *closeWorker) {
	p.setBusy(w.workerID, n.shardID)
	select {
	case w.requestC <- closeReq{node: n}:
	default:
		panic("worker received multiple jobs")
	}
}

func (p *closeWorkerPool) removeFromPending(idx int) {
	sz := len(p.pending)
	copy(p.pending[idx:], p.pending[idx+1:])
	p.pending = p.pending[:sz-1]
}

type engine struct {
	nodeStopper     *syncutil.Stopper
	commitStopper   *syncutil.Stopper
	taskStopper     *syncutil.Stopper
	nh              nodeLoader
	loaded          *loadedNodes
	env             *server.Env
	logdb           raftio.ILogDB
	stepWorkReady   *workReady
	stepCCIReady    *workReady
	commitWorkReady *workReady
	commitCCIReady  *workReady
	applyWorkReady  *workReady
	applyCCIReady   *workReady
	wp              *workerPool
	cp              *closeWorkerPool
	ec              chan error
	notifyCommit    bool
}

func newExecEngine(nh nodeLoader, cfg config.EngineConfig, notifyCommit bool,
	errorInjection bool, env *server.Env, logdb raftio.ILogDB) *engine {
	if cfg.ExecShards == 0 {
		panic("ExecShards == 0")
	}
	loaded := newLoadedNodes()
	s := &engine{
		nh:              nh,
		env:             env,
		logdb:           logdb,
		loaded:          loaded,
		nodeStopper:     syncutil.NewStopper(),
		commitStopper:   syncutil.NewStopper(),
		taskStopper:     syncutil.NewStopper(),
		stepWorkReady:   newWorkReady(cfg.ExecShards),
		stepCCIReady:    newWorkReady(cfg.ExecShards),
		commitWorkReady: newWorkReady(cfg.CommitShards),
		commitCCIReady:  newWorkReady(cfg.CommitShards),
		applyWorkReady:  newWorkReady(cfg.ApplyShards),
		applyCCIReady:   newWorkReady(cfg.ApplyShards),
		wp:              newWorkerPool(nh, cfg.SnapshotShards, loaded),
		cp:              newCloseWorkerPool(cfg.CloseShards),
		notifyCommit:    notifyCommit,
	}
	if errorInjection {
		s.ec = make(chan error, 1)
	}
	for i := uint64(1); i <= cfg.ExecShards; i++ {
		workerID := i
		s.nodeStopper.RunWorker(func() {
			if errorInjection {
				defer func() {
					if r := recover(); r != nil {
						if ce, ok := r.(error); ok {
							s.crash(ce)
						}
					}
				}()
			}
			s.stepWorkerMain(workerID)
		})
	}
	if notifyCommit {
		for i := uint64(1); i <= cfg.CommitShards; i++ {
			commitWorkerID := i
			s.commitStopper.RunWorker(func() {
				s.commitWorkerMain(commitWorkerID)
			})
		}
	}
	for i := uint64(1); i <= cfg.ApplyShards; i++ {
		applyWorkerID := i
		s.taskStopper.RunWorker(func() {
			s.applyWorkerMain(applyWorkerID)
		})
	}
	return s
}

func (e *engine) crash(err error) {
	select {
	case e.ec <- err:
	default:
	}
}

func (e *engine) close() error {
	e.nodeStopper.Stop()
	e.commitStopper.Stop()
	e.taskStopper.Stop()
	var err error
	err = firstError(err, e.wp.close())
	return firstError(err, e.cp.close())
}

func (e *engine) nodeLoaded(shardID uint64, replicaID uint64) bool {
	return e.loaded.get(shardID, replicaID) != nil
}

func (e *engine) destroyedC(shardID uint64, replicaID uint64) <-chan struct{} {
	if n := e.loaded.get(shardID, replicaID); n != nil {
		return n.sm.DestroyedC()
	}
	return nil
}

func (e *engine) load(workerID uint64,
	cci uint64, nodes map[uint64]*node,
	from from, ready *workReady) (map[uint64]*node, uint64) {
	result, offloaded, cci := e.loadBucketNodes(workerID, cci, nodes,
		ready.getPartitioner(), from)
	e.loaded.update(workerID, from, result)
	for _, n := range offloaded {
		n.offloaded()
	}
	return result, cci
}

func (e *engine) commitWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	cci := uint64(0)
	for {
		select {
		case <-e.commitStopper.ShouldStop():
			e.offloadNodeMap(nodes)
			return
		case <-ticker.C:
			nodes, cci = e.loadCommitNodes(workerID, cci, nodes)
			e.processCommits(make(map[uint64]struct{}), nodes)
		case <-e.commitCCIReady.waitCh(workerID):
			nodes, cci = e.loadCommitNodes(workerID, cci, nodes)
		case <-e.commitWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = e.loadCommitNodes(workerID, cci, nodes)
			}
			active := e.commitWorkReady.getReadyMap(workerID)
			e.processCommits(active, nodes)
		}
	}
}

func (e *engine) loadCommitNodes(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return e.load(workerID, cci, nodes, fromCommitWorker, e.commitWorkReady)
}

func (e *engine) processCommits(idmap map[uint64]struct{},
	nodes map[uint64]*node) {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	for shardID := range idmap {
		node, ok := nodes[shardID]
		if !ok || node.stopped() {
			continue
		}
		node.notifyCommittedEntries()
	}
}

func (e *engine) applyWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	batch := make([]rsm.Task, 0, taskBatchSize)
	entries := make([]sm.Entry, 0, taskBatchSize)
	cci := uint64(0)
	count := uint64(0)
	for {
		select {
		case <-e.taskStopper.ShouldStop():
			e.offloadNodeMap(nodes)
			return
		case <-ticker.C:
			nodes, cci = e.loadApplyNodes(workerID, cci, nodes)
			a := make(map[uint64]struct{})
			if err := e.processApplies(a, nodes, batch, entries); err != nil {
				panicNow(err)
			}
			count++
			if count%200 == 0 {
				batch = make([]rsm.Task, 0, taskBatchSize)
				entries = make([]sm.Entry, 0, taskBatchSize)
			}
		case <-e.applyCCIReady.waitCh(workerID):
			nodes, cci = e.loadApplyNodes(workerID, cci, nodes)
		case <-e.applyWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = e.loadApplyNodes(workerID, cci, nodes)
			}
			a := e.applyWorkReady.getReadyMap(workerID)
			if err := e.processApplies(a, nodes, batch, entries); err != nil {
				panicNow(err)
			}
		}
	}
}

func (e *engine) loadApplyNodes(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return e.load(workerID, cci, nodes, fromApplyWorker, e.applyWorkReady)
}

// S: save snapshot
// R: recover from snapshot
// existing op, new op, action
// S, S, ignore the new op
// S, R, R is queued as node state, will be handled when S is done
// R, R, won't happen, when in R state, processApplies will not process the node
// R, S, won't happen, when in R state, processApplies will not process the node

func (e *engine) processApplies(idmap map[uint64]struct{},
	nodes map[uint64]*node, batch []rsm.Task, entries []sm.Entry) error {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	for shardID := range idmap {
		node, ok := nodes[shardID]
		if !ok || node.stopped() {
			continue
		}
		if node.processStatusTransition() {
			continue
		}
		task, err := node.handleTask(batch, entries)
		if err != nil {
			return err
		}
		if task.IsSnapshotTask() {
			node.handleSnapshotTask(task)
		}
	}
	return nil
}

func (e *engine) stepWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	cci := uint64(0)
	stopC := e.nodeStopper.ShouldStop()
	updates := make([]pb.Update, 0)
	for {
		select {
		case <-stopC:
			e.offloadNodeMap(nodes)
			return
		case <-ticker.C:
			nodes, cci = e.loadStepNodes(workerID, cci, nodes)
			a := make(map[uint64]struct{})
			if err := e.processSteps(workerID, a, nodes, updates, stopC); err != nil {
				panicNow(err)
			}
		case <-e.stepCCIReady.waitCh(workerID):
			nodes, cci = e.loadStepNodes(workerID, cci, nodes)
		case <-e.stepWorkReady.waitCh(workerID):
			if cci == 0 || len(nodes) == 0 {
				nodes, cci = e.loadStepNodes(workerID, cci, nodes)
			}
			a := e.stepWorkReady.getReadyMap(workerID)
			if err := e.processSteps(workerID, a, nodes, updates, stopC); err != nil {
				panicNow(err)
			}
		}
	}
}

func (e *engine) loadStepNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return e.load(workerID, cci, nodes, fromStepWorker, e.stepWorkReady)
}

func (e *engine) loadBucketNodes(workerID uint64,
	csi uint64, nodes map[uint64]*node, partitioner server.IPartitioner,
	from from) (map[uint64]*node, []*node, uint64) {
	bucket := workerID - 1
	newCSI := e.nh.getShardSetIndex()
	var offloaded []*node
	if newCSI != csi {
		newNodes := make(map[uint64]*node)
		loaded := make([]*node, 0)
		newCSI = e.nh.forEachShard(func(cid uint64, v *node) bool {
			if n, ok := nodes[cid]; ok {
				if n.instanceID != v.instanceID {
					plog.Panicf("%s from two incarnations found", n.id())
				}
			} else {
				if partitioner.GetPartitionID(cid) == bucket {
					loaded = append(loaded, v)
				}
			}
			if partitioner.GetPartitionID(cid) == bucket {
				newNodes[cid] = v
			}
			return true
		})
		for cid, node := range nodes {
			if _, ok := newNodes[cid]; !ok {
				offloaded = append(offloaded, node)
			}
		}
		for _, n := range loaded {
			n.loaded()
		}
		return newNodes, offloaded, newCSI
	}
	return nodes, offloaded, csi
}

func (e *engine) processSteps(workerID uint64,
	active map[uint64]struct{},
	nodes map[uint64]*node, nodeUpdates []pb.Update, stopC chan struct{}) error {
	if len(nodes) == 0 {
		return nil
	}
	if len(active) == 0 {
		for cid := range nodes {
			active[cid] = struct{}{}
		}
	}
	nodeUpdates = nodeUpdates[:0]
	for cid := range active {
		node, ok := nodes[cid]
		if !ok || node.stopped() {
			continue
		}
		ud, hasUpdate, err := node.stepNode()
		if err != nil {
			return err
		}
		if hasUpdate {
			nodeUpdates = append(nodeUpdates, ud)
		}
	}
	if err := e.applySnapshotAndUpdate(nodeUpdates, nodes, true); err != nil {
		return err
	}
	// see raft thesis section 10.2.1 on details why we send Replicate message
	// before those entries are persisted to disk
	for _, ud := range nodeUpdates {
		node := nodes[ud.ShardID]
		node.sendReplicateMessages(ud)
		node.processReadyToRead(ud)
		node.processDroppedEntries(ud)
		node.processDroppedReadIndexes(ud)
		node.processLogQuery(ud.LogQueryResult)
		node.processLeaderUpdate(ud.LeaderUpdate)
	}
	if err := e.logdb.SaveRaftState(nodeUpdates, workerID); err != nil {
		return err
	}
	if err := e.onSnapshotSaved(nodeUpdates, nodes); err != nil {
		return err
	}
	if err := e.applySnapshotAndUpdate(nodeUpdates, nodes, false); err != nil {
		return err
	}
	for _, ud := range nodeUpdates {
		node := nodes[ud.ShardID]
		if err := node.processRaftUpdate(ud); err != nil {
			return err
		}
		e.processMoreCommittedEntries(ud)
		node.commitRaftUpdate(ud)
	}
	if lazyFreeCycle > 0 {
		resetNodeUpdate(nodeUpdates)
	}
	return nil
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

func (e *engine) processMoreCommittedEntries(ud pb.Update) {
	if ud.MoreCommittedEntries {
		e.setStepReady(ud.ShardID)
	}
}

func (e *engine) applySnapshotAndUpdate(updates []pb.Update,
	nodes map[uint64]*node, fastApply bool) error {
	notifyCommit := false
	for _, ud := range updates {
		if ud.FastApply != fastApply {
			continue
		}
		node := nodes[ud.ShardID]
		if node.notifyCommit {
			notifyCommit = true
		}
		if err := node.processSnapshot(ud); err != nil {
			return err
		}
		node.applyRaftUpdates(ud)
	}
	if !notifyCommit {
		e.setApplyReadyByUpdates(updates)
	} else {
		e.setCommitReadyByUpdates(updates)
	}
	return nil
}

func (e *engine) onSnapshotSaved(updates []pb.Update,
	nodes map[uint64]*node) error {
	for _, ud := range updates {
		if !pb.IsEmptySnapshot(ud.Snapshot) {
			node := nodes[ud.ShardID]
			if err := node.removeSnapshotFlagFile(ud.Snapshot.Index); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *engine) setCloseReady(n *node) {
	e.cp.ready <- closeReq{node: n}
}

func (e *engine) setStepReadyByMessageBatch(mb pb.MessageBatch) {
	e.stepWorkReady.shardReadyByMessageBatch(mb)
}

func (e *engine) setAllStepReady(nodes []*node) {
	e.stepWorkReady.allShardsReady(nodes)
}

func (e *engine) setStepReady(shardID uint64) {
	e.stepWorkReady.shardReady(shardID)
}

func (e *engine) setCommitReadyByUpdates(updates []pb.Update) {
	e.commitWorkReady.shardReadyByUpdates(updates)
}

func (e *engine) setCommitReady(shardID uint64) {
	e.commitWorkReady.shardReady(shardID)
}

func (e *engine) setApplyReadyByUpdates(updates []pb.Update) {
	e.applyWorkReady.shardReadyByUpdates(updates)
}

func (e *engine) setApplyReady(shardID uint64) {
	e.applyWorkReady.shardReady(shardID)
}

func (e *engine) setStreamReady(shardID uint64) {
	e.wp.streamReady.shardReady(shardID)
}

func (e *engine) setSaveReady(shardID uint64) {
	e.wp.saveReady.shardReady(shardID)
}

func (e *engine) setRecoverReady(shardID uint64) {
	e.wp.recoverReady.shardReady(shardID)
}

func (e *engine) setCCIReady(shardID uint64) {
	e.stepCCIReady.shardReady(shardID)
	e.commitCCIReady.shardReady(shardID)
	e.applyCCIReady.shardReady(shardID)
	e.wp.cciReady.shardReady(shardID)
}

func (e *engine) offloadNodeMap(nodes map[uint64]*node) {
	for _, node := range nodes {
		node.offloaded()
	}
}
