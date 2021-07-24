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
	"sync"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	minBatchedClient uint64 = 8
)

type queueCloseFn func(e pb.Entry)

type entryQueue struct {
	size          uint64
	left          []pb.Entry
	right         []pb.Entry
	leftInWrite   bool
	stopped       bool
	paused        bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
	queueCloseFn  queueCloseFn
	clientCount   *uint64
	currentTick   uint64
	forcedTick    uint64
}

func newEntryQueue(size uint64,
	lazyFreeCycle uint64, fn queueCloseFn, clientCount *uint64) *entryQueue {
	return &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]pb.Entry, size),
		right:         make([]pb.Entry, size),
		queueCloseFn:  fn,
		clientCount:   clientCount,
	}
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *entryQueue) tick() {
	q.currentTick++
}

func (q *entryQueue) release() {
	if q.queueCloseFn != nil {
		entries := q.getAll()
		for _, e := range entries {
			q.queueCloseFn(e)
		}
	}
}

func (q *entryQueue) targetQueue() []pb.Entry {
	if q.leftInWrite {
		return q.left
	}
	return q.right
}

func (q *entryQueue) add(ent pb.Entry) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.paused || q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = ent
	q.idx++
	return true, false
}

func (q *entryQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].Cmd = nil
				oldq[i].Request = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].Cmd = nil
				oldq[i].Request = nil
			}
		}
	}
}

func (q *entryQueue) notManyClients() bool {
	return atomic.LoadUint64(q.clientCount) < minBatchedClient
}

func (q *entryQueue) forced() bool {
	return q.currentTick-q.forcedTick > 1
}

func (q *entryQueue) batchReady() bool {
	if q.clientCount == nil || q.notManyClients() || q.forced() {
		return true
	}
	batched := q.targetQueue()[:q.idx]
	if len(batched) == 0 {
		return false
	}
	if uint64(len(batched)) >= (q.size * 9 / 10) {
		return true
	}
	cc := atomic.LoadUint64(q.clientCount)
	return uint64(len(batched)) >= (cc * 9 / 10)
}

func (q *entryQueue) get(paused bool) []pb.Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.getLocked(paused, false)
}

func (q *entryQueue) getAll() []pb.Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.getLocked(false, true)
}

func (q *entryQueue) getLocked(paused bool, forced bool) []pb.Entry {
	q.paused = paused
	if !forced && !q.batchReady() {
		return nil
	}
	t := q.targetQueue()
	q.forcedTick = q.currentTick
	q.cycle++
	sz := q.idx
	q.idx = 0
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	return t[:sz]
}

type readIndexQueue struct {
	size        uint64
	left        []*RequestState
	right       []*RequestState
	leftInWrite bool
	stopped     bool
	idx         uint64
	mu          sync.Mutex
}

func newReadIndexQueue(size uint64) *readIndexQueue {
	return &readIndexQueue{
		size:  size,
		left:  make([]*RequestState, size),
		right: make([]*RequestState, size),
	}
}

func (q *readIndexQueue) pendingSize() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.idx
}

func (q *readIndexQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *readIndexQueue) targetQueue() []*RequestState {
	if q.leftInWrite {
		return q.left
	}
	return q.right
}

func (q *readIndexQueue) add(rs *RequestState) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = rs
	q.idx++
	return true, false
}

func (q *readIndexQueue) get() []*RequestState {
	q.mu.Lock()
	defer q.mu.Unlock()
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	return t[:sz]
}

type readyCluster struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

func newReadyCluster() *readyCluster {
	r := &readyCluster{}
	r.maps[0] = make(map[uint64]struct{})
	r.maps[1] = make(map[uint64]struct{})
	r.ready = r.maps[0]
	return r
}

func (r *readyCluster) setClusterReady(clusterID uint64) {
	r.mu.Lock()
	r.ready[clusterID] = struct{}{}
	r.mu.Unlock()
}

func (r *readyCluster) getReadyClusters() map[uint64]struct{} {
	m := r.maps[(r.index+1)%2]
	for k := range m {
		delete(m, k)
	}
	r.mu.Lock()
	v := r.ready
	r.index++
	r.ready = r.maps[r.index%2]
	r.mu.Unlock()
	return v
}

type leaderInfoQueue struct {
	mu            sync.Mutex
	notifications []raftio.LeaderInfo
	workCh        chan struct{}
}

func newLeaderInfoQueue() *leaderInfoQueue {
	return &leaderInfoQueue{
		workCh:        make(chan struct{}, 1),
		notifications: make([]raftio.LeaderInfo, 0),
	}
}

func (li *leaderInfoQueue) workReady() chan struct{} {
	return li.workCh
}

func (li *leaderInfoQueue) addLeaderInfo(info raftio.LeaderInfo) {
	func() {
		li.mu.Lock()
		defer li.mu.Unlock()
		li.notifications = append(li.notifications, info)
	}()
	select {
	case li.workCh <- struct{}{}:
	default:
	}
}

func (li *leaderInfoQueue) getLeaderInfo() (raftio.LeaderInfo, bool) {
	li.mu.Lock()
	defer li.mu.Unlock()
	if len(li.notifications) > 0 {
		v := li.notifications[0]
		li.notifications = li.notifications[1:]
		return v, true
	}
	return raftio.LeaderInfo{}, false
}
