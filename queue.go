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

	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

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
}

func newEntryQueue(size uint64, lazyFreeCycle uint64) *entryQueue {
	e := &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]pb.Entry, size),
		right:         make([]pb.Entry, size),
	}
	return e
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *entryQueue) targetQueue() []pb.Entry {
	var t []pb.Entry
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

func (q *entryQueue) add(ent pb.Entry) (bool, bool) {
	q.mu.Lock()
	if q.paused || q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = ent
	q.idx++
	q.mu.Unlock()
	return true, false
}

func (q *entryQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].Cmd = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].Cmd = nil
			}
		}
	}
}

func (q *entryQueue) get(paused bool) []pb.Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.paused = paused
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
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
	e := &readIndexQueue{
		size:  size,
		left:  make([]*RequestState, size),
		right: make([]*RequestState, size),
	}
	return e
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
	var t []*RequestState
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

func (q *readIndexQueue) add(rs *RequestState) (bool, bool) {
	q.mu.Lock()
	if q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = rs
	q.idx++
	q.mu.Unlock()
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
	r.mu.Lock()
	v := r.ready
	r.index++
	selected := r.index % 2
	nm := r.maps[selected]
	for k := range nm {
		delete(nm, k)
	}
	r.ready = nm
	r.mu.Unlock()
	return v
}

type leaderInfoQueue struct {
	mu            sync.Mutex
	notifications []raftio.LeaderInfo
	workCh        chan struct{}
}

func newLeaderInfoQueue() *leaderInfoQueue {
	q := &leaderInfoQueue{
		workCh:        make(chan struct{}, 1),
		notifications: make([]raftio.LeaderInfo, 0),
	}
	return q
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
