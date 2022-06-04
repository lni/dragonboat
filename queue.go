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

	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
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
	return &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]pb.Entry, size),
		right:         make([]pb.Entry, size),
	}
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
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

type readyShard struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

func newReadyShard() *readyShard {
	r := &readyShard{}
	r.maps[0] = make(map[uint64]struct{})
	r.maps[1] = make(map[uint64]struct{})
	r.ready = r.maps[0]
	return r
}

func (r *readyShard) setShardReady(shardID uint64) {
	r.mu.Lock()
	r.ready[shardID] = struct{}{}
	r.mu.Unlock()
}

func (r *readyShard) getReadyShards() map[uint64]struct{} {
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
