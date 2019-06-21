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

package server

import (
	"sync"

	"github.com/lni/dragonboat/v3/raftpb"
)

// MessageQueue is the queue used to hold Raft messages.
type MessageQueue struct {
	size          uint64
	ch            chan struct{}
	rl            *RateLimiter
	left          []raftpb.Message
	right         []raftpb.Message
	snapshot      []raftpb.Message
	leftInWrite   bool
	stopped       bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
}

// NewMessageQueue creates a new MessageQueue instance.
func NewMessageQueue(size uint64,
	ch bool, lazyFreeCycle uint64, maxMemorySize uint64) *MessageQueue {
	q := &MessageQueue{
		rl:            NewRateLimiter(maxMemorySize),
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]raftpb.Message, size),
		right:         make([]raftpb.Message, size),
		snapshot:      make([]raftpb.Message, 0),
	}
	if ch {
		q.ch = make(chan struct{}, 1)
	}
	return q
}

// Close closes the queue so no further messages can be added.
func (q *MessageQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

// Notify notifies the notification channel listener that a new message is now
// available in the queue.
func (q *MessageQueue) Notify() {
	if q.ch != nil {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
}

// Ch returns the notification channel.
func (q *MessageQueue) Ch() <-chan struct{} {
	return q.ch
}

func (q *MessageQueue) targetQueue() []raftpb.Message {
	var t []raftpb.Message
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

// Add adds the specified message to the queue.
func (q *MessageQueue) Add(msg raftpb.Message) (bool, bool) {
	q.mu.Lock()
	if q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	if !q.tryAdd(msg) {
		return false, false
	}
	w := q.targetQueue()
	w[q.idx] = msg
	q.idx++
	q.mu.Unlock()
	return true, false
}

// AddSnapshot adds the specified snapshot to the queue.
func (q *MessageQueue) AddSnapshot(msg raftpb.Message) bool {
	if msg.Type != raftpb.InstallSnapshot {
		panic("not a snapshot message")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	q.snapshot = append(q.snapshot, msg)
	return true
}

func (q *MessageQueue) tryAdd(msg raftpb.Message) bool {
	if !q.rl.Enabled() || msg.Type != raftpb.Replicate {
		return true
	}
	if q.rl.RateLimited() {
		plog.Warningf("rate limited dropped a replicate msg from %d", msg.ClusterId)
		return false
	}
	q.rl.Increase(raftpb.GetEntrySliceSize(msg.Entries))
	return true
}

func (q *MessageQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].Entries = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].Entries = nil
			}
		}
	}
}

// Get returns everything current in the queue.
func (q *MessageQueue) Get() []raftpb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	if q.rl.Enabled() {
		q.rl.Set(0)
	}
	if len(q.snapshot) == 0 {
		return t[:sz]
	}
	ssm := q.snapshot
	q.snapshot = make([]raftpb.Message, 0)
	return append(ssm, t[:sz]...)
}
