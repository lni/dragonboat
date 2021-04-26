// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"sync/atomic"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

type delayed struct {
	m    pb.Message
	tick uint64
}

// MessageQueue is the queue used to hold Raft messages.
type MessageQueue struct {
	ch            chan struct{}
	rl            *RateLimiter
	left          []pb.Message
	right         []pb.Message
	nodrop        []pb.Message
	leftInWrite   bool
	stopped       bool
	delayed       []delayed
	tick          uint64
	size          uint64
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
		left:          make([]pb.Message, size),
		right:         make([]pb.Message, size),
		nodrop:        make([]pb.Message, 0),
		delayed:       make([]delayed, 0),
	}
	if ch {
		q.ch = make(chan struct{}, 1)
	}
	return q
}

// Tick increases the internal tick value.
func (q *MessageQueue) Tick() {
	atomic.AddUint64(&q.tick, 1)
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

func (q *MessageQueue) targetQueue() []pb.Message {
	var t []pb.Message
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

// Add adds the specified message to the queue.
func (q *MessageQueue) Add(msg pb.Message) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	if !q.tryAdd(msg) {
		return false, false
	}
	w := q.targetQueue()
	w[q.idx] = msg
	q.idx++
	return true, false
}

// AddDelayed adds the specified message to the queue and makes sure that the
// message will stay in the queue for at least delay ticks.
func (q *MessageQueue) AddDelayed(msg pb.Message, delay uint64) bool {
	if msg.Type != pb.SnapshotStatus {
		panic("not a snapshot status message")
	}
	tick := atomic.LoadUint64(&q.tick)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	q.delayed = append(q.delayed, delayed{msg, delay + tick})
	return true
}

// MustAdd adds the specified message to the queue.
func (q *MessageQueue) MustAdd(msg pb.Message) bool {
	if msg.CanDrop() {
		panic("not a snapshot or unreachable message")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	q.nodrop = append(q.nodrop, msg)
	return true
}

func (q *MessageQueue) tryAdd(msg pb.Message) bool {
	if !q.rl.Enabled() || msg.Type != pb.Replicate {
		return true
	}
	if q.rl.RateLimited() {
		plog.Warningf("rate limited dropped a Replicate msg from %d", msg.ClusterId)
		return false
	}
	q.rl.Increase(pb.GetEntrySliceInMemSize(msg.Entries))
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

func (q *MessageQueue) getDelayed() []pb.Message {
	if len(q.delayed) == 0 {
		return nil
	}
	sz := len(q.delayed)
	var result []pb.Message
	tick := atomic.LoadUint64(&q.tick)
	for idx, rec := range q.delayed {
		if rec.tick < tick {
			result = append(result, rec.m)
		} else {
			q.delayed[idx-len(result)] = rec
		}
	}
	if len(result) > 0 {
		q.delayed = q.delayed[:sz-len(result)]
	}
	return result
}

// Get returns everything current in the queue.
func (q *MessageQueue) Get() []pb.Message {
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
	if len(q.nodrop) == 0 && len(q.delayed) == 0 {
		return t[:sz]
	}

	var result []pb.Message
	if len(q.nodrop) > 0 {
		ssm := q.nodrop
		q.nodrop = make([]pb.Message, 0)
		result = append(result, ssm...)
	}
	delayed := q.getDelayed()
	if len(delayed) > 0 {
		result = append(result, delayed...)
	}
	return append(result, t[:sz]...)
}
