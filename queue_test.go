// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"testing"

	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
)

func TestEntryQueueCanBeCreated(t *testing.T) {
	q := newEntryQueue(5, 0)
	if q.size != 5 || len(q.left) != 5 || len(q.right) != 5 {
		t.Errorf("size unexpected")
	}
	if q.idx != 0 {
		t.Errorf("idx %d, want 0", q.idx)
	}
}

func TestLazyFreeCanBeDisabled(t *testing.T) {
	q := newEntryQueue(5, 0)
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.get(false)
	q.get(false)
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
}

func TestLazyFreeCanBeUsed(t *testing.T) {
	q := newEntryQueue(5, 1)
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.get(false)
	q.get(false)
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd != nil {
			t.Errorf("data unexpectedly not freed")
		}
	}
}

func TestLazyFreeCycleCanBeSet(t *testing.T) {
	q := newEntryQueue(5, 6)
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.add(raftpb.Entry{Cmd: make([]byte, 16)})
	q.get(false)
	q.get(false)
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get(false)
	q.get(false)
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get(false)
	q.get(false)
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd != nil {
			t.Errorf("data not freed at the expected cycle")
		}
	}
}

func TestEntryQueueCanBePaused(t *testing.T) {
	q := newEntryQueue(5, 0)
	if q.paused {
		t.Errorf("entry queue is paused by default")
	}
	for i := 0; i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.stopped {
			t.Errorf("stopped too early")
		}
	}
	v := q.get(true)
	if len(v) != 5 {
		t.Errorf("failed to get all entries")
	}
	if !q.paused {
		t.Errorf("not paused")
	}
	ok, stopped := q.add(raftpb.Entry{})
	if ok {
		t.Errorf("entry added to paused queue")
	}
	if stopped {
		t.Errorf("entry queue unexpectedly stopped")
	}
}

func TestEntryQueueCanBeClosed(t *testing.T) {
	q := newEntryQueue(5, 0)
	if q.stopped {
		t.Errorf("entry queue is stopped by default")
	}
	for i := 0; i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.stopped {
			t.Errorf("stopped too early")
		}
	}
	ok, _ := q.add(raftpb.Entry{})
	if ok {
		t.Errorf("not expect to add more")
	}
	q = newEntryQueue(5, 0)
	q.close()
	if !q.stopped {
		t.Errorf("entry queue is not marked as stopped")
	}
	if q.idx != 0 {
		t.Errorf("idx %d, want 0", q.idx)
	}
	ok, stopped := q.add(raftpb.Entry{})
	if ok {
		t.Errorf("not expect to add more")
	}
	if !stopped {
		t.Errorf("stopped flag is not returned")
	}
}

func TestEntryQueueAllowEntriesToBeAdded(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := uint64(0); i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: i + 1})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.idx != i+1 {
			t.Errorf("idx %d, want %d", q.idx, i+1)
		}
		var r []raftpb.Entry
		if q.leftInWrite {
			r = q.left
		} else {
			r = q.right
		}
		if r[i].Index != i+1 {
			t.Errorf("index %d, want %d", r[i].Index, i+1)
		}
	}
}

func TestEntryQueueAllowAddedEntriesToBeReturned(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := 0; i < 3; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: uint64(i + 1)})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
	}
	r := q.get(false)
	if len(r) != 3 {
		t.Errorf("len %d, want %d", len(r), 3)
	}
	if q.idx != 0 {
		t.Errorf("idx %d, want %d", q.idx, 0)
	}
	// check whether we can keep adding entries as long as we keep getting
	// previously written entries.
	expectedIndex := uint64(1)
	q = newEntryQueue(5, 0)
	for i := 0; i < 1000; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: uint64(i + 1)})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.idx == q.size {
			r := q.get(false)
			if len(r) != 5 {
				t.Errorf("len %d, want %d", len(r), 5)
			}
			for _, e := range r {
				if e.Index != expectedIndex {
					t.Errorf("index %d, expected %d", e.Index, expectedIndex)
				}
				expectedIndex++
			}
		}
	}
}

func TestShardCanBeSetAsReady(t *testing.T) {
	rc := newReadyShard()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setShardReady(1)
	rc.setShardReady(2)
	rc.setShardReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	_, ok := rc.ready[1]
	if !ok {
		t.Errorf("shard 1 not set as ready")
	}
	_, ok = rc.ready[2]
	if !ok {
		t.Errorf("shard 2 not set as ready")
	}
}

func TestReadyShardCanBeReturnedAndCleared(t *testing.T) {
	rc := newReadyShard()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setShardReady(1)
	rc.setShardReady(2)
	rc.setShardReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	r := rc.getReadyShards()
	if len(r) != 2 {
		t.Errorf("ready map sz %d, want 2", len(r))
	}
	if len(rc.ready) != 0 {
		t.Errorf("shard ready map not cleared")
	}
	r = rc.getReadyShards()
	if len(r) != 0 {
		t.Errorf("shard ready map not cleared")
	}
	rc.setShardReady(4)
	r = rc.getReadyShards()
	if len(r) != 1 {
		t.Errorf("shard ready not set")
	}
}

func TestLeaderInfoQueueCanBeCreated(t *testing.T) {
	q := newLeaderInfoQueue()
	if cap(q.workCh) != 1 {
		t.Errorf("unexpected queue cap")
	}
	if len(q.workCh) != 0 {
		t.Errorf("unexpected queue length")
	}
	if len(q.notifications) != 0 {
		t.Errorf("unexpected notifications length")
	}
	ch := q.workReady()
	if ch == nil {
		t.Errorf("failed to return work ready chan")
	}
}

func TestAddToLeaderInfoQueue(t *testing.T) {
	q := newLeaderInfoQueue()
	q.addLeaderInfo(raftio.LeaderInfo{})
	q.addLeaderInfo(raftio.LeaderInfo{})
	if len(q.workCh) != 1 {
		t.Errorf("unexpected workCh len")
	}
	if len(q.notifications) != 2 {
		t.Errorf("unexpected notifications len")
	}
}

func TestGetFromLeaderInfoQueue(t *testing.T) {
	q := newLeaderInfoQueue()
	_, ok := q.getLeaderInfo()
	if ok {
		t.Errorf("unexpectedly returned leader info")
	}
	v1 := raftio.LeaderInfo{ShardID: 101}
	v2 := raftio.LeaderInfo{ShardID: 2002}
	q.addLeaderInfo(v1)
	q.addLeaderInfo(v2)
	rv1, ok1 := q.getLeaderInfo()
	rv2, ok2 := q.getLeaderInfo()
	_, ok3 := q.getLeaderInfo()
	if !ok1 || rv1.ShardID != v1.ShardID {
		t.Errorf("unexpected result")
	}
	if !ok2 || rv2.ShardID != v2.ShardID {
		t.Errorf("unexpected result")
	}
	if ok3 {
		t.Errorf("unexpectedly return third reader info rec")
	}
}

func TestReadIndexQueueCanHandleAddFailure(t *testing.T) {
	q := newReadIndexQueue(1)
	added, stopped := q.add(&RequestState{})
	if !added || stopped {
		t.Errorf("unexpected failure")
	}
	added, stopped = q.add(&RequestState{})
	if added {
		t.Errorf("unexpectedly added the rs")
	}
	if stopped {
		t.Errorf("unexpectedly reported state as stopped")
	}
	q.close()
	added, stopped = q.add(&RequestState{})
	if added {
		t.Errorf("unexpectedly added the rs")
	}
	if !stopped {
		t.Errorf("failed to report state as stopped")
	}

}
