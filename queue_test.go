// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

// +build !dragonboat_slowtest,!dragonboat_errorinjectiontest

package dragonboat

import (
	"testing"

	"github.com/lni/dragonboat/raftpb"
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
	q.get()
	q.get()
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
	q.get()
	q.get()
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
	q.get()
	q.get()
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get()
	q.get()
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get()
	q.get()
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd != nil {
			t.Errorf("data not freed at the expected cycle")
		}
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
		ok, stopped := q.add(raftpb.Entry{Index: uint64(i + 1)})
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
		if r[i].Index != uint64(i+1) {
			t.Errorf("index %d, want %d", r[i].Index, uint64(i+1))
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
	r := q.get()
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
			r := q.get()
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

func TestClusterCanBeSetAsReady(t *testing.T) {
	rc := newReadyCluster()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setClusterReady(1)
	rc.setClusterReady(2)
	rc.setClusterReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	_, ok := rc.ready[1]
	if !ok {
		t.Errorf("cluster 1 not set as ready")
	}
	_, ok = rc.ready[2]
	if !ok {
		t.Errorf("cluster 2 not set as ready")
	}
}

func TestReadyClusterCanBeReturnedAndCleared(t *testing.T) {
	rc := newReadyCluster()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setClusterReady(1)
	rc.setClusterReady(2)
	rc.setClusterReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	r := rc.getReadyClusters()
	if len(r) != 2 {
		t.Errorf("ready map sz %d, want 2", len(r))
	}
	if len(rc.ready) != 0 {
		t.Errorf("cluster ready map not cleared")
	}
	r = rc.getReadyClusters()
	if len(r) != 0 {
		t.Errorf("cluster ready map not cleared")
	}
	rc.setClusterReady(4)
	r = rc.getReadyClusters()
	if len(r) != 1 {
		t.Errorf("cluster ready not set")
	}
}
