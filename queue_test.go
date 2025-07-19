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

	"github.com/stretchr/testify/assert"

	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
)

func TestEntryQueueCanBeCreated(t *testing.T) {
	q := newEntryQueue(5, 0)
	assert.Equal(t, uint64(5), q.size, "size unexpected")
	assert.Len(t, q.left, 5, "left queue size unexpected")
	assert.Len(t, q.right, 5, "right queue size unexpected")
	assert.Equal(t, uint64(0), q.idx, "idx %d, want 0", q.idx)
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
		assert.NotNil(t, tq[i].Cmd, "data unexpectedly freed")
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
		assert.Nil(t, tq[i].Cmd, "data unexpectedly not freed")
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
		assert.NotNil(t, tq[i].Cmd, "data unexpectedly freed")
	}
	q.get(false)
	q.get(false)
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		assert.NotNil(t, tq[i].Cmd, "data unexpectedly freed")
	}
	q.get(false)
	q.get(false)
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		assert.Nil(t, tq[i].Cmd, "data not freed at the expected cycle")
	}
}

func TestEntryQueueCanBePaused(t *testing.T) {
	q := newEntryQueue(5, 0)
	assert.False(t, q.paused, "entry queue is paused by default")
	for i := 0; i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{})
		assert.True(t, ok, "failed to add new entry")
		assert.False(t, stopped, "failed to add new entry")
		assert.False(t, q.stopped, "stopped too early")
	}
	v := q.get(true)
	assert.Len(t, v, 5, "failed to get all entries")
	assert.True(t, q.paused, "not paused")
	ok, stopped := q.add(raftpb.Entry{})
	assert.False(t, ok, "entry added to paused queue")
	assert.False(t, stopped, "entry queue unexpectedly stopped")
}

func TestEntryQueueCanBeClosed(t *testing.T) {
	q := newEntryQueue(5, 0)
	assert.False(t, q.stopped, "entry queue is stopped by default")
	for i := 0; i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{})
		assert.True(t, ok, "failed to add new entry")
		assert.False(t, stopped, "failed to add new entry")
		assert.False(t, q.stopped, "stopped too early")
	}
	ok, _ := q.add(raftpb.Entry{})
	assert.False(t, ok, "not expect to add more")
	q = newEntryQueue(5, 0)
	q.close()
	assert.True(t, q.stopped, "entry queue is not marked as stopped")
	assert.Equal(t, uint64(0), q.idx, "idx %d, want 0", q.idx)
	ok, stopped := q.add(raftpb.Entry{})
	assert.False(t, ok, "not expect to add more")
	assert.True(t, stopped, "stopped flag is not returned")
}

func TestEntryQueueAllowEntriesToBeAdded(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := uint64(0); i < 5; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: i + 1})
		assert.True(t, ok, "failed to add new entry")
		assert.False(t, stopped, "failed to add new entry")
		assert.Equal(t, i+1, q.idx, "idx %d, want %d", q.idx, i+1)
		var r []raftpb.Entry
		if q.leftInWrite {
			r = q.left
		} else {
			r = q.right
		}
		assert.Equal(t, i+1, r[i].Index, "index %d, want %d",
			r[i].Index, i+1)
	}
}

func TestEntryQueueAllowAddedEntriesToBeReturned(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := 0; i < 3; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: uint64(i + 1)})
		assert.True(t, ok, "failed to add new entry")
		assert.False(t, stopped, "failed to add new entry")
	}
	r := q.get(false)
	assert.Len(t, r, 3, "len %d, want %d", len(r), 3)
	assert.Equal(t, uint64(0), q.idx, "idx %d, want %d", q.idx, 0)
	// check whether we can keep adding entries as long as we keep getting
	// previously written entries.
	expectedIndex := uint64(1)
	q = newEntryQueue(5, 0)
	for i := 0; i < 1000; i++ {
		ok, stopped := q.add(raftpb.Entry{Index: uint64(i + 1)})
		assert.True(t, ok, "failed to add new entry")
		assert.False(t, stopped, "failed to add new entry")
		if q.idx == q.size {
			r := q.get(false)
			assert.Len(t, r, 5, "len %d, want %d", len(r), 5)
			for _, e := range r {
				assert.Equal(t, expectedIndex, e.Index,
					"index %d, expected %d", e.Index, expectedIndex)
				expectedIndex++
			}
		}
	}
}

func TestShardCanBeSetAsReady(t *testing.T) {
	rc := newReadyShard()
	assert.Empty(t, rc.ready, "ready map not empty")
	rc.setShardReady(1)
	rc.setShardReady(2)
	rc.setShardReady(2)
	assert.Len(t, rc.ready, 2, "ready map sz %d, want 2", len(rc.ready))
	_, ok := rc.ready[1]
	assert.True(t, ok, "shard 1 not set as ready")
	_, ok = rc.ready[2]
	assert.True(t, ok, "shard 2 not set as ready")
}

func TestReadyShardCanBeReturnedAndCleared(t *testing.T) {
	rc := newReadyShard()
	assert.Empty(t, rc.ready, "ready map not empty")
	rc.setShardReady(1)
	rc.setShardReady(2)
	rc.setShardReady(2)
	assert.Len(t, rc.ready, 2, "ready map sz %d, want 2", len(rc.ready))
	r := rc.getReadyShards()
	assert.Len(t, r, 2, "ready map sz %d, want 2", len(r))
	assert.Empty(t, rc.ready, "shard ready map not cleared")
	r = rc.getReadyShards()
	assert.Empty(t, r, "shard ready map not cleared")
	rc.setShardReady(4)
	r = rc.getReadyShards()
	assert.Len(t, r, 1, "shard ready not set")
}

func TestLeaderInfoQueueCanBeCreated(t *testing.T) {
	q := newLeaderInfoQueue()
	assert.Equal(t, 1, cap(q.workCh), "unexpected queue cap")
	assert.Empty(t, q.workCh, "unexpected queue length")
	assert.Empty(t, q.notifications, "unexpected notifications length")
	ch := q.workReady()
	assert.NotNil(t, ch, "failed to return work ready chan")
}

func TestAddToLeaderInfoQueue(t *testing.T) {
	q := newLeaderInfoQueue()
	q.addLeaderInfo(raftio.LeaderInfo{})
	q.addLeaderInfo(raftio.LeaderInfo{})
	assert.Len(t, q.workCh, 1, "unexpected workCh len")
	assert.Len(t, q.notifications, 2, "unexpected notifications len")
}

func TestGetFromLeaderInfoQueue(t *testing.T) {
	q := newLeaderInfoQueue()
	_, ok := q.getLeaderInfo()
	assert.False(t, ok, "unexpectedly returned leader info")
	v1 := raftio.LeaderInfo{ShardID: 101}
	v2 := raftio.LeaderInfo{ShardID: 2002}
	q.addLeaderInfo(v1)
	q.addLeaderInfo(v2)
	rv1, ok1 := q.getLeaderInfo()
	rv2, ok2 := q.getLeaderInfo()
	_, ok3 := q.getLeaderInfo()
	assert.True(t, ok1, "unexpected result")
	assert.Equal(t, v1.ShardID, rv1.ShardID, "unexpected result")
	assert.True(t, ok2, "unexpected result")
	assert.Equal(t, v2.ShardID, rv2.ShardID, "unexpected result")
	assert.False(t, ok3, "unexpectedly return third reader info rec")
}

func TestReadIndexQueueCanHandleAddFailure(t *testing.T) {
	q := newReadIndexQueue(1)
	added, stopped := q.add(&RequestState{})
	assert.True(t, added, "unexpected failure")
	assert.False(t, stopped, "unexpected failure")
	added, stopped = q.add(&RequestState{})
	assert.False(t, added, "unexpectedly added the rs")
	assert.False(t, stopped, "unexpectedly reported state as stopped")
	q.close()
	added, stopped = q.add(&RequestState{})
	assert.False(t, added, "unexpectedly added the rs")
	assert.True(t, stopped, "failed to report state as stopped")
}
