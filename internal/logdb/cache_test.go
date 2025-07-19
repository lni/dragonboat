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

package logdb

import (
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

func TestTrySaveSnapshot(t *testing.T) {
	c := newCache()
	require.True(t, c.trySaveSnapshot(1, 1, 10))
	require.False(t, c.trySaveSnapshot(1, 1, 10))
	require.False(t, c.trySaveSnapshot(1, 1, 9))
	require.True(t, c.trySaveSnapshot(1, 1, 11))
	require.True(t, c.trySaveSnapshot(1, 2, 10))
}

func TestCachedNodeInfoCanBeSet(t *testing.T) {
	c := newCache()
	require.Equal(t, 0, len(c.nodeInfo), "unexpected map len")

	toSet := c.setNodeInfo(100, 2)
	require.True(t, toSet, "unexpected return value %t", toSet)

	toSet = c.setNodeInfo(100, 2)
	require.False(t, toSet, "unexpected return value %t", toSet)

	require.Equal(t, 1, len(c.nodeInfo), "unexpected map len")

	ni := raftio.NodeInfo{
		ShardID:   100,
		ReplicaID: 2,
	}
	_, ok := c.nodeInfo[ni]
	require.True(t, ok, "node info not set")
}

func TestCachedStateCanBeSet(t *testing.T) {
	c := newCache()
	require.Equal(t, 0, len(c.ps), "unexpected savedState len %d", len(c.ps))

	st := pb.State{
		Term:   1,
		Vote:   2,
		Commit: 3,
	}

	toSet := c.setState(100, 2, st)
	require.True(t, toSet, "unexpected return value")

	toSet = c.setState(100, 2, st)
	require.False(t, toSet, "unexpected return value")

	st.Term = 3
	toSet = c.setState(100, 2, st)
	require.True(t, toSet, "unexpected return value")

	require.Equal(t, 1, len(c.ps), "unexpected savedState len %d", len(c.ps))

	ni := raftio.NodeInfo{
		ShardID:   100,
		ReplicaID: 2,
	}
	v, ok := c.ps[ni]
	require.True(t, ok, "unexpected savedState map value")
	require.True(t, reflect.DeepEqual(&v, &st),
		"unexpected persistent state values")
}

func TestLastEntryBatchCanBeSetAndGet(t *testing.T) {
	c := newCache()
	eb := pb.EntryBatch{Entries: make([]pb.Entry, 0)}
	for i := uint64(1); i < uint64(16); i++ {
		eb.Entries = append(eb.Entries, pb.Entry{Index: i, Term: i})
	}

	c.setLastBatch(10, 2, eb)
	lb := pb.EntryBatch{}
	reb, ok := c.getLastBatch(10, 2, lb)
	require.True(t, ok, "last batch not returned")
	require.Equal(t, 15, len(reb.Entries), "unexpected entry length")
}

func TestLastEntryBatchCanBeUpdated(t *testing.T) {
	c := newCache()
	eb := pb.EntryBatch{Entries: make([]pb.Entry, 0)}
	for i := uint64(1); i < uint64(16); i++ {
		eb.Entries = append(eb.Entries, pb.Entry{Index: i, Term: i})
	}

	eb2 := pb.EntryBatch{Entries: make([]pb.Entry, 0)}
	for i := uint64(100); i < uint64(116); i++ {
		eb2.Entries = append(eb2.Entries, pb.Entry{Index: i, Term: i})
	}

	c.setLastBatch(10, 2, eb)
	c.setLastBatch(10, 2, eb2)
}

func TestChangeReturnedLastBatchWillNotAffectTheCache(t *testing.T) {
	c := newCache()
	eb := pb.EntryBatch{Entries: make([]pb.Entry, 0)}
	for i := uint64(1); i < uint64(16); i++ {
		eb.Entries = append(eb.Entries, pb.Entry{Index: i, Term: 1})
	}

	c.setLastBatch(10, 2, eb)
	v, _ := c.getLastBatch(10, 2, pb.EntryBatch{})
	require.Equal(t, 15, len(v.Entries), "unexpected entry count")

	for i := uint64(0); i < uint64(15); i++ {
		v.Entries[i].Term = 2
	}

	v2, _ := c.getLastBatch(10, 2, pb.EntryBatch{})
	for i := uint64(0); i < uint64(15); i++ {
		require.NotEqual(t, uint64(2), v2.Entries[i].Term,
			"cache content changed")
	}
}

func TestMaxIndexCanBeSetAndGet(t *testing.T) {
	c := newCache()
	c.setMaxIndex(10, 10, 100)
	v, ok := c.getMaxIndex(10, 10)
	require.True(t, ok, "failed to get max index")
	require.Equal(t, uint64(100), v, "unexpected max index, got %d, want 100", v)
}
