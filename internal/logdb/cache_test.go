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
	if len(c.nodeInfo) != 0 {
		t.Errorf("unexpected map len")
	}
	toSet := c.setNodeInfo(100, 2)
	if !toSet {
		t.Errorf("unexpected return value %t", toSet)
	}
	toSet = c.setNodeInfo(100, 2)
	if toSet {
		t.Errorf("unexpected return value %t", toSet)
	}
	if len(c.nodeInfo) != 1 {
		t.Errorf("unexpected map len")
	}
	ni := raftio.NodeInfo{
		ShardID:   100,
		ReplicaID: 2,
	}
	_, ok := c.nodeInfo[ni]
	if !ok {
		t.Errorf("node info not set")
	}
}

func TestCachedStateCanBeSet(t *testing.T) {
	c := newCache()
	if len(c.ps) != 0 {
		t.Errorf("unexpected savedState len %d", len(c.ps))
	}
	st := pb.State{
		Term:   1,
		Vote:   2,
		Commit: 3,
	}
	toSet := c.setState(100, 2, st)
	if !toSet {
		t.Errorf("unexpected return value")
	}
	toSet = c.setState(100, 2, st)
	if toSet {
		t.Errorf("unexpected return value")
	}
	st.Term = 3
	toSet = c.setState(100, 2, st)
	if !toSet {
		t.Errorf("unexpected return value")
	}
	if len(c.ps) != 1 {
		t.Errorf("unexpected savedState len %d", len(c.ps))
	}
	ni := raftio.NodeInfo{
		ShardID:   100,
		ReplicaID: 2,
	}
	v, ok := c.ps[ni]
	if !ok {
		t.Errorf("unexpected savedState map value")
	}
	if !reflect.DeepEqual(&v, &st) {
		t.Errorf("unexpected persistent state values")
	}
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
	if !ok {
		t.Errorf("last batch not returned")
	}
	if len(reb.Entries) != 15 {
		t.Errorf("unexpected entry length")
	}
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
	if len(v.Entries) != 15 {
		t.Errorf("unexpected entry count")
	}
	for i := uint64(0); i < uint64(15); i++ {
		v.Entries[i].Term = 2
	}
	v2, _ := c.getLastBatch(10, 2, pb.EntryBatch{})
	for i := uint64(0); i < uint64(15); i++ {
		if v2.Entries[i].Term == 2 {
			t.Errorf("cache content changed")
		}
	}
}

func TestMaxIndexCanBeSetAndGet(t *testing.T) {
	c := newCache()
	c.setMaxIndex(10, 10, 100)
	v, ok := c.getMaxIndex(10, 10)
	if !ok {
		t.Fatalf("failed to get max index")
	}
	if v != 100 {
		t.Errorf("unexpected max index, got %d, want 100", v)
	}
}
