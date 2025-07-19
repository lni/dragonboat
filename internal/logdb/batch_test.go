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

package logdb

import (
	"math"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBatchIDRange(t *testing.T) {
	tests := []struct {
		low       uint64
		high      uint64
		batchLow  uint64
		batchHigh uint64
	}{
		{2, 3, 0, 1},
		{1, batchSize, 0, 1},
		{batchSize, 2 * batchSize, 1, 2},
		{1, batchSize + 1, 0, 2},
		{batchSize, 2*batchSize + 1, 1, 3},
		{batchSize + 1, 2 * batchSize, 1, 2},
		{batchSize + 1, 2*batchSize + 1, 1, 3},
	}

	for idx, tt := range tests {
		low, high := getBatchIDRange(tt.low, tt.high)
		assert.Equal(t, tt.batchLow, low,
			"%d, low %d, want %d", idx, low, tt.batchLow)
		assert.Equal(t, tt.batchHigh, high,
			"%d, high %d, want %d", idx, high, tt.batchHigh)
	}
}

func TestEntryBatchFieldsNotCompactedWhenIndexHasGap(t *testing.T) {
	fn := func() pb.EntryBatch {
		ents := make([]pb.Entry, 0)
		for i := uint64(1); i < batchSize; i++ {
			if i == batchSize/2 {
				continue
			}
			e := pb.Entry{
				Index: i,
				Term:  2,
			}
			ents = append(ents, e)
		}
		return pb.EntryBatch{Entries: ents}
	}
	eb1 := fn()
	eb2 := fn()
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "initial value not equal")
	eb1 = compactBatchFields(eb1)
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "unexpectedly compacted")
}

func TestEntryBatchFieldsNotCompactedWhenMultipleTerms(t *testing.T) {
	fn := func() pb.EntryBatch {
		ents := make([]pb.Entry, 0)
		for i := uint64(1); i < batchSize; i++ {
			term := uint64(1)
			if i == batchSize-1 || i == batchSize-2 {
				term = uint64(2)
			}
			e := pb.Entry{
				Index: i,
				Term:  term,
			}
			ents = append(ents, e)
		}
		return pb.EntryBatch{Entries: ents}
	}
	eb1 := fn()
	eb2 := fn()
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "initial value not equal")
	eb1 = compactBatchFields(eb1)
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "unexpectedly compacted")
}

func TestEntryBatchFieldsCanBeCompacted(t *testing.T) {
	fn := func() pb.EntryBatch {
		ents := make([]pb.Entry, 0)
		for i := uint64(1); i < batchSize; i++ {
			e := pb.Entry{
				Index: i,
				Term:  1,
			}
			ents = append(ents, e)
		}
		return pb.EntryBatch{Entries: ents}
	}
	eb1 := fn()
	eb2 := fn()
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "input not equal")
	eb1 = compactBatchFields(eb1)
	assert.False(t, reflect.DeepEqual(&eb1, &eb2), "eb not changed")
	assert.Less(t, eb1.Size(), eb2.Size(), "size didn't reduce")
	for i := 0; i < len(eb1.Entries); i++ {
		if i == 0 {
			assert.NotZero(t, eb1.Entries[i].Index, "first index is 0, %+v",
				eb1.Entries)
			assert.NotZero(t, eb1.Entries[i].Term, "first term is 0, %+v",
				eb1.Entries)
		} else {
			assert.Zero(t, eb1.Entries[i].Index, "first index/term is not 0")
			assert.Zero(t, eb1.Entries[i].Term, "first index/term is not 0")
		}
	}
	eb1 = restoreBatchFields(eb1)
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "not restored")
}

func TestNotCompactedEntryBatchIsNotRestored(t *testing.T) {
	fn := func() pb.EntryBatch {
		ents := make([]pb.Entry, 0)
		for i := uint64(1); i < batchSize; i++ {
			e := pb.Entry{
				Index: i,
				Term:  1,
			}
			ents = append(ents, e)
		}
		return pb.EntryBatch{Entries: ents}
	}
	eb1 := fn()
	eb2 := fn()
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "input not equal")
	eb1 = restoreBatchFields(eb1)
	assert.True(t, reflect.DeepEqual(&eb1, &eb2), "not restored")
}

func TestCompactBatchFieldsPanicWhenBatchIsTooSmall(t *testing.T) {
	require.Panics(t, func() {
		compactBatchFields(pb.EntryBatch{})
	})
	require.Panics(t, func() {
		compactBatchFields(pb.EntryBatch{Entries: []pb.Entry{{}}})
	})
}

func TestRestoreBatchFieldsPanicWhenBatchIsTooSmall(t *testing.T) {
	require.Panics(t, func() {
		restoreBatchFields(pb.EntryBatch{})
	})
	require.Panics(t, func() {
		restoreBatchFields(pb.EntryBatch{Entries: []pb.Entry{{}}})
	})
}

func TestMergeFirstBatchPanicWhenInputBatchIsEmpty(t *testing.T) {
	empty := pb.EntryBatch{}
	nonEmpty := pb.EntryBatch{Entries: []pb.Entry{{Index: 1, Term: 1}}}
	require.Panics(t, func() {
		getMergedFirstBatch(empty, nonEmpty)
	})
	require.Panics(t, func() {
		getMergedFirstBatch(nonEmpty, empty)
	})
}

func TestMergeFirstBatchPanicWhenIncomingBatchIsNotMoreRecent(t *testing.T) {
	eb := pb.EntryBatch{Entries: []pb.Entry{{Index: batchSize, Term: 1}}}
	lb := pb.EntryBatch{Entries: []pb.Entry{{Index: 2 * batchSize, Term: 1}}}
	require.Panics(t, func() {
		getMergedFirstBatch(eb, lb)
	})
}

func TestIncomingBatchIsTheMergedBatchWhenMoreRecentThanLastBatch(t *testing.T) {
	eb := pb.EntryBatch{Entries: []pb.Entry{{Index: 2 * batchSize, Term: 1}}}
	lb := pb.EntryBatch{Entries: []pb.Entry{{Index: batchSize, Term: 1}}}
	result := getMergedFirstBatch(eb, lb)
	assert.True(t, reflect.DeepEqual(&result, &eb), "unexpected result")
}

func TestGetMergedFirstBatch(t *testing.T) {
	tests := []struct {
		ebfirst  uint64
		eblast   uint64
		lbfirst  uint64
		lblast   uint64
		mfirst   uint64
		mlast    uint64
		newindex uint64
	}{
		{1, 10, 2, 10, 1, 10, 1},
		{1, 10, 2, 11, 1, 10, 1},
		{1, 10, 2, 9, 1, 10, 1},
		{2, 10, 2, 10, 2, 10, 2},
		{2, 10, 2, 9, 2, 10, 2},
		{2, 10, 2, 11, 2, 10, 2},
		{2, 10, 1, 10, 1, 10, 2},
		{3, 10, 1, 3, 1, 10, 3},
		{3, 10, 1, 2, 1, 10, 3},
		{3, 10, 1, 4, 1, 10, 3},
	}

	for idx, tt := range tests {
		eb := pb.EntryBatch{}
		lb := pb.EntryBatch{}
		for i := tt.ebfirst; i <= tt.eblast; i++ {
			entry := pb.Entry{
				Index: i,
				Term:  2,
			}
			eb.Entries = append(eb.Entries, entry)
		}
		for i := tt.lbfirst; i <= tt.lblast; i++ {
			entry := pb.Entry{
				Index: i,
				Term:  1,
			}
			lb.Entries = append(lb.Entries, entry)
		}
		result := getMergedFirstBatch(eb, lb)
		assert.Equal(t, tt.mfirst, result.Entries[0].Index,
			"%d, first index %d, want %d", idx, result.Entries[0].Index,
			tt.mfirst)
		assert.Equal(t, tt.mlast, result.Entries[len(result.Entries)-1].Index,
			"%d, last index %d, want %d", idx,
			result.Entries[len(result.Entries)-1].Index, tt.mlast)
		for i := 0; i < len(result.Entries); i++ {
			if result.Entries[i].Index >= tt.newindex {
				assert.Equal(t, uint64(2), result.Entries[i].Term, "unexpected term")
			}
		}
	}
}

func TestEntryBatchWillNotBeMergedToPreviousBatch(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		e1 := pb.Entry{
			Term:  1,
			Index: 1,
			Type:  pb.ApplicationEntry,
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		nextIndex := 1 + batchSize
		e2 := pb.Entry{
			Term:  1,
			Index: nextIndex,
			Type:  pb.ApplicationEntry,
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		maxIndex, err := db.(*ShardedDB).shards[0].getMaxIndex(shardID,
			replicaID)
		assert.NoError(t, err, "failed to get max index")
		assert.Equal(t, nextIndex, maxIndex, "unexpected max index")
		eb, ok := db.(*ShardedDB).shards[0].entries.(*batchedEntries).
			getBatchFromDB(shardID, replicaID, 1)
		assert.True(t, ok, "failed to get the eb")
		assert.Equal(t, 1, len(eb.Entries), "unexpected len %d, want 1",
			len(eb.Entries))
		assert.Equal(t, nextIndex, eb.Entries[0].Index,
			"unexpected index %d, want 10", eb.Entries[0].Index)
	}
	fs := vfs.GetTestFS()
	runBatchedLogDBTest(t, tf, fs)
}

func TestEntryBatchMergedNotLastBatch(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		ud := pb.Update{
			EntriesToSave: make([]pb.Entry, 0),
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		for i := uint64(1); i < batchSize+4; i++ {
			e := pb.Entry{Index: i, Term: 1}
			ud.EntriesToSave = append(ud.EntriesToSave, e)
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		ud = pb.Update{
			EntriesToSave: make([]pb.Entry, 0),
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		for i := batchSize - 4; i <= batchSize+2; i++ {
			e := pb.Entry{Index: i, Term: 2}
			ud.EntriesToSave = append(ud.EntriesToSave, e)
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		maxIndex, err := db.(*ShardedDB).shards[0].getMaxIndex(shardID,
			replicaID)
		assert.NoError(t, err, "failed to get max index")
		assert.Equal(t, batchSize+2, maxIndex, "unexpected max index")
		eb, ok := db.(*ShardedDB).shards[0].entries.(*batchedEntries).
			getBatchFromDB(shardID, replicaID, 0)
		assert.True(t, ok, "failed to get the eb")
		assert.Equal(t, batchSize-1, uint64(len(eb.Entries)),
			"unexpected len %d, want %d", len(eb.Entries), batchSize-1)
		for i := uint64(0); i < batchSize-1; i++ {
			e := eb.Entries[i]
			assert.Equal(t, i+1, e.Index, "unexpected index %d, want %d",
				e.Index, i+1)
			if e.Index < batchSize-4 {
				assert.Equal(t, uint64(1), e.Term, "unexpected term %d", e.Term)
			} else {
				assert.Equal(t, uint64(2), e.Term, "unexpected term %d", e.Term)
			}
		}
	}
	fs := vfs.GetTestFS()
	runBatchedLogDBTest(t, tf, fs)
}

func TestSaveEntriesAcrossMultipleBatches(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		e1 := pb.Entry{
			Term:  1,
			Index: 1,
			Type:  pb.ApplicationEntry,
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		e2 := pb.Entry{
			Term:  1,
			Index: 2,
			Type:  pb.ApplicationEntry,
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		ud = pb.Update{
			EntriesToSave: make([]pb.Entry, 0),
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		for idx := uint64(3); idx <= batchSize+1; idx++ {
			e := pb.Entry{
				Term:  1,
				Index: idx,
			}
			ud.EntriesToSave = append(ud.EntriesToSave, e)
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		assert.NoError(t, err, "failed to save recs")
		ents, _, err := db.IterateEntries([]pb.Entry{}, 0,
			shardID, replicaID, 1, batchSize+2, math.MaxUint64)
		assert.NoError(t, err, "iterate entries failed %v", err)
		assert.Equal(t, batchSize+1, uint64(len(ents)),
			"ents sz %d, want %d", len(ents), batchSize+1)
		eb, ok := db.(*ShardedDB).shards[0].entries.(*batchedEntries).
			getBatchFromDB(shardID, replicaID, 1)
		assert.True(t, ok, "failed to get first batch")
		for _, e := range eb.Entries {
			plog.Infof("idx %d", e.Index)
		}
	}
	fs := vfs.GetTestFS()
	runBatchedLogDBTest(t, tf, fs)
}
