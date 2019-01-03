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

package logdb

import (
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/raftpb"
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
		if low != tt.batchLow {
			t.Errorf("%d, low %d, want %d", idx, low, tt.batchLow)
		}
		if high != tt.batchHigh {
			t.Errorf("%d, high %d, want %d", idx, high, tt.batchHigh)
		}
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
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("initial value not equal")
	}
	eb1 = compactBatchFields(eb1)
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("unexpectedly compacted")
	}
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
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("initial value not equal")
	}
	eb1 = compactBatchFields(eb1)
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("unexpectedly compacted")
	}
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
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("input not equal")
	}
	eb1 = compactBatchFields(eb1)
	if reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("eb not changed")
	}
	if eb1.Size() >= eb2.Size() {
		t.Errorf("size didn't reduce")
	}
	for i := 0; i < len(eb1.Entries); i++ {
		if i == 0 {
			if eb1.Entries[i].Index == 0 || eb1.Entries[i].Term == 0 {
				t.Errorf("first index/term is 0, %+v", eb1.Entries)
			}
		} else {
			if eb1.Entries[i].Index != 0 || eb1.Entries[i].Term != 0 {
				t.Errorf("first index/term is not 0")
			}
		}
	}
	eb1 = restoreBatchFields(eb1)
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("not restored")
	}
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
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("input not equal")
	}
	eb1 = restoreBatchFields(eb1)
	if !reflect.DeepEqual(&eb1, &eb2) {
		t.Errorf("not restored")
	}
}

func TestCompactBatchFieldsPanicWhenBatchIsTooSmall(t *testing.T) {
	f := func(eb pb.EntryBatch) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("panic not triggered")
			}
		}()
		compactBatchFields(eb)
	}
	f(pb.EntryBatch{})
	f(pb.EntryBatch{Entries: []pb.Entry{pb.Entry{}}})
}

func TestRestoreBatchFieldsPanicWhenBatchIsTooSmall(t *testing.T) {
	f := func(eb pb.EntryBatch) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("panic not triggered")
			}
		}()
		restoreBatchFields(eb)
	}
	f(pb.EntryBatch{})
	f(pb.EntryBatch{Entries: []pb.Entry{pb.Entry{}}})
}

func TestMergeFirstBatchPanicWhenInputBatchIsEmpty(t *testing.T) {
	empty := pb.EntryBatch{}
	nonEmpty := pb.EntryBatch{Entries: []pb.Entry{pb.Entry{Index: 1, Term: 1}}}
	f := func(eb pb.EntryBatch, lb pb.EntryBatch) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("panic not triggered")
			}
		}()
		getMergedFirstBatch(eb, lb)
	}
	f(empty, nonEmpty)
	f(nonEmpty, empty)
}

func TestMergeFirstBatchPanicWhenIncomingBatchIsNotMoreRecent(t *testing.T) {
	eb := pb.EntryBatch{Entries: []pb.Entry{pb.Entry{Index: batchSize, Term: 1}}}
	lb := pb.EntryBatch{Entries: []pb.Entry{pb.Entry{Index: 2 * batchSize, Term: 1}}}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic not triggered")
		}
	}()
	getMergedFirstBatch(eb, lb)
}

func TestIncomingBatchIsTheMergedBatchWhenMoreRecentThanLastBatch(t *testing.T) {
	eb := pb.EntryBatch{Entries: []pb.Entry{pb.Entry{Index: 2 * batchSize, Term: 1}}}
	lb := pb.EntryBatch{Entries: []pb.Entry{pb.Entry{Index: batchSize, Term: 1}}}
	result := getMergedFirstBatch(eb, lb)
	if !reflect.DeepEqual(&result, &eb) {
		t.Errorf("unexpected result")
	}
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
		if result.Entries[0].Index != tt.mfirst {
			t.Errorf("%d, first index %d, want %d", idx, result.Entries[0].Index, tt.mfirst)
		}
		if result.Entries[len(result.Entries)-1].Index != tt.mlast {
			t.Errorf("%d, last index %d, want %d", idx, result.Entries[len(result.Entries)-1].Index, tt.mlast)
		}
		for i := 0; i < len(result.Entries); i++ {
			if result.Entries[i].Index >= tt.newindex {
				if result.Entries[i].Term != 2 {
					t.Errorf("unexpected term")
				}
			}
		}
	}
}
