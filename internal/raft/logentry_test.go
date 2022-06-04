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

package raft

import (
	"math"
	"testing"

	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func getTestEntryLog() *entryLog {
	logdb := NewTestLogDB()
	return newEntryLog(logdb, server.NewInMemRateLimiter(0))
}

func TestLogEntryLogCanBeCreated(t *testing.T) {
	logdb := NewTestLogDB()
	if err := logdb.Append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}); err != nil {
		t.Fatalf("%v", err)
	}
	first, last := logdb.GetRange()
	if first != 1 || last != 3 {
		t.Errorf("unexpected range")
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	if el.committed != 0 || el.processed != 0 || el.inmem.markerIndex != 4 {
		t.Errorf("unexpected log state %+v", el)
	}
}

func TestLogReturnSnapshotIndexAsFirstIndex(t *testing.T) {
	el := getTestEntryLog()
	ss := pb.Snapshot{Index: 100, Term: 3}
	el.inmem.restore(ss)
	if el.firstIndex() != 101 {
		t.Errorf("unexpected first index %d, want 101", el.firstIndex())
	}
}

func TestLogWithInMemSnapshotOnly(t *testing.T) {
	el := getTestEntryLog()
	ss := pb.Snapshot{Index: 100, Term: 3}
	el.restore(ss)
	if el.firstIndex() != 101 {
		t.Errorf("unexpected first index %d, want 101", el.firstIndex())
	}
	if el.lastIndex() != 100 {
		t.Errorf("unexpected last index %d, want 100", el.lastIndex())
	}
	for i := uint64(0); i < 110; i++ {
		ents, err := el.getEntries(i, i+1, math.MaxUint64)
		if err != ErrCompacted {
			t.Errorf("unexpected err %v", err)
		}
		if len(ents) != 0 {
			t.Errorf("unexpected results %v", ents)
		}
	}
}

func TestLogNoEntriesToApplyAfterSnapshotRestored(t *testing.T) {
	el := getTestEntryLog()
	ss := pb.Snapshot{Index: 100, Term: 3}
	el.restore(ss)
	if el.hasEntriesToApply() {
		t.Errorf("unexpected entry to apply")
	}
}

func TestLogFirstNotAppliedIndexAfterSnapshotRestored(t *testing.T) {
	el := getTestEntryLog()
	ss := pb.Snapshot{Index: 100, Term: 3}
	el.restore(ss)
	idx := el.firstNotAppliedIndex()
	if idx != 101 {
		t.Errorf("unexpected index %d, want 101", idx)
	}
	if el.toApplyIndexLimit() != 101 {
		t.Errorf("unexpected to apply limit %d, want 101", el.toApplyIndexLimit())
	}
}

func TestLogIterateOnReadyToBeAppliedEntries(t *testing.T) {
	ents := make([]pb.Entry, 0)
	for i := uint64(1); i <= 128; i++ {
		ents = append(ents, pb.Entry{Index: i, Term: i})
	}
	ents[10].Cmd = make([]byte, maxEntriesToApplySize)
	ents[20].Cmd = make([]byte, maxEntriesToApplySize)
	ents[30].Cmd = make([]byte, maxEntriesToApplySize*2)
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.committed = 128
	el.processed = 0
	results := make([]pb.Entry, 0)
	count := 0
	for {
		re, err := el.getEntriesToApply(maxEntriesToApplySize)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if len(re) == 0 {
			break
		}
		count++
		results = append(results, re...)
		el.processed = re[len(re)-1].Index
	}
	if len(results) != 128 {
		t.Errorf("failed to get all entries")
	}
	for idx, e := range results {
		if e.Index != uint64(idx+1) {
			t.Errorf("unexpected index")
		}
	}
	if count != 7 {
		t.Errorf("unexpected count %d, want 7", count)
	}
}

func TestLogReturnLastIndexInLogDBWhenNoSnapshotInMem(t *testing.T) {
	logdb := NewTestLogDB()
	if err := logdb.Append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	if el.firstIndex() != 1 {
		t.Errorf("unexpected first index, %d", el.firstIndex())
	}
}

func TestLogLastIndexReturnInMemLastIndexWhenPossible(t *testing.T) {
	el := getTestEntryLog()
	el.inmem.merge([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	if el.lastIndex() != 4 {
		t.Errorf("unexpected last index %d", el.lastIndex())
	}
}

func TestLogLastIndexReturnLogDBLastIndexWhenNothingInInMem(t *testing.T) {
	logdb := NewTestLogDB()
	if err := logdb.Append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	if el.lastIndex() != 2 {
		t.Errorf("unexpected last index %d", el.lastIndex())
	}
}

func TestLogLastTerm(t *testing.T) {
	el := getTestEntryLog()
	el.inmem.merge([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	term, err := el.lastTerm()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if term != 3 {
		t.Errorf("unexpected last term %d", term)
	}
	logdb := NewTestLogDB()
	if err := logdb.Append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 5},
	}); err != nil {
		t.Fatalf("%v", err)
	}
	el = newEntryLog(logdb, server.NewInMemRateLimiter(0))
	term, err = el.lastTerm()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if term != 5 {
		t.Errorf("unexpected last term %d", term)
	}
}

func TestLogTerm(t *testing.T) {
	el := getTestEntryLog()
	el.inmem.merge([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	for idx, ent := range el.inmem.entries {
		term, err := el.term(ent.Index)
		if err != nil {
			t.Errorf("%d, unexpected err %v", idx, err)
		}
		if term != ent.Term {
			t.Errorf("%d, unexpected term %d, want %d", idx, term, ent.Term)
		}
	}
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 5},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el = newEntryLog(logdb, server.NewInMemRateLimiter(0))
	for idx, ent := range ents {
		term, err := el.term(ent.Index)
		if err != nil {
			t.Errorf("%d, unexpected error %v", idx, err)
		}
		if term != ent.Term {
			t.Errorf("%d, unexpected term %d, want %d", idx, term, ent.Term)
		}
	}
}

func TestLogAppend(t *testing.T) {
	el := getTestEntryLog()
	el.append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	ets := el.entriesToSave()
	if len(ets) != 4 {
		t.Errorf("unexpected length %d", len(ets))
	}
}
func TestLogAppendPanicWhenAppendingCommittedEntry(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	el := getTestEntryLog()
	el.committed = 2
	el.append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
}

func TestLogGetEntryFromInMem(t *testing.T) {
	el := getTestEntryLog()
	el.append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	ents, err := el.getEntries(1, 5, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 4, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 2 {
		t.Errorf("unexpected length %d", len(ents))
	}
}

func TestLogGetEntryFromLogDB(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	ents, err := el.getEntries(1, 5, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 4, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 2 {
		t.Errorf("unexpected length %d", len(ents))
	}
}

func TestLogGetEntryFromLogDBAndInMem(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	ents, err := el.getEntries(1, 8, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 7 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 7, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 5 {
		t.Errorf("unexpected length %d", len(ents))
	}
	if ents[4].Index != 6 || ents[0].Index != 2 {
		t.Errorf("unexpected index")
	}
	ents, err = el.getEntries(1, 5, math.MaxUint64)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	if ents[0].Index != 1 || ents[3].Index != 4 {
		t.Errorf("unexpected index")
	}
	// entries() wrapper
	ents, err = el.entries(2, math.MaxUint64)
	if err != nil {
		t.Errorf("entries failed %v", err)
	}
	if len(ents) != 6 {
		t.Errorf("unexpected length")
	}
}

func TestLogSnapshot(t *testing.T) {
	inMemSnapshot := pb.Snapshot{Index: 123, Term: 2}
	logdbSnapshot := pb.Snapshot{Index: 234, Term: 3}
	el := getTestEntryLog()
	el.inmem.restore(inMemSnapshot)
	if err := el.logdb.ApplySnapshot(logdbSnapshot); err != nil {
		t.Fatalf("%v", err)
	}
	ss := el.snapshot()
	if ss.Index != inMemSnapshot.Index {
		t.Errorf("unexpected snapshot index")
	}
	el.inmem.savedSnapshotTo(inMemSnapshot.Index)
	ss = el.snapshot()
	if ss.Index != logdbSnapshot.Index {
		t.Errorf("unexpected snapshot index")
	}
}

func TestLogMatchTerm(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	tests := []struct {
		index uint64
		term  uint64
		match bool
	}{
		{1, 1, true},
		{1, 2, false},
		{4, 4, false},
		{4, 3, true},
		{5, 3, true},
		{5, 4, false},
		{7, 4, true},
		{8, 5, false},
	}
	for idx, tt := range tests {
		match, err := el.matchTerm(tt.index, tt.term)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if match != tt.match {
			t.Errorf("%d, incorrect matchTerm result", idx)
		}
	}
}

func TestLogUpToDate(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	tests := []struct {
		index uint64
		term  uint64
		ok    bool
	}{
		{1, 2, false},
		{8, 2, false},
		{1, 4, false},
		{7, 4, true},
		{8, 4, true},
		{8, 5, true},
		{2, 5, true},
	}
	for idx, tt := range tests {
		ok, err := el.upToDate(tt.index, tt.term)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if ok != tt.ok {
			t.Errorf("%d, incorrect up to date result", idx)
		}
	}
}

func TestLogGetConflictIndex(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	tests := []struct {
		ents     []pb.Entry
		conflict uint64
	}{
		{[]pb.Entry{}, 0},
		{[]pb.Entry{{Index: 1, Term: 2}}, 1},
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 0},
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2},
		{[]pb.Entry{{Index: 6, Term: 3}, {Index: 7, Term: 4}}, 0},
		{[]pb.Entry{{Index: 6, Term: 3}, {Index: 7, Term: 5}}, 7},
		{[]pb.Entry{{Index: 7, Term: 4}, {Index: 8, Term: 4}}, 8},
	}
	for idx, tt := range tests {
		conflict, err := el.getConflictIndex(tt.ents)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if conflict != tt.conflict {
			t.Errorf("%d, conflict index %d, want %d", idx, conflict, tt.conflict)
		}
	}
}

func TestLogCommitTo(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	el.commitTo(3)
	if el.committed != 3 {
		t.Errorf("commitedTo failed")
	}
	el.commitTo(2)
	if el.committed != 3 {
		t.Errorf("commitedTo failed")
	}
}

func TestLogCommitToPanicWhenCommitToUnavailableIndex(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	if err := logdb.Append(ents); err != nil {
		t.Fatalf("%v", err)
	}
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append([]pb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	})
	el.commitTo(8)
}

func TestLogRestoreSnapshot(t *testing.T) {
	ents := []pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	logdb := NewTestLogDB()
	el := newEntryLog(logdb, server.NewInMemRateLimiter(0))
	el.append(ents)
	ss := pb.Snapshot{Index: 100, Term: 10}
	el.restore(ss)
	if el.committed != 100 || el.processed != 100 {
		t.Errorf("committed/applied not updated")
	}
	if el.inmem.markerIndex != 101 {
		t.Errorf("marker index not updated")
	}
	if el.inmem.snapshot.Index != 100 {
		t.Errorf("snapshot index not updated")
	}
}

func TestLogCommitUpdateSetsApplied(t *testing.T) {
	el := getTestEntryLog()
	el.committed = 10
	cu := pb.UpdateCommit{
		Processed: 5,
	}
	el.commitUpdate(cu)
	if el.processed != 5 {
		t.Errorf("applied %d, want 5", el.processed)
	}
}

func TestLogCommitUpdatePanicWhenApplyTwice(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("didn't panic")
		}
	}()
	el := getTestEntryLog()
	el.processed = 6
	el.committed = 10
	cu := pb.UpdateCommit{
		Processed: 5,
	}
	el.commitUpdate(cu)
}

func TestLogCommitUpdatePanicWhenApplyingNotCommitEntry(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("didn't panic")
		}
	}()
	el := getTestEntryLog()
	el.processed = 6
	el.committed = 10
	cu := pb.UpdateCommit{
		Processed: 12,
	}
	el.commitUpdate(cu)
}

func TestGetUncommittedEntries(t *testing.T) {
	el := getTestEntryLog()
	ents := el.getUncommittedEntries()
	if len(ents) != 0 {
		t.Errorf("unexpected length")
	}
	el.append([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	})
	tests := []struct {
		committed  uint64
		length     int
		firstIndex uint64
	}{
		{0, 4, 1},
		{1, 3, 2},
		{2, 2, 3},
		{3, 1, 4},
		{4, 0, 0},
	}
	for idx, tt := range tests {
		el.committed = tt.committed
		ents = el.getUncommittedEntries()
		if len(ents) != tt.length {
			t.Errorf("unexpected length")
		}
		if len(ents) > 0 {
			if ents[0].Index != tt.firstIndex {
				t.Errorf("%d, first index %d, want %d", idx, ents[0].Index, tt.firstIndex)
			}
		}
	}
}
