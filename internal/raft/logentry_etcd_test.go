// Copyright 2015 The etcd Authors
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

//
// log_etcd_test.go is ported from etcd raft for testing purposes.
// tests have been updated to reflect the fact that we have a 3 stage log in
// dragonboat while etcd raft uses a 2 stage log.
//

package raft

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func getAllEntries(l *entryLog) []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	// try again if there was a racing compaction
	if errors.Is(err, ErrCompacted) {
		return getAllEntries(l)
	}
	panic(err)
}

func TestFindConflict(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	tests := []struct {
		ents      []pb.Entry
		wconflict uint64
	}{
		// no conflict, empty ent
		{[]pb.Entry{}, 0},
		// no conflict
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 3, Term: 3}}, 0},
		// no conflict, but has new entries
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		// conflicts with existing entries
		{[]pb.Entry{{Index: 1, Term: 4}, {Index: 2, Term: 4}}, 1},
		{[]pb.Entry{{Index: 2, Term: 1}, {Index: 3, Term: 4}, {Index: 4, Term: 4}}, 2},
		{[]pb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}, {Index: 5, Term: 4}, {Index: 6, Term: 4}}, 3},
	}

	for i, tt := range tests {
		raftLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
		raftLog.append(previousEnts)

		gconflict, err := raftLog.getConflictIndex(tt.ents)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if gconflict != tt.wconflict {
			t.Errorf("#%d: conflict = %d, want %d", i, gconflict, tt.wconflict)
		}
	}
}

func TestIsUpToDate(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	raftLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
	raftLog.append(previousEnts)
	tests := []struct {
		lastIndex uint64
		term      uint64
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		gUpToDate, err := raftLog.upToDate(tt.lastIndex, tt.term)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if gUpToDate != tt.wUpToDate {
			t.Errorf("#%d: uptodate = %v, want %v", i, gUpToDate, tt.wUpToDate)
		}
	}
}

func TestAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	tests := []struct {
		ents      []pb.Entry
		windex    uint64
		wents     []pb.Entry
		wunstable uint64
	}{
		{
			[]pb.Entry{},
			2,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			3,
		},
		{
			[]pb.Entry{{Index: 3, Term: 2}},
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}},
			3,
		},
		// conflicts with index 1
		{
			[]pb.Entry{{Index: 1, Term: 2}},
			1,
			[]pb.Entry{{Index: 1, Term: 2}},
			1,
		},
		// conflicts with index 2
		{
			[]pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}},
			3,
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}},
			2,
		},
	}

	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(previousEnts); err != nil {
			t.Fatalf("%v", err)
		}
		raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))

		raftLog.append(tt.ents)
		index := raftLog.lastIndex()
		if index != tt.windex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, index, tt.windex)
		}
		g, err := raftLog.entries(1, noLimit)
		if err != nil {
			t.Fatalf("#%d: unexpected error %v", i, err)
		}
		if !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: logEnts = %+v, want %+v", i, g, tt.wents)
		}
		if goff := raftLog.inmem.markerIndex; goff != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, goff, tt.wunstable)
		}
	}
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
// 	1. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it
// 	2.Append any new entries not already in the log
// If the given (index, term) does not match with the existing log:
// 	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := uint64(3)
	lastterm := uint64(3)
	commit := uint64(1)

	tests := []struct {
		logTerm   uint64
		index     uint64
		committed uint64
		ents      []pb.Entry

		wlasti  uint64
		wappend bool
		wcommit uint64
		wpanic  bool
	}{
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, []pb.Entry{{Index: lastindex + 2, Term: 4}},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, nil,
			lastindex, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, nil,
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex - 1, nil,
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			lastterm, lastindex, 0, nil,
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			0, 0, lastindex, nil,
			0, true, commit, false, // commit do not decrease
		},
		{
			lastterm, lastindex, lastindex, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			lastterm, lastindex, lastindex + 2, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex + 2, []pb.Entry{{Index: lastindex + 1, Term: 4}, {Index: lastindex + 2, Term: 4}},
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the the entry in the middle
		{
			lastterm - 1, lastindex - 1, lastindex, []pb.Entry{{Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []pb.Entry{{Index: lastindex - 1, Term: 4}},
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			lastterm - 3, lastindex - 3, lastindex, []pb.Entry{{Index: lastindex - 2, Term: 4}},
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []pb.Entry{{Index: lastindex - 1, Term: 4}, {Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
		raftLog.append(previousEnts)
		raftLog.committed = commit
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			var glasti uint64
			var gappend bool
			match, err := raftLog.matchTerm(tt.index, tt.logTerm)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if match {
				gappend = true
				if _, err := raftLog.tryAppend(tt.index, tt.ents); err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				glasti = tt.index + uint64(len(tt.ents))
				raftLog.commitTo(min(glasti, tt.committed))
			}
			gcommit := raftLog.committed

			if glasti != tt.wlasti {
				t.Errorf("#%d: lastindex = %d, want %d", i, glasti, tt.wlasti)
			}
			if gappend != tt.wappend {
				t.Errorf("#%d: append = %v, want %v", i, gappend, tt.wappend)
			}
			if gcommit != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, gcommit, tt.wcommit)
			}
			if gappend && len(tt.ents) != 0 {
				gents, err := raftLog.getEntries(raftLog.lastIndex()-uint64(len(tt.ents))+1, raftLog.lastIndex()+1, noLimit)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tt.ents, gents) {
					t.Errorf("%d: appended entries = %v, want %v", i, gents, tt.ents)
				}
			}
		}()
	}
}

func TestHasNextEnts(t *testing.T) {
	snap := pb.Snapshot{
		Term: 1, Index: 3,
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		hasNext bool
	}{
		{0, true},
		{3, true},
		{4, true},
		{5, false},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.ApplySnapshot(snap); err != nil {
			t.Fatalf("%v", err)
		}
		raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))
		raftLog.append(ents)
		if _, err := raftLog.tryCommit(5, 1); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		raftLog.commitUpdate(pb.UpdateCommit{Processed: tt.applied})

		hasNext := raftLog.hasEntriesToApply()
		if hasNext != tt.hasNext {
			t.Errorf("#%d: hasNext = %v, want %v", i, hasNext, tt.hasNext)
		}
	}
}

func TestNextEnts(t *testing.T) {
	snap := pb.Snapshot{
		Term: 1, Index: 3,
	}
	ents := []pb.Entry{
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		wents   []pb.Entry
	}{
		{0, ents[:2]},
		{3, ents[:2]},
		{4, ents[1:2]},
		{5, nil},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.ApplySnapshot(snap); err != nil {
			t.Fatalf("%v", err)
		}
		raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))
		raftLog.append(ents)
		if _, err := raftLog.tryCommit(5, 1); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		raftLog.commitUpdate(pb.UpdateCommit{Processed: tt.applied})

		nents, err := raftLog.entriesToApply()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if !reflect.DeepEqual(nents, tt.wents) {
			t.Errorf("#%d: nents = %+v, want %+v", i, nents, tt.wents)
		}
	}
}

func TestCommitTo(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}
	commit := uint64(2)
	tests := []struct {
		commit  uint64
		wcommit uint64
		wpanic  bool
	}{
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	}
	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			raftLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
			raftLog.append(previousEnts)
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			if raftLog.committed != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, raftLog.committed, tt.wcommit)
			}
		}()
	}
}

//TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	tests := []struct {
		lastIndex uint64
		compact   []uint64
		wleft     []int
		wallow    bool
	}{
		// out of upper bound
		{1000, []uint64{1001}, []int{-1}, false},
		{1000, []uint64{300, 500, 800, 900}, []int{700, 500, 200, 100}, true},
		// out of lower bound
		{1000, []uint64{300, 299}, []int{700, -1}, false},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow {
						t.Errorf("%d: allow = %v, want %v: %v", i, false, true, r)
					}
				}
			}()

			storage := NewTestLogDB()
			for i := uint64(1); i <= tt.lastIndex; i++ {
				if err := storage.Append([]pb.Entry{{Index: i}}); err != nil {
					t.Fatalf("%v", err)
				}
			}
			raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))
			if _, err := raftLog.tryCommit(tt.lastIndex, 0); err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			raftLog.commitUpdate(pb.UpdateCommit{Processed: raftLog.committed})

			for j := 0; j < len(tt.compact); j++ {
				err := storage.Compact(tt.compact[j])
				if err != nil {
					if tt.wallow {
						t.Errorf("#%d.%d allow = %t, want %t", i, j, false, tt.wallow)
					}
					continue
				}
				if len(getAllEntries(raftLog)) != tt.wleft[j] {
					t.Errorf("#%d.%d len = %d, want %d", i, j, len(getAllEntries(raftLog)), tt.wleft[j])
				}
			}
		}()
	}
}

func TestLogRestore(t *testing.T) {
	index := uint64(1000)
	term := uint64(1000)
	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(pb.Snapshot{Index: index, Term: term}); err != nil {
		t.Fatalf("%v", err)
	}
	raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))

	if len(getAllEntries(raftLog)) != 0 {
		t.Errorf("len = %d, want 0", len(getAllEntries(raftLog)))
	}
	if raftLog.firstIndex() != index+1 {
		t.Errorf("firstIndex = %d, want %d", raftLog.firstIndex(), index+1)
	}
	if raftLog.committed != index {
		t.Errorf("committed = %d, want %d", raftLog.committed, index)
	}
	if raftLog.inmem.markerIndex != index+1 {
		t.Errorf("unstable = %d, want %d", raftLog.inmem.markerIndex, index+1)
	}
	if mustTerm(raftLog.term(index)) != term {
		t.Errorf("term = %d, want %d", mustTerm(raftLog.term(index)), term)
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(pb.Snapshot{Index: offset}); err != nil {
		t.Fatalf("%v", err)
	}
	l := newEntryLog(storage, server.NewInMemRateLimiter(0))
	for i := uint64(1); i <= num; i++ {
		l.append([]pb.Entry{{Index: i + offset}})
	}

	first := offset + 1
	tests := []struct {
		lo, hi        uint64
		wpanic        bool
		wErrCompacted bool
	}{
		{
			first - 2, first + 1,
			false,
			true,
		},
		{
			first - 1, first + 1,
			false,
			true,
		},
		{
			first, first,
			false,
			false,
		},
		{
			first + num/2, first + num/2,
			false,
			false,
		},
		{
			first + num - 1, first + num - 1,
			false,
			false,
		},
		{
			first + num, first + num,
			false,
			false,
		},
		{
			first + num, first + num + 1,
			true,
			false,
		},
		{
			first + num + 1, first + num + 1,
			true,
			false,
		},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", i, true, false, r)
					}
				}
			}()
			err := l.checkBound(tt.lo, tt.hi)
			if tt.wpanic {
				t.Errorf("%d: panic = %v, want %v", i, false, true)
			}
			if tt.wErrCompacted && err != ErrCompacted {
				t.Errorf("%d: err = %v, want %v", i, err, ErrCompacted)
			}
			if !tt.wErrCompacted && err != nil {
				t.Errorf("%d: unexpected err %v", i, err)
			}
		}()
	}
}

func TestTerm(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)

	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(pb.Snapshot{Index: offset, Term: 1}); err != nil {
		t.Fatalf("%v", err)
	}
	l := newEntryLog(storage, server.NewInMemRateLimiter(0))
	for i = 1; i < num; i++ {
		l.append([]pb.Entry{{Index: offset + i, Term: i}})
	}

	tests := []struct {
		index uint64
		w     uint64
	}{
		{offset - 1, 0},
		{offset, 1},
		{offset + num/2, num / 2},
		{offset + num - 1, num - 1},
		{offset + num, 0},
	}

	for j, tt := range tests {
		term := mustTerm(l.term(tt.index))
		if term != tt.w {
			t.Errorf("#%d: at = %d, want %d", j, term, tt.w)
		}
	}
}

func TestTermWithUnstableSnapshot(t *testing.T) {
	storagesnapi := uint64(100)
	unstablesnapi := storagesnapi + 5

	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(pb.Snapshot{Index: storagesnapi, Term: 1}); err != nil {
		t.Fatalf("%v", err)
	}
	l := newEntryLog(storage, server.NewInMemRateLimiter(0))
	l.restore(pb.Snapshot{Index: unstablesnapi, Term: 1})

	tests := []struct {
		index uint64
		w     uint64
	}{
		// cannot get term from storage
		{storagesnapi, 0},
		// cannot get term from the gap between storage ents and unstable snapshot
		{storagesnapi + 1, 0},
		{unstablesnapi - 1, 0},
		// get term from unstable snapshot index
		{unstablesnapi, 1},
	}

	for i, tt := range tests {
		term := mustTerm(l.term(tt.index))
		if term != tt.w {
			t.Errorf("#%d: at = %d, want %d", i, term, tt.w)
		}
	}
}

func TestSlice(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	last := offset + num
	half := offset + num/2
	halfe := pb.Entry{Index: half, Term: half}

	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(pb.Snapshot{Index: offset}); err != nil {
		t.Fatalf("%v", err)
	}
	for i = 1; i < num/2; i++ {
		if err := storage.Append([]pb.Entry{{Index: offset + i, Term: offset + i}}); err != nil {
			t.Fatalf("%v", err)
		}
	}
	l := newEntryLog(storage, server.NewInMemRateLimiter(0))
	for i = num / 2; i < num; i++ {
		l.append([]pb.Entry{{Index: offset + i, Term: offset + i}})
	}

	tests := []struct {
		from  uint64
		to    uint64
		limit uint64

		w      []pb.Entry
		wpanic bool
	}{
		// test no limit
		{offset - 1, offset + 1, noLimit, nil, false},
		{offset, offset + 1, noLimit, nil, false},
		{half - 1, half + 1, noLimit, []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half, half + 1, noLimit, []pb.Entry{{Index: half, Term: half}}, false},
		{last - 1, last, noLimit, []pb.Entry{{Index: last - 1, Term: last - 1}}, false},
		{last, last + 1, noLimit, nil, true},

		// test limit
		{half - 1, half + 1, 0, []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 1, half + 1, uint64(halfe.SizeUpperLimit() + 1), []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 2, half + 1, uint64(halfe.SizeUpperLimit() + 1), []pb.Entry{{Index: half - 2, Term: half - 2}}, false},
		{half - 1, half + 1, uint64(halfe.SizeUpperLimit() * 2), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half - 1, half + 2, uint64(halfe.SizeUpperLimit() * 3), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
		{half, half + 2, uint64(halfe.SizeUpperLimit()), []pb.Entry{{Index: half, Term: half}}, false},
		{half, half + 2, uint64(halfe.SizeUpperLimit() * 2), []pb.Entry{{Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
	}

	for j, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", j, true, false, r)
					}
				}
			}()
			g, err := l.getEntries(tt.from, tt.to, tt.limit)
			if tt.from <= offset && err != ErrCompacted {
				t.Fatalf("#%d: err = %v, want %v", j, err, ErrCompacted)
			}
			if tt.from > offset && err != nil {
				t.Fatalf("#%d: unexpected error %v", j, err)
			}
			if !reflect.DeepEqual(g, tt.w) {
				t.Errorf("#%d: from %d to %d = %v, want %v", j, tt.from, tt.to, g, tt.w)
			}
		}()
	}
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	var i uint64
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	lastIndex := uint64(1000)
	unstableIndex := uint64(750)
	lastTerm := lastIndex
	storage := NewTestLogDB()
	for i = 1; i <= unstableIndex; i++ {
		if err := storage.Append([]pb.Entry{{Term: i, Index: i}}); err != nil {
			t.Fatalf("%v", err)
		}
	}
	raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))
	for i = unstableIndex; i < lastIndex; i++ {
		raftLog.append([]pb.Entry{{Term: i + 1, Index: i + 1}})
	}

	ok, err := raftLog.tryCommit(lastIndex, lastTerm)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Fatalf("maybeCommit returned false")
	}
	offset := uint64(500)
	if err := storage.Compact(offset); err != nil {
		t.Fatalf("%v", err)
	}
	if raftLog.lastIndex() != lastIndex {
		t.Errorf("lastIndex = %d, want %d", raftLog.lastIndex(), lastIndex)
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		if mustTerm(raftLog.term(j)) != j {
			t.Errorf("term(%d) = %d, want %d", j, mustTerm(raftLog.term(j)), j)
		}
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		match, err := raftLog.matchTerm(j, j)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if !match {
			t.Errorf("matchTerm(%d) = false, want true", j)
		}
	}

	unstableEnts := raftLog.entriesToSave()
	if g := len(unstableEnts); g != 250 {
		t.Errorf("len(unstableEntries) = %d, want = %d", g, 250)
	}
	if len(unstableEnts) == 0 {
		t.Fatalf("len(unstableEnts) == 0")
	}
	if unstableEnts[0].Index != 751 {
		t.Errorf("Index = %d, want = %d", unstableEnts[0].Index, 751)
	}

	prev := raftLog.lastIndex()
	raftLog.append([]pb.Entry{{Index: raftLog.lastIndex() + 1, Term: raftLog.lastIndex() + 1}})
	if raftLog.lastIndex() != prev+1 {
		t.Errorf("lastIndex = %d, want = %d", raftLog.lastIndex(), prev+1)
	}

	ents, err := raftLog.entries(raftLog.lastIndex(), noLimit)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(ents) != 1 {
		t.Errorf("len(entries) = %d, want = %d", len(ents), 1)
	}
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestUnstableEnts(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		unstable uint64
		wents    []pb.Entry
	}{
		{3, nil},
		{1, previousEnts},
	}

	for i, tt := range tests {
		// append stable entries to storage
		storage := NewTestLogDB()
		if err := storage.Append(previousEnts[:tt.unstable-1]); err != nil {
			t.Fatalf("%v", err)
		}

		// append unstable entries to raftlog
		raftLog := newEntryLog(storage, server.NewInMemRateLimiter(0))
		raftLog.append(previousEnts[tt.unstable-1:])
		ents := raftLog.entriesToSave()
		if l := len(ents); l > 0 {
			if _, err := raftLog.tryCommit(ents[l-1].Index, ents[l-1].Term); err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			cu := pb.UpdateCommit{
				Processed:     ents[l-1].Index,
				LastApplied:   ents[l-1].Index,
				StableLogTo:   ents[l-1].Index,
				StableLogTerm: ents[l-1].Term,
			}
			raftLog.commitUpdate(cu)
			//raftLog.savedLogTo(ents[l-1].Index, ents[l-i].Term)
			//raftLog.appliedLogTo(ents[l-1].Index)
		}
		if !reflect.DeepEqual(ents, tt.wents) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, ents, tt.wents)
		}
		if len(ents) > 0 {
			w := ents[len(ents)-1].Index + 1
			if g := raftLog.inmem.markerIndex; g != w {
				t.Errorf("#%d: unstable = %d, want %d", i, g, w)
			}
		}
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei   uint64
		stablet   uint64
		savedTo   uint64
		wunstable uint64
	}{
		{1, 1, 1, 1},
		{2, 2, 1, 1},
		{2, 1, 0, 1}, // bad term
		{3, 1, 0, 1}, // bad index
	}
	for i, tt := range tests {
		raftLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
		raftLog.append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
		cu := pb.UpdateCommit{
			StableLogTo:   tt.stablei,
			StableLogTerm: tt.stablet,
		}
		raftLog.commitUpdate(cu)
		if tt.savedTo > 0 && raftLog.inmem.savedTo != tt.stablei {
			t.Errorf("#%d: stable to %d, want %d", i, raftLog.inmem.savedTo, tt.stablei)
		}
		if raftLog.inmem.markerIndex != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.inmem.markerIndex, tt.wunstable)
		}
	}
}

func TestStableToWithSnap(t *testing.T) {
	snapi, snapt := uint64(5), uint64(2)
	tests := []struct {
		stablei uint64
		stablet uint64
		newEnts []pb.Entry

		wunstable uint64
	}{
		{snapi + 1, snapt, nil, snapi + 1},
		{snapi, snapt, nil, snapi + 1},
		{snapi - 1, snapt, nil, snapi + 1},

		{snapi + 1, snapt + 1, nil, snapi + 1},
		{snapi, snapt + 1, nil, snapi + 1},
		{snapi - 1, snapt + 1, nil, snapi + 1},

		{snapi + 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 2},
		{snapi, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},

		{snapi + 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
		{snapi - 1, snapt + 1, []pb.Entry{{Index: snapi + 1, Term: snapt}}, snapi + 1},
	}
	for i, tt := range tests {
		s := NewTestLogDB()
		if err := s.ApplySnapshot(pb.Snapshot{Index: snapi, Term: snapt}); err != nil {
			t.Fatalf("%v", err)
		}
		raftLog := newEntryLog(s, server.NewInMemRateLimiter(0))
		raftLog.append(tt.newEnts)
		cu := pb.UpdateCommit{
			StableLogTo:   tt.stablei,
			StableLogTerm: tt.stablet,
		}
		raftLog.commitUpdate(cu)
		if raftLog.inmem.savedTo != tt.wunstable-1 {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.inmem.savedTo, tt.wunstable)
		}
	}
}
