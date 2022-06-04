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

package raft

import (
	"math"
	"testing"

	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestInMemCheckMarkerIndex(t *testing.T) {
	im := inMemory{markerIndex: 10}
	im.checkMarkerIndex()
	im = inMemory{
		entries: []pb.Entry{
			{Index: 1, Term: 2},
			{Index: 2, Term: 2},
		},
		markerIndex: 1,
	}
	im.checkMarkerIndex()
}

func TestInMemCheckMarkerIndexPanicOnInvalidInMem(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	im := inMemory{
		entries: []pb.Entry{
			{Index: 1, Term: 2},
			{Index: 2, Term: 2},
		},
		markerIndex: 2,
	}
	im.checkMarkerIndex()
}

func TestInMemGetSnapshotIndex(t *testing.T) {
	im := inMemory{}
	if idx, ok := im.getSnapshotIndex(); ok || idx != 0 {
		t.Errorf("invalid result %t, %d", ok, idx)
	}
	im = inMemory{
		snapshot: &pb.Snapshot{
			Index: 100,
		},
	}
	if idx, ok := im.getSnapshotIndex(); !ok || idx != 100 {
		t.Errorf("invalid result %t, %d", ok, idx)
	}
}

func testGetEntriesPanicWithInvalidInput(low uint64,
	high uint64, marker uint64, firstIndex uint64, length uint64, t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	im := &inMemory{
		markerIndex: marker,
		entries:     make([]pb.Entry, 0),
	}
	for i := firstIndex; i < firstIndex+length; i++ {
		im.entries = append(im.entries, pb.Entry{Index: i, Term: 1})
	}
	im.getEntries(low, high)
}

func TestInMemGetEntriesPanicWithInvalidInput(t *testing.T) {
	tests := []struct {
		low        uint64
		high       uint64
		marker     uint64
		firstIndex uint64
		length     uint64
	}{
		{10, 9, 10, 10, 10},  // low > high
		{10, 11, 11, 10, 10}, // low < markerIndex
		{10, 11, 5, 5, 5},    // high > upperBound
	}
	for _, tt := range tests {
		testGetEntriesPanicWithInvalidInput(tt.low,
			tt.high, tt.marker, tt.firstIndex, tt.length, t)
	}
}

func TestInMemGetEntries(t *testing.T) {
	im := &inMemory{
		markerIndex: 2,
		entries: []pb.Entry{
			{Index: 2, Term: 2},
			{Index: 3, Term: 2},
		},
	}
	ents := im.getEntries(2, 3)
	if len(ents) != 1 {
		t.Errorf("ents len %d", len(ents))
	}
	ents = im.getEntries(2, 4)
	if len(ents) != 2 {
		t.Errorf("ents len %d", len(ents))
	}
}

func TestInMemGetLastIndexReturnSnapshotIndexWithEmptyEntries(t *testing.T) {
	for _, idx := range []uint64{100, 200, 205} {
		im := inMemory{
			snapshot: &pb.Snapshot{
				Index: idx,
			},
		}
		if index, ok := im.getLastIndex(); !ok || index != idx {
			t.Errorf("index %d, want %d, ok %t", index, idx, ok)
		}
	}
	in := inMemory{}
	index, ok := in.getLastIndex()
	if ok || index != 0 {
		t.Errorf("unexpected last index")
	}
}

func TestInMemGetLastIndex(t *testing.T) {
	tests := []struct {
		first  uint64
		length uint64
	}{
		{100, 5},
		{1, 100},
	}
	for idx, tt := range tests {
		im := inMemory{
			entries: make([]pb.Entry, 0),
		}
		for i := tt.first; i < tt.first+tt.length; i++ {
			im.entries = append(im.entries, pb.Entry{Index: i, Term: 1})
		}
		index, ok := im.getLastIndex()
		if !ok || index != tt.first+tt.length-1 {
			t.Errorf("%d, ok %t, index %d, want %d", idx, ok, index, tt.first+tt.length-1)
		}
	}
}

func TestInMemGetTermReturnSnapshotTerm(t *testing.T) {
	tests := []struct {
		markerIndex uint64
		ssIndex     uint64
		ssTerm      uint64
		index       uint64
		term        uint64
		ok          bool
	}{
		{10, 0, 0, 5, 0, false},
		{10, 5, 2, 5, 2, true},
		{10, 5, 2, 4, 0, false},
		{10, 5, 2, 10, 0, false},
	}
	for idx, tt := range tests {
		im := inMemory{
			markerIndex: tt.markerIndex,
			snapshot: &pb.Snapshot{
				Index: tt.ssIndex,
				Term:  tt.ssTerm,
			},
		}
		if im.snapshot.Index == 0 {
			im.snapshot = nil
		}
		r, ok := im.getTerm(tt.index)
		if r != tt.term {
			t.Errorf("%d, term %d, want %d", idx, r, tt.term)
		}
		if ok != tt.ok {
			t.Errorf("unexpected result")
		}
	}
}

func TestInMemGetTerm(t *testing.T) {
	tests := []struct {
		first  uint64
		length uint64
		index  uint64
		term   uint64
		ok     bool
	}{
		{100, 5, 103, 103, true},
		{100, 5, 104, 104, true},
		{100, 5, 105, 0, false},
	}
	for idx, tt := range tests {
		im := inMemory{
			markerIndex: tt.first,
			entries:     make([]pb.Entry, 0),
		}
		for i := tt.first; i < tt.first+tt.length; i++ {
			im.entries = append(im.entries, pb.Entry{Index: i, Term: i})
		}
		r, ok := im.getTerm(tt.index)
		if r != tt.term || ok != tt.ok {
			t.Errorf("%d, term %d, want %d, ok %t, want %t", idx, r, tt.term, ok, tt.ok)
		}
	}
}

func TestInMemRestore(t *testing.T) {
	im := inMemory{
		markerIndex: 10,
		entries: []pb.Entry{
			{Index: 10, Term: 1},
			{Index: 11, Term: 1},
		},
	}
	ss := pb.Snapshot{Index: 100}
	im.shrunk = true
	im.restore(ss)
	if im.shrunk {
		t.Errorf("shrunk flag not cleared")
	}
	if len(im.entries) != 0 || im.markerIndex != 101 || im.snapshot == nil {
		t.Errorf("unexpected im state")
	}
}

func TestInMemSaveSnapshotTo(t *testing.T) {
	im := inMemory{}
	im.savedSnapshotTo(10)
	im = inMemory{
		snapshot: &pb.Snapshot{Index: 100},
	}
	im.savedSnapshotTo(10)
	if im.snapshot == nil {
		t.Errorf("snapshot unexpected unset")
	}
	im.savedSnapshotTo(100)
	if im.snapshot != nil {
		t.Errorf("snapshot not unset")
	}
}

func testInMemMergeFullAppend(t *testing.T, shrunk bool) {
	im := inMemory{
		markerIndex: 5,
	}
	im.resize()
	im.entries = append(im.entries, []pb.Entry{
		{Index: 5, Term: 5},
		{Index: 6, Term: 6},
		{Index: 7, Term: 7},
	}...)
	im.shrunk = shrunk
	ents := []pb.Entry{
		{Index: 8, Term: 8},
		{Index: 9, Term: 9},
	}
	im.merge(ents)
	if im.shrunk != shrunk {
		t.Errorf("shrunk flag unexpectedly changed, %t:%t", im.shrunk, shrunk)
	}
	if len(im.entries) != 5 || im.markerIndex != 5 {
		t.Errorf("not fully appended")
	}
	if idx, ok := im.getLastIndex(); !ok || idx != 9 {
		t.Errorf("last index %d, want 9", idx)
	}
}

func TestInMemMergeFullAppend(t *testing.T) {
	testInMemMergeFullAppend(t, false)
	testInMemMergeFullAppend(t, true)
}

func TestInMemMergeReplace(t *testing.T) {
	im := inMemory{
		markerIndex: 5,
	}
	im.resize()
	im.entries = append(im.entries, []pb.Entry{
		{Index: 5, Term: 5},
		{Index: 6, Term: 6},
		{Index: 7, Term: 7},
	}...)
	im.shrunk = true
	ents := []pb.Entry{
		{Index: 2, Term: 2},
		{Index: 3, Term: 3},
	}
	im.merge(ents)
	if im.shrunk {
		t.Errorf("shrunk flag unexpectedly not cleared")
	}
	if len(im.entries) != 2 || im.markerIndex != 2 {
		t.Errorf("not fully appended")
	}
	if idx, ok := im.getLastIndex(); !ok || idx != 3 {
		t.Errorf("last index %d, want 3", idx)
	}
}

func TestInMemMergeWithHoleCausePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	im := inMemory{
		markerIndex: 5,
		entries: []pb.Entry{
			{Index: 5, Term: 5},
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
		},
	}
	ents := []pb.Entry{
		{Index: 9, Term: 9},
		{Index: 10, Term: 10},
	}
	im.merge(ents)
}

func TestInMemMerge(t *testing.T) {
	im := inMemory{
		markerIndex: 5,
	}
	im.resize()
	im.entries = append(im.entries, []pb.Entry{
		{Index: 5, Term: 5},
		{Index: 6, Term: 6},
		{Index: 7, Term: 7},
	}...)
	im.shrunk = true
	ents := []pb.Entry{
		{Index: 6, Term: 7},
		{Index: 7, Term: 10},
	}
	im.merge(ents)
	if im.shrunk {
		t.Errorf("shrunk flag unexpectedly not cleared")
	}
	if len(im.entries) != 3 || im.markerIndex != 5 {
		t.Errorf("not fully appended")
	}
	if idx, ok := im.getLastIndex(); !ok || idx != 7 {
		t.Errorf("last index %d, want 3", idx)
	}
	if term, ok := im.getTerm(6); !ok || term != 7 {
		t.Errorf("unexpected term %d, want 7", term)
	}
	if term, ok := im.getTerm(7); !ok || term != 10 {
		t.Errorf("unexpected term %d, want 10", term)
	}
}

func TestInMemEntriesToSaveReturnNotSavedEntries(t *testing.T) {
	im := inMemory{
		markerIndex: 5,
		entries: []pb.Entry{
			{Index: 5, Term: 5},
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
		},
	}
	im.savedTo = 4
	ents := im.entriesToSave()
	if len(ents) != 3 {
		t.Errorf("didn't return all entries")
	}
	if ents[0].Index != 5 {
		t.Errorf("unexpected first entry")
	}
	im.savedTo = 5
	ents = im.entriesToSave()
	if len(ents) != 2 {
		t.Errorf("didn't return all entries")
	}
	im.savedTo = 7
	ents = im.entriesToSave()
	if len(ents) != 0 {
		t.Errorf("unexpected entries returned")
	}
	im.savedTo = 8
	ents = im.entriesToSave()
	if len(ents) != 0 {
		t.Errorf("unexpected entries returned")
	}
}

func TestInMemSaveLogToUpdatesSaveTo(t *testing.T) {
	tests := []struct {
		index   uint64
		term    uint64
		savedTo uint64
	}{
		{4, 1, 4},
		{8, 1, 4},
		{6, 7, 4},
		{6, 6, 6},
	}
	for idx, tt := range tests {
		im := inMemory{
			markerIndex: 5,
			entries: []pb.Entry{
				{Index: 5, Term: 5},
				{Index: 6, Term: 6},
				{Index: 7, Term: 7},
			},
			savedTo: 4,
		}
		im.savedLogTo(tt.index, tt.term)
		if im.savedTo != tt.savedTo {
			t.Errorf("%d, savedTo %d, want %d", idx, im.savedTo, tt.savedTo)
		}
	}
}

func TestInMemSetSaveToWhenRestoringSnapshot(t *testing.T) {
	ss := pb.Snapshot{Index: 100, Term: 10}
	im := inMemory{
		markerIndex: 5,
		entries: []pb.Entry{
			{Index: 5, Term: 5},
		},
		savedTo: 4,
	}
	im.restore(ss)
	if im.savedTo != ss.Index {
		t.Errorf("savedTo %d, want %d", im.savedTo, ss.Index)
	}
}

func TestInMemMergeSetSaveTo(t *testing.T) {
	im := inMemory{
		markerIndex: 6,
		entries: []pb.Entry{
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
		},
		savedTo: 5,
	}
	ents := []pb.Entry{{Index: 6, Term: 6}, {Index: 7, Term: 8}}
	im.merge(ents)
	if im.savedTo != 5 {
		t.Errorf("savedTo %d, want 5", im.savedTo)
	}
	im = inMemory{
		markerIndex: 5,
		entries: []pb.Entry{
			{Index: 5, Term: 5},
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
			{Index: 8, Term: 8},
			{Index: 9, Term: 9},
			{Index: 10, Term: 10},
		},
		savedTo: 4,
	}
	im.merge(ents)
	if im.savedTo != 4 {
		t.Errorf("savedTo %d, want 4", im.savedTo)
	}
	im = inMemory{
		markerIndex: 5,
		entries: []pb.Entry{
			{Index: 5, Term: 5},
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
			{Index: 8, Term: 8},
			{Index: 9, Term: 9},
			{Index: 10, Term: 10},
		},
		savedTo: 6,
	}
	im.merge(ents)
	if im.savedTo != 5 {
		t.Errorf("savedTo %d, want 5", im.savedTo)
	}
	im = inMemory{
		markerIndex: 6,
		entries: []pb.Entry{
			{Index: 6, Term: 6},
			{Index: 7, Term: 7},
		},
		savedTo: 5,
	}
	ents = []pb.Entry{{Index: 8, Term: 8}, {Index: 9, Term: 9}}
	im.merge(ents)
	if im.savedTo != 5 {
		t.Errorf("savedTo %d, want 5", im.savedTo)
	}
}

func TestAppliedLogTo(t *testing.T) {
	tests := []struct {
		appliedTo  uint64
		length     int
		firstIndex uint64
	}{
		{4, 6, 5},
		{5, 5, 6},
		{11, 6, 5},
		{6, 4, 7},
		{10, 0, 11},
	}
	for idx, tt := range tests {
		im := inMemory{
			markerIndex: 5,
			entries: []pb.Entry{
				{Index: 5, Term: 5},
				{Index: 6, Term: 6},
				{Index: 7, Term: 7},
				{Index: 8, Term: 8},
				{Index: 9, Term: 9},
				{Index: 10, Term: 10},
			},
			savedTo: 4,
		}
		im.appliedLogTo(tt.appliedTo)
		if len(im.entries) != tt.length {
			t.Errorf("%d, unexpected entry slice len %d, want %d",
				idx, len(im.entries), tt.length)
		}
		if len(im.entries) > 0 && im.entries[0].Index != tt.firstIndex {
			t.Errorf("%d, unexpected first index %d, want %d",
				idx, im.entries[0].Index, tt.firstIndex)
		}
	}
}

func TestRateLimited(t *testing.T) {
	tests := []struct {
		rl      *server.InMemRateLimiter
		limited bool
	}{
		{nil, false},
		{server.NewInMemRateLimiter(0), false},
		{server.NewInMemRateLimiter(math.MaxUint64), false},
		{server.NewInMemRateLimiter(1), true},
		{server.NewInMemRateLimiter(math.MaxUint64 - 1), true},
	}
	for idx, tt := range tests {
		im := newInMemory(0, tt.rl)
		if im.rateLimited() != tt.limited {
			t.Errorf("%d, rate limited %t, want %t", idx, im.rateLimited(), tt.limited)
		}
	}
}

func TestRateLimitClearedAfterRestoringSnapshot(t *testing.T) {
	im := newInMemory(0, server.NewInMemRateLimiter(10000))
	im.merge([]pb.Entry{{Cmd: make([]byte, 1024)}})
	if im.rl.Get() == 0 {
		t.Errorf("log size not updated")
	}
	im.restore(pb.Snapshot{})
	if im.rl.Get() != 0 {
		t.Errorf("log size not cleared")
	}
}

func TestRateLimitIsUpdatedAfterMergingEntries(t *testing.T) {
	im := newInMemory(0, server.NewInMemRateLimiter(10000))
	im.merge([]pb.Entry{{Index: 1, Cmd: make([]byte, 1024)}})
	logsz := im.rl.Get()
	ents := []pb.Entry{
		{Index: 2, Cmd: make([]byte, 16)},
		{Index: 3, Cmd: make([]byte, 64)},
	}
	addSz := getEntrySliceInMemSize(ents)
	im.merge(ents)
	if logsz+addSz != im.rl.Get() {
		t.Errorf("log size %d, want %d", im.rl.Get(), logsz+addSz)
	}
}

func TestRateLimitIsDecreasedAfterEntriesAreApplied(t *testing.T) {
	ents := []pb.Entry{
		{Index: 2, Cmd: make([]byte, 16)},
		{Index: 3, Cmd: make([]byte, 64)},
		{Index: 4, Cmd: make([]byte, 128)},
	}
	im := newInMemory(2, server.NewInMemRateLimiter(10000))
	im.merge(ents)
	if im.rl.Get() != getEntrySliceInMemSize(ents) {
		t.Errorf("unexpected log size")
	}
	for idx := uint64(2); idx < uint64(5); idx++ {
		im.appliedLogTo(idx)
		if len(im.entries) > 0 {
			if im.entries[0].Index != idx+1 {
				t.Errorf("alignment error")
			}
		}
		if im.rl.Get() != getEntrySliceInMemSize(im.entries) {
			t.Errorf("log size not updated")
		}
	}
}

func TestRateLimitCanBeResetWhenMergingEntries(t *testing.T) {
	ents := []pb.Entry{
		{Index: 2, Cmd: make([]byte, 16)},
		{Index: 3, Cmd: make([]byte, 64)},
		{Index: 4, Cmd: make([]byte, 128)},
	}
	im := newInMemory(2, server.NewInMemRateLimiter(10000))
	im.merge(ents)
	ents = []pb.Entry{
		{Index: 1, Cmd: make([]byte, 16)},
	}
	im.merge(ents)
	expSz := getEntrySliceInMemSize(ents)
	if im.rl.Get() != expSz {
		t.Errorf("log size %d, want %d", im.rl.Get(), expSz)
	}
}

func TestRateLimitCanBeUpdatedAfterCutAndMergingEntries(t *testing.T) {
	ents := []pb.Entry{
		{Index: 2, Cmd: make([]byte, 16)},
		{Index: 3, Cmd: make([]byte, 64)},
		{Index: 4, Cmd: make([]byte, 128)},
	}
	im := newInMemory(2, server.NewInMemRateLimiter(10000))
	im.merge(ents)
	ents = []pb.Entry{
		{Index: 3, Cmd: make([]byte, 1024)},
		{Index: 4, Cmd: make([]byte, 1024)},
	}
	im.merge(ents)
	expSz := getEntrySliceInMemSize(im.entries)
	if im.rl.Get() != expSz {
		t.Errorf("log size %d, want %d", im.rl.Get(), expSz)
	}
}

func TestResize(t *testing.T) {
	im := inMemory{
		markerIndex: 10,
		entries: []pb.Entry{
			{Index: 10, Term: 1},
			{Index: 11, Term: 1},
		},
		shrunk: true,
	}
	im.resize()
	if uint64(cap(im.entries)) != entrySliceSize {
		t.Errorf("not resized")
	}
	if len(im.entries) != 2 {
		t.Errorf("unexpected len %d", len(im.entries))
	}
	if im.shrunk {
		t.Errorf("shrunk flag not clearaed")
	}
}

func TestTryResize(t *testing.T) {
	im := inMemory{
		markerIndex: 10,
		entries: []pb.Entry{
			{Index: 10, Term: 1},
			{Index: 11, Term: 1},
		},
	}
	initcap := cap(im.entries)
	initlen := len(im.entries)
	im.tryResize()
	if cap(im.entries) != initcap || len(im.entries) != initlen {
		t.Errorf("cap/len unexpectedly changed")
	}
	im.shrunk = true
	im.tryResize()
	if cap(im.entries) == initcap {
		t.Errorf("cap/len unexpectedly not changed")
	}
}

func TestNewEntrySlice(t *testing.T) {
	tests := []struct {
		input uint64
		oCap  uint64
		oLen  uint64
	}{
		{entrySliceSize, entrySliceSize, entrySliceSize},
		{entrySliceSize - 1, entrySliceSize, entrySliceSize - 1},
		{entrySliceSize + 1, entrySliceSize + 1, entrySliceSize + 1},
	}
	for idx, tt := range tests {
		ents := make([]pb.Entry, tt.input)
		im := inMemory{}
		output := im.newEntrySlice(ents)
		if uint64(cap(output)) != tt.oCap {
			t.Errorf("%d, unexpected cap %d, want %d", idx, cap(output), tt.oCap)
		}
		if uint64(len(output)) != tt.oLen {
			t.Errorf("%d, unexpected len %d, want %d", idx, len(output), tt.oLen)
		}
	}
}
