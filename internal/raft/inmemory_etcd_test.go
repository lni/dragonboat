// Copyright 2015 The etcd Authors
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

//
// inmemory_etcd_test.go is ported from etcd raft for testing purposes.
// some new tests are added by dragonboat authors
//
// tests in inmemory_etcd_test.go have been updated to reflect that inmemory.go
// is used to manage log entries that are likely to be used in immediate future,
// not just those have not been persisted yet.
//

package raft

import (
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestUnstableMaybeFirstIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// no snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			false, 0,
		},
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
		// has snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			true, 5,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Index: 4, Term: 1},
			true, 5,
		},
	}

	for i, tt := range tests {
		u := inMemory{
			entries:     tt.entries,
			markerIndex: tt.offset,
			snapshot:    tt.snap,
		}
		index, ok := u.getSnapshotIndex()
		if ok != tt.wok {
			t.Errorf("#%d: ok = %t, want %t", i, ok, tt.wok)
		}
		if ok {
			if index+1 != tt.windex {
				t.Errorf("#%d: index = %d, want %d", i, index, tt.windex)
			}
		}
	}
}

func TestMaybeLastIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// last in entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			true, 5,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			true, 5,
		},
		// last in snapshot
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Index: 4, Term: 1},
			true, 4,
		},
		// empty inMemory
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
	}

	for i, tt := range tests {
		u := inMemory{
			entries:     tt.entries,
			markerIndex: tt.offset,
			snapshot:    tt.snap,
		}
		index, ok := u.getLastIndex()
		if ok != tt.wok {
			t.Errorf("#%d: ok = %t, want %t", i, ok, tt.wok)
		}
		if index != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.windex)
		}
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot
		index   uint64

		wok   bool
		wterm uint64
	}{
		// term from entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			5,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			6,
			false, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			4,
			false, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			5,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			6,
			false, 0,
		},
		// term from snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			4,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			3,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Index: 4, Term: 1},
			5,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Index: 4, Term: 1},
			4,
			true, 1,
		},
		{
			[]pb.Entry{}, 0, nil,
			5,
			false, 0,
		},
	}

	for i, tt := range tests {
		u := inMemory{
			entries:     tt.entries,
			markerIndex: tt.offset,
			snapshot:    tt.snap,
		}
		term, ok := u.getTerm(tt.index)
		if ok != tt.wok {
			t.Errorf("#%d: ok = %t, want %t", i, ok, tt.wok)
		}
		if term != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
		}
	}
}

func TestUnstableRestore(t *testing.T) {
	u := inMemory{
		entries:     []pb.Entry{{Index: 5, Term: 1}},
		markerIndex: 5,
		snapshot:    &pb.Snapshot{Index: 4, Term: 1},
	}
	s := pb.Snapshot{Index: 6, Term: 2}
	u.restore(s)

	if u.markerIndex != s.Index+1 {
		t.Errorf("offset = %d, want %d", u.markerIndex, s.Index+1)
	}
	if len(u.entries) != 0 {
		t.Errorf("len = %d, want 0", len(u.entries))
	}
	if !reflect.DeepEqual(u.snapshot, &s) {
		t.Errorf("snap = %v, want %v", u.snapshot, &s)
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	tests := []struct {
		entries  []pb.Entry
		offset   uint64
		snap     *pb.Snapshot
		toappend []pb.Entry

		woffset  uint64
		wentries []pb.Entry
	}{
		// append to the end
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
		},
		// replace the inMemory entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
			4, []pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		// truncate the existing entries and append
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 6, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 8, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 2}, {Index: 8, Term: 2}},
		},
	}

	for i, tt := range tests {
		u := inMemory{
			entries:     tt.entries,
			markerIndex: tt.offset,
			snapshot:    tt.snap,
		}
		u.merge(tt.toappend)
		if u.markerIndex != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.markerIndex, tt.woffset)
		}
		if !reflect.DeepEqual(u.entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, u.entries, tt.wentries)
		}
	}
}

// added by dragonboat authors
func TestEntryMergeThreadSafety(t *testing.T) {
	tests := []struct {
		entries  []pb.Entry
		marker   uint64
		merge    []pb.Entry
		expIndex uint64
		expTerm  uint64
	}{
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5,
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 7, Term: 2}},
			7, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5,
			[]pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}},
			5, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5,
			[]pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
			5, 1,
		},
	}

	for idx, tt := range tests {
		im := inMemory{
			entries:     tt.entries,
			markerIndex: tt.marker,
		}
		old := im.entries[0:]
		im.merge(tt.merge)
		for _, e := range old {
			if e.Index == tt.expIndex {
				if e.Term != tt.expTerm {
					t.Errorf("%d, entry term unexpectedly changed", idx)
				}
			}
		}
	}
}

func TestUnstableStableTo(t *testing.T) {
	tests := []struct {
		entries     []pb.Entry
		offset      uint64
		snap        *pb.Snapshot
		index, term uint64
		savedTo     uint64
		woffset     uint64
		wlen        int
	}{
		{
			[]pb.Entry{}, 0, nil,
			5, 1,
			0,
			0, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			5, 1, // stable to the first entry
			5,
			6, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, nil,
			5, 1, // stable to the first entry
			5,
			6, 1,
		},
		{
			[]pb.Entry{{Index: 6, Term: 2}}, 6, nil,
			6, 1, // stable to the first entry and term mismatch
			0,
			7, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			4, 1, // stable to old entry
			0,
			5, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			4, 2, // stable to old entry
			0,
			5, 1,
		},
		// with snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			5, 1, // stable to the first entry
			5,
			6, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			5, 1, // stable to the first entry
			5,
			6, 1,
		},
		{
			[]pb.Entry{{Index: 6, Term: 2}}, 6, &pb.Snapshot{Index: 5, Term: 1},
			6, 1, // stable to the first entry and term mismatch
			0,
			7, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Index: 4, Term: 1},
			4, 1, // stable to snapshot
			0,
			5, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 2}}, 5, &pb.Snapshot{Index: 4, Term: 2},
			4, 1, // stable to old entry
			0,
			5, 1,
		},
	}

	for i, tt := range tests {
		u := inMemory{
			entries:     tt.entries,
			markerIndex: tt.offset,
			snapshot:    tt.snap,
		}
		u.savedLogTo(tt.index, tt.term)
		u.appliedLogTo(tt.index)
		if u.savedTo != tt.savedTo {
			t.Errorf("#%d: savedTo = %d, want %d", i, u.savedTo, tt.savedTo)
		}
		if u.markerIndex != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.markerIndex, tt.woffset)
		}
		if len(u.entries) != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(u.entries), tt.wlen)
		}
	}
}
