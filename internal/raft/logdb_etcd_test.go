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
// logdb_etcd_test.go is ported from etcd raft, it is used to test the
// TestLogDB struct in logdb_test.go - testing your tests is important!
// updates have been made to reflect the interface & implementation differences
//

package raft

import (
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestLogDBTerm(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i      uint64
		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, ErrUnavailable, 0, false},
	}

	for i, tt := range tests {
		v := make([]pb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerIndex: v[0].Index,
			markerTerm:  v[0].Term,
			entries:     v[1:],
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("#%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			plog.Infof("checking term for index %d", tt.i)
			term, err := s.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
	}
}

func TestLogDBLastIndex(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s := &TestLogDB{
		markerIndex: ents[0].Index,
		markerTerm:  ents[0].Term,
		entries:     ents[1:],
	}

	_, last := s.GetRange()
	if last != 5 {
		t.Errorf("term = %d, want %d", last, 5)
	}

	plog.Infof("going to append")
	if err := s.Append([]pb.Entry{{Index: 6, Term: 5}}); err != nil {
		t.Fatalf("%v", err)
	}
	_, last = s.GetRange()
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 6)
	}
}

func TestLogDBFirstIndex(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s := &TestLogDB{
		markerIndex: ents[0].Index,
		markerTerm:  ents[0].Term,
		entries:     ents[1:],
	}

	first, _ := s.GetRange()
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}

	if err := s.Compact(4); err != nil {
		t.Fatalf("%v", err)
	}
	first, _ = s.GetRange()
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}

func TestLogDBCompact(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, ErrCompacted, 3, 3, 3},
		{3, ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}
	for i, tt := range tests {
		v := make([]pb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerIndex: v[0].Index,
			markerTerm:  v[0].Term,
			entries:     v[1:],
		}
		err := s.Compact(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.markerIndex != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, s.markerIndex, tt.windex)
		}
		if s.markerTerm != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, s.markerTerm, tt.wterm)
		}
		if len(s.entries)+1 != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(s.entries), tt.wlen)
		}
	}
}

func TestLogDBCreateSnapshot(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	m := getTestMembership([]uint64{1, 2, 3})
	cs := &m

	tests := []struct {
		i uint64

		werr  error
		wsnap pb.Snapshot
	}{
		{4, nil, pb.Snapshot{Index: 4, Term: 4, Membership: *cs}},
		{5, nil, pb.Snapshot{Index: 5, Term: 5, Membership: *cs}},
	}

	for i, tt := range tests {
		v := make([]pb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerIndex: v[0].Index,
			markerTerm:  v[0].Term,
			entries:     v[1:],
		}
		snap, err := s.getSnapshot(tt.i, cs)
		if cerr := s.CreateSnapshot(snap); cerr != nil {
			t.Fatalf("%v", err)
		}
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(snap, tt.wsnap) {
			t.Errorf("#%d: snap = %+v, want %+v", i, snap, tt.wsnap)
		}
	}
}

func TestLogDBApplySnapshot(t *testing.T) {
	m := getTestMembership([]uint64{1, 2, 3})
	cs := &m

	tests := []pb.Snapshot{
		{Index: 4, Term: 4, Membership: *cs},
		{Index: 3, Term: 3, Membership: *cs},
	}

	s := &TestLogDB{}
	//Apply Snapshot successful
	i := 0
	tt := tests[i]
	err := s.ApplySnapshot(tt)
	if err != nil {
		t.Errorf("#%d: err = %v, want %v", i, err, nil)
	}

	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = s.ApplySnapshot(tt)
	if err != ErrSnapshotOutOfDate {
		t.Errorf("#%d: err = %v, want %v", i, err, ErrSnapshotOutOfDate)
	}
}

func TestLogDBAppend(t *testing.T) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		entries []pb.Entry

		werr     error
		wentries []pb.Entry
	}{
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// truncate the existing entries and append
		{
			[]pb.Entry{{Index: 4, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			[]pb.Entry{{Index: 6, Term: 5}},
			nil,
			[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}

	for i, tt := range tests {
		v := make([]pb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerIndex: v[0].Index,
			markerTerm:  v[0].Term,
			entries:     v[1:],
		}
		plog.Infof("appending #%d", i)
		err := s.Append(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(s.entries, tt.wentries[1:]) {
			t.Errorf("#%d: entries = %v, want %v", i, s.entries, tt.wentries[1:])
		}
	}
}
