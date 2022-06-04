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

package logdb

import (
	"math"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v4/internal/raft"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/leaktest"
)

// most tests below are ported from etcd rafts

const (
	LogReaderTestShardID   uint64 = 2
	LogReaderTestReplicaID uint64 = 12345
)

func getNewLogReaderTestDB(entries []pb.Entry, fs vfs.IFS) raftio.ILogDB {
	logdb := getNewTestDB("db-dir", "wal-db-dir", false, fs)
	ud := pb.Update{
		EntriesToSave: entries,
		ShardID:       LogReaderTestShardID,
		ReplicaID:     LogReaderTestReplicaID,
	}
	if err := logdb.SaveRaftState([]pb.Update{ud}, 1); err != nil {
		panic(err)
	}
	return logdb
}

func getTestLogReader(entries []pb.Entry, fs vfs.IFS) *LogReader {
	logdb := getNewLogReaderTestDB(entries, fs)
	ls := NewLogReader(LogReaderTestShardID, LogReaderTestReplicaID, logdb)
	ls.SetCompactor(testCompactor)
	ls.markerIndex = entries[0].Index
	ls.markerTerm = entries[0].Term
	ls.length = uint64(len(entries))
	return ls
}

func TestLogReaderEntries(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderEntries(t, fs)
}

func testLogReaderEntries(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		lo, hi, maxsize uint64
		werr            error
		wentries        []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() + ents[3].SizeUpperLimit()/2), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() + ents[3].SizeUpperLimit() - 1), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() + ents[3].SizeUpperLimit()), nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		s := getTestLogReader(ents, fs)
		entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
		s.logdb.Close()
		deleteTestDB(fs)
	}
}

func TestLogReaderTerm(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderTerm(t, fs)
}

func testLogReaderTerm(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i      uint64
		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}
	for i, tt := range tests {
		s := getTestLogReader(ents, fs)
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			term, err := s.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
		s.logdb.Close()
		deleteTestDB(fs)
	}
}

func TestLogReaderLastIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderLastIndex(t, fs)
}

func testLogReaderLastIndex(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s := getTestLogReader(ents, fs)
	_, last := s.GetRange()
	if last != 5 {
		t.Errorf("term = %d, want %d", last, 5)
	}
	if err := s.Append([]pb.Entry{{Index: 6, Term: 5}}); err != nil {
		t.Fatalf("%v", err)
	}
	_, last = s.GetRange()
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 5)
	}
	s.logdb.Close()
	deleteTestDB(fs)
}

func TestLogReaderFirstIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderFirstIndex(t, fs)
}

func testLogReaderFirstIndex(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	s := getTestLogReader(ents, fs)
	first, _ := s.GetRange()
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}
	_, li := s.GetRange()
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
	if err := s.Compact(4); err != nil {
		t.Fatalf("%v", err)
	}
	first, _ = s.GetRange()
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
	_, li = s.GetRange()
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
	s.logdb.Close()
	deleteTestDB(fs)
}

func TestLogReaderAppend(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderAppend(t, fs)
}

func testLogReaderAppend(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		entries  []pb.Entry
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
		s := getTestLogReader(ents, fs)
		if err := s.Append(tt.entries); err != nil {
			t.Fatalf("%v", err)
		}
		// put tt.entries to logdb
		ud := pb.Update{
			EntriesToSave: tt.entries,
			ShardID:       LogReaderTestShardID,
			ReplicaID:     LogReaderTestReplicaID,
		}
		if err := s.logdb.SaveRaftState([]pb.Update{ud}, 1); err != nil {
			t.Fatalf("%v", err)
		}
		bfi := tt.wentries[0].Index - 1
		_, err := s.Term(bfi)
		if err == nil {
			t.Errorf("suppose to fail")
		}
		ali := tt.wentries[len(tt.wentries)-1].Index + 1
		_, err = s.Term(ali)
		if err == nil {
			t.Errorf("suppose to fail, but it didn't fail, i %d", i)
		}
		for ii, e := range tt.wentries {
			if e.Index == 6 {
				plog.Infof("going to check term for index 6")
			}
			term, err := s.Term(e.Index)
			if e.Index == 6 {
				plog.Infof("Term returned")
			}
			if err != nil {
				t.Errorf("idx %d, ii %d Term() failed", i, ii)
			}
			if term != e.Term {
				t.Errorf("term %d, want %d", term, e.Term)
			}
		}
		s.logdb.Close()
		deleteTestDB(fs)
	}
}

func TestLogReaderApplySnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{{Index: 0, Term: 0}}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	tests := []pb.Snapshot{
		{Index: 4, Term: 4, Membership: *cs},
		{Index: 3, Term: 3, Membership: *cs},
	}
	s := getTestLogReader(ents, fs)
	//Apply Snapshot successful
	i := 0
	tt := tests[i]
	err := s.ApplySnapshot(tt)
	if err != nil {
		t.Errorf("#%d: err = %v, want %v", i, err, nil)
	}
	if fi, _ := s.GetRange(); fi != 5 {
		t.Errorf("first index %d, want 5", fi)
	}
	if _, li := s.GetRange(); li != 4 {
		t.Errorf("last index %d, want 4", li)
	}
	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = s.ApplySnapshot(tt)
	if err != raft.ErrSnapshotOutOfDate {
		t.Errorf("#%d: err = %v, want %v", i, err, raft.ErrSnapshotOutOfDate)
	}
	s.logdb.Close()
	deleteTestDB(fs)
}

func TestLogReaderCreateSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	tests := []struct {
		i     uint64
		term  uint64
		werr  error
		wsnap pb.Snapshot
	}{
		{4, 4, nil, pb.Snapshot{Index: 4, Term: 4, Membership: *cs}},
		{5, 5, nil, pb.Snapshot{Index: 5, Term: 5, Membership: *cs}},
	}
	for i, tt := range tests {
		s := getTestLogReader(ents, fs)
		err := s.CreateSnapshot(tt.wsnap)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.snapshot.Index != tt.wsnap.Index {
			t.Errorf("#%d: snap = %+v, want %+v", i, s.snapshot, tt.wsnap)
		}
		s.logdb.Close()
		deleteTestDB(fs)
	}
}

func TestLogReaderSetRange(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		firstIndex     uint64
		length         uint64
		expLength      uint64
		expMarkerIndex uint64
	}{
		{2, 2, 3, 3},
		{2, 5, 4, 3},
		{3, 5, 5, 3},
		{6, 6, 9, 3},
	}
	for idx, tt := range tests {
		s := getTestLogReader(ents, fs)
		s.SetRange(tt.firstIndex, tt.length)
		if s.markerIndex != tt.expMarkerIndex {
			t.Errorf("%d, marker index %d, want %d", idx, s.markerIndex, tt.expMarkerIndex)
		}
		if s.length != tt.expLength {
			t.Errorf("%d, length %d, want %d", idx, s.length, tt.expLength)
		}
		s.logdb.Close()
		deleteTestDB(fs)
	}
}

func TestLogReaderGetSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	ss := pb.Snapshot{Index: 4, Term: 4, Membership: *cs}
	s := getTestLogReader(ents, fs)
	defer deleteTestDB(fs)
	defer s.logdb.Close()
	if err := s.ApplySnapshot(ss); err != nil {
		t.Errorf("create snapshot failed %v", err)
	}
	rs := s.Snapshot()
	if rs.Index != ss.Index {
		t.Errorf("unexpected snapshot rec")
	}
}

func TestLogReaderInitialState(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	ss := pb.Snapshot{Index: 4, Term: 4, Membership: *cs}
	s := getTestLogReader(ents, fs)
	defer deleteTestDB(fs)
	defer s.logdb.Close()
	if err := s.ApplySnapshot(ss); err != nil {
		t.Errorf("create snapshot failed %v", err)
	}
	ps := pb.State{
		Term:   2,
		Vote:   3,
		Commit: 5,
	}
	s.SetState(ps)
	rps, ms := s.NodeState()
	if !reflect.DeepEqual(&ms, cs) || !reflect.DeepEqual(&rps, &ps) {
		t.Errorf("initial state unexpected")
	}
}
