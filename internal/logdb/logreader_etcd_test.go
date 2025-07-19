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
	"github.com/stretchr/testify/require"
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
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4},
		{Index: 5, Term: 5}, {Index: 6, Term: 6},
	}
	tests := []struct {
		lo, hi, maxsize uint64
		werr            error
		wentries        []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []pb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []pb.Entry{
			{Index: 4, Term: 4}, {Index: 5, Term: 5},
		}},
		{4, 7, math.MaxUint64, nil, []pb.Entry{
			{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6},
		}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []pb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit()),
			nil, []pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() +
			ents[3].SizeUpperLimit()/2), nil,
			[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() +
			ents[3].SizeUpperLimit() - 1), nil,
			[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].SizeUpperLimit() + ents[2].SizeUpperLimit() +
			ents[3].SizeUpperLimit()), nil,
			[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		s := getTestLogReader(ents, fs)
		entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		require.Equal(t, tt.werr, err, "#%d: err mismatch", i)
		require.Equal(t, tt.wentries, entries, "#%d: entries mismatch", i)
		require.NoError(t, s.logdb.Close())
		deleteTestDB(fs)
	}
}

func TestLogReaderTerm(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderTerm(t, fs)
}

func testLogReaderTerm(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
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
		if tt.wpanic {
			require.Panics(t, func() {
				_, err := s.Term(tt.i)
				require.NoError(t, err)
			}, "#%d: expected panic", i)
		} else {
			require.NotPanics(t, func() {
				term, err := s.Term(tt.i)
				require.Equal(t, tt.werr, err, "#%d: err mismatch", i)
				require.Equal(t, tt.wterm, term, "#%d: term mismatch", i)
			}, "#%d: unexpected panic", i)
		}
		require.NoError(t, s.logdb.Close())
		deleteTestDB(fs)
	}
}

func TestLogReaderLastIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderLastIndex(t, fs)
}

func testLogReaderLastIndex(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
	s := getTestLogReader(ents, fs)
	_, last := s.GetRange()
	require.Equal(t, uint64(5), last, "last index mismatch")
	require.NoError(t, s.Append([]pb.Entry{{Index: 6, Term: 5}}))
	_, last = s.GetRange()
	require.Equal(t, uint64(6), last, "last index after append mismatch")
	require.NoError(t, s.logdb.Close())
	deleteTestDB(fs)
}

func TestLogReaderFirstIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderFirstIndex(t, fs)
}

func testLogReaderFirstIndex(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
	s := getTestLogReader(ents, fs)
	first, _ := s.GetRange()
	require.Equal(t, uint64(4), first, "first index mismatch")
	_, li := s.GetRange()
	require.Equal(t, uint64(5), li, "last index mismatch")
	require.NoError(t, s.Compact(4))
	first, _ = s.GetRange()
	require.Equal(t, uint64(5), first, "first index after compact mismatch")
	_, li = s.GetRange()
	require.Equal(t, uint64(5), li, "last index after compact mismatch")
	require.NoError(t, s.logdb.Close())
	deleteTestDB(fs)
}

func TestLogReaderAppend(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testLogReaderAppend(t, fs)
}

func testLogReaderAppend(t *testing.T, fs vfs.IFS) {
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
	tests := []struct {
		entries  []pb.Entry
		werr     error
		wentries []pb.Entry
	}{
		{
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
			},
			nil,
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
			},
		},
		{
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6},
			},
			nil,
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6},
			},
		},
		{
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 4},
				{Index: 5, Term: 5}, {Index: 6, Term: 5},
			},
			nil,
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 4},
				{Index: 5, Term: 5}, {Index: 6, Term: 5},
			},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]pb.Entry{
				{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5},
			},
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
			[]pb.Entry{
				{Index: 3, Term: 3}, {Index: 4, Term: 4},
				{Index: 5, Term: 5}, {Index: 6, Term: 5},
			},
		},
	}
	for i, tt := range tests {
		s := getTestLogReader(ents, fs)
		require.NoError(t, s.Append(tt.entries))
		// put tt.entries to logdb
		ud := pb.Update{
			EntriesToSave: tt.entries,
			ShardID:       LogReaderTestShardID,
			ReplicaID:     LogReaderTestReplicaID,
		}
		require.NoError(t, s.logdb.SaveRaftState([]pb.Update{ud}, 1))
		bfi := tt.wentries[0].Index - 1
		_, err := s.Term(bfi)
		require.Error(t, err, "expected error for index %d", bfi)
		ali := tt.wentries[len(tt.wentries)-1].Index + 1
		_, err = s.Term(ali)
		require.Error(t, err, "expected error for index %d", ali)
		for ii, e := range tt.wentries {
			if e.Index == 6 {
				plog.Infof("going to check term for index 6")
			}
			term, err := s.Term(e.Index)
			if e.Index == 6 {
				plog.Infof("Term returned")
			}
			require.NoError(t, err, "idx %d, ii %d Term() failed", i, ii)
			require.Equal(t, e.Term, term, "term mismatch for index %d", e.Index)
		}
		require.NoError(t, s.logdb.Close())
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
	require.NoError(t, err, "#%d: unexpected error", i)
	fi, _ := s.GetRange()
	require.Equal(t, uint64(5), fi, "first index mismatch")
	_, li := s.GetRange()
	require.Equal(t, uint64(4), li, "last index mismatch")
	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = s.ApplySnapshot(tt)
	require.Equal(t, raft.ErrSnapshotOutOfDate, err, "#%d: error mismatch", i)
	require.NoError(t, s.logdb.Close())
	deleteTestDB(fs)
}

func TestLogReaderCreateSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
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
		require.Equal(t, tt.werr, err, "#%d: error mismatch", i)
		require.Equal(t, tt.wsnap.Index, s.snapshot.Index,
			"#%d: snapshot index mismatch", i)
		require.NoError(t, s.logdb.Close())
		deleteTestDB(fs)
	}
}

func TestLogReaderSetRange(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
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
		require.Equal(t, tt.expMarkerIndex, s.markerIndex,
			"%d: marker index mismatch", idx)
		require.Equal(t, tt.expLength, s.length,
			"%d: length mismatch", idx)
		require.NoError(t, s.logdb.Close())
		deleteTestDB(fs)
	}
}

func TestLogReaderGetSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	ss := pb.Snapshot{Index: 4, Term: 4, Membership: *cs}
	s := getTestLogReader(ents, fs)
	defer deleteTestDB(fs)
	defer func() {
		require.NoError(t, s.logdb.Close())
	}()
	require.NoError(t, s.ApplySnapshot(ss), "create snapshot failed")
	rs := s.Snapshot()
	require.Equal(t, ss.Index, rs.Index, "unexpected snapshot record")
}

func TestLogReaderInitialState(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	ents := []pb.Entry{
		{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5},
	}
	cs := &pb.Membership{
		Addresses: map[uint64]string{1: "", 2: "", 3: ""},
	}
	ss := pb.Snapshot{Index: 4, Term: 4, Membership: *cs}
	s := getTestLogReader(ents, fs)
	defer deleteTestDB(fs)
	defer func() {
		require.NoError(t, s.logdb.Close())
	}()
	require.NoError(t, s.ApplySnapshot(ss), "create snapshot failed")
	ps := pb.State{
		Term:   2,
		Vote:   3,
		Commit: 5,
	}
	s.SetState(ps)
	rps, ms := s.NodeState()
	require.True(t, reflect.DeepEqual(&ms, cs), "membership mismatch")
	require.True(t, reflect.DeepEqual(&rps, &ps), "state mismatch")
}
