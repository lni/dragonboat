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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

const (
	RDBTestDirectory = "db_test_dir_safe_to_delete"
)

func getDirSize(path string,
	includeLogSize bool, fs vfs.IFS) (int64, error) {
	var size int64
	results, err := fs.List(path)
	if err != nil {
		return 0, err
	}
	for _, v := range results {
		info, err := fs.Stat(fs.PathJoin(path, v))
		if err != nil {
			return 0, err
		}
		if !info.IsDir() {
			if !includeLogSize && strings.HasSuffix(info.Name(), ".log") {
				continue
			}
			size += info.Size()
		}
	}
	return size, err
}

func getNewTestDB(dir string,
	lldir string, batched bool, fs vfs.IFS) raftio.ILogDB {
	d := fs.PathJoin(RDBTestDirectory, dir)
	lld := fs.PathJoin(RDBTestDirectory, lldir)
	if err := fileutil.MkdirAll(d, fs); err != nil {
		panic(err)
	}
	if err := fileutil.MkdirAll(lld, fs); err != nil {
		panic(err)
	}
	expert := config.GetDefaultExpertConfig()
	expert.LogDB.Shards = 4
	expert.FS = fs
	cfg := config.NodeHostConfig{
		Expert: expert,
	}

	db, err := NewLogDB(cfg, nil,
		[]string{d}, []string{lld}, batched, false, newDefaultKVStore)
	if err != nil {
		panic(err)
	}
	return db
}

func deleteTestDB(fs vfs.IFS) {
	if err := fs.RemoveAll(RDBTestDirectory); err != nil {
		panic(err)
	}
}

func runLogDBTestAs(t *testing.T,
	batched bool, tf func(t *testing.T, db raftio.ILogDB), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	d := fs.PathJoin(RDBTestDirectory, dir)
	lld := fs.PathJoin(RDBTestDirectory, lldir)
	require.NoError(t, fs.RemoveAll(d))
	require.NoError(t, fs.RemoveAll(lld))
	db := getNewTestDB(dir, lldir, batched, fs)
	defer deleteTestDB(fs)
	defer func() {
		require.NoError(t, db.Close())
	}()
	tf(t, db)
}

func runLogDBTest(t *testing.T,
	tf func(t *testing.T, db raftio.ILogDB), fs vfs.IFS) {
	runLogDBTestAs(t, false, tf, fs)
	runLogDBTestAs(t, true, tf, fs)
}

func runBatchedLogDBTest(t *testing.T,
	tf func(t *testing.T, db raftio.ILogDB), fs vfs.IFS) {
	runLogDBTestAs(t, true, tf, fs)
}

func TestRDBReturnErrNoBootstrapInfoWhenNoBootstrap(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, db raftio.ILogDB) {
		_, err := db.GetBootstrapInfo(1, 2)
		require.ErrorIs(t, err, raftio.ErrNoBootstrapInfo)
	}
	runLogDBTest(t, tf, fs)
}

func TestBootstrapInfoCanBeSavedAndChecked(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		nodes := make(map[uint64]string)
		nodes[100] = "address1"
		nodes[200] = "address2"
		nodes[300] = "address3"
		bs := pb.Bootstrap{
			Join:      false,
			Addresses: nodes,
		}
		require.NoError(t, db.SaveBootstrapInfo(1, 2, bs))
		bootstrap, err := db.GetBootstrapInfo(1, 2)
		require.NoError(t, err)
		require.False(t, bootstrap.Join)
		require.Len(t, bootstrap.Addresses, 3)
		ni, err := db.ListNodeInfo()
		require.NoError(t, err)
		require.Len(t, ni, 1)
		require.Equal(t, uint64(1), ni[0].ShardID)
		require.Equal(t, uint64(2), ni[0].ReplicaID)
		require.NoError(t, db.SaveBootstrapInfo(2, 3, bs))
		require.NoError(t, db.SaveBootstrapInfo(3, 4, bs))
		ni, err = db.ListNodeInfo()
		require.NoError(t, err)
		require.Len(t, ni, 3)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSnapshotHasMaxIndexSet(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ud1 := pb.Update{
			EntriesToSave: []pb.Entry{{Index: 2}, {Index: 3}, {Index: 4}},
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud1}, 1)
		require.NoError(t, err)
		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 4)
		require.NoError(t, err)
		require.Equal(t, uint64(4), maxIndex)
		ud2 := pb.Update{
			ShardID:   3,
			ReplicaID: 4,
			Snapshot:  pb.Snapshot{Index: 3},
		}
		err = db.SaveRaftState([]pb.Update{ud2}, 1)
		require.NoError(t, err)
		maxIndex, err = p[3].getMaxIndex(3, 4)
		require.NoError(t, err)
		require.Equal(t, uint64(3), maxIndex)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSaveSnapshotTogetherWithUnexpectedEntriesWillPanic(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ud1 := pb.Update{
			EntriesToSave: []pb.Entry{{Index: 2}, {Index: 3}, {Index: 4}},
			ShardID:       3,
			ReplicaID:     4,
			Snapshot:      pb.Snapshot{Index: 5},
		}
		require.Panics(t, func() {
			err := db.SaveRaftState([]pb.Update{ud1}, 1)
			require.NoError(t, err)
		})
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSnapshotsSavedInSaveRaftState(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs1 := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		snapshot1 := pb.Snapshot{
			Filepath: "p1",
			FileSize: 100,
			Index:    5,
			Term:     1,
		}
		ud1 := pb.Update{
			EntriesToSave: []pb.Entry{e1},
			State:         hs1,
			ShardID:       3,
			ReplicaID:     4,
			Snapshot:      snapshot1,
		}
		hs2 := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e2 := pb.Entry{
			Term:  1,
			Index: 20,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		snapshot2 := pb.Snapshot{
			Filepath: "p2",
			FileSize: 200,
			Index:    12,
			Term:     1,
		}
		ud2 := pb.Update{
			EntriesToSave: []pb.Entry{e2},
			State:         hs2,
			ShardID:       3,
			ReplicaID:     3,
			Snapshot:      snapshot2,
		}
		uds := []pb.Update{ud1, ud2}
		err := db.SaveRaftState(uds, 1)
		require.NoError(t, err)
		v, err := db.GetSnapshot(3, 4)
		require.NoError(t, err)
		require.Equal(t, snapshot1.Index, v.Index)
		v, err = db.GetSnapshot(3, 3)
		require.NoError(t, err)
		require.Equal(t, snapshot2.Index, v.Index)

		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(20), maxIndex)
		maxIndex, err = p[3].getMaxIndex(3, 4)
		require.NoError(t, err)
		require.Equal(t, uint64(10), maxIndex)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSnapshotOnlyNodeIsHandledByReadRaftState(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ss := pb.Snapshot{
			Index: 100,
			Term:  2,
		}
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		ud := pb.Update{
			State:     hs,
			ShardID:   3,
			ReplicaID: 4,
			Snapshot:  ss,
		}
		require.NoError(t, db.SaveRaftState([]pb.Update{ud}, 1))
		rs, err := db.ReadRaftState(3, 4, ss.Index)
		require.NoError(t, err)
		require.Equal(t, uint64(0), rs.EntryCount)
		require.Equal(t, ss.Index, rs.FirstIndex)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestReadRaftStateReturnsNoSavedLogErrorWhenStateIsNeverSaved(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ss := pb.Snapshot{
			Index: 100,
			Term:  2,
		}
		ud := pb.Update{
			ShardID:   3,
			ReplicaID: 4,
			Snapshot:  ss,
		}
		require.NoError(t, db.SaveRaftState([]pb.Update{ud}, 1))
		_, err := db.ReadRaftState(3, 4, ss.Index)
		require.ErrorIs(t, err, raftio.ErrNoSavedLog)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestMaxIndexRuleIsEnforced(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		e2 := pb.Entry{
			Term:  2,
			Index: 3,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 2"),
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1, e2},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		rs, err := db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), rs.EntryCount)
		require.Equal(t, uint64(3), rs.FirstIndex)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSavedEntrieseAreOrderedByTheKey(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		ents := make([]pb.Entry, 0)
		for i := uint64(1); i < 1025; i++ {
			e := pb.Entry{
				Term:  2,
				Index: i,
				Type:  pb.ApplicationEntry,
				Cmd:   []byte("test-data"),
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		rs, err := db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1024), rs.EntryCount)
		re, _, err := db.IterateEntries([]pb.Entry{},
			0, 3, 4, 1, math.MaxUint64, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, re, 1024)
		lastIndex := re[0].Index
		for _, e := range re[1:] {
			require.Equal(t, lastIndex+1, e.Index)
			lastIndex = e.Index
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func testSaveRaftState(t *testing.T, db raftio.ILogDB) {
	hs := pb.State{
		Term:   2,
		Vote:   3,
		Commit: 100,
	}
	ud := pb.Update{
		State:     hs,
		ShardID:   3,
		ReplicaID: 4,
	}
	for i := uint64(1); i <= 10; i++ {
		term := uint64(1)
		if i > 5 {
			term = 2
		}
		e := pb.Entry{
			Term:  term,
			Index: i,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		ud.EntriesToSave = append(ud.EntriesToSave, e)
	}
	err := db.SaveRaftState([]pb.Update{ud}, 1)
	require.NoError(t, err)
	rs, err := db.ReadRaftState(3, 4, 0)
	require.NoError(t, err)
	require.False(t, reflect.DeepEqual(rs.State, raftio.RaftState{}))
	require.Equal(t, uint64(2), rs.State.Term)
	require.Equal(t, uint64(3), rs.State.Vote)
	require.Equal(t, uint64(100), rs.State.Commit)
	require.Equal(t, uint64(10), rs.EntryCount)
}

func TestSaveRaftState(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		testSaveRaftState(t, db)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestStateIsUpdated(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		rs, err := db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, hs.Term, rs.State.Term)
		require.Equal(t, hs.Vote, rs.State.Vote)
		require.Equal(t, hs.Commit, rs.State.Commit)
		hs2 := pb.State{
			Term:   3,
			Vote:   3,
			Commit: 100,
		}
		ud2 := pb.Update{
			EntriesToSave: []pb.Entry{},
			State:         hs2,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud2}, 1)
		require.NoError(t, err)
		rs, err = db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, hs2.Term, rs.State.Term)
		require.Equal(t, hs2.Vote, rs.State.Vote)
		require.Equal(t, hs2.Commit, rs.State.Commit)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestMaxIndexIsUpdated(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 4)
		require.NoError(t, err)
		require.Equal(t, uint64(10), maxIndex)
		e1 = pb.Entry{
			Term:  1,
			Index: 11,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e1},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		maxIndex, err = p[3].getMaxIndex(3, 4)
		require.NoError(t, err)
		require.Equal(t, uint64(11), maxIndex)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestReadAllEntriesOnlyReturnEntriesFromTheSpecifiedNode(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		e2 := pb.Entry{
			Term:  2,
			Index: 11,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 2"),
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1, e2},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		rs, err := db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(2), rs.EntryCount)
		// save the same data but with different node id
		ud.ReplicaID = 5
		err = db.SaveRaftState([]pb.Update{ud}, 2)
		require.NoError(t, err)
		rs, err = db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(2), rs.EntryCount)
		// save the same data but with different shard id
		ud.ReplicaID = 4
		ud.ShardID = 4
		err = db.SaveRaftState([]pb.Update{ud}, 3)
		require.NoError(t, err)
		rs, err = db.ReadRaftState(3, 4, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(2), rs.EntryCount)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestIterateEntriesOnlyReturnCurrentNodeEntries(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ents, _, err := db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, ents)
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 1"),
		}
		e2 := pb.Entry{
			Term:  2,
			Index: 11,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 2"),
		}
		e3 := pb.Entry{
			Term:  2,
			Index: 12,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 3"),
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1, e2, e3},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		// save the same data again but under a different node id
		ud.ReplicaID = 5
		err = db.SaveRaftState([]pb.Update{ud}, 2)
		require.NoError(t, err)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 3)
		// save the same data again but under a different shard id
		ud.ReplicaID = 4
		ud.ShardID = 4
		err = db.SaveRaftState([]pb.Update{ud}, 3)
		require.NoError(t, err)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 3)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestIterateEntries(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ents, _, err := db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, ents)
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		e1 := pb.Entry{
			Term:  1,
			Index: 10,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 1"),
		}
		e2 := pb.Entry{
			Term:  2,
			Index: 11,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 2"),
		}
		e3 := pb.Entry{
			Term:  2,
			Index: 12,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data 3"),
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1, e2, e3},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 11, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 1)
		require.Equal(t, uint64(10), ents[0].Index)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 3)
		ents, _, err = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, 0)
		require.NoError(t, err)
		require.Len(t, ents, 1)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 12, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 2)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 13, uint64(e1.Size()-1))
		require.NoError(t, err)
		require.Len(t, ents, 1)
		// write an entry with index 11
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			State:         hs,
			ShardID:       3,
			ReplicaID:     4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		ents, _, err = db.IterateEntries([]pb.Entry{},
			0, 3, 4, 10, 13, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 2)
		for _, ent := range ents {
			require.NotEqual(t, uint64(12), ent.Index)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSaveSnapshot(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		_, err := db.GetSnapshot(1, 2)
		require.NoError(t, err)
		s1 := pb.Snapshot{
			Index: 1,
			Term:  2,
		}
		s2 := pb.Snapshot{
			Index: 2,
			Term:  2,
		}
		rec1 := pb.Update{
			ShardID:   1,
			ReplicaID: 2,
			Snapshot:  s1,
		}
		rec2 := pb.Update{
			ShardID:   1,
			ReplicaID: 2,
			Snapshot:  s2,
		}
		require.NoError(t, db.SaveSnapshots([]pb.Update{rec1, rec2}))
		snapshot, err := db.GetSnapshot(1, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(2), snapshot.Index)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestOldSnapshotIsIgnored(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		rec0 := pb.Update{
			ShardID:   1,
			ReplicaID: 2,
			Snapshot:  pb.Snapshot{Index: 5},
		}
		rec1 := pb.Update{
			ShardID:   1,
			ReplicaID: 2,
			Snapshot:  pb.Snapshot{Index: 20},
		}
		rec2 := pb.Update{
			ShardID:   1,
			ReplicaID: 2,
			Snapshot:  pb.Snapshot{Index: 10},
		}
		require.NoError(t, db.SaveSnapshots([]pb.Update{rec0}))
		require.NoError(t, db.SaveSnapshots([]pb.Update{rec1}))
		require.NoError(t, db.SaveSnapshots([]pb.Update{rec2}))
		snapshot, err := db.GetSnapshot(1, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(20), snapshot.Index)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestParseNodeInfoKeyPanicOnUnexpectedKeySize(t *testing.T) {
	require.Panics(t, func() {
		parseNodeInfoKey(make([]byte, 21))
	})
}

func TestSaveEntriesWithIndexGap(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		e1 := pb.Entry{
			Term:  1,
			Index: 1,
			Type:  pb.ApplicationEntry,
		}
		e2 := pb.Entry{
			Term:  1,
			Index: 2,
			Type:  pb.ApplicationEntry,
		}
		ud := pb.Update{
			EntriesToSave: []pb.Entry{e1, e2},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		e1 = pb.Entry{
			Term:  1,
			Index: 4,
			Type:  pb.ApplicationEntry,
		}
		e2 = pb.Entry{
			Term:  1,
			Index: 5,
			Type:  pb.ApplicationEntry,
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e1, e2},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		ents, _, err := db.IterateEntries([]pb.Entry{}, 0,
			shardID, replicaID, 1, 6, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 2)
		require.Equal(t, uint64(1), ents[0].Index)
		require.Equal(t, uint64(2), ents[1].Index)
		ents, _, err = db.IterateEntries([]pb.Entry{}, 0,
			shardID, replicaID, 3, 6, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, ents)
		ents, _, err = db.IterateEntries([]pb.Entry{}, 0,
			shardID, replicaID, 4, 6, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, ents, 2)
		require.Equal(t, uint64(4), ents[0].Index)
		require.Equal(t, uint64(5), ents[1].Index)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func testAllWantedEntriesAreAccessible(t *testing.T,
	first uint64, last uint64) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		ents := make([]pb.Entry, 0)
		for i := first; i <= last; i++ {
			e := pb.Entry{
				Term:  1,
				Index: i,
				Type:  pb.ApplicationEntry,
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         pb.State{Commit: 1},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		results, _, err := db.IterateEntries(nil,
			0, shardID, replicaID, first, last+1, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, last-first+1, uint64(len(results)))
		require.Equal(t, last, results[len(results)-1].Index)
		require.Equal(t, first, results[0].Index)
		rs, err := db.ReadRaftState(shardID, replicaID, first-1)
		require.NoError(t, err)
		firstIndex := rs.FirstIndex
		length := rs.EntryCount
		require.Equal(t, first, firstIndex)
		require.Equal(t, last-first+1, length)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestRemoveEntriesTo(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	d := fs.PathJoin(RDBTestDirectory, dir)
	lld := fs.PathJoin(RDBTestDirectory, lldir)
	require.NoError(t, fs.RemoveAll(d))
	require.NoError(t, fs.RemoveAll(lld))
	defer func() {
		require.NoError(t, fs.RemoveAll(RDBTestDirectory))
	}()
	shardID := uint64(0)
	replicaID := uint64(4)
	ents := make([]pb.Entry, 0)
	maxIndex := uint64(1024)
	skipSizeCheck := false
	func() {
		db := getNewTestDB(dir, lldir, false, fs)
		sdb, ok := db.(*ShardedDB)
		require.True(t, ok)
		name := sdb.Name()
		plog.Infof("name: %s", name)
		skipSizeCheck = strings.Contains(name, "leveldb")
		failed, err := sdb.SelfCheckFailed()
		require.False(t, failed)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close())
		}()
		for i := uint64(0); i < maxIndex; i++ {
			e := pb.Entry{
				Term:  1,
				Index: i,
				Type:  pb.ApplicationEntry,
				Cmd:   make([]byte, 1024*4),
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         pb.State{Commit: 1},
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		require.NoError(t, db.RemoveEntriesTo(shardID, replicaID, maxIndex))
		done, err := db.CompactEntriesTo(shardID, replicaID, maxIndex)
		require.NoError(t, err)
		for i := 0; i < 1000; i++ {
			count := atomic.LoadUint64(&(sdb.completedCompactions))
			if count == 0 {
				time.Sleep(10 * time.Millisecond)
			} else {
				plog.Infof("count: %d, done", count)
				break
			}
			if i == 999 {
				require.Fail(t, "failed to trigger compaction")
			}
		}
		select {
		case <-done:
		default:
			require.Fail(t, "done chan not closed")
		}
		results, _, err := db.IterateEntries(nil,
			0, shardID, replicaID, 1, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, results)
	}()
	// leveldb has the leftover ldb file
	// https://github.com/google/leveldb/issues/573
	// https://github.com/google/leveldb/issues/593
	if !skipSizeCheck {
		sz, err := getDirSize(RDBTestDirectory, false, fs)
		require.NoError(t, err)
		plog.Infof("sz: %d", sz)
		require.LessOrEqual(t, sz, int64(1024*1024))
	}
}

func TestAllWantedEntriesAreAccessible(t *testing.T) {
	testAllWantedEntriesAreAccessible(t, 1, 2)
	testAllWantedEntriesAreAccessible(t, 3, batchSize/2)
	testAllWantedEntriesAreAccessible(t, 1, batchSize-1)
	testAllWantedEntriesAreAccessible(t, 1, batchSize)
	testAllWantedEntriesAreAccessible(t, 1, batchSize+1)
	testAllWantedEntriesAreAccessible(t, 1, batchSize*3-1)
	testAllWantedEntriesAreAccessible(t, 1, batchSize*3)
	testAllWantedEntriesAreAccessible(t, 1, batchSize*3+1)
	testAllWantedEntriesAreAccessible(t, batchSize-1, batchSize*3-1)
	testAllWantedEntriesAreAccessible(t, batchSize, batchSize*3-1)
	testAllWantedEntriesAreAccessible(t, batchSize+1, batchSize*3-1)
	testAllWantedEntriesAreAccessible(t, batchSize-1, batchSize*3)
	testAllWantedEntriesAreAccessible(t, batchSize, batchSize*3)
	testAllWantedEntriesAreAccessible(t, batchSize+1, batchSize*3)
	testAllWantedEntriesAreAccessible(t, batchSize-1, batchSize*3+1)
	testAllWantedEntriesAreAccessible(t, batchSize, batchSize*3+1)
	testAllWantedEntriesAreAccessible(t, batchSize+1, batchSize*3+1)
}

type noopCompactor struct{}

func (noopCompactor) Compact(uint64) error { return nil }

var testCompactor = &noopCompactor{}

func TestReadRaftStateWithSnapshot(t *testing.T) {
	tests := []struct {
		snapshotIndex uint64
		entryCount    uint64
		firstIndex    uint64
		lastIndex     uint64
	}{
		{16, 85, 17, 100},
		{100, 0, 101, 100},
	}
	for _, tt := range tests {
		snapshotIndex := tt.snapshotIndex
		entryCount := tt.entryCount
		firstIndex := tt.firstIndex
		lastIndex := tt.lastIndex
		tf := func(t *testing.T, db raftio.ILogDB) {
			shardID := uint64(0)
			replicaID := uint64(4)
			ents := make([]pb.Entry, 0)
			hs := pb.State{
				Term:   1,
				Vote:   3,
				Commit: 100,
			}
			ss := pb.Snapshot{
				Index: snapshotIndex,
				Term:  1,
			}
			for i := uint64(1); i <= 100; i++ {
				e := pb.Entry{
					Term:  1,
					Index: i,
					Type:  pb.ApplicationEntry,
				}
				ents = append(ents, e)
			}
			ud := pb.Update{
				EntriesToSave: ents,
				State:         hs,
				Snapshot:      ss,
				ShardID:       shardID,
				ReplicaID:     replicaID,
			}
			err := db.SaveRaftState([]pb.Update{ud}, 1)
			require.NoError(t, err)
			state, err := db.ReadRaftState(shardID, replicaID, ss.Index)
			require.NoError(t, err)
			require.Equal(t, ss.Index, state.FirstIndex)
			require.Equal(t, entryCount, state.EntryCount)
			logReader := NewLogReader(shardID, replicaID, db)
			logReader.SetCompactor(testCompactor)
			require.NoError(t, logReader.ApplySnapshot(ss))
			logReader.SetRange(state.FirstIndex, state.EntryCount)
			fi, li := logReader.GetRange()
			require.Equal(t, firstIndex, fi)
			require.Equal(t, lastIndex, li)
		}
		fs := vfs.GetTestFS()
		runLogDBTest(t, tf, fs)
	}
}

func TestReadRaftStateWithEntriesOnly(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		ents := make([]pb.Entry, 0)
		hs := pb.State{
			Term:   1,
			Vote:   3,
			Commit: 100,
		}
		for i := uint64(1); i <= batchSize*3+1; i++ {
			e := pb.Entry{
				Term:  1,
				Index: i,
				Type:  pb.ApplicationEntry,
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         hs,
			ShardID:       shardID,
			ReplicaID:     replicaID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, 1)
		require.NoError(t, err)
		state, err := db.ReadRaftState(shardID, replicaID, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(1), state.FirstIndex)
		require.Equal(t, batchSize*3+1, state.EntryCount)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestRemoveNodeData(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(0)
		replicaID := uint64(4)
		ents := make([]pb.Entry, 0)
		hs := pb.State{
			Term:   1,
			Vote:   3,
			Commit: 100,
		}
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    1,
			Term:     2,
		}
		for i := uint64(1); i <= batchSize*3+1; i++ {
			e := pb.Entry{
				Term:  1,
				Index: i,
				Type:  pb.ApplicationEntry,
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         hs,
			ShardID:       shardID,
			ReplicaID:     replicaID,
			Snapshot:      ss,
		}
		require.NoError(t, db.SaveRaftState([]pb.Update{ud}, 1))
		state, err := db.ReadRaftState(shardID, replicaID, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(1), state.FirstIndex)
		require.Equal(t, batchSize*3+1, state.EntryCount)
		require.NoError(t, db.RemoveNodeData(shardID, replicaID))
		_, err = db.ReadRaftState(shardID, replicaID, 1)
		require.ErrorIs(t, err, raftio.ErrNoSavedLog)

		snapshot, err := db.GetSnapshot(shardID, replicaID)
		require.NoError(t, err)
		require.True(t, pb.IsEmptySnapshot(snapshot))

		_, err = db.GetBootstrapInfo(shardID, replicaID)
		require.ErrorIs(t, err, raftio.ErrNoBootstrapInfo)

		iteratedEnts, sz, err := db.IterateEntries(nil, 0, shardID, replicaID, 0,
			math.MaxUint64, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, iteratedEnts)
		require.Zero(t, sz)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestImportSnapshot(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		shardID := uint64(2)
		replicaID := uint64(4)
		ents := make([]pb.Entry, 0)
		hs := pb.State{
			Term:   1,
			Vote:   3,
			Commit: 100,
		}
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    120,
			Term:     2,
		}
		for i := uint64(0); i <= batchSize*3+1; i++ {
			e := pb.Entry{
				Term:  1,
				Index: i,
				Type:  pb.ApplicationEntry,
			}
			ents = append(ents, e)
		}
		ud := pb.Update{
			EntriesToSave: ents,
			State:         hs,
			ShardID:       shardID,
			ReplicaID:     replicaID,
			Snapshot:      ss,
		}
		require.NoError(t, db.SaveRaftState([]pb.Update{ud}, 1))
		ssimport := pb.Snapshot{
			Type:    pb.OnDiskStateMachine,
			ShardID: shardID,
			Index:   110,
			Term:    2,
		}
		require.NoError(t, db.ImportSnapshot(ssimport, replicaID))
		snapshot, err := db.GetSnapshot(shardID, replicaID)
		require.NoError(t, err)
		require.Equal(t, ssimport.Index, snapshot.Index)

		bs, err := db.GetBootstrapInfo(shardID, replicaID)
		require.NoError(t, err)
		require.Equal(t, pb.OnDiskStateMachine, bs.Type)

		state, err := db.ReadRaftState(shardID, replicaID, 1)
		require.NoError(t, err)
		require.NotEqual(t, raftio.RaftState{}, state)
		require.Equal(t, snapshot.Index, state.State.Commit)

		sdb := db.(*ShardedDB).shards[2]
		sdb.cs.maxIndex = make(map[raftio.NodeInfo]uint64)
		maxIndex, err := sdb.getMaxIndex(shardID, replicaID)
		require.NoError(t, err)
		require.Equal(t, ssimport.Index, maxIndex)

		state, err = db.ReadRaftState(shardID, replicaID, snapshot.Index)
		require.NoError(t, err)
		require.NotEqual(t, raftio.RaftState{}, state)
		require.Equal(t, snapshot.Index, state.FirstIndex)
		require.NotEqual(t, 0, state.EntryCount)
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}
