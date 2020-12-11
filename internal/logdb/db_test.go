// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	RDBTestDirectory = "db_test_dir_safe_to_delete"
)

func getDirSize(path string, includeLogSize bool, fs vfs.IFS) (int64, error) {
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

func getNewTestDB(dir string, lldir string, batched bool, fs vfs.IFS) raftio.ILogDB {
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
	cfg := config.NodeHostConfig{
		Expert: expert,
	}

	db, err := NewLogDB(cfg, nil,
		[]string{d}, []string{lld}, batched, false, fs, newDefaultKVStore)
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
	if err := fs.RemoveAll(d); err != nil {
		t.Fatalf("%v", err)
	}
	if err := fs.RemoveAll(lld); err != nil {
		t.Fatalf("%v", err)
	}
	db := getNewTestDB(dir, lldir, batched, fs)
	defer deleteTestDB(fs)
	defer db.Close()
	tf(t, db)
}

func runLogDBTest(t *testing.T, tf func(t *testing.T, db raftio.ILogDB), fs vfs.IFS) {
	runLogDBTestAs(t, false, tf, fs)
	runLogDBTestAs(t, true, tf, fs)
}

func runBatchedLogDBTest(t *testing.T, tf func(t *testing.T, db raftio.ILogDB), fs vfs.IFS) {
	runLogDBTestAs(t, true, tf, fs)
}

func TestRDBReturnErrNoBootstrapInfoWhenNoBootstrap(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, db raftio.ILogDB) {
		bootstrap, err := db.GetBootstrapInfo(1, 2)
		if err != raftio.ErrNoBootstrapInfo {
			t.Errorf("unexpected error %v", err)
		}
		if bootstrap != nil {
			t.Errorf("not nil value")
		}
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
		if err := db.SaveBootstrapInfo(1, 2, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		bootstrap, err := db.GetBootstrapInfo(1, 2)
		if err != nil {
			t.Errorf("failed to get bootstrap info %v", err)
		}
		if bootstrap.Join {
			t.Errorf("unexpected join value")
		}
		if len(bootstrap.Addresses) != 3 {
			t.Errorf("unexpected addresses len")
		}
		ni, err := db.ListNodeInfo()
		if err != nil {
			t.Errorf("failed to list node info %v", err)
		}
		if len(ni) != 1 {
			t.Errorf("failed to get node info list")
		}
		if ni[0].ClusterID != 1 || ni[0].NodeID != 2 {
			t.Errorf("unexpected cluster id/node id, %v", ni[0])
		}
		if err := db.SaveBootstrapInfo(2, 3, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		if err := db.SaveBootstrapInfo(3, 4, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		ni, err = db.ListNodeInfo()
		if err != nil {
			t.Errorf("failed to list node info %v", err)
		}
		if len(ni) != 3 {
			t.Errorf("failed to get node info list")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSnapshotHasMaxIndexSet(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ud1 := pb.Update{
			EntriesToSave: []pb.Entry{{Index: 2}, {Index: 3}, {Index: 4}},
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud1}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save raft state %v", err)
		}
		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 4 {
			t.Errorf("max index %d, want 4", maxIndex)
		}
		ud2 := pb.Update{
			ClusterID: 3,
			NodeID:    4,
			Snapshot:  pb.Snapshot{Index: 3},
		}
		err = db.SaveRaftState([]pb.Update{ud2}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save raft state %v", err)
		}
		maxIndex, err = p[3].getMaxIndex(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 3 {
			t.Errorf("max index %d, want 3", maxIndex)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSaveSnapshotTogetherWithUnexpectedEntriesWillPanic(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ud1 := pb.Update{
			EntriesToSave: []pb.Entry{{Index: 2}, {Index: 3}, {Index: 4}},
			ClusterID:     3,
			NodeID:        4,
			Snapshot:      pb.Snapshot{Index: 5},
		}
		err := db.SaveRaftState([]pb.Update{ud1}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save raft state %v", err)
		}
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic not triggered")
		}
	}()
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
			ClusterID:     3,
			NodeID:        4,
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
			ClusterID:     3,
			NodeID:        3,
			Snapshot:      snapshot2,
		}
		uds := []pb.Update{ud1, ud2}
		err := db.SaveRaftState(uds, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		v, _ := db.ListSnapshots(3, 4, math.MaxUint64)
		if len(v) != 1 {
			t.Fatalf("snapshot not saved")
		}
		if v[0].Index != snapshot1.Index {
			t.Errorf("snapshot index %d, want %d", v[0].Index, snapshot1.Index)
		}
		v, _ = db.ListSnapshots(3, 3, math.MaxUint64)
		if len(v) != 1 {
			t.Errorf("snapshot not saved")
		}
		if v[0].Index != snapshot2.Index {
			t.Errorf("snapshot index %d, want %d", v[0].Index, snapshot2.Index)
		}
		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 3)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 20 {
			t.Errorf("max index %d, want 20", maxIndex)
		}
		maxIndex, err = p[3].getMaxIndex(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 10 {
			t.Errorf("max index %d, want 10", maxIndex)
		}
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
			ClusterID: 3,
			NodeID:    4,
			Snapshot:  ss,
		}
		if err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1)); err != nil {
			t.Fatalf("failed to save single de rec")
		}
		rs, err := db.ReadRaftState(3, 4, ss.Index)
		if err != nil {
			t.Fatalf("read raft state failed %v", err)
		}
		if rs.EntryCount != 0 || rs.FirstIndex != ss.Index {
			t.Errorf("unexpected rs %+v", rs)
		}
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
			ClusterID: 3,
			NodeID:    4,
			Snapshot:  ss,
		}
		if err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1)); err != nil {
			t.Fatalf("failed to save single de rec")
		}
		_, err := db.ReadRaftState(3, 4, ss.Index)
		if err != raftio.ErrNoSavedLog {
			t.Fatalf("failed to return expected error %v", err)
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			State:         hs,
			ClusterID:     3,
			NodeID:        4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to read")
		}
		if rs.EntryCount != 1 {
			t.Errorf("entry sz %d, want 1", rs.EntryCount)
			return
		}
		if rs.FirstIndex != 3 {
			t.Errorf("entry index %d, want 3", rs.FirstIndex)
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to read")
		}
		if rs.EntryCount != 1024 {
			t.Errorf("entries size %d, want %d", rs.EntryCount, 1024)
		}
		re, _, err := db.IterateEntries([]pb.Entry{}, 0, 3, 4, 1, math.MaxUint64, math.MaxUint64)
		if err != nil {
			t.Errorf("IterateEntries failed %v", err)
		}
		if len(re) != 1024 {
			t.Errorf("didn't return all entries")
		}
		lastIndex := re[0].Index
		for _, e := range re[1:] {
			if e.Index != lastIndex+1 {
				t.Errorf("index not sequential")
			}
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
		ClusterID: 3,
		NodeID:    4,
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
	err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
	if err != nil {
		t.Errorf("failed to save single de rec")
	}
	rs, err := db.ReadRaftState(3, 4, 0)
	if err != nil {
		t.Errorf("failed to read")
	}
	if rs.State == nil {
		t.Errorf("failed to get hs")
	}
	if rs.State.Term != 2 ||
		rs.State.Vote != 3 ||
		rs.State.Commit != 100 {
		t.Errorf("bad hs returned value")
	}
	if rs.EntryCount != 10 {
		t.Errorf("didn't return all entries")
	}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("read raft state failed %v", err)
		}
		if rs.State.Term != hs.Term ||
			rs.State.Vote != hs.Vote ||
			rs.State.Commit != hs.Commit {
			t.Errorf("unexpected persistent state value %v", rs)
		}
		hs2 := pb.State{
			Term:   3,
			Vote:   3,
			Commit: 100,
		}
		ud2 := pb.Update{
			EntriesToSave: []pb.Entry{},
			State:         hs2,
			ClusterID:     3,
			NodeID:        4,
		}
		err = db.SaveRaftState([]pb.Update{ud2}, newContext(1, 1))
		if err != nil {
			t.Errorf("save raft state failed %v", err)
		}
		rs, err = db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("read raft state failed %v", err)
		}
		if rs.State.Term != hs2.Term ||
			rs.State.Vote != hs2.Vote ||
			rs.State.Commit != hs2.Commit {
			t.Errorf("unexpected persistent state value %v", rs)
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		p := db.(*ShardedDB).shards
		maxIndex, err := p[3].getMaxIndex(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 10 {
			t.Errorf("max index %d, want 10", maxIndex)
		}
		e1 = pb.Entry{
			Term:  1,
			Index: 11,
			Type:  pb.ApplicationEntry,
			Cmd:   []byte("test data"),
		}
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e1},
			State:         hs,
			ClusterID:     3,
			NodeID:        4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		maxIndex, err = p[3].getMaxIndex(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxIndex != 11 {
			t.Errorf("max index %d, want 11", maxIndex)
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
		// save the same data but with different node id
		ud.NodeID = 5
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err = db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
		// save the same data but with different cluster id
		ud.NodeID = 4
		ud.ClusterID = 4
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err = db.ReadRaftState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestIterateEntriesOnlyReturnCurrentNodeEntries(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ents, _, _ := db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 0 {
			t.Errorf("ents sz %d, want 0", len(ents))
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		// save the same data again but under a different node id
		ud.NodeID = 5
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save updated de rec")
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
		// save the same data again but under a different cluster id
		ud.NodeID = 4
		ud.ClusterID = 4
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save updated de rec")
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestIterateEntries(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		ents, _, _ := db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 0 {
			t.Errorf("ents sz %d, want 0", len(ents))
		}
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
			ClusterID:     3,
			NodeID:        4,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 11, math.MaxUint64)
		if len(ents) != 1 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
		if ents[0].Index != 10 {
			t.Errorf("unexpected index %d", ents[0].Index)
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, 0)
		if len(ents) != 1 {
			t.Errorf("ents sz %d, want 1", len(ents))
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 12, math.MaxUint64)
		if len(ents) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, uint64(e1.Size()-1))
		if len(ents) != 1 {
			t.Errorf("ents sz %d, want 1", len(ents))
		}
		// write an entry with index 11
		ud = pb.Update{
			EntriesToSave: []pb.Entry{e2},
			State:         hs,
			ClusterID:     3,
			NodeID:        4,
		}
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ents, _, _ = db.IterateEntries([]pb.Entry{}, 0, 3, 4, 10, 13, math.MaxUint64)
		if len(ents) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		for _, ent := range ents {
			if ent.Index == 12 {
				t.Errorf("index 12 found")
			}
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestDeleteNotExistSnapshotIsAllowed(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		if err := db.DeleteSnapshot(1, 2, 1); err != nil {
			t.Errorf("delete snapshot failed %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestSaveSnapshot(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		snapshotList, err := db.ListSnapshots(1, 2, math.MaxUint64)
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
		if len(snapshotList) > 0 {
			t.Errorf("snapshot list sz %d, want 0", len(snapshotList))
		}
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    1,
			Term:     2,
		}
		s2 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    2,
			Term:     2,
		}
		rec1 := pb.Update{
			ClusterID: 1,
			NodeID:    2,
			Snapshot:  s1,
		}
		rec2 := pb.Update{
			ClusterID: 1,
			NodeID:    2,
			Snapshot:  s2,
		}
		err = db.SaveSnapshots([]pb.Update{rec1, rec2})
		if err != nil {
			t.Errorf("err %v want nil", err)
		}
		snapshotList, err = db.ListSnapshots(1, 2, math.MaxUint64)
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
		if len(snapshotList) != 2 {
			t.Errorf("snapshot list sz %d, want 2", len(snapshotList))
		}
		if snapshotList[0].Index != 1 {
			t.Errorf("index %d want 1", snapshotList[0].Index)
		}
		if snapshotList[1].Index != 2 {
			t.Errorf("index %d want 2", snapshotList[1].Index)
		}
		if err := db.DeleteSnapshot(1, 2, 1); err != nil {
			t.Errorf("failed to delete snapshot %v", err)
		}
		snapshotList, err = db.ListSnapshots(1, 2, math.MaxUint64)
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
		if len(snapshotList) != 1 {
			t.Errorf("snapshot list sz %d, want 1", len(snapshotList))
		}
		if snapshotList[0].Index != 2 {
			t.Errorf("unexpected snapshot returned")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestParseNodeInfoKeyPanicOnUnexpectedKeySize(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	parseNodeInfoKey(make([]byte, 21))
}

func TestSaveEntriesWithIndexGap(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		clusterID := uint64(0)
		nodeID := uint64(4)
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save recs")
		}
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Errorf("failed to save recs")
		}
		ents, _, err := db.IterateEntries([]pb.Entry{}, 0,
			clusterID, nodeID, 1, 6, math.MaxUint64)
		if err != nil {
			t.Errorf("iterate entries failed %v", err)
		}
		if uint64(len(ents)) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		if ents[0].Index != 1 || ents[1].Index != 2 {
			t.Errorf("unexpected index")
		}
		ents, _, err = db.IterateEntries([]pb.Entry{}, 0,
			clusterID, nodeID, 3, 6, math.MaxUint64)
		if err != nil {
			t.Errorf("iterate entries failed %v", err)
		}
		if uint64(len(ents)) != 0 {
			t.Errorf("ents sz %d, want 0", len(ents))
		}
		ents, _, err = db.IterateEntries([]pb.Entry{}, 0,
			clusterID, nodeID, 4, 6, math.MaxUint64)
		if err != nil {
			t.Errorf("iterate entries failed %v", err)
		}
		if uint64(len(ents)) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		if ents[0].Index != 4 || ents[1].Index != 5 {
			t.Errorf("unexpected index")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func testAllWantedEntriesAreAccessible(t *testing.T, first uint64, last uint64) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		clusterID := uint64(0)
		nodeID := uint64(4)
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save recs")
		}
		results, _, err := db.IterateEntries(nil,
			0, clusterID, nodeID, first, last+1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to get entries %v", err)
		}
		if uint64(len(results)) != last-first+1 {
			t.Errorf("got %d entries, want %d", len(results), last-first+1)
		}
		if results[len(results)-1].Index != last {
			t.Errorf("last index %d, want %d", results[len(results)-1].Index, last)
		}
		if results[0].Index != first {
			t.Errorf("first index %d, want %d", results[0].Index, first)
		}
		rs, err := db.ReadRaftState(clusterID, nodeID, first-1)
		if err != nil {
			t.Fatalf("failed to get entry range %v", err)
		}
		firstIndex := rs.FirstIndex
		length := rs.EntryCount
		if firstIndex != first {
			t.Errorf("first index %d, want %d", firstIndex, first)
		}
		if length != last-first+1 {
			t.Errorf("length %d, want %d", length, last-first+1)
		}
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
	if err := fs.RemoveAll(d); err != nil {
		t.Fatalf("%v", err)
	}
	if err := fs.RemoveAll(lld); err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		if err := fs.RemoveAll(RDBTestDirectory); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	clusterID := uint64(0)
	nodeID := uint64(4)
	ents := make([]pb.Entry, 0)
	maxIndex := uint64(1024)
	skipSizeCheck := false
	func() {
		db := getNewTestDB(dir, lldir, false, fs)
		sdb, ok := db.(*ShardedDB)
		if !ok {
			t.Fatalf("failed to get sdb")
		}
		name := sdb.Name()
		plog.Infof("name: %s", name)
		skipSizeCheck = strings.Contains(name, "leveldb")
		failed, err := sdb.SelfCheckFailed()
		if err != nil || failed {
			t.Fatalf("self check failed")
		}
		defer db.Close()
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
		}
		err = db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save recs")
		}
		if err := db.RemoveEntriesTo(clusterID, nodeID, maxIndex); err != nil {
			t.Fatalf("failed to remove entries, %v", err)
		}
		done, err := db.CompactEntriesTo(clusterID, nodeID, maxIndex)
		if err != nil {
			t.Fatalf("failed to compact entries to, %v", err)
		}
		for i := 0; i < 1000; i++ {
			count := atomic.LoadUint64(&(sdb.completedCompactions))
			if count == 0 {
				time.Sleep(10 * time.Millisecond)
			} else {
				plog.Infof("count: %d, done", count)
				break
			}
			if i == 999 {
				t.Fatalf("failed to trigger compaction")
			}
		}
		select {
		case <-done:
		default:
			t.Fatalf("done chan not closed")
		}
		results, _, err := db.IterateEntries(nil,
			0, clusterID, nodeID, 1, 100, math.MaxUint64)
		if err != nil {
			t.Errorf("iterate entries failed %v", err)
		}
		if len(results) > 0 {
			t.Errorf("entries not deleted, %d", len(results))
		}
	}()
	// leveldb has the leftover ldb file
	// https://github.com/google/leveldb/issues/573
	// https://github.com/google/leveldb/issues/593
	if !skipSizeCheck {
		sz, err := getDirSize(RDBTestDirectory, false, fs)
		if err != nil {
			t.Fatalf("failed to get sz %v", err)
		}
		plog.Infof("sz: %d", sz)
		if sz > 1024*1024 {
			t.Errorf("unexpected size, %d", sz)
		}
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
			clusterID := uint64(0)
			nodeID := uint64(4)
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
				ClusterID:     clusterID,
				NodeID:        nodeID,
			}
			err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
			if err != nil {
				t.Fatalf("failed to save recs")
			}
			state, err := db.ReadRaftState(clusterID, nodeID, ss.Index)
			if err != nil {
				t.Fatalf("failed to read raft state %v", err)
			}
			if state.FirstIndex != ss.Index {
				t.Errorf("first index %d, want %d", state.FirstIndex, 1)
			}
			if state.EntryCount != entryCount {
				t.Errorf("length %d, want %d", state.EntryCount, entryCount)
			}
			logReader := NewLogReader(clusterID, nodeID, db)
			if err := logReader.ApplySnapshot(ss); err != nil {
				t.Fatalf("apply snapshot failed")
			}
			logReader.SetRange(state.FirstIndex, state.EntryCount)
			fi, li := logReader.GetRange()
			if fi != firstIndex || li != lastIndex {
				t.Errorf("unexpected range %d:%d", fi, li)
			}
		}
		fs := vfs.GetTestFS()
		runLogDBTest(t, tf, fs)
	}
}

func TestReadRaftStateWithEntriesOnly(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		clusterID := uint64(0)
		nodeID := uint64(4)
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save recs")
		}
		state, err := db.ReadRaftState(clusterID, nodeID, 1)
		if err != nil {
			t.Fatalf("failed to read raft state %v", err)
		}
		if state.FirstIndex != 1 {
			t.Errorf("first index %d, want %d", state.FirstIndex, 1)
		}
		if state.EntryCount != batchSize*3+1 {
			t.Errorf("length %d, want %d", state.EntryCount, batchSize*3+1)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestRemoveNodeData(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		clusterID := uint64(0)
		nodeID := uint64(4)
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
			Snapshot:      ss,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save recs")
		}
		state, err := db.ReadRaftState(clusterID, nodeID, 1)
		if err != nil {
			t.Fatalf("failed to read raft state %v", err)
		}
		if state.FirstIndex != 1 {
			t.Errorf("first index %d, want %d", state.FirstIndex, 1)
		}
		if state.EntryCount != batchSize*3+1 {
			t.Errorf("length %d, want %d", state.EntryCount, batchSize*3+1)
		}
		if err := db.RemoveNodeData(clusterID, nodeID); err != nil {
			t.Fatalf("failed to remove node data")
		}
		plog.Infof("RemoveNodeData done")
		_, err = db.ReadRaftState(clusterID, nodeID, 1)
		if err != raftio.ErrNoSavedLog {
			t.Fatalf("raft state not deleted %v", err)
		}
		snapshots, err := db.ListSnapshots(clusterID, nodeID, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to list snapshots %v", err)
		}
		if len(snapshots) != 0 {
			t.Fatalf("snapshot not deleted")
		}
		bs, err := db.GetBootstrapInfo(clusterID, nodeID)
		if err != raftio.ErrNoBootstrapInfo {
			t.Fatalf("failed to delete bootstrap %v", err)
		}
		if bs != nil {
			t.Fatalf("bs not nil")
		}
		ents, sz, err := db.IterateEntries(nil, 0, clusterID, nodeID, 0,
			math.MaxUint64, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to get entries %v", err)
		}
		if len(ents) != 0 || sz != 0 {
			t.Fatalf("entry returned")
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}

func TestImportSnapshot(t *testing.T) {
	tf := func(t *testing.T, db raftio.ILogDB) {
		clusterID := uint64(2)
		nodeID := uint64(4)
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
			ClusterID:     clusterID,
			NodeID:        nodeID,
			Snapshot:      ss,
		}
		err := db.SaveRaftState([]pb.Update{ud}, newContext(1, 1))
		if err != nil {
			t.Fatalf("failed to save recs")
		}
		ssimport := pb.Snapshot{
			Type:      pb.OnDiskStateMachine,
			ClusterId: clusterID,
			Index:     110,
			Term:      2,
		}
		if err := db.ImportSnapshot(ssimport, nodeID); err != nil {
			t.Fatalf("import snapshot failed %v", err)
		}
		snapshots, err := db.ListSnapshots(clusterID, nodeID, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to list snapshots %v", err)
		}
		if len(snapshots) != 1 {
			t.Fatalf("%d snapshot rec found", len(snapshots))
		}
		if snapshots[0].Index != ssimport.Index {
			t.Errorf("unexpected snapshot index %d", snapshots[0].Index)
		}
		bs, err := db.GetBootstrapInfo(clusterID, nodeID)
		if err != nil {
			t.Fatalf("failed to delete bootstrap %v", err)
		}
		if bs.Type != pb.OnDiskStateMachine {
			t.Errorf("unexpected type %d", bs.Type)
		}
		state, err := db.ReadRaftState(clusterID, nodeID, 1)
		if err != nil {
			t.Fatalf("raft state not deleted %v", err)
		}
		if state == nil {
			t.Fatalf("failed to get raft state")
		}
		if state.State.Commit != snapshots[0].Index {
			t.Errorf("unexpected commit value")
		}
		sdb := db.(*ShardedDB).shards[2]
		sdb.cs.maxIndex = make(map[raftio.NodeInfo]uint64)
		maxIndex, err := sdb.getMaxIndex(clusterID, nodeID)
		if err != nil {
			t.Errorf("failed to get max index")
		}
		if maxIndex != ssimport.Index {
			t.Errorf("unexpected max index value %d", maxIndex)
		}
		state, err = db.ReadRaftState(clusterID, nodeID, snapshots[0].Index)
		if err != nil {
			t.Fatalf("raft state not deleted %v", err)
		}
		if state == nil {
			t.Fatalf("failed to get raft state")
		}
		if state.FirstIndex != snapshots[0].Index {
			t.Errorf("first index: %d, ss index %d", state.FirstIndex, snapshots[0].Index)
		}
		if state.EntryCount != 0 {
			t.Errorf("unexpected entry count %d", state.EntryCount)
		}
	}
	fs := vfs.GetTestFS()
	runLogDBTest(t, tf, fs)
}
