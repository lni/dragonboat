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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/internal/logdb/gorocksdb"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func getDBDirSize(dataDir string, idx uint64) (int64, error) {
	p := filepath.Join(RDBTestDirectory, dataDir, fmt.Sprintf("logdb-%d", idx))
	return getDirSize(p)
}

func TestCompactRangeInLogDBWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	useRangeDelete = true
	compactFunc := func(db raftio.ILogDB, clusterID uint64, nodeID uint64, maxIndex uint64) {
		rrdb, ok := db.(*ShardedRDB)
		if !ok {
			t.Errorf("failed to get *MultiDiskRDB")
		}
		err := rrdb.RemoveEntriesTo(clusterID, nodeID, maxIndex-10)
		if err != nil {
			t.Errorf("delete range failed %v", err)
		}
		for i := 0; i < 1000; i++ {
			time.Sleep(100 * time.Millisecond)
			if atomic.LoadUint64(&rrdb.completedCompactions) > 0 {
				break
			}
		}
	}
	testCompactRangeWithCompactionFilterWorks(t, compactFunc)
}

func TestCompactionTaskCanBeCreated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	if p.len() != 0 {
		t.Errorf("size is not 0")
	}
}

func TestCompactionTaskCanBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{clusterID: 1, nodeID: 2, index: 3})
	if p.len() != 1 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	if len(p.pendings) != 1 {
		t.Errorf("p.pending len is not 1")
	}
	v, ok := p.pendings[raftio.NodeInfo{ClusterID: 1, NodeID: 2}]
	if !ok {
		t.Errorf("not added")
	}
	if v != 3 {
		t.Errorf("unexpected index %d", v)
	}
}

func TestCompactionTaskCanBeUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{clusterID: 1, nodeID: 2, index: 3})
	p.addTask(task{clusterID: 1, nodeID: 2, index: 10})
	if len(p.pendings) != 1 {
		t.Errorf("p.pending len is not 1")
	}
	v, ok := p.pendings[raftio.NodeInfo{ClusterID: 1, NodeID: 2}]
	if !ok {
		t.Errorf("not added")
	}
	if v != 10 {
		t.Errorf("unexpected index %d", v)
	}
}

func TestCompactionTaskGetReturnTheExpectedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	tt := task{clusterID: 1, nodeID: 2, index: 3}
	p.addTask(tt)
	if p.len() != 1 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	task, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	if !reflect.DeepEqual(&tt, &task) {
		t.Errorf("%v vs %v", tt, task)
	}
}

func TestCompactionTaskGetReturnAllExpectedValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{clusterID: 1, nodeID: 2, index: 3})
	p.addTask(task{clusterID: 2, nodeID: 2, index: 10})
	if p.len() != 2 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	task1, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	task2, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	if task1.index != 3 && task1.index != 10 {
		t.Errorf("unexpected task obj")
	}
	if task2.index != 3 && task2.index != 10 {
		t.Errorf("unexpected task obj")
	}
	_, ok = p.getTask()
	if ok {
		t.Errorf("unexpected ok flag value")
	}
}

func TestMovingCompactionIndexBackWillCausePanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("not panic")
		}
	}()
	p := newCompactions()
	p.addTask(task{clusterID: 1, nodeID: 2, index: 3})
	p.addTask(task{clusterID: 1, nodeID: 2, index: 2})
}

func testCompactRangeWithCompactionFilterWorks(t *testing.T,
	f func(raftio.ILogDB, uint64, uint64, uint64)) {
	dir := "compaction-db-dir"
	lldir := "compaction-wal-db-dir"
	deleteTestDB()
	db := getNewTestDB(dir, lldir)
	defer deleteTestDB()
	defer db.Close()
	hs := pb.State{
		Term:   2,
		Vote:   3,
		Commit: 100,
	}
	ud := pb.Update{
		EntriesToSave: []pb.Entry{},
		State:         hs,
		ClusterID:     0,
		NodeID:        4,
	}
	maxIndex := uint64(0)
	for i := uint64(0); i < 128; i++ {
		for j := uint64(0); j < 16; j++ {
			e := pb.Entry{
				Term:  2,
				Index: i*16 + j,
				Type:  pb.ApplicationEntry,
				Cmd:   make([]byte, 1024*32),
			}
			maxIndex = e.Index
			ud.EntriesToSave = append(ud.EntriesToSave, e)
		}
	}
	err := db.SaveRaftState([]pb.Update{ud}, newRDBContext(1, nil))
	if err != nil {
		t.Fatalf("failed to save the ud rec")
	}
	rrdb, ok := db.(*ShardedRDB)
	if !ok {
		t.Fatalf("failed to get *MultiDiskRDB")
	}
	var cr gorocksdb.Range
	key1 := newKey(maxKeySize, nil)
	key2 := newKey(maxKeySize, nil)
	key1.SetEntryBatchKey(0, 4, 0)
	key2.SetEntryBatchKey(0, 4, math.MaxUint64)
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetForceBottommostLevelCompaction()
	defer opts.Destroy()
	cr.Start = key1.Key()
	cr.Limit = key2.Key()
	rrdb.shards[0].kvs.(*rocksdbKV).db.CompactRangeWithOptions(opts, cr)
	initialSz, err := getDBDirSize(dir, 0)
	if err != nil {
		t.Errorf("failed to get db size %v", err)
	}
	if initialSz < 8*1024*1024 {
		t.Errorf("sz %d < 8MBytes", initialSz)
	}
	f(db, ud.ClusterID, ud.NodeID, maxIndex)
	sz, err := getDBDirSize(dir, 0)
	if err != nil {
		t.Fatalf("failed to get db size %v", err)
	}
	if sz > initialSz/10 {
		t.Errorf("sz %d > initialSz/10", sz)
	}
}

func TestRawCompactRangeWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	useRangeDelete = true
	compactFunc := func(db raftio.ILogDB,
		clusterID uint64, nodeID uint64, maxIndex uint64) {
		rrdb, ok := db.(*ShardedRDB)
		if !ok {
			t.Errorf("failed to get *MultiDiskRDB")
		}
		batchID := getBatchID(maxIndex)
		if batchID == 0 || batchID == 1 {
			return
		}
		firstKey := newKey(maxKeySize, nil)
		lastKey := newKey(maxKeySize, nil)
		firstKey.SetEntryBatchKey(clusterID, nodeID, 0)
		lastKey.SetEntryBatchKey(clusterID, nodeID, batchID-1)
		err := rrdb.shards[0].kvs.(*rocksdbKV).deleteRange(firstKey.Key(), lastKey.Key())
		if err != nil {
			t.Errorf("delete range failed %v", err)
		}
		var cr gorocksdb.Range
		key1 := newKey(maxKeySize, nil)
		key2 := newKey(maxKeySize, nil)
		key1.SetEntryBatchKey(clusterID, nodeID, 0)
		key2.SetEntryBatchKey(clusterID, nodeID, batchID)
		opts := gorocksdb.NewCompactionOptions()
		opts.SetExclusiveManualCompaction(false)
		opts.SetForceBottommostLevelCompaction()
		defer opts.Destroy()
		st := time.Now()
		cr.Start = key1.Key()
		cr.Limit = key2.Key()
		rrdb.shards[0].kvs.(*rocksdbKV).db.CompactRangeWithOptions(opts, cr)
		cost := time.Now().Sub(st).Nanoseconds()
		plog.Infof("cost %d nanoseconds to complete the compact range op", cost)
	}
	testCompactRangeWithCompactionFilterWorks(t, compactFunc)
}
