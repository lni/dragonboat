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

// +build dragonboat_pebble

package logdb

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	//"time"

	//"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

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
	key1 := newKey(maxKeySize, nil)
	key2 := newKey(maxKeySize, nil)
	key1.SetEntryBatchKey(0, 4, 0)
	key2.SetEntryBatchKey(0, 4, math.MaxUint64)
	pdb := rrdb.shards[0].kvs.(*pebbleKV).db
	pdb.Compact(key1.Key(), key2.Key())
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
	if sz > initialSz/5 {
		t.Errorf("sz %d > initialSz/5", sz)
	}
}

/*
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
		pdb := rrdb.shards[0].kvs.(*pebbleKV)
		err := pdb.Compaction(firstKey.Key(), lastKey.Key())
		if err != nil {
			t.Errorf("compaction failed %v", err)
		}
		key1 := newKey(maxKeySize, nil)
		key2 := newKey(maxKeySize, nil)
		key1.SetEntryBatchKey(clusterID, nodeID, 0)
		key2.SetEntryBatchKey(clusterID, nodeID, batchID)
		st := time.Now()
		pdb.db.Compact(key1.Key(), key2.Key())
		cost := time.Now().Sub(st).Nanoseconds()
		plog.Infof("cost %d nanoseconds to complete the compact range op", cost)
	}
	testCompactRangeWithCompactionFilterWorks(t, compactFunc)
}*/

func modifyLogDBContent(fp string) {
	idx := int64(0)
	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	defer f.Close()
	if err != nil {
		panic("failed to open the file")
	}
	located := false
	data := make([]byte, 4)
	for {
		_, err := f.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic("read failed")
			}
		}
		if string(data) == "XXXX" {
			// got it
			located = true
			break
		}
		idx += 4
	}
	if !located {
		panic("failed to locate the data")
	}
	_, err = f.Seek(idx, 0)
	if err != nil {
		panic(err)
	}
	_, err = f.Write([]byte("YYYY"))
	if err != nil {
		panic(err)
	}
	plog.Infof("sst file modified")
}

func sstFileToCorruptFilePath() []string {
	dp := filepath.Join(RDBTestDirectory, "db-dir", "logdb-3")
	fi, err := ioutil.ReadDir(dp)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	for _, v := range fi {
		if strings.HasSuffix(v.Name(), ".sst") {
			result = append(result, filepath.Join(dp, v.Name()))
		}
	}
	return result
}

// this is largely to check the rocksdb wrapper doesn't slightly swallow
// detected data corruption related errors
func testDiskDataCorruptionIsHandled(t *testing.T, f func(raftio.ILogDB)) {
	dir := "db-dir"
	lldir := "wal-db-dir"
	db := getNewTestDB(dir, lldir)
	defer deleteTestDB()
	hs := pb.State{
		Term:   2,
		Vote:   3,
		Commit: 100,
	}
	e1 := pb.Entry{
		Term:  1,
		Index: 10,
		Type:  pb.ApplicationEntry,
		Cmd:   []byte("XXXXXXXXXXXXXXXXXXXXXXXX"),
	}
	ud := pb.Update{
		EntriesToSave: []pb.Entry{e1},
		State:         hs,
		ClusterID:     3,
		NodeID:        4,
	}
	err := db.SaveRaftState([]pb.Update{ud}, newRDBContext(1, nil))
	if err != nil {
		t.Errorf("failed to save single de rec")
	}
	db.Close()
	db = getNewTestDB(dir, lldir)
	db.Close()
	for _, fp := range sstFileToCorruptFilePath() {
		plog.Infof(fp)
		modifyLogDBContent(fp)
	}
	db = getNewTestDB(dir, lldir)
	defer db.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("didn't crash")
		}
	}()
	f(db)
}

/*
func TestReadRaftStateWithDiskCorruptionHandled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := func(fdb raftio.ILogDB) {
		fdb.ReadRaftState(3, 4, 0)
	}
	testDiskDataCorruptionIsHandled(t, f)
}

func TestIteratorWithDiskCorruptionHandled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := func(fdb raftio.ILogDB) {
		rdb := fdb.(*ShardedRDB).shards[3]
		fk := rdb.keys.get()
		fk.SetEntryKey(3, 4, 10)
		iter := rdb.kvs.(*pebbleKV).db.NewIter(rdb.kvs.(*pebbleKV).ro)
		iter.SeekGE(fk.key)
		for ; iteratorIsValid(iter); iter.Next() {
			val := iter.Value()
			var e pb.Entry
			if err := e.Unmarshal(val); err != nil {
				panic(err)
			}
			plog.Infof(string(e.Cmd))
		}
	}
	testDiskDataCorruptionIsHandled(t, f)
}*/
