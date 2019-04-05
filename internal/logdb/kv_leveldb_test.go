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

// +build dragonboat_leveldb

package logdb

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

func modifyLogDBContent(fp string) bool {
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
			plog.Infof("got it in %s", fp)
			located = true
			break
		}
		idx += 4
	}
	if !located {
		return false
	}
	_, err = f.Seek(idx, 0)
	if err != nil {
		panic(err)
	}
	_, err = f.Write([]byte("YYYY"))
	if err != nil {
		panic(err)
	}
	return true
}

func sstFileToCorruptFilePath() []string {
	dp := filepath.Join(RDBTestDirectory, "db-dir", "logdb-3")
	fi, err := ioutil.ReadDir(dp)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	for _, v := range fi {
		if strings.HasSuffix(v.Name(), ".ldb") {
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
	deleteTestDB()
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
	for i := 0; i < 128; i++ {
		err := db.SaveRaftState([]pb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
	}
	rdb := db.(*ShardedRDB).shards[3]
	key1 := newKey(maxKeySize, nil)
	key2 := newKey(maxKeySize, nil)
	key1.SetEntryBatchKey(0, 4, 0)
	key2.SetEntryBatchKey(100, 4, math.MaxUint64)
	rdb.kvs.(*leveldbKV).Compaction(key1.Key(), key2.Key())
	db.Close()
	done := false
	for _, fp := range sstFileToCorruptFilePath() {
		plog.Infof("processing %s", fp)
		if modifyLogDBContent(fp) {
			done = true
		}
	}
	if !done {
		t.Fatalf("failed to locate the data")
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

func TestCompactRangeInLogDBWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		t.Errorf("failed to get *MultiDiskRDB")
	}
	key1 := newKey(maxKeySize, nil)
	key2 := newKey(maxKeySize, nil)
	key1.SetEntryBatchKey(0, 4, 0)
	key2.SetEntryBatchKey(0, 4, math.MaxUint64)
	rrdb.shards[0].kvs.(*leveldbKV).Compaction(key1.Key(), key2.Key())
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
		t.Errorf("sz (%d) > initialSz/10 (%d)", sz, initialSz/10)
	}
}

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
		iter := rdb.kvs.(*leveldbKV).db.NewIterator(rdb.kvs.(*leveldbKV).ro)
		defer iter.Close()
		iter.Seek(fk.key)
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
}
