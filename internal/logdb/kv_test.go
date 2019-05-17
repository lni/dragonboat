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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lni/dragonboat/internal/logdb/kv"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	pb "github.com/lni/dragonboat/raftpb"
)

func TestKVCanBeCreatedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to open kv rocksdb")
	}
	defer deleteTestDB()
	if err := kvs.Close(); err != nil {
		t.Errorf("failed to close kv rocksdb")
	}
}

func runKVTest(t *testing.T, tf func(t *testing.T, kvs kv.IKVStore)) {
	defer leaktest.AfterTest(t)()
	defer deleteTestDB()
	kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to open kv rocksdb")
	}
	defer func() {
		if err := kvs.Close(); err != nil {
			t.Fatalf("failed to close kvs %v", err)
		}
	}()
	tf(t, kvs)
}

func TestKVGetAndSet(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		if err := kvs.SaveValue([]byte("test-key"), []byte("test-value")); err != nil {
			t.Errorf("failed to save the value")
		}
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		if err := kvs.GetValue([]byte("test-key"), op); err != nil {
			t.Errorf("get value failed")
		}
		if !opcalled {
			t.Errorf("op func not called")
		}
		if !found {
			t.Errorf("failed to get value")
		}
	}
	runKVTest(t, tf)
}

func TestKVValueCanBeDeleted(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		if err := kvs.SaveValue([]byte("test-key"), []byte("test-value")); err != nil {
			t.Errorf("failed to save the value")
		}
		if err := kvs.DeleteValue([]byte("test-key")); err != nil {
			t.Errorf("failed to delete")
		}
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		if err := kvs.GetValue([]byte("test-key"), op); err != nil {
			t.Errorf("get value failed")
		}
		if !opcalled {
			t.Errorf("op func not called")
		}
		if found {
			t.Errorf("failed to delete result")
		}
	}
	runKVTest(t, tf)
}

func TestKVWriteBatch(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch(nil)
		defer wb.Destroy()
		wb.Put([]byte("test-key"), []byte("test-value"))
		if wb.Count() != 1 {
			t.Errorf("incorrect count")
		}
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Errorf("failed to commit the write batch")
		}
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		if err := kvs.GetValue([]byte("test-key"), op); err != nil {
			t.Errorf("get value failed")
		}
		if !opcalled {
			t.Errorf("op func not called")
		}
		if !found {
			t.Errorf("failed to get the result")
		}
	}
	runKVTest(t, tf)
}

func testKVIterateValue(t *testing.T,
	fk []byte, lk []byte, inc bool, count uint64) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("val%d", i)
			if err := kvs.SaveValue([]byte(key), []byte(val)); err != nil {
				t.Errorf("failed to save the value")
			}
		}
		opcalled := uint64(0)
		op := func(k []byte, v []byte) (bool, error) {
			opcalled++
			return true, nil
		}
		if err := kvs.IterateValue(fk, lk, inc, op); err != nil {
			t.Fatalf("iterate value failed %v", err)
		}
		if opcalled != count {
			t.Errorf("op called %d times, want %d", opcalled, count)
		}
	}
	runKVTest(t, tf)
}

func TestKVIterateValue(t *testing.T) {
	testKVIterateValue(t, []byte("key0"), []byte("key5"), true, 6)
	testKVIterateValue(t, []byte("key0"), []byte("key5"), false, 5)
}

func TestWriteBatchCanBeCleared(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch(nil)
		wb.Put([]byte("key-1"), []byte("val-1"))
		wb.Put([]byte("key-2"), []byte("val-2"))
		if wb.Count() != 2 {
			t.Errorf("unexpected count %d, want 2", wb.Count())
		}
		wb.Clear()
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Fatalf("failed to commit write batch")
		}
		if err := kvs.GetValue([]byte("key-1"),
			func(data []byte) error {
				if len(data) != 0 {
					t.Fatalf("unexpected value")
				}
				return nil
			}); err != nil {
			t.Fatalf("get value failed %v", err)
		}
	}
	runKVTest(t, tf)
}

func TestHasEntryRecord(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		has, err := hasEntryRecord(kvs, true)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if has {
			t.Errorf("unexpected result")
		}
		has, err = hasEntryRecord(kvs, false)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if has {
			t.Errorf("unexpected result")
		}
		eb := pb.EntryBatch{}
		data, err := eb.Marshal()
		if err != nil {
			t.Fatalf("%v", err)
		}
		k := newKey(entryKeySize, nil)
		k.SetEntryBatchKey(1, 1, 1)
		if err := kvs.SaveValue(k.Key(), data); err != nil {
			t.Fatalf("failed to save entry batch")
		}
		has, err = hasEntryRecord(kvs, true)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if !has {
			t.Errorf("unexpected result")
		}
		has, err = hasEntryRecord(kvs, false)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if has {
			t.Errorf("unexpected result")
		}
		ent := pb.Entry{}
		data, err = ent.Marshal()
		if err != nil {
			t.Fatalf("%v", err)
		}
		k = newKey(entryKeySize, nil)
		k.SetEntryKey(1, 1, 1)
		if err := kvs.SaveValue(k.Key(), data); err != nil {
			t.Fatalf("failed to save entry batch")
		}
		has, err = hasEntryRecord(kvs, true)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if !has {
			t.Errorf("unexpected result")
		}
		has, err = hasEntryRecord(kvs, false)
		if err != nil {
			t.Fatalf("hasEntryRecord failed %v", err)
		}
		if !has {
			t.Errorf("unexpected result")
		}
	}
	runKVTest(t, tf)
}

func TestEntriesCanBeRemovedFromKVStore(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch(nil)
		defer wb.Destroy()
		for i := uint64(1); i <= 100; i++ {
			key := newKey(entryKeySize, nil)
			key.SetEntryKey(100, 1, i)
			data := make([]byte, 16)
			wb.Put(key.Key(), data)
		}
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Fatalf("failed to commit wb %v", err)
		}
		fk := newKey(entryKeySize, nil)
		lk := newKey(entryKeySize, nil)
		fk.SetEntryKey(100, 1, 1)
		lk.SetEntryKey(100, 1, 100)
		count := 0
		op := func(key []byte, data []byte) (bool, error) {
			count++
			return true, nil
		}
		if err := kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
			t.Fatalf("iterate value failed %v", err)
		}
		if count != 100 {
			t.Fatalf("failed to get all key value pairs, count %d", count)
		}
		lk.SetEntryKey(100, 1, 21)
		if err := kvs.BulkRemoveEntries(fk.Key(), lk.Key()); err != nil {
			t.Fatalf("remove entry failed %v", err)
		}
		if err := kvs.CompactEntries(fk.Key(), lk.Key()); err != nil {
			t.Fatalf("compaction failed %v", err)
		}
		count = 0
		lk.SetEntryKey(100, 1, 100)
		if err := kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
			t.Fatalf("iterate value failed %v", err)
		}
		if count != 80 {
			t.Fatalf("failed to get all key value pairs, count %d", count)
		}
	}
	runKVTest(t, tf)
}

func TestCompactionReleaseStorageSpace(t *testing.T) {
	deleteTestDB()
	defer deleteTestDB()
	maxIndex := uint64(1024 * 128)
	func() {
		kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
		if err != nil {
			t.Fatalf("failed to open kv rocksdb")
		}
		defer kvs.Close()
		wb := kvs.GetWriteBatch(nil)
		defer wb.Destroy()
		for i := uint64(1); i <= maxIndex; i++ {
			key := newKey(entryKeySize, nil)
			key.SetEntryKey(100, 1, i)
			data := make([]byte, 64)
			rand.Read(data)
			wb.Put(key.Key(), data)
		}
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Fatalf("failed to commit wb %v", err)
		}
	}()
	sz, err := getDirSize(RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to get sz %v", err)
	}
	if sz < 1024*1024*8 {
		t.Errorf("unexpected size %d", sz)
	}
	kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to open kv rocksdb")
	}
	defer kvs.Close()
	fk := newKey(entryKeySize, nil)
	lk := newKey(entryKeySize, nil)
	fk.SetEntryKey(100, 1, 1)
	lk.SetEntryKey(100, 1, maxIndex+1)
	if err := kvs.BulkRemoveEntries(fk.Key(), lk.Key()); err != nil {
		t.Fatalf("remove entry failed %v", err)
	}
	if err := kvs.CompactEntries(fk.Key(), lk.Key()); err != nil {
		t.Fatalf("compaction failed %v", err)
	}
	sz, err = getDirSize(RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to get sz %v", err)
	}
	if sz > 1024*1024 {
		t.Errorf("unexpected size %d", sz)
	}
}

var flagContent = "YYYY"
var corruptedContent = "XXXX"

func getDataFilePathList(dir string) ([]string, error) {
	fi, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for _, v := range fi {
		if strings.HasSuffix(v.Name(), ".ldb") || strings.HasSuffix(v.Name(), ".sst") {
			result = append(result, filepath.Join(dir, v.Name()))
		}
	}
	return result, nil
}

func modifyDataFile(fp string) (bool, error) {
	idx := int64(0)
	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()
	located := false
	data := make([]byte, 4)
	for {
		_, err := f.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return false, err
			}
		}
		if string(data) == flagContent {
			located = true
			break
		}
		idx += 4
	}
	if !located {
		return false, nil
	}
	_, err = f.Seek(idx, 0)
	if err != nil {
		return false, err
	}
	_, err = f.Write([]byte(corruptedContent))
	if err != nil {
		return false, err
	}
	return true, nil
}

func TestIterateValueWithDiskCorruptionIsHandled(t *testing.T) {
	deleteTestDB()
	defer deleteTestDB()
	func() {
		kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
		if err != nil {
			t.Fatalf("failed to open kv rocksdb")
		}
		defer kvs.Close()
		wb := kvs.GetWriteBatch(nil)
		defer wb.Destroy()
		data := make([]byte, 0)
		for i := 0; i < 16; i++ {
			data = append(data, []byte(flagContent)...)
		}
		for i := uint64(1); i <= 1024; i++ {
			key := newKey(entryKeySize, nil)
			key.SetEntryKey(100, 1, i)
			wb.Put(key.Key(), data)
		}
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Fatalf("failed to commit wb %v", err)
		}
		if err := kvs.FullCompaction(); err != nil {
			t.Fatalf("full compaction failed %v", err)
		}
	}()
	files, err := getDataFilePathList(RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to get data files %v", err)
	}
	corrupted := false
	for _, fp := range files {
		done, err := modifyDataFile(fp)
		if err != nil {
			t.Fatalf("failed to modify data file %v", err)
		}
		if done {
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Fatalf("failed to corrupt data files")
	}
	kvs, err := newDefaultKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to open kv rocksdb")
	}
	defer kvs.Close()
	fk := newKey(entryKeySize, nil)
	lk := newKey(entryKeySize, nil)
	fk.SetEntryKey(100, 1, 1)
	lk.SetEntryKey(100, 1, 1024)
	count := 0
	op := func(key []byte, data []byte) (bool, error) {
		count++
		return true, nil
	}
	if err := kvs.IterateValue(fk.Key(), lk.Key(), true, op); err == nil {
		t.Fatalf("no checksum error returned")
	}
}
