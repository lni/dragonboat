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
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestKVCanBeCreatedAndClosed(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	cfg := config.GetDefaultLogDBConfig()
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
	if err != nil {
		t.Fatalf("failed to open kv store %v", err)
	}
	defer deleteTestDB(fs)
	if err := kvs.Close(); err != nil {
		t.Errorf("failed to close kv store %v", err)
	}
}

func runKVTest(t *testing.T, tf func(t *testing.T, kvs kv.IKVStore), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	defer deleteTestDB(fs)
	cfg := config.GetDefaultLogDBConfig()
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
	if err != nil {
		t.Fatalf("failed to open kv store %v", err)
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestKVWriteBatch(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch()
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestKVIterateValue(t *testing.T) {
	testKVIterateValue(t, []byte("key0"), []byte("key5"), true, 6)
	testKVIterateValue(t, []byte("key0"), []byte("key5"), false, 5)
}

func TestWriteBatchCanBeCleared(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch()
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestEntriesCanBeRemovedFromKVStore(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch()
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
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestCompactionReleaseStorageSpace(t *testing.T) {
	fs := vfs.GetTestFS()
	deleteTestDB(fs)
	defer deleteTestDB(fs)
	maxIndex := uint64(1024 * 128)
	fk := newKey(entryKeySize, nil)
	lk := newKey(entryKeySize, nil)
	fk.SetEntryKey(100, 1, 1)
	lk.SetEntryKey(100, 1, maxIndex+1)
	cfg := config.GetDefaultLogDBConfig()
	func() {
		kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
		if err != nil {
			t.Fatalf("failed to open kv store %v", err)
		}
		defer kvs.Close()
		wb := kvs.GetWriteBatch()
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
	sz, err := getDirSize(RDBTestDirectory, true, fs)
	if err != nil {
		t.Fatalf("failed to get sz %v", err)
	}
	if sz < 1024*1024*8 {
		t.Errorf("unexpected size %d", sz)
	}
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
	if err != nil {
		t.Fatalf("failed to open kv store %v", err)
	}
	defer kvs.Close()
	if err := kvs.BulkRemoveEntries(fk.Key(), lk.Key()); err != nil {
		t.Fatalf("remove entry failed %v", err)
	}
	if err := kvs.CompactEntries(fk.Key(), lk.Key()); err != nil {
		t.Fatalf("compaction failed %v", err)
	}
	sz, err = getDirSize(RDBTestDirectory, false, fs)
	if err != nil {
		t.Fatalf("failed to get sz %v", err)
	}
	if sz > 1024*1024 {
		t.Errorf("unexpected size %d", sz)
	}
}

var flagContent = "YYYY"
var corruptedContent = "XXXX"

func getDataFilePathList(dir string, wal bool, fs vfs.IFS) ([]string, error) {
	elms, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for _, path := range elms {
		v, err := fs.Stat(fs.PathJoin(dir, path))
		if err != nil {
			return nil, err
		}
		if !wal {
			if strings.HasSuffix(v.Name(), ".ldb") || strings.HasSuffix(v.Name(), ".sst") {
				result = append(result, fs.PathJoin(dir, v.Name()))
			}
		} else {
			if strings.HasSuffix(v.Name(), ".log") {
				result = append(result, fs.PathJoin(dir, v.Name()))
			}
		}
	}
	return result, nil
}

func cutDataFile(fp string, fs vfs.IFS) (bool, error) {
	buf := bytes.NewBuffer(nil)
	f, err := fs.Open(fp)
	if err != nil {
		return false, err
	}
	_, err = io.Copy(buf, f)
	if err != nil {
		return false, err
	}
	f.Close()
	data := buf.Bytes()
	f, err = fs.Create(fp)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Write(data[:len(data)-1])
	if err != nil {
		return false, err
	}
	return true, nil
}

func modifyDataFile(fp string, fs vfs.IFS) (bool, error) {
	tmpFp := fs.PathJoin(fs.PathDir(fp), "tmp")
	if err := func() error {
		idx := int64(0)
		f, err := fs.ReuseForWrite(fp, tmpFp)
		if err != nil {
			return err
		}
		defer f.Close()
		located := false
		data := make([]byte, 4)
		for {
			if _, err := f.Read(data); err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}
			if string(data) == flagContent {
				located = true
				break
			}
			idx += 4
		}
		if !located {
			return errors.New("failed to locaate the data")
		}
		if _, err = f.WriteAt([]byte(corruptedContent), idx); err != nil {
			plog.Infof("failed to write")
			return err
		}
		return nil
	}(); err != nil {
		return false, err
	}
	if err := fs.Rename(tmpFp, fp); err != nil {
		return false, err
	}
	return true, nil
}

func testDiskCorruptionIsHandled(t *testing.T, wal bool, cut bool, fs vfs.IFS) {
	deleteTestDB(fs)
	defer deleteTestDB(fs)
	cfg := config.GetDefaultLogDBConfig()
	func() {
		kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
		if err != nil {
			t.Fatalf("failed to open kv store %v", err)
		}
		defer kvs.Close()
		if cut && !wal {
			t.Fatalf("cut && !wal")
		}
		if wal && kvs.Name() != "rocksdb" {
			t.Skip("test skipped, WAL hardware corruption is not handled")
		}
		if wal && kvs.Name() == "rocksdb" &&
			!settings.Soft.KVTolerateCorruptedTailRecords {
			t.Skip("test skipped, RocksDBTolerateCorruptedTailRecords disabled")
		}
		wb := kvs.GetWriteBatch()
		defer wb.Destroy()
		data := make([]byte, 0)
		for i := 0; i < 16; i++ {
			data = append(data, []byte(flagContent)...)
		}
		for i := uint64(1); i <= 1; i++ {
			key := newKey(entryKeySize, nil)
			key.SetEntryKey(100, 1, i)
			wb.Put(key.Key(), data)
		}
		if err := kvs.CommitWriteBatch(wb); err != nil {
			t.Fatalf("failed to commit wb %v", err)
		}
		if !wal {
			if err := kvs.FullCompaction(); err != nil {
				t.Fatalf("full compaction failed %v", err)
			}
		}
	}()
	files, err := getDataFilePathList(RDBTestDirectory, wal, fs)
	if err != nil {
		t.Fatalf("failed to get data files %v", err)
	}
	corrupted := false
	for _, fp := range files {
		var done bool
		var err error
		if cut {
			done, err = cutDataFile(fp, fs)
		} else {
			done, err = modifyDataFile(fp, fs)
		}
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
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory, RDBTestDirectory, fs)
	if err == nil {
		defer kvs.Close()
	}
	if !cut {
		if wal && err == nil {
			t.Fatalf("corrupted WAL not reported")
		} else {
			return
		}
	}
	if err != nil {
		t.Fatalf("failed to open kv store %v", err)
	}
	fk := newKey(entryKeySize, nil)
	lk := newKey(entryKeySize, nil)
	fk.SetEntryKey(100, 1, 1)
	lk.SetEntryKey(100, 1, 1024)
	count := 0
	op := func(key []byte, data []byte) (bool, error) {
		count++
		return true, nil
	}
	err = kvs.IterateValue(fk.Key(), lk.Key(), true, op)
	if !cut {
		if err == nil {
			t.Fatalf("no checksum error returned")
		}
	} else {
		if err != nil {
			t.Fatalf("failed to iterate the db: %v", err)
		}
		if count != 0 {
			t.Fatalf("unexpected count: %d", count)
		}
	}
}

func TestTailCorruptionIsIgnored(t *testing.T) {
	fs := vfs.GetTestFS()
	testDiskCorruptionIsHandled(t, true, true, fs)
}

func TestSSTCorruptionIsHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	testDiskCorruptionIsHandled(t, false, false, fs)
}

// see testDiskCorruptionIsHandled's comments for more details
func TestWALCorruptionIsHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	testDiskCorruptionIsHandled(t, true, false, fs)
}
