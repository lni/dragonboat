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
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

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
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
		RDBTestDirectory, fs)
	require.NoError(t, err, "failed to open kv store")
	defer deleteTestDB(fs)
	require.NoError(t, kvs.Close(), "failed to close kv store")
}

func runKVTest(t *testing.T, tf func(t *testing.T, kvs kv.IKVStore),
	fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	defer deleteTestDB(fs)
	cfg := config.GetDefaultLogDBConfig()
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
		RDBTestDirectory, fs)
	require.NoError(t, err, "failed to open kv store")
	defer func() {
		require.NoError(t, kvs.Close(), "failed to close kvs")
	}()
	tf(t, kvs)
}

func TestKVGetAndSet(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		err := kvs.SaveValue([]byte("test-key"), []byte("test-value"))
		require.NoError(t, err, "failed to save the value")
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		err = kvs.GetValue([]byte("test-key"), op)
		require.NoError(t, err, "get value failed")
		require.True(t, opcalled, "op func not called")
		require.True(t, found, "failed to get value")
	}
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestKVValueCanBeDeleted(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		err := kvs.SaveValue([]byte("test-key"), []byte("test-value"))
		require.NoError(t, err, "failed to save the value")
		err = kvs.DeleteValue([]byte("test-key"))
		require.NoError(t, err, "failed to delete")
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		err = kvs.GetValue([]byte("test-key"), op)
		require.NoError(t, err, "get value failed")
		require.True(t, opcalled, "op func not called")
		require.False(t, found, "failed to delete result")
	}
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestKVWriteBatch(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		wb := kvs.GetWriteBatch()
		defer wb.Destroy()
		wb.Put([]byte("test-key"), []byte("test-value"))
		require.Equal(t, 1, wb.Count(), "incorrect count")
		err := kvs.CommitWriteBatch(wb)
		require.NoError(t, err, "failed to commit the write batch")
		found := false
		opcalled := false
		op := func(val []byte) error {
			opcalled = true
			if string(val) == "test-value" {
				found = true
			}
			return nil
		}
		err = kvs.GetValue([]byte("test-key"), op)
		require.NoError(t, err, "get value failed")
		require.True(t, opcalled, "op func not called")
		require.True(t, found, "failed to get the result")
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
			err := kvs.SaveValue([]byte(key), []byte(val))
			require.NoError(t, err, "failed to save the value")
		}
		opcalled := uint64(0)
		op := func(k []byte, v []byte) (bool, error) {
			opcalled++
			return true, nil
		}
		err := kvs.IterateValue(fk, lk, inc, op)
		require.NoError(t, err, "iterate value failed")
		require.Equal(t, count, opcalled, "op called wrong number of times")
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
		require.Equal(t, 2, wb.Count(), "unexpected count")
		wb.Clear()
		err := kvs.CommitWriteBatch(wb)
		require.NoError(t, err, "failed to commit write batch")
		err = kvs.GetValue([]byte("key-1"),
			func(data []byte) error {
				require.Empty(t, data, "unexpected value")
				return nil
			})
		require.NoError(t, err, "get value failed")
	}
	fs := vfs.GetTestFS()
	runKVTest(t, tf, fs)
}

func TestHasEntryRecord(t *testing.T) {
	tf := func(t *testing.T, kvs kv.IKVStore) {
		has, err := hasEntryRecord(kvs, true)
		require.NoError(t, err, "hasEntryRecord failed")
		require.False(t, has, "unexpected result")
		has, err = hasEntryRecord(kvs, false)
		require.NoError(t, err, "hasEntryRecord failed")
		require.False(t, has, "unexpected result")
		eb := pb.EntryBatch{}
		data, err := eb.Marshal()
		require.NoError(t, err)
		k := newKey(entryKeySize, nil)
		k.SetEntryBatchKey(1, 1, 1)
		err = kvs.SaveValue(k.Key(), data)
		require.NoError(t, err, "failed to save entry batch")
		has, err = hasEntryRecord(kvs, true)
		require.NoError(t, err, "hasEntryRecord failed")
		require.True(t, has, "unexpected result")
		has, err = hasEntryRecord(kvs, false)
		require.NoError(t, err, "hasEntryRecord failed")
		require.False(t, has, "unexpected result")
		ent := pb.Entry{}
		data, err = ent.Marshal()
		require.NoError(t, err)
		k = newKey(entryKeySize, nil)
		k.SetEntryKey(1, 1, 1)
		err = kvs.SaveValue(k.Key(), data)
		require.NoError(t, err, "failed to save entry batch")
		has, err = hasEntryRecord(kvs, true)
		require.NoError(t, err, "hasEntryRecord failed")
		require.True(t, has, "unexpected result")
		has, err = hasEntryRecord(kvs, false)
		require.NoError(t, err, "hasEntryRecord failed")
		require.True(t, has, "unexpected result")
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
		err := kvs.CommitWriteBatch(wb)
		require.NoError(t, err, "failed to commit wb")
		fk := newKey(entryKeySize, nil)
		lk := newKey(entryKeySize, nil)
		fk.SetEntryKey(100, 1, 1)
		lk.SetEntryKey(100, 1, 100)
		count := 0
		op := func(key []byte, data []byte) (bool, error) {
			count++
			return true, nil
		}
		err = kvs.IterateValue(fk.Key(), lk.Key(), true, op)
		require.NoError(t, err, "iterate value failed")
		require.Equal(t, 100, count, "failed to get all key value pairs")
		lk.SetEntryKey(100, 1, 21)
		err = kvs.BulkRemoveEntries(fk.Key(), lk.Key())
		require.NoError(t, err, "remove entry failed")
		err = kvs.CompactEntries(fk.Key(), lk.Key())
		require.NoError(t, err, "compaction failed")
		count = 0
		lk.SetEntryKey(100, 1, 100)
		err = kvs.IterateValue(fk.Key(), lk.Key(), true, op)
		require.NoError(t, err, "iterate value failed")
		require.Equal(t, 80, count, "failed to get all key value pairs")
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
		kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
			RDBTestDirectory, fs)
		require.NoError(t, err, "failed to open kv store")
		defer func() {
			require.NoError(t, kvs.Close())
		}()
		wb := kvs.GetWriteBatch()
		defer wb.Destroy()
		for i := uint64(1); i <= maxIndex; i++ {
			key := newKey(entryKeySize, nil)
			key.SetEntryKey(100, 1, i)
			data := make([]byte, 64)
			_, err = rand.Read(data)
			require.NoError(t, err)
			wb.Put(key.Key(), data)
		}
		err = kvs.CommitWriteBatch(wb)
		require.NoError(t, err, "failed to commit wb")
	}()
	sz, err := getDirSize(RDBTestDirectory, true, fs)
	require.NoError(t, err, "failed to get sz")
	require.GreaterOrEqual(t, sz, int64(1024*1024*8), "unexpected size")
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
		RDBTestDirectory, fs)
	require.NoError(t, err, "failed to open kv store")
	defer func() {
		require.NoError(t, kvs.Close())
	}()
	err = kvs.BulkRemoveEntries(fk.Key(), lk.Key())
	require.NoError(t, err, "remove entry failed")
	err = kvs.CompactEntries(fk.Key(), lk.Key())
	require.NoError(t, err, "compaction failed")
	sz, err = getDirSize(RDBTestDirectory, false, fs)
	require.NoError(t, err, "failed to get sz")
	require.LessOrEqual(t, sz, int64(1024*1024), "unexpected size")
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
			if strings.HasSuffix(v.Name(), ".ldb") ||
				strings.HasSuffix(v.Name(), ".sst") {
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
	if err := f.Close(); err != nil {
		return false, err
	}
	data := buf.Bytes()
	f, err = fs.Create(fp)
	if err != nil {
		return false, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
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
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
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
		kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
			RDBTestDirectory, fs)
		require.NoError(t, err, "failed to open kv store")
		defer func() {
			require.NoError(t, kvs.Close())
		}()
		if cut && !wal {
			require.Fail(t, "cut && !wal")
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
		err = kvs.CommitWriteBatch(wb)
		require.NoError(t, err, "failed to commit wb")
		if !wal {
			err := kvs.FullCompaction()
			require.NoError(t, err, "full compaction failed")
		}
	}()
	files, err := getDataFilePathList(RDBTestDirectory, wal, fs)
	require.NoError(t, err, "failed to get data files")
	corrupted := false
	for _, fp := range files {
		var done bool
		var err error
		if cut {
			done, err = cutDataFile(fp, fs)
		} else {
			done, err = modifyDataFile(fp, fs)
		}
		require.NoError(t, err, "failed to modify data file")
		if done {
			corrupted = true
			break
		}
	}
	require.True(t, corrupted, "failed to corrupt data files")
	kvs, err := newDefaultKVStore(cfg, nil, RDBTestDirectory,
		RDBTestDirectory, fs)
	if err == nil {
		defer func() {
			require.NoError(t, kvs.Close())
		}()
	}
	if !cut {
		if wal && err == nil {
			require.Fail(t, "corrupted WAL not reported")
		} else {
			return
		}
	}
	require.NoError(t, err, "failed to open kv store")
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
		require.Error(t, err, "no checksum error returned")
	} else {
		require.NoError(t, err, "failed to iterate the db")
		require.Equal(t, 0, count, "unexpected count")
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
