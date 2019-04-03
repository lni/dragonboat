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

package tests

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/lni/dragonboat/internal/tests/kvpb"
	sm "github.com/lni/dragonboat/statemachine"
)

func TestRocksDBCanBeCreatedAndUsed(t *testing.T) {
	dbdir := "rocksdb_db_test_safe_to_delete"
	defer os.RemoveAll(dbdir)
	db, err := createDB(dbdir)
	if err != nil {
		t.Fatalf("failed to create db %v", err)
	}
	key := []byte("test-key")
	val := []byte("test-val")
	if err := db.db.Put(db.wo, key, val); err != nil {
		t.Fatalf("failed to put kv %v", err)
	}
	result, err := db.lookup(key)
	if err != nil {
		t.Fatalf("lookup failed %v", err)
	}
	if !bytes.Equal(result, val) {
		t.Fatalf("result changed")
	}
	db.close()
}

func TestIsNewRun(t *testing.T) {
	dbdir := "rocksdb_db_test_safe_to_delete"
	defer os.RemoveAll(dbdir)
	os.MkdirAll(dbdir, 0755)
	if !isNewRun(dbdir) {
		t.Errorf("not a new run")
	}
	f, err := os.Create(path.Join(dbdir, currentDBFilename))
	if err != nil {
		t.Fatalf("failed to create the current db file")
	}
	f.Close()
	if isNewRun(dbdir) {
		t.Errorf("still considered as a new run")
	}
}

func TestGetNodeDBDirName(t *testing.T) {
	names := make(map[string]struct{})
	for c := uint64(0); c < 128; c++ {
		for n := uint64(0); n < 128; n++ {
			name := getNodeDBDirName(c, n)
			names[name] = struct{}{}
		}
	}
	if len(names) != 128*128 {
		t.Errorf("dup found")
	}
}

func TestGetNewRandomDBDirName(t *testing.T) {
	names := make(map[string]struct{})
	for c := uint64(0); c < 128; c++ {
		for n := uint64(0); n < 128; n++ {
			name := getNodeDBDirName(c, n)
			dbdir := getNewRandomDBDirName(name)
			names[dbdir] = struct{}{}
		}
	}
	if len(names) != 128*128 {
		t.Errorf("dup found")
	}
}

func TestCorruptedDBDirFileIsReported(t *testing.T) {
	dbdir := "rocksdb_db_test_safe_to_delete"
	os.MkdirAll(dbdir, 0755)
	defer os.RemoveAll(dbdir)
	content := "content"
	if err := saveCurrentDBDirName(dbdir, content); err != nil {
		t.Fatalf("failed to save current file %v", err)
	}
	if err := replaceCurrentDBFile(dbdir); err != nil {
		t.Errorf("failed to rename the current db file %v", err)
	}
	func() {
		f, err := os.OpenFile(path.Join(dbdir, currentDBFilename), os.O_RDWR, 0755)
		if err != nil {
			t.Fatalf("failed to open file %v", err)
		}
		defer f.Close()
		v := make([]byte, 1)
		if _, err := io.ReadFull(f, v); err != nil {
			t.Fatalf("failed to read %v", err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			t.Fatalf("seek failed %v", err)
		}
		v[0] = byte(v[0] + 1)
		if _, err := f.Write(v); err != nil {
			t.Fatalf("write failed %v", err)
		}
	}()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("no panic")
		}
	}()
	getCurrentDBDirName(dbdir)
}

func TestSaveCurrentDBDirName(t *testing.T) {
	dbdir := "rocksdb_db_test_safe_to_delete"
	os.MkdirAll(dbdir, 0755)
	defer os.RemoveAll(dbdir)
	content := "content"
	if err := saveCurrentDBDirName(dbdir, content); err != nil {
		t.Fatalf("failed to save current file %v", err)
	}
	if _, err := os.Stat(path.Join(dbdir, updatingDBFilename)); os.IsNotExist(err) {
		t.Fatalf("file not exist")
	}
	if !isNewRun(dbdir) {
		t.Errorf("suppose to be a new run")
	}
	if err := replaceCurrentDBFile(dbdir); err != nil {
		t.Errorf("failed to rename the current db file %v", err)
	}
	if isNewRun(dbdir) {
		t.Errorf("still a new run")
	}
	result, err := getCurrentDBDirName(dbdir)
	if err != nil {
		t.Fatalf("failed to get current db dir name %v", err)
	}
	if result != content {
		t.Errorf("content changed")
	}
}

func TestCleanupNodeDataDir(t *testing.T) {
	dbdir := "rocksdb_db_test_safe_to_delete"
	os.MkdirAll(dbdir, 0755)
	defer os.RemoveAll(dbdir)
	toKeep := "dir_to_keep"
	if err := os.MkdirAll(path.Join(dbdir, toKeep), 0755); err != nil {
		t.Fatalf("failed to create dir %v", err)
	}
	if err := os.MkdirAll(path.Join(dbdir, "d1"), 0755); err != nil {
		t.Fatalf("failed to create dir %v", err)
	}
	if err := os.MkdirAll(path.Join(dbdir, "d2"), 0755); err != nil {
		t.Fatalf("failed to create dir %v", err)
	}
	if err := saveCurrentDBDirName(dbdir, path.Join(dbdir, toKeep)); err != nil {
		t.Fatalf("failed to save current file %v", err)
	}
	if err := replaceCurrentDBFile(dbdir); err != nil {
		t.Errorf("failed to rename the current db file %v", err)
	}
	if err := cleanupNodeDataDir(dbdir); err != nil {
		t.Errorf("cleanup failed %v", err)
	}
	tests := []struct {
		name  string
		exist bool
	}{
		{dbdir, true},
		{path.Join(dbdir, toKeep), true},
		{path.Join(dbdir, "d1"), false},
		{path.Join(dbdir, "d2"), false},
	}
	for idx, tt := range tests {
		if _, err := os.Stat(tt.name); os.IsNotExist(err) {
			if tt.exist {
				t.Errorf("unexpected cleanup result %d", idx)
			}
		}
	}
}

func removeAllDBDir() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}
	for _, fi := range files {
		if fi.IsDir() && strings.HasPrefix(fi.Name(), dbNamePrefix) {
			if err := os.RemoveAll(fi.Name()); err != nil {
				panic(err)
			}
		}
	}
}

func runDiskKVTest(t *testing.T, f func(t *testing.T, odsm sm.IOnDiskStateMachine)) {
	clusterID := uint64(128)
	nodeID := uint64(256)
	removeAllDBDir()
	defer removeAllDBDir()
	odsm := NewDiskKVTest(clusterID, nodeID)
	f(t, odsm)
}

func TestDiskKVCanBeOpened(t *testing.T) {
	tf := func(t *testing.T, odsm sm.IOnDiskStateMachine) {
		idx, err := odsm.Open()
		if err != nil {
			t.Fatalf("failed to open %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx %d", idx)
		}
		_, err = odsm.Lookup([]byte(appliedIndexKey))
		if err != nil {
			t.Fatalf("lookup failed %v", err)
		}
		odsm.Close()
	}
	runDiskKVTest(t, tf)
}

func TestDiskKVCanBeUpdated(t *testing.T) {
	tf := func(t *testing.T, odsm sm.IOnDiskStateMachine) {
		idx, err := odsm.Open()
		if err != nil {
			t.Fatalf("failed to open %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx %d", idx)
		}
		pair1 := &kvpb.PBKV{
			Key: "test-key1",
			Val: "test-val1",
		}
		pair2 := &kvpb.PBKV{
			Key: "test-key2",
			Val: "test-val2",
		}
		data1, err := pair1.Marshal()
		if err != nil {
			panic(err)
		}
		data2, err := pair2.Marshal()
		if err != nil {
			panic(err)
		}
		ents := []sm.Entry{
			{Index: 1, Cmd: data1},
			{Index: 2, Cmd: data2},
		}
		odsm.Update(ents)
		result, err := odsm.Lookup([]byte(appliedIndexKey))
		if err != nil {
			t.Fatalf("lookup failed %v", err)
		}
		idx = binary.LittleEndian.Uint64(result)
		if idx != 2 {
			t.Errorf("last applied %d, want 2", idx)
		}
		result, err = odsm.Lookup([]byte("test-key1"))
		if err != nil {
			t.Fatalf("lookup failed %v", err)
		}
		if !bytes.Equal(result, []byte("test-val1")) {
			t.Errorf("value not set")
		}
		odsm.Close()
	}
	runDiskKVTest(t, tf)
}

func TestDiskKVSnapshot(t *testing.T) {
	tf := func(t *testing.T, odsm sm.IOnDiskStateMachine) {
		idx, err := odsm.Open()
		if err != nil {
			t.Fatalf("failed to open %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx %d", idx)
		}
		pair1 := &kvpb.PBKV{
			Key: "test-key1",
			Val: "test-val1",
		}
		pair2 := &kvpb.PBKV{
			Key: "test-key2",
			Val: "test-val2",
		}
		pair3 := &kvpb.PBKV{
			Key: "test-key3",
			Val: "test-val3",
		}
		data1, err := pair1.Marshal()
		if err != nil {
			panic(err)
		}
		data2, err := pair2.Marshal()
		if err != nil {
			panic(err)
		}
		data3, err := pair3.Marshal()
		if err != nil {
			panic(err)
		}
		ents := []sm.Entry{
			{Index: 1, Cmd: data1},
			{Index: 2, Cmd: data2},
		}
		odsm.Update(ents)
		hash1 := odsm.GetHash()
		buf := bytes.NewBuffer(make([]byte, 0, 128))
		ctx, err := odsm.PrepareSnapshot()
		if err != nil {
			t.Fatalf("prepare snapshot failed %v", err)
		}
		_, err = odsm.CreateSnapshot(ctx, buf, nil)
		if err != nil {
			t.Fatalf("create snapshot failed %v", err)
		}
		odsm.Update([]sm.Entry{{Index: 3, Cmd: data3}})
		result, err := odsm.Lookup([]byte("test-key3"))
		if err != nil {
			t.Fatalf("lookup failed %v", err)
		}
		if !bytes.Equal(result, []byte("test-val3")) {
			t.Errorf("value not set")
		}
		hash2 := odsm.GetHash()
		if hash1 == hash2 {
			t.Errorf("hash doesn't change")
		}
		reader := bytes.NewBuffer(buf.Bytes())
		if err := odsm.RecoverFromSnapshot(reader, nil); err != nil {
			t.Fatalf("recover from snapshot failed %v", err)
		}
		hash3 := odsm.GetHash()
		if hash3 != hash1 {
			t.Errorf("hash changed")
		}
		result, err = odsm.Lookup([]byte("test-key3"))
		if err != nil {
			t.Fatalf("lookup failed %v", err)
		}
		if len(result) > 0 {
			t.Fatalf("test-key3 still available in the db")
		}
		odsm.Close()
		result, err = odsm.Lookup([]byte("test-key3"))
		if err == nil {
			t.Fatalf("lookup allowed after close")
		}
		if len(result) != 0 {
			t.Fatalf("returned something %v", result)
		}
	}
	runDiskKVTest(t, tf)
}
