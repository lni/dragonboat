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

// +build dragonboat_lmdb

package logdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lni/dragonboat/internal/utils/leaktest"
)

func TestLMDBKVCompaction(t *testing.T) {
	tf := func(t *testing.T, kvs IKvStore) {
		for i := 0; i < 128; i++ {
			key := fmt.Sprintf("key%.3d", i)
			val := fmt.Sprintf("val%.3d", i)
			if err := kvs.SaveValue([]byte(key), []byte(val)); err != nil {
				t.Errorf("failed to save the value")
			}
		}
		fk := []byte(fmt.Sprintf("key%.3d", 0))
		lk := []byte(fmt.Sprintf("key%.3d", 100))
		err := kvs.Compaction(fk, lk)
		if err != nil {
			t.Fatalf("compaction failed %v", err)
		}
		plog.Infof("next compaction")
		lk = []byte(fmt.Sprintf("key%.3d", 110))
		err = kvs.Compaction(fk, lk)
		if err != nil {
			t.Fatalf("compaction failed %v", err)
		}
		for i := 0; i < 110; i++ {
			key := []byte(fmt.Sprintf("key%.3d", i))
			found := false
			op := func(val []byte) error {
				if len(val) > 0 {
					found = true
				}
				return nil
			}
			err = kvs.GetValue(key, op)
			if err != nil {
				t.Fatalf("get value failed %v", err)
			}
			if found {
				t.Fatalf("not suppose to return, i = %d", i)
			}
		}
		key := []byte(fmt.Sprintf("key%.3d", 110))
		found := false
		op := func(val []byte) error {
			if len(val) > 0 {
				found = true
			}
			return nil
		}
		err = kvs.GetValue(key, op)
		if err != nil {
			t.Fatalf("get value failed %v", err)
		}
		if !found {
			t.Fatalf("failed to get val")
		}
	}
	runKVTest(t, tf)
}

func modifyLMDBContent(fp string, from string, to string) {
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
		if string(data) == from {
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
	_, err = f.Write([]byte(to))
	if err != nil {
		panic(err)
	}
}

func TestLMDBDataCorruptionIsHandled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer deleteTestDB()
	kvs, err := newKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to new kv store %v", err)
	}
	key := []byte("test-key")
	val := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	if err := kvs.SaveValue(key, val); err != nil {
		t.Fatalf("failed to save value %v", err)
	}
	if err := kvs.Close(); err != nil {
		t.Fatalf("failed to close %v", err)
	}
	fp := filepath.Join(RDBTestDirectory, "data.mdb")
	modifyLMDBContent(fp, "xxxx", "yyyy")
	kvs, err = newKVStore(RDBTestDirectory, RDBTestDirectory)
	if err != nil {
		t.Fatalf("failed to new kv store %v", err)
	}
	kvs.GetValue(key, func(v []byte) error {
		plog.Infof("v: %s", string(v))
		if len(v) > 0 && strings.Contains(string(v), "yyyy") {
			t.Fatalf("corrupted data returned")
		}
		return nil
	})
}
