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

package rsm

import (
	"testing"

	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v4/internal/vfs"
)

const (
	rdbTestDirectory = "rdb_test_dir_safe_to_delete"
)

func TestFileCanBeAddedToFileCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fc := NewFileCollection()
	fc.AddFile(1, "test.data", make([]byte, 12))
	fc.AddFile(2, "test.data2", make([]byte, 16))
	if len(fc.files) != 2 || len(fc.idmap) != 2 {
		t.Errorf("file count is %d, want 2", len(fc.files))
	}
	if fc.files[0].Filepath != "test.data" ||
		fc.files[0].FileId != 1 || len(fc.files[0].Metadata) != 12 {
		t.Errorf("not the expected first file record")
	}
	if fc.files[1].Filepath != "test.data2" ||
		fc.files[1].FileId != 2 || len(fc.files[1].Metadata) != 16 {
		t.Errorf("not the expected first file record")
	}
}

func TestFileWithDuplicatedIDCanNotBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("AddFile didn't panic")
		}
	}()
	fc := NewFileCollection()
	fc.AddFile(1, "test.data", make([]byte, 12))
	fc.AddFile(2, "test.data", make([]byte, 12))
	fc.AddFile(1, "test.data2", make([]byte, 16))
}

func TestPrepareFiles(t *testing.T) {
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("this test only support the default fs")
	}
	defer leaktest.AfterTest(t)()
	if err := fs.MkdirAll(rdbTestDirectory, 0755); err != nil {
		t.Errorf("failed to make dir %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(rdbTestDirectory); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	f1, err := fs.Create(fs.PathJoin(rdbTestDirectory, "test1.data"))
	if err != nil {
		t.Fatalf("failed to create the file, %v", err)
	}
	n, err := f1.Write(make([]byte, 16))
	if n != 16 || err != nil {
		t.Fatalf("failed to write file %v", err)
	}
	f1.Close()
	f2, err := fs.Create(fs.PathJoin(rdbTestDirectory, "test2.data"))
	if err != nil {
		t.Fatalf("failed to create the file, %v", err)
	}
	n, err = f2.Write(make([]byte, 32))
	if n != 32 || err != nil {
		t.Fatalf("failed to write file %v", err)
	}
	f2.Close()
	fc := NewFileCollection()
	fc.AddFile(1, fs.PathJoin(rdbTestDirectory, "test1.data"), make([]byte, 8))
	fc.AddFile(2, fs.PathJoin(rdbTestDirectory, "test2.data"), make([]byte, 2))
	if fc.Size() != 2 {
		t.Errorf("unexpected collection size")
	}
	rf := fc.GetFileAt(0)
	if rf.Filepath != fs.PathJoin(rdbTestDirectory, "test1.data") {
		t.Errorf("unexpected path, got %s, want %s",
			rf.Filepath, fs.PathJoin(rdbTestDirectory, "test1.data"))
	}
	files, err := fc.PrepareFiles(rdbTestDirectory, rdbTestDirectory)
	if err != nil {
		t.Fatalf("prepareFiles failed %v", err)
	}
	if files[0].FileId != 1 || files[0].Filename() != "external-file-1" || files[0].FileSize != 16 {
		t.Errorf("unexpected returned file record %v", files[0])
	}
	if files[1].FileId != 2 || files[1].Filename() != "external-file-2" || files[1].FileSize != 32 {
		t.Errorf("unexpected returned file record %v", files[1])
	}
	fi1, err := fs.Stat(fs.PathJoin(rdbTestDirectory, "external-file-1"))
	if err != nil {
		t.Errorf("failed to get stat, %v", err)
	}
	if fi1.Size() != 16 {
		t.Errorf("unexpected size")
	}
	fi2, err := fs.Stat(fs.PathJoin(rdbTestDirectory, "external-file-2"))
	if err != nil {
		t.Errorf("failed to get stat, %v", err)
	}
	if fi2.Size() != 32 {
		t.Errorf("unexpected size")
	}
}
