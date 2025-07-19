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
	"github.com/stretchr/testify/require"

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
	require.Equal(t, 2, len(fc.files))
	require.Equal(t, 2, len(fc.idmap))
	require.Equal(t, "test.data", fc.files[0].Filepath)
	require.Equal(t, uint64(1), fc.files[0].FileId)
	require.Equal(t, 12, len(fc.files[0].Metadata))
	require.Equal(t, "test.data2", fc.files[1].Filepath)
	require.Equal(t, uint64(2), fc.files[1].FileId)
	require.Equal(t, 16, len(fc.files[1].Metadata))
}

func TestFileWithDuplicatedIDCanNotBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fc := NewFileCollection()
	fc.AddFile(1, "test.data", make([]byte, 12))
	fc.AddFile(2, "test.data", make([]byte, 12))
	require.Panics(t, func() {
		fc.AddFile(1, "test.data2", make([]byte, 16))
	})
}

func TestPrepareFiles(t *testing.T) {
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("this test only support the default fs")
	}
	defer leaktest.AfterTest(t)()
	err := fs.MkdirAll(rdbTestDirectory, 0755)
	require.NoError(t, err)
	defer func() {
		err := fs.RemoveAll(rdbTestDirectory)
		require.NoError(t, err)
	}()
	f1, err := fs.Create(fs.PathJoin(rdbTestDirectory, "test1.data"))
	require.NoError(t, err)
	n, err := f1.Write(make([]byte, 16))
	require.Equal(t, 16, n)
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	f2, err := fs.Create(fs.PathJoin(rdbTestDirectory, "test2.data"))
	require.NoError(t, err)
	n, err = f2.Write(make([]byte, 32))
	require.Equal(t, 32, n)
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	fc := NewFileCollection()
	fc.AddFile(1, fs.PathJoin(rdbTestDirectory, "test1.data"),
		make([]byte, 8))
	fc.AddFile(2, fs.PathJoin(rdbTestDirectory, "test2.data"),
		make([]byte, 2))
	require.Equal(t, uint64(2), fc.Size())
	rf := fc.GetFileAt(0)
	expectedPath := fs.PathJoin(rdbTestDirectory, "test1.data")
	require.Equal(t, expectedPath, rf.Filepath)
	files, err := fc.PrepareFiles(rdbTestDirectory, rdbTestDirectory)
	require.NoError(t, err)
	require.Equal(t, uint64(1), files[0].FileId)
	require.Equal(t, "external-file-1", files[0].Filename())
	require.Equal(t, uint64(16), files[0].FileSize)
	require.Equal(t, uint64(2), files[1].FileId)
	require.Equal(t, "external-file-2", files[1].Filename())
	require.Equal(t, uint64(32), files[1].FileSize)
	fi1, err := fs.Stat(fs.PathJoin(rdbTestDirectory, "external-file-1"))
	require.NoError(t, err)
	require.Equal(t, int64(16), fi1.Size())
	fi2, err := fs.Stat(fs.PathJoin(rdbTestDirectory, "external-file-2"))
	require.NoError(t, err)
	require.Equal(t, int64(32), fi2.Size())
}
