// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package tan

import (
	"sync"
	"testing"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestVersionSetCanBeCreated(t *testing.T) {
	var vs versionSet
	var mu sync.Mutex
	dirname := "db-dir"
	fs := vfs.NewMem()
	defer vfs.ReportLeakedFD(fs, t)
	opt := &Options{MaxManifestFileSize: MaxManifestFileSize, FS: fs}
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	dir, err := fs.OpenDir(dirname)
	require.NoError(t, err)
	defer dir.Close()
	require.NoError(t, vs.create(dirname, opt, dir, &mu))
	require.NoError(t, vs.close())
	// manifest file is accessible from the fs
	manifestFn := makeFilename(fs, dirname, fileTypeManifest, vs.manifestFileNum)
	f, err := fs.Open(manifestFn)
	require.NoError(t, err)
	f.Close()
}

func TestVersionSetCanBeApplied(t *testing.T) {
	fs := vfs.NewMem()
	testVersionSetCanBeApplied(t, fs)
}

func testVersionSetCanBeApplied(t *testing.T, fs vfs.FS) {
	var vs versionSet
	defer vs.close()
	var mu sync.Mutex
	dirname := "db-dir"
	opt := &Options{MaxManifestFileSize: MaxManifestFileSize, FS: fs}
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	dir, err := fs.OpenDir(dirname)
	require.NoError(t, err)
	defer dir.Close()
	require.NoError(t, vs.create(dirname, opt, dir, &mu))

	var ve1 versionEdit
	var ve2 versionEdit
	nfn := vs.getNextFileNum()
	require.Equal(t, fileNum(2), nfn)
	ve1.newFiles = append(ve1.newFiles, newFileEntry{
		meta: &fileMetadata{
			fileNum: 2,
		},
	})

	mu.Lock()
	vs.logLock()
	require.NoError(t, vs.logAndApply(&ve1, dir))
	mu.Unlock()

	v := vs.currentVersion()
	require.Equal(t, 1, len(v.files))
	require.Equal(t, int32(1), v.refcnt)
	f, ok := v.files[2]
	require.True(t, ok)
	require.Equal(t, int32(1), f.refs)
	require.Equal(t, ve1.nextFileNum, vs.nextFileNum)

	nfn = vs.getNextFileNum()
	require.Equal(t, fileNum(3), nfn)
	ve2.newFiles = append(ve2.newFiles, newFileEntry{
		meta: &fileMetadata{
			fileNum: 3,
		},
	})
	ve2.deletedFiles = make(map[deletedFileEntry]*fileMetadata)
	ve2.deletedFiles[deletedFileEntry{fileNum: 2}] = &fileMetadata{fileNum: 2}
	mu.Lock()
	vs.logLock()
	require.NoError(t, vs.logAndApply(&ve2, dir))
	mu.Unlock()

	v = vs.currentVersion()
	require.Equal(t, 1, len(v.files))
	require.Equal(t, int32(1), v.refcnt)
	_, ok2 := v.files[2]
	f3, ok3 := v.files[3]
	require.False(t, ok2)
	require.True(t, ok3)
	require.Equal(t, int32(1), f3.refs)
	require.Equal(t, ve2.nextFileNum, vs.nextFileNum)
	require.Equal(t, 1, len(vs.obsoleteTables))
	require.Equal(t, fileNum(2), vs.obsoleteTables[0].fileNum)
	require.Equal(t, fileNum(1), vs.manifestFileNum)
}

func TestSwitchManifestFile(t *testing.T) {
	var vs versionSet
	var mu sync.Mutex
	dirname := "db-dir"
	fs := vfs.NewMem()
	defer vfs.ReportLeakedFD(fs, t)
	opt := &Options{MaxManifestFileSize: 1, FS: fs}
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	dir, err := fs.OpenDir(dirname)
	require.NoError(t, err)
	defer dir.Close()
	require.NoError(t, vs.create(dirname, opt, dir, &mu))
	require.Equal(t, fileNum(1), vs.manifestFileNum)

	var ve1 versionEdit
	fn := vs.getNextFileNum()
	require.Equal(t, fileNum(2), fn)
	ve1.nextFileNum = fn + 1
	ve1.newFiles = append(ve1.newFiles, newFileEntry{
		meta: &fileMetadata{
			fileNum: fn,
		},
	})

	mu.Lock()
	vs.logLock()
	require.NoError(t, vs.logAndApply(&ve1, dir))
	mu.Unlock()

	require.Equal(t, fileNum(3), vs.manifestFileNum)
	require.Equal(t, 1, len(vs.obsoleteManifests))
	require.NoError(t, vs.close())
}

func TestLoadManifest(t *testing.T) {
	fs := vfs.NewMem()
	defer vfs.ReportLeakedFD(fs, t)
	testVersionSetCanBeApplied(t, fs)
	var vs versionSet
	defer vs.close()
	var mu sync.Mutex
	dirname := "db-dir"
	opt := &Options{MaxManifestFileSize: MaxManifestFileSize, FS: fs}
	require.NoError(t, vs.load(dirname, opt, &mu))
	v := vs.currentVersion()
	require.Equal(t, 1, len(v.files))
	require.Equal(t, int32(1), v.refcnt)
	_, ok2 := v.files[2]
	f3, ok3 := v.files[3]
	require.False(t, ok2)
	require.True(t, ok3)
	require.Equal(t, int32(1), f3.refs)
	require.Equal(t, fileNum(4), vs.nextFileNum)
	require.Equal(t, 0, len(vs.obsoleteTables))
	require.Equal(t, fileNum(1), vs.manifestFileNum)
	require.Nil(t, vs.manifest)
}
