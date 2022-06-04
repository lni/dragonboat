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
	"testing"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestParseFilename(t *testing.T) {
	tests := []struct {
		fileType    fileType
		fileNum     fileNum
		ok          bool
		needFileNum bool
	}{
		{fileTypeLog, fileNum(10), true, true},
		{fileTypeLogTemp, fileNum(10), true, true},
		{fileTypeIndex, fileNum(10), true, true},
		{fileTypeIndexTemp, fileNum(10), true, true},
		{fileTypeLock, fileNum(0), true, false},
		{fileTypeManifest, fileNum(10), true, true},
		{fileTypeCurrent, fileNum(0), true, false},
		{fileTypeTemp, fileNum(10), true, false},
	}

	fs := vfs.NewMem()
	dbdir := "db-dir"
	for idx, tt := range tests {
		fn := makeFilename(fs, dbdir, tt.fileType, tt.fileNum)
		ft, logNum, ok := parseFilename(fs, fn)
		require.Equalf(t, tt.ok, ok, "idx: %d", idx)
		require.Equalf(t, tt.fileType, ft, "idx: %d", idx)
		if tt.needFileNum {
			require.Equalf(t, tt.fileNum, logNum, "idx: %d", idx)
		}
	}
}

func TestMakeBootstrapFilename(t *testing.T) {
	fs := vfs.NewMem()
	dbdir := "db-dir"
	fn := makeBootstrapFilename(fs, dbdir, 2, 3, false)
	require.Equal(t, "db-dir/BOOTSTRAP-2-3", fn)
	fn = makeBootstrapFilename(fs, dbdir, 2, 3, true)
	require.Equal(t, "db-dir/BOOTSTRAP-2-3.tmp", fn)
}

func TestParseBootstrapFilename(t *testing.T) {
	fn := "BOOTSTRAP-2-3"
	shardID, replicaID, ok := parseBootstrapFilename(fn)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(2), shardID)
	require.Equal(t, uint64(3), replicaID)
	fn = "BOOTSTRAP-2-"
	_, _, ok = parseBootstrapFilename(fn)
	require.Equal(t, false, ok)
}
