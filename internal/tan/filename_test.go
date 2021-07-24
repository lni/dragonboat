// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

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
	clusterID, nodeID, ok := parseBootstrapFilename(fn)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(2), clusterID)
	require.Equal(t, uint64(3), nodeID)
	fn = "BOOTSTRAP-2-"
	_, _, ok = parseBootstrapFilename(fn)
	require.Equal(t, false, ok)
}
