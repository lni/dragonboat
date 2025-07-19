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

package tools

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

var (
	testDataDir    = "import_test_safe_to_delete"
	testDstDataDir = "import_test_dst_safe_to_delete"
)

func TestCheckImportSettings(t *testing.T) {
	members := make(map[uint64]string)
	err := checkImportSettings(config.NodeHostConfig{}, members, 1)
	require.Equal(t, ErrInvalidMembers, err, "invalid members not reported")

	members[1] = "a1"
	err = checkImportSettings(config.NodeHostConfig{RaftAddress: "a2"},
		members, 1)
	require.Equal(t, ErrInvalidMembers, err, "invalid member address not reported")

	err = checkImportSettings(config.NodeHostConfig{RaftAddress: "a1"},
		members, 1)
	require.NoError(t, err)
}

func TestGetSnapshotFilenames(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testDataDir))
	require.NoError(t, fs.MkdirAll(testDataDir, 0755))

	defer func() {
		require.NoError(t, fs.RemoveAll(testDataDir))
	}()

	for i := 0; i < 16; i++ {
		fn := fmt.Sprintf("%d.%s", i, server.SnapshotFileSuffix)
		dst := fs.PathJoin(testDataDir, fn)
		f, err := fs.Create(dst)
		require.NoError(t, err, "failed to create file")
		require.NoError(t, f.Close(), "failed to close file")
	}

	fns, err := getSnapshotFilenames(testDataDir, fs)
	require.NoError(t, err, "failed to get filenames")
	require.Equal(t, 16, len(fns), "failed to return all filenames")

	fps, err := getSnapshotFiles(testDataDir, fs)
	require.NoError(t, err, "failed to get filenames")
	require.Equal(t, 16, len(fps), "failed to return all filenames")

	_, err = getSnapshotFilepath(testDataDir, fs)
	require.Equal(t, ErrIncompleteSnapshot, err,
		"failed to report ErrIncompleteSnapshot")
}

func TestSnapshotFilepath(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testDataDir))
	require.NoError(t, fs.MkdirAll(testDataDir, 0755))

	defer func() {
		require.NoError(t, fs.RemoveAll(testDataDir))
	}()

	fn := fmt.Sprintf("testdata.%s", server.SnapshotFileSuffix)
	dst := fs.PathJoin(testDataDir, fn)
	f, err := fs.Create(dst)
	require.NoError(t, err, "failed to create file")
	require.NoError(t, f.Close(), "failed to close file")

	fp, err := getSnapshotFilepath(testDataDir, fs)
	require.NoError(t, err, "failed to get snapshot file path")
	require.Equal(t, fs.PathJoin(testDataDir, fn), fp, "unexpected fp")
}

func TestCheckMembers(t *testing.T) {
	membership := pb.Membership{
		Addresses:  map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		NonVotings: map[uint64]string{4: "a4"},
		Removed:    map[uint64]bool{5: true},
	}
	tests := []struct {
		members map[uint64]string
		ok      bool
	}{
		{map[uint64]string{1: "a2"}, false},
		{map[uint64]string{4: "a4"}, false},
		{map[uint64]string{4: "a5"}, false},
		{map[uint64]string{5: "a5"}, false},
		{map[uint64]string{6: "a6"}, true},
	}
	for idx, tt := range tests {
		err := checkMembers(membership, tt.members)
		if tt.ok {
			require.NoError(t, err, "test case %d failed", idx)
		} else {
			require.Error(t, err, "test case %d should have failed", idx)
		}
	}
}

func createTestDataFile(path string, sz uint64, fs vfs.IFS) error {
	f, err := fs.Create(path)
	if err != nil {
		return err
	}
	data := make([]byte, sz)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return f.Close()
}

func TestCopySnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testDataDir))
	require.NoError(t, fs.RemoveAll(testDstDataDir))
	require.NoError(t, fs.MkdirAll(testDataDir, 0755))
	require.NoError(t, fs.MkdirAll(testDstDataDir, 0755))

	defer func() {
		require.NoError(t, fs.RemoveAll(testDataDir))
	}()
	defer func() {
		require.NoError(t, fs.RemoveAll(testDstDataDir))
	}()

	src := fs.PathJoin(testDataDir, "test.gbsnap")
	require.NoError(t, createTestDataFile(src, 1024, fs),
		"failed to create test file")

	extsrc := fs.PathJoin(testDataDir, "external-1")
	require.NoError(t, createTestDataFile(extsrc, 2048, fs),
		"failed to create external test file")

	ss := pb.Snapshot{
		Filepath: src,
		Files:    []*pb.SnapshotFile{{Filepath: extsrc}},
	}
	require.NoError(t, copySnapshot(ss, testDataDir, testDstDataDir, fs),
		"failed to copy snapshot files")

	exp := fs.PathJoin(testDstDataDir, "test.gbsnap")
	fi, err := fs.Stat(exp)
	require.NoError(t, err, "failed to get file stat")
	require.Equal(t, int64(1024), fi.Size(), "failed to copy the file")

	exp = fs.PathJoin(testDstDataDir, "external-1")
	fi, err = fs.Stat(exp)
	require.NoError(t, err, "failed to get file stat")
	require.Equal(t, int64(2048), fi.Size(), "failed to copy the file")
}

func TestCopySnapshotFile(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testDataDir))
	require.NoError(t, fs.MkdirAll(testDataDir, 0755))

	defer func() {
		require.NoError(t, fs.RemoveAll(testDataDir))
	}()

	src := fs.PathJoin(testDataDir, "test.data")
	dst := fs.PathJoin(testDataDir, "test.data.copied")
	f, err := fs.Create(src)
	require.NoError(t, err, "failed to create test file")

	data := make([]byte, 125)
	_, err = rand.Read(data)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err, "failed to write test data")
	require.NoError(t, f.Close(), "failed to close file")

	require.NoError(t, copyFile(src, dst, fs), "failed to copy file")

	buf := &bytes.Buffer{}
	dstf, err := fs.Open(dst)
	require.NoError(t, err, "failed to open")
	defer func() {
		require.NoError(t, dstf.Close())
	}()

	_, err = io.Copy(buf, dstf)
	require.NoError(t, err)
	require.True(t, bytes.Equal(buf.Bytes(), data), "content changed")
}

func TestMissingMetadataFileIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testDataDir))
	require.NoError(t, fs.MkdirAll(testDataDir, 0755))

	defer func() {
		require.NoError(t, fs.RemoveAll(testDataDir))
	}()

	_, err := getSnapshotRecord(testDataDir, "test.data", fs)
	require.Error(t, err, "failed to report error")
}

func TestGetProcessedSnapshotRecord(t *testing.T) {
	fs := vfs.GetTestFS()
	ss := pb.Snapshot{
		Filepath: "/original_dir/test.gbsnap",
		FileSize: 123,
		Index:    1023,
		Term:     10,
		Checksum: make([]byte, 8),
		Dummy:    false,
		Membership: pb.Membership{
			Removed:    make(map[uint64]bool),
			NonVotings: make(map[uint64]string),
			Addresses:  make(map[uint64]string),
		},
		Type:    pb.OnDiskStateMachine,
		ShardID: 345,
		Files:   make([]*pb.SnapshotFile, 0),
	}
	ss.Membership.Addresses[1] = "a1"
	ss.Membership.Addresses[2] = "a2"
	ss.Membership.Removed[3] = true
	ss.Membership.NonVotings[4] = "a4"

	f1 := &pb.SnapshotFile{
		Filepath: "/original_dir/external-1",
		FileSize: 1,
		FileId:   1,
		Metadata: make([]byte, 8),
	}
	f2 := &pb.SnapshotFile{
		Filepath: "/original_dir/external-2",
		FileSize: 2,
		FileId:   2,
		Metadata: make([]byte, 8),
	}
	ss.Files = append(ss.Files, f1)
	ss.Files = append(ss.Files, f2)

	members := make(map[uint64]string)
	members[1] = "a1"
	members[5] = "a5"
	finalDir := "final_data"

	newss := getProcessedSnapshotRecord(finalDir, ss, members, fs)
	require.Equal(t, ss.Index, newss.Index, "index/term not copied")
	require.Equal(t, ss.Term, newss.Term, "index/term not copied")
	require.Equal(t, ss.Dummy, newss.Dummy, "dummy/ShardId/Type fields not copied")
	require.Equal(t, ss.ShardID, newss.ShardID, "dummy/ShardId/Type fields not copied")
	require.Equal(t, ss.Type, newss.Type, "dummy/ShardId/Type fields not copied")
	require.Equal(t, finalDir, fs.PathDir(newss.Filepath),
		"filepath not processed")

	for _, file := range newss.Files {
		require.Equal(t, finalDir, fs.PathDir(file.Filepath),
			"filepath in files not processed")
	}

	v, ok := newss.Membership.Addresses[1]
	require.True(t, ok, "node 1 not in new ss")
	require.Equal(t, "a1", v, "node 1 not in new ss")

	_, ok = newss.Membership.Addresses[2]
	require.False(t, ok, "node 2 not removed from new ss")

	v, ok = newss.Membership.Addresses[5]
	require.True(t, ok, "node 5 not in new ss")
	require.Equal(t, "a5", v, "node 5 not in new ss")

	require.Equal(t, 2, len(newss.Membership.Addresses),
		"unexpected member count")
	require.Equal(t, 0, len(newss.Membership.NonVotings),
		"NonVotings not empty")
	require.Equal(t, 3, len(newss.Membership.Removed),
		"unexpected removed count")

	_, ok1 := newss.Membership.Removed[2]
	_, ok2 := newss.Membership.Removed[3]
	_, ok3 := newss.Membership.Removed[4]
	require.True(t, ok1, "unexpected removed content")
	require.True(t, ok2, "unexpected removed content")
	require.True(t, ok3, "unexpected removed content")
}
