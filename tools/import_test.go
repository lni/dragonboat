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

package tools

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/server"
	pb "github.com/lni/dragonboat/raftpb"
)

var (
	testDataDir    = "import_test_safe_to_delete"
	testDstDataDir = "import_test_dst_safe_to_delete"
)

func TestCheckImportSettings(t *testing.T) {
	members := make(map[uint64]string)
	err := checkImportSettings(config.NodeHostConfig{}, members, 1)
	if err != ErrInvalidMembers {
		t.Errorf("invalid members not reported")
	}
	members[1] = "a1"
	err = checkImportSettings(config.NodeHostConfig{RaftAddress: "a2"}, members, 1)
	if err != ErrInvalidMembers {
		t.Errorf("invalid member address not reported")
	}
	err = checkImportSettings(config.NodeHostConfig{RaftAddress: "a1"}, members, 1)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestGetSnapshotFilenames(t *testing.T) {
	os.RemoveAll(testDataDir)
	os.MkdirAll(testDataDir, 0755)
	defer os.RemoveAll(testDataDir)
	for i := 0; i < 16; i++ {
		fn := fmt.Sprintf("%d.%s", i, server.SnapshotFileSuffix)
		dst := filepath.Join(testDataDir, fn)
		f, err := os.Create(dst)
		if err != nil {
			t.Fatalf("failed to create file %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file %v", err)
		}
	}
	fns, err := getSnapshotFilenames(testDataDir)
	if err != nil {
		t.Fatalf("failed to get filenames %v", err)
	}
	if len(fns) != 16 {
		t.Errorf("failed to return all filenames")
	}
	fps, err := getSnapshotFiles(testDataDir)
	if err != nil {
		t.Fatalf("failed to get filenames %v", err)
	}
	if len(fps) != 16 {
		t.Errorf("failed to return all filenames")
	}
	_, err = getSnapshotFilepath(testDataDir)
	if err != ErrIncompleteSnapshot {
		t.Errorf("failed to report ErrIncompleteSnapshot, got %v", err)
	}
}

func TestSnapshotFilepath(t *testing.T) {
	os.RemoveAll(testDataDir)
	os.MkdirAll(testDataDir, 0755)
	defer os.RemoveAll(testDataDir)
	fn := fmt.Sprintf("testdata.%s", server.SnapshotFileSuffix)
	dst := filepath.Join(testDataDir, fn)
	f, err := os.Create(dst)
	if err != nil {
		t.Fatalf("failed to create file %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file %v", err)
	}
	fp, err := getSnapshotFilepath(testDataDir)
	if err != nil {
		t.Errorf("failed to get snapshot file path %v", err)
	}
	if fp != filepath.Join(testDataDir, fn) {
		t.Errorf("unexpected fp %s", fp)
	}
}

func TestCheckMembers(t *testing.T) {
	membership := pb.Membership{
		Addresses: map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		Observers: map[uint64]string{4: "a4"},
		Removed:   map[uint64]bool{5: true},
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
		if err != nil && tt.ok {
			t.Errorf("%d, failed", idx)
		}
	}
}

func createTestDataFile(path string, sz uint64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	data := make([]byte, sz)
	rand.Read(data)
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return f.Close()
}

func TestCopySnapshot(t *testing.T) {
	os.RemoveAll(testDataDir)
	os.RemoveAll(testDstDataDir)
	os.MkdirAll(testDataDir, 0755)
	os.MkdirAll(testDstDataDir, 0755)
	defer os.RemoveAll(testDataDir)
	defer os.RemoveAll(testDstDataDir)
	src := filepath.Join(testDataDir, "test.gbsnap")
	if err := createTestDataFile(src, 1024); err != nil {
		t.Fatalf("failed to create test file %v", err)
	}
	extsrc := filepath.Join(testDataDir, "external-1")
	if err := createTestDataFile(extsrc, 2048); err != nil {
		t.Fatalf("failed to create external test file %v", err)
	}
	ss := pb.Snapshot{
		Filepath: src,
		Files:    []*pb.SnapshotFile{&pb.SnapshotFile{Filepath: extsrc}},
	}
	if err := copySnapshot(ss, testDataDir, testDstDataDir); err != nil {
		t.Fatalf("failed to copy snapshot files %v", err)
	}
	exp := filepath.Join(testDstDataDir, "test.gbsnap")
	fs, err := os.Stat(exp)
	if err != nil {
		t.Fatalf("failed to get file stat %v", err)
	}
	if fs.Size() != 1024 {
		t.Errorf("failed to copy the file")
	}
	exp = filepath.Join(testDstDataDir, "external-1")
	fs, err = os.Stat(exp)
	if err != nil {
		t.Fatalf("failed to get file stat %v", err)
	}
	if fs.Size() != 2048 {
		t.Errorf("failed to copy the file")
	}
}

func TestCopySnapshotFile(t *testing.T) {
	os.RemoveAll(testDataDir)
	os.MkdirAll(testDataDir, 0755)
	defer os.RemoveAll(testDataDir)
	src := filepath.Join(testDataDir, "test.data")
	dst := filepath.Join(testDataDir, "test.data.copied")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("failed to create test file %v", err)
	}
	data := make([]byte, 125)
	rand.Read(data)
	_, err = f.Write(data)
	if err != nil {
		t.Fatalf("failed to write test data %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file %v", err)
	}
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("failed to copy file %v", err)
	}
	dstdata, err := ioutil.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read the copied file %v", err)
	}
	if !bytes.Equal(dstdata, data) {
		t.Fatalf("content changed")
	}
}

func TestMissingMetadataFileIsReported(t *testing.T) {
	os.RemoveAll(testDataDir)
	os.MkdirAll(testDataDir, 0755)
	defer os.RemoveAll(testDataDir)
	_, err := getSnapshotRecord(testDataDir, "test.data")
	if err == nil {
		t.Fatalf("failed to report error")
	}
}

func TestGetProcessedSnapshotRecord(t *testing.T) {
	ss := pb.Snapshot{
		Filepath: "/original_dir/test.gbsnap",
		FileSize: 123,
		Index:    1023,
		Term:     10,
		Checksum: make([]byte, 8),
		Dummy:    false,
		Membership: pb.Membership{
			Removed:   make(map[uint64]bool),
			Observers: make(map[uint64]string),
			Addresses: make(map[uint64]string),
		},
		Type:      pb.OnDiskStateMachine,
		ClusterId: 345,
		Files:     make([]*pb.SnapshotFile, 0),
	}
	ss.Membership.Addresses[1] = "a1"
	ss.Membership.Addresses[2] = "a2"
	ss.Membership.Removed[3] = true
	ss.Membership.Observers[4] = "a4"
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
	newss := getProcessedSnapshotRecord(finalDir, ss, members)
	if newss.Index != ss.Index || newss.Term != ss.Term {
		t.Errorf("index/term not copied")
	}
	if newss.Dummy != ss.Dummy || newss.ClusterId != ss.ClusterId || newss.Type != ss.Type {
		t.Errorf("dummy/ClusterId/Type fields not copied")
	}
	if filepath.Dir(newss.Filepath) != finalDir {
		t.Errorf("filepath not processed %s", newss.Filepath)
	}
	for _, file := range newss.Files {
		if filepath.Dir(file.Filepath) != finalDir {
			t.Errorf("filepath in files not processed %s", file.Filepath)
		}
	}
	v, ok := newss.Membership.Addresses[1]
	if !ok || v != "a1" {
		t.Errorf("node 1 not in new ss")
	}
	_, ok = newss.Membership.Addresses[2]
	if ok {
		t.Errorf("node 2 not removed from new ss")
	}
	v, ok = newss.Membership.Addresses[5]
	if !ok || v != "a5" {
		t.Errorf("node 5 not in new ss")
	}
	if len(newss.Membership.Addresses) != 2 {
		t.Errorf("unexpected member count")
	}
	if len(newss.Membership.Observers) != 0 {
		t.Errorf("Observers not empty")
	}
	if len(newss.Membership.Removed) != 3 {
		t.Errorf("unexpected removed count")
	}
	_, ok1 := newss.Membership.Removed[2]
	_, ok2 := newss.Membership.Removed[3]
	_, ok3 := newss.Membership.Removed[4]
	if !ok1 || !ok2 || !ok3 {
		t.Errorf("unexpected removed content")
	}
}
