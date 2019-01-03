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

package fileutil

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/raftpb"
)

const (
	testDir      = "utils_test_dir_safe_to_delete"
	testFilename = "test.flag.filename"
)

func createTestDir() {
	os.MkdirAll(testDir, 0755)
}

func removeTestDir() {
	os.RemoveAll(testDir)
}

func TestFlagFileCanBeCreated(t *testing.T) {
	createTestDir()
	defer removeTestDir()
	if HasFlagFile(testDir, testFilename) {
		t.Errorf("flag file already exist?")
	}
	ss := raftpb.Snapshot{
		Filepath: "/data/name1",
		FileSize: 12345,
		Index:    200,
		Term:     300,
	}
	if err := CreateFlagFile(testDir, testFilename, &ss); err != nil {
		t.Errorf("create flag file failed, %v", err)
	}
	if !HasFlagFile(testDir, testFilename) {
		t.Errorf("no flag file")
	}
	newss := raftpb.Snapshot{}
	if err := GetFlagFileContent(testDir, testFilename, &newss); err != nil {
		t.Errorf("get flag file content failed, %v", err)
	}
	if !reflect.DeepEqual(&ss, &newss) {
		t.Errorf("object changed")
	}
	if err := RemoveFlagFile(testDir, testFilename); err != nil {
		t.Errorf("remove flag file failed, %v", err)
	}
	if HasFlagFile(testDir, testFilename) {
		t.Errorf("flag file not deleted")
	}
}

func TestFlagFileIsChecksumChecked(t *testing.T) {
	createTestDir()
	defer removeTestDir()
	ss := raftpb.Snapshot{
		Filepath: "/data/name1",
		FileSize: 12345,
		Index:    200,
		Term:     300,
	}
	if err := CreateFlagFile(testDir, testFilename, &ss); err != nil {
		t.Errorf("create flag file failed, %v", err)
	}
	if !HasFlagFile(testDir, testFilename) {
		t.Errorf("no flag file")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("corrupted flag file not detected")
		}
	}()
	fp := filepath.Join(testDir, testFilename)
	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		t.Errorf("failed to open the file, %v", err)
	}
	defer f.Close()
	n, err := f.Write([]byte{1})
	if n != 1 {
		t.Errorf("failed to write 1 byte")
	}
	if err != nil {
		t.Errorf("write failed %v", err)
	}
	newss := raftpb.Snapshot{}
	if err := GetFlagFileContent(testDir, testFilename, &newss); err != nil {
		t.Errorf("get flag file content failed, %v", err)
	}
}
