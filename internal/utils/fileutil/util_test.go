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
	"testing"

	pb "github.com/lni/dragonboat/raftpb"
)

var (
	testDirName = "utils_test_data_safe_to_delete"
)

func TestMarkDirAsDeleted(t *testing.T) {
	os.RemoveAll(testDirName)
	if err := MkdirAll(testDirName); err != nil {
		t.Fatalf("%v", err)
	}
	defer os.RemoveAll(testDirName)
	s := &pb.Message{}
	if err := MarkDirAsDeleted(testDirName, s); err != nil {
		t.Fatalf("failed to makr dir as deleted %v", err)
	}
	fp := filepath.Join(testDirName, deleteFilename)
	ok, err := Exist(fp)
	if err != nil {
		t.Fatalf("failed to check file %v", err)
	}
	if !ok {
		t.Fatalf("delete flag file not created")
	}
}

func TestIsDirMarkedAsDeleted(t *testing.T) {
	os.RemoveAll(testDirName)
	if err := MkdirAll(testDirName); err != nil {
		t.Fatalf("%v", err)
	}
	defer os.RemoveAll(testDirName)
	marked, err := IsDirMarkedAsDeleted(testDirName)
	if err != nil {
		t.Fatalf("IsDirMarkedAsDeleted failed %v", err)
	}
	if marked {
		t.Errorf("unexpectedly marked as deleted")
	}
	s := &pb.Message{}
	if err := MarkDirAsDeleted(testDirName, s); err != nil {
		t.Fatalf("failed to makr dir as deleted %v", err)
	}
	marked, err = IsDirMarkedAsDeleted(testDirName)
	if err != nil {
		t.Fatalf("IsDirMarkedAsDeleted failed %v", err)
	}
	if !marked {
		t.Errorf("unexpectedly not marked as deleted")
	}
	fp := filepath.Join(testDirName, deleteFilename)
	os.RemoveAll(fp)
	marked, err = IsDirMarkedAsDeleted(testDirName)
	if err != nil {
		t.Fatalf("IsDirMarkedAsDeleted failed %v", err)
	}
	if marked {
		t.Errorf("unexpectedly marked as deleted")
	}
}
