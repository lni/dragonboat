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

package server

import (
	"os"
	"strings"
	"testing"

	pb "github.com/lni/dragonboat/raftpb"
)

func TestGetSnapshotDirName(t *testing.T) {
	v := GetSnapshotDirName(1)
	if v != "snapshot-0000000000000001" {
		t.Errorf("unexpected value, %s", v)
	}
	v = GetSnapshotDirName(255)
	if v != "snapshot-00000000000000FF" {
		t.Errorf("unexpected value, %s", v)
	}
}

func TestMustBeChild(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		ok     bool
	}{
		{"/home/test", "/home", false},
		{"/home/test", "/home/test", false},
		{"/home/test", "/home/data", false},
		{"/home/test", "/home/test1", false},
		{"/home/test", "/home/test/data", true},
		{"/home/test", "", false},
	}
	for idx, tt := range tests {
		ok := true
		f := func() {
			defer func() {
				if r := recover(); r != nil {
					ok = false
				}
				if ok != tt.ok {
					t.Errorf("idx %d, expected ok value %t", idx, tt.ok)
				}
			}()
			mustBeChild(tt.parent, tt.child)
		}
		f()
	}
}

func TestTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	dir := env.GetTempDir()
	if !strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
	env = NewSnapshotEnv(f, 1, 1, 1, 2, ReceivingMode)
	dir = env.GetTempDir()
	if !strings.Contains(dir, ".receiving") {
		t.Errorf("unexpected suffix: %s", dir)
	}
}

func TestFinalSnapshotDirDoesNotContainTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	dir := env.GetFinalDir()
	if strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
}

func TestRootDirIsTheParentOfTempFinalDirs(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	rootDir := env.GetRootDir()
	mustBeChild(rootDir, tmpDir)
	mustBeChild(rootDir, finalDir)
}

func TestRenameTempDirToFinalDir(t *testing.T) {
	rd := "server-pkg-test-data-safe-to-delete"
	f := func(cid uint64, nid uint64) string {
		return rd
	}
	defer func() {
		os.RemoveAll(rd)
	}()
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	os.MkdirAll(tmpDir, 0755)
	os.MkdirAll(finalDir, 0755)
	exists, err := env.RenameTempDirToFinalDir()
	if err == nil {
		t.Errorf("err is nil")
	}
	if !exists {
		t.Errorf("exists not true")
	}
}

func TestRenameTempDirToFinalDirCanComplete(t *testing.T) {
	rd := "server-pkg-test-data-safe-to-delete"
	f := func(cid uint64, nid uint64) string {
		return rd
	}
	defer func() {
		os.RemoveAll(rd)
	}()
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	os.MkdirAll(tmpDir, 0755)
	if env.IsFinalDirExists() {
		t.Errorf("final dir already exist")
	}
	exists, err := env.RenameTempDirToFinalDir()
	if err != nil {
		t.Errorf("rename tmp dir to final dir failed %v", err)
	}
	if exists {
		t.Errorf("exists is true")
	}
	if !env.IsFinalDirExists() {
		t.Errorf("final dir does not exist")
	}
	if env.HasFlagFile() {
		t.Errorf("flag file not suppose to be there")
	}
}

func TestFlagFileExists(t *testing.T) {
	rd := "server-pkg-test-data-safe-to-delete"
	f := func(cid uint64, nid uint64) string {
		return rd
	}
	defer func() {
		os.RemoveAll(rd)
	}()
	env := NewSnapshotEnv(f, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	os.MkdirAll(tmpDir, 0755)
	if env.IsFinalDirExists() {
		t.Errorf("final dir already exist")
	}
	msg := &pb.Message{}
	if err := env.CreateFlagFile(msg); err != nil {
		t.Errorf("failed to create flag file")
	}
	exists, err := env.RenameTempDirToFinalDir()
	if err != nil {
		t.Errorf("rename tmp dir to final dir failed %v", err)
	}
	if exists {
		t.Errorf("exists is true")
	}
	if !env.IsFinalDirExists() {
		t.Errorf("final dir does not exist")
	}
	if !env.HasFlagFile() {
		t.Errorf("flag file not suppose to be there")
	}
}
