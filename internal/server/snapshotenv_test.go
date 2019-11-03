// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

	pb "github.com/lni/dragonboat/v3/raftpb"
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
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshottingMode)
	dir := env.GetTempDir()
	if !strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
	env = NewSSEnv(f, 1, 1, 1, 2, ReceivingMode)
	dir = env.GetTempDir()
	if !strings.Contains(dir, ".receiving") {
		t.Errorf("unexpected suffix: %s", dir)
	}
}

func TestFinalSnapshotDirDoesNotContainTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshottingMode)
	dir := env.GetFinalDir()
	if strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
}

func TestRootDirIsTheParentOfTempFinalDirs(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	rootDir := env.GetRootDir()
	mustBeChild(rootDir, tmpDir)
	mustBeChild(rootDir, finalDir)
}

func runEnvTest(t *testing.T, f func(t *testing.T, env *SSEnv)) {
	rd := "server-pkg-test-data-safe-to-delete"
	ff := func(cid uint64, nid uint64) string {
		return rd
	}
	defer func() {
		os.RemoveAll(rd)
	}()
	env := NewSSEnv(ff, 1, 1, 1, 2, SnapshottingMode)
	tmpDir := env.GetTempDir()
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("%v", err)
	}
	f(t, env)
}

func TestRenameTempDirToFinalDir(t *testing.T) {
	tf := func(t *testing.T, env *SSEnv) {
		finalDir := env.GetFinalDir()
		if err := os.MkdirAll(finalDir, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		// TODO:
		// this fails on windows, see snapshotenv.go for details
		err := env.renameTempDirToFinalDir()
		if err != ErrSnapshotOutOfDate {
			t.Errorf("err is nil")
		}
	}
	runEnvTest(t, tf)
}

func TestRenameTempDirToFinalDirCanComplete(t *testing.T) {
	tf := func(t *testing.T, env *SSEnv) {
		if env.isFinalDirExists() {
			t.Errorf("final dir already exist")
		}
		err := env.renameTempDirToFinalDir()
		if err != nil {
			t.Errorf("rename tmp dir to final dir failed %v", err)
		}
		if !env.isFinalDirExists() {
			t.Errorf("final dir does not exist")
		}
		if env.HasFlagFile() {
			t.Errorf("flag file not suppose to be there")
		}
	}
	runEnvTest(t, tf)
}

func TestFlagFileExists(t *testing.T) {
	tf := func(t *testing.T, env *SSEnv) {
		if env.isFinalDirExists() {
			t.Errorf("final dir already exist")
		}
		msg := &pb.Message{}
		if err := env.createFlagFile(msg); err != nil {
			t.Errorf("failed to create flag file")
		}
		err := env.renameTempDirToFinalDir()
		if err != nil {
			t.Errorf("rename tmp dir to final dir failed %v", err)
		}
		if !env.isFinalDirExists() {
			t.Errorf("final dir does not exist")
		}
		if !env.HasFlagFile() {
			t.Errorf("flag file not suppose to be there")
		}
	}
	runEnvTest(t, tf)
}

func TestFinalizeSnapshotCanComplete(t *testing.T) {
	tf := func(t *testing.T, env *SSEnv) {
		m := &pb.Message{}
		if err := env.FinalizeSnapshot(m); err != nil {
			t.Errorf("failed to finalize snapshot %v", err)
		}
		if !env.HasFlagFile() {
			t.Errorf("no flag file")
		}
		if !env.isFinalDirExists() {
			t.Errorf("no final dir")
		}
	}
	runEnvTest(t, tf)
}

func TestFinalizeSnapshotReturnOutOfDateWhenFinalDirExist(t *testing.T) {
	tf := func(t *testing.T, env *SSEnv) {
		finalDir := env.GetFinalDir()
		if err := os.MkdirAll(finalDir, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		m := &pb.Message{}
		if err := env.FinalizeSnapshot(m); err != ErrSnapshotOutOfDate {
			t.Errorf("didn't return ErrSnapshotOutOfDate %v", err)
		}
		if env.HasFlagFile() {
			t.Errorf("flag file exist")
		}
	}
	runEnvTest(t, tf)
}
