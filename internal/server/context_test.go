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
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
)

const (
	singleNodeHostTestDir = "test_nodehost_dir_safe_to_delete"
	testBinVer            = raftio.LogDBBinVersion
	testAddress           = "localhost:1111"
	testDeploymentID      = 100
)

func getTestNodeHostConfig() config.NodeHostConfig {
	return config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 50,
		RaftAddress:    testAddress,
	}
}

func TestCheckNodeHostDirWorksWhenEverythingMatches(t *testing.T) {
	defer os.RemoveAll(singleNodeHostTestDir)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic not expected")
		}
	}()
	c := getTestNodeHostConfig()
	ctx, err := NewContext(c)
	if err != nil {
		t.Fatalf("failed to new context %v", err)
	}
	ctx.CreateNodeHostDir(testDeploymentID)
	dirs, _ := ctx.GetLogDBDirs(testDeploymentID)
	status := raftpb.RaftDataStatus{
		Address:  testAddress,
		BinVer:   raftio.LogDBBinVersion,
		HardHash: settings.Hard.Hash(),
	}
	err = fileutil.CreateFlagFile(dirs[0], addressFilename, &status)
	if err != nil {
		t.Errorf("failed to create flag file %v", err)
	}
	ctx.CheckNodeHostDir(testDeploymentID, testAddress, raftio.LogDBBinVersion)
}

func testNodeHostDirectoryDetectsMismatches(t *testing.T,
	addr string, binVer uint32, hardHashMismatch bool, expErr error) {
	defer os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	ctx, err := NewContext(c)
	if err != nil {
		t.Fatalf("failed to new context %v", err)
	}
	ctx.CreateNodeHostDir(testDeploymentID)
	dirs, _ := ctx.GetLogDBDirs(testDeploymentID)
	status := raftpb.RaftDataStatus{
		Address:  addr,
		BinVer:   binVer,
		HardHash: settings.Hard.Hash(),
	}
	if hardHashMismatch {
		status.HardHash = 0
	}
	err = fileutil.CreateFlagFile(dirs[0], addressFilename, &status)
	if err != nil {
		t.Errorf("failed to create flag file %v", err)
	}
	err = ctx.CheckNodeHostDir(testDeploymentID, testAddress, testBinVer)
	plog.Infof("err: %v", err)
	if err != expErr {
		t.Errorf("expect err %v, got %v", expErr, err)
	}
}

func TestCanDetectMismatchedBinVer(t *testing.T) {
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, raftio.LogDBBinVersion+1, false, ErrIncompatibleData)
}

func TestCanDetectMismatchedAddress(t *testing.T) {
	testNodeHostDirectoryDetectsMismatches(t,
		"invalid:12345", raftio.LogDBBinVersion, false, ErrNotOwner)
}

func TestCanDetectMismatchedHardHash(t *testing.T) {
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, raftio.LogDBBinVersion, true, ErrHardSettingsChanged)
}

func TestLockFileCanBeLockedAndUnlocked(t *testing.T) {
	defer os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	ctx, err := NewContext(c)
	if err != nil {
		t.Fatalf("failed to new context %v", err)
	}
	ctx.CreateNodeHostDir(c.DeploymentID)
	if err := ctx.LockNodeHostDir(c.DeploymentID); err != nil {
		t.Fatalf("failed to lock the directory %v", err)
	}
	for fp := range ctx.flocks {
		if filepath.Base(fp) != lockFilename {
			t.Fatalf("not the lock file")
		}
		fl := fileutil.New(fp)
		locked, err := fl.TryLock()
		if err != nil {
			t.Fatalf("try lock failed %v", err)
		}
		if locked {
			t.Fatalf("managed to lock the file again")
		}
	}
	ctx.Stop()
	if err := ctx.LockNodeHostDir(c.DeploymentID); err != nil {
		t.Fatalf("failed to lock the directory %v", err)
	}
}
