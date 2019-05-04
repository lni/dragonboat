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
	"testing"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
)

const (
	singleNodeHostTestDir = "test_nodehost_dir_safe_to_delete"
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

func TestNodeHostDirectoryWorksWhenEverythingMatches(t *testing.T) {
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

func TestNodeHostDirectoryDetectsMismatchedBinVer(t *testing.T) {
	testNodeHostDirectoryDetectsMismatches(t, testAddress, raftio.LogDBBinVersion+1, ErrHardSettingsChanged)
}

func TestNodeHostDirectoryDetectsMismatchedAddress(t *testing.T) {
	testNodeHostDirectoryDetectsMismatches(t, "invalid:12345", raftio.LogDBBinVersion, ErrNotOwner)
}

func testNodeHostDirectoryDetectsMismatches(t *testing.T,
	addr string, binVer uint32, expErr error) {
	defer os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	ctx, err := NewContext(c)
	if err != nil {
		t.Fatalf("failed to new context %v", err)
	}
	ctx.CreateNodeHostDir(testDeploymentID)
	dirs, _ := ctx.GetLogDBDirs(testDeploymentID)
	status := raftpb.RaftDataStatus{
		Address: addr,
		BinVer:  binVer,
	}
	err = fileutil.CreateFlagFile(dirs[0], addressFilename, &status)
	if err != nil {
		t.Errorf("failed to create flag file %v", err)
	}
	err = ctx.CheckNodeHostDir(testDeploymentID, testAddress, binVer)
	if err != expErr {
		t.Errorf("expect err %v, got %v", expErr, err)
	}
}
