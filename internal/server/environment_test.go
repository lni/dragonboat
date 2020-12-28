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
	"fmt"
	"strings"
	"testing"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
)

const (
	singleNodeHostTestDir = "test_nodehost_dir_safe_to_delete"
	testLogDBName         = "test-name"
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
	fs := vfs.GetTestFS()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		c := getTestNodeHostConfig()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic not expected")
			}
		}()
		env, err := NewEnv(c, fs)
		if err != nil {
			t.Fatalf("failed to new environment %v", err)
		}
		if _, _, err := env.CreateNodeHostDir(testDeploymentID); err != nil {
			t.Fatalf("%v", err)
		}
		dir, _ := env.getDataDirs()
		testName := "test-name"
		cfg := config.NodeHostConfig{
			Expert:       config.GetDefaultExpertConfig(),
			RaftAddress:  testAddress,
			DeploymentID: testDeploymentID,
		}
		status := raftpb.RaftDataStatus{
			Address: testAddress,
			BinVer:  raftio.LogDBBinVersion,
			HardHash: settings.HardHash(cfg.Expert.ExecShards,
				cfg.Expert.LogDB.Shards, settings.Hard.LRUMaxSessionCount,
				settings.Hard.LogDBEntryBatchSize),
			LogdbType:    testName,
			Hostname:     env.hostname,
			DeploymentId: testDeploymentID,
		}
		err = fileutil.CreateFlagFile(dir, flagFilename, &status, fs)
		if err != nil {
			t.Errorf("failed to create flag file %v", err)
		}
		if err := env.CheckNodeHostDir(cfg,
			raftio.LogDBBinVersion, testName); err != nil {
			t.Fatalf("check node host dir failed %v", err)
		}
	}()
	reportLeakedFD(fs, t)
}

func TestRaftAddressIsAllowedToChangeWhenRequested(t *testing.T) {
	fs := vfs.GetTestFS()
	c := getTestNodeHostConfig()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	binVer := uint32(100)
	testLogDBName := "test-name"
	hostname := ""
	env, err := NewEnv(c, fs)
	if err != nil {
		t.Fatalf("failed to new environment %v", err)
	}
	if _, _, err := env.CreateNodeHostDir(testDeploymentID); err != nil {
		t.Fatalf("%v", err)
	}
	dir, _ := env.getDataDirs()
	cfg := config.NodeHostConfig{
		Expert:       config.GetDefaultExpertConfig(),
		DeploymentID: testDeploymentID,
		RaftAddress:  "addr1:12345",
	}
	status := raftpb.RaftDataStatus{
		Address: "addr2:54321",
		BinVer:  binVer,
		HardHash: settings.HardHash(cfg.Expert.ExecShards,
			cfg.Expert.LogDB.Shards, settings.Hard.LRUMaxSessionCount,
			settings.Hard.LogDBEntryBatchSize),
		LogdbType: testLogDBName,
		Hostname:  hostname,
	}
	err = fileutil.CreateFlagFile(dir, flagFilename, &status, fs)
	if err != nil {
		t.Errorf("failed to create flag file %v", err)
	}
	if err := env.CheckNodeHostDir(cfg, binVer, testLogDBName); err == nil {
		t.Fatalf("changed raft address not detected")
	}
	cfg.AddressByNodeHostID = true
	if err := env.CheckNodeHostDir(cfg, binVer, testLogDBName); err != nil {
		t.Fatalf("changed raft address not allowed")
	}
}

func testNodeHostDirectoryDetectsMismatches(t *testing.T,
	addr string, hostname string, binVer uint32, name string,
	hardHashMismatch bool, expErr error, fs vfs.IFS) {
	c := getTestNodeHostConfig()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	env, err := NewEnv(c, fs)
	if err != nil {
		t.Fatalf("failed to new environment %v", err)
	}
	if _, _, err := env.CreateNodeHostDir(testDeploymentID); err != nil {
		t.Fatalf("%v", err)
	}
	dir, _ := env.getDataDirs()
	cfg := config.NodeHostConfig{
		Expert:       config.GetDefaultExpertConfig(),
		DeploymentID: testDeploymentID,
		RaftAddress:  testAddress,
	}

	status := raftpb.RaftDataStatus{
		Address: addr,
		BinVer:  binVer,
		HardHash: settings.HardHash(cfg.Expert.ExecShards,
			cfg.Expert.LogDB.Shards, settings.Hard.LRUMaxSessionCount,
			settings.Hard.LogDBEntryBatchSize),
		LogdbType: name,
		Hostname:  hostname,
	}
	if hardHashMismatch {
		status.HardHash = 1
	}
	err = fileutil.CreateFlagFile(dir, flagFilename, &status, fs)
	if err != nil {
		t.Errorf("failed to create flag file %v", err)
	}
	err = env.CheckNodeHostDir(cfg, testBinVer, testLogDBName)
	if err != expErr {
		t.Errorf("expect err %v, got %v", expErr, err)
	}
	reportLeakedFD(fs, t)
}

func TestCanDetectMismatchedHostname(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, "incorrect-hostname", raftio.LogDBBinVersion,
		testLogDBName, false, ErrHostnameChanged, fs)
}

func TestCanDetectMismatchedLogDBName(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, "", raftio.LogDBBinVersion,
		"incorrect name", false, ErrLogDBType, fs)
}

func TestCanDetectMismatchedBinVer(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, "", raftio.LogDBBinVersion+1,
		testLogDBName, false, ErrIncompatibleData, fs)
}

func TestCanDetectMismatchedAddress(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostDirectoryDetectsMismatches(t,
		"invalid:12345", "", raftio.LogDBBinVersion,
		testLogDBName, false, ErrNotOwner, fs)
}

func TestCanDetectMismatchedHardHash(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostDirectoryDetectsMismatches(t,
		testAddress, "", raftio.LogDBBinVersion,
		testLogDBName, true, ErrHardSettingsChanged, fs)
}

func TestLockFileCanBeLockedAndUnlocked(t *testing.T) {
	fs := vfs.GetTestFS()
	c := getTestNodeHostConfig()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	env, err := NewEnv(c, fs)
	if err != nil {
		t.Fatalf("failed to new environment %v", err)
	}
	if _, _, err := env.CreateNodeHostDir(c.DeploymentID); err != nil {
		t.Fatalf("%v", err)
	}
	if err := env.LockNodeHostDir(); err != nil {
		t.Fatalf("failed to lock the directory %v", err)
	}
	env.Stop()
	reportLeakedFD(fs, t)
}

func TestRemoveSavedSnapshots(t *testing.T) {
	fs := vfs.GetTestFS()
	if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
		t.Fatalf("%v", err)
	}
	if err := fs.MkdirAll(singleNodeHostTestDir, 0755); err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	for i := 0; i < 16; i++ {
		ssdir := fs.PathJoin(singleNodeHostTestDir, fmt.Sprintf("snapshot-%X", i))
		if err := fs.MkdirAll(ssdir, 0755); err != nil {
			t.Fatalf("failed to mkdir %v", err)
		}
	}
	for i := 1; i <= 2; i++ {
		ssdir := fs.PathJoin(singleNodeHostTestDir, fmt.Sprintf("mydata-%X", i))
		if err := fs.MkdirAll(ssdir, 0755); err != nil {
			t.Fatalf("failed to mkdir %v", err)
		}
	}
	if err := removeSavedSnapshots(singleNodeHostTestDir, fs); err != nil {
		t.Fatalf("failed to remove saved snapshots %v", err)
	}
	files, err := fs.List(singleNodeHostTestDir)
	if err != nil {
		t.Fatalf("failed to read dir %v", err)
	}
	for _, fn := range files {
		fi, err := fs.Stat(fs.PathJoin(singleNodeHostTestDir, fn))
		if err != nil {
			t.Fatalf("failed to get stat %v", err)
		}
		if !fi.IsDir() {
			t.Errorf("found unexpected file %v", fi)
		}
		if fi.Name() != "mydata-1" && fi.Name() != "mydata-2" {
			t.Errorf("unexpected dir found %s", fi.Name())
		}
	}
	reportLeakedFD(fs, t)
}

func TestCompatibleLogDBType(t *testing.T) {
	sc := &Env{}
	if !sc.compatibleLogDBType("rocksdb", "pebble") ||
		!sc.compatibleLogDBType("pebble", "rocksdb") {
		t.Errorf("rocksdb/pebble marked as not compatible")
	}
	if sc.compatibleLogDBType("rocksdb", "1") ||
		sc.compatibleLogDBType("pebble", "2") ||
		sc.compatibleLogDBType("1", "rocksdb") ||
		sc.compatibleLogDBType("2", "pebble") {
		t.Errorf("unexpectedly marked as compatible")
	}
}

func TestWALDirCanBeSet(t *testing.T) {
	walDir := "d2-wal-dir-name"
	nhConfig := config.NodeHostConfig{
		NodeHostDir: "d1",
		WALDir:      walDir,
	}
	fs := vfs.GetTestFS()
	c, err := NewEnv(nhConfig, fs)
	if err != nil {
		t.Fatalf("failed to get environment %v", err)
	}
	defer c.Stop()
	dir, lldir := c.GetLogDBDirs(12345)
	if dir == lldir {
		t.Errorf("wal dir not considered")
	}
	if !strings.Contains(lldir, walDir) {
		t.Errorf("wal dir not used, %s", lldir)
	}
	if strings.Contains(dir, walDir) {
		t.Errorf("wal dir appeared in node host dir, %s", dir)
	}
}
