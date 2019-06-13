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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest
// +build !dragonboat_no_rocksdb
// +build !dragonboat_pebble_test
// +build !dragonboat_leveldb_test

package dragonboat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/plugin/rocksdb"
	"github.com/lni/dragonboat/raftio"
	sm "github.com/lni/dragonboat/statemachine"
)

func TestBatchedAndPlainEntriesAreNotCompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 100,
		RaftAddress:    nodeHostTestAddr1,
		LogDBFactory:   rocksdb.NewBatchedLogDB,
	}
	plog.Infof("going to create nh using batched logdb")
	nh, err := NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create node host %v", err)
	}
	bf := nh.logdb.BinaryFormat()
	if bf != raftio.LogDBBinVersion {
		t.Errorf("unexpected logdb bin ver %d", bf)
	}
	nh.Stop()
	plog.Infof("node host 1 stopped")
	nhc.LogDBFactory = nil
	func() {
		plog.Infof("going to create nh using plain logdb")
		nh, err := NewNodeHost(nhc)
		plog.Infof("err : %v", err)
		if err != server.ErrLogDBBrokenChange {
			if err == nil && nh != nil {
				plog.Infof("going to stop nh")
				nh.Stop()
			}
			t.Fatalf("didn't return the expected error")
		}
	}()
	os.RemoveAll(singleNodeHostTestDir)
	plog.Infof("going to create nh using plain logdb with existing data deleted")
	nh, err = NewNodeHost(nhc)
	plog.Infof("err2 : %v", err)
	if err != nil {
		t.Fatalf("failed to create node host %v", err)
	}
	defer nh.Stop()
	bf = nh.logdb.BinaryFormat()
	if bf != raftio.PlainLogDBBinVersion {
		t.Errorf("unexpected logdb bin ver %d", bf)
	}
}

func TestNodeHostReturnsErrLogDBBrokenChangeWhenLogDBTypeChanges(t *testing.T) {
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 200,
		RaftAddress:    nodeHostTestAddr1,
		LogDBFactory:   rocksdb.NewBatchedLogDB,
	}
	func() {
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer nh.Stop()
	}()
	nhc.LogDBFactory = nil
	_, err := NewNodeHost(nhc)
	if err != server.ErrLogDBBrokenChange {
		t.Fatalf("failed to return ErrIncompatibleData")
	}
}

func TestNodeHostByDefaultUsePlainEntryLogDB(t *testing.T) {
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 20,
		RaftAddress:    nodeHostTestAddr1,
	}
	func() {
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer nh.Stop()
		rc := config.Config{
			NodeID:       1,
			ClusterID:    1,
			ElectionRTT:  3,
			HeartbeatRTT: 1,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return &PST{}
		}
		if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh, 1)
		cs := nh.GetNoOPSession(1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = nh.SyncPropose(ctx, cs, []byte("test-data"))
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
	}()
	nhc.LogDBFactory = rocksdb.NewBatchedLogDB
	_, err := NewNodeHost(nhc)
	if err != server.ErrIncompatibleData {
		t.Fatalf("failed to return server.ErrIncompatibleData")
	}
}

func TestNodeHostByDefaultChecksWhetherToUseBatchedLogDB(t *testing.T) {
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 20,
		RaftAddress:    nodeHostTestAddr1,
		LogDBFactory:   rocksdb.NewBatchedLogDB,
	}
	tf := func() {
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer nh.Stop()
		rc := config.Config{
			NodeID:       1,
			ClusterID:    1,
			ElectionRTT:  3,
			HeartbeatRTT: 1,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return &PST{}
		}
		if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh, 1)
		cs := nh.GetNoOPSession(1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = nh.SyncPropose(ctx, cs, []byte("test-data"))
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
	}
	tf()
	nhc.LogDBFactory = nil
	tf()
}
