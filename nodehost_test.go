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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/leaktest"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/plugin/leveldb"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/dragonboat/v3/tools"
	"github.com/lni/dragonboat/v3/tools/upgrade310"
)

var ovs = logdb.RDBContextValueSize

func getTestNodeHostConfig() *config.NodeHostConfig {
	return &config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 50,
		RaftAddress:    "localhost:1111",
	}
}

type noopLogDB struct {
}

func (n *noopLogDB) BinaryFormat() uint32                                      { return 0 }
func (n *noopLogDB) Name() string                                              { return "noopLogDB" }
func (n *noopLogDB) Close()                                                    {}
func (n *noopLogDB) GetLogDBThreadContext() raftio.IContext                    { return nil }
func (n *noopLogDB) HasNodeInfo(clusterID uint64, nodeID uint64) (bool, error) { return true, nil }
func (n *noopLogDB) CreateNodeInfo(clusterID uint64, nodeID uint64) error      { return nil }
func (n *noopLogDB) ListNodeInfo() ([]raftio.NodeInfo, error)                  { return nil, nil }
func (n *noopLogDB) SaveBootstrapInfo(clusterID uint64, nodeID uint64, bs pb.Bootstrap) error {
	return nil
}
func (n *noopLogDB) GetBootstrapInfo(clusterID uint64, nodeID uint64) (*pb.Bootstrap, error) {
	return nil, nil
}
func (n *noopLogDB) SaveRaftState(updates []pb.Update, ctx raftio.IContext) error { return nil }
func (n *noopLogDB) IterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	return nil, 0, nil
}
func (n *noopLogDB) ReadRaftState(clusterID uint64, nodeID uint64,
	lastIndex uint64) (*raftio.RaftState, error) {
	return nil, nil
}
func (n *noopLogDB) RemoveEntriesTo(clusterID uint64, nodeID uint64, index uint64) error { return nil }
func (n *noopLogDB) RemoveNodeData(clusterID uint64, nodeID uint64) error                { return nil }
func (n *noopLogDB) SaveSnapshots([]pb.Update) error                                     { return nil }
func (n *noopLogDB) DeleteSnapshot(clusterID uint64, nodeID uint64, index uint64) error  { return nil }
func (n *noopLogDB) ListSnapshots(clusterID uint64, nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	return nil, nil
}
func (n *noopLogDB) ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error {
	return nil
}

func runNodeHostTest(t *testing.T, f func()) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	f()
}

func TestLogDBCanBeExtended(t *testing.T) {
	tf := func() {
		c := getTestNodeHostConfig()
		ldb := &noopLogDB{}
		c.LogDBFactory = func([]string, []string) (raftio.ILogDB, error) {
			return ldb, nil
		}
		nh, err := NewNodeHost(*c)
		if err != nil {
			t.Fatalf("failed to create NodeHost %v", err)
		}
		defer nh.Stop()
		if nh.logdb.Name() != ldb.Name() {
			t.Errorf("logdb type name %s, expect %s", nh.logdb.Name(), ldb.Name())
		}
	}
	runNodeHostTest(t, tf)
}

func TestTCPTransportIsUsedByDefault(t *testing.T) {
	tf := func() {
		c := getTestNodeHostConfig()
		nh, err := NewNodeHost(*c)
		if err != nil {
			t.Fatalf("failed to create NodeHost %v", err)
		}
		defer nh.Stop()
		tt := nh.transport.(*transport.Transport)
		if tt.GetRaftRPC().Name() != transport.TCPRaftRPCName {
			t.Errorf("raft rpc type name %s, expect %s",
				tt.GetRaftRPC().Name(), transport.TCPRaftRPCName)
		}
	}
	runNodeHostTest(t, tf)
}

func TestRaftRPCCanBeExtended(t *testing.T) {
	tf := func() {
		c := getTestNodeHostConfig()
		c.RaftRPCFactory = transport.NewNOOPTransport
		nh, err := NewNodeHost(*c)
		if err != nil {
			t.Fatalf("failed to create NodeHost %v", err)
		}
		defer nh.Stop()
		tt := nh.transport.(*transport.Transport)
		if tt.GetRaftRPC().Name() != transport.NOOPRaftName {
			t.Errorf("raft rpc type name %s, expect %s",
				tt.GetRaftRPC().Name(), transport.NOOPRaftName)
		}
	}
	runNodeHostTest(t, tf)
}

func TestNewNodeHostReturnErrorOnInvalidConfig(t *testing.T) {
	tf := func() {
		c := getTestNodeHostConfig()
		c.RaftAddress = "12345"
		if err := c.Validate(); err == nil {
			t.Fatalf("config is not considered as invalid")
		}
		_, err := NewNodeHost(*c)
		if err == nil {
			t.Fatalf("NewNodeHost failed to return error")
		}
	}
	runNodeHostTest(t, tf)
}

func TestDeploymentIDCanBeSetUsingNodeHostConfig(t *testing.T) {
	tf := func() {
		c := getTestNodeHostConfig()
		c.DeploymentID = 100
		nh, err := NewNodeHost(*c)
		if err != nil {
			t.Fatalf("failed to create NodeHost %v", err)
		}
		defer nh.Stop()
		if nh.deploymentID != 100 {
			t.Errorf("deployment id not set")
		}
	}
	runNodeHostTest(t, tf)
}

var (
	singleNodeHostTestAddr = "localhost:26000"
	nodeHostTestAddr1      = "localhost:26000"
	nodeHostTestAddr2      = "localhost:26001"
	singleNodeHostTestDir  = "single_nodehost_test_dir_safe_to_delete"
)

type PST struct {
	mu       sync.Mutex
	stopped  bool
	saved    bool
	restored bool
	slowSave bool
}

func (n *PST) setRestored(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.restored = v
}

func (n *PST) getRestored() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.restored
}

func (n *PST) Close() error { return nil }

// Lookup locally looks up the data.
func (n *PST) Lookup(key interface{}) (interface{}, error) {
	return make([]byte, 1), nil
}

// Update updates the object.
func (n *PST) Update(data []byte) (sm.Result, error) {
	return sm.Result{Value: uint64(len(data))}, nil
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (n *PST) SaveSnapshot(w io.Writer,
	fileCollection sm.ISnapshotFileCollection,
	done <-chan struct{}) error {
	plog.Infof("save snapshot called")
	n.saved = true
	if !n.slowSave {
		_, err := w.Write([]byte("random-data"))
		if err != nil {
			panic(err)
		}
		return nil
	}
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			n.stopped = true
			plog.Infof("saveSnapshot stopped")
			return sm.ErrSnapshotStopped
		default:
		}
	}
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *PST) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	n.setRestored(true)
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			n.stopped = true
			return sm.ErrSnapshotStopped
		default:
		}
	}
}

// GetHash returns a uint64 value representing the current state of the object.
func (n *PST) GetHash() (uint64, error) {
	return 0, nil
}

func createSingleNodeTestNodeHost(addr string,
	datadir string, slowSave bool) (*NodeHost, *PST, error) {
	// config for raft
	rc := config.Config{
		NodeID:             uint64(1),
		ClusterID:          2,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 100,
		RaftAddress:    peers[1],
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		return nil, nil, err
	}
	rnhc := nh.NodeHostConfig()
	if !reflect.DeepEqual(&nhc, &rnhc) {
		panic("configuration changed")
	}
	var pst *PST
	newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		pst = &PST{slowSave: slowSave}
		return pst
	}
	if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
		return nil, nil, err
	}
	return nh, pst, nil
}

func createConcurrentTestNodeHost(addr string,
	datadir string, snapshotEntry uint64, concurrent bool) (*NodeHost, error) {
	// config for raft
	rc := config.Config{
		NodeID:             uint64(1),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    snapshotEntry,
		CompactionOverhead: 100,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 1,
		RaftAddress:    peers[1],
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	var newConcurrentSM func(uint64, uint64) sm.IConcurrentStateMachine
	var newSM func(uint64, uint64) sm.IStateMachine
	if snapshotEntry == 0 {
		newConcurrentSM = func(uint64, uint64) sm.IConcurrentStateMachine {
			return &tests.ConcurrentUpdate{}
		}
		newSM = func(uint64, uint64) sm.IStateMachine {
			return &tests.TestUpdate{}
		}
	} else {
		newConcurrentSM = func(uint64, uint64) sm.IConcurrentStateMachine {
			return &tests.ConcurrentSnapshot{}
		}
		newSM = func(uint64, uint64) sm.IStateMachine {
			return &tests.TestSnapshot{}
		}
	}
	rc.ClusterID = 1 + taskWorkerCount
	if err := nh.StartConcurrentCluster(peers, false, newConcurrentSM, rc); err != nil {
		return nil, err
	}
	rc.ClusterID = 1
	if err := nh.StartCluster(peers, false, newSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
}

func createFakeDiskTestNodeHost(addr string,
	datadir string, initialApplied uint64,
	slowOpen bool) (*NodeHost, sm.IOnDiskStateMachine, error) {
	rc := config.Config{
		ClusterID:          uint64(1),
		NodeID:             uint64(1),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    30,
		CompactionOverhead: 30,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 1,
		RaftAddress:    peers[1],
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		return nil, nil, err
	}
	fakeDiskSM := tests.NewFakeDiskSM(initialApplied)
	if slowOpen {
		atomic.StoreUint32(&fakeDiskSM.SlowOpen, 1)
	}
	newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
		return fakeDiskSM
	}
	if err := nh.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
		return nil, nil, err
	}
	return nh, fakeDiskSM, nil
}

func singleConcurrentNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), snapshotEntry uint64, concurrent bool) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, err := createConcurrentTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, snapshotEntry, concurrent)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	defer os.RemoveAll(singleNodeHostTestDir)
	defer func() {
		nh.Stop()
	}()
	waitForLeaderToBeElected(t, nh, 1)
	waitForLeaderToBeElected(t, nh, 1+taskWorkerCount)
	tf(t, nh)
}

func singleFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost, initialApplied uint64), initialApplied uint64) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createFakeDiskTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, initialApplied, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh, 1)
	defer os.RemoveAll(singleNodeHostTestDir)
	defer func() {
		nh.Stop()
	}()
	tf(t, nh, initialApplied)
}

func twoFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost)) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	nh1dir := path.Join(singleNodeHostTestDir, "nh1")
	nh2dir := path.Join(singleNodeHostTestDir, "nh2")
	os.RemoveAll(singleNodeHostTestDir)
	nh1, nh2, err := createFakeDiskTwoTestNodeHosts(nodeHostTestAddr1,
		nodeHostTestAddr2, nh1dir, nh2dir)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh1, 1)
	defer os.RemoveAll(singleNodeHostTestDir)
	defer func() {
		nh1.Stop()
		nh2.Stop()
	}()
	tf(t, nh1, nh2)
}

func createFakeDiskTwoTestNodeHosts(addr1 string, addr2 string,
	datadir1 string, datadir2 string) (*NodeHost, *NodeHost, error) {
	rc := config.Config{
		ClusterID:          1,
		NodeID:             1,
		ElectionRTT:        3,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    5,
		CompactionOverhead: 2,
	}
	peers := make(map[uint64]string)
	peers[1] = addr1
	nhc1 := config.NodeHostConfig{
		WALDir:         datadir1,
		NodeHostDir:    datadir1,
		RTTMillisecond: 10,
		RaftAddress:    addr1,
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: 10,
		RaftAddress:    addr2,
	}
	plog.Infof("dir1 %s, dir2 %s", datadir1, datadir2)
	nh1, err := NewNodeHost(nhc1)
	if err != nil {
		return nil, nil, err
	}
	nh2, err := NewNodeHost(nhc2)
	if err != nil {
		return nil, nil, err
	}
	newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
		return tests.NewFakeDiskSM(0)
	}
	if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
		return nil, nil, err
	}
	return nh1, nh2, nil
}

func createRateLimitedTestNodeHost(addr string,
	datadir string) (*NodeHost, error) {
	// config for raft
	rc := config.Config{
		NodeID:          uint64(1),
		ClusterID:       1,
		ElectionRTT:     5,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		MaxInMemLogSize: 1024 * 3,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 10,
		RaftAddress:    peers[1],
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	newRSM := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &tests.NoOP{MillisecondToSleep: 20}
	}
	if err := nh.StartCluster(peers, false, newRSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
}

func createRateLimitedTwoTestNodeHosts(addr1 string, addr2 string,
	datadir1 string, datadir2 string) (*NodeHost, *NodeHost, error) {
	rc := config.Config{
		ClusterID:       1,
		ElectionRTT:     3,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		MaxInMemLogSize: 1024 * 3,
	}
	peers := make(map[uint64]string)
	peers[1] = addr1
	peers[2] = addr2
	nhc1 := config.NodeHostConfig{
		WALDir:         datadir1,
		NodeHostDir:    datadir1,
		RTTMillisecond: 10,
		RaftAddress:    peers[1],
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: 10,
		RaftAddress:    peers[2],
	}
	plog.Infof("dir1 %s, dir2 %s", datadir1, datadir2)
	nh1, err := NewNodeHost(nhc1)
	if err != nil {
		return nil, nil, err
	}
	nh2, err := NewNodeHost(nhc2)
	if err != nil {
		return nil, nil, err
	}
	sm1 := &tests.NoOP{}
	sm2 := &tests.NoOP{}
	newRSM1 := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return sm1
	}
	newRSM2 := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return sm2
	}
	rc.NodeID = 1
	if err := nh1.StartCluster(peers, false, newRSM1, rc); err != nil {
		return nil, nil, err
	}
	rc.NodeID = 2
	if err := nh2.StartCluster(peers, false, newRSM2, rc); err != nil {
		return nil, nil, err
	}
	var leaderNh *NodeHost
	var followerNh *NodeHost
	for i := 0; i < 200; i++ {
		leaderID, ready, err := nh1.GetLeaderID(1)
		if err == nil && ready {
			if leaderID == 1 {
				leaderNh = nh1
				followerNh = nh2
				sm2.MillisecondToSleep = 20
			} else {
				leaderNh = nh2
				followerNh = nh1
				sm1.MillisecondToSleep = 20
			}
			return leaderNh, followerNh, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, nil, errors.New("failed to get usable nodehosts")
}

func rateLimitedTwoNodeHostTest(t *testing.T,
	tf func(t *testing.T, leaderNh *NodeHost, followerNh *NodeHost)) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	nh1dir := path.Join(singleNodeHostTestDir, "nh1")
	nh2dir := path.Join(singleNodeHostTestDir, "nh2")
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	nh1, nh2, err := createRateLimitedTwoTestNodeHosts(nodeHostTestAddr1,
		nodeHostTestAddr2, nh1dir, nh2dir)
	if err != nil {
		t.Fatalf("failed to create nodehost2 %v", err)
	}
	defer func() {
		nh1.Stop()
		nh2.Stop()
	}()
	tf(t, nh1, nh2)
}

func rateLimitedNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost)) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	nh, err := createRateLimitedTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh, 1)
	defer func() {
		nh.Stop()
	}()
	tf(t, nh)
}

func waitForLeaderToBeElected(t *testing.T, nh *NodeHost, clusterID uint64) {
	for i := 0; i < 200; i++ {
		_, ready, err := nh.GetLeaderID(clusterID)
		if err == nil && ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("failed to elect leader")
}

func createProposalsToTriggerSnapshot(t *testing.T,
	nh *NodeHost, count uint64, timeoutExpected bool) {
	for i := uint64(0); i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cs, err := nh.SyncGetSession(ctx, 2)
		if err != nil {
			if err == ErrTimeout {
				cancel()
				return
			}
			t.Fatalf("unexpected error %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := nh.SyncCloseSession(ctx, cs); err != nil {
			if err == ErrTimeout {
				cancel()
				return
			}

			t.Fatalf("failed to close client session %v", err)
		}
		cancel()
	}
	if timeoutExpected {
		t.Fatalf("failed to trigger ")
	}
}

func TestJoinedClusterCanBeRestartedOrJoinedAgain(t *testing.T) {
	tf := func() {
		datadir := singleNodeHostTestDir
		rc := config.Config{
			NodeID:             uint64(1),
			ClusterID:          2,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10,
			CompactionOverhead: 5,
		}
		peers := make(map[uint64]string)
		nhc := config.NodeHostConfig{
			WALDir:         datadir,
			NodeHostDir:    datadir,
			RTTMillisecond: 50,
			RaftAddress:    singleNodeHostTestAddr,
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		defer nh.Stop()
		newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return &PST{}
		}
		if err := nh.StartCluster(peers, true, newPST, rc); err != nil {
			t.Fatalf("failed to join the cluster")
		}
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to stop the cluster")
		}
		if err := nh.StartCluster(peers, true, newPST, rc); err != nil {
			t.Fatalf("failed to join the cluster again")
		}
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to stop the cluster")
		}
		if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
			t.Fatalf("failed to restartthe cluster again")
		}
	}
	runNodeHostTest(t, tf)
}

func TestSnapshotCanBeStopped(t *testing.T) {
	tf := func() {
		nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, true)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 2)
		defer os.RemoveAll(singleNodeHostTestDir)
		createProposalsToTriggerSnapshot(t, nh, 50, true)
		nh.Stop()
		time.Sleep(100 * time.Millisecond)
		if !pst.saved || !pst.stopped {
			t.Errorf("snapshot not stopped")
		}
	}
	runNodeHostTest(t, tf)
}

func TestRecoverFromSnapshotCanBeStopped(t *testing.T) {
	tf := func() {
		nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 2)
		defer os.RemoveAll(singleNodeHostTestDir)
		createProposalsToTriggerSnapshot(t, nh, 25, false)
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) == 0 {
			t.Fatalf("failed to save snapshots")
		}
		if snapshots[0].Dummy {
			t.Errorf("regular snapshot created dummy snapshot")
		}
		nh.Stop()
		nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false)
		if err != nil {
			t.Fatalf("failed to restart nodehost %v", err)
		}
		wait := 0
		for !pst.getRestored() {
			time.Sleep(100 * time.Millisecond)
			wait++
			if wait > 100 {
				break
			}
		}
		nh.Stop()
		wait = 0
		for !pst.stopped {
			time.Sleep(100 * time.Millisecond)
			wait++
			if wait > 100 {
				break
			}
		}
		if !pst.getRestored() {
			t.Errorf("not restored")
		}
		if !pst.stopped {
			t.Errorf("not stopped")
		}
	}
	runNodeHostTest(t, tf)
}

func TestInvalidContextDeadlineIsReported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		rcs, err := nh.SyncGetSession(rctx, 2)
		rcancel()
		if err != nil {
			t.Fatalf("failed to get regular session")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 95*time.Millisecond)
		defer cancel()
		cs := nh.GetNoOPSession(2)
		_, err = nh.SyncPropose(ctx, cs, make([]byte, 1))
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		_, err = nh.SyncRead(ctx, 2, nil)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		_, err = nh.SyncGetSession(ctx, 2)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		err = nh.SyncCloseSession(ctx, rcs)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		_, err = nh.SyncRequestSnapshot(ctx, 2, DefaultSnapshotOption)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		err = nh.SyncRequestDeleteNode(ctx, 2, 1, 0)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		err = nh.SyncRequestAddNode(ctx, 2, 100, "a1", 0)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		err = nh.SyncRequestAddObserver(ctx, 2, 100, "a1", 0)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestErrClusterNotFoundCanBeReturned(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		_, _, err := nh.GetLeaderID(1234)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.StaleRead(1234, nil)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.RequestSnapshot(1234, DefaultSnapshotOption, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.RequestDeleteNode(1234, 10, 0, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.RequestAddNode(1234, 10, "a1", 0, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.RequestAddObserver(1234, 10, "a1", 0, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		err = nh.RequestLeaderTransfer(1234, 10)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, err = nh.GetNodeUser(1234)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		cs := nh.GetNoOPSession(1234)
		_, err = nh.propose(cs, make([]byte, 1), nil, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		_, _, err = nh.readIndex(1234, nil, 5*time.Second)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
		err = nh.stopNode(1234, 1, true)
		if err != ErrClusterNotFound {
			t.Errorf("failed to return ErrClusterNotFound, %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestGetClusterMembership(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nh.GetClusterMembership(ctx, 2)
		if err != nil {
			t.Fatalf("failed to get cluster membership")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestRegisterASessionTwiceWillBeReported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := nh.SyncGetSession(ctx, 2)
		if err != nil {
			t.Errorf("failed to get client session %v", err)
		}
		cs.PrepareForRegister()
		rs, err := nh.ProposeSession(cs, 5*time.Second)
		if err != nil {
			t.Errorf("failed to propose client session %v", err)
		}
		r := <-rs.CompletedC
		if !r.Rejected() {
			t.Errorf("failed to reject the cs registeration")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestUnregisterNotRegisterClientSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := nh.SyncGetSession(ctx, 2)
		if err != nil {
			t.Errorf("failed to get client session %v", err)
		}
		err = nh.SyncCloseSession(ctx, cs)
		if err != nil {
			t.Errorf("failed to unregister the client session %v", err)
		}
		err = nh.SyncCloseSession(ctx, cs)
		if err != ErrRejected {
			t.Errorf("failed to reject the request %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSnapshotFilePayloadChecksumIsSaved(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		cs := nh.GetNoOPSession(2)
		logdb := nh.logdb
		snapshotted := false
		var snapshot pb.Snapshot
		for i := 0; i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, err := nh.SyncPropose(ctx, cs, []byte("test-data"))
			cancel()
			if err != nil {
				continue
			}
			snapshots, err := logdb.ListSnapshots(2, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("failed to list snapshots")
			}
			if len(snapshots) > 0 {
				snapshotted = true
				snapshot = snapshots[0]
				break
			}
		}
		if !snapshotted {
			t.Fatalf("snapshot not triggered")
		}
		crc, err := rsm.GetV2PayloadChecksum(snapshot.Filepath)
		if err != nil {
			t.Fatalf("failed to get payload checksum")
		}
		if !bytes.Equal(crc, snapshot.Checksum) {
			t.Errorf("checksum changed")
		}
		ss := pb.Snapshot{}
		if err := fileutil.GetFlagFileContent(filepath.Dir(snapshot.Filepath),
			"snapshot.metadata", &ss); err != nil {
			t.Fatalf("failed to get content %v", err)
		}
		if !reflect.DeepEqual(&ss, &snapshot) {
			t.Errorf("snapshot record changed")
		}
	}
	singleNodeHostTest(t, tf)
}

func testZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T, dirName string) {
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	defer os.RemoveAll(singleNodeHostTestDir)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	if err = nh.serverCtx.CreateSnapshotDir(nh.deploymentID, 2, 1); err != nil {
		t.Fatalf("failed to get snap dir")
	}
	snapDir := nh.serverCtx.GetSnapshotDir(nh.deploymentID, 2, 1)
	z1 := filepath.Join(snapDir, dirName)
	plog.Infof("creating %s", z1)
	if err = os.MkdirAll(z1, 0755); err != nil {
		t.Fatalf("failed to create dir %v", err)
	}
	nh.Stop()
	nh, _, err = createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	defer nh.Stop()
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	_, err = os.Stat(z1)
	if !os.IsNotExist(err) {
		t.Fatalf("failed to delete zombie dir")
	}
}

func TestZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-01.receiving")
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-10.generating")
}

func singleNodeHostTest(t *testing.T, tf func(t *testing.T, nh *NodeHost)) {
	osv := delaySampleRatio
	defer func() {
		delaySampleRatio = osv
	}()
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	delaySampleRatio = 1
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh, 2)
	defer os.RemoveAll(singleNodeHostTestDir)
	defer nh.Stop()
	tf(t, nh)
}

func testNodeHostReadIndex(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		rs, err := nh.ReadIndex(2, time.Second)
		if err != nil {
			t.Errorf("failed to read index %v", err)
		}
		if rs.node == nil {
			t.Fatal("rs.node not set")
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete read index")
		}
		_, err = nh.ReadLocalNode(rs, make([]byte, 128))
		if err != nil {
			t.Errorf("read local failed %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostReadIndex(t *testing.T) {
	testNodeHostReadIndex(t)
}

func TestNALookupCanReturnErrNotImplemented(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		rs, err := nh.ReadIndex(2, time.Second)
		if err != nil {
			t.Errorf("failed to read index %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete read index")
		}
		_, err = nh.NAReadLocalNode(rs, make([]byte, 128))
		if err != sm.ErrNotImplemented {
			t.Errorf("failed to return sm.ErrNotImplemented, got %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostSyncIOAPIs(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		cs := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		v, err := nh.SyncPropose(ctx, cs, make([]byte, 128))
		if err != nil {
			t.Errorf("make proposal failed %v", err)
		}
		if v.Value != 128 {
			t.Errorf("unexpected result")
		}
		data, err := nh.SyncRead(ctx, 2, make([]byte, 128))
		if err != nil {
			t.Errorf("make linearizable read failed %v", err)
		}
		if data == nil || len(data.([]byte)) == 0 {
			t.Errorf("failed to get result")
		}
		if err := nh.StopCluster(2); err != nil {
			t.Errorf("failed to stop cluster 2 %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSyncRequestDeleteNode(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestDeleteNode(ctx, 2, 2, 0)
		if err != nil {
			t.Errorf("failed to delete node %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSyncRequestAddNode(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestAddNode(ctx, 2, 2, "localhost:25000", 0)
		if err != nil {
			t.Errorf("failed to add node %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSyncRequestAddObserver(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestAddObserver(ctx, 2, 2, "localhost:25000", 0)
		if err != nil {
			t.Errorf("failed to add observer %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostAddNode(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		rs, err := nh.RequestAddNode(2, 2, "localhost:25000", 0, time.Second)
		if err != nil {
			t.Errorf("failed to add node %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete add node")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostGetNodeUser(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		n, err := nh.GetNodeUser(2)
		if err != nil {
			t.Errorf("failed to get NodeUser")
		}
		if n == nil {
			t.Errorf("got a nil NodeUser")
		}
		n, err = nh.GetNodeUser(123)
		if err != ErrClusterNotFound {
			t.Errorf("didn't return expected err")
		}
		if n != nil {
			t.Errorf("got unexpected node user")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostNodeUserPropose(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		n, err := nh.GetNodeUser(2)
		if err != nil {
			t.Errorf("failed to get NodeUser")
		}
		cs := nh.GetNoOPSession(2)
		rs, err := n.Propose(cs, make([]byte, 16), time.Second)
		if err != nil {
			t.Errorf("failed to make propose %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete proposal")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostNodeUserRead(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		n, err := nh.GetNodeUser(2)
		if err != nil {
			t.Errorf("failed to get NodeUser")
		}
		rs, err := n.ReadIndex(time.Second)
		if err != nil {
			t.Errorf("failed to read index %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete read index")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostAddObserverRemoveNode(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		rs, err := nh.RequestAddObserver(2, 2, "localhost:25000", 0, time.Second)
		if err != nil {
			t.Errorf("failed to add node %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete add node")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		membership, err := nh.SyncGetClusterMembership(ctx, 2)
		if err != nil {
			t.Fatalf("failed to get cluster membership %v", err)
		}
		if len(membership.Nodes) != 1 || len(membership.Removed) != 0 {
			t.Errorf("unexpected nodes/removed len")
		}
		if len(membership.Observers) != 1 {
			t.Errorf("unexpected nodes len")
		}
		_, ok := membership.Observers[2]
		if !ok {
			t.Errorf("node 2 not added")
		}
		// remove it
		rs, err = nh.RequestDeleteNode(2, 2, 0, time.Second)
		if err != nil {
			t.Errorf("failed to remove node %v", err)
		}
		v = <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete remove node")
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		membership, err = nh.SyncGetClusterMembership(ctx, 2)
		if err != nil {
			t.Fatalf("failed to get cluster membership %v", err)
		}
		if len(membership.Nodes) != 1 || len(membership.Removed) != 1 {
			t.Errorf("unexpected nodes/removed len")
		}
		if len(membership.Observers) != 0 {
			t.Errorf("unexpected nodes len")
		}
		_, ok = membership.Removed[2]
		if !ok {
			t.Errorf("node 2 not removed")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostLeadershipTransfer(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.RequestLeaderTransfer(2, 1); err != nil {
			t.Errorf("leader transfer failed %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostHasNodeInfo(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if ok := nh.HasNodeInfo(2, 1); !ok {
			t.Errorf("node info missing")
		}
		if ok := nh.HasNodeInfo(2, 2); ok {
			t.Errorf("unexpected node info")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestPushSnapshotStatusForRemovedClusterReturnTrue(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if !nh.pushSnapshotStatus(123, 123, true) {
			t.Errorf("unexpected push snapshot status result")
		}
		if !nh.pushSnapshotStatus(123, 123, false) {
			t.Errorf("unexpected push snapshot status result")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestOnDiskStateMachineDoesNotSupportClientSession(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost, initialApplied uint64) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("no panic when proposing session on disk SM")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := nh.SyncGetSession(ctx, 1)
		cancel()
		if err != nil {
			t.Fatalf("failed to get new session")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 0)
}

func TestStaleReadOnUninitializedNodeReturnError(t *testing.T) {
	tf := func() {
		nh, fakeDiskSM, err := createFakeDiskTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, 0, true)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		testSM := fakeDiskSM.(*tests.FakeDiskSM)
		defer os.RemoveAll(singleNodeHostTestDir)
		defer func() {
			nh.Stop()
		}()
		n, ok := nh.getClusterNotLocked(1)
		if !ok {
			t.Fatalf("failed to get the node")
		}
		if n.initialized() {
			t.Fatalf("node unexpectedly initialized")
		}
		if _, err := nh.StaleRead(1, nil); err != ErrClusterNotInitialized {
			t.Fatalf("expected to return ErrClusterNotInitialized")
		}
		atomic.StoreUint32(&testSM.SlowOpen, 0)
		for !n.initialized() {
		}
		v, err := nh.StaleRead(1, nil)
		if err != nil {
			t.Fatalf("stale read failed %v", err)
		}
		if len(v.([]byte)) != 8 {
			t.Fatalf("unexpected result %v", v)
		}
	}
	runNodeHostTest(t, tf)
}

func TestOnDiskStateMachineCanTakeDummySnapshot(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost, initialApplied uint64) {
		session := nh.GetNoOPSession(1)
		logdb := nh.logdb
		snapshotted := false
		var ss pb.Snapshot
		for i := uint64(2); i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err != nil {
				continue
			}
			snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("list snapshot failed %v", err)
			}
			if len(snapshots) > 0 {
				snapshotted = true
				ss = snapshots[0]
				if !ss.Dummy {
					t.Fatalf("dummy snapshot is not recorded as dummy")
				}
				break
			}
		}
		if !snapshotted {
			t.Fatalf("failed to snapshot")
		}
		fi, err := os.Stat(ss.Filepath)
		if err != nil {
			t.Fatalf("failed to get file st %v", err)
		}
		if fi.Size() != 1060 {
			t.Fatalf("unexpected dummy snapshot file size %d", fi.Size())
		}
		reader, err := rsm.NewSnapshotReader(ss.Filepath)
		if err != nil {
			t.Fatalf("failed to read snapshot %v", err)
		}
		h, err := reader.GetHeader()
		if err != nil {
			t.Errorf("failed to get header")
		}
		if h.Version != uint64(rsm.CurrentSnapshotVersion) {
			t.Errorf("unexpected snapshot version, got %d, want %d",
				h.Version, rsm.CurrentSnapshotVersion)
		}
		reader.ValidateHeader(h)
		reader.Close()
		shrunk, err := rsm.IsShrinkedSnapshotFile(ss.Filepath)
		if err != nil {
			t.Fatalf("failed to check shrunk %v", err)
		}
		if !shrunk {
			t.Errorf("not a dummy snapshot")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 0)
}

func TestOnDiskSMCanStreamSnapshot(t *testing.T) {
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		logdb := nh1.logdb
		snapshotted := false
		session := nh1.GetNoOPSession(1)
		for i := uint64(2); i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := nh1.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("list snapshot failed %v", err)
			}
			if len(snapshots) >= 3 {
				snapshotted = true
				break
			}
		}
		if !snapshotted {
			t.Fatalf("failed to take 3 snapshots")
		}
		rs, err := nh1.RequestAddNode(1, 2, nodeHostTestAddr2, 0, time.Second)
		if err != nil {
			t.Fatalf("failed to add node %v", err)
		}
		s := <-rs.CompletedC
		if !s.Completed() {
			t.Fatalf("failed to complete the add node request")
		}
		rc := config.Config{
			ClusterID:          1,
			NodeID:             2,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		peers := make(map[uint64]string)
		if err := nh2.StartOnDiskCluster(peers, true, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		snapshotted = false
		logdb = nh2.logdb
		waitForLeaderToBeElected(t, nh2, 1)
		for i := uint64(2); i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := nh2.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			snapshots, err := logdb.ListSnapshots(1, 2, math.MaxUint64)
			if err != nil {
				t.Fatalf("list snapshot failed %v", err)
			}
			if len(snapshots) >= 3 {
				snapshotted = true
				for _, ss := range snapshots {
					if ss.OnDiskIndex == 0 {
						t.Errorf("on disk index not recorded in ss")
					}
					shrunk, err := rsm.IsShrinkedSnapshotFile(ss.Filepath)
					if err != nil {
						t.Errorf("failed to check whether snapshot is shrunk %v", err)
					}
					if !shrunk {
						t.Errorf("failed to shrink snapshot")
					}
				}
				break
			}
		}
		if !snapshotted {
			t.Fatalf("failed to take 3 snapshots")
		}

	}
	twoFakeDiskNodeHostTest(t, tf)
}

func TestConcurrentStateMachineLookup(t *testing.T) {
	clusterID := 1 + taskWorkerCount
	done := uint32(0)
	tf := func(t *testing.T, nh *NodeHost) {
		count := uint32(0)
		stopper := syncutil.NewStopper()
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				session := nh.GetNoOPSession(clusterID)
				_, err := nh.SyncPropose(ctx, session, []byte("test"))
				if err != nil {
					plog.Infof("write failed %v", err)
					t.Fatalf("failed to make proposal %v", err)
				}
				plog.Infof("write completed")
				cancel()
				if atomic.LoadUint32(&count) > 0 {
					return
				}
			}
		})
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				rs, err := nh.ReadIndex(clusterID, 200*time.Millisecond)
				if err != nil {
					continue
				}
				s := <-rs.CompletedC
				if !s.Completed() {
					continue
				}
				st := random.LockGuardedRand.Uint64()%7 + 1
				time.Sleep(time.Duration(st) * time.Millisecond)
				result, err := nh.ReadLocalNode(rs, []byte("test"))
				if err != nil {
					continue
				}
				v := binary.LittleEndian.Uint32(result.([]byte))
				if v%2 == 1 {
					plog.Infof("a concurrent read has been confirmed")
					atomic.AddUint32(&count, 1)
					atomic.StoreUint32(&done, 1)
					return
				}
			}
		})
		stopper.Stop()
		if atomic.LoadUint32(&done) == 0 {
			t.Fatalf("failed to have any concurrent read")
		}
	}
	singleConcurrentNodeHostTest(t, tf, 0, true)
}

func TestConcurrentStateMachineSaveSnapshot(t *testing.T) {
	clusterID := 1 + taskWorkerCount
	tf := func(t *testing.T, nh *NodeHost) {
		nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
		for _, ci := range nhi.ClusterInfoList {
			if ci.ClusterID == clusterID {
				if ci.StateMachineType != sm.ConcurrentStateMachine {
					t.Errorf("unexpected state machine type")
				}
			}
			if ci.IsObserver {
				t.Errorf("unexpected IsObserver value")
			}
		}
		result := make(map[uint64]struct{})
		session := nh.GetNoOPSession(clusterID)
		for i := 0; i < 10000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			v, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			result[v.Value] = struct{}{}
			if len(result) > 1 {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("failed to make proposal when saving snapshots")
	}
	singleConcurrentNodeHostTest(t, tf, 10, true)
}

func TestErrorCanBeReturnedWhenLookingUpConcurrentStateMachine(t *testing.T) {
	clusterID := 1 + taskWorkerCount
	tf := func(t *testing.T, nh *NodeHost) {
		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, err := nh.SyncRead(ctx, clusterID, []byte("test"))
			cancel()
			if err != sm.ErrSnapshotStopped {
				t.Fatalf("error not returned")
			}
		}
	}
	singleConcurrentNodeHostTest(t, tf, 10, true)
}

func TestRegularStateMachineDoesNotAllowConucrrentUpdate(t *testing.T) {
	failed := uint32(0)
	tf := func(t *testing.T, nh *NodeHost) {
		nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
		for _, ci := range nhi.ClusterInfoList {
			if ci.ClusterID == 1 {
				if ci.StateMachineType != sm.RegularStateMachine {
					t.Errorf("unexpected state machine type")
				}
			}
			if ci.IsObserver {
				t.Errorf("unexpected IsObserver value")
			}
		}
		stopper := syncutil.NewStopper()
		stopper.RunWorker(func() {
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				session := nh.GetNoOPSession(1)
				_, err := nh.SyncPropose(ctx, session, []byte("test"))
				if err != nil {
					plog.Infof("failed to make proposal %v\n", err)
				}
				cancel()
				if atomic.LoadUint32(&failed) == 1 {
					return
				}
			}
		})
		stopper.RunWorker(func() {
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				result, err := nh.SyncRead(ctx, 1, []byte("test"))
				cancel()
				if err != nil {
					continue
				}
				v := binary.LittleEndian.Uint32(result.([]byte))
				if v == 1 {
					plog.Infof("got a v == 1 result")
					atomic.StoreUint32(&failed, 1)
					return
				}
			}
		})
		stopper.Stop()
		if atomic.LoadUint32(&failed) == 1 {
			t.Fatalf("unexpected concurrent update observed")
		}
	}
	singleConcurrentNodeHostTest(t, tf, 0, false)
}

func TestRegularStateMachineDoesNotAllowConcurrentSaveSnapshot(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		result := make(map[uint64]struct{})
		session := nh.GetNoOPSession(1)
		for i := 0; i < 50; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			v, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			if err != nil {
				continue
			}
			result[v.Value] = struct{}{}
			if len(result) > 1 {
				t.Fatalf("unexpected concurrent save snapshot observed")
			}
		}
	}
	singleConcurrentNodeHostTest(t, tf, 10, false)
}

func TestTooBigPayloadIsRejectedWhenRateLimited(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		bigPayload := make([]byte, 1024*1024)
		session := nh.GetNoOPSession(1)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := nh.SyncPropose(ctx, session, bigPayload)
		cancel()
		if err != ErrPayloadTooBig {
			t.Errorf("failed to return ErrPayloadTooBig")
		}
	}
	rateLimitedNodeHostTest(t, tf)
}

func TestRateLimitCanBeTriggered(t *testing.T) {
	limited := uint64(0)
	stopper := syncutil.NewStopper()
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(1)
		for i := 0; i < 10; i++ {
			stopper.RunWorker(func() {
				for j := 0; j < 16; j++ {
					if atomic.LoadUint64(&limited) == 1 {
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					_, err := nh.SyncPropose(ctx, session, make([]byte, 1024))
					cancel()
					if err == ErrSystemBusy {
						atomic.StoreUint64(&limited, 1)
						return
					}
				}
			})
		}
		stopper.Stop()
		if atomic.LoadUint64(&limited) != 1 {
			t.Fatalf("failed to observe ErrSystemBusy")
		}

		for i := 0; i < 10000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, make([]byte, 1024))
			cancel()
			if err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}

		t.Fatalf("failed to make proposal again")
	}
	rateLimitedNodeHostTest(t, tf)
}

func TestRateLimitCanUseFollowerFeedback(t *testing.T) {
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		session := nh1.GetNoOPSession(1)
		limited := false
		for i := 0; i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 1024))
			cancel()
			if err == ErrSystemBusy {
				limited = true
				break
			}
		}
		if !limited {
			t.Fatalf("failed to observe rate limited")
		}
		for i := 0; i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 1024))
			cancel()
			if err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("failed to make proposal again")
	}
	rateLimitedTwoNodeHostTest(t, tf)
}

func TestUpdateResultIsReturnedToCaller(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		rand.Read(cmd)
		result, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Errorf("failed to make proposal %v", err)
		}
		if result.Value != uint64(1518) {
			t.Errorf("unexpected result value")
		}
		if !bytes.Equal(result.Data, cmd) {
			t.Errorf("unexpected result data")
		}
	}
	rateLimitedNodeHostTest(t, tf)
}

func TestIsObserverIsReturnedWhenNodeIsObserver(t *testing.T) {
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		rc := config.Config{
			ClusterID:          1,
			NodeID:             2,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			IsObserver:         true,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		rs, err := nh1.RequestAddObserver(1, 2, nodeHostTestAddr2, 0, 2000*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to add observer %v", err)
		}
		<-rs.CompletedC
		if err := nh2.StartOnDiskCluster(nil, true, newSM, rc); err != nil {
			t.Errorf("failed to start observer %v", err)
		}
		for i := 0; i < 10000; i++ {
			nhi := nh2.GetNodeHostInfo(DefaultNodeHostInfoOption)
			for _, ci := range nhi.ClusterInfoList {
				if ci.Pending {
					continue
				}
				if ci.IsObserver && ci.NodeID == 2 {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Errorf("failed to get is observer flag")
	}
	twoFakeDiskNodeHostTest(t, tf)
}

func TestSnapshotIndexWillPanicOnRegularRequestResult(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		cs := nh.GetNoOPSession(2)
		rs, err := nh.Propose(cs, make([]byte, 1), 2*time.Second)
		if err != nil {
			t.Fatalf("propose failed %v", err)
		}
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("no panic")
			}
		}()
		v := <-rs.CompletedC
		plog.Infof("%d", v.SnapshotIndex())
	}
	singleNodeHostTest(t, tf)
}

func TestSyncRequestSnapshot(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		idx, err := nh.SyncRequestSnapshot(ctx, 2, DefaultSnapshotOption)
		cancel()
		if err != nil {
			t.Fatalf("%v", err)
		}
		if idx == 0 {
			t.Errorf("unexpected index %d", idx)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSnapshotCanBeExportedAfterSnapshotting(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		idx, err := nh.SyncRequestSnapshot(ctx, 2, DefaultSnapshotOption)
		cancel()
		if err != nil {
			t.Fatalf("%v", err)
		}
		if idx == 0 {
			t.Errorf("unexpected index %d", idx)
		}
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		if err := os.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer os.RemoveAll(sspath)
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		plog.Infof("going to export snapshot")
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		exportIdx, err := nh.SyncRequestSnapshot(ctx, 2, opt)
		cancel()
		if err != nil {
			t.Fatalf("%v", err)
		}
		if exportIdx != idx {
			t.Errorf("unexpected index %d, want %d", exportIdx, idx)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSnapshotCanBeRequested(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		sr, err := nh.RequestSnapshot(2, SnapshotOption{}, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		var index uint64
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete the requested snapshot")
		}
		index = v.SnapshotIndex()
		plog.Infof("going to request snapshot again")
		sr, err = nh.RequestSnapshot(2, SnapshotOption{}, 3*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot")
		}
		v = <-sr.CompletedC
		if !v.Rejected() {
			t.Errorf("failed to complete the requested snapshot")
		}
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) == 0 {
			t.Fatalf("failed to save snapshots")
		}
		if snapshots[0].Index != index {
			t.Errorf("unexpected index value")
		}
		reader, err := rsm.NewSnapshotReader(snapshots[0].Filepath)
		if err != nil {
			t.Fatalf("failed to new snapshot reader %v", err)
		}
		defer reader.Close()
		header, err := reader.GetHeader()
		if err != nil {
			t.Fatalf("failed to get header %v", err)
		}
		if rsm.SnapshotVersion(header.Version) != rsm.V2SnapshotVersion {
			t.Errorf("unexpected snapshot version")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestRequestSnapshotTimeoutWillBeReported(t *testing.T) {
	tf := func() {
		nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		pst.slowSave = true
		defer os.RemoveAll(singleNodeHostTestDir)
		defer nh.Stop()
		waitForLeaderToBeElected(t, nh, 2)
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err = nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		sr, err := nh.RequestSnapshot(2, SnapshotOption{}, 200*time.Millisecond)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		plog.Infof("going to wait for snapshot request to complete")
		v := <-sr.CompletedC
		if !v.Timeout() {
			t.Errorf("failed to report timeout")
		}
	}
	runNodeHostTest(t, tf)
}

func TestSyncRemoveData(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to remove cluster %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := nh.SyncRemoveData(ctx, 2, 1); err != nil {
			t.Fatalf("sync remove data fail %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestRemoveNodeDataWillFailWhenNodeIsStillRunning(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.RemoveData(2, 1); err != ErrClusterNotStopped {
			t.Fatalf("remove data didn't fail")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestRestartingAnNodeWithRemovedDataWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to remove cluster %v", err)
		}
		for {
			if err := nh.RemoveData(2, 1); err != nil {
				if err == ErrClusterNotStopped {
					time.Sleep(100 * time.Millisecond)
					continue
				} else {
					t.Fatalf("remove data failed %v", err)
				}
			}
			break
		}
		rc := config.Config{
			NodeID:             uint64(1),
			ClusterID:          2,
			ElectionRTT:        5,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10,
			CompactionOverhead: 5,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return &PST{}
		}
		if err := nh.StartCluster(peers, false, newPST, rc); err != ErrNodeRemoved {
			t.Fatalf("start cluster failed %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestRemoveNodeDataRemovesAllNodeData(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		sr, err := nh.RequestSnapshot(2, SnapshotOption{}, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete the requested snapshot")
		}
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to stop cluster %v", err)
		}
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) == 0 {
			t.Fatalf("failed to save snapshots")
		}
		snapshotDir := nh.serverCtx.GetSnapshotDir(nh.deploymentID, 2, 1)
		exist, err := fileutil.Exist(snapshotDir)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Fatalf("snapshot dir %s does not exist", snapshotDir)
		}
		files, err := ioutil.ReadDir(snapshotDir)
		if err != nil {
			t.Fatalf("failed to read dir %v", err)
		}
		sscount := 0
		for _, fi := range files {
			if !fi.IsDir() {
				continue
			}
			if server.SnapshotDirNameRe.Match([]byte(fi.Name())) {
				sscount++
			}
		}
		if sscount == 0 {
			t.Fatalf("no snapshot dir found")
		}
		removed := false
		for i := 0; i < 1000; i++ {
			err := nh.RemoveData(2, 1)
			plog.Infof("err : %v", err)
			if err == ErrClusterNotStopped {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatalf("failed to remove data %v", err)
			}
			if err == nil {
				removed = true
				break
			}
		}
		if !removed {
			t.Fatalf("failed to remove node data")
		}
		marked, err := fileutil.IsDirMarkedAsDeleted(snapshotDir)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !marked {
			t.Fatalf("snapshot dir %s still exist", snapshotDir)
		}
		files, err = ioutil.ReadDir(snapshotDir)
		if err != nil {
			t.Fatalf("failed to read dir %v", err)
		}
		for _, fi := range files {
			if !fi.IsDir() {
				continue
			}
			if server.SnapshotDirNameRe.Match([]byte(fi.Name())) {
				t.Fatalf("failed to delete the snapshot dir %s", fi.Name())
			}
		}
		bs, err := logdb.GetBootstrapInfo(2, 1)
		if err != raftio.ErrNoBootstrapInfo {
			t.Fatalf("failed to delete bootstrap %v", err)
		}
		if bs != nil {
			t.Fatalf("bs not nil")
		}
		ents, sz, err := logdb.IterateEntries(nil, 0, 2, 1, 0,
			math.MaxUint64, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to get entries %v", err)
		}
		if len(ents) != 0 || sz != 0 {
			t.Fatalf("entry returned")
		}
		snapshots, err = logdb.ListSnapshots(2, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) != 0 {
			t.Fatalf("snapshot not deleted")
		}
		_, err = logdb.ReadRaftState(2, 1, 1)
		if err != raftio.ErrNoSavedLog {
			t.Fatalf("raft state not deleted %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestSnapshotCanBeExported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		if err := os.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer os.RemoveAll(sspath)
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		sr, err := nh.RequestSnapshot(2, opt, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		var index uint64
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Fatalf("failed to complete the requested snapshot")
		}
		index = v.SnapshotIndex()
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		// exported snapshot is not managed by the system
		if len(snapshots) != 0 {
			t.Fatalf("snapshot record unexpectedly inserted into the system")
		}
		plog.Infof("snapshot index %d", index)
		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		snapshotFile := fmt.Sprintf("snapshot-%016X.gbsnap", index)
		fp := path.Join(sspath, snapshotDir, snapshotFile)
		exist, err := fileutil.Exist(fp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot file not saved")
		}
		metafp := path.Join(sspath, snapshotDir, "snapshot.metadata")
		exist, err = fileutil.Exist(metafp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot metadata not saved")
		}
		var ss pb.Snapshot
		if err := fileutil.GetFlagFileContent(filepath.Join(sspath, snapshotDir),
			"snapshot.metadata", &ss); err != nil {
			t.Fatalf("failed to get snapshot from its metadata file")
		}
		if ss.OnDiskIndex != 0 {
			t.Errorf("on disk index is not 0")
		}
		if ss.Imported {
			t.Errorf("incorrectly recorded as imported")
		}
		if ss.Type != pb.RegularStateMachine {
			t.Errorf("incorrect type")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestOnDiskStateMachineCanExportSnapshot(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost, initialApplied uint64) {
		session := nh.GetNoOPSession(1)
		proposed := false
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err == nil {
				proposed = true
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if !proposed {
			t.Fatalf("failed to make proposal")
		}
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		if err := os.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer os.RemoveAll(sspath)
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		sr, err := nh.RequestSnapshot(1, opt, 3*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		var index uint64
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Fatalf("failed to complete the requested snapshot")
		}
		index = v.SnapshotIndex()
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) != 0 {
			t.Fatalf("snapshot record unexpectedly inserted into the system")
		}
		plog.Infof("snapshot index %d", index)
		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		snapshotFile := fmt.Sprintf("snapshot-%016X.gbsnap", index)
		fp := path.Join(sspath, snapshotDir, snapshotFile)
		exist, err := fileutil.Exist(fp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot file not saved")
		}
		metafp := path.Join(sspath, snapshotDir, "snapshot.metadata")
		exist, err = fileutil.Exist(metafp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot metadata not saved")
		}
		shrunk, err := rsm.IsShrinkedSnapshotFile(fp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if shrunk {
			t.Errorf("exported snapshot is considered as shrunk")
		}
		var ss pb.Snapshot
		if err := fileutil.GetFlagFileContent(filepath.Join(sspath, snapshotDir),
			"snapshot.metadata", &ss); err != nil {
			t.Fatalf("failed to get snapshot from its metadata file")
		}
		if ss.OnDiskIndex == 0 {
			t.Errorf("on disk index is not recorded")
		}
		if ss.Imported {
			t.Errorf("incorrectly recorded as imported")
		}
		if ss.Type != pb.OnDiskStateMachine {
			t.Errorf("incorrect type")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 0)
}

func testImportedSnapshotIsAlwaysRestored(t *testing.T, newDir bool) {
	tf := func() {
		rc := config.Config{
			ClusterID:          1,
			NodeID:             1,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 10,
			RaftAddress:    nodeHostTestAddr1,
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewSimDiskSM(0)
		}
		if err := nh.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh, 1)
		makeProposals := func(nn *NodeHost) {
			session := nh.GetNoOPSession(1)
			for i := 0; i < 16; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, err := nn.SyncPropose(ctx, session, []byte("test-data"))
				cancel()
				if err != nil {
					t.Errorf("failed to make proposal %v", err)
				}
			}
		}
		makeProposals(nh)
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		if err := os.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer os.RemoveAll(sspath)
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		var index uint64
		for i := 0; i < 1000; i++ {
			if i == 999 {
				t.Fatalf("failed to export snapshot")
			}
			sr, err := nh.RequestSnapshot(1, opt, 3*time.Second)
			if err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			v := <-sr.CompletedC
			if v.Rejected() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if v.Completed() {
				index = v.SnapshotIndex()
				break
			}
		}
		plog.Infof("index of exported snapshot %d", index)
		makeProposals(nh)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		rv, err := nh.SyncRead(ctx, 1, nil)
		cancel()
		if err != nil {
			t.Fatalf("failed to read applied value %v", err)
		}
		applied := rv.(uint64)
		if applied <= index {
			t.Fatalf("invalid applied value %d", applied)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
		if err := nh.SyncRequestAddNode(ctx, 1, 2, "noidea:8080", 0); err != nil {
			t.Fatalf("failed to add node %v", err)
		}
		nh.Stop()
		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		dir := path.Join(sspath, snapshotDir)
		members := make(map[uint64]string)
		members[1] = nhc.RaftAddress
		if newDir {
			nhc.NodeHostDir = filepath.Join(nhc.NodeHostDir, "newdir")
		}
		if err := tools.ImportSnapshot(nhc, dir, members, 1); err != nil {
			t.Fatalf("failed to import snapshot %v", err)
		}
		ok, err := upgrade310.CanUpgradeToV310(nhc)
		if err != nil {
			t.Errorf("failed to check whether upgrade is possible")
		}
		if ok {
			t.Errorf("should not be considered as ok to upgrade")
		}
		func() {
			rnh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create node host %v", err)
			}
			defer rnh.Stop()
			rnewSM := func(uint64, uint64) sm.IOnDiskStateMachine {
				return tests.NewSimDiskSM(applied)
			}
			if err := rnh.StartOnDiskCluster(nil, false, rnewSM, rc); err != nil {
				t.Fatalf("failed to start cluster %v", err)
			}
			waitForLeaderToBeElected(t, rnh, 1)
			ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
			rv, err = rnh.SyncRead(ctx, 1, nil)
			cancel()
			if err != nil {
				t.Fatalf("failed to read applied value %v", err)
			}
			if index != rv.(uint64) {
				t.Fatalf("invalid returned value %d", rv.(uint64))
			}
			plog.Infof("checking proposes")
			makeProposals(rnh)
		}()
		ok, err = upgrade310.CanUpgradeToV310(nhc)
		if err != nil {
			t.Errorf("failed to check whether upgrade is possible")
		}
		if !ok {
			t.Errorf("can not upgrade")
		}
	}
	runNodeHostTest(t, tf)
}

func TestImportedSnapshotIsAlwaysRestored(t *testing.T) {
	testImportedSnapshotIsAlwaysRestored(t, true)
	testImportedSnapshotIsAlwaysRestored(t, false)
}

func TestClusterWithoutQuorumCanBeRestoreByImportingSnapshot(t *testing.T) {
	tf := func() {
		nh1dir := path.Join(singleNodeHostTestDir, "nh1")
		nh2dir := path.Join(singleNodeHostTestDir, "nh2")
		rc := config.Config{
			ClusterID:          1,
			NodeID:             1,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		nhc1 := config.NodeHostConfig{
			WALDir:         nh1dir,
			NodeHostDir:    nh1dir,
			RTTMillisecond: 10,
			RaftAddress:    nodeHostTestAddr1,
		}
		nhc2 := config.NodeHostConfig{
			WALDir:         nh2dir,
			NodeHostDir:    nh2dir,
			RTTMillisecond: 10,
			RaftAddress:    nodeHostTestAddr2,
		}
		plog.Infof("dir1 %s, dir2 %s", nh1dir, nh2dir)
		var once sync.Once
		nh1, err := NewNodeHost(nhc1)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		nh2, err := NewNodeHost(nhc2)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh1, 1)
		defer os.RemoveAll(singleNodeHostTestDir)
		defer once.Do(func() {
			nh1.Stop()
			nh2.Stop()
		})
		session := nh1.GetNoOPSession(1)
		mkproposal := func(nh *NodeHost) {
			done := false
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
				cancel()
				if err == nil {
					done = true
					break
				}
			}
			if !done {
				t.Fatalf("failed to make proposal on restored cluster")
			}
		}
		mkproposal(nh1)
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		if err := os.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer os.RemoveAll(sspath)
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		sr, err := nh1.RequestSnapshot(1, opt, 3*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		var index uint64
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Fatalf("failed to complete the requested snapshot")
		}
		index = v.SnapshotIndex()
		plog.Infof("exported snapshot index %d", index)
		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		dir := path.Join(sspath, snapshotDir)
		members := make(map[uint64]string)
		members[1] = nhc1.RaftAddress
		members[10] = nhc2.RaftAddress
		once.Do(func() {
			nh1.Stop()
			nh2.Stop()
		})
		if err := tools.ImportSnapshot(nhc1, dir, members, 1); err != nil {
			t.Fatalf("failed to import snapshot %v", err)
		}
		if err := tools.ImportSnapshot(nhc2, dir, members, 10); err != nil {
			t.Fatalf("failed to import snapshot %v", err)
		}
		plog.Infof("snapshots imported")
		rnh1, err := NewNodeHost(nhc1)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		rnh2, err := NewNodeHost(nhc2)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		defer func() {
			rnh1.Stop()
			rnh2.Stop()
		}()
		if err := rnh1.StartOnDiskCluster(nil, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		rc.NodeID = 10
		if err := rnh2.StartOnDiskCluster(nil, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, rnh1, 1)
		mkproposal(rnh1)
		mkproposal(rnh2)
	}
	runNodeHostTest(t, tf)
}

type chunks struct {
	received  uint64
	confirmed uint64
}

var (
	testSnapshotDir = "test_snapshot_dir_safe_to_delete"
)

func (c *chunks) onReceive(pb.MessageBatch) {
	c.received++
}

func (c *chunks) confirm(clusterID uint64, nodeID uint64, index uint64) {
	c.confirmed++
}

func (c *chunks) getDeploymentID() uint64 {
	return 0
}

func (c *chunks) getSnapshotDirFunc(clusterID uint64, nodeID uint64) string {
	return testSnapshotDir
}

type testSink2 struct {
	receiver chunkReceiver
}

func (s *testSink2) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	s.receiver.AddChunk(chunk)
	return true, false
}

func (s *testSink2) Stop() {
	s.Receive(pb.SnapshotChunk{ChunkCount: pb.PoisonChunkCount})
}

func (s *testSink2) ClusterID() uint64 {
	return 2000
}

func (s *testSink2) ToNodeID() uint64 {
	return 300
}

type dataCorruptionSink struct {
	receiver chunkReceiver
	enabled  bool
}

func (s *dataCorruptionSink) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	if s.enabled && len(chunk.Data) > 0 {
		idx := rand.Uint64() % uint64(len(chunk.Data))
		chunk.Data[idx] = byte(chunk.Data[idx] + 1)
	}
	s.receiver.AddChunk(chunk)
	return true, false
}

func (s *dataCorruptionSink) Stop() {
	s.Receive(pb.SnapshotChunk{ChunkCount: pb.PoisonChunkCount})
}

func (s *dataCorruptionSink) ClusterID() uint64 {
	return 2000
}

func (s *dataCorruptionSink) ToNodeID() uint64 {
	return 300
}

type chunkReceiver interface {
	AddChunk(chunk pb.SnapshotChunk) bool
}

func getTestSnapshotMeta() *rsm.SnapshotMeta {
	return &rsm.SnapshotMeta{
		Index: 1000,
		Term:  5,
		From:  150,
	}
}

func testCorruptedChunkWriterOutputCanBeHandledByChunks(t *testing.T,
	enabled bool, exp uint64) {
	os.RemoveAll(testSnapshotDir)
	c := &chunks{}
	if err := os.MkdirAll(c.getSnapshotDirFunc(0, 0), 0755); err != nil {
		t.Fatalf("%v", err)
	}
	cks := transport.NewSnapshotChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc)
	sink := &dataCorruptionSink{receiver: cks, enabled: enabled}
	meta := getTestSnapshotMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	defer os.RemoveAll(testSnapshotDir)
	for i := 0; i < 10; i++ {
		data := make([]byte, rsm.ChunkSize)
		rand.Read(data)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("failed to flush %v", err)
	}
	if c.received != exp {
		t.Fatalf("unexpected received count: %d, want %d", c.received, exp)
	}
	if c.confirmed != exp {
		t.Fatalf("unexpected confirmed count: %d, want %d", c.confirmed, exp)
	}
}

func TestCorruptedChunkWriterOutputCanBeHandledByChunks(t *testing.T) {
	testCorruptedChunkWriterOutputCanBeHandledByChunks(t, false, 1)
	testCorruptedChunkWriterOutputCanBeHandledByChunks(t, true, 0)
}

func TestChunkWriterOutputCanBeHandledByChunks(t *testing.T) {
	os.RemoveAll(testSnapshotDir)
	c := &chunks{}
	if err := os.MkdirAll(c.getSnapshotDirFunc(0, 0), 0755); err != nil {
		t.Fatalf("%v", err)
	}
	cks := transport.NewSnapshotChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc)
	sink := &testSink2{receiver: cks}
	meta := getTestSnapshotMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	defer os.RemoveAll(testSnapshotDir)
	payload := make([]byte, 0)
	payload = append(payload, rsm.GetEmptyLRUSession()...)
	for i := 0; i < 10; i++ {
		data := make([]byte, rsm.ChunkSize)
		rand.Read(data)
		payload = append(payload, data...)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("failed to flush %v", err)
	}
	if c.received != 1 {
		t.Fatalf("failed to receive the snapshot")
	}
	if c.confirmed != 1 {
		t.Fatalf("failed to confirm")
	}
	fp := path.Join(testSnapshotDir,
		"snapshot-00000000000003E8", "snapshot-00000000000003E8.gbsnap")
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to get a snapshot reader %v", err)
	}
	if _, err = reader.GetHeader(); err != nil {
		t.Fatalf("failed to get header %v", err)
	}
	got := make([]byte, 0)
	buf := make([]byte, 1024*256)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("snapshot content changed")
	}
}

func TestNodeHostReturnsErrorWhenTransportCanNotBeCreated(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 200,
			RaftAddress:    "microsoft.com:12345",
		}
		nh, err := NewNodeHost(nhc)
		if err == nil {
			nh.Stop()
			t.Fatalf("NewNodeHost didn't fail")
		}
	}
	runNodeHostTest(t, tf)
}

func TestNodeHostChecksLogDBType(t *testing.T) {
	tf := func() {
		f := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return &noopLogDB{}, nil
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 20,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   f,
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
		if err != server.ErrLogDBType {
			t.Fatalf("didn't report logdb type error %v", err)
		}
	}
	runNodeHostTest(t, tf)
}

func TestNodeHostReturnsErrorWhenLogDBCanNotBeCreated(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 200,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   leveldb.NewLogDB,
		}
		func() {
			nh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost %v", err)
			}
			defer nh.Stop()
			nhc.RaftAddress = nodeHostTestAddr2
			_, err = NewNodeHost(nhc)
			if err != server.ErrLockDirectory {
				t.Fatalf("failed to return ErrLockDirectory")
			}
		}()
		_, err := NewNodeHost(nhc)
		if err != server.ErrNotOwner {
			t.Fatalf("failed to return ErrNotOwner")
		}
		nhc.RaftAddress = nodeHostTestAddr1
		nhc.LogDBFactory = leveldb.NewBatchedLogDB
		_, err = NewNodeHost(nhc)
		if err != server.ErrIncompatibleData {
			t.Fatalf("failed to return ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf)
}

func TestBatchedAndPlainEntriesAreNotCompatible(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			WALDir:         singleNodeHostTestDir,
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 100,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   logdb.NewDefaultBatchedLogDB,
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
		nhc.LogDBFactory = logdb.NewDefaultLogDB
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
	runNodeHostTest(t, tf)
}

func TestNodeHostReturnsErrLogDBBrokenChangeWhenLogDBTypeChanges(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 200,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   logdb.NewDefaultBatchedLogDB,
		}
		func() {
			nh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost %v", err)
			}
			defer nh.Stop()
		}()
		nhc.LogDBFactory = logdb.NewDefaultLogDB
		_, err := NewNodeHost(nhc)
		if err != server.ErrLogDBBrokenChange {
			t.Fatalf("failed to return ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf)
}

func TestNodeHostByDefaultUsePlainEntryLogDB(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 20,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   logdb.NewDefaultLogDB,
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
		nhc.LogDBFactory = logdb.NewDefaultBatchedLogDB
		_, err := NewNodeHost(nhc)
		if err != server.ErrIncompatibleData {
			t.Fatalf("failed to return server.ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf)
}

func TestNodeHostByDefaultChecksWhetherToUseBatchedLogDB(t *testing.T) {
	xf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 20,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   logdb.NewDefaultBatchedLogDB,
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
		nhc.LogDBFactory = logdb.NewDefaultLogDB
		tf()
	}
	runNodeHostTest(t, xf)
}

func TestNodeHostWithUnexpectedDeploymentIDWillBeDetected(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 20,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   leveldb.NewLogDB,
			DeploymentID:   100,
		}
		func() {
			nh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost, %v", err)
			}
			defer nh.Stop()
		}()
		nhc.DeploymentID = 200
		_, err := NewNodeHost(nhc)
		if err != server.ErrDeploymentIDChanged {
			t.Errorf("failed to return ErrDeploymentIDChanged, got %v", err)
		}
	}
	runNodeHostTest(t, tf)
}

func TestNodeHostWithLevelDBLogDBCanBeCreated(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 20,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   leveldb.NewLogDB,
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer nh.Stop()
	}
	runNodeHostTest(t, tf)
}

func TestLeaderInfoIsCorrectlyReported(t *testing.T) {
	tf := func(t *testing.T, nh1 *NodeHost) {
		nhi := nh1.GetNodeHostInfo(DefaultNodeHostInfoOption)
		if len(nhi.ClusterInfoList) != 1 {
			t.Errorf("unexpected len: %d", len(nhi.ClusterInfoList))
		}
		if nhi.ClusterInfoList[0].ClusterID != 2 {
			t.Fatalf("unexpected cluster id")
		}
		if !nhi.ClusterInfoList[0].IsLeader {
			t.Errorf("not leader")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := nh1.SyncRequestAddNode(ctx, 2, 2, "noidea:8080", 0); err != nil {
			t.Fatalf("failed to add node %v", err)
		}
		for i := 0; i < 500; i++ {
			nhi := nh1.GetNodeHostInfo(DefaultNodeHostInfoOption)
			if len(nhi.ClusterInfoList) != 1 {
				t.Errorf("unexpected len: %d", len(nhi.ClusterInfoList))
			}
			if nhi.ClusterInfoList[0].IsLeader {
				time.Sleep(20 * time.Millisecond)
			} else {
				return
			}
		}
		t.Fatalf("no leader info change")
	}
	singleNodeHostTest(t, tf)
}

func TestDroppedRequestsAreReported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := nh.SyncRequestAddNode(ctx, 2, 2, "noidea:8080", 0); err != nil {
			t.Fatalf("failed to add node %v", err)
		}
		for i := 0; i < 1000; i++ {
			leaderID, ok, err := nh.GetLeaderID(2)
			if err != nil {
				t.Fatalf("failed to get leader id %v", err)
			}
			if err == nil && !ok && leaderID == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
			if i == 999 {
				t.Fatalf("leader failed to step down")
			}
		}
		func() {
			nctx, ncancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer ncancel()
			cs := nh.GetNoOPSession(2)
			for i := 0; i < 10; i++ {
				if _, err := nh.SyncPropose(nctx, cs, make([]byte, 1)); err != ErrClusterNotReady {
					t.Errorf("failed to get ErrClusterNotReady, got %v", err)
				}
			}
		}()
		func() {
			nctx, ncancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer ncancel()
			for i := 0; i < 10; i++ {
				if err := nh.SyncRequestAddNode(nctx, 2, 3, "noidea:8080", 0); err != ErrClusterNotReady {
					t.Errorf("failed to get ErrClusterNotReady, got %v", err)
				}
			}
		}()
		func() {
			nctx, ncancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer ncancel()
			for i := 0; i < 10; i++ {
				if _, err := nh.SyncRead(nctx, 2, nil); err != ErrClusterNotReady {
					t.Errorf("failed to get ErrClusterNotReady, got %v", err)
				}
			}
		}()
	}
	singleNodeHostTest(t, tf)
}

type testRaftEventListener struct {
	mu       sync.Mutex
	received []raftio.LeaderInfo
}

func (rel *testRaftEventListener) LeaderUpdated(info raftio.LeaderInfo) {
	rel.mu.Lock()
	defer rel.mu.Unlock()
	plog.Infof("leader info: %+v", info)
	rel.received = append(rel.received, info)
}

func (rel *testRaftEventListener) get() []raftio.LeaderInfo {
	rel.mu.Lock()
	defer rel.mu.Unlock()
	r := make([]raftio.LeaderInfo, 0)
	for _, rec := range rel.received {
		r = append(r, rec)
	}
	return r
}

func TestRaftEventsAreReported(t *testing.T) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	rel := &testRaftEventListener{
		received: make([]raftio.LeaderInfo, 0),
	}
	rc := config.Config{
		NodeID:       1,
		ClusterID:    1,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
	peers := make(map[uint64]string)
	peers[1] = nodeHostTestAddr1
	nhc := config.NodeHostConfig{
		NodeHostDir:       singleNodeHostTestDir,
		RTTMillisecond:    10,
		RaftAddress:       peers[1],
		RaftEventListener: rel,
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create node host")
	}
	defer os.RemoveAll(singleNodeHostTestDir)
	defer nh.Stop()
	var pst *PST
	newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		pst = &PST{slowSave: false}
		return pst
	}
	if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
		t.Fatalf("failed to start cluster")
	}
	waitForLeaderToBeElected(t, nh, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	if err := nh.SyncRequestAddNode(ctx, 1, 2, "127.0.0.1:8080", 0); err != nil {
		t.Fatalf("add node failed %v", err)
	}
	cancel()
	var received []raftio.LeaderInfo
	for i := 0; i < 1000; i++ {
		received = rel.get()
		if len(received) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
		if i == 999 {
			t.Fatalf("failed to get the second LeaderUpdated notification")
		}
	}
	exp0 := raftio.LeaderInfo{
		ClusterID: 1,
		NodeID:    1,
		LeaderID:  0,
		Term:      1,
	}
	exp1 := raftio.LeaderInfo{
		ClusterID: 1,
		NodeID:    1,
		LeaderID:  0,
		Term:      2,
	}
	exp2 := raftio.LeaderInfo{
		ClusterID: 1,
		NodeID:    1,
		LeaderID:  1,
		Term:      2,
	}
	exp3 := raftio.LeaderInfo{
		ClusterID: 1,
		NodeID:    1,
		LeaderID:  raftio.NoLeader,
		Term:      2,
	}
	expected := []raftio.LeaderInfo{exp0, exp1, exp2, exp3}
	for idx := range expected {
		if !reflect.DeepEqual(&(received[idx]), &expected[idx]) {
			t.Errorf("unexpecded leader info, %d, %v, %v",
				idx, received[idx], expected[idx])
		}
	}
}

func TestV2DataCanBeHandled(t *testing.T) {
	if logdb.DefaultKVStoreTypeName != "rocksdb" {
		t.Skip("skipping test as the logdb type is not compatible")
	}
	v2datafp := "internal/logdb/testdata/v2-rocksdb-batched.tar.bz2"
	targetDir := "test-v2-data-safe-to-remove"
	os.RemoveAll(targetDir)
	defer os.RemoveAll(targetDir)
	topDirName := "single_nodehost_test_dir_safe_to_delete"
	testHostname := "lindfield.local"
	if err := fileutil.ExtractTarBz2(v2datafp, targetDir); err != nil {
		t.Fatalf("%v", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("failed to get hostname %v", err)
	}
	testPath := filepath.Join(targetDir, topDirName, testHostname)
	expPath := filepath.Join(targetDir, topDirName, hostname)
	if expPath != testPath {
		if err := os.Rename(testPath, expPath); err != nil {
			t.Fatalf("failed to rename the dir %v", err)
		}
	}
	osv := delaySampleRatio
	defer func() {
		delaySampleRatio = osv
	}()
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	delaySampleRatio = 1
	defer leaktest.AfterTest(t)()
	v2dataDir := filepath.Join(targetDir, topDirName)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		v2dataDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	defer nh.Stop()
	logdb := nh.logdb
	rs, err := logdb.ReadRaftState(2, 1, 0)
	if err != nil {
		t.Fatalf("failed to get raft state %v", err)
	}
	if rs.EntryCount != 3 || rs.State.Commit != 3 {
		t.Errorf("unexpected rs value")
	}
}
