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

package dragonboat

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/lni/goutils/random"
	"github.com/lni/goutils/syncutil"
	gvfs "github.com/lni/goutils/vfs"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/pebble"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/dragonboat/v3/tools"
	"github.com/lni/dragonboat/v3/tools/upgrade310"
)

func reportLeakedFD(fs vfs.IFS, t *testing.T) {
	gvfs.ReportLeakedFD(fs, t)
}

var ovs = logdb.RDBContextValueSize

func getTestNodeHostConfig(fs vfs.IFS) *config.NodeHostConfig {
	return &config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 2,
		RaftAddress:    "localhost:1111",
		FS:             fs,
	}
}

type testSysEventListener struct {
	mu                    sync.Mutex
	nodeHostShuttingdown  uint64
	nodeUnloaded          []raftio.NodeInfo
	nodeReady             []raftio.NodeInfo
	membershipChanged     []raftio.NodeInfo
	snapshotCreated       []raftio.SnapshotInfo
	snapshotRecovered     []raftio.SnapshotInfo
	snapshotReceived      []raftio.SnapshotInfo
	sendSnapshotStarted   []raftio.SnapshotInfo
	sendSnapshotCompleted []raftio.SnapshotInfo
	snapshotCompacted     []raftio.SnapshotInfo
	logCompacted          []raftio.EntryInfo
	logdbCompacted        []raftio.EntryInfo
	connectionEstablished uint64
}

func copyNodeInfo(info []raftio.NodeInfo) []raftio.NodeInfo {
	return append([]raftio.NodeInfo{}, info...)
}

func (t *testSysEventListener) NodeHostShuttingDown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodeHostShuttingdown++
}

func (t *testSysEventListener) NodeReady(info raftio.NodeInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodeReady = append(t.nodeReady, info)
}

func (t *testSysEventListener) getNodeReady() []raftio.NodeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copyNodeInfo(t.nodeReady)
}

func (t *testSysEventListener) NodeUnloaded(info raftio.NodeInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodeUnloaded = append(t.nodeUnloaded, info)
}

func (t *testSysEventListener) getNodeUnloaded() []raftio.NodeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copyNodeInfo(t.nodeUnloaded)
}

func (t *testSysEventListener) MembershipChanged(info raftio.NodeInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.membershipChanged = append(t.membershipChanged, info)
}

func (t *testSysEventListener) getMembershipChanged() []raftio.NodeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copyNodeInfo(t.membershipChanged)
}

func (t *testSysEventListener) ConnectionEstablished(info raftio.ConnectionInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connectionEstablished++
}

func (t *testSysEventListener) getConnectionEstablished() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connectionEstablished
}

func (t *testSysEventListener) ConnectionFailed(info raftio.ConnectionInfo) {}

func copySnapshotInfo(info []raftio.SnapshotInfo) []raftio.SnapshotInfo {
	return append([]raftio.SnapshotInfo{}, info...)
}

func (t *testSysEventListener) SendSnapshotStarted(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sendSnapshotStarted = append(t.sendSnapshotStarted, info)
}

func (t *testSysEventListener) getSendSnapshotStarted() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.sendSnapshotStarted)
}

func (t *testSysEventListener) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sendSnapshotCompleted = append(t.sendSnapshotCompleted, info)
}

func (t *testSysEventListener) getSendSnapshotCompleted() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.sendSnapshotCompleted)
}

func (t *testSysEventListener) SendSnapshotAborted(info raftio.SnapshotInfo) {}
func (t *testSysEventListener) SnapshotReceived(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshotReceived = append(t.snapshotReceived, info)
}

func (t *testSysEventListener) getSnapshotReceived() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.snapshotReceived)
}

func (t *testSysEventListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshotRecovered = append(t.snapshotRecovered, info)
}

func (t *testSysEventListener) getSnapshotRecovered() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.snapshotRecovered)
}

func (t *testSysEventListener) SnapshotCreated(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshotCreated = append(t.snapshotCreated, info)
}

func (t *testSysEventListener) getSnapshotCreated() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.snapshotCreated)
}

func (t *testSysEventListener) SnapshotCompacted(info raftio.SnapshotInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshotCompacted = append(t.snapshotCompacted, info)
}

func (t *testSysEventListener) getSnapshotCompacted() []raftio.SnapshotInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copySnapshotInfo(t.snapshotCompacted)
}

func copyEntryInfo(info []raftio.EntryInfo) []raftio.EntryInfo {
	return append([]raftio.EntryInfo{}, info...)
}

func (t *testSysEventListener) LogCompacted(info raftio.EntryInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logCompacted = append(t.logCompacted, info)
}

func (t *testSysEventListener) getLogCompacted() []raftio.EntryInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copyEntryInfo(t.logCompacted)
}

func (t *testSysEventListener) LogDBCompacted(info raftio.EntryInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logdbCompacted = append(t.logdbCompacted, info)
}

func (t *testSysEventListener) getLogDBCompacted() []raftio.EntryInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return copyEntryInfo(t.logdbCompacted)
}

type TimeoutStateMachine struct {
	updateDelay   uint64
	lookupDelay   uint64
	snapshotDelay uint64
}

func (t *TimeoutStateMachine) Update(date []byte) (sm.Result, error) {
	if t.updateDelay > 0 {
		time.Sleep(time.Duration(t.updateDelay) * time.Millisecond)
	}
	return sm.Result{}, nil
}

func (t *TimeoutStateMachine) Lookup(data interface{}) (interface{}, error) {
	if t.lookupDelay > 0 {
		plog.Infof("---------> Lookup called!")
		time.Sleep(time.Duration(t.lookupDelay) * time.Millisecond)
	}
	return data, nil
}

func (t *TimeoutStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	if t.snapshotDelay > 0 {
		time.Sleep(time.Duration(t.snapshotDelay) * time.Millisecond)
	}
	_, err := w.Write([]byte("done"))
	return err
}

func (t *TimeoutStateMachine) RecoverFromSnapshot(r io.Reader,
	fc []sm.SnapshotFile, stopc <-chan struct{}) error {
	return nil
}

func (t *TimeoutStateMachine) Close() error {
	return nil
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
func (n *noopLogDB) CompactEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) (<-chan struct{}, error) {
	return nil, nil
}
func (n *noopLogDB) RemoveNodeData(clusterID uint64, nodeID uint64) error               { return nil }
func (n *noopLogDB) SaveSnapshots([]pb.Update) error                                    { return nil }
func (n *noopLogDB) DeleteSnapshot(clusterID uint64, nodeID uint64, index uint64) error { return nil }
func (n *noopLogDB) ListSnapshots(clusterID uint64, nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	return nil, nil
}
func (n *noopLogDB) ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error {
	return nil
}

func runNodeHostTest(t *testing.T, f func(), fs vfs.IFS) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
		t.Fatalf("%v", err)
	}
	f()
	reportLeakedFD(fs, t)
}

func TestLogDBCanBeExtended(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		c := getTestNodeHostConfig(fs)
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
	runNodeHostTest(t, tf, fs)
}

func TestTCPTransportIsUsedByDefault(t *testing.T) {
	if vfs.GetTestFS() != vfs.DefaultFS {
		t.Skip("memfs test mode, skipped")
	}
	fs := vfs.GetTestFS()
	tf := func() {
		c := getTestNodeHostConfig(fs)
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
	runNodeHostTest(t, tf, fs)
}

func TestRaftRPCCanBeExtended(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		c := getTestNodeHostConfig(fs)
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
	runNodeHostTest(t, tf, fs)
}

func TestNewNodeHostReturnErrorOnInvalidConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		c := getTestNodeHostConfig(fs)
		c.RaftAddress = "12345"
		if err := c.Validate(); err == nil {
			t.Fatalf("config is not considered as invalid")
		}
		_, err := NewNodeHost(*c)
		if err == nil {
			t.Fatalf("NewNodeHost failed to return error")
		}
	}
	runNodeHostTest(t, tf, fs)
}

func TestDeploymentIDCanBeSetUsingNodeHostConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		c := getTestNodeHostConfig(fs)
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
	runNodeHostTest(t, tf, fs)
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

func createSnapshotCompressedTestNodeHost(addr string,
	datadir string, fs vfs.IFS) (*NodeHost, error) {
	rc := config.Config{
		NodeID:                  1,
		ClusterID:               1,
		ElectionRTT:             3,
		HeartbeatRTT:            1,
		CheckQuorum:             true,
		SnapshotEntries:         10,
		CompactionOverhead:      5,
		SnapshotCompressionType: config.Snappy,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 100,
		RaftAddress:    peers[1],
		FS:             fs,
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	newSM := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &tests.VerboseSnapshotSM{}
	}
	if err := nh.StartCluster(peers, false, newSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
}

func createSingleNodeTestNodeHost(addr string,
	datadir string, slowSave bool, compress bool, fs vfs.IFS) (*NodeHost, *PST, error) {
	rc := config.Config{
		NodeID:             uint64(1),
		ClusterID:          2,
		ElectionRTT:        3,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	return createSingleNodeTestNodeHostCfg(addr, datadir, slowSave, rc, compress, fs)
}

func createSingleNodeTestNodeHostCfg(addr string,
	datadir string, slowSave bool, rc config.Config,
	compress bool, fs vfs.IFS) (*NodeHost, *PST, error) {
	if compress {
		rc.EntryCompressionType = config.Snappy
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:              datadir,
		NodeHostDir:         datadir,
		RTTMillisecond:      2,
		RaftAddress:         peers[1],
		FS:                  fs,
		SystemEventListener: &testSysEventListener{},
	}
	if err := nhc.Prepare(); err != nil {
		return nil, nil, err
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
	datadir string, snapshotEntry uint64,
	concurrent bool, fs vfs.IFS) (*NodeHost, error) {
	// config for raft
	rc := config.Config{
		NodeID:             uint64(1),
		ElectionRTT:        3,
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
		FS:             fs,
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
	slowOpen bool, compressed bool, fs vfs.IFS) (*NodeHost, sm.IOnDiskStateMachine, error) {
	rc := config.Config{
		ClusterID:          uint64(1),
		NodeID:             uint64(1),
		ElectionRTT:        3,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    30,
		CompactionOverhead: 30,
	}
	if compressed {
		rc.SnapshotCompressionType = config.Snappy
		rc.EntryCompressionType = config.Snappy
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 1,
		RaftAddress:    peers[1],
		FS:             fs,
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

func snapshotCompressedTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		defer leaktest.AfterTest(t)()
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh, err := createSnapshotCompressedTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer func() {
			nh.Stop()
		}()
		waitForLeaderToBeElected(t, nh, 1)
		tf(t, nh)
	}()
	reportLeakedFD(fs, t)
}

func singleConcurrentNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost),
	snapshotEntry uint64, concurrent bool, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		defer leaktest.AfterTest(t)()
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh, err := createConcurrentTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, snapshotEntry, concurrent, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer func() {
			nh.Stop()
		}()
		waitForLeaderToBeElected(t, nh, 1)
		waitForLeaderToBeElected(t, nh, 1+taskWorkerCount)
		tf(t, nh)
	}()
	reportLeakedFD(fs, t)
}

func singleFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost, initialApplied uint64),
	initialApplied uint64, compressed bool, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		defer leaktest.AfterTest(t)()
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh, _, err := createFakeDiskTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, initialApplied, false, compressed, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 1)
		defer func() {
			nh.Stop()
		}()
		tf(t, nh, initialApplied)
	}()
	reportLeakedFD(fs, t)
}

func twoFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost), fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		defer leaktest.AfterTest(t)()
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh1, nh2, err := createFakeDiskTwoTestNodeHosts(nodeHostTestAddr1,
			nodeHostTestAddr2, nh1dir, nh2dir, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer func() {
			nh1.Stop()
			nh2.Stop()
		}()
		tf(t, nh1, nh2)
	}()
	reportLeakedFD(fs, t)
}

func createFakeDiskTwoTestNodeHosts(addr1 string, addr2 string,
	datadir1 string, datadir2 string, fs vfs.IFS) (*NodeHost, *NodeHost, error) {
	peers := make(map[uint64]string)
	peers[1] = addr1
	nhc1 := config.NodeHostConfig{
		WALDir:              datadir1,
		NodeHostDir:         datadir1,
		RTTMillisecond:      10,
		RaftAddress:         addr1,
		FS:                  fs,
		SystemEventListener: &testSysEventListener{},
	}
	nhc2 := config.NodeHostConfig{
		WALDir:              datadir2,
		NodeHostDir:         datadir2,
		RTTMillisecond:      10,
		RaftAddress:         addr2,
		FS:                  fs,
		SystemEventListener: &testSysEventListener{},
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
	return nh1, nh2, nil
}

func createRateLimitedTestNodeHost(addr string,
	datadir string, fs vfs.IFS) (*NodeHost, error) {
	// config for raft
	rc := config.Config{
		NodeID:          uint64(1),
		ClusterID:       1,
		ElectionRTT:     3,
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
		FS:             fs,
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
	datadir1 string, datadir2 string, fs vfs.IFS) (*NodeHost, *NodeHost, error) {
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
		FS:             fs,
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: 10,
		RaftAddress:    peers[2],
		FS:             fs,
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
		// wait for leader to be elected
		time.Sleep(100 * time.Millisecond)
	}
	return nil, nil, errors.New("failed to get usable nodehosts")
}

func rateLimitedTwoNodeHostTest(t *testing.T,
	tf func(t *testing.T, leaderNh *NodeHost, followerNh *NodeHost), fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
		defer leaktest.AfterTest(t)()
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh1, nh2, err := createRateLimitedTwoTestNodeHosts(nodeHostTestAddr1,
			nodeHostTestAddr2, nh1dir, nh2dir, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost2 %v", err)
		}
		defer func() {
			nh1.Stop()
			nh2.Stop()
		}()
		tf(t, nh1, nh2)
	}()
	reportLeakedFD(fs, t)
}

func rateLimitedNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		logdb.RDBContextValueSize = 1024 * 1024
		defer func() {
			logdb.RDBContextValueSize = ovs
		}()
		defer leaktest.AfterTest(t)()
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh, err := createRateLimitedTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 1)
		defer func() {
			nh.Stop()
		}()
		tf(t, nh)
	}()
	reportLeakedFD(fs, t)
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
		//time.Sleep(100 * time.Millisecond)
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
	fs := vfs.GetTestFS()
	tf := func() {
		datadir := singleNodeHostTestDir
		rc := config.Config{
			NodeID:             uint64(1),
			ClusterID:          2,
			ElectionRTT:        3,
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
			FS:             fs,
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
			t.Fatalf("failed to join the cluster, %v", err)
		}
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to stop the cluster, %v", err)
		}
		for i := 0; i < 1000; i++ {
			err := nh.StartCluster(peers, true, newPST, rc)
			if err == nil {
				return
			}
			if err == ErrClusterAlreadyExist {
				time.Sleep(5 * time.Millisecond)
				continue
			} else {
				t.Fatalf("failed to join the cluster again, %v", err)
			}
		}
	}
	runNodeHostTest(t, tf, fs)
}

func TestCompactionCanBeRequested(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
		if err != nil {
			t.Fatalf("failed to make proposal, %v", err)
		}
		opt := SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionOverhead:         0,
		}
		if _, err := nh.SyncRequestSnapshot(ctx, 2, opt); err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		for i := 0; i < 100; i++ {
			op, err := nh.RequestCompaction(2, 1)
			if err == ErrRejected {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatalf("failed to request compaction %v", err)
			}
			select {
			case <-op.CompletedC():
				break
			case <-ctx.Done():
				t.Fatalf("failed to complete the compaction")
			}
			break
		}
		_, err = nh.RequestCompaction(2, 1)
		if err != ErrRejected {
			t.Fatalf("not rejected")
		}
		listener, ok := nh.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		if len(listener.getLogDBCompacted()) == 0 {
			t.Fatalf("logdb compaction not notified")
		}
	}
	rc := config.Config{
		NodeID:                 uint64(1),
		ClusterID:              2,
		ElectionRTT:            3,
		HeartbeatRTT:           1,
		CheckQuorum:            true,
		SnapshotEntries:        10,
		CompactionOverhead:     5,
		DisableAutoCompactions: true,
	}
	singleNodeHostTestCfg(t, rc, tf, fs)
}

func TestSnapshotCanBeStopped(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, true, false, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 2)
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
		createProposalsToTriggerSnapshot(t, nh, 50, true)
		nh.Stop()
		time.Sleep(100 * time.Millisecond)
		if !pst.saved || !pst.stopped {
			t.Errorf("snapshot not stopped")
		}
		reportLeakedFD(fs, t)
	}
	runNodeHostTest(t, tf, fs)
}

func TestRecoverFromSnapshotCanBeStopped(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false, false, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		waitForLeaderToBeElected(t, nh, 2)
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
			singleNodeHostTestDir, false, false, fs)
		if err != nil {
			t.Fatalf("failed to restart nodehost %v", err)
		}
		wait := 0
		for !pst.getRestored() {
			time.Sleep(10 * time.Millisecond)
			wait++
			if wait > 1000 {
				break
			}
		}
		nh.Stop()
		wait = 0
		for !pst.stopped {
			time.Sleep(10 * time.Millisecond)
			wait++
			if wait > 1000 {
				break
			}
		}
		if !pst.getRestored() {
			t.Errorf("not restored")
		}
		if !pst.stopped {
			t.Errorf("not stopped")
		}
		reportLeakedFD(fs, t)
	}
	runNodeHostTest(t, tf, fs)
}

func TestInvalidAddressIsRejected(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := nh.SyncRequestAddNode(ctx, 2, 100, "a1", 0)
		if err != ErrInvalidAddress {
			t.Errorf("failed to return ErrInvalidAddress, %v", err)
		}
	}
	singleNodeHostTest(t, tf, vfs.GetTestFS())
}

func TestInvalidContextDeadlineIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		plog.Infof("nh is ready")
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		rcs, err := nh.SyncGetSession(rctx, 2)
		rcancel()
		if err != nil {
			t.Fatalf("failed to get regular session")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
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
		err = nh.SyncRequestAddNode(ctx, 2, 100, "a1.com:12345", 0)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
		err = nh.SyncRequestAddObserver(ctx, 2, 100, "a1.com:12345", 0)
		if err != ErrTimeoutTooSmall {
			t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestErrClusterNotFoundCanBeReturned(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestGetClusterMembership(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nh.GetClusterMembership(ctx, 2)
		if err != nil {
			t.Fatalf("failed to get cluster membership")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestRegisterASessionTwiceWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestUnregisterNotRegisterClientSessionWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestSnapshotFilePayloadChecksumIsSaved(t *testing.T) {
	fs := vfs.GetTestFS()
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
		crc, err := rsm.GetV2PayloadChecksum(snapshot.Filepath, fs)
		if err != nil {
			t.Fatalf("failed to get payload checksum")
		}
		if !bytes.Equal(crc, snapshot.Checksum) {
			t.Errorf("checksum changed")
		}
		ss := pb.Snapshot{}
		if err := fileutil.GetFlagFileContent(fs.PathDir(snapshot.Filepath),
			"snapshot.metadata", &ss, fs); err != nil {
			t.Fatalf("failed to get content %v", err)
		}
		if !reflect.DeepEqual(&ss, &snapshot) {
			t.Errorf("snapshot record changed")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func testZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T, dirName string, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false, false, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		if err = nh.serverCtx.CreateSnapshotDir(nh.deploymentID, 2, 1); err != nil {
			t.Fatalf("failed to get snap dir")
		}
		snapDir := nh.serverCtx.GetSnapshotDir(nh.deploymentID, 2, 1)
		z1 := fs.PathJoin(snapDir, dirName)
		plog.Infof("creating %s", z1)
		if err = fs.MkdirAll(z1, 0755); err != nil {
			t.Fatalf("failed to create dir %v", err)
		}
		nh.Stop()
		nh, _, err = createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false, false, fs)
		defer nh.Stop()
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		_, err = fs.Stat(z1)
		if !vfs.IsNotExist(err) {
			t.Fatalf("failed to delete zombie dir")
		}
	}()
	reportLeakedFD(fs, t)
}

func TestZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T) {
	fs := vfs.GetTestFS()
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-01.receiving", fs)
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-10.generating", fs)
}

func runSingleNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), cfg config.Config, compressed bool, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
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
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
		nh, _, err := createSingleNodeTestNodeHostCfg(singleNodeHostTestAddr,
			singleNodeHostTestDir, false, cfg, compressed, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		plog.Infof("going to wait for leader election")
		waitForLeaderToBeElected(t, nh, 2)
		plog.Infof("leader is ready")
		defer nh.Stop()
		tf(t, nh)
	}()
	reportLeakedFD(fs, t)
}

func singleNodeHostTestCfg(t *testing.T,
	cfg config.Config, tf func(t *testing.T, nh *NodeHost), fs vfs.IFS) {
	runSingleNodeHostTest(t, tf, cfg, false, fs)
}

func doSingleNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), compress bool, fs vfs.IFS) {
	rc := config.Config{
		NodeID:             uint64(1),
		ClusterID:          2,
		ElectionRTT:        3,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	runSingleNodeHostTest(t, tf, rc, compress, fs)
}

func singleNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), fs vfs.IFS) {
	doSingleNodeHostTest(t, tf, false, fs)
}

func singleNodeHostCompressionTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), fs vfs.IFS) {
	doSingleNodeHostTest(t, tf, true, fs)
}

func testNodeHostReadIndex(t *testing.T, fs vfs.IFS) {
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostReadIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	testNodeHostReadIndex(t, fs)
}

func TestNALookupCanReturnErrNotImplemented(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostSyncIOAPIs(t *testing.T) {
	fs := vfs.GetTestFS()
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
		listener, ok := nh.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		if len(listener.getNodeReady()) != 1 {
			t.Errorf("node ready not signalled")
		} else {
			ni := listener.getNodeReady()[0]
			if ni.ClusterID != 2 || ni.NodeID != 1 {
				t.Fatalf("incorrect node ready info")
			}
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestEntryCompression(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		cs := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := nh.SyncPropose(ctx, cs, make([]byte, 1024))
		if err != nil {
			t.Errorf("make proposal failed %v", err)
		}
		logdb := nh.logdb
		ents, _, err := logdb.IterateEntries(nil, 0, 2, 1, 1, 100, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to get entries %v", err)
		}
		hasEncodedEntry := false
		for _, e := range ents {
			if e.Type == pb.EncodedEntry {
				hasEncodedEntry = true
				payload := rsm.GetEntryPayload(e)
				plog.Infof("compressed size: %d, original size: %d", len(e.Cmd), len(payload))
				if !bytes.Equal(payload, make([]byte, 1024)) {
					t.Errorf("payload changed")
				}
			}
		}
		if !hasEncodedEntry {
			t.Errorf("failed to locate any encoded entry")
		}
	}
	singleNodeHostCompressionTest(t, tf, fs)
}

func TestSyncRequestDeleteNode(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestDeleteNode(ctx, 2, 2, 0)
		if err != nil {
			t.Errorf("failed to delete node %v", err)
		}
		listener, ok := nh.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		retry := 0
		for retry < 10000 {
			if len(listener.getMembershipChanged()) != 1 {
				time.Sleep(time.Millisecond)
				retry++
			} else {
				break
			}
		}
		ni := listener.getMembershipChanged()[0]
		if ni.ClusterID != 2 || ni.NodeID != 1 {
			t.Fatalf("incorrect node ready info")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestSyncRequestAddNode(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestAddNode(ctx, 2, 2, "localhost:25000", 0)
		if err != nil {
			t.Errorf("failed to add node %v", err)
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestSyncRequestAddObserver(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := nh.SyncRequestAddObserver(ctx, 2, 2, "localhost:25000", 0)
		if err != nil {
			t.Errorf("failed to add observer %v", err)
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostAddNode(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostGetNodeUser(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostNodeUserPropose(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostNodeUserRead(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostAddObserverRemoveNode(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostLeadershipTransfer(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.RequestLeaderTransfer(2, 1); err != nil {
			t.Errorf("leader transfer failed %v", err)
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestNodeHostHasNodeInfo(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		if ok := nh.HasNodeInfo(2, 1); !ok {
			t.Errorf("node info missing")
		}
		if ok := nh.HasNodeInfo(2, 2); ok {
			t.Errorf("unexpected node info")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestPushSnapshotStatusForRemovedClusterReturnTrue(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		if !nh.pushSnapshotStatus(123, 123, true) {
			t.Errorf("unexpected push snapshot status result")
		}
		if !nh.pushSnapshotStatus(123, 123, false) {
			t.Errorf("unexpected push snapshot status result")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestOnDiskStateMachineDoesNotSupportClientSession(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleFakeDiskNodeHostTest(t, tf, 0, false, fs)
}

func TestStaleReadOnUninitializedNodeReturnError(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh, fakeDiskSM, err := createFakeDiskTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, 0, true, false, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		testSM := fakeDiskSM.(*tests.FakeDiskSM)
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
	runNodeHostTest(t, tf, fs)
}

func testOnDiskStateMachineCanTakeDummySnapshot(t *testing.T, compressed bool, fs vfs.IFS) {
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
			} else if i%100 == 0 {
				// this is an ugly hack to workaround RocksDB's incorrect fsync
				// implementation on macos.
				// fcntl(fd, F_FULLFSYNC) is required for a proper fsync on macos,
				// sadly rocksdb is not doing that. this means we can make proposals
				// very fast as they are not actually fsynced on macos but making
				// snapshots are going to be much much slower as dragonboat properly
				// fsyncs its snapshot data. we can end up completing all required
				// proposals even before completing the first ongoing snapshotting
				// operation.
				time.Sleep(200 * time.Millisecond)
			}
		}
		if !snapshotted {
			t.Fatalf("failed to snapshot")
		}
		fi, err := fs.Stat(ss.Filepath)
		if err != nil {
			t.Fatalf("failed to get file st %v", err)
		}
		if fi.Size() != 1060 {
			t.Fatalf("unexpected dummy snapshot file size %d", fi.Size())
		}
		reader, err := rsm.NewSnapshotReader(ss.Filepath, fs)
		if err != nil {
			t.Fatalf("failed to read snapshot %v", err)
		}
		h, err := reader.GetHeader()
		if err != nil {
			t.Errorf("failed to get header")
		}
		// dummy snapshot is always not compressed
		if h.CompressionType != config.NoCompression {
			t.Errorf("dummy snapshot compressed")
		}
		if h.Version != uint64(rsm.SnapshotVersion) {
			t.Errorf("unexpected snapshot version, got %d, want %d",
				h.Version, rsm.SnapshotVersion)
		}
		reader.Close()
		shrunk, err := rsm.IsShrinkedSnapshotFile(ss.Filepath, fs)
		if err != nil {
			t.Fatalf("failed to check shrunk %v", err)
		}
		if !shrunk {
			t.Errorf("not a dummy snapshot")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 0, compressed, fs)
}

func TestOnDiskStateMachineCanTakeDummySnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	testOnDiskStateMachineCanTakeDummySnapshot(t, true, fs)
	testOnDiskStateMachineCanTakeDummySnapshot(t, false, fs)
}

func TestOnDiskSMCanStreamSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		rc := config.Config{
			ClusterID:               1,
			NodeID:                  1,
			ElectionRTT:             3,
			HeartbeatRTT:            1,
			CheckQuorum:             true,
			SnapshotEntries:         5,
			CompactionOverhead:      2,
			SnapshotCompressionType: config.Snappy,
			EntryCompressionType:    config.Snappy,
		}
		sm1 := tests.NewFakeDiskSM(0)
		sm1.SetAborted()
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return sm1
		}
		if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh1, 1)
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
			} else if i%50 == 0 {
				// see comments in testOnDiskStateMachineCanTakeDummySnapshot
				time.Sleep(100 * time.Millisecond)
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
		rc = config.Config{
			ClusterID:          1,
			NodeID:             2,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		sm2 := tests.NewFakeDiskSM(0)
		sm2.SetAborted()
		newSM2 := func(uint64, uint64) sm.IOnDiskStateMachine {
			return sm2
		}
		sm1.ClearAborted()
		if err := nh2.StartOnDiskCluster(nil, true, newSM2, rc); err != nil {
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
				if !sm2.Recovered() {
					t.Fatalf("not recovered")
				}
				if !sm1.Aborted() {
					t.Fatalf("not aborted")
				}
				for _, ss := range snapshots {
					if ss.OnDiskIndex == 0 {
						t.Errorf("on disk index not recorded in ss")
					}
					shrunk, err := rsm.IsShrinkedSnapshotFile(ss.Filepath, fs)
					if err != nil {
						t.Errorf("failed to check whether snapshot is shrunk %v", err)
					}
					if !shrunk {
						t.Errorf("snapshot %d is not shrunk", ss.Index)
					}
				}
				break
			} else if i%50 == 0 {
				// see comments in testOnDiskStateMachineCanTakeDummySnapshot
				time.Sleep(100 * time.Millisecond)
			}
		}
		if !snapshotted {
			t.Fatalf("failed to take 3 snapshots")
		}
		listener, ok := nh2.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		if len(listener.getSnapshotReceived()) == 0 {
			t.Fatalf("snapshot received not notified")
		}
		if len(listener.getSnapshotRecovered()) == 0 {
			t.Fatalf("failed to be notified for recovered snapshot")
		}
		if len(listener.getSnapshotCompacted()) == 0 {
			t.Fatalf("snapshot compaction not notified")
		}
		if len(listener.getLogCompacted()) == 0 {
			t.Fatalf("log compaction not notified")
		}
		listener, ok = nh1.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		if len(listener.getSendSnapshotStarted()) == 0 {
			t.Fatalf("send snapshot started not notified")
		}
		if len(listener.getSendSnapshotCompleted()) == 0 {
			t.Fatalf("send snapshot completed not notified")
		}
		if listener.getConnectionEstablished() == 0 {
			t.Fatalf("connection established not notified")
		}
	}
	twoFakeDiskNodeHostTest(t, tf, fs)
}

func TestConcurrentStateMachineLookup(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleConcurrentNodeHostTest(t, tf, 0, true, fs)
}

func TestConcurrentStateMachineSaveSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleConcurrentNodeHostTest(t, tf, 10, true, fs)
}

func TestErrorCanBeReturnedWhenLookingUpConcurrentStateMachine(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleConcurrentNodeHostTest(t, tf, 10, true, fs)
}

func TestRegularStateMachineDoesNotAllowConucrrentUpdate(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleConcurrentNodeHostTest(t, tf, 0, false, fs)
}

func TestRegularStateMachineDoesNotAllowConcurrentSaveSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleConcurrentNodeHostTest(t, tf, 10, false, fs)
}

func TestTooBigPayloadIsRejectedWhenRateLimited(t *testing.T) {
	fs := vfs.GetTestFS()
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
	rateLimitedNodeHostTest(t, tf, fs)
}

func TestProposalsCanBeMadeWhenRateLimited(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(1)
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, make([]byte, 2500))
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
		}
	}
	rateLimitedNodeHostTest(t, tf, fs)
}

func makeTestProposal(nh *NodeHost, count int) bool {
	session := nh.GetNoOPSession(1)
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := nh.SyncPropose(ctx, session, make([]byte, 1024))
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestRateLimitCanBeTriggered(t *testing.T) {
	fs := vfs.GetTestFS()
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
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
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
		if makeTestProposal(nh, 10000) {
			return
		}
		t.Fatalf("failed to make proposal again")
	}
	rateLimitedNodeHostTest(t, tf, fs)
}

func TestRateLimitCanUseFollowerFeedback(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		session := nh1.GetNoOPSession(1)
		limited := false
		for i := 0; i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 1024))
			cancel()
			if err == ErrClusterNotReady {
				time.Sleep(20 * time.Millisecond)
			} else if err == ErrSystemBusy {
				limited = true
				break
			}
		}
		if !limited {
			t.Fatalf("failed to observe rate limited")
		}
		if makeTestProposal(nh1, 1000) {
			return
		}
		t.Fatalf("failed to make proposal again")
	}
	rateLimitedTwoNodeHostTest(t, tf, fs)
}

func TestUpdateResultIsReturnedToCaller(t *testing.T) {
	fs := vfs.GetTestFS()
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
	rateLimitedNodeHostTest(t, tf, fs)
}

func TestIsObserverIsReturnedWhenNodeIsObserver(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		rc := config.Config{
			ClusterID:               1,
			NodeID:                  1,
			ElectionRTT:             3,
			HeartbeatRTT:            1,
			CheckQuorum:             true,
			SnapshotEntries:         5,
			CompactionOverhead:      2,
			SnapshotCompressionType: config.NoCompression,
		}
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Errorf("failed to start observer %v", err)
		}
		waitForLeaderToBeElected(t, nh1, 1)
		rc = config.Config{
			ClusterID:          1,
			NodeID:             2,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			IsObserver:         true,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		newSM2 := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		rs, err := nh1.RequestAddObserver(1, 2, nodeHostTestAddr2, 0, 2000*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to add observer %v", err)
		}
		<-rs.CompletedC
		if err := nh2.StartOnDiskCluster(nil, true, newSM2, rc); err != nil {
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
	twoFakeDiskNodeHostTest(t, tf, fs)
}

func TestSnapshotIndexWillPanicOnRegularRequestResult(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestSyncRequestSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
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
		listener, ok := nh.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		retry := 0
		for retry < 10000 {
			if len(listener.getSnapshotCreated()) != 1 {
				time.Sleep(time.Millisecond)
				retry++
			} else {
				break
			}
		}
		si := listener.getSnapshotCreated()[0]
		if si.ClusterID != 2 || si.NodeID != 1 {
			t.Fatalf("incorrect created snapshot info")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestSnapshotCanBeExportedAfterSnapshotting(t *testing.T) {
	fs := vfs.GetTestFS()
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
		if err := fs.RemoveAll(sspath); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := fs.RemoveAll(sspath); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
	singleNodeHostTest(t, tf, fs)
}

func TestCanOverrideSnapshotOverhead(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		session := nh.GetNoOPSession(2)
		cmd := make([]byte, 1)
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			if err != nil {
				// see comments in testOnDiskStateMachineCanTakeDummySnapshot
				if err == ErrTimeout {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				t.Fatalf("failed to make proposal %v", err)
			}
		}
		opt := SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionOverhead:         0,
		}
		sr, err := nh.RequestSnapshot(2, opt, 2*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot")
		}
		v := <-sr.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete the requested snapshot")
		}
		if v.SnapshotIndex() < 16 {
			t.Fatalf("unexpected snapshot index %d", v.SnapshotIndex())
		}
		logdb := nh.logdb
		for i := 0; i < 1000; i++ {
			if i == 999 {
				t.Fatalf("failed to compact the entries")
			}
			time.Sleep(10 * time.Millisecond)
			op, err := nh.RequestCompaction(2, 1)
			if err == nil {
				<-op.CompletedC()
			}
			ents, _, err := logdb.IterateEntries(nil, 0, 2, 1, 12, 14, math.MaxUint64)
			if err != nil {
				t.Fatalf("failed to iterate entries, %v", err)
			}
			if len(ents) != 0 {
				continue
			} else {
				return
			}
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestSnapshotCanBeRequested(t *testing.T) {
	fs := vfs.GetTestFS()
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
		reader, err := rsm.NewSnapshotReader(snapshots[0].Filepath, fs)
		if err != nil {
			t.Fatalf("failed to new snapshot reader %v", err)
		}
		defer reader.Close()
		header, err := reader.GetHeader()
		if err != nil {
			t.Fatalf("failed to get header %v", err)
		}
		if rsm.SSVersion(header.Version) != rsm.V2SnapshotVersion {
			t.Errorf("unexpected snapshot version")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestRequestSnapshotTimeoutWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, false, false, fs)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		pst.slowSave = true
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
	runNodeHostTest(t, tf, fs)
}

func TestSyncRemoveData(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.StopCluster(2); err != nil {
			t.Fatalf("failed to remove cluster %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := nh.SyncRemoveData(ctx, 2, 1); err != nil {
			t.Fatalf("sync remove data fail %v", err)
		}
		listener, ok := nh.sysUserListener.userListener.(*testSysEventListener)
		if !ok {
			t.Fatalf("failed to get the system event listener")
		}
		if len(listener.getNodeUnloaded()) != 1 {
			t.Errorf("node ready not signalled")
		} else {
			ni := listener.getNodeUnloaded()[0]
			if ni.ClusterID != 2 || ni.NodeID != 1 {
				t.Fatalf("incorrect node unloaded info")
			}
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestRemoveNodeDataWillFailWhenNodeIsStillRunning(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.RemoveData(2, 1); err != ErrClusterNotStopped {
			t.Fatalf("remove data didn't fail")
		}
	}
	singleNodeHostTest(t, tf, fs)
}

func TestRestartingAnNodeWithRemovedDataWillBeRejected(t *testing.T) {
	fs := vfs.GetTestFS()
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
			ElectionRTT:        3,
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
	singleNodeHostTest(t, tf, fs)
}

func TestRemoveNodeDataRemovesAllNodeData(t *testing.T) {
	fs := vfs.GetTestFS()
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
		exist, err := fileutil.Exist(snapshotDir, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Fatalf("snapshot dir %s does not exist", snapshotDir)
		}
		files, err := fs.List(snapshotDir)
		if err != nil {
			t.Fatalf("failed to read dir %v", err)
		}
		sscount := 0
		for _, fn := range files {
			fi, err := fs.Stat(fs.PathJoin(snapshotDir, fn))
			if err != nil {
				t.Fatalf("failed to get stat for %s", fn)
			}
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
		marked, err := fileutil.IsDirMarkedAsDeleted(snapshotDir, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !marked {
			t.Fatalf("snapshot dir %s still exist", snapshotDir)
		}
		files, err = fs.List(snapshotDir)
		if err != nil {
			t.Fatalf("failed to read dir %v", err)
		}
		for _, fn := range files {
			fi, err := fs.Stat(fs.PathJoin(snapshotDir, fn))
			if err != nil {
				t.Fatalf("failed to get stat for %s", fn)
			}
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
		sysop, err := nh.RequestCompaction(2, 1)
		if err != nil {
			t.Fatalf("failed to request compaction %v", err)
		}
		<-sysop.CompletedC()
	}
	singleNodeHostTest(t, tf, fs)
}

func TestSnapshotCanBeExported(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		sspath := "exported_snapshot_safe_to_delete"
		if err := fs.RemoveAll(sspath); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := fs.RemoveAll(sspath); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
		fp := fs.PathJoin(sspath, snapshotDir, snapshotFile)
		exist, err := fileutil.Exist(fp, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot file not saved")
		}
		metafp := fs.PathJoin(sspath, snapshotDir, "snapshot.metadata")
		exist, err = fileutil.Exist(metafp, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot metadata not saved")
		}
		var ss pb.Snapshot
		if err := fileutil.GetFlagFileContent(fs.PathJoin(sspath, snapshotDir),
			"snapshot.metadata", &ss, fs); err != nil {
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
	singleNodeHostTest(t, tf, fs)
}

func TestOnDiskStateMachineCanExportSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
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
		if err := fs.RemoveAll(sspath); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := fs.RemoveAll(sspath); err != nil {
				t.Fatalf("%v", err)
			}
		}()
		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		aborted := false
		index := uint64(0)
		for {
			sr, err := nh.RequestSnapshot(1, opt, 3*time.Second)
			if err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			v := <-sr.CompletedC
			if v.Aborted() {
				aborted = true
				continue
			}
			if !v.Completed() {
				t.Fatalf("failed to complete the requested snapshot, %s", v.code)
			}
			index = v.SnapshotIndex()
			break
		}
		if !aborted {
			t.Fatalf("never aborted")
		}
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
		fp := fs.PathJoin(sspath, snapshotDir, snapshotFile)
		exist, err := fileutil.Exist(fp, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot file not saved")
		}
		metafp := fs.PathJoin(sspath, snapshotDir, "snapshot.metadata")
		exist, err = fileutil.Exist(metafp, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !exist {
			t.Errorf("snapshot metadata not saved")
		}
		shrunk, err := rsm.IsShrinkedSnapshotFile(fp, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if shrunk {
			t.Errorf("exported snapshot is considered as shrunk")
		}
		var ss pb.Snapshot
		if err := fileutil.GetFlagFileContent(fs.PathJoin(sspath, snapshotDir),
			"snapshot.metadata", &ss, fs); err != nil {
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
	singleFakeDiskNodeHostTest(t, tf, 0, false, fs)
}

func testImportedSnapshotIsAlwaysRestored(t *testing.T,
	newDir bool, ct config.CompressionType, fs vfs.IFS) {
	tf := func() {
		rc := config.Config{
			ClusterID:               1,
			NodeID:                  1,
			ElectionRTT:             3,
			HeartbeatRTT:            1,
			CheckQuorum:             true,
			SnapshotEntries:         5,
			CompactionOverhead:      2,
			SnapshotCompressionType: ct,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
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
		if err := fs.RemoveAll(sspath); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := fs.RemoveAll(sspath); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
		dir := fs.PathJoin(sspath, snapshotDir)
		members := make(map[uint64]string)
		members[1] = nhc.RaftAddress
		if newDir {
			nhc.NodeHostDir = fs.PathJoin(nhc.NodeHostDir, "newdir")
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
	runNodeHostTest(t, tf, fs)
}

func TestImportedSnapshotIsAlwaysRestored(t *testing.T) {
	if vfs.GetTestFS() != vfs.DefaultFS {
		t.Skip("not using the default fs")
	} else {
		fs := vfs.GetTestFS()
		testImportedSnapshotIsAlwaysRestored(t, true, config.NoCompression, fs)
		testImportedSnapshotIsAlwaysRestored(t, false, config.NoCompression, fs)
		testImportedSnapshotIsAlwaysRestored(t, false, config.Snappy, fs)
	}
}

func TestClusterWithoutQuorumCanBeRestoreByImportingSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
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
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
		}
		nhc2 := config.NodeHostConfig{
			WALDir:         nh2dir,
			NodeHostDir:    nh2dir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr2,
			FS:             fs,
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
		sm1 := tests.NewFakeDiskSM(0)
		sm1.SetAborted()
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return sm1
		}
		newSM2 := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh1, 1)
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
				} else {
					time.Sleep(200 * time.Millisecond)
				}
			}
			if !done {
				t.Fatalf("failed to make proposal on restored cluster")
			}
		}
		mkproposal(nh1)
		sspath := "exported_snapshot_safe_to_delete"
		if err := fs.RemoveAll(sspath); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.MkdirAll(sspath, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := fs.RemoveAll(sspath); err != nil {
				t.Fatalf("%v", err)
			}
		}()
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
		dir := fs.PathJoin(sspath, snapshotDir)
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
		if err := rnh2.StartOnDiskCluster(nil, false, newSM2, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, rnh1, 1)
		mkproposal(rnh1)
		mkproposal(rnh2)
	}
	runNodeHostTest(t, tf, fs)
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

func (s *testSink2) Receive(chunk pb.Chunk) (bool, bool) {
	s.receiver.AddChunk(chunk)
	return true, false
}

func (s *testSink2) Stop() {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
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

func (s *dataCorruptionSink) Receive(chunk pb.Chunk) (bool, bool) {
	if s.enabled && len(chunk.Data) > 0 {
		idx := rand.Uint64() % uint64(len(chunk.Data))
		chunk.Data[idx] = byte(chunk.Data[idx] + 1)
	}
	s.receiver.AddChunk(chunk)
	return true, false
}

func (s *dataCorruptionSink) Stop() {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
}

func (s *dataCorruptionSink) ClusterID() uint64 {
	return 2000
}

func (s *dataCorruptionSink) ToNodeID() uint64 {
	return 300
}

type chunkReceiver interface {
	AddChunk(chunk pb.Chunk) bool
}

func getTestSSMeta() *rsm.SSMeta {
	return &rsm.SSMeta{
		Index: 1000,
		Term:  5,
		From:  150,
	}
}

func testCorruptedChunkWriterOutputCanBeHandledByChunks(t *testing.T,
	enabled bool, exp uint64, fs vfs.IFS) {
	if err := fs.RemoveAll(testSnapshotDir); err != nil {
		t.Fatalf("%v", err)
	}
	c := &chunks{}
	if err := fs.MkdirAll(c.getSnapshotDirFunc(0, 0), 0755); err != nil {
		t.Fatalf("%v", err)
	}
	cks := transport.NewChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc, fs)
	sink := &dataCorruptionSink{receiver: cks, enabled: enabled}
	meta := getTestSSMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	defer func() {
		if err := fs.RemoveAll(testSnapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
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
	fs := vfs.GetTestFS()
	testCorruptedChunkWriterOutputCanBeHandledByChunks(t, false, 1, fs)
	testCorruptedChunkWriterOutputCanBeHandledByChunks(t, true, 0, fs)
}

func TestChunkWriterOutputCanBeHandledByChunks(t *testing.T) {
	fs := vfs.GetTestFS()
	if err := fs.RemoveAll(testSnapshotDir); err != nil {
		t.Fatalf("%v", err)
	}
	c := &chunks{}
	if err := fs.MkdirAll(c.getSnapshotDirFunc(0, 0), 0755); err != nil {
		t.Fatalf("%v", err)
	}
	cks := transport.NewChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc, fs)
	sink := &testSink2{receiver: cks}
	meta := getTestSSMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	if _, err := cw.Write(rsm.GetEmptyLRUSession()); err != nil {
		t.Fatalf("write failed %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(testSnapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
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
	fp := fs.PathJoin(testSnapshotDir,
		"snapshot-00000000000003E8", "snapshot-00000000000003E8.gbsnap")
	reader, err := rsm.NewSnapshotReader(fp, fs)
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
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("memfs test mode, skipped")
	}
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    "microsoft.com:12345",
			FS:             fs,
		}
		nh, err := NewNodeHost(nhc)
		if err == nil {
			nh.Stop()
			t.Fatalf("NewNodeHost didn't fail")
		}
	}
	runNodeHostTest(t, tf, fs)
}

func TestNodeHostChecksLogDBType(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		f := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return &noopLogDB{}, nil
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   f,
			FS:             fs,
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
	runNodeHostTest(t, tf, fs)
}

// FIXME:
// disabled for now as the new file lock implementation no longer blocks in
// the same process
/*
func TestNodeHostReturnsErrorWhenLogDBCanNotBeCreated(t *testing.T) {
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 200,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   pebble.NewLogDB,
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
		nhc.LogDBFactory = pebble.NewBatchedLogDB
		_, err = NewNodeHost(nhc)
		if err != server.ErrIncompatibleData {
			t.Fatalf("failed to return ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf)
}
*/

func TestBatchedAndPlainEntriesAreNotCompatible(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		bff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultBatchedLogDB(dirs, lldirs, fs)
		}
		nff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultLogDB(dirs, lldirs, fs)
		}
		nhc := config.NodeHostConfig{
			WALDir:         singleNodeHostTestDir,
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   bff,
			FS:             fs,
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		bf := nh.logdb.BinaryFormat()
		if bf != raftio.LogDBBinVersion {
			t.Errorf("unexpected logdb bin ver %d", bf)
		}
		nh.Stop()
		nhc.LogDBFactory = nff
		func() {
			nh, err := NewNodeHost(nhc)
			plog.Infof("err : %v", err)
			if err != server.ErrLogDBBrokenChange {
				if err == nil && nh != nil {
					nh.Stop()
				}
				t.Fatalf("didn't return the expected error")
			}
		}()
		fp, err := filepath.Abs(singleNodeHostTestDir)
		if err != nil {
			t.Fatalf("failed to get abs %v", err)
		}
		if err := fs.RemoveAll(fp); err != nil {
			t.Fatalf("%v", err)
		}
		nh, err = NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		defer nh.Stop()
		bf = nh.logdb.BinaryFormat()
		if bf != raftio.PlainLogDBBinVersion {
			t.Errorf("unexpected logdb bin ver %d", bf)
		}
	}
	runNodeHostTest(t, tf, fs)
}

func TestNodeHostReturnsErrLogDBBrokenChangeWhenLogDBTypeChanges(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		bff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultBatchedLogDB(dirs, lldirs, fs)
		}
		nff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultLogDB(dirs, lldirs, fs)
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   bff,
			FS:             fs,
		}
		func() {
			nh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost %v", err)
			}
			defer nh.Stop()
		}()
		nhc.LogDBFactory = nff
		_, err := NewNodeHost(nhc)
		if err != server.ErrLogDBBrokenChange {
			t.Fatalf("failed to return ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf, fs)
}

func getLogDBTestFunc(t *testing.T, nhc config.NodeHostConfig) func() {
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
	return tf
}

func TestNodeHostByDefaultUsePlainEntryLogDB(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		bff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultBatchedLogDB(dirs, lldirs, fs)
		}
		nff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultLogDB(dirs, lldirs, fs)
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   nff,
			FS:             fs,
		}
		xf := getLogDBTestFunc(t, nhc)
		xf()
		nhc.LogDBFactory = bff
		_, err := NewNodeHost(nhc)
		if err != server.ErrIncompatibleData {
			t.Fatalf("failed to return server.ErrIncompatibleData")
		}
	}
	runNodeHostTest(t, tf, fs)
}

func TestNodeHostByDefaultChecksWhetherToUseBatchedLogDB(t *testing.T) {
	fs := vfs.GetTestFS()
	xf := func() {
		bff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultBatchedLogDB(dirs, lldirs, fs)
		}
		nff := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewDefaultLogDB(dirs, lldirs, fs)
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   bff,
			FS:             fs,
		}
		tf := getLogDBTestFunc(t, nhc)
		tf()
		nhc.LogDBFactory = nff
		tf()
	}
	runNodeHostTest(t, xf, fs)
}

func TestNodeHostWithUnexpectedDeploymentIDWillBeDetected(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		pf := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewLogDB(dirs, lldirs, false, false, fs, pebble.NewKVStore)
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   pf,
			DeploymentID:   100,
			FS:             fs,
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
	runNodeHostTest(t, tf, fs)
}

func TestNodeHostUsingPebbleCanBeCreated(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		pf := func(dirs []string, lldirs []string) (raftio.ILogDB, error) {
			return logdb.NewLogDB(dirs, lldirs, false, false, fs, pebble.NewKVStore)
		}
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			LogDBFactory:   pf,
			FS:             fs,
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create nodehost %v", err)
		}
		defer nh.Stop()
	}
	runNodeHostTest(t, tf, fs)
}

func TestLeaderInfoIsCorrectlyReported(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
}

func TestDroppedRequestsAreReported(t *testing.T) {
	fs := vfs.GetTestFS()
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
	singleNodeHostTest(t, tf, fs)
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
	return append(r, rel.received...)
}

func TestRaftEventsAreReported(t *testing.T) {
	fs := vfs.GetTestFS()
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	defer leaktest.AfterTest(t)()
	if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
		t.Fatalf("%v", err)
	}
	rel := &testRaftEventListener{
		received: make([]raftio.LeaderInfo, 0),
	}
	rc := config.Config{
		NodeID:       1,
		ClusterID:    1,
		ElectionRTT:  3,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
	peers := make(map[uint64]string)
	peers[1] = nodeHostTestAddr1
	nhc := config.NodeHostConfig{
		NodeHostDir:       singleNodeHostTestDir,
		RTTMillisecond:    2,
		RaftAddress:       peers[1],
		RaftEventListener: rel,
		FS:                fs,
	}
	nh, err := NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create node host")
	}
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
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
		LeaderID:  raftio.NoLeader,
		Term:      1,
	}
	exp1 := raftio.LeaderInfo{
		ClusterID: 1,
		NodeID:    1,
		LeaderID:  raftio.NoLeader,
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
	fs := vfs.GetTestFS()
	if logdb.DefaultKVStoreTypeName != "rocksdb" {
		t.Skip("skipping test as the logdb type is not compatible")
	}
	v2datafp := "internal/logdb/testdata/v2-rocksdb-batched.tar.bz2"
	targetDir := "test-v2-data-safe-to-remove"
	if err := fs.RemoveAll(targetDir); err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		if err := fs.RemoveAll(targetDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	topDirName := "single_nodehost_test_dir_safe_to_delete"
	testHostname := "lindfield.local"
	if err := fileutil.ExtractTarBz2(v2datafp, targetDir, fs); err != nil {
		t.Fatalf("%v", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("failed to get hostname %v", err)
	}
	testPath := fs.PathJoin(targetDir, topDirName, testHostname)
	expPath := fs.PathJoin(targetDir, topDirName, hostname)
	if expPath != testPath {
		if err := fs.Rename(testPath, expPath); err != nil {
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
	v2dataDir := fs.PathJoin(targetDir, topDirName)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		v2dataDir, false, false, fs)
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

func TestSnapshotCanBeCompressed(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
		cancel()
		if err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		logdb := nh.logdb
		ssList, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Fatalf("failed to list snapshots: %v", err)
		}
		if len(ssList) != 1 {
			t.Fatalf("failed to get snapshot rec, %d", len(ssList))
		}
		fi, err := fs.Stat(ssList[0].Filepath)
		if err != nil {
			t.Fatalf("failed to get file path %v", err)
		}
		if fi.Size() > 1024*364 {
			t.Errorf("snapshot file not compressed, sz %d", fi.Size())
		}
	}
	snapshotCompressedTest(t, tf, fs)
}

func makeProposals(nh *NodeHost) {
	session := nh.GetNoOPSession(1)
	for i := 0; i < 16; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
		cancel()
		if err != nil {
			plog.Errorf("failed to make proposal %v", err)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func testWitnessIO(t *testing.T,
	witnessTestFunc func(*NodeHost, *NodeHost, *tests.SimDiskSM), fs vfs.IFS) {
	tf := func() {
		rc := config.Config{
			ClusterID:    1,
			NodeID:       1,
			ElectionRTT:  3,
			HeartbeatRTT: 1,
			CheckQuorum:  true,
		}
		peers := make(map[uint64]string)
		peers[1] = nodeHostTestAddr1
		nhc1 := config.NodeHostConfig{
			NodeHostDir:    fs.PathJoin(singleNodeHostTestDir, "nh1"),
			RTTMillisecond: 2,
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
		}
		nh1, err := NewNodeHost(nhc1)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		defer nh1.Stop()
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewSimDiskSM(0)
		}
		if err := nh1.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh1, 1)
		for i := 0; i < 8; i++ {
			makeProposals(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
			if _, err := nh1.SyncRequestSnapshot(ctx, 1, opt); err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			cancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		if err := nh1.SyncRequestAddWitness(ctx, 1, 2, nodeHostTestAddr2, 0); err != nil {
			t.Fatalf("failed to add witness %v", err)
		}
		cancel()
		rc2 := rc
		rc2.NodeID = 2
		rc2.IsWitness = true
		nhc2 := nhc1
		nhc2.RaftAddress = nodeHostTestAddr2
		nhc2.NodeHostDir = fs.PathJoin(singleNodeHostTestDir, "nh2")
		nh2, err := NewNodeHost(nhc2)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		defer nh2.Stop()
		witness := tests.NewSimDiskSM(0)
		newWitness := func(uint64, uint64) sm.IOnDiskStateMachine {
			return witness
		}
		if err := nh2.StartOnDiskCluster(peers, false, newWitness, rc2); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		waitForLeaderToBeElected(t, nh2, 1)
		witnessTestFunc(nh1, nh2, witness)
	}
	runNodeHostTest(t, tf, fs)
}

func TestWitnessSnapshotIsCorrectlyHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(nh1 *NodeHost, nh2 *NodeHost, witness *tests.SimDiskSM) {
		for {
			if witness.GetRecovered() > 0 {
				t.Fatalf("unexpected recovered count %d", witness.GetRecovered())
			}
			snapshots, err := nh2.logdb.ListSnapshots(1, 2, math.MaxUint64)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if len(snapshots) == 0 {
				time.Sleep(100 * time.Millisecond)
			} else {
				for _, ss := range snapshots {
					if !ss.Witness {
						t.Errorf("not a witness snapshot")
					}
				}
				return
			}
		}
	}
	testWitnessIO(t, tf, fs)
}

func TestWitnessCanReplicateEntries(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(nh1 *NodeHost, nh2 *NodeHost, witness *tests.SimDiskSM) {
		for i := 0; i < 8; i++ {
			makeProposals(nh1)
		}
		if witness.GetApplied() > 0 {
			t.Fatalf("unexpected applied count %d", witness.GetApplied())
		}
	}
	testWitnessIO(t, tf, fs)
}

func TestWitnessCanNotInitiateIORequest(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(nh1 *NodeHost, nh2 *NodeHost, witness *tests.SimDiskSM) {
		opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
		if _, err := nh2.RequestSnapshot(1, opt, time.Second); err != ErrInvalidOperation {
			t.Fatalf("requesting snapshot on witness not rejected")
		}
		session := nh2.GetNoOPSession(1)
		if _, err := nh2.Propose(session, []byte("test-data"), time.Second); err != ErrInvalidOperation {
			t.Fatalf("proposal not rejected on witness")
		}
		session = client.NewSession(1, nh2.serverCtx.GetRandomSource())
		session.PrepareForRegister()
		if _, err := nh2.ProposeSession(session, time.Second); err != ErrInvalidOperation {
			t.Fatalf("propose session not rejected on witness")
		}
		if _, err := nh2.ReadIndex(1, time.Second); err != ErrInvalidOperation {
			t.Fatalf("sync read not rejected on witness")
		}
		if _, err := nh2.RequestAddNode(1, 3, "a3.com:12345", 0, time.Second); err != ErrInvalidOperation {
			t.Fatalf("add node not rejected on witness")
		}
		if _, err := nh2.RequestDeleteNode(1, 3, 0, time.Second); err != ErrInvalidOperation {
			t.Fatalf("delete node not rejected on witness")
		}
		if _, err := nh2.RequestAddObserver(1, 3, "a3.com:12345", 0, time.Second); err != ErrInvalidOperation {
			t.Fatalf("add observer not rejected on witness")
		}
		if _, err := nh2.RequestAddWitness(1, 3, "a3.com:12345", 0, time.Second); err != ErrInvalidOperation {
			t.Fatalf("add witness not rejected on witness")
		}
		if err := nh2.RequestLeaderTransfer(1, 3); err != ErrInvalidOperation {
			t.Fatalf("leader transfer not rejected on witness")
		}
		if _, err := nh2.StaleRead(1, nil); err != ErrInvalidOperation {
			t.Fatalf("stale read not rejected on witness")
		}
	}
	testWitnessIO(t, tf, fs)
}

func TestTimeoutCanBeReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	nhc := getTestNodeHostConfig(fs)
	rc := config.Config{
		NodeID:       1,
		ClusterID:    1,
		ElectionRTT:  3,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
	peers := make(map[uint64]string)
	peers[1] = nhc.RaftAddress
	nh, err := NewNodeHost(*nhc)
	if err != nil {
		t.Fatalf("failed to create node host %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer nh.Stop()
	newSM := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &TimeoutStateMachine{
			updateDelay:   20,
			lookupDelay:   20,
			snapshotDelay: 20,
		}
	}
	if err := nh.StartCluster(peers, false, newSM, rc); err != nil {
		t.Fatalf("failed to start cluster %v", err)
	}
	waitForLeaderToBeElected(t, nh, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
	session := nh.GetNoOPSession(1)
	_, err = nh.SyncPropose(ctx, session, []byte("test"))
	cancel()
	if err != ErrTimeout {
		t.Errorf("failed to return ErrTimeout, %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Millisecond)
	_, err = nh.SyncRead(ctx, 1, []byte("test"))
	cancel()
	if err != ErrTimeout {
		t.Errorf("failed to return ErrTimeout, %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Millisecond)
	_, err = nh.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	cancel()
	if err != ErrTimeout {
		t.Errorf("failed to return ErrTimeout, %v", err)
	}
}
