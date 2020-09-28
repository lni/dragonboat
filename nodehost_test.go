// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
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
	"github.com/lni/dragonboat/v3/internal/invariants"
	"github.com/lni/dragonboat/v3/internal/logdb"
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

const defaultTestPort = 26001

func getTestPort() int {
	pv := os.Getenv("DRAGONBOAT_TEST_PORT")
	if len(pv) > 0 {
		port, err := strconv.Atoi(pv)
		if err != nil {
			panic(err)
		}
		return port
	}
	return defaultTestPort
}

var rttMillisecond uint64
var mu sync.Mutex

var rttValues = []uint64{10, 20, 30, 50, 100, 200, 500}

func getRTTMillisecond(fs vfs.IFS, dir string) uint64 {
	mu.Lock()
	defer mu.Unlock()
	if rttMillisecond > 0 {
		return rttMillisecond
	}
	rttMillisecond = calcRTTMillisecond(fs, dir)
	return rttMillisecond
}

func calcRTTMillisecond(fs vfs.IFS, dir string) uint64 {
	testFile := fs.PathJoin(dir, ".dragonboat_test_file_safe_to_delete")
	defer func() {
		_ = fs.RemoveAll(testFile)
	}()
	_ = fs.MkdirAll(dir, 0755)
	f, err := fs.Create(testFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		f.Close()
	}()
	data := make([]byte, 512)
	total := uint64(0)
	repeat := 5
	for i := 0; i < repeat; i++ {
		if _, err := f.Write(data); err != nil {
			panic(err)
		}
		start := time.Now()
		if err := f.Sync(); err != nil {
			panic(err)
		}
		total += uint64(time.Since(start).Milliseconds())
	}
	rtt := total / uint64(repeat)
	for i := range rttValues {
		if rttValues[i] > rtt {
			if i == 0 {
				return rttValues[0]
			}
			return rttValues[i-1]
		}
	}
	return rttValues[len(rttValues)-1]
}

// typical proposal timeout
func pto(nh *NodeHost) time.Duration {
	rtt := nh.NodeHostConfig().RTTMillisecond
	return time.Duration(rtt*15) * time.Millisecond
}

func lpto(nh *NodeHost) time.Duration {
	rtt := nh.NodeHostConfig().RTTMillisecond
	return time.Duration(rtt*100) * time.Millisecond
}
func getTestExpertConfig() config.ExpertConfig {
	cfg := config.GetDefaultExpertConfig()
	cfg.LogDBShards = 4
	return cfg
}

func reportLeakedFD(fs vfs.IFS, t *testing.T) {
	gvfs.ReportLeakedFD(fs, t)
}

func getTestNodeHostConfig(fs vfs.IFS) *config.NodeHostConfig {
	return &config.NodeHostConfig{
		WALDir:              singleNodeHostTestDir,
		NodeHostDir:         singleNodeHostTestDir,
		RTTMillisecond:      getRTTMillisecond(fs, singleNodeHostTestDir),
		RaftAddress:         singleNodeHostTestAddr,
		FS:                  fs,
		Expert:              getTestExpertConfig(),
		SystemEventListener: &testSysEventListener{},
	}
}

func getTestConfig() *config.Config {
	return &config.Config{
		NodeID:       1,
		ClusterID:    1,
		ElectionRTT:  3,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
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

type updateConfig func(*config.Config) *config.Config
type updateNodeHostConfig func(*config.NodeHostConfig) *config.NodeHostConfig
type testFunc func(*NodeHost)
type beforeTest func()
type afterTest func()

type testOption struct {
	updateConfig         updateConfig
	updateNodeHostConfig updateNodeHostConfig
	tf                   testFunc
	rf                   testFunc
	bt                   beforeTest
	at                   afterTest
	defaultTestNode      bool
	fakeDiskNode         bool
	fakeDiskInitialIndex uint64
	createSM             sm.CreateStateMachineFunc
	createConcurrentSM   sm.CreateConcurrentStateMachineFunc
	createOnDiskSM       sm.CreateOnDiskStateMachineFunc
	join                 bool
	newNodeHostToFail    bool
	restartNodeHost      bool
	noElection           bool
	compressed           bool
}

func createSingleTestNode(t *testing.T, to *testOption, nh *NodeHost) {
	if to.createSM == nil &&
		to.createConcurrentSM == nil &&
		to.createOnDiskSM == nil && !to.defaultTestNode && !to.fakeDiskNode {
		return
	}
	if to.defaultTestNode {
		to.createSM = func(uint64, uint64) sm.IStateMachine {
			return &PST{}
		}
	}
	if to.fakeDiskNode {
		to.createOnDiskSM = func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(to.fakeDiskInitialIndex)
		}
	}
	cfg := getTestConfig()
	if to.updateConfig != nil {
		cfg = to.updateConfig(cfg)
	}
	if to.compressed {
		cfg.SnapshotCompressionType = config.Snappy
		cfg.EntryCompressionType = config.Snappy
	}
	peers := make(map[uint64]string)
	if !to.join {
		peers[cfg.ClusterID] = nh.RaftAddress()
	}
	if to.createSM != nil {
		if err := nh.StartCluster(peers, to.join, to.createSM, *cfg); err != nil {
			t.Fatalf("start cluster failed: %v", err)
		}
	} else if to.createConcurrentSM != nil {
		if err := nh.StartConcurrentCluster(peers,
			to.join, to.createConcurrentSM, *cfg); err != nil {
			t.Fatalf("start concurrent cluster failed: %v", err)
		}
	} else if to.createOnDiskSM != nil {
		if err := nh.StartOnDiskCluster(peers,
			to.join, to.createOnDiskSM, *cfg); err != nil {
			t.Fatalf("start on disk cluster fail: %v", err)
		}
	} else {
		t.Fatalf("?!?")
	}
}

func runNodeHostTest(t *testing.T, to *testOption, fs vfs.IFS) {
	func() {
		defer leaktest.AfterTest(t)()
		defer func() {
			if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
		_ = fs.RemoveAll(singleNodeHostTestDir)
		if to.bt != nil {
			to.bt()
		}
		nhc := getTestNodeHostConfig(fs)
		if to.updateNodeHostConfig != nil {
			nhc = to.updateNodeHostConfig(nhc)
		}
		nh, err := NewNodeHost(*nhc)
		if err != nil && !to.newNodeHostToFail {
			t.Fatalf("failed to create nodehost: %v", err)
		}
		if err != nil && to.newNodeHostToFail {
			return
		}
		if err == nil && to.newNodeHostToFail {
			t.Fatalf("NewNodeHost didn't fail as expected")
		}
		if !to.restartNodeHost {
			defer func() {
				nh.Stop()
				if to.at != nil {
					to.at()
				}
			}()
		}
		createSingleTestNode(t, to, nh)
		if !to.noElection {
			waitForLeaderToBeElected(t, nh, 1)
		}
		if to.tf != nil {
			to.tf(nh)
		}
		if to.restartNodeHost {
			nh.Stop()
			nh, err = NewNodeHost(*nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost: %v", err)
			}
			defer func() {
				nh.Stop()
				if to.at != nil {
					to.at()
				}
			}()
			createSingleTestNode(t, to, nh)
			if to.rf != nil {
				to.rf(nh)
			}
		}
	}()
	reportLeakedFD(fs, t)
}

func createProposalsToTriggerSnapshot(t *testing.T,
	nh *NodeHost, count uint64, timeoutExpected bool) {
	for i := uint64(0); i < count; i++ {
		pto := lpto(nh)
		ctx, cancel := context.WithTimeout(context.Background(), pto)
		cs, err := nh.SyncGetSession(ctx, 1)
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

func runNodeHostTestDC(t *testing.T, f func(), removeDir bool, fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	if removeDir {
		if err := fs.RemoveAll(singleNodeHostTestDir); err != nil {
			t.Fatalf("%v", err)
		}
	}
	f()
	reportLeakedFD(fs, t)
}

func TestLogDBCanBeExtended(t *testing.T) {
	fs := vfs.GetTestFS()
	ldb := &noopLogDB{}
	to := &testOption{
		updateNodeHostConfig: func(nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.LogDBFactory = func(config.NodeHostConfig,
				config.LogDBCallback, []string, []string) (raftio.ILogDB, error) {
				return ldb, nil
			}
			return nhc
		},

		tf: func(nh *NodeHost) {
			if nh.logdb.Name() != ldb.Name() {
				t.Errorf("logdb type name %s, expect %s", nh.logdb.Name(), ldb.Name())
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestTCPTransportIsUsedByDefault(t *testing.T) {
	if vfs.GetTestFS() != vfs.DefaultFS {
		t.Skip("memfs test mode, skipped")
	}
	fs := vfs.GetTestFS()
	to := &testOption{
		tf: func(nh *NodeHost) {
			tt := nh.transport.(*transport.Transport)
			if tt.GetTrans().Name() != transport.TCPRaftRPCName {
				t.Errorf("transport type name %s, expect %s",
					tt.GetTrans().Name(), transport.TCPRaftRPCName)
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestTransportTypeCanBeSet(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.RaftRPCFactory = transport.NewNOOPTransport
			return nhc
		},
		tf: func(nh *NodeHost) {
			tt := nh.transport.(*transport.Transport)
			if tt.GetTrans().Name() != transport.NOOPRaftName {
				t.Errorf("transport type name %s, expect %s",
					tt.GetTrans().Name(), transport.NOOPRaftName)
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestNewNodeHostReturnErrorOnInvalidConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.RaftAddress = "12345"
			if err := nhc.Validate(); err == nil {
				t.Fatalf("config is not considered as invalid")
			}
			return nhc
		},
		newNodeHostToFail: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestDeploymentIDCanBeSetUsingNodeHostConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.DeploymentID = 1000
			return nhc
		},
		tf: func(nh *NodeHost) {
			nhc := nh.NodeHostConfig()
			if did := nhc.GetDeploymentID(); did != 1000 {
				t.Errorf("unexpected did: %d", did)
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

var (
	singleNodeHostTestAddr = fmt.Sprintf("localhost:%d", getTestPort())
	nodeHostTestAddr1      = fmt.Sprintf("localhost:%d", getTestPort())
	nodeHostTestAddr2      = fmt.Sprintf("localhost:%d", getTestPort()+1)
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
		RTTMillisecond: getRTTMillisecond(fs, datadir),
		RaftAddress:    peers[1],
		FS:             fs,
		Expert:         getTestExpertConfig(),
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
	rc.ClusterID = 1 + applyWorkerCount
	if err := nh.StartConcurrentCluster(peers, false, newConcurrentSM, rc); err != nil {
		return nil, err
	}
	rc.ClusterID = 1
	if err := nh.StartCluster(peers, false, newSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
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
		waitForLeaderToBeElected(t, nh, 1+applyWorkerCount)
		tf(t, nh)
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
		RTTMillisecond:      getRTTMillisecond(fs, datadir1),
		RaftAddress:         addr1,
		FS:                  fs,
		SystemEventListener: &testSysEventListener{},
		Expert:              getTestExpertConfig(),
	}
	nhc2 := config.NodeHostConfig{
		WALDir:              datadir2,
		NodeHostDir:         datadir2,
		RTTMillisecond:      getRTTMillisecond(fs, datadir2),
		RaftAddress:         addr2,
		FS:                  fs,
		SystemEventListener: &testSysEventListener{},
		Expert:              getTestExpertConfig(),
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
		RTTMillisecond: getRTTMillisecond(fs, datadir1),
		RaftAddress:    peers[1],
		FS:             fs,
		Expert:         getTestExpertConfig(),
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: getRTTMillisecond(fs, datadir2),
		RaftAddress:    peers[2],
		FS:             fs,
		Expert:         getTestExpertConfig(),
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
				sm2.MillisecondToSleep = nhc1.RTTMillisecond * 10
			} else {
				leaderNh = nh2
				followerNh = nh1
				sm1.MillisecondToSleep = nhc1.RTTMillisecond * 10
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

func TestJoinedClusterCanBeRestartedOrJoinedAgain(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			cfg := getTestConfig()
			peers := make(map[uint64]string)
			newPST := func(uint64, uint64) sm.IStateMachine { return &PST{} }
			if err := nh.StopCluster(1); err != nil {
				t.Fatalf("failed to stop the cluster: %v", err)
			}
			for i := 0; i < 1000; i++ {
				err := nh.StartCluster(peers, true, newPST, *cfg)
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
		},
		join:       true,
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestCompactionCanBeRequested(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateConfig: func(c *config.Config) *config.Config {
			c.SnapshotEntries = 10
			c.CompactionOverhead = 5
			c.DisableAutoCompactions = true
			return c
		},
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			session := nh.GetNoOPSession(1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
			if err != nil {
				t.Fatalf("failed to make proposal, %v", err)
			}
			opt := SnapshotOption{
				OverrideCompactionOverhead: true,
				CompactionOverhead:         0,
			}
			if _, err := nh.SyncRequestSnapshot(ctx, 1, opt); err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			for i := 0; i < 100; i++ {
				op, err := nh.RequestCompaction(1, 1)
				if err == ErrRejected {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Fatalf("failed to request compaction %v", err)
				}
				select {
				case <-op.ResultC():
					break
				case <-ctx.Done():
					t.Fatalf("failed to complete the compaction")
				}
				break
			}
			if _, err = nh.RequestCompaction(1, 1); err != ErrRejected {
				t.Fatalf("not rejected")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotCanBeStopped(t *testing.T) {
	fs := vfs.GetTestFS()
	pst := &PST{slowSave: true}
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.SnapshotEntries = 10
			return c
		},
		createSM: func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return pst
		},
		tf: func(nh *NodeHost) {
			createProposalsToTriggerSnapshot(t, nh, 50, true)
		},
		at: func() {
			if !pst.saved || !pst.stopped {
				t.Errorf("snapshot not stopped")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRecoverFromSnapshotCanBeStopped(t *testing.T) {
	fs := vfs.GetTestFS()
	pst := &PST{slowSave: false}
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.SnapshotEntries = 10
			return c
		},
		createSM: func(clusterID uint64, nodeID uint64) sm.IStateMachine {
			return pst
		},
		tf: func(nh *NodeHost) {
			createProposalsToTriggerSnapshot(t, nh, 50, false)
		},
		rf: func(nh *NodeHost) {
			wait := 0
			for !pst.getRestored() {
				time.Sleep(10 * time.Millisecond)
				wait++
				if wait > 1000 {
					break
				}
			}
		},
		at: func() {
			wait := 0
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
		},
		restartNodeHost: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestInvalidAddressIsRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddNode(ctx, 1, 100, "a1", 0)
			if err != ErrInvalidAddress {
				t.Errorf("failed to return ErrInvalidAddress, %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestInvalidContextDeadlineIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			plog.Infof("nh is ready")
			pto := lpto(nh)
			rctx, rcancel := context.WithTimeout(context.Background(), pto)
			rcs, err := nh.SyncGetSession(rctx, 1)
			rcancel()
			if err != nil {
				t.Fatalf("failed to get regular session")
			}
			// 8 * time.Millisecond is smaller than the smallest possible RTTMillisecond
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Millisecond)
			defer cancel()
			cs := nh.GetNoOPSession(1)
			_, err = nh.SyncPropose(ctx, cs, make([]byte, 1))
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			_, err = nh.SyncRead(ctx, 1, nil)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			_, err = nh.SyncGetSession(ctx, 1)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			err = nh.SyncCloseSession(ctx, rcs)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			_, err = nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			err = nh.SyncRequestDeleteNode(ctx, 1, 1, 0)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			err = nh.SyncRequestAddNode(ctx, 1, 100, "a1.com:12345", 0)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
			err = nh.SyncRequestAddObserver(ctx, 1, 100, "a1.com:12345", 0)
			if err != ErrTimeoutTooSmall {
				t.Errorf("failed to return ErrTimeoutTooSmall, %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestErrClusterNotFoundCanBeReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			_, _, err := nh.GetLeaderID(1234)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, err = nh.StaleRead(1234, nil)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, err = nh.RequestSnapshot(1234, DefaultSnapshotOption, pto)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, err = nh.RequestDeleteNode(1234, 10, 0, pto)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, err = nh.RequestAddNode(1234, 10, "a1", 0, pto)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, err = nh.RequestAddObserver(1234, 10, "a1", 0, pto)
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
			_, err = nh.propose(cs, make([]byte, 1), pto)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			_, _, err = nh.readIndex(1234, pto)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
			err = nh.stopNode(1234, 1, true)
			if err != ErrClusterNotFound {
				t.Errorf("failed to return ErrClusterNotFound, %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestGetClusterMembership(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			_, err := nh.GetClusterMembership(ctx, 1)
			if err != nil {
				t.Fatalf("failed to get cluster membership")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRegisterASessionTwiceWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			cs, err := nh.SyncGetSession(ctx, 1)
			if err != nil {
				t.Errorf("failed to get client session %v", err)
			}
			cs.PrepareForRegister()
			rs, err := nh.ProposeSession(cs, pto)
			if err != nil {
				t.Errorf("failed to propose client session %v", err)
			}
			r := <-rs.ResultC()
			if !r.Rejected() {
				t.Errorf("failed to reject the cs registeration")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestUnregisterNotRegisterClientSessionWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			cs, err := nh.SyncGetSession(ctx, 1)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotFilePayloadChecksumIsSaved(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateConfig: func(c *config.Config) *config.Config {
			c.SnapshotEntries = 10
			return c
		},
		tf: func(nh *NodeHost) {
			cs := nh.GetNoOPSession(1)
			logdb := nh.logdb
			snapshotted := false
			var snapshot pb.Snapshot
			for i := 0; i < 1000; i++ {
				pto := pto(nh)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nh.SyncPropose(ctx, cs, []byte("test-data"))
				cancel()
				if err != nil {
					continue
				}
				snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func testZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T, dirName string, fs vfs.IFS) {
	var z1 string
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			did := nh.nhConfig.GetDeploymentID()
			if err := nh.serverCtx.CreateSnapshotDir(did, 1, 1); err != nil {
				t.Fatalf("failed to get snap dir")
			}
			snapDir := nh.serverCtx.GetSnapshotDir(did, 1, 1)
			z1 = fs.PathJoin(snapDir, dirName)
			plog.Infof("creating %s", z1)
			if err := fs.MkdirAll(z1, 0755); err != nil {
				t.Fatalf("failed to create dir %v", err)
			}
		},
		rf: func(nh *NodeHost) {
			_, err := fs.Stat(z1)
			if !vfs.IsNotExist(err) {
				t.Fatalf("failed to delete zombie dir")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestZombieSnapshotDirWillBeDeletedDuringAddCluster(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-01.receiving", fs)
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-10.generating", fs)
}

func TestNodeHostReadIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			rs, err := nh.ReadIndex(1, pto)
			if err != nil {
				t.Errorf("failed to read index %v", err)
			}
			if rs.node == nil {
				t.Fatal("rs.node not set")
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete read index")
			}
			_, err = nh.ReadLocalNode(rs, make([]byte, 128))
			if err != nil {
				t.Errorf("read local failed %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNALookupCanReturnErrNotImplemented(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			rs, err := nh.ReadIndex(1, pto)
			if err != nil {
				t.Errorf("failed to read index %v", err)
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete read index")
			}
			_, err = nh.NAReadLocalNode(rs, make([]byte, 128))
			if err != sm.ErrNotImplemented {
				t.Errorf("failed to return sm.ErrNotImplemented, got %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostSyncIOAPIs(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			cs := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			v, err := nh.SyncPropose(ctx, cs, make([]byte, 128))
			if err != nil {
				t.Errorf("make proposal failed %v", err)
			}
			if v.Value != 128 {
				t.Errorf("unexpected result")
			}
			data, err := nh.SyncRead(ctx, 1, make([]byte, 128))
			if err != nil {
				t.Errorf("make linearizable read failed %v", err)
			}
			if data == nil || len(data.([]byte)) == 0 {
				t.Errorf("failed to get result")
			}
			if err := nh.StopCluster(1); err != nil {
				t.Errorf("failed to stop cluster 2 %v", err)
			}
			listener, ok := nh.sysListener.userListener.(*testSysEventListener)
			if !ok {
				t.Fatalf("failed to get the system event listener")
			}
			if len(listener.getNodeReady()) != 1 {
				t.Errorf("node ready not signalled")
			} else {
				ni := listener.getNodeReady()[0]
				if ni.ClusterID != 1 || ni.NodeID != 1 {
					t.Fatalf("incorrect node ready info")
				}
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestEntryCompression(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateConfig: func(c *config.Config) *config.Config {
			c.EntryCompressionType = config.Snappy
			return c
		},
		tf: func(nh *NodeHost) {
			cs := nh.GetNoOPSession(1)
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			_, err := nh.SyncPropose(ctx, cs, make([]byte, 1024))
			if err != nil {
				t.Errorf("make proposal failed %v", err)
			}
			logdb := nh.logdb
			ents, _, err := logdb.IterateEntries(nil, 0, 1, 1, 1, 100, math.MaxUint64)
			if err != nil {
				t.Errorf("failed to get entries %v", err)
			}
			hasEncodedEntry := false
			for _, e := range ents {
				if e.Type == pb.EncodedEntry {
					hasEncodedEntry = true
					payload := rsm.GetPayload(e)
					plog.Infof("compressed size: %d, original size: %d", len(e.Cmd), len(payload))
					if !bytes.Equal(payload, make([]byte, 1024)) {
						t.Errorf("payload changed")
					}
				}
			}
			if !hasEncodedEntry {
				t.Errorf("failed to locate any encoded entry")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestDeleteNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestDeleteNode(ctx, 1, 2, 0)
			if err != nil {
				t.Errorf("failed to delete node %v", err)
			}
			listener, ok := nh.sysListener.userListener.(*testSysEventListener)
			if !ok {
				t.Fatalf("failed to get the system event listener")
			}
			retry := 0
			for retry < 10000 {
				if len(listener.getMembershipChanged()) != 2 {
					time.Sleep(time.Millisecond)
					retry++
				} else {
					break
				}
			}
			ni := listener.getMembershipChanged()[1]
			if ni.ClusterID != 1 || ni.NodeID != 1 {
				t.Fatalf("incorrect node ready info")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestAddNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddNode(ctx, 1, 2, "localhost:25000", 0)
			if err != nil {
				t.Errorf("failed to add node %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestAddObserver(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddObserver(ctx, 1, 2, "localhost:25000", 0)
			if err != nil {
				t.Errorf("failed to add observer %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostAddNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			rs, err := nh.RequestAddNode(1, 2, "localhost:25000", 0, pto)
			if err != nil {
				t.Errorf("failed to add node %v", err)
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete add node")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostGetNodeUser(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			n, err := nh.GetNodeUser(1)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostNodeUserPropose(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			n, err := nh.GetNodeUser(1)
			if err != nil {
				t.Errorf("failed to get NodeUser")
			}
			cs := nh.GetNoOPSession(1)
			rs, err := n.Propose(cs, make([]byte, 16), pto)
			if err != nil {
				t.Errorf("failed to make propose %v", err)
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete proposal")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostNodeUserRead(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			n, err := nh.GetNodeUser(1)
			if err != nil {
				t.Errorf("failed to get NodeUser")
			}
			rs, err := n.ReadIndex(pto)
			if err != nil {
				t.Errorf("failed to read index %v", err)
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete read index")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostAddObserverRemoveNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			rs, err := nh.RequestAddObserver(1, 2, "localhost:25000", 0, pto)
			if err != nil {
				t.Errorf("failed to add node %v", err)
			}
			v := <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete add node")
			}
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			membership, err := nh.SyncGetClusterMembership(ctx, 1)
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
			rs, err = nh.RequestDeleteNode(1, 2, 0, pto)
			if err != nil {
				t.Errorf("failed to remove node %v", err)
			}
			v = <-rs.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete remove node")
			}
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			defer cancel()
			membership, err = nh.SyncGetClusterMembership(ctx, 1)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

// FIXME:
// Leadership transfer is not actually tested
func TestNodeHostLeadershipTransfer(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if err := nh.RequestLeaderTransfer(1, 1); err != nil {
				t.Errorf("leader transfer failed %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostHasNodeInfo(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if ok := nh.HasNodeInfo(1, 1); !ok {
				t.Errorf("node info missing")
			}
			if ok := nh.HasNodeInfo(1, 2); ok {
				t.Errorf("unexpected node info")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestPushSnapshotStatusForRemovedClusterReturnTrue(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if !nh.pushSnapshotStatus(123, 123, true) {
				t.Errorf("unexpected push snapshot status result")
			}
			if !nh.pushSnapshotStatus(123, 123, false) {
				t.Errorf("unexpected push snapshot status result")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestOnDiskStateMachineDoesNotSupportClientSession(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		fakeDiskNode: true,
		tf: func(nh *NodeHost) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("no panic when proposing session on disk SM")
				}
			}()
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh.SyncGetSession(ctx, 1)
			cancel()
			if err == nil {
				t.Fatalf("managed to get new session")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestStaleReadOnUninitializedNodeReturnError(t *testing.T) {
	fs := vfs.GetTestFS()
	fakeDiskSM := tests.NewFakeDiskSM(0)
	atomic.StoreUint32(&fakeDiskSM.SlowOpen, 1)
	to := &testOption{
		createOnDiskSM: func(uint64, uint64) sm.IOnDiskStateMachine {
			return fakeDiskSM
		},
		tf: func(nh *NodeHost) {
			n, ok := nh.getCluster(1)
			if !ok {
				t.Fatalf("failed to get the node")
			}
			if n.initialized() {
				t.Fatalf("node unexpectedly initialized")
			}
			if _, err := nh.StaleRead(1, nil); err != ErrClusterNotInitialized {
				t.Fatalf("expected to return ErrClusterNotInitialized")
			}
			atomic.StoreUint32(&fakeDiskSM.SlowOpen, 0)
			for !n.initialized() {
				runtime.Gosched()
			}
			v, err := nh.StaleRead(1, nil)
			if err != nil {
				t.Fatalf("stale read failed %v", err)
			}
			if len(v.([]byte)) != 8 {
				t.Fatalf("unexpected result %v", v)
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func testOnDiskStateMachineCanTakeDummySnapshot(t *testing.T, compressed bool, fs vfs.IFS) {
	to := &testOption{
		fakeDiskNode: true,
		compressed:   compressed,
		updateConfig: func(c *config.Config) *config.Config {
			c.SnapshotEntries = 30
			c.CompactionOverhead = 30
			return c
		},
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			logdb := nh.logdb
			snapshotted := false
			var ss pb.Snapshot
			for i := uint64(2); i < 1000; i++ {
				pto := pto(nh)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		},
	}
	runNodeHostTest(t, to, fs)
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
			pto := pto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		pto := pto(nh1)
		rs, err := nh1.RequestAddNode(1, 2, nodeHostTestAddr2, 0, pto)
		if err != nil {
			t.Fatalf("failed to add node %v", err)
		}
		s := <-rs.ResultC()
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
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		listener, ok := nh2.sysListener.userListener.(*testSysEventListener)
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
		listener, ok = nh1.sysListener.userListener.(*testSysEventListener)
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
	clusterID := 1 + applyWorkerCount
	done := uint32(0)
	tf := func(t *testing.T, nh *NodeHost) {
		count := uint32(0)
		stopper := syncutil.NewStopper()
		pto := pto(nh)
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				session := nh.GetNoOPSession(clusterID)
				_, err := nh.SyncPropose(ctx, session, []byte("test"))
				cancel()
				if err == ErrTimeout {
					continue
				}
				if err != nil {
					t.Fatalf("failed to make proposal %v", err)
				}
				if atomic.LoadUint32(&count) > 0 {
					return
				}
			}
		})
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				if i%5 == 0 {
					time.Sleep(time.Millisecond)
				}
				rs, err := nh.ReadIndex(clusterID, pto)
				if err != nil {
					continue
				}
				s := <-rs.ResultC()
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
	clusterID := 1 + applyWorkerCount
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
		pto := pto(nh)
		for i := 0; i < 10000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
	clusterID := 1 + applyWorkerCount
	tf := func(t *testing.T, nh *NodeHost) {
		for i := 0; i < 100; i++ {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		plog.Infof("going to run tests")
		stopper := syncutil.NewStopper()
		pto := pto(nh)
		stopper.RunWorker(func() {
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		pto := pto(nh)
		for i := 0; i < 50; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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

func TestLogDBRateLimit(t *testing.T) {
	if invariants.DragonboatRocksDBTest {
		t.Skip("not supported on rocksdb")
	}
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateConfig: func(c *config.Config) *config.Config {
			c.MaxInMemLogSize = 1024 * 3
			return c
		},
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			logDBConfig := config.GetDefaultLogDBConfig()
			logDBConfig.KVMaxWriteBufferNumber = 2
			logDBConfig.KVWriteBufferSize = 1024 * 32
			c.LogDB = logDBConfig
			return c
		},
		tf: func(nh *NodeHost) {
			for i := 0; i < 1024; i++ {
				pto := pto(nh)
				session := nh.GetNoOPSession(1)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nh.SyncPropose(ctx, session, make([]byte, 512))
				cancel()
				if err == ErrSystemBusy {
					return
				}
			}
			t.Fatalf("failed to return ErrSystemBusy")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestTooBigPayloadIsRejectedWhenRateLimited(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.MaxInMemLogSize = 1024 * 3
			return c
		},
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &tests.NoOP{MillisecondToSleep: 20}
		},
		tf: func(nh *NodeHost) {
			bigPayload := make([]byte, 1024*1024)
			session := nh.GetNoOPSession(1)
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh.SyncPropose(ctx, session, bigPayload)
			cancel()
			if err != ErrPayloadTooBig {
				t.Errorf("failed to return ErrPayloadTooBig")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestProposalsCanBeMadeWhenRateLimited(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.MaxInMemLogSize = 1024 * 3
			return c
		},
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &tests.NoOP{MillisecondToSleep: 20}
		},
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			for i := 0; i < 16; i++ {
				pto := pto(nh)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nh.SyncPropose(ctx, session, make([]byte, 16))
				cancel()
				if err == ErrTimeout {
					continue
				}
				if err != nil {
					t.Fatalf("failed to make proposal %v", err)
				}
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func makeTestProposal(nh *NodeHost, count int) bool {
	session := nh.GetNoOPSession(1)
	for i := 0; i < count; i++ {
		pto := pto(nh)
		ctx, cancel := context.WithTimeout(context.Background(), pto)
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
	limited := uint32(0)
	stopper := syncutil.NewStopper()
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.MaxInMemLogSize = 1024 * 3
			return c
		},
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &tests.NoOP{MillisecondToSleep: 20}
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			session := nh.GetNoOPSession(1)
			for i := 0; i < 10; i++ {
				stopper.RunWorker(func() {
					for j := 0; j < 16; j++ {
						if atomic.LoadUint32(&limited) == 1 {
							return
						}
						ctx, cancel := context.WithTimeout(context.Background(), pto)
						_, err := nh.SyncPropose(ctx, session, make([]byte, 1024))
						cancel()
						if err == ErrSystemBusy {
							atomic.StoreUint32(&limited, 1)
							return
						}
					}
				})
			}
			stopper.Stop()
			if atomic.LoadUint32(&limited) != 1 {
				t.Fatalf("failed to observe ErrSystemBusy")
			}
			if makeTestProposal(nh, 10000) {
				return
			}
			t.Fatalf("failed to make proposal again")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRateLimitCanUseFollowerFeedback(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		session := nh1.GetNoOPSession(1)
		limited := false
		for i := 0; i < 1000; i++ {
			pto := pto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
	to := &testOption{
		updateConfig: func(c *config.Config) *config.Config {
			c.MaxInMemLogSize = 1024 * 3
			return c
		},
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &tests.NoOP{MillisecondToSleep: 20}
		},
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		},
	}
	runNodeHostTest(t, to, fs)
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
		pto := pto(nh1)
		rs, err := nh1.RequestAddObserver(1, 2, nodeHostTestAddr2, 0, pto)
		if err != nil {
			t.Fatalf("failed to add observer %v", err)
		}
		<-rs.ResultC()
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
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			cs := nh.GetNoOPSession(1)
			pto := pto(nh)
			rs, err := nh.Propose(cs, make([]byte, 1), pto)
			if err != nil {
				t.Fatalf("propose failed %v", err)
			}
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("no panic")
				}
			}()
			v := <-rs.ResultC()
			plog.Infof("%d", v.SnapshotIndex())
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			cmd := make([]byte, 1518)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			idx, err := nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			cancel()
			if err != nil {
				t.Fatalf("%v", err)
			}
			if idx == 0 {
				t.Errorf("unexpected index %d", idx)
			}
			listener, ok := nh.sysListener.userListener.(*testSysEventListener)
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
			if si.ClusterID != 1 || si.NodeID != 1 {
				t.Fatalf("incorrect created snapshot info")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotCanBeExportedAfterSnapshotting(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			cmd := make([]byte, 1518)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			idx, err := nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
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
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			exportIdx, err := nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			if err != nil {
				t.Fatalf("%v", err)
			}
			if exportIdx != idx {
				t.Errorf("unexpected index %d, want %d", exportIdx, idx)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestCanOverrideSnapshotOverhead(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			cmd := make([]byte, 1)
			pto := pto(nh)
			for i := 0; i < 16; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
			lpto := lpto(nh)
			sr, err := nh.RequestSnapshot(1, opt, lpto)
			if err != nil {
				t.Fatalf("failed to request snapshot")
			}
			v := <-sr.ResultC()
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
				op, err := nh.RequestCompaction(1, 1)
				if err == nil {
					<-op.ResultC()
				}
				ents, _, err := logdb.IterateEntries(nil, 0, 1, 1, 12, 14, math.MaxUint64)
				if err != nil {
					t.Fatalf("failed to iterate entries, %v", err)
				}
				if len(ents) != 0 {
					continue
				} else {
					return
				}
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotCanBeRequested(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			session := nh.GetNoOPSession(1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			cmd := make([]byte, 1518)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			sr, err := nh.RequestSnapshot(1, SnapshotOption{}, pto)
			if err != nil {
				t.Errorf("failed to request snapshot")
			}
			var index uint64
			v := <-sr.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete the requested snapshot")
			}
			index = v.SnapshotIndex()
			plog.Infof("going to request snapshot again")
			sr, err = nh.RequestSnapshot(1, SnapshotOption{}, pto)
			if err != nil {
				t.Fatalf("failed to request snapshot")
			}
			v = <-sr.ResultC()
			if !v.Rejected() {
				t.Errorf("failed to complete the requested snapshot")
			}
			logdb := nh.logdb
			snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestClientCanBeNotifiedOnCommittedConfigChange(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.NotifyCommit = true
			return c
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			rs, err := nh.RequestAddNode(1, 2, "localhost:3456", 0, pto)
			if err != nil {
				t.Fatalf("failed to request add node")
			}
			if rs.committedC == nil {
				t.Fatalf("committedC not set")
			}
			cn := <-rs.ResultC()
			if !cn.Committed() {
				t.Fatalf("failed to get committed notification")
			}
			cn = <-rs.ResultC()
			if !cn.Completed() {
				t.Fatalf("failed to get completed notification")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestClientCanBeNotifiedOnCommittedProposals(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.NotifyCommit = true
			return c
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			cmd := make([]byte, 128)
			session := nh.GetNoOPSession(1)
			rs, err := nh.Propose(session, cmd, pto)
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			if rs.committedC == nil {
				t.Fatalf("committedC not set")
			}
			cn := <-rs.ResultC()
			if !cn.Committed() {
				t.Fatalf("failed to get committed notification")
			}
			cn = <-rs.ResultC()
			if !cn.Completed() {
				t.Fatalf("failed to get completed notification")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRequestSnapshotTimeoutWillBeReported(t *testing.T) {
	fs := vfs.GetTestFS()
	pst := &PST{slowSave: true}
	to := &testOption{
		createSM: func(uint64, uint64) sm.IStateMachine {
			return pst
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			sr, err := nh.RequestSnapshot(1, SnapshotOption{}, pto)
			if err != nil {
				t.Fatalf("failed to request snapshot")
			}
			v := <-sr.ResultC()
			if !v.Timeout() {
				t.Errorf("failed to report timeout")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRemoveData(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if err := nh.StopCluster(1); err != nil {
				t.Fatalf("failed to remove cluster %v", err)
			}
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			if err := nh.SyncRemoveData(ctx, 1, 1); err != nil {
				t.Fatalf("sync remove data fail %v", err)
			}
			listener, ok := nh.sysListener.userListener.(*testSysEventListener)
			if !ok {
				t.Fatalf("failed to get the system event listener")
			}
			if len(listener.getNodeUnloaded()) != 1 {
				t.Errorf("node ready not signalled")
			} else {
				ni := listener.getNodeUnloaded()[0]
				if ni.ClusterID != 1 || ni.NodeID != 1 {
					t.Fatalf("incorrect node unloaded info")
				}
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRemoveNodeDataWillFailWhenNodeIsStillRunning(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if err := nh.RemoveData(1, 1); err != ErrClusterNotStopped {
				t.Errorf("remove data didn't fail")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRestartingAnNodeWithRemovedDataWillBeRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			if err := nh.StopCluster(1); err != nil {
				t.Fatalf("failed to remove cluster %v", err)
			}
			for {
				if err := nh.RemoveData(1, 1); err != nil {
					if err == ErrClusterNotStopped {
						time.Sleep(100 * time.Millisecond)
						continue
					} else {
						t.Errorf("remove data failed %v", err)
					}
				}
				break
			}
			rc := getTestConfig()
			peers := make(map[uint64]string)
			peers[1] = nh.RaftAddress()
			newPST := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
				return &PST{}
			}
			if err := nh.StartCluster(peers, false, newPST, *rc); err != ErrNodeRemoved {
				t.Errorf("start cluster failed %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRemoveNodeDataRemovesAllNodeData(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			cmd := make([]byte, 1518)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			if err != nil {
				t.Fatalf("failed to make proposal %v", err)
			}
			sr, err := nh.RequestSnapshot(1, SnapshotOption{}, pto)
			if err != nil {
				t.Errorf("failed to request snapshot")
			}
			v := <-sr.ResultC()
			if !v.Completed() {
				t.Errorf("failed to complete the requested snapshot")
			}
			if err := nh.StopCluster(1); err != nil {
				t.Fatalf("failed to stop cluster %v", err)
			}
			logdb := nh.logdb
			snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if len(snapshots) == 0 {
				t.Fatalf("failed to save snapshots")
			}
			snapshotDir := nh.serverCtx.GetSnapshotDir(nh.nhConfig.GetDeploymentID(), 1, 1)
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
				err := nh.RemoveData(1, 1)
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
			bs, err := logdb.GetBootstrapInfo(1, 1)
			if err != raftio.ErrNoBootstrapInfo {
				t.Fatalf("failed to delete bootstrap %v", err)
			}
			if bs != nil {
				t.Fatalf("bs not nil")
			}
			ents, sz, err := logdb.IterateEntries(nil, 0, 1, 1, 0,
				math.MaxUint64, math.MaxUint64)
			if err != nil {
				t.Fatalf("failed to get entries %v", err)
			}
			if len(ents) != 0 || sz != 0 {
				t.Fatalf("entry returned")
			}
			snapshots, err = logdb.ListSnapshots(1, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if len(snapshots) != 0 {
				t.Fatalf("snapshot not deleted")
			}
			_, err = logdb.ReadRaftState(1, 1, 1)
			if err != raftio.ErrNoSavedLog {
				t.Fatalf("raft state not deleted %v", err)
			}
			sysop, err := nh.RequestCompaction(1, 1)
			if err != nil {
				t.Fatalf("failed to request compaction %v", err)
			}
			<-sysop.ResultC()
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotCanBeExported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
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
			session := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
			sr, err := nh.RequestSnapshot(1, opt, pto)
			if err != nil {
				t.Errorf("failed to request snapshot")
			}
			var index uint64
			v := <-sr.ResultC()
			if !v.Completed() {
				t.Fatalf("failed to complete the requested snapshot")
			}
			index = v.SnapshotIndex()
			logdb := nh.logdb
			snapshots, err := logdb.ListSnapshots(1, 1, math.MaxUint64)
			if err != nil {
				t.Fatalf("%v", err)
			}
			// exported snapshot is not managed by the system
			if len(snapshots) != 0 {
				t.Fatalf("snapshot record unexpectedly inserted into the system")
			}
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestOnDiskStateMachineCanExportSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		fakeDiskNode: true,
		tf: func(nh *NodeHost) {
			session := nh.GetNoOPSession(1)
			proposed := false
			for i := 0; i < 16; i++ {
				pto := pto(nh)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
			pto := lpto(nh)
			for {
				sr, err := nh.RequestSnapshot(1, opt, pto)
				if err == ErrRejected {
					continue
				}
				if err != nil {
					t.Fatalf("failed to request snapshot %v", err)
				}
				v := <-sr.ResultC()
				if v.Aborted() {
					aborted = true
					continue
				}
				if v.code == requestRejected {
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
		},
	}
	runNodeHostTest(t, to, fs)
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
			RTTMillisecond: getRTTMillisecond(fs, singleNodeHostTestDir),
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
			LogDB:          config.GetDefaultLogDBConfig(),
			Expert:         getTestExpertConfig(),
		}
		nh, err := NewNodeHost(nhc)
		if err != nil {
			t.Fatalf("failed to create node host %v", err)
		}
		pto := lpto(nh)
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
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
			sr, err := nh.RequestSnapshot(1, opt, pto)
			if err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			v := <-sr.ResultC()
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
		ctx, cancel := context.WithTimeout(context.Background(), pto)
		rv, err := nh.SyncRead(ctx, 1, nil)
		cancel()
		if err != nil {
			t.Fatalf("failed to read applied value %v", err)
		}
		applied := rv.(uint64)
		if applied <= index {
			t.Fatalf("invalid applied value %d", applied)
		}
		ctx, cancel = context.WithTimeout(context.Background(), pto)
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
			ctx, cancel = context.WithTimeout(context.Background(), pto)
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
	runNodeHostTestDC(t, tf, true, fs)
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
			RTTMillisecond: getRTTMillisecond(fs, nh1dir),
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
			Expert:         getTestExpertConfig(),
		}
		nhc2 := config.NodeHostConfig{
			WALDir:         nh2dir,
			NodeHostDir:    nh2dir,
			RTTMillisecond: getRTTMillisecond(fs, nh2dir),
			RaftAddress:    nodeHostTestAddr2,
			FS:             fs,
			Expert:         getTestExpertConfig(),
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
			pto := pto(nh)
			for i := 0; i < 100; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		pto := lpto(nh1)
		sr, err := nh1.RequestSnapshot(1, opt, pto)
		if err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		var index uint64
		v := <-sr.ResultC()
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
	runNodeHostTestDC(t, tf, true, fs)
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
		c.confirm, c.getSnapshotDirFunc, 0, fs)
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
		c.confirm, c.getSnapshotDirFunc, 0, fs)
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
	defer reader.Close()
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
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.RaftAddress = "microsoft.com:12345"
			return c
		},
		newNodeHostToFail: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostChecksLogDBType(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			f := func(config config.NodeHostConfig, cb config.LogDBCallback,
				dirs []string, lldirs []string) (raftio.ILogDB, error) {
				return &noopLogDB{}, nil
			}
			c.LogDBFactory = f
			return c
		},
		at: func() {
			nhc := getTestNodeHostConfig(fs)
			_, err := NewNodeHost(*nhc)
			if err != server.ErrLogDBType {
				t.Fatalf("didn't report logdb type error %v", err)
			}
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

var spawnChild = flag.Bool("spawn-child", false, "spawned child")

func spawn(execName string) ([]byte, error) {
	return exec.Command(execName, "-spawn-child",
		"-test.v", "-test.run=TestNodeHostFileLock$").CombinedOutput()
}

func TestNodeHostFileLock(t *testing.T) {
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("not using the default fs, skipped")
	}
	tf := func() {
		child := *spawnChild
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: getRTTMillisecond(fs, singleNodeHostTestDir),
			RaftAddress:    nodeHostTestAddr1,
			Expert:         getTestExpertConfig(),
		}
		if !child {
			nh, err := NewNodeHost(nhc)
			if err != nil {
				t.Fatalf("failed to create nodehost %v", err)
			}
			defer nh.Stop()
			out, err := spawn(os.Args[0])
			if err == nil {
				t.Fatalf("file lock didn't prevent the second nh to start, %s", out)
			}
			if !bytes.Contains(out, []byte("returned ErrLockDirectory")) {
				t.Fatalf("unexpected output: %s", out)
			}
		} else {
			nhc.RaftAddress = nodeHostTestAddr2
			cnh, err := NewNodeHost(nhc)
			if err == server.ErrLockDirectory {
				t.Fatalf("returned ErrLockDirectory")
			}
			if err == nil {
				defer cnh.Stop()
			}
		}
	}
	runNodeHostTestDC(t, tf, false, fs)
}

func TestNodeHostReturnsErrLogDBBrokenChangeWhenLogDBTypeChanges(t *testing.T) {
	fs := vfs.GetTestFS()
	bff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs, fs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs, fs)
	}
	to := &testOption{
		at: func() {
			nhc := getTestNodeHostConfig(fs)
			nhc.LogDBFactory = nff
			_, err := NewNodeHost(*nhc)
			if err != server.ErrLogDBBrokenChange {
				t.Errorf("failed to return ErrLogDBBrokenChange")
			}
		},
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.LogDBFactory = bff
			return c
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostByDefaultUsePlainEntryLogDB(t *testing.T) {
	fs := vfs.GetTestFS()
	bff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs, fs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs, fs)
	}
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.LogDBFactory = nff
			return c
		},
		noElection: true,
		at: func() {
			nhc := getTestNodeHostConfig(fs)
			nhc.LogDBFactory = bff
			_, err := NewNodeHost(*nhc)
			if err != server.ErrIncompatibleData {
				t.Errorf("failed to return server.ErrIncompatibleData")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostByDefaultChecksWhetherToUseBatchedLogDB(t *testing.T) {
	fs := vfs.GetTestFS()
	bff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs, fs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs, fs)
	}
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.LogDBFactory = bff
			return c
		},
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &PST{}
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			cs := nh.GetNoOPSession(1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh.SyncPropose(ctx, cs, []byte("test-data"))
			cancel()
			if err != nil {
				t.Errorf("failed to make proposal %v", err)
			}
		},
		at: func() {
			nhc := getTestNodeHostConfig(fs)
			nhc.LogDBFactory = nff
			if nh, err := NewNodeHost(*nhc); err != nil {
				t.Errorf("failed to create node host")
			} else {
				nh.Stop()
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostWithUnexpectedDeploymentIDWillBeDetected(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		noElection: true,
		at: func() {
			nhc := getTestNodeHostConfig(fs)
			nhc.DeploymentID = 200
			_, err := NewNodeHost(*nhc)
			if err != server.ErrDeploymentIDChanged {
				t.Errorf("failed to return ErrDeploymentIDChanged, got %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestLeaderInfoIsCorrectlyReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh1 *NodeHost) {
			nhi := nh1.GetNodeHostInfo(DefaultNodeHostInfoOption)
			if len(nhi.ClusterInfoList) != 1 {
				t.Errorf("unexpected len: %d", len(nhi.ClusterInfoList))
			}
			if nhi.ClusterInfoList[0].ClusterID != 1 {
				t.Fatalf("unexpected cluster id")
			}
			if !nhi.ClusterInfoList[0].IsLeader {
				t.Errorf("not leader")
			}
			pto := pto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			if err := nh1.SyncRequestAddNode(ctx, 1, 2, "noidea:8080", 0); err != nil {
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestDroppedRequestsAreReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			if err := nh.SyncRequestAddNode(ctx, 1, 2, "noidea:8080", 0); err != nil {
				t.Fatalf("failed to add node %v", err)
			}
			for i := 0; i < 1000; i++ {
				leaderID, ok, err := nh.GetLeaderID(1)
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
			unlimited := 30 * time.Minute
			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				cs := nh.GetNoOPSession(1)
				for i := 0; i < 10; i++ {
					if _, err := nh.SyncPropose(nctx, cs, make([]byte, 1)); err != ErrClusterNotReady {
						t.Errorf("failed to get ErrClusterNotReady, got %v", err)
					}
				}
			}()
			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				for i := 0; i < 10; i++ {
					if err := nh.SyncRequestAddNode(nctx, 1, 3, "noidea:8080", 0); err != ErrClusterNotReady {
						t.Errorf("failed to get ErrClusterNotReady, got %v", err)
					}
				}
			}()
			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				for i := 0; i < 10; i++ {
					if _, err := nh.SyncRead(nctx, 1, nil); err != ErrClusterNotReady {
						t.Errorf("failed to get ErrClusterNotReady, got %v", err)
					}
				}
			}()
		},
	}
	runNodeHostTest(t, to, fs)
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
		RTTMillisecond:    getRTTMillisecond(fs, singleNodeHostTestDir),
		RaftAddress:       peers[1],
		RaftEventListener: rel,
		FS:                fs,
		Expert:            getTestExpertConfig(),
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
	pto := pto(nh)
	ctx, cancel := context.WithTimeout(context.Background(), pto)
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
	if vfs.GetTestFS() != vfs.DefaultFS {
		t.Skip("skipped as not using the default fs")
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
	v2dataDir := fs.PathJoin(targetDir, topDirName)
	to := &testOption{
		noElection: true,
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.WALDir = v2dataDir
			c.NodeHostDir = v2dataDir
			return c
		},
		tf: func(nh *NodeHost) {
			logdb := nh.logdb
			rs, err := logdb.ReadRaftState(2, 1, 0)
			if err != nil {
				t.Fatalf("failed to get raft state %v", err)
			}
			if rs.EntryCount != 3 || rs.State.Commit != 3 {
				t.Errorf("unexpected rs value")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotCanBeCompressed(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		compressed: true,
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &tests.VerboseSnapshotSM{}
		},
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		},
	}
	runNodeHostTest(t, to, fs)
}

func makeProposals(nh *NodeHost) {
	session := nh.GetNoOPSession(1)
	pto := pto(nh)
	for i := 0; i < 16; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), pto)
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
		dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nhc1 := config.NodeHostConfig{
			NodeHostDir:    dir,
			RTTMillisecond: getRTTMillisecond(fs, dir),
			RaftAddress:    nodeHostTestAddr1,
			FS:             fs,
			Expert:         getTestExpertConfig(),
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
			pto := lpto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
			if _, err := nh1.SyncRequestSnapshot(ctx, 1, opt); err != nil {
				t.Fatalf("failed to request snapshot %v", err)
			}
			cancel()
		}
		pto := pto(nh1)
		ctx, cancel := context.WithTimeout(context.Background(), pto)
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
	runNodeHostTestDC(t, tf, true, fs)
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
		pto := lpto(nh1)
		opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
		if _, err := nh2.RequestSnapshot(1, opt, pto); err != ErrInvalidOperation {
			t.Fatalf("requesting snapshot on witness not rejected")
		}
		session := nh2.GetNoOPSession(1)
		if _, err := nh2.Propose(session, []byte("test-data"), pto); err != ErrInvalidOperation {
			t.Fatalf("proposal not rejected on witness")
		}
		session = client.NewSession(1, nh2.serverCtx.GetRandomSource())
		session.PrepareForRegister()
		if _, err := nh2.ProposeSession(session, pto); err != ErrInvalidOperation {
			t.Fatalf("propose session not rejected on witness")
		}
		if _, err := nh2.ReadIndex(1, pto); err != ErrInvalidOperation {
			t.Fatalf("sync read not rejected on witness")
		}
		if _, err := nh2.RequestAddNode(1, 3, "a3.com:12345", 0, pto); err != ErrInvalidOperation {
			t.Fatalf("add node not rejected on witness")
		}
		if _, err := nh2.RequestDeleteNode(1, 3, 0, pto); err != ErrInvalidOperation {
			t.Fatalf("delete node not rejected on witness")
		}
		if _, err := nh2.RequestAddObserver(1, 3, "a3.com:12345", 0, pto); err != ErrInvalidOperation {
			t.Fatalf("add observer not rejected on witness")
		}
		if _, err := nh2.RequestAddWitness(1, 3, "a3.com:12345", 0, pto); err != ErrInvalidOperation {
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
	rtt := getRTTMillisecond(fs, singleNodeHostTestDir)
	to := &testOption{
		createSM: func(uint64, uint64) sm.IStateMachine {
			return &TimeoutStateMachine{
				updateDelay:   rtt * 10,
				snapshotDelay: rtt * 10,
			}
		},
		tf: func(nh *NodeHost) {
			timeout := time.Duration(rtt*5) * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			session := nh.GetNoOPSession(1)
			_, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			if err != ErrTimeout {
				t.Errorf("failed to return ErrTimeout, %v", err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			_, err = nh.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
			cancel()
			if err != ErrTimeout {
				t.Errorf("failed to return ErrTimeout, %v", err)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func testIOErrorIsHandled(t *testing.T, op vfs.Op) {
	inj := vfs.OnIndex(-1, op)
	fs := vfs.Wrap(vfs.GetTestFS(), inj)
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			inj.SetIndex(0)
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			session := nh.GetNoOPSession(1)
			_, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			if err != ErrTimeout {
				t.Fatalf("proposal unexpectedly completed, %v", err)
			}
			select {
			case e := <-nh.engine.ec:
				if e != vfs.ErrInjected && e.Error() != vfs.ErrInjected.Error() {
					t.Fatalf("failed to return the expected error, %v", e)
				}
			default:
				t.Fatalf("failed to trigger error")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestIOErrorIsHandled(t *testing.T) {
	if invariants.DragonboatRocksDBTest {
		t.Skip("not supported on rocksdb")
	}
	testIOErrorIsHandled(t, vfs.OpWrite)
	testIOErrorIsHandled(t, vfs.OpSync)
}
