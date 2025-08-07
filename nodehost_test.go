// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
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
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/goutils/random"
	"github.com/lni/goutils/syncutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/id"
	"github.com/lni/dragonboat/v4/internal/invariants"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/registry"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/tests"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/internal/vfs"
	chantrans "github.com/lni/dragonboat/v4/plugin/chan"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/dragonboat/v4/tools"
	"github.com/lni/dragonboat/v4/tools/upgrade310"
)

const (
	defaultTestPort = 26001
	testNodeHostID1 = "123e4567-e89b-12d3-a456-426614174000"
	testNodeHostID2 = "123e4567-e89b-12d3-a456-426614174001"
)

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
		if err := f.Close(); err != nil {
			panic(err)
		}
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
	if invariants.Race {
		return 5 * time.Second
	}
	return time.Duration(rtt*45) * time.Millisecond
}

func lpto(nh *NodeHost) time.Duration {
	rtt := nh.NodeHostConfig().RTTMillisecond
	if invariants.Race {
		return 30 * time.Second
	}
	return time.Duration(rtt*100) * time.Millisecond
}

func getTestExpertConfig(fs vfs.IFS) config.ExpertConfig {
	cfg := config.GetDefaultExpertConfig()
	cfg.LogDB.Shards = 4
	cfg.FS = fs
	return cfg
}

func reportLeakedFD(fs vfs.IFS, t *testing.T) {
	vfs.ReportLeakedFD(fs, t)
}

func getTestNodeHostConfig(fs vfs.IFS) *config.NodeHostConfig {
	cfg := &config.NodeHostConfig{
		WALDir:              singleNodeHostTestDir,
		NodeHostDir:         singleNodeHostTestDir,
		RTTMillisecond:      getRTTMillisecond(fs, singleNodeHostTestDir),
		RaftAddress:         singleNodeHostTestAddr,
		Expert:              getTestExpertConfig(fs),
		SystemEventListener: &testSysEventListener{},
	}
	return cfg
}

func getTestConfig() *config.Config {
	return &config.Config{
		ReplicaID:    1,
		ShardID:      1,
		ElectionRTT:  3,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
}

func waitNodeInfoEvent(t *testing.T, f func() []raftio.NodeInfo, count int) {
	for i := 0; i < 1000; i++ {
		if len(f()) == count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Fail(t, "failed to get node info event")
}

func waitSnapshotInfoEvent(t *testing.T,
	f func() []raftio.SnapshotInfo,
	count int) {
	for i := 0; i < 1000; i++ {
		if len(f()) == count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Fail(t, "failed to get snapshot info event")
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

func (t *testSysEventListener) NodeDeleted(info raftio.NodeInfo) {}

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

func (t *testSysEventListener) ConnectionEstablished(
	info raftio.ConnectionInfo,
) {
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

func (t *testSysEventListener) SendSnapshotCompleted(
	info raftio.SnapshotInfo,
) {
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
	closed        bool
}

func (t *TimeoutStateMachine) Update(e sm.Entry) (sm.Result, error) {
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
	fc sm.ISnapshotFileCollection,
	stopc <-chan struct{}) error {
	if t.snapshotDelay > 0 {
		time.Sleep(time.Duration(t.snapshotDelay) * time.Millisecond)
	}
	_, err := w.Write([]byte("done"))
	return err
}

func (t *TimeoutStateMachine) RecoverFromSnapshot(r io.Reader,
	fc []sm.SnapshotFile,
	stopc <-chan struct{}) error {
	return nil
}

func (t *TimeoutStateMachine) Close() error {
	t.closed = true
	return nil
}

type noopLogDB struct {
}

func (n *noopLogDB) BinaryFormat() uint32 { return 0 }
func (n *noopLogDB) Name() string         { return "noopLogDB" }
func (n *noopLogDB) Close() error         { return nil }
func (n *noopLogDB) HasNodeInfo(shardID uint64,
	replicaID uint64) (bool, error) {
	return true, nil
}
func (n *noopLogDB) CreateNodeInfo(shardID uint64, replicaID uint64) error {
	return nil
}
func (n *noopLogDB) ListNodeInfo() ([]raftio.NodeInfo, error) { return nil, nil }
func (n *noopLogDB) SaveBootstrapInfo(shardID uint64,
	replicaID uint64,
	bs pb.Bootstrap) error {
	return nil
}
func (n *noopLogDB) GetBootstrapInfo(shardID uint64,
	replicaID uint64) (pb.Bootstrap, error) {
	return pb.Bootstrap{}, nil
}
func (n *noopLogDB) SaveRaftState(updates []pb.Update,
	workerID uint64) error {
	return nil
}
func (n *noopLogDB) IterateEntries(ents []pb.Entry,
	size uint64,
	shardID uint64,
	replicaID uint64,
	low uint64,
	high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	return nil, 0, nil
}
func (n *noopLogDB) ReadRaftState(shardID uint64,
	replicaID uint64,
	lastIndex uint64) (raftio.RaftState, error) {
	return raftio.RaftState{}, nil
}
func (n *noopLogDB) RemoveEntriesTo(shardID uint64,
	replicaID uint64,
	index uint64) error {
	return nil
}
func (n *noopLogDB) CompactEntriesTo(shardID uint64,
	replicaID uint64,
	index uint64) (<-chan struct{}, error) {
	return nil, nil
}
func (n *noopLogDB) RemoveNodeData(shardID uint64, replicaID uint64) error {
	return nil
}
func (n *noopLogDB) SaveSnapshots([]pb.Update) error { return nil }
func (n *noopLogDB) GetSnapshot(shardID uint64,
	replicaID uint64) (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}
func (n *noopLogDB) ImportSnapshot(snapshot pb.Snapshot,
	replicaID uint64) error {
	return nil
}

type updateConfig func(*config.Config) *config.Config
type updateNodeHostConfig func(*config.NodeHostConfig) *config.NodeHostConfig
type testFunc func(*NodeHost)
type beforeTest func()
type afterTest func(*NodeHost)

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
	fsErrorInjection     bool
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
		if !nh.nhConfig.DefaultNodeRegistryEnabled {
			peers[cfg.ShardID] = nh.RaftAddress()
		} else {
			peers[cfg.ShardID] = nh.ID()
		}
	}
	if to.createSM != nil {
		err := nh.StartReplica(peers, to.join, to.createSM, *cfg)
		require.NoError(t, err, "start shard failed")
	} else if to.createConcurrentSM != nil {
		err := nh.StartConcurrentReplica(peers,
			to.join, to.createConcurrentSM, *cfg)
		require.NoError(t, err, "start concurrent shard failed")
	} else if to.createOnDiskSM != nil {
		err := nh.StartOnDiskReplica(peers,
			to.join, to.createOnDiskSM, *cfg)
		require.NoError(t, err, "start on disk shard fail")
	} else {
		require.Fail(t, "?!?")
	}
}

func runNodeHostTest(t *testing.T, to *testOption, fs vfs.IFS) {
	func() {
		if !to.fsErrorInjection {
			defer leaktest.AfterTest(t)()
		}
		// FIXME:
		// the following RemoveAll call will fail on windows after running error
		// injection tests as some pebble log files are not closed
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
			require.NoError(t, err, "failed to create nodehost")
		}
		if err != nil && to.newNodeHostToFail {
			return
		}
		if err == nil && to.newNodeHostToFail {
			require.Fail(t, "NewNodeHost didn't fail as expected")
		}
		if !to.restartNodeHost {
			defer func() {
				require.NotPanics(t, func() {
					if to.fsErrorInjection {
						defer func() {
							if r := recover(); r != nil {
								return
							}
						}()
					}
					nh.Close()
				})
				if to.at != nil {
					to.at(nh)
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
			nh.Close()
			nh, err = NewNodeHost(*nhc)
			require.NoError(t, err, "failed to create nodehost")
			defer func() {
				nh.Close()
				if to.at != nil {
					to.at(nh)
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
	nh *NodeHost,
	count uint64,
	timeoutExpected bool) {
	for i := uint64(0); i < count; i++ {
		pto := lpto(nh)
		ctx, cancel := context.WithTimeout(context.Background(), pto)
		cs, err := nh.SyncGetSession(ctx, 1)
		if err != nil {
			if err == ErrTimeout {
				cancel()
				return
			}
			require.NoError(t, err, "unexpected error")
		}
		//time.Sleep(100 * time.Millisecond)
		err = nh.SyncCloseSession(ctx, cs)
		if err != nil {
			if err == ErrTimeout {
				cancel()
				return
			}
			require.NoError(t, err, "failed to close client session")
		}
		cancel()
	}
	if timeoutExpected {
		require.Fail(t, "failed to trigger ")
	}
}

func runNodeHostTestDC(t *testing.T,
	f func(),
	removeDir bool,
	fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	defer func() {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}()
	if removeDir {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}
	f()
	reportLeakedFD(fs, t)
}

type testLogDBFactory struct {
	ldb raftio.ILogDB
}

func (t *testLogDBFactory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string,
	wals []string) (raftio.ILogDB, error) {
	return t.ldb, nil
}

func (t *testLogDBFactory) Name() string {
	return t.ldb.Name()
}

func TestLogDBCanBeExtended(t *testing.T) {
	fs := vfs.GetTestFS()
	ldb := &noopLogDB{}
	to := &testOption{
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.Expert.LogDBFactory = &testLogDBFactory{ldb: ldb}
			return nhc
		},

		tf: func(nh *NodeHost) {
			assert.Equal(t, ldb.Name(), nh.mu.logdb.Name())
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
			assert.Equal(t, transport.TCPTransportName, tt.GetTrans().Name())
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

type noopTransportFactory struct{}

func (noopTransportFactory) Create(cfg config.NodeHostConfig,
	h raftio.MessageHandler,
	ch raftio.ChunkHandler) raftio.ITransport {
	return transport.NewNOOPTransport(cfg, h, ch)
}

func (noopTransportFactory) Validate(string) bool {
	return true
}

func TestTransportFactoryIsStillHonored(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.Expert.TransportFactory = noopTransportFactory{}
			return nhc
		},
		tf: func(nh *NodeHost) {
			tt := nh.transport.(*transport.Transport)
			assert.Equal(t, transport.NOOPRaftName, tt.GetTrans().Name())
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestTransportFactoryCanBeSet(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.Expert.TransportFactory = &transport.NOOPTransportFactory{}
			return nhc
		},
		tf: func(nh *NodeHost) {
			tt := nh.transport.(*transport.Transport)
			assert.Equal(t, transport.NOOPRaftName, tt.GetTrans().Name())
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

type validatorTestModule struct {
}

func (tm *validatorTestModule) Create(nhConfig config.NodeHostConfig,
	handler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return transport.NewNOOPTransport(nhConfig, handler, chunkHandler)
}

func (tm *validatorTestModule) Validate(addr string) bool {
	return addr == "localhost:12346" || addr == "localhost:26001"
}

func TestAddressValidatorCanBeSet(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.Expert.TransportFactory = &validatorTestModule{}
			return nhc
		},
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			err := nh.SyncRequestAddReplica(ctx, 1, 100, "localhost:12345", 0)
			cancel()
			require.ErrorIs(t, err, ErrInvalidAddress)
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			err = nh.SyncRequestAddReplica(ctx, 1, 100, "localhost:12346", 0)
			cancel()
			require.NoError(t, err, "failed to add node")
		},
	}
	runNodeHostTest(t, to, fs)
}

type chanTransportFactory struct{}

func (*chanTransportFactory) Create(nhConfig config.NodeHostConfig,
	handler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return chantrans.NewChanTransport(nhConfig, handler, chunkHandler)
}

func (tm *chanTransportFactory) Validate(addr string) bool {
	return addr == nodeHostTestAddr1 || addr == nodeHostTestAddr2
}

func TestGossip(t *testing.T) {
	testDefaultNodeRegistryEnabled(t, true, nil)
}

func TestMediumSizedClusterGossip(t *testing.T) {
	if os.Getenv("LONG_TEST") == "" {
		t.Skip("Skipping long test")
	}
	defer leaktest.AfterTest(t)()
	for i := 0; i < 16; i++ {
		fs := vfs.GetTestFS()
		dir := fs.PathJoin(singleNodeHostTestDir, fmt.Sprintf("nh%d", i))
		cfg := config.NodeHostConfig{
			NodeHostDir:                dir,
			RTTMillisecond:             getRTTMillisecond(fs, dir),
			RaftAddress:                fmt.Sprintf("127.0.0.1:%d", 25000+i*10),
			DefaultNodeRegistryEnabled: true,
			Expert: config.ExpertConfig{
				FS:                      fs,
				TestGossipProbeInterval: 50 * time.Millisecond,
			},
			Gossip: config.GossipConfig{
				BindAddress:      fmt.Sprintf("127.0.0.1:%d", 25000+i*10+1),
				AdvertiseAddress: fmt.Sprintf("127.0.0.1:%d", 25000+i*10+1),
				Seed:             []string{"127.0.0.1:25001", "127.0.0.1:25011"},
			},
		}
		nh, err := NewNodeHost(cfg)
		require.NoError(t, err, "failed to create nh")
		defer nh.Close()
	}
}

func TestCustomTransportCanUseNodeHostID(t *testing.T) {
	factory := &chanTransportFactory{}
	testDefaultNodeRegistryEnabled(t, true, factory)
}

func TestCustomTransportCanGoWithoutNodeHostID(t *testing.T) {
	factory := &chanTransportFactory{}
	testDefaultNodeRegistryEnabled(t, false, factory)
}

func testDefaultNodeRegistryEnabled(t *testing.T,
	addressByNodeHostID bool,
	factory config.TransportFactory) {
	fs := vfs.GetTestFS()
	datadir1 := fs.PathJoin(singleNodeHostTestDir, "nh1")
	datadir2 := fs.PathJoin(singleNodeHostTestDir, "nh2")
	require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	defer func() {
		require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	}()
	addr1 := nodeHostTestAddr1
	addr2 := nodeHostTestAddr2
	nhc1 := config.NodeHostConfig{
		NodeHostDir:                datadir1,
		RTTMillisecond:             getRTTMillisecond(fs, datadir1),
		RaftAddress:                addr1,
		DefaultNodeRegistryEnabled: addressByNodeHostID,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	if addressByNodeHostID {
		nhc1.Gossip = config.GossipConfig{
			BindAddress:      "127.0.0.1:25001",
			AdvertiseAddress: "127.0.0.1:25001",
			Seed:             []string{"127.0.0.1:25002"},
		}
	}
	nhc2 := config.NodeHostConfig{
		NodeHostDir:                datadir2,
		RTTMillisecond:             getRTTMillisecond(fs, datadir2),
		RaftAddress:                addr2,
		DefaultNodeRegistryEnabled: addressByNodeHostID,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	if addressByNodeHostID {
		nhc2.Gossip = config.GossipConfig{
			BindAddress:      "127.0.0.1:25002",
			AdvertiseAddress: "127.0.0.1:25002",
			Seed:             []string{"127.0.0.1:25001"},
		}
	}
	nhid1, err := id.NewUUID(testNodeHostID1)
	require.NoError(t, err, "failed to parse nhid")
	nhc1.NodeHostID = nhid1.String()
	nhid2, err := id.NewUUID(testNodeHostID2)
	require.NoError(t, err, "failed to parse nhid")
	nhc2.NodeHostID = nhid2.String()
	nhc1.Expert.TransportFactory = factory
	nhc2.Expert.TransportFactory = factory
	nh1, err := NewNodeHost(nhc1)
	require.NoError(t, err, "failed to create nh")
	defer nh1.Close()
	nh2, err := NewNodeHost(nhc2)
	require.NoError(t, err, "failed to create nh2")
	defer nh2.Close()
	peers := make(map[uint64]string)
	if addressByNodeHostID {
		peers[1] = testNodeHostID1
		peers[2] = testNodeHostID2
	} else {
		peers[1] = addr1
		peers[2] = addr2
	}
	createSM := func(uint64, uint64) sm.IStateMachine {
		return &PST{}
	}
	rc := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     10,
		HeartbeatRTT:    1,
		SnapshotEntries: 0,
	}
	require.NoError(t, nh1.StartReplica(peers, false, createSM, rc))
	rc.ReplicaID = 2
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh1, 1)
	waitForLeaderToBeElected(t, nh2, 1)
	pto := lpto(nh1)
	session := nh1.GetNoOPSession(1)
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), pto)
		if _, err := nh1.SyncPropose(ctx, session, make([]byte, 0)); err == nil {
			cancel()
			return
		}
		cancel()
		time.Sleep(100 * time.Millisecond)
	}
	require.Fail(t, "failed to make proposal")
}

func TestNodeHostRegistry(t *testing.T) {
	fs := vfs.GetTestFS()
	datadir1 := fs.PathJoin(singleNodeHostTestDir, "nh1")
	datadir2 := fs.PathJoin(singleNodeHostTestDir, "nh2")
	require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	defer func() {
		require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	}()
	addr1 := nodeHostTestAddr1
	addr2 := nodeHostTestAddr2
	nhc1 := config.NodeHostConfig{
		NodeHostDir:                datadir1,
		RTTMillisecond:             getRTTMillisecond(fs, datadir1),
		RaftAddress:                addr1,
		DefaultNodeRegistryEnabled: true,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	nhc1.Gossip = config.GossipConfig{
		BindAddress:      "127.0.0.1:25001",
		AdvertiseAddress: "127.0.0.1:25001",
		Seed:             []string{"127.0.0.1:25002"},
	}
	nhc2 := config.NodeHostConfig{
		NodeHostDir:                datadir2,
		RTTMillisecond:             getRTTMillisecond(fs, datadir2),
		RaftAddress:                addr2,
		DefaultNodeRegistryEnabled: true,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	nhc2.Gossip = config.GossipConfig{
		BindAddress:      "127.0.0.1:25002",
		AdvertiseAddress: "127.0.0.1:25002",
		Seed:             []string{"127.0.0.1:25001"},
	}
	nhid1, err := id.NewUUID(testNodeHostID1)
	require.NoError(t, err, "failed to parse nhid")
	nhc1.NodeHostID = nhid1.String()
	nhc1.Gossip.Meta = []byte(testNodeHostID1)
	nhid2, err := id.NewUUID(testNodeHostID2)
	require.NoError(t, err, "failed to parse nhid")
	nhc2.NodeHostID = nhid2.String()
	nhc2.Gossip.Meta = []byte(testNodeHostID2)
	nh1, err := NewNodeHost(nhc1)
	require.NoError(t, err, "failed to create nh")
	defer nh1.Close()
	nh2, err := NewNodeHost(nhc2)
	require.NoError(t, err, "failed to create nh2")
	defer nh2.Close()
	peers := make(map[uint64]string)
	peers[1] = testNodeHostID1
	createSM := func(uint64, uint64) sm.IStateMachine {
		return &PST{}
	}
	rc := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     10,
		HeartbeatRTT:    1,
		SnapshotEntries: 0,
	}
	require.NoError(t, nh1.StartReplica(peers, false, createSM, rc))
	rc.ShardID = 2
	peers[1] = testNodeHostID2
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	rc.ShardID = 3
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh1, 1)
	waitForLeaderToBeElected(t, nh2, 2)
	waitForLeaderToBeElected(t, nh2, 3)
	good := false
	for i := 0; i < 1000; i++ {
		r1, ok := nh1.GetNodeHostRegistry()
		assert.True(t, ok)
		r2, ok := nh2.GetNodeHostRegistry()
		assert.True(t, ok)
		if r1.NumOfShards() != 3 || r2.NumOfShards() != 3 {
			time.Sleep(10 * time.Millisecond)
		} else {
			good = true
			break
		}
	}
	require.True(t, good, "registry failed to report the expected num of shards")
	rc.ShardID = 100
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh2, 100)
	for i := 0; i < 1000; i++ {
		r1, ok := nh1.GetNodeHostRegistry()
		assert.True(t, ok)
		r2, ok := nh2.GetNodeHostRegistry()
		assert.True(t, ok)
		if r1.NumOfShards() != 4 || r2.NumOfShards() != 4 {
			time.Sleep(10 * time.Millisecond)
		} else {
			v1, ok := r1.GetMeta(testNodeHostID1)
			assert.True(t, ok)
			assert.Equal(t, testNodeHostID1, string(v1))
			v2, ok := r1.GetMeta(testNodeHostID2)
			assert.True(t, ok)
			assert.Equal(t, testNodeHostID2, string(v2))
			return
		}
	}
	require.Fail(t, "failed to report the expected num of shards")
}

func TestGossipCanHandleDynamicRaftAddress(t *testing.T) {
	fs := vfs.GetTestFS()
	datadir1 := fs.PathJoin(singleNodeHostTestDir, "nh1")
	datadir2 := fs.PathJoin(singleNodeHostTestDir, "nh2")
	require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	defer func() {
		require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	}()
	addr1 := nodeHostTestAddr1
	addr2 := nodeHostTestAddr2
	nhc1 := config.NodeHostConfig{
		NodeHostDir:                datadir1,
		RTTMillisecond:             getRTTMillisecond(fs, datadir1),
		RaftAddress:                addr1,
		DefaultNodeRegistryEnabled: true,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	nhc2 := config.NodeHostConfig{
		NodeHostDir:                datadir2,
		RTTMillisecond:             getRTTMillisecond(fs, datadir2),
		RaftAddress:                addr2,
		DefaultNodeRegistryEnabled: true,
		Expert: config.ExpertConfig{
			FS:                      fs,
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
	}
	nhid1, err := id.NewUUID(testNodeHostID1)
	require.NoError(t, err, "failed to parse nhid")
	nhc1.NodeHostID = nhid1.String()
	nhid2, err := id.NewUUID(testNodeHostID2)
	require.NoError(t, err, "failed to parse nhid")
	nhc2.NodeHostID = nhid2.String()
	nhc1.Gossip = config.GossipConfig{
		BindAddress:      "127.0.0.1:25001",
		AdvertiseAddress: "127.0.0.1:25001",
		Seed:             []string{"127.0.0.1:25002"},
	}
	nhc2.Gossip = config.GossipConfig{
		BindAddress:      "127.0.0.1:25002",
		AdvertiseAddress: "127.0.0.1:25002",
		Seed:             []string{"127.0.0.1:25001"},
	}
	nh1, err := NewNodeHost(nhc1)
	require.NoError(t, err, "failed to create nh")
	defer nh1.Close()
	nh2, err := NewNodeHost(nhc2)
	require.NoError(t, err, "failed to create nh2")
	nh2NodeHostID := nh2.ID()
	peers := make(map[uint64]string)
	peers[1] = testNodeHostID1
	peers[2] = testNodeHostID2
	createSM := func(uint64, uint64) sm.IStateMachine {
		return &PST{}
	}
	rc := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     3,
		HeartbeatRTT:    1,
		SnapshotEntries: 0,
	}
	require.NoError(t, nh1.StartReplica(peers, false, createSM, rc))
	rc.ReplicaID = 2
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh1, 1)
	waitForLeaderToBeElected(t, nh2, 1)
	pto := lpto(nh1)
	session := nh1.GetNoOPSession(1)
	testProposal := func() {
		done := false
		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 0))
			cancel()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			done = true
			break
		}
		require.True(t, done, "failed to make proposal")
	}
	testProposal()
	nh2.Close()
	nhc2.RaftAddress = nodeHostTestAddr3
	nh2, err = NewNodeHost(nhc2)
	require.NoError(t, err, "failed to restart nh2")
	defer nh2.Close()
	require.Equal(t, nh2NodeHostID, nh2.ID(), "NodeHostID changed")
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh2, 1)
	testProposal()
}

type testRegistry struct {
	*registry.Registry

	mu *sync.Mutex // PROTECTS(nodeAddrs)
	// map of nhid -> host:port
	nodeAddrs map[string]string
}

func (tr *testRegistry) Resolve(shardID uint64,
	replicaID uint64) (string, string, error) {
	nhid, ck, err := tr.Registry.Resolve(shardID, replicaID)
	if err != nil {
		return "", "", err
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.nodeAddrs[nhid], ck, nil
}

type testRegistryFactory struct {
	mu sync.Mutex // PROTECTS(nodeAddrs)
	// map of nhid -> host:port
	nodeAddrs map[string]string
}

func (trf *testRegistryFactory) Set(nhid, addr string) {
	trf.mu.Lock()
	defer trf.mu.Unlock()
	trf.nodeAddrs[nhid] = addr
}

func (trf *testRegistryFactory) Create(nhid string,
	streamConnections uint64,
	v config.TargetValidator) (raftio.INodeRegistry, error) {
	return &testRegistry{
		registry.NewNodeRegistry(streamConnections, v),
		&trf.mu,
		trf.nodeAddrs,
	}, nil
}

func TestExternalNodeRegistryFunction(t *testing.T) {
	fs := vfs.GetTestFS()
	datadir1 := fs.PathJoin(singleNodeHostTestDir, "nh1")
	datadir2 := fs.PathJoin(singleNodeHostTestDir, "nh2")
	require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	defer func() {
		require.NoError(t, os.RemoveAll(singleNodeHostTestDir))
	}()
	addr1 := nodeHostTestAddr1
	addr2 := nodeHostTestAddr2
	nhc1 := config.NodeHostConfig{
		NodeHostDir:    datadir1,
		RTTMillisecond: getRTTMillisecond(fs, datadir1),
		RaftAddress:    addr1,
		Expert: config.ExpertConfig{
			FS: fs,
		},
	}
	nhc2 := config.NodeHostConfig{
		NodeHostDir:    datadir2,
		RTTMillisecond: getRTTMillisecond(fs, datadir2),
		RaftAddress:    addr2,
		Expert: config.ExpertConfig{
			FS: fs,
		},
	}
	nhid1, err := id.NewUUID(testNodeHostID1)
	require.NoError(t, err, "failed to parse nhid")
	nhc1.NodeHostID = nhid1.String()
	nhid2, err := id.NewUUID(testNodeHostID2)
	require.NoError(t, err, "failed to parse nhid")
	nhc2.NodeHostID = nhid2.String()
	testRegistryFactory := &testRegistryFactory{
		nodeAddrs: map[string]string{
			nhc1.NodeHostID: nodeHostTestAddr1,
			nhc2.NodeHostID: nodeHostTestAddr2,
		},
	}
	nhc1.Expert.NodeRegistryFactory = testRegistryFactory
	nhc2.Expert.NodeRegistryFactory = testRegistryFactory

	nh1, err := NewNodeHost(nhc1)
	require.NoError(t, err, "failed to create nh")
	defer nh1.Close()
	nh2, err := NewNodeHost(nhc2)
	require.NoError(t, err, "failed to create nh2")
	nh2NodeHostID := nh2.ID()
	peers := make(map[uint64]string)
	peers[1] = testNodeHostID1
	peers[2] = testNodeHostID2
	createSM := func(uint64, uint64) sm.IStateMachine {
		return &PST{}
	}
	rc := config.Config{
		ShardID:         1,
		ReplicaID:       1,
		ElectionRTT:     3,
		HeartbeatRTT:    1,
		SnapshotEntries: 0,
	}
	require.NoError(t, nh1.StartReplica(peers, false, createSM, rc))
	rc.ReplicaID = 2
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh1, 1)
	waitForLeaderToBeElected(t, nh2, 1)
	pto := lpto(nh1)
	session := nh1.GetNoOPSession(1)
	testProposal := func() {
		done := false
		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 0))
			cancel()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			done = true
			break
		}
		require.True(t, done, "failed to make proposal")
	}
	testProposal()
	nh2.Close()
	nhc2.RaftAddress = nodeHostTestAddr3
	testRegistryFactory.Set(nh2NodeHostID, nodeHostTestAddr3)
	nh2, err = NewNodeHost(nhc2)
	require.NoError(t, err, "failed to restart nh2")
	defer nh2.Close()
	require.Equal(t, nh2NodeHostID, nh2.ID(), "NodeHostID changed")
	require.NoError(t, nh2.StartReplica(peers, false, createSM, rc))
	waitForLeaderToBeElected(t, nh2, 1)
	testProposal()
}

func TestNewNodeHostReturnErrorOnInvalidConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.RaftAddress = "12345"
			require.Error(t, nhc.Validate(), "config is not considered as invalid")
			return nhc
		},
		newNodeHostToFail: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestDeploymentIDCanBeSetUsingNodeHostConfig(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		updateNodeHostConfig: func(
			nhc *config.NodeHostConfig) *config.NodeHostConfig {
			nhc.DeploymentID = 1000
			return nhc
		},
		tf: func(nh *NodeHost) {
			nhc := nh.NodeHostConfig()
			assert.Equal(t, uint64(1000), nhc.GetDeploymentID())
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

var (
	singleNodeHostTestAddr = fmt.Sprintf("localhost:%d", getTestPort())
	nodeHostTestAddr1      = fmt.Sprintf("localhost:%d", getTestPort())
	nodeHostTestAddr2      = fmt.Sprintf("localhost:%d", getTestPort()+1)
	nodeHostTestAddr3      = fmt.Sprintf("localhost:%d", getTestPort()+2)
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
func (n *PST) Update(e sm.Entry) (sm.Result, error) {
	return sm.Result{Value: uint64(len(e.Cmd))}, nil
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
			return sm.ErrSnapshotStopped
		default:
		}
	}
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *PST) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{}) error {
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
	datadir string,
	snapshotEntry uint64,
	concurrent bool,
	fs vfs.IFS) (*NodeHost, error) {
	// config for raft
	rc := config.Config{
		ReplicaID:          uint64(1),
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
		Expert:         getTestExpertConfig(fs),
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
	rc.ShardID = 1 + nhc.Expert.Engine.ApplyShards
	if err := nh.StartConcurrentReplica(peers, false, newConcurrentSM, rc); err != nil {
		return nil, err
	}
	rc.ShardID = 1
	if err := nh.StartReplica(peers, false, newSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
}

func singleConcurrentNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost),
	snapshotEntry uint64,
	concurrent bool,
	fs vfs.IFS) {
	defer func() {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}()
	func() {
		defer leaktest.AfterTest(t)()
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
		nh, err := createConcurrentTestNodeHost(singleNodeHostTestAddr,
			singleNodeHostTestDir, snapshotEntry, concurrent, fs)
		require.NoError(t, err, "failed to create nodehost")
		defer func() {
			nh.Close()
		}()
		nhc := nh.NodeHostConfig()
		waitForLeaderToBeElected(t, nh, 1)
		waitForLeaderToBeElected(t, nh, 1+nhc.Expert.Engine.ApplyShards)
		tf(t, nh)
	}()
	reportLeakedFD(fs, t)
}

func twoFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost),
	fs vfs.IFS) {
	defer func() {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}()
	func() {
		defer leaktest.AfterTest(t)()
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
		nh1, nh2, err := createFakeDiskTwoTestNodeHosts(nodeHostTestAddr1,
			nodeHostTestAddr2, nh1dir, nh2dir, fs)
		require.NoError(t, err, "failed to create nodehost")
		defer func() {
			nh1.Close()
			nh2.Close()
		}()
		tf(t, nh1, nh2)
	}()
	reportLeakedFD(fs, t)
}

func createFakeDiskTwoTestNodeHosts(addr1 string,
	addr2 string,
	datadir1 string,
	datadir2 string,
	fs vfs.IFS) (*NodeHost, *NodeHost, error) {
	peers := make(map[uint64]string)
	peers[1] = addr1
	nhc1 := config.NodeHostConfig{
		WALDir:              datadir1,
		NodeHostDir:         datadir1,
		RTTMillisecond:      getRTTMillisecond(fs, datadir1),
		RaftAddress:         addr1,
		SystemEventListener: &testSysEventListener{},
		Expert:              getTestExpertConfig(fs),
	}
	nhc2 := config.NodeHostConfig{
		WALDir:              datadir2,
		NodeHostDir:         datadir2,
		RTTMillisecond:      getRTTMillisecond(fs, datadir2),
		RaftAddress:         addr2,
		SystemEventListener: &testSysEventListener{},
		Expert:              getTestExpertConfig(fs),
	}
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

func createRateLimitedTwoTestNodeHosts(addr1 string,
	addr2 string,
	datadir1 string,
	datadir2 string,
	fs vfs.IFS) (*NodeHost, *NodeHost, *tests.NoOP, *tests.NoOP, error) {
	rc := config.Config{
		ShardID:         1,
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
		Expert:         getTestExpertConfig(fs),
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         datadir2,
		NodeHostDir:    datadir2,
		RTTMillisecond: getRTTMillisecond(fs, datadir2),
		RaftAddress:    peers[2],
		Expert:         getTestExpertConfig(fs),
	}
	nh1, err := NewNodeHost(nhc1)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	nh2, err := NewNodeHost(nhc2)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sm1 := &tests.NoOP{}
	sm2 := &tests.NoOP{}
	newRSM1 := func(shardID uint64, replicaID uint64) sm.IStateMachine {
		return sm1
	}
	newRSM2 := func(shardID uint64, replicaID uint64) sm.IStateMachine {
		return sm2
	}
	rc.ReplicaID = 1
	if err := nh1.StartReplica(peers, false, newRSM1, rc); err != nil {
		return nil, nil, nil, nil, err
	}
	rc.ReplicaID = 2
	if err := nh2.StartReplica(peers, false, newRSM2, rc); err != nil {
		return nil, nil, nil, nil, err
	}
	var leaderNh *NodeHost
	var followerNh *NodeHost

	for i := 0; i < 200; i++ {
		leaderID, _, ready, err := nh1.GetLeaderID(1)
		if err == nil && ready {
			if leaderID == 1 {
				leaderNh = nh1
				followerNh = nh2
				sm2.SetSleepTime(nhc1.RTTMillisecond * 10)
			} else {
				leaderNh = nh2
				followerNh = nh1
				sm1.SetSleepTime(nhc1.RTTMillisecond * 10)
			}
			return leaderNh, followerNh, sm1, sm2, nil
		}
		// wait for leader to be elected
		time.Sleep(100 * time.Millisecond)
	}
	return nil, nil, nil, nil, errors.New("failed to get usable nodehosts")
}

func rateLimitedTwoNodeHostTest(t *testing.T,
	tf func(t *testing.T,
		leaderNh *NodeHost,
		followerNh *NodeHost,
		n1 *tests.NoOP,
		n2 *tests.NoOP),
	fs vfs.IFS) {
	defer func() {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}()
	func() {
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
		defer leaktest.AfterTest(t)()
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
		nh1, nh2, n1, n2, err := createRateLimitedTwoTestNodeHosts(nodeHostTestAddr1,
			nodeHostTestAddr2, nh1dir, nh2dir, fs)
		require.NoError(t, err, "failed to create nodehost2")
		defer func() {
			nh1.Close()
			nh2.Close()
		}()
		tf(t, nh1, nh2, n1, n2)
	}()
	reportLeakedFD(fs, t)
}

func waitForLeaderToBeElected(t *testing.T, nh *NodeHost, shardID uint64) {
	for i := 0; i < 200; i++ {
		_, term, ready, err := nh.GetLeaderID(shardID)
		if err == nil && ready {
			if term == 0 {
				panic("term is 0")
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Fail(t, "failed to elect leader")
}

func TestJoinedShardCanBeRestartedOrJoinedAgain(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			cfg := getTestConfig()
			peers := make(map[uint64]string)
			newPST := func(uint64, uint64) sm.IStateMachine { return &PST{} }
			require.NoError(t, nh.StopShard(1), "failed to stop the shard")
			for i := 0; i < 1000; i++ {
				err := nh.StartReplica(peers, true, newPST, *cfg)
				if err == nil {
					return
				}
				if err == ErrShardAlreadyExist {
					time.Sleep(5 * time.Millisecond)
					continue
				} else {
					require.NoError(t, err, "failed to join the shard again")
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
			require.NoError(t, err, "failed to make proposal")
			opt := SnapshotOption{
				OverrideCompactionOverhead: true,
				CompactionOverhead:         0,
			}
			_, err = nh.SyncRequestSnapshot(ctx, 1, opt)
			require.NoError(t, err, "failed to request snapshot")
			for i := 0; i < 100; i++ {
				op, err := nh.RequestCompaction(1, 1)
				if err == ErrRejected {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				require.NoError(t, err, "failed to request compaction")
				select {
				case <-op.ResultC():
				case <-ctx.Done():
					require.FailNow(t, "failed to complete the compaction")
				}
				break
			}
			_, err = nh.RequestCompaction(1, 1)
			require.Equal(t, ErrRejected, err, "not rejected")
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
		createSM: func(shardID uint64, replicaID uint64) sm.IStateMachine {
			return pst
		},
		tf: func(nh *NodeHost) {
			createProposalsToTriggerSnapshot(t, nh, 50, true)
		},
		at: func(*NodeHost) {
			assert.True(t, pst.saved && pst.stopped, "snapshot not stopped")
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
		createSM: func(shardID uint64, replicaID uint64) sm.IStateMachine {
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
		at: func(*NodeHost) {
			wait := 0
			for !pst.stopped {
				time.Sleep(10 * time.Millisecond)
				wait++
				if wait > 1000 {
					break
				}
			}
			assert.True(t, pst.getRestored(), "not restored")
			assert.True(t, pst.stopped, "not stopped")
		},
		restartNodeHost: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestGetRequestState(t *testing.T) {
	tests := []struct {
		code RequestResultCode
		err  error
	}{
		{requestCompleted, nil},
		{requestRejected, ErrRejected},
		{requestTimeout, ErrTimeout},
		{requestTerminated, ErrShardClosed},
		{requestDropped, ErrShardNotReady},
		{requestAborted, ErrAborted},
	}

	for _, tt := range tests {
		rs := &RequestState{
			CompletedC: make(chan RequestResult, 1),
		}
		result := RequestResult{code: tt.code}
		rs.notify(result)
		_, err := getRequestState(context.TODO(), rs)
		assert.ErrorIs(t, err, tt.err)
	}
}

func TestGetRequestStateTimeoutAndCancel(t *testing.T) {
	func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Millisecond)
		defer cancel()
		time.Sleep(2 * time.Millisecond)
		rs := &RequestState{
			CompletedC: make(chan RequestResult, 1),
		}
		_, err := getRequestState(ctx, rs)
		assert.ErrorIs(t, err, ErrTimeout)
	}()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		cancel()
		rs := &RequestState{
			CompletedC: make(chan RequestResult, 1),
		}
		_, err := getRequestState(ctx, rs)
		assert.ErrorIs(t, err, ErrCanceled)
	}()
}

func TestNodeHostIDIsStatic(t *testing.T) {
	fs := vfs.GetTestFS()
	id := ""
	to := &testOption{
		restartNodeHost: true,
		noElection:      true,
		tf: func(nh *NodeHost) {
			id = nh.ID()
		},
		rf: func(nh *NodeHost) {
			require.Equal(t, id, nh.ID(), "NodeHost ID value changed")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostIDCanBeSet(t *testing.T) {
	fs := vfs.GetTestFS()
	nhidStr := testNodeHostID1
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.NodeHostID = nhidStr
			return c
		},
		noElection: true,
		tf: func(nh *NodeHost) {
			nhid, err := id.NewUUID(nhidStr)
			require.NoError(t, err, "failed to create NodeHostID")
			require.Equal(t, nhid.String(), nh.ID(), "failed to set nhid")
		},
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
			err := nh.SyncRequestAddReplica(ctx, 1, 100, "a1", 0)
			assert.ErrorIs(t, err, ErrInvalidAddress)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestInvalidContextDeadlineIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			rctx, rcancel := context.WithTimeout(context.Background(), pto)
			rcs, err := nh.SyncGetSession(rctx, 1)
			rcancel()
			require.NoError(t, err, "failed to get regular session")
			// 8 * time.Millisecond is smaller than the smallest RTTMillisecond
			ctx, cancel := context.WithTimeout(context.Background(),
				8*time.Millisecond)
			defer cancel()
			cs := nh.GetNoOPSession(1)
			_, err = nh.SyncPropose(ctx, cs, make([]byte, 1))
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			_, err = nh.SyncRead(ctx, 1, nil)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			_, err = nh.SyncGetSession(ctx, 1)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			err = nh.SyncCloseSession(ctx, rcs)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			_, err = nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			err = nh.SyncRequestDeleteReplica(ctx, 1, 1, 0)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			err = nh.SyncRequestAddReplica(ctx, 1, 100, "a1.com:12345", 0)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
			err = nh.SyncRequestAddNonVoting(ctx, 1, 100, "a1.com:12345", 0)
			assert.ErrorIs(t, err, ErrTimeoutTooSmall)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestErrShardNotFoundCanBeReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			_, _, _, err := nh.GetLeaderID(1234)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.StaleRead(1234, nil)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.RequestSnapshot(1234, DefaultSnapshotOption, pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.RequestDeleteReplica(1234, 10, 0, pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.RequestAddReplica(1234, 10, "a1", 0, pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.RequestAddNonVoting(1234, 10, "a1", 0, pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			err = nh.RequestLeaderTransfer(1234, 10)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, err = nh.GetNodeUser(1234)
			assert.ErrorIs(t, err, ErrShardNotFound)
			cs := nh.GetNoOPSession(1234)
			_, err = nh.propose(cs, make([]byte, 1), pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			_, _, err = nh.readIndex(1234, pto)
			assert.ErrorIs(t, err, ErrShardNotFound)
			err = nh.stopNode(1234, 1, true)
			assert.ErrorIs(t, err, ErrShardNotFound)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestGetShardMembership(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			_, err := nh.SyncGetShardMembership(ctx, 1)
			require.NoError(t, err, "failed to get shard membership")
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
			assert.NoError(t, err, "failed to get client session")
			cs.PrepareForRegister()
			rs, err := nh.ProposeSession(cs, pto)
			assert.NoError(t, err, "failed to propose client session")
			r := <-rs.ResultC()
			assert.True(t, r.Rejected(),
				"failed to reject the cs registeration")
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
			assert.NoError(t, err, "failed to get client session")
			err = nh.SyncCloseSession(ctx, cs)
			assert.NoError(t, err, "failed to unregister the client session")
			err = nh.SyncCloseSession(ctx, cs)
			assert.ErrorIs(t, err, ErrRejected, "failed to reject the request")
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
			logdb := nh.mu.logdb
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
				ss, err := logdb.GetSnapshot(1, 1)
				require.NoError(t, err, "failed to list snapshots")
				if !pb.IsEmptySnapshot(ss) {
					snapshotted = true
					snapshot = ss
					break
				}
			}
			require.True(t, snapshotted, "snapshot not triggered")
			crc, err := rsm.GetV2PayloadChecksum(snapshot.Filepath, fs)
			require.NoError(t, err, "failed to get payload checksum")
			assert.True(t, bytes.Equal(crc, snapshot.Checksum), "checksum changed")
			ss := pb.Snapshot{}
			err = fileutil.GetFlagFileContent(fs.PathDir(snapshot.Filepath),
				"snapshot.metadata", &ss, fs)
			require.NoError(t, err, "failed to get content")
			assert.Equal(t, &snapshot, &ss, "snapshot record changed")
		},
	}
	runNodeHostTest(t, to, fs)
}

func testZombieSnapshotDirWillBeDeletedDuringAddShard(t *testing.T,
	dirName string,
	fs vfs.IFS) {
	var z1 string
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			did := nh.nhConfig.GetDeploymentID()
			require.NoError(t, nh.env.CreateSnapshotDir(did, 1, 1),
				"failed to get snap dir")
			snapDir := nh.env.GetSnapshotDir(did, 1, 1)
			z1 = fs.PathJoin(snapDir, dirName)
			require.NoError(t, fs.MkdirAll(z1, 0755), "failed to create dir")
		},
		rf: func(nh *NodeHost) {
			_, err := fs.Stat(z1)
			require.True(t, vfs.IsNotExist(err), "failed to delete zombie dir")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestZombieSnapshotDirWillBeDeletedDuringAddShard(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	testZombieSnapshotDirWillBeDeletedDuringAddShard(t,
		"snapshot-AB-01.receiving", fs)
	testZombieSnapshotDirWillBeDeletedDuringAddShard(t,
		"snapshot-AB-10.generating", fs)
}

func TestNodeHostReadIndex(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			rs, err := nh.ReadIndex(1, pto)
			require.NoError(t, err, "failed to read index")
			require.NotNil(t, rs.node, "rs.node not set")
			v := <-rs.ResultC()
			assert.True(t, v.Completed(), "failed to complete read index")
			_, err = nh.ReadLocalNode(rs, make([]byte, 128))
			assert.NoError(t, err, "read local failed")
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
			require.NoError(t, err, "failed to read index")
			v := <-rs.ResultC()
			require.True(t, v.Completed(), "failed to complete read index")
			_, err = nh.NAReadLocalNode(rs, make([]byte, 128))
			assert.ErrorIs(t, err, sm.ErrNotImplemented)
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
			require.NoError(t, err, "make proposal failed")
			assert.Equal(t, uint64(128), v.Value, "unexpected result")
			data, err := nh.SyncRead(ctx, 1, make([]byte, 128))
			require.NoError(t, err, "make linearizable read failed")
			require.NotNil(t, data)
			require.NotEmpty(t, data.([]byte), "failed to get result")
			require.NoError(t, nh.StopShard(1), "failed to stop shard 2")
			listener, ok := nh.events.sys.ul.(*testSysEventListener)
			require.True(t, ok, "failed to get the system event listener")
			waitNodeInfoEvent(t, listener.getNodeReady, 1)
			ni := listener.getNodeReady()[0]
			assert.Equal(t, uint64(1), ni.ShardID, "incorrect node ready info")
			assert.Equal(t, uint64(1), ni.ReplicaID, "incorrect node ready info")
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
			require.NoError(t, err, "make proposal failed")
			logdb := nh.mu.logdb
			ents, _, err := logdb.IterateEntries(nil,
				0, 1, 1, 1, 100, math.MaxUint64)
			require.NoError(t, err, "failed to get entries")
			hasEncodedEntry := false
			for _, e := range ents {
				if e.Type == pb.EncodedEntry {
					hasEncodedEntry = true
					payload, err := rsm.GetPayload(e)
					require.NoError(t, err, "failed to get payload")
					assert.Equal(t, make([]byte, 1024), payload, "payload changed")
				}
			}
			assert.True(t, hasEncodedEntry, "failed to locate any encoded entry")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestOrderedMembershipChange(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		updateConfig: func(c *config.Config) *config.Config {
			c.OrderedConfigChange = true
			return c
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			{
				ctx, cancel := context.WithTimeout(context.Background(), 2*pto)
				defer cancel()
				m, err := nh.SyncGetShardMembership(ctx, 1)
				require.NoError(t, err, "get membership failed")
				err = nh.SyncRequestAddReplica(ctx,
					1, 2, "localhost:25000", m.ConfigChangeID+1)
				assert.Error(t, err, "unexpectedly completed")
			}
			{
				ctx, cancel := context.WithTimeout(context.Background(), 2*pto)
				defer cancel()
				m, err := nh.SyncGetShardMembership(ctx, 1)
				require.NoError(t, err, "get membership failed")
				err = nh.SyncRequestAddReplica(ctx,
					1, 2, "localhost:25000", m.ConfigChangeID)
				require.NoError(t, err, "failed to add node")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestDeleteReplica(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestDeleteReplica(ctx, 1, 2, 0)
			require.NoError(t, err, "failed to delete node")
			listener, ok := nh.events.sys.ul.(*testSysEventListener)
			require.True(t, ok, "failed to get the system event listener")
			waitNodeInfoEvent(t, listener.getMembershipChanged, 2)
			ni := listener.getMembershipChanged()[1]
			assert.Equal(t, uint64(1), ni.ShardID)
			assert.Equal(t, uint64(1), ni.ReplicaID)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestAddReplica(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddReplica(ctx, 1, 2, "localhost:25000", 0)
			assert.NoError(t, err, "failed to add node")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRequestAddNonVoting(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddNonVoting(ctx, 1, 2, "localhost:25000", 0)
			assert.NoError(t, err, "failed to add nonVoting")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostAddReplica(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			rs, err := nh.RequestAddReplica(1, 2, "localhost:25000", 0, pto)
			require.NoError(t, err, "failed to add node")
			v := <-rs.ResultC()
			assert.True(t, v.Completed(), "failed to complete add node")
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
			assert.NoError(t, err, "failed to get NodeUser")
			assert.NotNil(t, n, "got a nil NodeUser")
			n, err = nh.GetNodeUser(123)
			assert.ErrorIs(t, err, ErrShardNotFound, "didn't return expected err")
			assert.Nil(t, n, "got unexpected node user")
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
			require.NoError(t, err, "failed to get NodeUser")
			cs := nh.GetNoOPSession(1)
			rs, err := n.Propose(cs, make([]byte, 16), pto)
			assert.NoError(t, err, "failed to make propose")
			v := <-rs.ResultC()
			assert.True(t, v.Completed(), "failed to complete proposal")
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
			require.NoError(t, err, "failed to get NodeUser")
			rs, err := n.ReadIndex(pto)
			assert.NoError(t, err, "failed to read index")
			v := <-rs.ResultC()
			assert.True(t, v.Completed(), "failed to complete read index")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostAddNonVotingRemoveNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			rs, err := nh.RequestAddNonVoting(1, 2, "localhost:25000", 0, pto)
			require.NoError(t, err, "failed to add node")
			v := <-rs.ResultC()
			require.True(t, v.Completed(), "failed to complete add node")
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			membership, err := nh.SyncGetShardMembership(ctx, 1)
			require.NoError(t, err, "failed to get shard membership")
			require.Equal(t, 1, len(membership.Nodes), "unexpected nodes len")
			require.Equal(t, 0, len(membership.Removed), "unexpected removed len")
			require.Equal(t, 1, len(membership.NonVotings), "unexpected nodes len")
			_, ok := membership.NonVotings[2]
			require.True(t, ok, "node 2 not added")
			// remove it
			rs, err = nh.RequestDeleteReplica(1, 2, 0, pto)
			require.NoError(t, err, "failed to remove node")
			v = <-rs.ResultC()
			require.True(t, v.Completed(), "failed to complete remove node")
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			defer cancel()
			membership, err = nh.SyncGetShardMembership(ctx, 1)
			require.NoError(t, err, "failed to get shard membership")
			require.Equal(t, 1, len(membership.Nodes), "unexpected nodes len")
			require.Equal(t, 1, len(membership.Removed), "unexpected removed len")
			require.Equal(t, 0, len(membership.NonVotings), "unexpected nodes len")
			_, ok = membership.Removed[2]
			require.True(t, ok, "node 2 not removed")
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
			err := nh.RequestLeaderTransfer(1, 1)
			assert.NoError(t, err, "leader transfer failed")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostHasNodeInfo(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			assert.True(t, nh.HasNodeInfo(1, 1), "node info missing")
			assert.False(t, nh.HasNodeInfo(1, 2), "unexpected node info")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestOnDiskStateMachineDoesNotSupportClientSession(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		fakeDiskNode: true,
		tf: func(nh *NodeHost) {
			require.Panics(t, func() {
				pto := pto(nh)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nh.SyncGetSession(ctx, 1)
				cancel()
				assert.Error(t, err, "managed to get new session")
			}, "no panic when proposing session on disk SM")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestStaleReadOnUninitializedNodeReturnError(t *testing.T) {
	fs := vfs.GetTestFS()
	fakeDiskSM := tests.NewFakeDiskSM(0)
	fakeDiskSM.SlowOpen.Store(true)
	to := &testOption{
		createOnDiskSM: func(uint64, uint64) sm.IOnDiskStateMachine {
			return fakeDiskSM
		},
		tf: func(nh *NodeHost) {
			n, ok := nh.getShard(1)
			require.True(t, ok, "failed to get the node")
			require.False(t, n.initialized(), "node unexpectedly initialized")
			_, err := nh.StaleRead(1, nil)
			require.ErrorIs(t, err, ErrShardNotInitialized,
				"expected to return ErrShardNotInitialized")
			fakeDiskSM.SlowOpen.Store(false)
			for !n.initialized() {
				runtime.Gosched()
			}
			v, err := nh.StaleRead(1, nil)
			require.NoError(t, err, "stale read failed")
			require.Len(t, v.([]byte), 8, "unexpected result")
		},
		noElection: true,
	}
	runNodeHostTest(t, to, fs)
}

func TestStartReplicaWaitForReadiness(t *testing.T) {
	fs := vfs.GetTestFS()
	fakeDiskSM := tests.NewFakeDiskSM(0)
	fakeDiskSM.SlowOpen.Store(true)
	to := &testOption{
		defaultTestNode: false,
		noElection:      true,
		tf: func(nh *NodeHost) {
			cfg := getTestConfig()
			cfg.WaitReady = true

			go func() {
				defer fakeDiskSM.SlowOpen.Store(false)
				var n *node
				var ok bool

				for i := 0; i < 10; i++ {
					n, ok = nh.getShard(cfg.ShardID)
					if ok {
						break
					}
					time.Sleep(time.Millisecond * 100)
				}
				require.NotNil(t, n, "failed to get the node")
				require.False(t, n.initialized(), "node unexpectedly initialized")
				_, err := nh.StaleRead(1, nil)
				require.ErrorIs(t, err, ErrShardNotInitialized,
					"expected to return ErrShardNotInitialized")
			}()

			initialMembers := map[uint64]Target{
				1: nh.RaftAddress(),
			}

			err := nh.StartOnDiskReplica(initialMembers,
				false,
				func(shardID uint64, replicaID uint64) sm.IOnDiskStateMachine {
					return fakeDiskSM
				},
				*cfg)
			require.NoError(t, err, "failed to StartOnDiskReplica")

			v, err := nh.StaleRead(1, nil)
			require.NoError(t, err, "stale read failed")
			require.Len(t, v.([]byte), 8, "unexpected result")
		},
	}
	runNodeHostTest(t, to, fs)
}

func testOnDiskStateMachineCanTakeDummySnapshot(t *testing.T, compressed bool) {
	fs := vfs.GetTestFS()
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
			logdb := nh.mu.logdb
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
				snapshot, err := logdb.GetSnapshot(1, 1)
				require.NoError(t, err, "list snapshot failed")
				if !pb.IsEmptySnapshot(snapshot) {
					snapshotted = true
					ss = snapshot
					require.True(t, ss.Dummy, "dummy snapshot is not recorded as dummy")
					break
				} else if i%100 == 0 {
					time.Sleep(200 * time.Millisecond)
				}
			}
			require.True(t, snapshotted, "failed to snapshot")
			fi, err := fs.Stat(ss.Filepath)
			require.NoError(t, err, "failed to get file st")
			require.Equal(t, int64(1060), fi.Size(), "unexpected dummy snapshot size")
			reader, h, err := rsm.NewSnapshotReader(ss.Filepath, fs)
			require.NoError(t, err, "failed to read snapshot")
			assert.Equal(t, config.NoCompression, h.CompressionType,
				"dummy snapshot compressed")
			assert.Equal(t, rsm.DefaultVersion,
				rsm.SSVersion(h.Version), "unexpected snapshot version")
			require.NoError(t, reader.Close())
			shrunk, err := rsm.IsShrunkSnapshotFile(ss.Filepath, fs)
			require.NoError(t, err, "failed to check shrunk")
			assert.True(t, shrunk, "not a dummy snapshot")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestOnDiskStateMachineCanTakeDummySnapshot(t *testing.T) {
	testOnDiskStateMachineCanTakeDummySnapshot(t, true)
	testOnDiskStateMachineCanTakeDummySnapshot(t, false)
}

func TestOnDiskSMCanStreamSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		rc := config.Config{
			ShardID:                 1,
			ReplicaID:               1,
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
		require.NoError(t, nh1.StartOnDiskReplica(peers, false, newSM, rc),
			"failed to start shard")
		waitForLeaderToBeElected(t, nh1, 1)
		logdb := nh1.mu.logdb
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
			snapshot, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err, "list snapshot failed")
			if !pb.IsEmptySnapshot(snapshot) {
				snapshotted = true
				break
			} else if i%50 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
		require.True(t, snapshotted, "failed to take 3 snapshots")
		pto := pto(nh1)
		rs, err := nh1.RequestAddReplica(1, 2, nodeHostTestAddr2, 0, pto)
		require.NoError(t, err, "failed to add node")
		s := <-rs.ResultC()
		require.True(t, s.Completed(), "failed to complete the add node request")
		rc = config.Config{
			ShardID:            1,
			ReplicaID:          2,
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
		require.NoError(t, nh2.StartOnDiskReplica(nil, true, newSM2, rc),
			"failed to start shard")
		ssIndex := uint64(0)
		logdb = nh2.mu.logdb
		waitForLeaderToBeElected(t, nh2, 1)
		for i := uint64(2); i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh2.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			plog.Infof("nh2 proposal result: %v", err)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			ss, err := logdb.GetSnapshot(1, 2)
			require.NoError(t, err, "list snapshot failed")
			if !pb.IsEmptySnapshot(ss) {
				require.True(t, sm2.Recovered(), "not recovered")
				require.True(t, sm1.Aborted(), "not aborted")
				require.NotZero(t, ss.OnDiskIndex,
					"on disk index not recorded in ss")
				shrunk, err := rsm.IsShrunkSnapshotFile(ss.Filepath, fs)
				require.NoError(t, err, "failed to check whether snapshot is shrunk")
				require.True(t, shrunk, "snapshot %d is not shrunk", ss.Index)
				if ssIndex == 0 {
					ssIndex = ss.Index
				} else {
					if ssIndex != ss.Index {
						break
					}
				}
			} else if i%50 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
		require.NotZero(t, ssIndex, "failed to take 2 snapshots")
		listener, ok := nh2.events.sys.ul.(*testSysEventListener)
		require.True(t, ok, "failed to get the system event listener")
		require.NotEmpty(t, listener.getSnapshotReceived(),
			"snapshot received not notified")
		require.NotEmpty(t, listener.getSnapshotRecovered(),
			"failed to be notified for recovered snapshot")
		require.NotEmpty(t, listener.getLogCompacted(),
			"log compaction not notified")
		listener, ok = nh1.events.sys.ul.(*testSysEventListener)
		require.True(t, ok, "failed to get the system event listener")
		require.NotEmpty(t, listener.getSendSnapshotStarted(),
			"send snapshot started not notified")
		require.NotEmpty(t, listener.getSendSnapshotCompleted(),
			"send snapshot completed not notified")
		require.NotZero(t, listener.getConnectionEstablished(),
			"connection established not notified")
	}
	twoFakeDiskNodeHostTest(t, tf, fs)
}

func TestConcurrentStateMachineLookup(t *testing.T) {
	fs := vfs.GetTestFS()
	done := uint32(0)
	tf := func(t *testing.T, nh *NodeHost) {
		nhc := nh.NodeHostConfig()
		shardID := 1 + nhc.Expert.Engine.ApplyShards
		count := uint32(0)
		stopper := syncutil.NewStopper()
		pto := pto(nh)
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				session := nh.GetNoOPSession(shardID)
				_, err := nh.SyncPropose(ctx, session, []byte("test"))
				cancel()
				if err == ErrTimeout {
					continue
				}
				require.NoError(t, err, "failed to make proposal")
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
				rs, err := nh.ReadIndex(shardID, pto)
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
					atomic.AddUint32(&count, 1)
					atomic.StoreUint32(&done, 1)
					return
				}
			}
		})
		stopper.Stop()
		require.NotZero(t, atomic.LoadUint32(&done),
			"failed to have any concurrent read")
	}
	singleConcurrentNodeHostTest(t, tf, 0, true, fs)
}

func TestConcurrentStateMachineSaveSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		nhc := nh.NodeHostConfig()
		shardID := 1 + nhc.Expert.Engine.ApplyShards
		nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
		for _, ci := range nhi.ShardInfoList {
			if ci.ShardID == shardID {
				assert.Equal(t, sm.Type(sm.ConcurrentStateMachine), ci.StateMachineType,
					"unexpected state machine type")
			}
			assert.False(t, ci.IsNonVoting, "unexpected IsNonVoting value")
		}
		result := make(map[uint64]struct{})
		session := nh.GetNoOPSession(shardID)
		pto := pto(nh)
		for i := 0; i < 10000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			v, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			require.NoError(t, err, "failed to make proposal")
			result[v.Value] = struct{}{}
			if len(result) > 1 {
				return
			}
			time.Sleep(time.Millisecond)
		}
		require.FailNow(t, "failed to make proposal when saving snapshots")
	}
	singleConcurrentNodeHostTest(t, tf, 10, true, fs)
}

func TestErrorCanBeReturnedWhenLookingUpConcurrentStateMachine(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh *NodeHost) {
		nhc := nh.NodeHostConfig()
		shardID := 1 + nhc.Expert.Engine.ApplyShards
		for i := 0; i < 100; i++ {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh.SyncRead(ctx, shardID, []byte("test"))
			cancel()
			require.ErrorIs(t, err, sm.ErrSnapshotStopped, "error not returned")
		}
	}
	singleConcurrentNodeHostTest(t, tf, 10, true, fs)
}

func TestRegularStateMachineDoesNotAllowConucrrentUpdate(t *testing.T) {
	fs := vfs.GetTestFS()
	failed := uint32(0)
	tf := func(t *testing.T, nh *NodeHost) {
		nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
		for _, ci := range nhi.ShardInfoList {
			if ci.ShardID == 1 {
				assert.Equal(t, sm.Type(sm.RegularStateMachine), ci.StateMachineType,
					"unexpected state machine type")
			}
			assert.False(t, ci.IsNonVoting, "unexpected IsNonVoting value")
		}
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
					atomic.StoreUint32(&failed, 1)
					return
				}
			}
		})
		stopper.Stop()
		require.Zero(t, atomic.LoadUint32(&failed),
			"unexpected concurrent update observed")
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
			require.LessOrEqual(t, len(result), 1,
				"unexpected concurrent save snapshot observed")
		}
	}
	singleConcurrentNodeHostTest(t, tf, 10, false, fs)
}

func TestLogDBRateLimit(t *testing.T) {
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
			logDBConfig.KVWriteBufferSize = 1024 * 8
			c.Expert.LogDB = logDBConfig
			return c
		},
		tf: func(nh *NodeHost) {
			if nh.mu.logdb.Name() == "Tan" {
				t.Skip("skipped, using tan logdb")
			}
			rateLimited := false
			for i := 0; i < 10240; i++ {
				pto := pto(nh)
				session := nh.GetNoOPSession(1)
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nh.SyncPropose(ctx, session, make([]byte, 512))
				cancel()
				if err == ErrSystemBusy {
					rateLimited = true
					break
				}
			}
			require.True(t, rateLimited, "failed to return ErrSystemBusy")
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
			require.Equal(t, ErrPayloadTooBig, err,
				"failed to return ErrPayloadTooBig")
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
				require.NoError(t, err, "failed to make proposal %v", err)
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
			require.Equal(t, uint32(1), atomic.LoadUint32(&limited),
				"failed to observe ErrSystemBusy")
			require.True(t, makeTestProposal(nh, 10000),
				"failed to make proposal again")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRateLimitCanUseFollowerFeedback(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost,
		n1 *tests.NoOP, n2 *tests.NoOP) {
		session := nh1.GetNoOPSession(1)
		limited := false
		for i := 0; i < 2000; i++ {
			pto := pto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			_, err := nh1.SyncPropose(ctx, session, make([]byte, 1024))
			cancel()
			if err == ErrShardNotReady {
				time.Sleep(20 * time.Millisecond)
			} else if err == ErrSystemBusy {
				limited = true
				break
			}
		}
		require.True(t, limited, "failed to observe rate limited")

		n1.SetSleepTime(0)
		n2.SetSleepTime(0)
		require.True(t, makeTestProposal(nh1, 2000),
			"failed to make proposal again")
		plog.Infof("rate limit lifted, all good")
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
			_, err := rand.Read(cmd)
			require.NoError(t, err)
			result, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			assert.NoError(t, err, "failed to make proposal %v", err)
			assert.Equal(t, uint64(1518), result.Value, "unexpected result value")
			assert.Equal(t, cmd, result.Data, "unexpected result data")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRaftLogQuery(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			for i := 0; i < 10; i++ {
				makeTestProposal(nh, 10)
			}
			_, err := nh.QueryRaftLog(1, 2, 1, math.MaxUint64)
			assert.Equal(t, ErrInvalidRange, err)

			rs, err := nh.QueryRaftLog(1, 1, 11, math.MaxUint64)
			assert.NoError(t, err)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			select {
			case v := <-rs.CompletedC:
				assert.True(t, v.Completed())
				entries, logRange := v.RaftLogs()
				assert.Equal(t, 10, len(entries))
				assert.Equal(t, LogRange{FirstIndex: 1, LastIndex: 13}, logRange)
			case <-ticker.C:
				require.Fail(t, "no results")
			}
			rs.Release()

			rs, err = nh.QueryRaftLog(1, 1, 1000, math.MaxUint64)
			assert.NoError(t, err)
			select {
			case v := <-rs.CompletedC:
				assert.True(t, v.Completed())
				entries, logRange := v.RaftLogs()
				assert.Equal(t, 12, len(entries))
				assert.Equal(t, LogRange{FirstIndex: 1, LastIndex: 13}, logRange)
			case <-ticker.C:
				require.Fail(t, "no results")
			}
			rs.Release()

			rs, err = nh.QueryRaftLog(1, 13, 1000, math.MaxUint64)
			assert.NoError(t, err)
			select {
			case v := <-rs.CompletedC:
				assert.True(t, v.RequestOutOfRange())
				entries, logRange := v.RaftLogs()
				assert.Equal(t, 0, len(entries))
				assert.Equal(t, LogRange{FirstIndex: 1, LastIndex: 13}, logRange)
			case <-ticker.C:
				require.Fail(t, "no results")
			}
			rs.Release()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opts := SnapshotOption{
				CompactionIndex:            10,
				OverrideCompactionOverhead: true,
			}
			_, err = nh.SyncRequestSnapshot(ctx, 1, opts)
			assert.NoError(t, err)
			done := false
			for i := 0; i < 1000; i++ {
				func() {
					rs, err := nh.QueryRaftLog(1, 1, 11, math.MaxUint64)
					assert.NoError(t, err)
					ticker := time.NewTicker(2 * time.Second)
					defer ticker.Stop()
					select {
					case v := <-rs.CompletedC:
						if v.Completed() {
							time.Sleep(10 * time.Millisecond)
							return
						}
						assert.True(t, v.RequestOutOfRange())
						entries, logRange := v.RaftLogs()
						assert.Equal(t, 0, len(entries))
						assert.Equal(t, LogRange{FirstIndex: 11, LastIndex: 13}, logRange)
						done = true
						return
					case <-ticker.C:
						require.Fail(t, "no results")
					}
					rs.Release()
				}()
				if done {
					return
				}
			}
			require.True(t, done, "failed to observe RequestOutOfRange error")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestIsNonVotingIsReturnedWhenNodeIsNonVoting(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nh1 *NodeHost, nh2 *NodeHost) {
		rc := config.Config{
			ShardID:                 1,
			ReplicaID:               1,
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
		err := nh1.StartOnDiskReplica(peers, false, newSM, rc)
		require.NoError(t, err, "failed to start nonVoting %v", err)
		waitForLeaderToBeElected(t, nh1, 1)

		rc = config.Config{
			ShardID:            1,
			ReplicaID:          2,
			ElectionRTT:        3,
			HeartbeatRTT:       1,
			IsNonVoting:        true,
			CheckQuorum:        true,
			SnapshotEntries:    5,
			CompactionOverhead: 2,
		}
		newSM2 := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		pto := pto(nh1)
		rs, err := nh1.RequestAddNonVoting(1, 2, nodeHostTestAddr2, 0, pto)
		require.NoError(t, err, "failed to add nonVoting %v", err)
		<-rs.ResultC()

		err = nh2.StartOnDiskReplica(nil, true, newSM2, rc)
		require.NoError(t, err, "failed to start nonVoting %v", err)

		nonVotingReady := false
		for i := 0; i < 10000; i++ {
			nhi := nh2.GetNodeHostInfo(DefaultNodeHostInfoOption)
			for _, ci := range nhi.ShardInfoList {
				if ci.Pending {
					continue
				}
				if ci.IsNonVoting && ci.ReplicaID == 2 {
					nonVotingReady = true
					break
				}
			}
			if nonVotingReady {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.True(t, nonVotingReady, "failed to get is nonVoting flag")
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
			require.NoError(t, err, "propose failed %v", err)

			v := <-rs.ResultC()
			require.Panics(t, func() {
				plog.Infof("%d", v.SnapshotIndex())
			})
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
			require.NoError(t, err, "failed to make proposal %v", err)

			ctx, cancel = context.WithTimeout(context.Background(), pto)
			idx, err := nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			cancel()
			require.NoError(t, err)
			require.NotZero(t, idx, "unexpected index %d", idx)

			listener, ok := nh.events.sys.ul.(*testSysEventListener)
			require.True(t, ok, "failed to get the system event listener")
			waitSnapshotInfoEvent(t, listener.getSnapshotCreated, 1)
			si := listener.getSnapshotCreated()[0]
			assert.Equal(t, uint64(1), si.ShardID, "incorrect shard id")
			assert.Equal(t, uint64(1), si.ReplicaID, "incorrect replica id")
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
			require.NoError(t, err, "failed to make proposal %v", err)

			ctx, cancel = context.WithTimeout(context.Background(), pto)
			idx, err := nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			cancel()
			require.NoError(t, err)
			require.NotZero(t, idx, "unexpected index %d", idx)

			sspath := "exported_snapshot_safe_to_delete"
			require.NoError(t, fs.RemoveAll(sspath))
			require.NoError(t, fs.MkdirAll(sspath, 0755))
			defer func() {
				require.NoError(t, fs.RemoveAll(sspath))
			}()

			opt := SnapshotOption{
				Exported:   true,
				ExportPath: sspath,
			}
			ctx, cancel = context.WithTimeout(context.Background(), pto)
			exportIdx, err := nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			require.NoError(t, err)
			require.Equal(t, idx, exportIdx,
				"unexpected index %d, want %d", exportIdx, idx)
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
					if err == ErrTimeout {
						time.Sleep(500 * time.Millisecond)
						continue
					}
					require.NoError(t, err, "failed to make proposal %v", err)
				}
			}

			opt := SnapshotOption{
				OverrideCompactionOverhead: true,
				CompactionOverhead:         0,
			}
			lpto := lpto(nh)
			sr, err := nh.RequestSnapshot(1, opt, lpto)
			require.NoError(t, err, "failed to request snapshot")

			v := <-sr.ResultC()
			require.True(t, v.Completed(), "failed to complete the requested snapshot")
			require.GreaterOrEqual(t, v.SnapshotIndex(), uint64(16),
				"unexpected snapshot index %d", v.SnapshotIndex())

			logdb := nh.mu.logdb
			compacted := false
			for i := 0; i < 1000; i++ {
				time.Sleep(10 * time.Millisecond)
				op, err := nh.RequestCompaction(1, 1)
				if err == nil {
					<-op.ResultC()
				}
				ents, _, err := logdb.IterateEntries(nil, 0, 1, 1, 12, 14, math.MaxUint64)
				require.NoError(t, err, "failed to iterate entries, %v", err)
				if len(ents) == 0 {
					compacted = true
					break
				}
			}
			require.True(t, compacted, "failed to compact the entries")
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
			require.NoError(t, err, "failed to make proposal %v", err)

			sr, err := nh.RequestSnapshot(1, SnapshotOption{}, pto)
			require.NoError(t, err, "failed to request snapshot")

			var index uint64
			v := <-sr.ResultC()
			require.True(t, v.Completed(),
				"failed to complete the requested snapshot")
			index = v.SnapshotIndex()

			sr, err = nh.RequestSnapshot(1, SnapshotOption{}, pto)
			require.NoError(t, err, "failed to request snapshot")
			v = <-sr.ResultC()
			require.True(t, v.Rejected(), "snapshot request not rejected")

			logdb := nh.mu.logdb
			snapshot, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err)
			require.False(t, pb.IsEmptySnapshot(snapshot),
				"failed to save snapshots")
			require.Equal(t, index, snapshot.Index, "unexpected index value")

			reader, header, err := rsm.NewSnapshotReader(snapshot.Filepath, fs)
			require.NoError(t, err, "failed to new snapshot reader %v", err)
			defer func() {
				require.NoError(t, reader.Close())
			}()
			require.Equal(t, rsm.V2, rsm.SSVersion(header.Version),
				"unexpected snapshot version")
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
			rs, err := nh.RequestAddReplica(1, 2, "localhost:3456", 0, pto)
			require.NoError(t, err, "failed to request add node")
			require.NotNil(t, rs.committedC, "committedC not set")

			cn := <-rs.ResultC()
			require.True(t, cn.Committed(), "failed to get committed notification")

			cn = <-rs.ResultC()
			require.True(t, cn.Completed(), "failed to get completed notification")
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
			require.NoError(t, err, "failed to make proposal %v", err)
			require.NotNil(t, rs.committedC, "committedC not set")

			cn := <-rs.ResultC()
			require.True(t, cn.Committed(), "failed to get committed notification")

			cn = <-rs.ResultC()
			require.True(t, cn.Completed(), "failed to get completed notification")
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
			require.NoError(t, err, "failed to request snapshot")
			v := <-sr.ResultC()
			require.True(t, v.Timeout(), "failed to report timeout")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSyncRemoveData(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			err := nh.StopShard(1)
			require.NoError(t, err, "failed to remove shard %v", err)

			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err = nh.SyncRemoveData(ctx, 1, 1)
			require.NoError(t, err, "sync remove data failed: %v", err)

			listener, ok := nh.events.sys.ul.(*testSysEventListener)
			require.True(t, ok, "failed to get the system event listener")
			waitNodeInfoEvent(t, listener.getNodeUnloaded, 1)
			ni := listener.getNodeUnloaded()[0]
			assert.Equal(t, uint64(1), ni.ShardID, "incorrect shard id")
			assert.Equal(t, uint64(1), ni.ReplicaID, "incorrect replica id")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRemoveNodeDataWillFailWhenNodeIsStillRunning(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			err := nh.RemoveData(1, 1)
			require.Equal(t, ErrShardNotStopped, err, "remove data didn't fail")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestRestartingAnNodeWithRemovedDataWillBeRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			err := nh.StopShard(1)
			require.NoError(t, err, "failed to remove shard %v", err)
			for {
				err := nh.RemoveData(1, 1)
				if err != nil {
					if err == ErrShardNotStopped {
						time.Sleep(100 * time.Millisecond)
						continue
					} else {
						require.NoError(t, err, "remove data failed %v", err)
					}
				}
				break
			}

			rc := getTestConfig()
			peers := make(map[uint64]string)
			peers[1] = nh.RaftAddress()
			newPST := func(shardID uint64, replicaID uint64) sm.IStateMachine {
				return &PST{}
			}
			err = nh.StartReplica(peers, false, newPST, *rc)
			require.Equal(t, ErrReplicaRemoved, err, "start shard failed %v", err)
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
			require.NoError(t, err, "failed to make proposal %v", err)

			sr, err := nh.RequestSnapshot(1, SnapshotOption{}, pto)
			require.NoError(t, err, "failed to request snapshot")
			v := <-sr.ResultC()
			require.True(t, v.Completed(), "failed to complete the snapshot")

			require.NoError(t, nh.StopShard(1), "failed to stop shard %v", err)

			logdb := nh.mu.logdb
			snapshot, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err)
			require.False(t, pb.IsEmptySnapshot(snapshot), "no snapshot saved")

			snapshotDir := nh.env.GetSnapshotDir(nh.nhConfig.GetDeploymentID(), 1, 1)
			exist, err := fileutil.Exist(snapshotDir, fs)
			require.NoError(t, err)
			require.True(t, exist, "snapshot dir %s does not exist", snapshotDir)

			files, err := fs.List(snapshotDir)
			require.NoError(t, err, "failed to read dir %v", err)
			sscount := 0
			for _, fn := range files {
				fi, err := fs.Stat(fs.PathJoin(snapshotDir, fn))
				require.NoError(t, err, "failed to get stat for %s", fn)
				if fi.IsDir() && server.SnapshotDirNameRe.Match([]byte(fi.Name())) {
					sscount++
				}
			}
			require.NotZero(t, sscount, "no snapshot dir found")

			removed := false
			for i := 0; i < 1000; i++ {
				err := nh.RemoveData(1, 1)
				if err == ErrShardNotStopped {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				require.NoError(t, err, "failed to remove data %v", err)
				removed = true
				break
			}
			require.True(t, removed, "failed to remove node data")

			marked, err := fileutil.IsDirMarkedAsDeleted(snapshotDir, fs)
			require.NoError(t, err)
			require.True(t, marked, "snapshot dir %s still exist", snapshotDir)

			files, err = fs.List(snapshotDir)
			require.NoError(t, err, "failed to read dir %v", err)
			for _, fn := range files {
				fi, err := fs.Stat(fs.PathJoin(snapshotDir, fn))
				require.NoError(t, err, "failed to get stat for %s", fn)
				if fi.IsDir() {
					match := server.SnapshotDirNameRe.Match([]byte(fi.Name()))
					require.False(t, match, "snapshot dir %s not deleted", fi.Name())
				}
			}

			bs, err := logdb.GetBootstrapInfo(1, 1)
			require.ErrorIs(t, err, raftio.ErrNoBootstrapInfo,
				"failed to delete bootstrap %v", err)
			require.Equal(t, pb.Bootstrap{}, bs, "bs not nil")

			ents, sz, err := logdb.IterateEntries(nil, 0, 1, 1, 0,
				math.MaxUint64, math.MaxUint64)
			require.NoError(t, err, "failed to get entries %v", err)
			require.Empty(t, ents, "entry returned")
			require.Zero(t, sz, "size not zero")

			snapshot, err = logdb.GetSnapshot(1, 1)
			require.NoError(t, err)
			require.True(t, pb.IsEmptySnapshot(snapshot), "snapshot not deleted")

			_, err = logdb.ReadRaftState(1, 1, 1)
			require.ErrorIs(t, err, raftio.ErrNoSavedLog,
				"raft state not deleted %v", err)

			sysop, err := nh.RequestCompaction(1, 1)
			require.NoError(t, err, "failed to request compaction %v", err)
			<-sysop.ResultC()
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestSnapshotOptionIsChecked(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			opts := SnapshotOption{
				OverrideCompactionOverhead: true,
				CompactionIndex:            100,
				CompactionOverhead:         10,
			}
			assert.Equal(t, ErrInvalidOption, opts.Validate())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := nh.SyncRequestSnapshot(ctx, 1, opts)
			assert.Equal(t, ErrInvalidOption, err)
			rs, err := nh.RequestSnapshot(1, opts, time.Second)
			assert.Equal(t, ErrInvalidOption, err)
			assert.Nil(t, rs)
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
			require.NoError(t, fs.RemoveAll(sspath))
			require.NoError(t, fs.MkdirAll(sspath, 0755))
			defer func() {
				require.NoError(t, fs.RemoveAll(sspath))
			}()

			session := nh.GetNoOPSession(1)
			pto := lpto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			cmd := make([]byte, 1518)
			_, err := nh.SyncPropose(ctx, session, cmd)
			cancel()
			require.NoError(t, err, "failed to make proposal %v", err)

			opt := SnapshotOption{
				Exported:   true,
				ExportPath: sspath,
			}
			sr, err := nh.RequestSnapshot(1, opt, pto)
			require.NoError(t, err, "failed to request snapshot")
			v := <-sr.ResultC()
			require.True(t, v.Completed(), "failed to complete the snapshot")
			index := v.SnapshotIndex()

			logdb := nh.mu.logdb
			snapshot, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err)
			require.True(t, pb.IsEmptySnapshot(snapshot),
				"snapshot record unexpectedly inserted into the system")

			snapshotDir := fmt.Sprintf("snapshot-%016X", index)
			snapshotFile := fmt.Sprintf("snapshot-%016X.gbsnap", index)
			fp := fs.PathJoin(sspath, snapshotDir, snapshotFile)
			exist, err := fileutil.Exist(fp, fs)
			require.NoError(t, err)
			require.True(t, exist, "snapshot file not saved")

			metafp := fs.PathJoin(sspath, snapshotDir, "snapshot.metadata")
			exist, err = fileutil.Exist(metafp, fs)
			require.NoError(t, err)
			require.True(t, exist, "snapshot metadata not saved")

			var ss pb.Snapshot
			err = fileutil.GetFlagFileContent(fs.PathJoin(sspath, snapshotDir),
				"snapshot.metadata", &ss, fs)
			require.NoError(t, err, "failed to get snapshot from metadata")
			assert.Zero(t, ss.OnDiskIndex, "on disk index is not 0")
			assert.False(t, ss.Imported, "incorrectly recorded as imported")
			assert.Equal(t, pb.RegularStateMachine, ss.Type, "incorrect type")
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
			require.True(t, proposed, "failed to make proposal")

			sspath := "exported_snapshot_safe_to_delete"
			require.NoError(t, fs.RemoveAll(sspath))
			require.NoError(t, fs.MkdirAll(sspath, 0755))
			defer func() {
				require.NoError(t, fs.RemoveAll(sspath))
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
				require.NoError(t, err, "failed to request snapshot %v", err)
				v := <-sr.ResultC()
				if v.Aborted() {
					aborted = true
					continue
				}
				if v.code == requestRejected {
					continue
				}
				require.True(t, v.Completed(),
					"failed to complete snapshot, %s", v.code)
				index = v.SnapshotIndex()
				break
			}
			require.True(t, aborted, "never aborted")

			logdb := nh.mu.logdb
			snapshot, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err)
			require.True(t, pb.IsEmptySnapshot(snapshot), "snapshot inserted")

			snapshotDir := fmt.Sprintf("snapshot-%016X", index)
			snapshotFile := fmt.Sprintf("snapshot-%016X.gbsnap", index)
			fp := fs.PathJoin(sspath, snapshotDir, snapshotFile)
			exist, err := fileutil.Exist(fp, fs)
			require.NoError(t, err)
			require.True(t, exist, "snapshot file not saved")

			metafp := fs.PathJoin(sspath, snapshotDir, "snapshot.metadata")
			exist, err = fileutil.Exist(metafp, fs)
			require.NoError(t, err)
			require.True(t, exist, "snapshot metadata not saved")

			shrunk, err := rsm.IsShrunkSnapshotFile(fp, fs)
			require.NoError(t, err)
			require.False(t, shrunk, "exported snapshot is considered as shrunk")

			var ss pb.Snapshot
			err = fileutil.GetFlagFileContent(fs.PathJoin(sspath, snapshotDir),
				"snapshot.metadata", &ss, fs)
			require.NoError(t, err, "failed to get snapshot from metadata")
			assert.NotZero(t, ss.OnDiskIndex, "on disk index not recorded")
			assert.False(t, ss.Imported, "incorrectly recorded as imported")
			assert.Equal(t, pb.OnDiskStateMachine, ss.Type, "incorrect type")
		},
	}
	runNodeHostTest(t, to, fs)
}

func testImportedSnapshotIsAlwaysRestored(t *testing.T,
	newDir bool, ct config.CompressionType, fs vfs.IFS) {
	tf := func() {
		rc := config.Config{
			ShardID:                 1,
			ReplicaID:               1,
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
			Expert:         getTestExpertConfig(fs),
		}
		nh, err := NewNodeHost(nhc)
		require.NoError(t, err, "failed to create node host %v", err)
		pto := lpto(nh)
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewSimDiskSM(0)
		}
		err = nh.StartOnDiskReplica(peers, false, newSM, rc)
		require.NoError(t, err, "failed to start shard %v", err)
		waitForLeaderToBeElected(t, nh, 1)

		makeProposals := func(nn *NodeHost) {
			session := nn.GetNoOPSession(1)
			for i := 0; i < 16; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), pto)
				_, err := nn.SyncPropose(ctx, session, []byte("test-data"))
				cancel()
				require.NoError(t, err, "failed to make proposal %v", err)
			}
		}
		makeProposals(nh)

		sspath := "exported_snapshot_safe_to_delete"
		require.NoError(t, fs.RemoveAll(sspath))
		require.NoError(t, fs.MkdirAll(sspath, 0755))
		defer func() {
			require.NoError(t, fs.RemoveAll(sspath))
		}()

		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		var index uint64
		exported := false
		for i := 0; i < 1000; i++ {
			sr, err := nh.RequestSnapshot(1, opt, pto)
			require.NoError(t, err, "failed to request snapshot %v", err)
			v := <-sr.ResultC()
			if v.Rejected() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if v.Completed() {
				index = v.SnapshotIndex()
				exported = true
				break
			}
		}
		require.True(t, exported, "failed to export snapshot")
		makeProposals(nh)

		ctx, cancel := context.WithTimeout(context.Background(), pto)
		rv, err := nh.SyncRead(ctx, 1, nil)
		cancel()
		require.NoError(t, err, "failed to read applied value %v", err)
		applied := rv.(uint64)
		require.Greater(t, applied, index, "invalid applied value %d", applied)

		ctx, cancel = context.WithTimeout(context.Background(), pto)
		err = nh.SyncRequestAddReplica(ctx, 1, 2, "noidea:8080", 0)
		require.NoError(t, err, "failed to add node %v", err)
		nh.Close()

		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		dir := fs.PathJoin(sspath, snapshotDir)
		members := make(map[uint64]string)
		members[1] = nhc.RaftAddress
		if newDir {
			nhc.NodeHostDir = fs.PathJoin(nhc.NodeHostDir, "newdir")
		}
		err = tools.ImportSnapshot(nhc, dir, members, 1)
		require.NoError(t, err, "failed to import snapshot %v", err)

		ok, err := upgrade310.CanUpgradeToV310(nhc)
		require.NoError(t, err, "failed to check whether upgrade is possible")
		require.False(t, ok, "should not be considered as ok to upgrade")

		func() {
			rnh, err := NewNodeHost(nhc)
			require.NoError(t, err, "failed to create node host %v", err)
			defer rnh.Close()
			rnewSM := func(uint64, uint64) sm.IOnDiskStateMachine {
				return tests.NewSimDiskSM(applied)
			}
			err = rnh.StartOnDiskReplica(nil, false, rnewSM, rc)
			require.NoError(t, err, "failed to start shard %v", err)
			waitForLeaderToBeElected(t, rnh, 1)

			ctx, cancel = context.WithTimeout(context.Background(), pto)
			rv, err = rnh.SyncRead(ctx, 1, nil)
			cancel()
			require.NoError(t, err, "failed to read applied value %v", err)
			require.Equal(t, index, rv.(uint64), "invalid returned value %d", rv)
			makeProposals(rnh)
		}()

		ok, err = upgrade310.CanUpgradeToV310(nhc)
		require.NoError(t, err, "failed to check whether upgrade is possible")
		require.True(t, ok, "can not upgrade")
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

func TestShardWithoutQuorumCanBeRestoreByImportingSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
		nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
		rc := config.Config{
			ShardID:            1,
			ReplicaID:          1,
			ElectionRTT:        10,
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
			Expert:         getTestExpertConfig(fs),
		}
		nhc2 := config.NodeHostConfig{
			WALDir:         nh2dir,
			NodeHostDir:    nh2dir,
			RTTMillisecond: getRTTMillisecond(fs, nh2dir),
			RaftAddress:    nodeHostTestAddr2,
			Expert:         getTestExpertConfig(fs),
		}
		var once sync.Once
		nh1, err := NewNodeHost(nhc1)
		require.NoError(t, err, "failed to create node host %v", err)
		nh2, err := NewNodeHost(nhc2)
		require.NoError(t, err, "failed to create node host %v", err)

		sm1 := tests.NewFakeDiskSM(0)
		sm1.SetAborted()
		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return sm1
		}
		newSM2 := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewFakeDiskSM(0)
		}
		err = nh1.StartOnDiskReplica(peers, false, newSM, rc)
		require.NoError(t, err, "failed to start shard %v", err)
		waitForLeaderToBeElected(t, nh1, 1)

		defer func() {
			require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
		}()
		defer once.Do(func() {
			nh1.Close()
			nh2.Close()
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
			require.True(t, done, "failed to make proposal on restored shard")
		}
		mkproposal(nh1)

		sspath := "exported_snapshot_safe_to_delete"
		require.NoError(t, fs.RemoveAll(sspath))
		require.NoError(t, fs.MkdirAll(sspath, 0755))
		defer func() {
			require.NoError(t, fs.RemoveAll(sspath))
		}()

		opt := SnapshotOption{
			Exported:   true,
			ExportPath: sspath,
		}
		pto := lpto(nh1)
		sr, err := nh1.RequestSnapshot(1, opt, pto)
		require.NoError(t, err, "failed to request snapshot %v", err)
		v := <-sr.ResultC()
		require.True(t, v.Completed(), "failed to complete snapshot")
		index := v.SnapshotIndex()

		snapshotDir := fmt.Sprintf("snapshot-%016X", index)
		dir := fs.PathJoin(sspath, snapshotDir)
		members := make(map[uint64]string)
		members[1] = nhc1.RaftAddress
		members[10] = nhc2.RaftAddress
		once.Do(func() {
			nh1.Close()
			nh2.Close()
		})
		require.NoError(t, tools.ImportSnapshot(nhc1, dir, members, 1))
		require.NoError(t, tools.ImportSnapshot(nhc2, dir, members, 10))

		rnh1, err := NewNodeHost(nhc1)
		require.NoError(t, err, "failed to create node host %v", err)
		rnh2, err := NewNodeHost(nhc2)
		require.NoError(t, err, "failed to create node host %v", err)
		defer func() {
			rnh1.Close()
			rnh2.Close()
		}()

		require.NoError(t, rnh1.StartOnDiskReplica(nil, false, newSM, rc))
		rc.ReplicaID = 10
		require.NoError(t, rnh2.StartOnDiskReplica(nil, false, newSM2, rc))
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

func (c *chunks) confirm(shardID uint64, replicaID uint64, index uint64) {
	c.confirmed++
}

func (c *chunks) getSnapshotDirFunc(shardID uint64, replicaID uint64) string {
	return testSnapshotDir
}

type testSink2 struct {
	receiver chunkReceiver
}

func (s *testSink2) Receive(chunk pb.Chunk) (bool, bool) {
	s.receiver.Add(chunk)
	return true, false
}

func (s *testSink2) Close() error {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
	return nil
}

func (s *testSink2) ShardID() uint64 {
	return 2000
}

func (s *testSink2) ToReplicaID() uint64 {
	return 300
}

type dataCorruptionSink struct {
	receiver chunkReceiver
	enabled  bool
}

func (s *dataCorruptionSink) Receive(chunk pb.Chunk) (bool, bool) {
	if s.enabled && len(chunk.Data) > 0 {
		idx := mathrand.Uint64() % uint64(len(chunk.Data))
		chunk.Data[idx] = chunk.Data[idx] + 1
	}
	s.receiver.Add(chunk)
	return true, false
}

func (s *dataCorruptionSink) Close() error {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
	return nil
}

func (s *dataCorruptionSink) ShardID() uint64 {
	return 2000
}

func (s *dataCorruptionSink) ToReplicaID() uint64 {
	return 300
}

type chunkReceiver interface {
	Add(chunk pb.Chunk) bool
}

func getTestSSMeta() rsm.SSMeta {
	return rsm.SSMeta{
		Index: 1000,
		Term:  5,
		From:  150,
	}
}

func testCorruptedChunkWriterOutputCanBeHandledByChunk(t *testing.T,
	enabled bool, exp uint64, fs vfs.IFS) {
	require.NoError(t, fs.RemoveAll(testSnapshotDir))
	c := &chunks{}
	dir := c.getSnapshotDirFunc(0, 0)
	require.NoError(t, fs.MkdirAll(dir, 0755))

	cks := transport.NewChunk(c.onReceive, c.confirm, c.getSnapshotDirFunc, 0, fs)
	sink := &dataCorruptionSink{receiver: cks, enabled: enabled}
	meta := getTestSSMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	defer func() {
		require.NoError(t, fs.RemoveAll(testSnapshotDir))
	}()

	for i := 0; i < 10; i++ {
		data := make([]byte, rsm.ChunkSize)
		_, err := rand.Read(data)
		require.NoError(t, err)
		_, err = cw.Write(data)
		require.NoError(t, err, "failed to write the data %v", err)
	}

	require.NoError(t, cw.Close())
	assert.Equal(t, exp, c.received,
		"unexpected received count: %d, want %d", c.received, exp)
	assert.Equal(t, exp, c.confirmed,
		"unexpected confirmed count: %d, want %d", c.confirmed, exp)
}

func TestCorruptedChunkWriterOutputCanBeHandledByChunk(t *testing.T) {
	fs := vfs.GetTestFS()
	testCorruptedChunkWriterOutputCanBeHandledByChunk(t, false, 1, fs)
	testCorruptedChunkWriterOutputCanBeHandledByChunk(t, true, 0, fs)
}

func TestChunkWriterOutputCanBeHandledByChunk(t *testing.T) {
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(testSnapshotDir))
	c := &chunks{}
	dir := c.getSnapshotDirFunc(0, 0)
	require.NoError(t, fs.MkdirAll(dir, 0755))
	defer func() {
		require.NoError(t, fs.RemoveAll(testSnapshotDir))
	}()

	cks := transport.NewChunk(c.onReceive, c.confirm, c.getSnapshotDirFunc, 0, fs)
	sink := &testSink2{receiver: cks}
	meta := getTestSSMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	require.NoError(t, err, "write failed %v", err)

	payload := make([]byte, 0)
	payload = append(payload, rsm.GetEmptyLRUSession()...)
	for i := 0; i < 10; i++ {
		data := make([]byte, rsm.ChunkSize)
		_, err := rand.Read(data)
		require.NoError(t, err)
		payload = append(payload, data...)
		_, err = cw.Write(data)
		require.NoError(t, err, "failed to write the data %v", err)
	}
	require.NoError(t, cw.Close(), "failed to flush %v", err)

	require.Equal(t, uint64(1), c.received, "failed to receive the snapshot")
	require.Equal(t, uint64(1), c.confirmed, "failed to confirm")

	fp := fs.PathJoin(testSnapshotDir,
		"snapshot-00000000000003E8", "snapshot-00000000000003E8.gbsnap")
	reader, _, err := rsm.NewSnapshotReader(fp, fs)
	require.NoError(t, err, "failed to get a snapshot reader %v", err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

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
		require.NoError(t, err)
	}
	require.Equal(t, payload, got, "snapshot content changed")
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
	ldb := &noopLogDB{}
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.Expert.LogDBFactory = &testLogDBFactory{ldb: ldb}
			return c
		},
		at: func(*NodeHost) {
			nhc := getTestNodeHostConfig(fs)
			_, err := NewNodeHost(*nhc)
			require.Equal(t, server.ErrLogDBType, err,
				"didn't report logdb type error %v", err)
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
			Expert:         getTestExpertConfig(fs),
		}
		if !child {
			nh, err := NewNodeHost(nhc)
			require.NoError(t, err, "failed to create nodehost %v", err)
			defer nh.Close()
			out, err := spawn(os.Args[0])
			require.Error(t, err, "file lock didn't prevent start, %s", out)
			require.Contains(t, string(out), "failed to lock data directory",
				"unexpected output: %s", out)
		} else {
			nhc.RaftAddress = nodeHostTestAddr2
			cnh, err := NewNodeHost(nhc)
			require.NotEqual(t, server.ErrLockDirectory, err)
			if err == nil {
				defer cnh.Close()
			}
		}
	}
	runNodeHostTestDC(t, tf, !*spawnChild, fs)
}

func TestChangeNodeHostID(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func() {
		nhc := config.NodeHostConfig{
			NodeHostDir:    singleNodeHostTestDir,
			RTTMillisecond: getRTTMillisecond(fs, singleNodeHostTestDir),
			RaftAddress:    nodeHostTestAddr1,
			Expert:         getTestExpertConfig(fs),
		}
		nh, err := NewNodeHost(nhc)
		require.NoError(t, err, "failed to create nodehost %v", err)
		nh.Close()

		v := id.New()
		nhc.NodeHostID = v.String()
		_, err = NewNodeHost(nhc)
		require.ErrorIs(t, err, server.ErrNodeHostIDChanged,
			"failed to reject changed NodeHostID %v", err)
	}
	runNodeHostTestDC(t, tf, !*spawnChild, fs)
}

type testLogDBFactory2 struct {
	f func(config.NodeHostConfig,
		config.LogDBCallback, []string, []string) (raftio.ILogDB, error)
	name string
}

func (t *testLogDBFactory2) Create(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	return t.f(cfg, cb, dirs, wals)
}

func (t *testLogDBFactory2) Name() string {
	return t.name
}

func TestNodeHostReturnsErrLogDBBrokenChangeWhenLogDBTypeChanges(t *testing.T) {
	fs := vfs.GetTestFS()
	bff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs)
	}
	to := &testOption{
		at: func(*NodeHost) {
			nhc := getTestNodeHostConfig(fs)
			nhc.Expert.LogDBFactory = &testLogDBFactory2{f: nff}
			_, err := NewNodeHost(*nhc)
			require.Equal(t, server.ErrLogDBBrokenChange, err,
				"failed to return ErrLogDBBrokenChange")
		},
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.Expert.LogDBFactory = &testLogDBFactory2{f: bff}
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
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs)
	}
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.Expert.LogDBFactory = &testLogDBFactory2{f: nff}
			return c
		},
		noElection: true,
		at: func(*NodeHost) {
			nhc := getTestNodeHostConfig(fs)
			nhc.Expert.LogDBFactory = &testLogDBFactory2{f: bff}
			_, err := NewNodeHost(*nhc)
			require.Equal(t, server.ErrIncompatibleData, err,
				"failed to return server.ErrIncompatibleData")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostByDefaultChecksWhetherToUseBatchedLogDB(t *testing.T) {
	fs := vfs.GetTestFS()
	bff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultBatchedLogDB(config, cb, dirs, lldirs)
	}
	nff := func(config config.NodeHostConfig, cb config.LogDBCallback,
		dirs []string, lldirs []string) (raftio.ILogDB, error) {
		return logdb.NewDefaultLogDB(config, cb, dirs, lldirs)
	}
	to := &testOption{
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.Expert.LogDBFactory = &testLogDBFactory2{f: bff}
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
			require.NoError(t, err, "failed to make proposal %v", err)
		},
		at: func(*NodeHost) {
			nhc := getTestNodeHostConfig(fs)
			nhc.Expert.LogDBFactory = &testLogDBFactory2{f: nff}
			nh, err := NewNodeHost(*nhc)
			require.NoError(t, err, "failed to create node host")
			if nh != nil {
				nh.Close()
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeHostWithUnexpectedDeploymentIDWillBeDetected(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		noElection: true,
		at: func(*NodeHost) {
			nhc := getTestNodeHostConfig(fs)
			nhc.DeploymentID = 200
			_, err := NewNodeHost(*nhc)
			require.Equal(t, server.ErrDeploymentIDChanged, err,
				"failed to return ErrDeploymentIDChanged, got %v", err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestGossipInfoIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	advertiseAddress := "202.96.1.2:12345"
	to := &testOption{
		noElection: true,
		updateNodeHostConfig: func(c *config.NodeHostConfig) *config.NodeHostConfig {
			c.DefaultNodeRegistryEnabled = true
			c.Gossip = config.GossipConfig{
				BindAddress:      "localhost:23001",
				AdvertiseAddress: advertiseAddress,
				Seed:             []string{"localhost:23002"},
			}
			return c
		},
		tf: func(nh *NodeHost) {
			nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
			assert.Equal(t, advertiseAddress, nhi.Gossip.AdvertiseAddress,
				"unexpected advertise address")
			assert.True(t, nhi.Gossip.Enabled, "gossip info not enabled")
			assert.Equal(t, 1, nhi.Gossip.NumOfKnownNodeHosts,
				"unexpected NumOfKnownNodeHosts")
			assert.Equal(t, nh.ID(), nhi.NodeHostID, "unexpected NodeHostID")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestLeaderInfoIsReported(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			leaderAvailable := false
			for i := 0; i < 500; i++ {
				nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
				require.Len(t, nhi.ShardInfoList, 1, "unexpected len")
				require.Equal(t, uint64(1), nhi.ShardInfoList[0].ShardID,
					"unexpected shard id")
				if nhi.ShardInfoList[0].LeaderID != 1 {
					time.Sleep(20 * time.Millisecond)
				} else {
					leaderAvailable = true
					break
				}
			}
			require.True(t, leaderAvailable, "failed to get leader info")

			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			defer cancel()
			err := nh.SyncRequestAddReplica(ctx, 1, 2, "noidea:8080", 0)
			require.NoError(t, err, "failed to add node %v", err)

			leaderChanged := false
			for i := 0; i < 500; i++ {
				nhi := nh.GetNodeHostInfo(DefaultNodeHostInfoOption)
				require.Len(t, nhi.ShardInfoList, 1, "unexpected len")
				if nhi.ShardInfoList[0].LeaderID == 1 {
					time.Sleep(20 * time.Millisecond)
				} else {
					leaderChanged = true
					break
				}
			}
			require.True(t, leaderChanged, "no leader info change")
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
			err := nh.SyncRequestAddReplica(ctx, 1, 2, "noidea:8080", 0)
			require.NoError(t, err, "failed to add node %v", err)

			leaderSteppedDown := false
			for i := 0; i < 1000; i++ {
				_, _, ok, err := nh.GetLeaderID(1)
				require.NoError(t, err, "failed to get leader id %v", err)
				if !ok {
					leaderSteppedDown = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.True(t, leaderSteppedDown, "leader failed to step down")

			unlimited := 30 * time.Minute
			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				cs := nh.GetNoOPSession(1)
				for i := 0; i < 10; i++ {
					_, err := nh.SyncPropose(nctx, cs, make([]byte, 1))
					assert.Equal(t, ErrShardNotReady, err, "unexpected error")
				}
			}()

			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				for i := 0; i < 10; i++ {
					err := nh.SyncRequestAddReplica(nctx, 1, 3, "noidea:8080", 0)
					assert.Equal(t, ErrShardNotReady, err, "unexpected error")
				}
			}()

			func() {
				nctx, ncancel := context.WithTimeout(context.Background(), unlimited)
				defer ncancel()
				for i := 0; i < 10; i++ {
					_, err := nh.SyncRead(nctx, 1, nil)
					assert.Equal(t, ErrShardNotReady, err, "unexpected error")
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
	rel := &testRaftEventListener{
		received: make([]raftio.LeaderInfo, 0),
	}
	to := &testOption{
		defaultTestNode: true,
		updateNodeHostConfig: func(nh *config.NodeHostConfig) *config.NodeHostConfig {
			nh.RaftEventListener = rel
			return nh
		},
		tf: func(nh *NodeHost) {
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			err := nh.SyncRequestAddReplica(ctx, 1, 2, "127.0.0.1:8080", 0)
			require.NoError(t, err, "add node failed %v", err)
			cancel()

			var received []raftio.LeaderInfo
			notified := false
			for i := 0; i < 1000; i++ {
				received = rel.get()
				if len(received) >= 4 {
					notified = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.True(t, notified, "failed to get LeaderUpdated notification")

			exp0 := raftio.LeaderInfo{ShardID: 1, ReplicaID: 1,
				LeaderID: raftio.NoLeader, Term: 1}
			exp1 := raftio.LeaderInfo{ShardID: 1, ReplicaID: 1,
				LeaderID: raftio.NoLeader, Term: 2}
			exp2 := raftio.LeaderInfo{ShardID: 1, ReplicaID: 1,
				LeaderID: 1, Term: 2}
			exp3 := raftio.LeaderInfo{ShardID: 1, ReplicaID: 1,
				LeaderID: raftio.NoLeader, Term: 2}
			expected := []raftio.LeaderInfo{exp0, exp1, exp2, exp3}
			for idx := range expected {
				assert.Equal(t, expected[idx], received[idx],
					"unexpecded leader info at index %d", idx)
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestV2DataCanBeHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	if vfs.GetTestFS() != vfs.DefaultFS {
		t.Skip("skipped as not using the default fs")
	}
	v2datafp := "internal/logdb/testdata/v2-rocksdb-batched.tar.bz2"
	targetDir := "test-v2-data-safe-to-remove"
	require.NoError(t, fs.RemoveAll(targetDir))
	defer func() {
		require.NoError(t, fs.RemoveAll(targetDir))
	}()

	topDirName := "single_nodehost_test_dir_safe_to_delete"
	testHostname := "lindfield.local"
	require.NoError(t, fileutil.ExtractTarBz2(v2datafp, targetDir, fs))
	hostname, err := os.Hostname()
	require.NoError(t, err, "failed to get hostname %v", err)

	testPath := fs.PathJoin(targetDir, topDirName, testHostname)
	expPath := fs.PathJoin(targetDir, topDirName, hostname)
	if expPath != testPath {
		require.NoError(t, fs.Rename(testPath, expPath),
			"failed to rename the dir %v", err)
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
			name := nh.mu.logdb.Name()
			if name != "sharded-pebble" {
				t.Skip("skipped as not using rocksdb compatible logdb")
			}
			logdb := nh.mu.logdb
			rs, err := logdb.ReadRaftState(2, 1, 0)
			require.NoError(t, err, "failed to get raft state %v", err)
			assert.Equal(t, uint64(3), rs.EntryCount, "unexpected entry count")
			assert.Equal(t, uint64(3), rs.State.Commit, "unexpected commit")
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
			require.NoError(t, err, "failed to request snapshot %v", err)

			logdb := nh.mu.logdb
			ss, err := logdb.GetSnapshot(1, 1)
			require.NoError(t, err, "failed to list snapshots: %v", err)
			require.False(t, pb.IsEmptySnapshot(ss), "failed to get snapshot rec")

			fi, err := fs.Stat(ss.Filepath)
			require.NoError(t, err, "failed to get file path %v", err)
			require.Less(t, fi.Size(), int64(1024*364),
				"snapshot file not compressed, sz %d", fi.Size())
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
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func testWitnessIO(t *testing.T,
	witnessTestFunc func(*NodeHost, *NodeHost, *tests.SimDiskSM), fs vfs.IFS) {
	tf := func() {
		rc := config.Config{
			ShardID:      1,
			ReplicaID:    1,
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
			Expert:         getTestExpertConfig(fs),
		}
		nh1, err := NewNodeHost(nhc1)
		require.NoError(t, err, "failed to create node host %v", err)
		defer nh1.Close()

		newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
			return tests.NewSimDiskSM(0)
		}
		require.NoError(t, nh1.StartOnDiskReplica(peers, false, newSM, rc))
		waitForLeaderToBeElected(t, nh1, 1)

		for i := 0; i < 8; i++ {
			makeProposals(nh1)
			pto := lpto(nh1)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			opt := SnapshotOption{
				OverrideCompactionOverhead: true,
				CompactionOverhead:         1,
			}
			_, err := nh1.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			require.NoError(t, err, "failed to request snapshot %v", err)
		}

		pto := pto(nh1)
		ctx, cancel := context.WithTimeout(context.Background(), pto)
		err = nh1.SyncRequestAddWitness(ctx, 1, 2, nodeHostTestAddr2, 0)
		cancel()
		require.NoError(t, err, "failed to add witness %v", err)

		rc2 := rc
		rc2.ReplicaID = 2
		rc2.IsWitness = true
		nhc2 := nhc1
		nhc2.RaftAddress = nodeHostTestAddr2
		nhc2.NodeHostDir = fs.PathJoin(singleNodeHostTestDir, "nh2")
		nh2, err := NewNodeHost(nhc2)
		require.NoError(t, err, "failed to create node host %v", err)
		defer nh2.Close()

		witness := tests.NewSimDiskSM(0)
		newWitness := func(uint64, uint64) sm.IOnDiskStateMachine {
			return witness
		}
		err = nh2.StartOnDiskReplica(nil, true, newWitness, rc2)
		require.NoError(t, err, "failed to start shard %v", err)
		waitForLeaderToBeElected(t, nh2, 1)
		witnessTestFunc(nh1, nh2, witness)
	}
	runNodeHostTestDC(t, tf, true, fs)
}

func TestWitnessSnapshotIsCorrectlyHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(nh1 *NodeHost, nh2 *NodeHost, witness *tests.SimDiskSM) {
		for {
			require.Zero(t, witness.GetRecovered(),
				"unexpected recovered count %d", witness.GetRecovered())
			snapshot, err := nh2.mu.logdb.GetSnapshot(1, 2)
			require.NoError(t, err)

			if pb.IsEmptySnapshot(snapshot) {
				time.Sleep(100 * time.Millisecond)
			} else {
				require.True(t, snapshot.Witness, "not a witness snapshot")
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
		require.Zero(t, witness.GetApplied(),
			"unexpected applied count %d", witness.GetApplied())
	}
	testWitnessIO(t, tf, fs)
}

func TestWitnessCanNotInitiateIORequest(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(nh1 *NodeHost, nh2 *NodeHost, witness *tests.SimDiskSM) {
		pto := lpto(nh1)
		opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
		_, err := nh2.RequestSnapshot(1, opt, pto)
		assert.Equal(t, ErrInvalidOperation, err, "snapshot not rejected")

		session := nh2.GetNoOPSession(1)
		_, err = nh2.Propose(session, []byte("test-data"), pto)
		assert.Equal(t, ErrInvalidOperation, err, "proposal not rejected")

		session = client.NewSession(1, nh2.env.GetRandomSource())
		session.PrepareForRegister()
		_, err = nh2.ProposeSession(session, pto)
		assert.Equal(t, ErrInvalidOperation, err, "propose session not rejected")

		_, err = nh2.ReadIndex(1, pto)
		assert.Equal(t, ErrInvalidOperation, err, "sync read not rejected")

		_, err = nh2.RequestAddReplica(1, 3, "a3.com:12345", 0, pto)
		assert.Equal(t, ErrInvalidOperation, err, "add node not rejected")

		_, err = nh2.RequestDeleteReplica(1, 3, 0, pto)
		assert.Equal(t, ErrInvalidOperation, err, "delete node not rejected")

		_, err = nh2.RequestAddNonVoting(1, 3, "a3.com:12345", 0, pto)
		assert.Equal(t, ErrInvalidOperation, err, "add non-voting not rejected")

		_, err = nh2.RequestAddWitness(1, 3, "a3.com:12345", 0, pto)
		assert.Equal(t, ErrInvalidOperation, err, "add witness not rejected")

		err = nh2.RequestLeaderTransfer(1, 3)
		assert.Equal(t, ErrInvalidOperation, err, "leader transfer not rejected")

		_, err = nh2.StaleRead(1, nil)
		assert.Equal(t, ErrInvalidOperation, err, "stale read not rejected")
	}
	testWitnessIO(t, tf, fs)
}

func TestStateMachineIsClosedAfterOffloaded(t *testing.T) {
	fs := vfs.GetTestFS()
	tsm := &TimeoutStateMachine{}
	to := &testOption{
		createSM: func(uint64, uint64) sm.IStateMachine {
			return tsm
		},
		at: func(nh *NodeHost) {
			require.True(t, tsm.closed, "sm not closed")
		},
	}
	runNodeHostTest(t, to, fs)
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
			assert.Equal(t, ErrTimeout, err, "unexpected propose error")

			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			_, err = nh.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
			cancel()
			assert.Equal(t, ErrTimeout, err, "unexpected snapshot error")
		},
	}
	runNodeHostTest(t, to, fs)
}

func testIOErrorIsHandled(t *testing.T, op vfs.Op) {
	inj := vfs.OnIndex(-1, op)
	fs := vfs.Wrap(vfs.GetTestFS(), inj)
	to := &testOption{
		fsErrorInjection: true,
		defaultTestNode:  true,
		tf: func(nh *NodeHost) {
			if nh.mu.logdb.Name() == "Tan" {
				t.Skip("skipped, using tan logdb")
			}
			inj.SetIndex(0)
			pto := pto(nh)
			ctx, cancel := context.WithTimeout(context.Background(), pto)
			session := nh.GetNoOPSession(1)
			_, err := nh.SyncPropose(ctx, session, []byte("test"))
			cancel()
			require.Equal(t, ErrTimeout, err,
				"proposal unexpectedly completed, %v", err)
			select {
			case e := <-nh.engine.ec:
				require.Equal(t, vfs.ErrInjected, e,
					"failed to return the expected error, %v", e)
			default:
				require.Fail(t, "failed to trigger error")
			}
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestIOErrorIsHandled(t *testing.T) {
	testIOErrorIsHandled(t, vfs.OpWrite)
	testIOErrorIsHandled(t, vfs.OpSync)
}

func TestInstallSnapshotMessageIsNeverDropped(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			nh.partitioned = 1
			handler := newNodeHostMessageHandler(nh)
			msg := pb.Message{Type: pb.InstallSnapshot, ShardID: 1, To: 1}
			batch := pb.MessageBatch{Requests: []pb.Message{msg}}
			s, m := handler.HandleMessageBatch(batch)
			assert.Equal(t, uint64(1), s, "snapshot message dropped")
			assert.Equal(t, uint64(0), m, "unexpected message count")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestMessageToUnknownNodeIsIgnored(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			handler := newNodeHostMessageHandler(nh)
			msg1 := pb.Message{Type: pb.Ping, ShardID: 1, To: 1}
			msg2 := pb.Message{Type: pb.Pong, ShardID: 1, To: 1}
			msg3 := pb.Message{Type: pb.Pong, ShardID: 1, To: 2}
			batch := pb.MessageBatch{Requests: []pb.Message{msg1, msg2, msg3}}
			s, m := handler.HandleMessageBatch(batch)
			assert.Equal(t, uint64(0), s, "unexpected snapshot count")
			assert.Equal(t, uint64(2), m, "unexpected message count")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeCanBeUnloadedOnceClosed(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			countNodes := func(nh *NodeHost) uint64 {
				count := uint64(0)
				nh.mu.shards.Range(func(key, value interface{}) bool {
					count++
					return true
				})
				return count
			}
			node, ok := nh.getShard(1)
			require.True(t, ok, "failed to get node")
			node.requestRemoval()

			unloaded := false
			for retry := 0; retry < 1000; retry++ {
				if countNodes(nh) == 0 {
					unloaded = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.True(t, unloaded, "failed to unload the node")
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestUsingClosedNodeHostIsNotAllowed(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		at: func(nh *NodeHost) {
			assert.Equal(t, ErrClosed,
				nh.StartReplica(nil, false, nil, config.Config{}))
			assert.Equal(t, ErrClosed,
				nh.StartConcurrentReplica(nil, false, nil, config.Config{}))
			assert.Equal(t, ErrClosed,
				nh.StartOnDiskReplica(nil, false, nil, config.Config{}))
			assert.Equal(t, ErrClosed, nh.StopShard(1))
			assert.Equal(t, ErrClosed, nh.StopReplica(1, 1))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := nh.SyncPropose(ctx, nil, nil)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.SyncRead(ctx, 1, nil)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.SyncGetShardMembership(ctx, 1)
			assert.Equal(t, ErrClosed, err)
			_, _, _, err = nh.GetLeaderID(1)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.Propose(nil, nil, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.ReadIndex(1, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.ReadLocalNode(nil, nil)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.NAReadLocalNode(nil, nil)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.StaleRead(1, nil)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.RequestSnapshot(1, DefaultSnapshotOption, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.RequestCompaction(1, 1)
			assert.Equal(t, ErrClosed, err)
			assert.Equal(t, ErrClosed, nh.SyncRequestDeleteReplica(ctx, 1, 1, 0))
			assert.Equal(t, ErrClosed, nh.SyncRequestAddReplica(ctx, 1, 1, "", 0))
			assert.Equal(t, ErrClosed, nh.SyncRequestAddNonVoting(ctx, 1, 1, "", 0))
			assert.Equal(t, ErrClosed, nh.SyncRequestAddWitness(ctx, 1, 1, "", 0))
			_, err = nh.RequestDeleteReplica(1, 1, 0, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.RequestAddReplica(1, 1, "", 0, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.RequestAddNonVoting(1, 1, "", 0, time.Second)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.RequestAddWitness(1, 1, "", 0, time.Second)
			assert.Equal(t, ErrClosed, err)
			assert.Equal(t, ErrClosed, nh.RequestLeaderTransfer(1, 2))
			assert.Equal(t, ErrClosed, nh.SyncRemoveData(ctx, 1, 1))
			assert.Equal(t, ErrClosed, nh.RemoveData(1, 1))
			_, err = nh.GetNodeUser(1)
			assert.Equal(t, ErrClosed, err)
			_, err = nh.QueryRaftLog(1, 1, 2, 100)
			assert.Equal(t, ErrClosed, err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestContextDeadlineIsChecked(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			ctx := context.Background()
			_, err := nh.SyncPropose(ctx, nil, nil)
			assert.Equal(t, ErrDeadlineNotSet, err)
			_, err = nh.SyncRead(ctx, 1, nil)
			assert.Equal(t, ErrDeadlineNotSet, err)
			_, err = nh.SyncGetShardMembership(ctx, 1)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncCloseSession(ctx, nil)
			assert.Equal(t, ErrDeadlineNotSet, err)
			_, err = nh.SyncGetSession(ctx, 1)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncCloseSession(ctx, nil)
			assert.Equal(t, ErrDeadlineNotSet, err)
			_, err = nh.SyncRequestSnapshot(ctx, 1, DefaultSnapshotOption)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncRequestDeleteReplica(ctx, 1, 1, 0)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncRequestAddReplica(ctx, 1, 2, "", 0)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncRequestAddNonVoting(ctx, 1, 2, "", 0)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncRequestAddWitness(ctx, 1, 2, "", 0)
			assert.Equal(t, ErrDeadlineNotSet, err)
			err = nh.SyncRemoveData(ctx, 1, 1)
			assert.Equal(t, ErrDeadlineNotSet, err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestGetTimeoutFromContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, err := getTimeoutFromContext(context.Background())
	require.Equal(t, ErrDeadlineNotSet, err,
		"failed to return ErrDeadlineNotSet, %v", err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d, err := getTimeoutFromContext(ctx)
	require.NoError(t, err, "failed to get timeout, %v", err)
	require.True(t, d >= 55*time.Second, "unexpected result")
}

func TestHandleSnapshotStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nh := &NodeHost{stopper: syncutil.NewStopper()}
	engine := newExecEngine(nh, config.GetDefaultEngineConfig(), false, false, nil, nil)
	defer func() {
		require.NoError(t, engine.close())
	}()
	nh.engine = engine
	nh.events.sys = newSysEventListener(nil, nh.stopper.ShouldStop())
	h := messageHandler{nh: nh}
	mq := server.NewMessageQueue(1024, false, lazyFreeCycle, 1024)
	node := &node{shardID: 1, replicaID: 1, mq: mq}
	h.nh.mu.shards.Store(uint64(1), node)
	h.HandleSnapshotStatus(1, 2, true)
	for i := uint64(0); i <= streamPushDelayTick; i++ {
		node.mq.Tick()
	}
	msgs := node.mq.Get()
	require.Len(t, msgs, 1, "no msg")
	assert.Equal(t, pb.SnapshotStatus, msgs[0].Type, "unexpected message type")
	assert.True(t, msgs[0].Reject, "message not rejected")
}

func TestSnapshotReceivedMessageCanBeConverted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nh := &NodeHost{stopper: syncutil.NewStopper()}
	engine := newExecEngine(nh, config.GetDefaultEngineConfig(), false, false, nil, nil)
	defer func() {
		require.NoError(t, engine.close())
	}()
	nh.engine = engine
	nh.events.sys = newSysEventListener(nil, nh.stopper.ShouldStop())
	h := messageHandler{nh: nh}
	mq := server.NewMessageQueue(1024, false, lazyFreeCycle, 1024)
	node := &node{shardID: 1, replicaID: 1, mq: mq}
	h.nh.mu.shards.Store(uint64(1), node)
	mb := pb.MessageBatch{
		Requests: []pb.Message{{To: 1, From: 2, ShardID: 1, Type: pb.SnapshotReceived}},
	}
	sc, mc := h.HandleMessageBatch(mb)
	require.Zero(t, sc, "snapshot count not zero")
	require.Equal(t, uint64(1), mc, "message count not 1")
	for i := uint64(0); i <= streamConfirmedDelayTick; i++ {
		node.mq.Tick()
	}
	msgs := node.mq.Get()
	require.Len(t, msgs, 1, "no msg")
	assert.Equal(t, pb.SnapshotStatus, msgs[0].Type, "unexpected type")
	assert.False(t, msgs[0].Reject, "message rejected")
}

func TestIncorrectlyRoutedMessagesAreIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nh := &NodeHost{stopper: syncutil.NewStopper()}
	engine := newExecEngine(nh, config.GetDefaultEngineConfig(), false, false, nil, nil)
	defer func() {
		require.NoError(t, engine.close())
	}()
	nh.engine = engine
	nh.events.sys = newSysEventListener(nil, nh.stopper.ShouldStop())
	h := messageHandler{nh: nh}
	mq := server.NewMessageQueue(1024, false, lazyFreeCycle, 1024)
	node := &node{shardID: 1, replicaID: 1, mq: mq}
	h.nh.mu.shards.Store(uint64(1), node)
	mb := pb.MessageBatch{
		Requests: []pb.Message{{To: 3, From: 2, ShardID: 1, Type: pb.SnapshotReceived}},
	}
	sc, mc := h.HandleMessageBatch(mb)
	assert.Zero(t, sc, "snapshot count not zero")
	assert.Zero(t, mc, "message count not zero")
	msgs := node.mq.Get()
	assert.Empty(t, msgs, "received unexpected message")
}

func TestProposeOnClosedNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			u, err := nh.GetNodeUser(1)
			require.NoError(t, err, "failed to get node, %v", err)
			err = nh.StopReplica(1, 1)
			require.NoError(t, err, "failed to stop node, %v", err)
			cs := nh.GetNoOPSession(1)
			_, err = u.Propose(cs, nil, time.Second)
			require.Error(t, err, "propose on closed node didn't cause error")
			plog.Infof("%v returned from closed node", err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestReadIndexOnClosedNode(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			u, err := nh.GetNodeUser(1)
			require.NoError(t, err, "failed to get node, %v", err)
			err = nh.StopReplica(1, 1)
			require.NoError(t, err, "failed to stop node, %v", err)
			_, err = u.ReadIndex(time.Second)
			require.Error(t, err, "ReadIndex on closed node didn't cause error")
			plog.Infof("%v returned from closed node", err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestNodeCanNotStartWhenStillLoadedInEngine(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			nodes := make(map[uint64]*node)
			nodes[2] = &node{shardID: 2, replicaID: 1}
			nh.engine.loaded.update(1, fromStepWorker, nodes)
			err := nh.startShard(nil, false, nil,
				config.Config{ShardID: 2, ReplicaID: 1}, pb.RegularStateMachine)
			require.Equal(t, ErrShardAlreadyExist, err,
				"failed to return ErrShardAlreadyExist, %v", err)
		},
	}
	runNodeHostTest(t, to, fs)
}

func TestBootstrapInfoIsValidated(t *testing.T) {
	fs := vfs.GetTestFS()
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			_, _, err := nh.bootstrapShard(nil, false,
				config.Config{ShardID: 1, ReplicaID: 1}, pb.OnDiskStateMachine)
			require.Equal(t, ErrInvalidShardSettings, err,
				"failed to fail the boostrap, %v", err)
		},
	}
	runNodeHostTest(t, to, fs)
}

// slow tests

type stressRSM struct{}

func (s *stressRSM) Update(sm.Entry) (sm.Result, error) {
	plog.Infof("updated")
	return sm.Result{}, nil
}

func (s *stressRSM) Lookup(interface{}) (interface{}, error) {
	return nil, nil
}

func (s *stressRSM) SaveSnapshot(w io.Writer,
	f sm.ISnapshotFileCollection, c <-chan struct{}) error {
	data := make([]byte, settings.SnapshotChunkSize*3)
	_, err := w.Write(data)
	return err
}

func (s *stressRSM) RecoverFromSnapshot(r io.Reader,
	f []sm.SnapshotFile, c <-chan struct{}) error {
	plog.Infof("RecoverFromSnapshot called")
	data := make([]byte, settings.SnapshotChunkSize*3)
	n, err := io.ReadFull(r, data)
	if uint64(n) != settings.SnapshotChunkSize*3 {
		return errors.New("unexpected size")
	}
	return err
}

func (s *stressRSM) Close() error {
	return nil
}

// this test takes around 6 minutes on mbp and 30 seconds on a linux box with
// proper SSD
func TestSlowTestStressedSnapshotWorker(t *testing.T) {
	if len(os.Getenv("SLOW_TEST")) == 0 {
		t.Skip("skipped TestSlowTestStressedSnapshotWorker, SLOW_TEST not set")
	}
	fs := vfs.GetTestFS()
	require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	defer func() {
		require.NoError(t, fs.RemoveAll(singleNodeHostTestDir))
	}()

	nh1dir := fs.PathJoin(singleNodeHostTestDir, "nh1")
	nh2dir := fs.PathJoin(singleNodeHostTestDir, "nh2")
	rc := config.Config{
		ShardID:            1,
		ReplicaID:          1,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    5,
		CompactionOverhead: 1,
	}
	peers := make(map[uint64]string)
	peers[1] = nodeHostTestAddr1
	nhc1 := config.NodeHostConfig{
		WALDir:         nh1dir,
		NodeHostDir:    nh1dir,
		RTTMillisecond: 5 * getRTTMillisecond(fs, nh1dir),
		RaftAddress:    nodeHostTestAddr1,
		Expert:         getTestExpertConfig(fs),
	}
	nh1, err := NewNodeHost(nhc1)
	require.NoError(t, err, "failed to create node host %v", err)
	defer nh1.Close()

	newRSM := func(uint64, uint64) sm.IStateMachine {
		return &stressRSM{}
	}
	for i := uint64(1); i <= uint64(96); i++ {
		rc.ShardID = i
		err := nh1.StartReplica(peers, false, newRSM, rc)
		require.NoError(t, err, "failed to start shard %v", err)
		cs := nh1.GetNoOPSession(i)
		total := 20
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := nh1.SyncPropose(ctx, cs, make([]byte, 1))
			cancel()
			if err == ErrTimeout || err == ErrShardNotReady {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			total--
			if total == 0 {
				break
			}
		}
	}

	for i := uint64(1); i <= uint64(96); i++ {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := nh1.SyncRequestAddReplica(ctx, i, 2, nodeHostTestAddr2, 0)
			cancel()
			if err != nil {
				if err == ErrTimeout || err == ErrShardNotReady {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				require.NoError(t, err, "failed to add node %v", err)
			} else {
				break
			}
		}
	}
	plog.Infof("all nodes added")

	nhc2 := config.NodeHostConfig{
		WALDir:         nh2dir,
		NodeHostDir:    nh2dir,
		RTTMillisecond: 5 * getRTTMillisecond(fs, nh2dir),
		RaftAddress:    nodeHostTestAddr2,
		Expert:         getTestExpertConfig(fs),
	}
	nh2, err := NewNodeHost(nhc2)
	require.NoError(t, err, "failed to create node host 2 %v", err)
	defer nh2.Close()

	for i := uint64(1); i <= uint64(96); i++ {
		rc.ShardID = i
		rc.ReplicaID = 2
		peers := make(map[uint64]string)
		err := nh2.StartReplica(peers, true, newRSM, rc)
		require.NoError(t, err, "failed to start shard %v", err)
	}

	for i := uint64(1); i <= uint64(96); i++ {
		cs := nh2.GetNoOPSession(i)
		total := 1000
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := nh2.SyncPropose(ctx, cs, make([]byte, 1))
			cancel()
			if err != nil {
				if err == ErrTimeout || err == ErrShardNotReady {
					total--
					require.NotZero(t, total, "failed to make proposal on shard %d", i)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				require.NoError(t, err, "failed to make proposal %v", err)
			} else {
				break
			}
		}
	}
}
