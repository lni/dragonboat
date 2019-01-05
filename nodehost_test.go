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

package dragonboat

import (
	"context"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/tests"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	"github.com/lni/dragonboat/statemachine"
)

func ExampleNewNodeHost() {
	// Let's say we want to put all LogDB's WAL data in a directory named wal,
	// everything else is stored in a directory named dragonboat. Assume the
	// RTT between nodes is 200 milliseconds, and the nodehost address is
	// myhostname:5012
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 200,
		RaftAddress:    "myhostname:5012",
	}
	// Creates a nodehost instance using the above NodeHostConfig instnace.
	nh := NewNodeHost(nhc)
	log.Printf("nodehost created, running on %s", nh.RaftAddress())
}

func ExampleNodeHost_StartCluster() {
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "dragonboat",
		RTTMillisecond: 200,
		RaftAddress:    "myhostname:5012",
	}
	// Creates a nodehost instance using the above NodeHostConfig instnace.
	nh := NewNodeHost(nhc)
	// config for raft
	rc := config.Config{
		NodeID:             1,
		ClusterID:          100,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10000,
		CompactionOverhead: 5000,
	}
	peers := make(map[uint64]string)
	peers[100] = "myhostname1:5012"
	peers[200] = "myhostname2:5012"
	peers[300] = "myhostname3:5012"
	// Use this NO-OP data store in this example
	NewStateMachine := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		return &tests.NoOP{}
	}
	if err := nh.StartCluster(peers, false, NewStateMachine, rc); err != nil {
		log.Fatalf("failed to add cluster, %v\n", err)
	}
}

func ExampleNodeHost_Propose(nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// Use NO-OP client session, cluster ID is 100
	// Check the example on the GetNewSession method to see how to use a
	// real client session object to make proposals.
	cs := nh.GetNoOPSession(100)
	// make a proposal with the proposal content "test-data", timeout is set to
	// 2000 milliseconds.
	rs, err := nh.Propose(cs, []byte("test-data"), 2000*time.Millisecond)
	if err != nil {
		// failed to start the proposal
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the proposal failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the proposal has been committed and applied
		// put the request state instance back to the recycle pool
	} else if s.Terminated() {
		// proposal terminated as the system is being shut down, time to exit
	}
	// note that s.Code == RequestRejected is not suppose to happen as we are
	// using a NO-OP client session in this example.
}

func ExampleNodeHost_ReadIndex(nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	data := make([]byte, 1024)
	rs, err := nh.ReadIndex(100, 2000*time.Millisecond)
	if err != nil {
		// ReadIndex failed to start
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the ReadIndex operation failed to complete before the deadline, maybe
		// retry the request
	} else if s.Completed() {
		// the ReadIndex operation completed. the local IStateMachine is ready to be
		// queried
		nh.ReadLocal(100, data)
	} else if s.Terminated() {
		// the ReadIndex operation terminated as the system is being shut down,
		// time to exit
	}
}

func ExampleNodeHost_RequestDeleteNode(nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// request node with ID 1 to be removed as a member node of raft cluster 100.
	// the third parameter is OrderID, it is only relevant when using Master
	// servers.
	rs, err := nh.RequestDeleteNode(100, 1, 0, 2000*time.Millisecond)
	if err != nil {
		// failed to start the membership change request
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the request failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the requested node has been removed from the raft cluster, ready to
		// remove the node from the NodeHost running at myhostname1:5012, e.g.
		// nh.RemoveCluster(100)
	} else if s.Terminated() {
		// request terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// request rejected as it is out of order. this can only happen when
		// you are using IMasterClient. Try again with a correct order id value.
	}
}

func ExampleNodeHost_RequestAddNode(nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// request node with ID 4 running at myhostname4:5012 to be added as a member
	// node of raft cluster 100. the fourth parameter is OrderID, it is only
	// relevant when using Master servers.
	rs, err := nh.RequestAddNode(100,
		4, "myhostname4:5012", 0, 2000*time.Millisecond)
	if err != nil {
		// failed to start the membership change request
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the request failed to complete before the deadline, maybe retry the
		// request
	} else if s.Completed() {
		// the requested new node has been added to the raft cluster, ready to
		// add the node to the NodeHost running at myhostname4:5012. run the
		// following code on the NodeHost running at myhostname4:5012 -
		//
		// NewStateMachine := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		// 	 return &tests.NoOP{}
		// }
		// rc := config.Config{
		//	 NodeID:             4,
		//	 ClusterID:          100,
		//	 ElectionRTT:        5,
		//	 HeartbeatRTT:       1,
		//	 CheckQuorum:        true,
		//	 SnapshotEntries:    10000,
		//	 CompactionOverhead: 5000,
		// }
		// nh.StartCluster(nil, true, NewStateMachine, rc)
	} else if s.Terminated() {
		// request terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// request rejected as it is out of order. this can only happen when
		// you are using IMasterClient. Try again with a correct order id value.
	}
}

func ExampleNodeHost_GetNewSession(ctx context.Context, nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// Create a client session first, cluster ID is 100
	// Check the example on the GetNewSession method to see how to use a
	// real client session object to make proposals.
	cs, err := nh.GetNewSession(ctx, 100)
	if err != nil {
		// failed to get the client session, if it is a timeout error then try
		// again later.
		return
	}
	defer nh.CloseSession(ctx, cs)
	// make a proposal with the proposal content "test-data", timeout is set to
	// 2000 milliseconds.
	rs, err := nh.Propose(cs, []byte("test-data"), 2000*time.Millisecond)
	if err != nil {
		// failed to start the proposal
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {
		// the proposal failed to complete before the deadline. maybe retry
		// the request with the same client session instance s.
		// on timeout, there is actually no guarantee on whether the proposed
		// entry has been applied or not, the idea is that when retrying with
		// the same proposal using the same client session instance, dragonboat
		// makes sure that the proposal is retried and it will be applied if
		// and only if it has not been previously applied.
	} else if s.Completed() {
		// the proposal has been committed and applied, call
		// s.ProposalCompleted() to notify the client session that the previous
		// request has been successfully completed. this makes the client
		// session ready to be used when you make the next proposal.
		cs.ProposalCompleted()
	} else if s.Terminated() {
		// proposal terminated as the system is being shut down, time to exit
	} else if s.Rejected() {
		// client session s is not evicted from the server side, probably because
		// there are too many concurrent client sessions. in case you want to
		// strictly ensure that each proposal will never be applied twice, we
		// recommend to fail the client program. Note that this is highly unlikely
		// to happen.
		panic("client session already evicted")
	}
	//
	// now you can use the same client session instance s to make more proposals
	//
}

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
func (n *noopLogDB) SaveSnapshots([]pb.Update) error                                     { return nil }
func (n *noopLogDB) DeleteSnapshot(clusterID uint64, nodeID uint64, index uint64) error  { return nil }
func (n *noopLogDB) ListSnapshots(clusterID uint64, nodeID uint64) ([]pb.Snapshot, error) {
	return nil, nil
}

func TestRocksDBIsUsedByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	nh := NewNodeHost(*c)
	plog.Infof("new node host returned")
	defer nh.Stop()
	if nh.logdb.Name() != logdb.RocksDBLogDBName {
		t.Errorf("logdb type name %s, expect %s",
			nh.logdb.Name(), logdb.RocksDBLogDBName)
	}
	plog.Infof("all good")
}

func TestLogDBCanBeExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	ldb := &noopLogDB{}
	c.LogDBFactory = func([]string, []string) (raftio.ILogDB, error) {
		return ldb, nil
	}
	nh := NewNodeHost(*c)
	defer nh.Stop()
	if nh.logdb.Name() != ldb.Name() {
		t.Errorf("logdb type name %s, expect %s", nh.logdb.Name(), ldb.Name())
	}
}

func TestTCPTransportIsUsedByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	nh := NewNodeHost(*c)
	defer nh.Stop()
	tt := nh.transport.(*transport.Transport)
	if tt.GetRaftRPC().Name() != transport.TCPRaftRPCName {
		t.Errorf("raft rpc type name %s, expect %s",
			tt.GetRaftRPC().Name(), transport.TCPRaftRPCName)
	}
}

func TestRaftRPCCanBeExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	c.RaftRPCFactory = transport.NewNOOPTransport
	nh := NewNodeHost(*c)
	defer nh.Stop()
	tt := nh.transport.(*transport.Transport)
	if tt.GetRaftRPC().Name() != transport.NOOPRaftName {
		t.Errorf("raft rpc type name %s, expect %s",
			tt.GetRaftRPC().Name(), transport.NOOPRaftName)
	}
}

func TestMasterClientIsNotCreateWhenNoMasterServerIsConfigured(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	nh := NewNodeHost(*c)
	defer nh.Stop()
	if nh.masterClient != nil {
		t.Errorf("master client unexpectedly created")
	}
}

type noopMasterClient struct {
	sendCount   uint64
	handleCount uint64
}

func (n *noopMasterClient) Name() string { return "noop-masterclient" }
func (n *noopMasterClient) Stop()        {}
func (n *noopMasterClient) GetDeploymentID(ctx context.Context,
	url string) (uint64, error) {
	return 1, nil
}
func (n *noopMasterClient) HandleMasterRequests(ctx context.Context) error {
	atomic.AddUint64(&n.handleCount, 1)
	return nil
}

func (n *noopMasterClient) SendNodeHostInfo(ctx context.Context, url string,
	nhi NodeHostInfo) error {
	atomic.AddUint64(&n.sendCount, 1)
	return nil
}

func (n *noopMasterClient) getSendCount() uint64 {
	return atomic.LoadUint64(&n.sendCount)
}

func (n *noopMasterClient) getHandleCount() uint64 {
	return atomic.LoadUint64(&n.handleCount)
}

func TestMasterClientCanBeExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	c.MasterServers = []string{"localhost:22222"}
	mc := &noopMasterClient{}
	factory := func(*NodeHost) IMasterClient {
		return mc
	}
	nh := NewNodeHostWithMasterClientFactory(*c, factory)
	defer nh.Stop()
	if nh.masterClient.Name() != mc.Name() {
		t.Errorf("master client type name %s, expect %s",
			nh.masterClient.Name(), mc.Name())
	}
}

func TestMasterClientIsPeriodicallyUsed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	ov := NodeHostInfoReportSecond
	NodeHostInfoReportSecond = 1
	c := getTestNodeHostConfig()
	c.MasterServers = []string{"localhost:22222"}
	mc := &noopMasterClient{}
	factory := func(*NodeHost) IMasterClient {
		return mc
	}
	nh := NewNodeHostWithMasterClientFactory(*c, factory)
	defer func() { NodeHostInfoReportSecond = ov }()
	defer nh.Stop()
	if nh.masterClient.Name() != mc.Name() {
		t.Errorf("master client type name %s, expect %s",
			nh.masterClient.Name(), mc.Name())
	}
	var prevSendCount uint64
	var prevHandleCount uint64
	done := false
	for iter := 1; iter < 50; iter++ {
		prevSendCount = mc.getSendCount()
		prevHandleCount = mc.getHandleCount()
		time.Sleep(3 * time.Second)
		sc := mc.getSendCount()
		hc := mc.getHandleCount()
		if sc >= prevSendCount+2 && hc >= prevHandleCount+2 {
			done = true
			break
		} else {
			plog.Infof("sc %d, prev sc %d, hc %d, prev hc %d",
				sc, prevSendCount, hc, prevHandleCount)
		}
	}
	if !done {
		t.Errorf("SendNodeHostInfo or HandleMasterRequests not periodically called")
	}
}

func TestDeploymentIDCanBeSetUsingNodeHostConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	c.DeploymentID = 100
	nh := NewNodeHost(*c)
	defer nh.Stop()
	if nh.deploymentID != 100 {
		t.Errorf("deployment id not set")
	}
}

var (
	singleNodeHostTestAddr = "localhost:26000"
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

func (n *PST) Close() {}

// Lookup locally looks up the data.
func (n *PST) Lookup(key []byte) []byte {
	return make([]byte, 1)
}

// Update updates the object.
func (n *PST) Update(data []byte) uint64 {
	return uint64(len(data))
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (n *PST) SaveSnapshot(w io.Writer,
	fileCollection statemachine.ISnapshotFileCollection,
	done <-chan struct{}) (uint64, error) {
	plog.Infof("save snapshot called")
	n.saved = true
	if !n.slowSave {
		n, err := w.Write([]byte("random-data"))
		if err != nil {
			panic(err)
		}
		return uint64(n), nil
	}
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			n.stopped = true
			plog.Infof("saveSnapshot stopped")
			return 0, statemachine.ErrSnapshotStopped
		default:
		}
	}
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *PST) RecoverFromSnapshot(r io.Reader,
	files []statemachine.SnapshotFile, done <-chan struct{}) error {
	n.setRestored(true)
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			n.stopped = true
			return statemachine.ErrSnapshotStopped
		default:
		}
	}
}

// GetHash returns a uint64 value representing the current state of the object.
func (n *PST) GetHash() uint64 {
	// the hash value is always 0, so it is of course always consistent
	return 0
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
		RTTMillisecond: 50,
		RaftAddress:    peers[1],
	}
	nh := NewNodeHost(nhc)
	var pst *PST
	newPST := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		pst = &PST{slowSave: slowSave}
		return pst
	}
	if err := nh.StartCluster(peers, false, newPST, rc); err != nil {
		return nil, nil, err
	}
	return nh, pst, nil
}

func waitForLeaderToBeElected(t *testing.T, nh *NodeHost) {
	for i := 0; i < 200; i++ {
		_, ready, err := nh.GetLeaderID(2)
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
		cs, err := nh.GetNewSession(ctx, 2)
		if err != nil {
			if err == ErrTimeout {
				cancel()
				return
			}
			t.Fatalf("unexpected error %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := nh.CloseSession(ctx, cs); err != nil {
			t.Fatalf("failed to close client session %v", err)
		}
		cancel()
	}
	if timeoutExpected {
		t.Fatalf("failed to trigger ")
	}
}

func TestJoinedClusterCanBeRestartedOrJoinedAgain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
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
	nh := NewNodeHost(nhc)
	defer nh.Stop()
	newPST := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
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

func TestSnapshotCanBeStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, pst, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, true)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh)
	defer os.RemoveAll(singleNodeHostTestDir)
	createProposalsToTriggerSnapshot(t, nh, 50, true)
	nh.Stop()
	time.Sleep(100 * time.Millisecond)
	if !pst.saved || !pst.stopped {
		t.Errorf("snapshot not stopped")
	}
}

func TestRecoverFromSnapshotCanBeStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh)
	defer os.RemoveAll(singleNodeHostTestDir)
	createProposalsToTriggerSnapshot(t, nh, 25, false)
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

func TestRegisterASessionTwiceWillBeReported(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := nh.GetNewSession(ctx, 2)
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
		cs, err := nh.GetNewSession(ctx, 2)
		if err != nil {
			t.Errorf("failed to get client session %v", err)
		}
		err = nh.CloseSession(ctx, cs)
		if err != nil {
			t.Errorf("failed to unregister the client session %v", err)
		}
		err = nh.CloseSession(ctx, cs)
		if err != ErrRejected {
			t.Errorf("failed to reject the request %v", err)
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
	if _, err = nh.serverCtx.PrepareSnapshotDir(nh.deploymentID, 2, 1); err != nil {
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
	defer leaktest.AfterTest(t)()
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-01.receiving")
	testZombieSnapshotDirWillBeDeletedDuringAddCluster(t, "snapshot-AB-10.generating")
}

func singleNodeHostTest(t *testing.T, tf func(t *testing.T, nh *NodeHost)) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh)
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
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete read index")
		}
		_, err = nh.ReadLocal(2, make([]byte, 128))
		if err != nil {
			t.Errorf("read local failed %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostReadIndex(t *testing.T) {
	testNodeHostReadIndex(t)
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
		if v != 128 {
			t.Errorf("unexpected result")
		}
		data, err := nh.SyncRead(ctx, 2, make([]byte, 128))
		if err != nil {
			t.Errorf("make linearizable read failed %v", err)
		}
		if len(data) == 0 {
			t.Errorf("failed to get result")
		}
		if err := nh.StopCluster(2); err != nil {
			t.Errorf("failed to stop cluster 2 %v", err)
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
		membership, err := nh.GetClusterMembership(ctx, 2)
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
		membership, err = nh.GetClusterMembership(ctx, 2)
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

/******************************************************************************
* Benchmarks
******************************************************************************/

func benchmarkNoPool128Allocs(b *testing.B, sz uint64) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := make([]byte, sz)
			b.SetBytes(int64(sz))
			if uint64(len(m)) < sz {
				b.Errorf("len(m) < %d", sz)
			}
		}
	})
}

func BenchmarkNoPool128Allocs512Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 512)
}

func BenchmarkNoPool128Allocs15Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 15)
}

func BenchmarkNoPool128Allocs2Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 2)
}

func BenchmarkNoPool128Allocs16Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 16)
}

func BenchmarkNoPool128Allocs17Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 17)
}

func BenchmarkAddToEntryQueue(b *testing.B) {
	b.ReportAllocs()
	q := newEntryQueue(1000000, 0)
	total := uint64(0)
	entry := pb.Entry{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := atomic.AddUint64(&total, 1)
			if t%2048 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get()
			} else {
				q.add(entry)
			}
		}
	})
}

func benchmarkProposeN(b *testing.B, sz int) {
	b.ReportAllocs()
	data := make([]byte, sz)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint64(0)
	q := newEntryQueue(2048, 0)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	session := client.NewNoOPSession(1, random.LockGuardedRand)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&total, 1)
			b.SetBytes(int64(sz))
			rs, err := pp.propose(session, data, nil, time.Second)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get()
			}
			pp.applied(rs.key, rs.clientID, rs.seriesID, 1, false)
			rs.Release()
		}
	})
}

func BenchmarkPropose16(b *testing.B) {
	benchmarkProposeN(b, 16)
}

func BenchmarPropose128(b *testing.B) {
	benchmarkProposeN(b, 128)
}

func BenchmarkPropose1024(b *testing.B) {
	benchmarkProposeN(b, 1024)
}

func BenchmarkPendingProposalNextKey(b *testing.B) {
	b.ReportAllocs()
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	q := newEntryQueue(2048, 0)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	b.RunParallel(func(pb *testing.PB) {
		clientID := rand.Uint64()
		for pb.Next() {
			pp.nextKey(clientID)
		}
	})
}

func BenchmarkReadIndexRead(b *testing.B) {
	b.ReportAllocs()
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint64(0)
	q := newReadIndexQueue(2048)
	pri := newPendingReadIndex(p, q, 200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&total, 1)
			rs, err := pri.read(nil, time.Second)
			if err != nil {
				b.Errorf("%v", err)
			}
			if v%128 == 0 {
				atomic.StoreUint64(&total, 0)
				q.get()
			}
			rs.Release()
		}
	})
}

func benchmarkMarshalEntryN(b *testing.B, sz int) {
	b.ReportAllocs()
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, sz),
	}
	data := make([]byte, e.Size())
	for i := 0; i < b.N; i++ {
		n, err := e.MarshalTo(data)
		if n > len(data) {
			b.Errorf("n > len(data)")
		}
		b.SetBytes(int64(n))
		if err != nil {
			b.Errorf("%v", err)
		}
	}
}

func BenchmarkMarshalEntry16(b *testing.B) {
	benchmarkMarshalEntryN(b, 16)
}

func BenchmarkMarshalEntry128(b *testing.B) {
	benchmarkMarshalEntryN(b, 128)
}

func BenchmarkMarshalEntry1024(b *testing.B) {
	benchmarkMarshalEntryN(b, 1024)
}

func BenchmarkWorkerReady(b *testing.B) {
	b.ReportAllocs()
	rc := newWorkReady(1)
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.clusterReady(1)
		}
	})
}

func BenchmarkReadyCluster(b *testing.B) {
	b.ReportAllocs()
	rc := newReadyCluster()
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.setClusterReady(1)
		}
	})
}

func benchmarkSaveRaftState(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb")
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, sz),
	}
	bytes := e.Size() * 128
	u := pb.Update{
		ClusterID: 1,
		NodeID:    1,
	}
	iidx := e.Index
	for i := uint64(0); i < 128; i++ {
		e.Index = iidx + i
		u.EntriesToSave = append(u.EntriesToSave, e)
	}
	b.StartTimer()
	b.RunParallel(func(pbt *testing.PB) {
		rdbctx := db.GetLogDBThreadContext()
		for pbt.Next() {
			rdbctx.Reset()
			if err := db.SaveRaftState([]pb.Update{u}, rdbctx); err != nil {
				b.Errorf("%v", err)
			}
			b.SetBytes(int64(bytes))
		}
	})
}

func BenchmarkSaveRaftState16(b *testing.B) {
	benchmarkSaveRaftState(b, 16)
}

func BenchmarkSaveRaftState128(b *testing.B) {
	benchmarkSaveRaftState(b, 128)
}

func BenchmarkSaveRaftState1024(b *testing.B) {
	benchmarkSaveRaftState(b, 1024)
}

type benchmarkMessageHandler struct {
	expected uint64
	count    uint64
	ch       chan struct{}
}

func (h *benchmarkMessageHandler) wait() {
	<-h.ch
}

func (h *benchmarkMessageHandler) reset() {
	atomic.StoreUint64(&h.count, 0)
}

func (h *benchmarkMessageHandler) HandleMessageBatch(batch pb.MessageBatch) {
	v := atomic.AddUint64(&h.count, uint64(len(batch.Requests)))
	if v >= h.expected {
		h.ch <- struct{}{}
	}
}

func (h *benchmarkMessageHandler) HandleUnreachable(clusterID uint64, nodeID uint64) {
}

func (h *benchmarkMessageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, rejected bool) {
}

func (h *benchmarkMessageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
}

func benchmarkTransport(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("transport")
	l.SetLevel(logger.ERROR)
	l = logger.GetLogger("grpc")
	l.SetLevel(logger.ERROR)
	addr1 := "localhost:43567"
	addr2 := "localhost:43568"
	nhc1 := config.NodeHostConfig{
		RaftAddress: addr1,
	}
	ctx1 := server.NewContext(nhc1)
	nhc2 := config.NodeHostConfig{
		RaftAddress: addr2,
	}
	ctx2 := server.NewContext(nhc2)
	nodes1 := transport.NewNodes(settings.Soft.StreamConnections)
	nodes2 := transport.NewNodes(settings.Soft.StreamConnections)
	nodes1.AddRemoteAddress(1, 2, addr2)
	t1 := transport.NewTransport(nhc1, ctx1, nodes1, nil)
	t1.SetUnmanagedDeploymentID()
	t2 := transport.NewTransport(nhc2, ctx2, nodes2, nil)
	t2.SetUnmanagedDeploymentID()
	defer t2.Stop()
	defer t1.Stop()
	handler1 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	handler2 := &benchmarkMessageHandler{
		ch:       make(chan struct{}, 1),
		expected: 128,
	}
	t2.SetMessageHandler(handler1)
	t1.SetMessageHandler(handler2)
	msgs := make([]pb.Message, 0)
	e := pb.Entry{
		Index:       12843560,
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    234926831800,
		SeriesID:    12843560,
		RespondedTo: 12843550,
		Cmd:         make([]byte, sz),
	}
	for i := 0; i < 128; i++ {
		m := pb.Message{
			Type:      pb.Replicate,
			To:        2,
			From:      1,
			ClusterId: 1,
			Term:      100,
			LogTerm:   100,
			LogIndex:  123456789,
			Commit:    123456789,
		}
		for j := 0; j < 64; j++ {
			m.Entries = append(m.Entries, e)
		}
		msgs = append(msgs, m)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		handler1.reset()
		for _, msg := range msgs {
			t1.ASyncSend(msg)
		}
		handler1.wait()
	}
}

func BenchmarkTransport16(b *testing.B) {
	benchmarkTransport(b, 16)
}

func BenchmarkTransport128(b *testing.B) {
	benchmarkTransport(b, 128)
}

func BenchmarkTransport1024(b *testing.B) {
	benchmarkTransport(b, 1024)
}

type noopNodeProxy struct {
}

func (n *noopNodeProxy) RestoreRemotes(pb.Snapshot)                     {}
func (n *noopNodeProxy) ApplyUpdate(pb.Entry, uint64, bool, bool, bool) {}
func (n *noopNodeProxy) ApplyConfigChange(pb.ConfigChange)              {}
func (n *noopNodeProxy) ConfigChangeProcessed(uint64, bool)             {}
func (n *noopNodeProxy) NodeID() uint64                                 { return 1 }
func (n *noopNodeProxy) ClusterID() uint64                              { return 1 }

func benchmarkStateMachineStep(b *testing.B, sz int, noopSession bool) {
	b.ReportAllocs()
	b.StopTimer()
	ds := &tests.NoOP{}
	done := make(chan struct{})
	nds := rsm.NewNativeStateMachine(ds, done)
	sm := rsm.NewStateMachine(nds, nil, false, &noopNodeProxy{})
	idx := uint64(0)
	var s *client.Session
	if noopSession {
		s = client.NewNoOPSession(1, random.LockGuardedRand)
	} else {
		s = &client.Session{
			ClusterID: 1,
			ClientID:  1234576,
		}
	}
	e := pb.Entry{
		Term:        123,
		Type:        pb.ApplicationEntry,
		Key:         13563799145,
		ClientID:    s.ClientID,
		SeriesID:    s.SeriesID,
		RespondedTo: s.RespondedTo,
		Cmd:         make([]byte, sz),
	}
	entries := make([]pb.Entry, 0)
	batch := make([]rsm.Commit, 0)
	commit := rsm.Commit{
		SnapshotAvailable: false,
	}
	if !noopSession {
		idx++
		e.Index = idx
		e.SeriesID = client.SeriesIDForRegister
		entries = append(entries, e)
		commit.Entries = entries
		sm.CommitC() <- commit
		sm.Handle(batch)
	}
	b.StartTimer()
	for x := 0; x < b.N; x++ {
		entries = entries[:0]
		for i := uint64(0); i < 128; i++ {
			idx++
			e.Index = idx
			entries = append(entries, e)
		}
		commit.Entries = entries
		sm.CommitC() <- commit
		sm.Handle(batch)
	}
}

func BenchmarkStateMachineStepNoOPSession16(b *testing.B) {
	benchmarkStateMachineStep(b, 16, true)
}

func BenchmarkStateMachineStepNoOPSession128(b *testing.B) {
	benchmarkStateMachineStep(b, 128, true)
}

func BenchmarkStateMachineStepNoOPSession1024(b *testing.B) {
	benchmarkStateMachineStep(b, 1024, true)
}

func BenchmarkStateMachineStep16(b *testing.B) {
	benchmarkStateMachineStep(b, 16, false)
}

func BenchmarkStateMachineStep128(b *testing.B) {
	benchmarkStateMachineStep(b, 128, false)
}

func BenchmarkStateMachineStep1024(b *testing.B) {
	benchmarkStateMachineStep(b, 1024, false)
}
