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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/tests"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
	"github.com/lni/dragonboat/tools"
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
	NewStateMachine := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
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
		nh.ReadLocalNode(rs, data)
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
	// the third parameter is OrderID.
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
		// request rejected as it is out of order. try again with the latest order
		// id value returned by NodeHost's GetClusterMembership() method.
	}
}

func ExampleNodeHost_RequestAddNode(nh *NodeHost) {
	// nh is a NodeHost instance, a Raft cluster with ID 100 has already been added
	// this to NodeHost.
	// see the example on StartCluster on how to start Raft cluster.
	//
	// request node with ID 4 running at myhostname4:5012 to be added as a member
	// node of raft cluster 100. the fourth parameter is OrderID.
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
		// NewStateMachine := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
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
		// request rejected as it is out of order. try again with the latest order
		// id value returned by NodeHost's GetClusterMembership() method.
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
func (n *noopLogDB) ListSnapshots(clusterID uint64, nodeID uint64) ([]pb.Snapshot, error) {
	return nil, nil
}
func (n *noopLogDB) ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error {
	return nil
}

/*
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
}*/

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
	nodeHostTestAddr1      = "localhost:26000"
	nodeHostTestAddr2      = "localhost:26001"
	singleNodeHostTestDir  = "single_nodehost_test_dir_safe_to_delete"
)

type PST struct {
	mu                      sync.Mutex
	stopped                 bool
	saved                   bool
	restored                bool
	slowSave                bool
	saveSnapshotCanComplete bool
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
func (n *PST) Update(data []byte) sm.Result {
	return sm.Result{Value: uint64(len(data))}
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
	nh := NewNodeHost(nhc)
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
	datadir string, initialApplied uint64) (*NodeHost, error) {
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
	nh := NewNodeHost(nhc)
	newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
		return tests.NewFakeDiskSM(initialApplied)
	}
	if err := nh.StartOnDiskCluster(peers, false, newSM, rc); err != nil {
		return nil, err
	}
	return nh, nil
}

func singleConcurrentNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost), snapshotEntry uint64, concurrent bool) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, err := createConcurrentTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, snapshotEntry, concurrent)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh, 1)
	waitForLeaderToBeElected(t, nh, 1+taskWorkerCount)
	defer os.RemoveAll(singleNodeHostTestDir)
	defer func() {
		nh.Stop()
	}()
	tf(t, nh)
}

func singleFakeDiskNodeHostTest(t *testing.T,
	tf func(t *testing.T, nh *NodeHost, initialApplied uint64), initialApplied uint64) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, err := createFakeDiskTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, initialApplied)
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
	nh1 := NewNodeHost(nhc1)
	nh2 := NewNodeHost(nhc2)
	newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
		return tests.NewFakeDiskSM(3)
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
	nh := NewNodeHost(nhc)
	newRSM := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return &tests.NoOP{MillisecondToSleep: 20}
	}
	oldv := settings.Soft.ExpectedMaxInMemLogSize
	settings.Soft.ExpectedMaxInMemLogSize = 3 * 1024
	defer func() {
		settings.Soft.ExpectedMaxInMemLogSize = oldv
	}()
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
	nh1 := NewNodeHost(nhc1)
	nh2 := NewNodeHost(nhc2)
	sm1 := &tests.NoOP{}
	sm2 := &tests.NoOP{}
	newRSM1 := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return sm1
	}
	newRSM2 := func(clusterID uint64, nodeID uint64) sm.IStateMachine {
		return sm2
	}
	oldv := settings.Soft.ExpectedMaxInMemLogSize
	settings.Soft.ExpectedMaxInMemLogSize = 3 * 1024
	defer func() {
		settings.Soft.ExpectedMaxInMemLogSize = oldv
	}()
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

func TestSnapshotCanBeStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
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

func TestRecoverFromSnapshotCanBeStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	waitForLeaderToBeElected(t, nh, 2)
	defer os.RemoveAll(singleNodeHostTestDir)
	createProposalsToTriggerSnapshot(t, nh, 25, false)
	logdb := nh.logdb
	snapshots, err := logdb.ListSnapshots(2, 1)
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
			snapshots, err := logdb.ListSnapshots(2, 1)
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

func TestBatchedAndPlainEntriesAreNotCompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 100,
		RaftAddress:    nodeHostTestAddr1,
	}
	nh := NewNodeHost(nhc)
	nh.Stop()
	nhc.LogDBFactory = OpenBatchedLogDB
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	NewNodeHost(nhc)
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
		_, err := nh.GetNewSession(ctx, 1)
		cancel()
		if err != nil {
			t.Fatalf("failed to get new session")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 5)
}

func TestOnDiskStateMachineCanBeOpened(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost, initialApplied uint64) {
		nhi := nh.GetNodeHostInfo()
		for _, ci := range nhi.ClusterInfoList {
			if ci.ClusterID == 1 {
				if ci.StateMachineType != sm.OnDiskStateMachine {
					t.Errorf("unexpected state machine type")
				}
			}
			if ci.IsObserver {
				t.Errorf("invalid IsObserver value")
			}
		}
		session := nh.GetNoOPSession(1)
		for i := uint64(2); i < initialApplied*2; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := nh.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err != nil && i > 4 {
				t.Errorf("SyncPropose iter %d returned err %v", i, err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		count, err := nh.SyncRead(ctx, 1, nil)
		cancel()
		if err != nil {
			t.Errorf("SyncRead returned err %v", err)
		}
		if binary.LittleEndian.Uint64(count) != initialApplied {
			t.Errorf("got %d, want %d", binary.LittleEndian.Uint64(count), initialApplied)
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 5)
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
			snapshots, err := logdb.ListSnapshots(1, 1)
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
		plog.Infof("going to check %s", ss.Filepath)
		reader, err := rsm.NewSnapshotReader(ss.Filepath)
		if err != nil {
			t.Fatalf("failed to read snapshot %v", err)
		}
		h, err := reader.GetHeader()
		if err != nil {
			t.Errorf("failed to get header")
		}
		if h.DataStoreSize != 0 || h.SessionSize != 16 {
			t.Errorf("not a dummy snapshot file %d:%d", h.DataStoreSize, h.SessionSize)
			reader.ValidateHeader(h)
		}
		if h.Version != uint64(rsm.CurrentSnapshotVersion) {
			t.Errorf("unexpected snapshot version, got %d, want %d",
				h.Version, rsm.CurrentSnapshotVersion)
		}
		reader.ValidateHeader(h)
	}
	singleFakeDiskNodeHostTest(t, tf, 3)
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
				continue
			}
			snapshots, err := logdb.ListSnapshots(1, 1)
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
			return tests.NewFakeDiskSM(3)
		}
		peers := make(map[uint64]string)
		if err := nh2.StartOnDiskCluster(peers, true, newSM, rc); err != nil {
			t.Fatalf("failed to start cluster %v", err)
		}
		snapshotted = false
		logdb = nh2.logdb
		for i := uint64(2); i < 1000; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := nh2.SyncPropose(ctx, session, []byte("test-data"))
			cancel()
			if err != nil {
				continue
			}
			snapshots, err := logdb.ListSnapshots(1, 2)
			if len(snapshots) >= 3 {
				snapshotted = true
				for _, ss := range snapshots {
					shrinked, err := rsm.IsShrinkedSnapshotFile(ss.Filepath)
					if err != nil {
						t.Errorf("failed to check whether snapshot is shrinked %v", err)
					}
					if !shrinked {
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
				nh.SyncPropose(ctx, session, []byte("test"))
				cancel()
				if atomic.LoadUint32(&count) > 0 {
					return
				}
			}
		})
		stopper.RunWorker(func() {
			for i := 0; i < 10000; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				result, err := nh.SyncRead(ctx, clusterID, []byte("test"))
				cancel()
				if err != nil {
					continue
				}
				v := binary.LittleEndian.Uint32(result)
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
		nhi := nh.GetNodeHostInfo()
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
		nhi := nh.GetNodeHostInfo()
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
				nh.SyncPropose(ctx, session, []byte("test"))
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
				v := binary.LittleEndian.Uint32(result)
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
		if bytes.Compare(result.Data, cmd) != 0 {
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
			return tests.NewFakeDiskSM(3)
		}
		rs, err := nh1.RequestAddObserver(1, 2, nodeHostTestAddr2, 0, 2000*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to add observer %v", err)
		}
		select {
		case <-rs.CompletedC:
		}
		if err := nh2.StartOnDiskCluster(nil, true, newSM, rc); err != nil {
			t.Errorf("failed to start observer %v", err)
		}
		for i := 0; i < 10000; i++ {
			nhi := nh2.GetNodeHostInfo()
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
		sr, err := nh.RequestSnapshot(2, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		var index uint64
		select {
		case v := <-sr.CompleteC:
			if !v.Completed() {
				t.Errorf("failed to complete the requested snapshot")
			}
			index = v.GetIndex()
		}
		plog.Infof("going to request snapshot again")
		sr, err = nh.RequestSnapshot(2, 3*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot")
		}
		select {
		case v := <-sr.CompleteC:
			if !v.Rejected() {
				t.Errorf("failed to complete the requested snapshot")
			}
		}
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1)
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
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
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
	sr, err := nh.RequestSnapshot(2, 200*time.Millisecond)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	plog.Infof("going to wait for snapshot request to complete")
	select {
	case v := <-sr.CompleteC:
		if !v.Timeout() {
			t.Errorf("failed to report timeout")
		}
	}
}

func TestRemoveNodeDataWillFailWhenNodeIsStillRunning(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if err := nh.RemoveData(2, 1); err != ErrClusterNotStopped {
			t.Fatalf("remove data didn't fail")
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
		sr, err := nh.RequestSnapshot(2, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		select {
		case v := <-sr.CompleteC:
			if !v.Completed() {
				t.Errorf("failed to complete the requested snapshot")
			}
		}
		nh.StopCluster(2)
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(snapshots) == 0 {
			t.Fatalf("failed to save snapshots")
		}
		snapshotDir := nh.serverCtx.GetSnapshotDir(nh.deploymentID, 2, 1)
		if !fileutil.Exist(snapshotDir) {
			t.Fatalf("snapshot dir %s does not exist", snapshotDir)
		}
		removed := false
		for i := 0; i < 1000; i++ {
			err := nh.RemoveData(2, 1)
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
		if fileutil.Exist(snapshotDir) {
			t.Fatalf("snapshot dir %s still exist", snapshotDir)
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
		snapshots, err = logdb.ListSnapshots(2, 1)
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
		os.MkdirAll(sspath, 0755)
		defer os.RemoveAll(sspath)
		session := nh.GetNoOPSession(2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cmd := make([]byte, 1518)
		_, err := nh.SyncPropose(ctx, session, cmd)
		cancel()
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
		sr, err := nh.ExportSnapshot(2, sspath, 3*time.Second)
		if err != nil {
			t.Errorf("failed to request snapshot")
		}
		var index uint64
		select {
		case v := <-sr.CompleteC:
			if !v.Completed() {
				t.Fatalf("failed to complete the requested snapshot")
			}
			index = v.GetIndex()
		}
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(2, 1)
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
		if !fileutil.Exist(fp) {
			t.Errorf("snapshot file not saved")
		}
		metafp := path.Join(sspath, snapshotDir, "snapshot.metadata")
		if !fileutil.Exist(metafp) {
			t.Errorf("snapshot metadata not saved")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestOnDiskStateMachineCanExportSnapshot(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost, initialApplied uint64) {
		sspath := "exported_snapshot_safe_to_delete"
		os.RemoveAll(sspath)
		os.MkdirAll(sspath, 0755)
		defer os.RemoveAll(sspath)
		sr, err := nh.ExportSnapshot(1, sspath, 3*time.Second)
		if err != nil {
			t.Fatalf("failed to request snapshot %v", err)
		}
		var index uint64
		select {
		case v := <-sr.CompleteC:
			if !v.Completed() {
				t.Fatalf("failed to complete the requested snapshot")
			}
			index = v.GetIndex()
		}
		logdb := nh.logdb
		snapshots, err := logdb.ListSnapshots(1, 1)
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
		if !fileutil.Exist(fp) {
			t.Errorf("snapshot file not saved")
		}
		metafp := path.Join(sspath, snapshotDir, "snapshot.metadata")
		if !fileutil.Exist(metafp) {
			t.Errorf("snapshot metadata not saved")
		}
		shrinked, err := rsm.IsShrinkedSnapshotFile(fp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if shrinked {
			t.Errorf("exported snapshot is considered as shrinked")
		}
	}
	singleFakeDiskNodeHostTest(t, tf, 3)
}

func TestClusterWithoutQuorumCanBeRestoreByImportingSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nh1dir := path.Join(singleNodeHostTestDir, "nh1")
	nh2dir := path.Join(singleNodeHostTestDir, "nh2")
	os.RemoveAll(singleNodeHostTestDir)
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
		DeploymentID:   1,
	}
	nhc2 := config.NodeHostConfig{
		WALDir:         nh2dir,
		NodeHostDir:    nh2dir,
		RTTMillisecond: 10,
		RaftAddress:    nodeHostTestAddr2,
		DeploymentID:   1,
	}
	plog.Infof("dir1 %s, dir2 %s", nh1dir, nh2dir)
	var once sync.Once
	nh1 := NewNodeHost(nhc1)
	nh2 := NewNodeHost(nhc2)
	newSM := func(uint64, uint64) sm.IOnDiskStateMachine {
		return tests.NewFakeDiskSM(3)
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
	os.MkdirAll(sspath, 0755)
	defer os.RemoveAll(sspath)
	sr, err := nh1.ExportSnapshot(1, sspath, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to request snapshot %v", err)
	}
	var index uint64
	select {
	case v := <-sr.CompleteC:
		if !v.Completed() {
			t.Fatalf("failed to complete the requested snapshot")
		}
		index = v.GetIndex()
	}
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
	rnh1 := NewNodeHost(nhc1)
	rnh2 := NewNodeHost(nhc2)
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

func (s *testSink2) ClusterID() uint64 {
	return 2000
}

func (s *testSink2) ToNodeID() uint64 {
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

func TestChunkWriterOutputCanBeHandledByChunks(t *testing.T) {
	os.RemoveAll(testSnapshotDir)
	c := &chunks{}
	chunks := transport.NewSnapshotChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc)
	sink := &testSink2{receiver: chunks}
	meta := getTestSnapshotMeta()
	cw := rsm.NewChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send LRU session %v", err)
	}
	defer os.RemoveAll(testSnapshotDir)
	payload := make([]byte, 0)
	payload = append(payload, rsm.GetEmptyLRUSession()...)
	for i := 0; i < 10; i++ {
		data := make([]byte, rsm.SnapshotChunkSize)
		rand.Read(data)
		payload = append(payload, data...)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Flush(); err != nil {
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
