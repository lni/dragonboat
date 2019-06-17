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
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/raft"
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
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	raftTestTopDir            = "raft_node_test_safe_to_delete"
	logdbDir                  = "logdb_test_dir_safe_to_delete"
	lowLatencyLogDBDir        = "logdb_ll_test_dir_safe_to_delete"
	snapDir                   = "snap_test_dir_safe_to_delete/snap-%d-%d"
	testClusterID      uint64 = 1100
	tickMillisecond    uint64 = 50
)

func getMemberNodes(r *rsm.StateMachine) []uint64 {
	m, _, _, _ := r.GetMembership()
	n := make([]uint64, 0)
	for nid := range m {
		n = append(n, nid)
	}
	return n
}

type testMessageRouter struct {
	clusterID    uint64
	msgReceiveCh map[uint64]*server.MessageQueue
	dropRate     uint8
}

func mustComplete(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Fatalf("got %v, want %d", v, requestCompleted)
		}
	default:
		t.Fatalf("failed to complete the proposal")
	}
}

func mustReject(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.CompletedC:
		if !v.Rejected() {
			t.Errorf("got %v, want %d", v, requestRejected)
		}
	default:
		t.Errorf("failed to complete the add node request")
	}
}

func mustHasLeaderNode(nodes []*node, t *testing.T) *node {
	for _, node := range nodes {
		if node.isLeader() {
			return node
		}
	}
	t.Fatalf("no leader")
	return nil
}

func newTestMessageRouter(clusterID uint64,
	nodeIDList []uint64) *testMessageRouter {
	chMap := make(map[uint64]*server.MessageQueue)
	for _, nodeID := range nodeIDList {
		ch := server.NewMessageQueue(1000, false, 0, 1024*1024*256)
		chMap[nodeID] = ch
	}
	rand.Seed(time.Now().UnixNano())
	return &testMessageRouter{msgReceiveCh: chMap, clusterID: clusterID}
}

func (r *testMessageRouter) shouldDrop(msg pb.Message) bool {
	if raft.IsLocalMessageType(msg.Type) {
		return false
	}
	if r.dropRate == 0 {
		return false
	}
	if rand.Uint32()%100 < uint32(r.dropRate) {
		return true
	}
	return false
}

func (r *testMessageRouter) sendMessage(msg pb.Message) {
	if msg.ClusterId != r.clusterID {
		panic("cluster id does not match")
	}
	if r.shouldDrop(msg) {
		return
	}
	if q, ok := r.msgReceiveCh[msg.To]; ok {
		q.Add(msg)
	}
}

func (r *testMessageRouter) getMessageReceiveChannel(clusterID uint64,
	nodeID uint64) *server.MessageQueue {
	if clusterID != r.clusterID {
		panic("cluster id does not match")
	}
	ch, ok := r.msgReceiveCh[nodeID]
	if !ok {
		panic("node id not found in the test msg router")
	}
	return ch
}

func (r *testMessageRouter) addChannel(nodeID uint64, q *server.MessageQueue) {
	r.msgReceiveCh[nodeID] = q
}

func cleanupTestDir() {
	os.RemoveAll(raftTestTopDir)
}

func getTestRaftNodes(count int) ([]*node, []*rsm.StateMachine,
	*testMessageRouter, raftio.ILogDB) {
	return doGetTestRaftNodes(1, count, false, nil)
}

type dummyEngine struct {
}

func (d *dummyEngine) setNodeReady(clusterID uint64)              {}
func (d *dummyEngine) setTaskReady(clusterID uint64)              {}
func (d *dummyEngine) setStreamReady(clusterID uint64)            {}
func (d *dummyEngine) setRequestedSnapshotReady(clusterID uint64) {}
func (d *dummyEngine) setAvailableSnapshotReady(clusterID uint64) {}

func doGetTestRaftNodes(startID uint64, count int, ordered bool,
	ldb raftio.ILogDB) ([]*node, []*rsm.StateMachine,
	*testMessageRouter, raftio.ILogDB) {
	nodes := make([]*node, 0)
	smList := make([]*rsm.StateMachine, 0)
	nodeIDList := make([]uint64, 0)
	// peers map
	peers := make(map[uint64]string)
	endID := startID + uint64(count-1)
	for i := startID; i <= endID; i++ {
		nodeIDList = append(nodeIDList, i)
		peers[i] = fmt.Sprintf("peer:%d", 12345+i)
	}
	// pools
	requestStatePool := &sync.Pool{}
	requestStatePool.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = requestStatePool
		return obj
	}
	var err error
	if ldb == nil {
		nodeLogDir := filepath.Join(raftTestTopDir, logdbDir)
		nodeLowLatencyLogDir := filepath.Join(raftTestTopDir, lowLatencyLogDBDir)
		if err := os.MkdirAll(nodeLogDir, 0755); err != nil {
			panic(err)
		}
		if err := os.MkdirAll(nodeLowLatencyLogDir, 0755); err != nil {
			panic(err)
		}
		ldb, err = logdb.NewDefaultLogDB([]string{nodeLogDir}, []string{nodeLowLatencyLogDir})
		if err != nil {
			plog.Panicf("failed to open logdb, %v", err)
		}
	}
	// message router
	router := newTestMessageRouter(testClusterID, nodeIDList)
	for i := startID; i <= endID; i++ {
		// create the snapshotter object
		nodeSnapDir := fmt.Sprintf(snapDir, testClusterID, i)
		snapdir := filepath.Join(raftTestTopDir, nodeSnapDir)
		if err := os.MkdirAll(snapdir, 0755); err != nil {
			panic(err)
		}
		rootDirFunc := func(cid uint64, nid uint64) string {
			return snapdir
		}
		snapshotter := newSnapshotter(testClusterID, i, rootDirFunc, ldb, nil)
		// create the sm
		sm := &tests.NoOP{}
		ds := rsm.NewNativeStateMachine(testClusterID,
			i, rsm.NewRegularStateMachine(sm), make(chan struct{}))
		// node registry
		nr := transport.NewNodes(settings.Soft.StreamConnections)
		config := config.Config{
			NodeID:              uint64(i),
			ClusterID:           testClusterID,
			ElectionRTT:         20,
			HeartbeatRTT:        2,
			CheckQuorum:         true,
			SnapshotEntries:     10,
			CompactionOverhead:  10,
			OrderedConfigChange: ordered,
		}
		addr := fmt.Sprintf("a%d", i)
		ch := router.getMessageReceiveChannel(testClusterID, uint64(i))
		node, err := newNode(addr,
			peers,
			true,
			snapshotter,
			ds,
			pb.RegularStateMachine,
			&dummyEngine{},
			nil,
			nil,
			router.sendMessage,
			ch,
			make(chan struct{}),
			nr,
			requestStatePool,
			config,
			tickMillisecond,
			ldb)
		if err != nil {
			panic(err)
		}
		nodes = append(nodes, node)
		smList = append(smList, node.sm)
	}
	return nodes, smList, router, ldb
}

var rtc raftio.IContext

func step(nodes []*node) bool {
	hasEvent := false
	nodeUpdates := make([]pb.Update, 0)
	activeNodes := make([]*node, 0)
	// step the events, collect all ready structs
	for _, node := range nodes {
		if !node.initialized() {
			commit := rsm.Task{
				InitialSnapshot: true,
			}
			index, _ := node.sm.RecoverFromSnapshot(commit)
			node.setInitialStatus(index)
		}
		if node.initialized() {
			if node.handleEvents() {
				hasEvent = true
				ud, ok := node.getUpdate()
				if ok {
					nodeUpdates = append(nodeUpdates, ud)
					activeNodes = append(activeNodes, node)
				}
				// quiesce state
				if node.newQuiesceState() {
					node.sendEnterQuiesceMessages()
				}
			}
		}
	}
	// batch the snapshot records together and store them into the logdb
	if err := nodes[0].logdb.SaveSnapshots(nodeUpdates); err != nil {
		panic(err)
	}
	for idx, ud := range nodeUpdates {
		node := activeNodes[idx]
		if _, err := node.processSnapshot(ud); err != nil {
			panic(err)
		}
		node.applyRaftUpdates(ud)
		node.sendReplicateMessages(ud)
		node.processReadyToRead(ud)
	}
	if rtc == nil {
		rtc = nodes[0].logdb.GetLogDBThreadContext()
	} else {
		rtc.Reset()
	}
	// persistent state and entries are saved first
	// then the snapshot. order can not be change.
	if err := nodes[0].logdb.SaveRaftState(nodeUpdates, rtc); err != nil {
		panic(err)
	}
	for idx, ud := range nodeUpdates {
		node := activeNodes[idx]
		running, err := node.processRaftUpdate(ud)
		if err != nil {
			panic(err)
		}
		node.commitRaftUpdate(ud)
		if ud.LastApplied-node.ss.getReqSnapshotIndex() > node.config.SnapshotEntries {
			if err := node.saveSnapshot(rsm.Task{}); err != nil {
				panic(err)
			}
		}
		if running {
			rec, err := node.sm.Handle(make([]rsm.Task, 0), nil)
			if err != nil {
				panic(err)
			}
			if rec.IsSnapshotTask() {
				if rec.SnapshotAvailable || rec.InitialSnapshot {
					if _, err := node.sm.RecoverFromSnapshot(rec); err != nil {
						panic(err)
					}
				} else if rec.SnapshotRequested {
					if err := node.saveSnapshot(rsm.Task{}); err != nil {
						panic(err)
					}
				}
			}
		}
	}
	return hasEvent
}

func singleStepNodes(nodes []*node, smList []*rsm.StateMachine,
	r *testMessageRouter) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	for _, node := range nodes {
		tickMsg := pb.Message{Type: pb.LocalTick, To: node.nodeID}
		tickMsg.ClusterId = testClusterID
		r.sendMessage(tickMsg)
	}
	step(nodes)
}

func stepNodes(nodes []*node, smList []*rsm.StateMachine,
	r *testMessageRouter, timeout uint64) {
	logdb.RDBContextValueSize = 1024 * 1024
	defer func() {
		logdb.RDBContextValueSize = ovs
	}()
	s := timeout/tickMillisecond + 10
	for i := uint64(0); i < s; i++ {
		for _, node := range nodes {
			tickMsg := pb.Message{Type: pb.LocalTick, To: node.nodeID}
			tickMsg.ClusterId = testClusterID
			r.sendMessage(tickMsg)
		}
		step(nodes)
	}
}

func stepNodesUntilThereIsLeader(nodes []*node, smList []*rsm.StateMachine,
	r *testMessageRouter) {
	count := 0
	for {
		stepNodes(nodes, smList, r, 1000)
		count++
		if isStableGroup(nodes) {
			stepNodes(nodes, smList, r, 2000)
			if isStableGroup(nodes) {
				return
			}
		}
		if count > 200 {
			panic("failed to has any leader after 200 second")
		}
	}
}

func isStableGroup(nodes []*node) bool {
	hasLeader := false
	inElection := false
	for _, node := range nodes {
		if node.isLeader() {
			hasLeader = true
			continue
		}
		if node.p == nil || !node.isFollower() {
			inElection = true
		}
	}
	return hasLeader && !inElection
}

func stopNodes(nodes []*node) {
	for _, node := range nodes {
		node.close()
	}
}

func TestNodeCanBeCreatedAndStarted(t *testing.T) {
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir()
	nodes, smList, router, ldb := getTestRaftNodes(3)
	if len(nodes) != 3 {
		t.Errorf("len(nodes)=%d, want 3", len(nodes))
	}
	if len(smList) != 3 {
		t.Errorf("len(smList)=%d, want 3", len(smList))
	}
	defer stopNodes(nodes)
	defer ldb.Close()
	stepNodesUntilThereIsLeader(nodes, smList, router)
}

func getMaxLastApplied(smList []*rsm.StateMachine) uint64 {
	maxLastApplied := uint64(0)
	for _, sm := range smList {
		la := sm.GetLastApplied()
		if la > maxLastApplied {
			maxLastApplied = la
		}
	}
	return maxLastApplied
}

func getProposalTestClient(n *node,
	nodes []*node, smList []*rsm.StateMachine,
	router *testMessageRouter) (*client.Session, bool) {
	cs := client.NewSession(n.clusterID, random.NewLockedRand())
	cs.PrepareForRegister()
	rs, err := n.pendingProposals.propose(cs, nil, nil, time.Duration(5*time.Second))
	if err != nil {
		plog.Errorf("error: %v", err)
		return nil, false
	}
	stepNodes(nodes, smList, router, 5000)
	select {
	case v := <-rs.CompletedC:
		if v.Completed() && v.GetResult().Value == cs.ClientID {
			cs.PrepareForPropose()
			return cs, true
		}
		plog.Infof("unknown result/code: %v", v)
	case <-n.stopc:
		plog.Errorf("stopc triggered")
		return nil, false
	}
	plog.Errorf("failed get test client")
	return nil, false
}

func closeProposalTestClient(n *node,
	nodes []*node, smList []*rsm.StateMachine,
	router *testMessageRouter, session *client.Session) {
	session.PrepareForUnregister()
	rs, err := n.pendingProposals.propose(session,
		nil, nil, time.Duration(5*time.Second))
	if err != nil {
		return
	}
	stepNodes(nodes, smList, router, 5000)
	select {
	case v := <-rs.CompletedC:
		if v.Completed() && v.GetResult().Value == session.ClientID {
			return
		}
	case <-n.stopc:
		return
	}
}

func getTestTimeout(timeoutInMillisecond uint64) time.Duration {
	return time.Duration(timeoutInMillisecond) * time.Millisecond
}

func makeCheckedTestProposal(t *testing.T, session *client.Session,
	data []byte, timeoutInMillisecond uint64,
	nodes []*node, smList []*rsm.StateMachine, router *testMessageRouter,
	expectedCode RequestResultCode, checkResult bool, expectedResult uint64) {
	n := mustHasLeaderNode(nodes, t)
	timeout := getTestTimeout(timeoutInMillisecond)
	rs, err := n.propose(session, data, nil, timeout)
	if err != nil {
		t.Fatalf("failed to make proposal")
	}
	stepNodes(nodes, smList, router, timeoutInMillisecond+1000)
	select {
	case v := <-rs.CompletedC:
		if v.code != expectedCode {
			t.Errorf("got %v, want %d", v, expectedCode)
		}
		if checkResult {
			if v.GetResult().Value != expectedResult {
				t.Errorf("result %d, want %d", v.GetResult(), expectedResult)
			}
		}
	default:
		t.Errorf("failed to complete the proposal")
	}
}

func runRaftNodeTest(t *testing.T, quiesce bool,
	tf func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB)) {
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir()
	nodes, smList, router, ldb := getTestRaftNodes(3)
	if quiesce {
		for idx := range nodes {
			(nodes[idx]).quiesceManager.enabled = true
		}
		for _, node := range nodes {
			if node.quiesced() {
				t.Errorf("node quiesced on startup")
			}
		}
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	defer stopNodes(nodes)
	defer ldb.Close()
	tf(t, nodes, smList, router, ldb)
}

func TestLastAppliedValueCanBeReturned(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		sm := smList[0]
		for i := uint64(5); i <= 100; i++ {
			sm.SetBatchedLastApplied(i)
			if !n.handleEvents() {
				t.Errorf("handle events reported no event")
			}
			ud, ok := n.getUpdate()
			if !ok {
				t.Errorf("no update")
			} else {
				if ud.LastApplied != i {
					t.Errorf("last applied value not returned, got %d want %d",
						ud.LastApplied, i)
				}
			}
			ud.UpdateCommit.LastApplied = 0
			n.p.Commit(ud)
		}
		if n.handleEvents() {
			t.Errorf("unexpected event")
		}
		if ud, ok := n.getUpdate(); ok {
			t.Errorf("unexpected update, %+v", ud)
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestLastAppliedValueIsAlwaysOneWayIncreasing(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("not panic")
			}
		}()
		n := nodes[0]
		sm := smList[0]
		sm.SetBatchedLastApplied(1)
		n.handleEvents()
		n.getUpdate()
	}
	runRaftNodeTest(t, false, tf)
}

func TestProposalCanBeMadeWithMessageDrops(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		router.dropRate = 3
		n := mustHasLeaderNode(nodes, t)
		var ok bool
		var session *client.Session
		for i := 0; i < 3; i++ {
			session, ok = getProposalTestClient(n, nodes, smList, router)
			if ok {
				break
			}
		}
		if session == nil {
			t.Errorf("failed to get session")
			return
		}
		for i := 0; i < 20; i++ {
			maxLastApplied := getMaxLastApplied(smList)
			makeCheckedTestProposal(t, session, []byte("test-data"), 4000,
				nodes, smList, router, requestCompleted, false, 0)
			session.ProposalCompleted()
			if getMaxLastApplied(smList) != maxLastApplied+1 {
				t.Errorf("didn't move the last applied value in smList")
			}
		}
		closeProposalTestClient(n, nodes, smList, router, session)
	}
	runRaftNodeTest(t, false, tf)
}

func TestLeaderIDCanBeQueried(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		v, ok := n.getLeaderID()
		if !ok {
			t.Errorf("failed to get leader id")
		}
		if v < 1 || v > 3 {
			t.Errorf("unexpected leader id %d", v)
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestMembershipCanBeLocallyRead(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		v, _, _, _ := n.sm.GetMembership()
		if len(v) != 3 {
			t.Errorf("unexpected member count %d", len(v))
		}
		addr1, ok := v[1]
		if !ok || addr1 != "peer:12346" {
			t.Errorf("unexpected membership data %v", v)
		}
		addr2, ok := v[2]
		if !ok || addr2 != "peer:12347" {
			t.Errorf("unexpected membership data %v", v)
		}
		addr3, ok := v[3]
		if !ok || addr3 != "peer:12348" {
			t.Errorf("unexpected membership data %v", v)
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestProposalWithClientSessionCanBeMade(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		data := []byte("test-data")
		maxLastApplied := getMaxLastApplied(smList)
		makeCheckedTestProposal(t, session, data, 4000,
			nodes, smList, router, requestCompleted, true, uint64(len(data)))

		if getMaxLastApplied(smList) != maxLastApplied+1 {
			t.Errorf("didn't move the last applied value in smList")
		}
		closeProposalTestClient(n, nodes, smList, router, session)
	}
	runRaftNodeTest(t, false, tf)
}

func TestProposalWithNotRegisteredClientWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		session.ClientID = 123456789
		data := []byte("test-data")
		maxLastApplied := getMaxLastApplied(smList)
		makeCheckedTestProposal(t, session, data, 2000,
			nodes, smList, router, requestRejected, true, 0)
		if getMaxLastApplied(smList) != maxLastApplied+1 {
			t.Errorf("didn't move the last applied value in smList")
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestDuplicatedProposalReturnsTheSameResult(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		data := []byte("test-data")
		maxLastApplied := getMaxLastApplied(smList)
		makeCheckedTestProposal(t, session, data, 2000,
			nodes, smList, router, requestCompleted, true, uint64(len(data)))
		if getMaxLastApplied(smList) != maxLastApplied+1 {
			t.Errorf("didn't move the last applied value in smList")
		}
		data = []byte("test-data-2")
		maxLastApplied = getMaxLastApplied(smList)
		makeCheckedTestProposal(t, session, data, 2000,
			nodes, smList, router, requestCompleted, true, uint64(len(data)-2))
		if getMaxLastApplied(smList) != maxLastApplied+1 {
			t.Errorf("didn't move the last applied value in smList")
		}
		closeProposalTestClient(n, nodes, smList, router, session)
	}
	runRaftNodeTest(t, false, tf)
}

func TestReproposeRespondedDataWillTimeout(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		data := []byte("test-data")
		maxLastApplied := getMaxLastApplied(smList)
		_, err := n.propose(session, data, nil, time.Duration(2*time.Second))
		if err != nil {
			t.Fatalf("failed to make proposal")
		}
		stepNodes(nodes, smList, router, 2000)
		if getMaxLastApplied(smList) != maxLastApplied+1 {
			t.Errorf("didn't move the last applied value in smList")
		}
		respondedSeriesID := session.SeriesID
		session.ProposalCompleted()
		for i := 0; i < 3; i++ {
			makeCheckedTestProposal(t, session, data, 2000,
				nodes, smList, router, requestCompleted, true, uint64(len(data)))
			session.ProposalCompleted()
			respondedSeriesID = session.RespondedTo
		}
		session.SeriesID = respondedSeriesID
		plog.Infof("series id %d, responded to %d",
			session.SeriesID, session.RespondedTo)
		rs, _ := n.propose(session, data, nil, time.Duration(2*time.Second))
		stepNodes(nodes, smList, router, 2000)
		select {
		case v := <-rs.CompletedC:
			if !v.Timeout() {
				t.Errorf("didn't timeout, v: %d", v.code)
			}
		default:
			t.Errorf("failed to complete the proposal")
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestProposalsWithIllFormedSessionAreChecked(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		s1 := client.NewSession(n.clusterID, random.NewLockedRand())
		s1.SeriesID = client.SeriesIDForRegister
		_, err := n.propose(s1, nil, nil, time.Second)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.clusterID, random.NewLockedRand())
		s1.SeriesID = client.SeriesIDForUnregister
		_, err = n.propose(s1, nil, nil, time.Second)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.clusterID, random.NewLockedRand())
		s1.SeriesID = 100
		s1.ClusterID = 123456
		_, err = n.propose(s1, nil, nil, time.Second)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.clusterID, random.NewLockedRand())
		s1.SeriesID = 1
		s1.ClientID = 0
		_, err = n.propose(s1, nil, nil, time.Second)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestProposalsWithCorruptedSessionWillPanic(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		s1 := client.NewSession(n.clusterID, random.NewLockedRand())
		s1.SeriesID = 100
		s1.RespondedTo = 200
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("panic not triggered")
			}
		}()
		_, err := n.propose(s1, nil, nil, time.Second)
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestRaftNodeQuiesceCanBeDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// quiesce is disabled by default
	defer cleanupTestDir()
	nodes, smList, router, ldb := getTestRaftNodes(3)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	for _, node := range nodes {
		if node.quiesced() {
			t.Errorf("node quiesced on startup")
		}
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	defer stopNodes(nodes)
	defer ldb.Close()
	// need to step more than quiesce.threshold() as the startup
	// config change messages are going to be recorded as activities
	for i := uint64(0); i <= nodes[0].quiesceThreshold()*2; i++ {
		singleStepNodes(nodes, smList, router)
	}
	for _, node := range nodes {
		if node.quiesced() {
			t.Errorf("node is quiesced when quiesce is not enabled")
		}
	}
}

func TestNodesCanEnterQuiesce(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].quiesceThreshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		// step more, nodes should stay in quiesce state.
		for i := uint64(0); i <= nodes[0].quiesceThreshold()*3; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
	}
	runRaftNodeTest(t, true, tf)
}

func TestNodesCanExitQuiesceByMakingProposal(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].quiesceThreshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		n := nodes[0]
		done := false
		for i := 0; i < 5; i++ {
			_, ok := getProposalTestClient(n, nodes, smList, router)
			if ok {
				done = true
				break
			}
		}
		if !done {
			t.Errorf("failed to get proposal client -- didn't exit from quiesce?")
		}
		for i := uint64(0); i <= 3; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if node.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	runRaftNodeTest(t, true, tf)
}

func TestNodesCanExitQuiesceByReadIndex(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].quiesceThreshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		n := nodes[0]
		rs, err := n.read(nil, time.Second)
		if err != nil {
			t.Errorf("failed to read")
		}
		var done bool
		for i := uint64(0); i <= 5; i++ {
			singleStepNodes(nodes, smList, router)
			select {
			case <-rs.CompletedC:
				done = true
			default:
			}
			if done {
				break
			}
		}
		for _, node := range nodes {
			if node.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	runRaftNodeTest(t, true, tf)
}

func TestNodesCanExitQuiesceByConfigChange(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].quiesceThreshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		n := nodes[0]
		done := false
		for i := 0; i < 5; i++ {
			rs, err := n.requestAddNodeWithOrderID(24680, "localhost:12345", 0, time.Second)
			if err != nil {
				t.Errorf("request to add node failed, %v", err)
			}
			hasResp := false
			for i := uint64(0); i < 25; i++ {
				singleStepNodes(nodes, smList, router)
				select {
				case v := <-rs.CompletedC:
					if v.Completed() {
						done = true
					}
					hasResp = true
				default:
					continue
				}
			}
			if !hasResp {
				t.Errorf("config change timeout not fired")
				return
			}
			if done {
				break
			}
		}
		for i := uint64(0); i < 20; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if node.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	runRaftNodeTest(t, true, tf)
}

func TestLinearizableReadCanBeMade(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		rs, err := n.propose(session, []byte("test-data"), nil, time.Duration(2*time.Second))
		if err != nil {
			t.Fatalf("failed to make proposal")
		}
		stepNodes(nodes, smList, router, 2000)
		mustComplete(rs, t)
		closeProposalTestClient(n, nodes, smList, router, session)
		rs, err = n.read(nil, time.Duration(2*time.Second))
		if err != nil {
			t.Fatalf("")
		}
		if rs.node == nil {
			t.Fatalf("rs.node not set")
		}
		stepNodes(nodes, smList, router, 2500)
		mustComplete(rs, t)
	}
	runRaftNodeTest(t, false, tf)
}

func testNodeCanBeAdded(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		router.dropRate = 3
		n := mustHasLeaderNode(nodes, t)
		rs, err := n.requestAddNodeWithOrderID(4, "a4:4", 0, time.Duration(5*time.Second))
		if err != nil {
			t.Fatalf("request to delete node failed")
		}
		stepNodes(nodes, smList, router, 6000)
		mustComplete(rs, t)
		for _, node := range nodes {
			if !sliceEqual([]uint64{1, 2, 3, 4}, getMemberNodes(node.sm)) {
				t.Errorf("failed to delete the node, %v", getMemberNodes(node.sm))
			}
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestNodeCanBeAddedWithMessageDrops(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i := 0; i < 10; i++ {
		testNodeCanBeAdded(t)
	}
}

func TestNodeCanBeDeleted(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		rs, err := n.requestDeleteNodeWithOrderID(2, 0, time.Duration(2*time.Second))
		if err != nil {
			t.Fatalf("request to delete node failed")
		}
		stepNodes(nodes, smList, router, 2000)
		mustComplete(rs, t)
		if nodes[0].stopped() {
			t.Errorf("node id 1 is not suppose to be in stopped state")
		}
		if !nodes[1].stopped() {
			t.Errorf("node is not stopped")
		}
		if nodes[2].stopped() {
			t.Errorf("node id 3 is not suppose to be in stopped state")
		}
	}
	runRaftNodeTest(t, false, tf)
}

func sliceEqual(s1 []uint64, s2 []uint64) bool {
	if len(s1) != len(s2) {
		return false
	}
	sort.Slice(s1, func(i, j int) bool { return s1[i] < s1[j] })
	sort.Slice(s2, func(i, j int) bool { return s2[i] < s2[j] })
	for idx, v := range s1 {
		if v != s2[idx] {
			return false
		}
	}
	return true
}

func TestNodeCanBeAdded2(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		for i := 0; i < 5; i++ {
			rs, err := n.propose(session, []byte("test-data"), nil, time.Duration(2*time.Second))
			if err != nil {
				t.Fatalf("")
			}
			stepNodes(nodes, smList, router, 2000)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		rs, err := n.requestAddNodeWithOrderID(4, "a4:4", 0, time.Duration(2*time.Second))
		if err != nil {
			t.Fatalf("request to add node failed")
		}
		stepNodes(nodes, smList, router, 2000)
		mustComplete(rs, t)
		for _, node := range nodes {
			if node.stopped() {
				t.Errorf("node %d is stopped, this is unexpected", node.nodeID)
			}
			if !sliceEqual([]uint64{1, 2, 3, 4}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
		// now bring the node 5 online
		newNodes, newSMList, newRouter, _ := doGetTestRaftNodes(4, 1, true, ldb)
		if len(newNodes) != 1 {
			t.Fatalf("failed to get 1 nodes")
		}
		router.addChannel(4, newRouter.msgReceiveCh[4])
		nodes = append(nodes, newNodes[0])
		smList = append(smList, newSMList[0])
		nodes[3].sendRaftMessage = router.sendMessage
		stepNodes(nodes, smList, router, 10000)
		if smList[0].GetLastApplied() != newSMList[0].GetLastApplied() {
			t.Errorf("last applied: %d, want %d",
				newSMList[0].GetLastApplied(), smList[0].GetLastApplied())
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestNodeCanBeAddedWhenOrderIsEnforced(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir()
	nodes, smList, router, ldb := doGetTestRaftNodes(1, 3, true, nil)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	defer stopNodes(nodes)
	defer ldb.Close()
	n := nodes[0]
	rs, err := n.requestAddNodeWithOrderID(5, "a5:5", 0, time.Duration(2*time.Second))
	if err != nil {
		t.Fatalf("request to add node failed")
	}
	stepNodes(nodes, smList, router, 2000)
	mustReject(rs, t)
	for _, node := range nodes {
		if node.stopped() {
			t.Errorf("node %d is stopped, this is unexpected", node.nodeID)
		}
		if !sliceEqual([]uint64{1, 2, 3}, getMemberNodes(node.sm)) {
			t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
		}
	}
	_, _, _, ccid := n.sm.GetMembership()
	rs, err = n.requestAddNodeWithOrderID(5, "a5:5", ccid, time.Duration(2*time.Second))
	if err != nil {
		t.Fatalf("request to add node failed")
	}
	stepNodes(nodes, smList, router, 2000)
	mustComplete(rs, t)
	for _, node := range nodes {
		if node.stopped() {
			t.Errorf("node %d is stopped, this is unexpected", node.nodeID)
		}
		if !sliceEqual([]uint64{1, 2, 3, 5}, getMemberNodes(node.sm)) {
			t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
		}
	}
}

func TestNodeCanBeDeletedWhenOrderIsEnforced(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir()
	nodes, smList, router, ldb := doGetTestRaftNodes(1, 3, true, nil)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	defer stopNodes(nodes)
	defer ldb.Close()
	n := nodes[0]
	rs, err := n.requestDeleteNodeWithOrderID(2, 0, time.Duration(2*time.Second))
	if err != nil {
		t.Fatalf("request to delete node failed")
	}
	stepNodes(nodes, smList, router, 2000)
	mustReject(rs, t)
	for _, node := range nodes {
		if node.stopped() {
			t.Errorf("node %d is stopped, this is unexpected", node.nodeID)
		}
		if !sliceEqual([]uint64{1, 2, 3}, getMemberNodes(node.sm)) {
			t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
		}
	}
	_, _, _, ccid := n.sm.GetMembership()
	rs, err = n.requestDeleteNodeWithOrderID(2, ccid, time.Duration(2*time.Second))
	if err != nil {
		t.Fatalf("request to add node failed")
	}
	stepNodes(nodes, smList, router, 2000)
	mustComplete(rs, t)
	for _, node := range nodes {
		if node.nodeID == 2 {
			continue
		}
		if !sliceEqual([]uint64{1, 3}, getMemberNodes(node.sm)) {
			t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
		}
	}
}

func getSnapshotFileCount(dir string) (int, error) {
	fiList, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, f := range fiList {
		if !f.IsDir() {
			continue
		}
		if strings.HasPrefix(f.Name(), "snapshot-") {
			count++
		}
	}
	return count, nil
}

func TestSnapshotCanBeMade(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		maxLastApplied := getMaxLastApplied(smList)
		proposalCount := 50
		for i := 0; i < proposalCount; i++ {
			data := fmt.Sprintf("test-data-%d", i)
			rs, err := n.propose(session, []byte(data), nil, time.Second)
			if err != nil {
				t.Fatalf("failed to make proposal")
			}
			stepNodes(nodes, smList, router, 1000)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		if getMaxLastApplied(smList) != maxLastApplied+uint64(proposalCount) {
			t.Errorf("not all %d proposals applied", proposalCount)
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		// check we do have snapshots saved on disk
		for _, node := range nodes {
			sd := fmt.Sprintf(snapDir, testClusterID, node.nodeID)
			dir := filepath.Join(raftTestTopDir, sd)
			count, err := getSnapshotFileCount(dir)
			if err != nil {
				t.Fatalf("failed to get snapshot count")
			}
			if count < 3 {
				t.Errorf("%s has less than 3 snapshot images", dir)
			}
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestSnapshotCanBeMadeTwice(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testMessageRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		maxLastApplied := getMaxLastApplied(smList)
		proposalCount := 50
		for i := 0; i < proposalCount; i++ {
			data := fmt.Sprintf("test-data-%d", i)
			rs, err := n.propose(session, []byte(data), nil, time.Second)
			if err != nil {
				t.Fatalf("failed to make proposal")
			}
			stepNodes(nodes, smList, router, 1000)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		if getMaxLastApplied(smList) != maxLastApplied+uint64(proposalCount) {
			t.Errorf("not all %d proposals applied", proposalCount)
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		// check we do have snapshots saved on disk
		for _, node := range nodes {
			if err := node.saveSnapshot(rsm.Task{}); err != nil {
				t.Fatalf("%v", err)
			}
			if err := node.saveSnapshot(rsm.Task{}); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	runRaftNodeTest(t, false, tf)
}

func TestNodesCanBeRestarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir()
	nodes, smList, router, ldb := getTestRaftNodes(3)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	n := mustHasLeaderNode(nodes, t)
	session, ok := getProposalTestClient(n, nodes, smList, router)
	if !ok {
		t.Errorf("failed to get session")
		return
	}
	maxLastApplied := getMaxLastApplied(smList)
	for i := 0; i < 25; i++ {
		rs, err := n.propose(session, []byte("test-data"), nil, time.Duration(5*time.Second))
		if err != nil {
			t.Fatalf("")
		}
		stepNodes(nodes, smList, router, 5500)
		mustComplete(rs, t)
		session.ProposalCompleted()
	}
	if getMaxLastApplied(smList) != maxLastApplied+25 {
		t.Errorf("not all %d proposals applied", 25)
	}
	closeProposalTestClient(n, nodes, smList, router, session)
	for _, node := range nodes {
		sd := fmt.Sprintf(snapDir, testClusterID, node.nodeID)
		dir := filepath.Join(raftTestTopDir, sd)
		count, err := getSnapshotFileCount(dir)
		if err != nil {
			t.Fatalf("failed to get snapshot count")
		}
		if count == 0 {
			t.Fatalf("no snapshot available, count: %d", count)
		}
	}
	// stop the whole thing
	for _, node := range nodes {
		node.close()
	}
	ldb.Close()
	// restart
	nodes, smList, router, ldb = getTestRaftNodes(3)
	defer stopNodes(nodes)
	defer ldb.Close()
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	stepNodes(nodes, smList, router, 5000)
	if getMaxLastApplied(smList) < maxLastApplied+5 {
		t.Errorf("not recovered from snapshot, got %d, marker %d",
			getMaxLastApplied(smList), maxLastApplied+5)
	}
}

func TestGetTimeoutMillisecondFromContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, err := getTimeoutFromContext(context.Background())
	if err != ErrDeadlineNotSet {
		t.Errorf("err %v, want ErrDeadlineNotSet", err)
	}
	d := time.Now()
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	_, err = getTimeoutFromContext(ctx)
	if err != ErrInvalidDeadline {
		t.Errorf("err %v, want ErrInvalidDeadline", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := getTimeoutFromContext(ctx)
	if err != nil {
		t.Errorf("err %v, want nil", err)
	}
	timeout := v.Nanoseconds() / 1000000
	if timeout <= 4500 || timeout > 5000 {
		t.Errorf("v %d, want [4500,5000]", timeout)
	}
}

func TestPayloadTooBig(t *testing.T) {
	tests := []struct {
		maxInMemLogSize uint64
		payloadSize     uint64
		tooBig          bool
	}{
		{0, 1, false},
		{0, 1024 * 1024 * 1024, false},
		{settings.EntryNonCmdFieldsSize + 1, 1, false},
		{settings.EntryNonCmdFieldsSize + 1, 2, true},
		{settings.EntryNonCmdFieldsSize * 2, settings.EntryNonCmdFieldsSize, false},
		{settings.EntryNonCmdFieldsSize * 2, settings.EntryNonCmdFieldsSize + 1, true},
	}
	for idx, tt := range tests {
		cfg := config.Config{
			NodeID:          1,
			HeartbeatRTT:    1,
			ElectionRTT:     10,
			MaxInMemLogSize: tt.maxInMemLogSize,
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("invalid cfg %v", err)
		}
		n := node{config: cfg}
		if n.payloadTooBig(int(tt.payloadSize)) != tt.tooBig {
			t.Errorf("%d, unexpected too big result %t", idx, tt.tooBig)
		}
	}
}

//
// node states
//

func TestProcessUninitilizedNode(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	_, ok := n.getUninitializedNodeTask()
	if !ok {
		t.Errorf("failed to returned the recover request")
	}
	n2 := &node{ss: &snapshotState{}}
	n2.initializedMu.initialized = true
	_, ok = n2.getUninitializedNodeTask()
	if ok {
		t.Errorf("unexpected recover from snapshot request")
	}
}

func TestProcessRecoveringNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	if n.processRecoverSnapshotStatus() {
		t.Errorf("processRecoveringNode not skipped")
	}
}

func TestProcessTakingSnapshotNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	if n.processTakeSnapshotStatus() {
		t.Errorf("processTakingSnapshotNode not skipped")
	}
}

func TestRecoveringFromSnapshotNodeCanComplete(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setRecoveringFromSnapshot()
	n.ss.notifySnapshotStatus(false, true, false, true, 100)
	if n.processRecoverSnapshotStatus() {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.recoveringFromSnapshot() {
		t.Errorf("still recovering")
	}
	if !n.initialized() {
		t.Errorf("not marked as initialized")
	}
	if n.ss.snapshotIndex != 100 {
		t.Errorf("unexpected snapshot index %d, want 100", n.ss.snapshotIndex)
	}
}

func TestNotReadyRecoveringFromSnapshotNode(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setRecoveringFromSnapshot()
	if !n.processRecoverSnapshotStatus() {
		t.Errorf("not skipped")
	}
}

func TestTakingSnapshotNodeCanComplete(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setTakingSnapshot()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.initializedMu.initialized = true
	if n.processTakeSnapshotStatus() {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.takingSnapshot() {
		t.Errorf("still taking snapshot")
	}
}

func TestTakingSnapshotOnUninitializedNodeWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	n := &node{ss: &snapshotState{}}
	n.ss.setTakingSnapshot()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.processTakeSnapshotStatus()
}

type testDummyNodeProxy struct{}

func (np *testDummyNodeProxy) NodeReady()                                        {}
func (np *testDummyNodeProxy) RestoreRemotes(pb.Snapshot)                        {}
func (np *testDummyNodeProxy) ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool) {}
func (np *testDummyNodeProxy) ApplyConfigChange(pb.ConfigChange)                 {}
func (np *testDummyNodeProxy) ConfigChangeProcessed(uint64, bool)                {}
func (np *testDummyNodeProxy) NodeID() uint64                                    { return 1 }
func (np *testDummyNodeProxy) ClusterID() uint64                                 { return 1 }
func (np *testDummyNodeProxy) ShouldStop() <-chan struct{}                       { return nil }

func TestNotReadyTakingSnapshotNodeIsSkippedWhenConcurrencyIsNotSupported(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeStateMachine(1, 1, &rsm.RegularStateMachine{}, nil), nil, false, &testDummyNodeProxy{})
	if n.concurrentSnapshot() {
		t.Errorf("concurrency not suppose to be supported")
	}
	n.ss.setTakingSnapshot()
	n.initializedMu.initialized = true
	if !n.processTakeSnapshotStatus() {
		t.Fatalf("node not skipped")
	}
}

func TestNotReadyTakingSnapshotNodeIsNotSkippedWhenConcurrencyIsSupported(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeStateMachine(1, 1, &rsm.ConcurrentStateMachine{}, nil), nil, false, &testDummyNodeProxy{})
	if !n.concurrentSnapshot() {
		t.Errorf("concurrency not supported")
	}
	n.ss.setTakingSnapshot()
	n.initializedMu.initialized = true
	if n.processTakeSnapshotStatus() {
		t.Fatalf("node unexpectedly skipped")
	}
}
