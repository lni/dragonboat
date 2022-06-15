// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/raft"
	"github.com/lni/dragonboat/v4/internal/registry"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/tests"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	raftTestTopDir            = "raft_node_test_safe_to_delete"
	logdbDir                  = "logdb_test_dir_safe_to_delete"
	lowLatencyLogDBDir        = "logdb_ll_test_dir_safe_to_delete"
	snapDir                   = "snap_test_dir_safe_to_delete/snap-%d-%d"
	testShardID        uint64 = 1100
	tickMillisecond    uint64 = 50
)

func getMemberNodes(r *rsm.StateMachine) []uint64 {
	m := r.GetMembership()
	n := make([]uint64, 0)
	for nid := range m.Addresses {
		n = append(n, nid)
	}
	return n
}

func mustComplete(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Fatalf("got %v, want %v", v.code, requestCompleted)
		}
	default:
		t.Fatalf("failed to complete the proposal")
	}
}

func mustReject(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.ResultC():
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

type testRouter struct {
	shardID  uint64
	qm       map[uint64]*server.MessageQueue
	dropRate uint8
}

func newTestRouter(shardID uint64, replicaIDList []uint64) *testRouter {
	m := make(map[uint64]*server.MessageQueue)
	for _, replicaID := range replicaIDList {
		m[replicaID] = server.NewMessageQueue(1000, false, 0, 1024*1024*256)
	}
	return &testRouter{qm: m, shardID: shardID}
}

func (r *testRouter) shouldDrop(msg pb.Message) bool {
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

func (r *testRouter) send(msg pb.Message) {
	if msg.ShardID != r.shardID {
		panic("shard id does not match")
	}
	if r.shouldDrop(msg) {
		return
	}
	if q, ok := r.qm[msg.To]; ok {
		q.Add(msg)
	}
}

func (r *testRouter) getQ(shardID uint64,
	replicaID uint64) *server.MessageQueue {
	if shardID != r.shardID {
		panic("shard id does not match")
	}
	q, ok := r.qm[replicaID]
	if !ok {
		panic("node id not found in the test msg router")
	}
	return q
}

func (r *testRouter) addQ(replicaID uint64, q *server.MessageQueue) {
	r.qm[replicaID] = q
}

func cleanupTestDir(fs vfs.IFS) {
	if err := fs.RemoveAll(raftTestTopDir); err != nil {
		panic(err)
	}
}

func getTestRaftNodes(count int, ordered bool,
	fs vfs.IFS) ([]*node, []*rsm.StateMachine, *testRouter, raftio.ILogDB) {
	return doGetTestRaftNodes(1, count, ordered, nil, fs)
}

type dummyEngine struct {
}

func (d *dummyEngine) setCloseReady(n *node)          {}
func (d *dummyEngine) setStepReady(shardID uint64)    {}
func (d *dummyEngine) setCommitReady(shardID uint64)  {}
func (d *dummyEngine) setApplyReady(shardID uint64)   {}
func (d *dummyEngine) setStreamReady(shardID uint64)  {}
func (d *dummyEngine) setSaveReady(shardID uint64)    {}
func (d *dummyEngine) setRecoverReady(shardID uint64) {}

func doGetTestRaftNodes(startID uint64, count int, ordered bool,
	ldb raftio.ILogDB, fs vfs.IFS) ([]*node, []*rsm.StateMachine,
	*testRouter, raftio.ILogDB) {
	nodes := make([]*node, 0)
	smList := make([]*rsm.StateMachine, 0)
	replicaIDList := make([]uint64, 0)
	// peers map
	peers := make(map[uint64]string)
	endID := startID + uint64(count-1)
	for i := startID; i <= endID; i++ {
		replicaIDList = append(replicaIDList, i)
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
		nodeLogDir := fs.PathJoin(raftTestTopDir, logdbDir)
		nodeLowLatencyLogDir := fs.PathJoin(raftTestTopDir, lowLatencyLogDBDir)
		if err := fs.MkdirAll(nodeLogDir, 0755); err != nil {
			panic(err)
		}
		if err := fs.MkdirAll(nodeLowLatencyLogDir, 0755); err != nil {
			panic(err)
		}
		cfg := config.NodeHostConfig{
			Expert: config.GetDefaultExpertConfig(),
		}
		cfg.Expert.LogDB.Shards = 2
		cfg.Expert.FS = fs
		ldb, err = logdb.NewDefaultLogDB(cfg,
			nil, []string{nodeLogDir}, []string{nodeLowLatencyLogDir})
		if err != nil {
			plog.Panicf("failed to open logdb, %+v", err)
		}
	}
	// message router
	router := newTestRouter(testShardID, replicaIDList)
	for i := startID; i <= endID; i++ {
		// create the snapshotter object
		nodeSnapDir := fmt.Sprintf(snapDir, testShardID, i)
		snapdir := fs.PathJoin(raftTestTopDir, nodeSnapDir)
		if err := fs.MkdirAll(snapdir, 0755); err != nil {
			panic(err)
		}
		rootDirFunc := func(cid uint64, nid uint64) string {
			return snapdir
		}
		lr := logdb.NewLogReader(testShardID, i, ldb)
		snapshotter := newSnapshotter(testShardID, i, rootDirFunc, ldb, lr, fs)
		lr.SetCompactor(snapshotter)
		// create the sm
		noopSM := &tests.NoOP{}
		cfg := config.Config{
			ReplicaID:           i,
			ShardID:             testShardID,
			ElectionRTT:         20,
			HeartbeatRTT:        2,
			CheckQuorum:         true,
			SnapshotEntries:     10,
			CompactionOverhead:  10,
			OrderedConfigChange: ordered,
		}
		create := func(shardID uint64, replicaID uint64,
			done <-chan struct{}) rsm.IManagedStateMachine {
			return rsm.NewNativeSM(cfg, rsm.NewInMemStateMachine(noopSM), done)
		}
		// node registry
		nr := registry.NewNodeRegistry(settings.Soft.StreamConnections, nil)
		ch := router.getQ(testShardID, i)
		nhConfig := config.NodeHostConfig{RTTMillisecond: tickMillisecond}
		node, err := newNode(peers,
			true,
			cfg,
			nhConfig,
			create,
			snapshotter,
			lr,
			&dummyEngine{},
			nil,
			nil,
			nil,
			router.send,
			nr,
			requestStatePool,
			ldb,
			nil,
			newSysEventListener(nil, nil))
		if err != nil {
			panic(err)
		}
		node.mq = ch
		nodes = append(nodes, node)
		smList = append(smList, node.sm)
	}
	return nodes, smList, router, ldb
}

func step(nodes []*node) bool {
	hasEvent := false
	nodeUpdates := make([]pb.Update, 0)
	activeNodes := make([]*node, 0)
	// step the events, collect all ready structs
	for _, node := range nodes {
		if !node.initialized() {
			commit := rsm.Task{Initial: true}
			ss, _ := node.sm.Recover(commit)
			node.setInitialStatus(ss.Index)
		}
		if node.initialized() {
			hasEvent, err := node.handleEvents()
			if err != nil {
				panic(err)
			}
			if hasEvent {
				ud, ok, err := node.getUpdate()
				if err != nil {
					panic(err)
				}
				if ok {
					nodeUpdates = append(nodeUpdates, ud)
					activeNodes = append(activeNodes, node)
				}
				// quiesce state
				if node.qs.newQuiesceState() {
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
		if err := node.processSnapshot(ud); err != nil {
			panic(err)
		}
		node.applyRaftUpdates(ud)
		node.sendReplicateMessages(ud)
		node.processReadyToRead(ud)
		node.processLeaderUpdate(ud.LeaderUpdate)
	}
	// persistent state and entries are saved first
	// then the snapshot. order can not be changed.
	if err := nodes[0].logdb.SaveRaftState(nodeUpdates, 1); err != nil {
		panic(err)
	}
	for idx, ud := range nodeUpdates {
		node := activeNodes[idx]
		if err := node.processRaftUpdate(ud); err != nil {
			panic(err)
		}
		node.commitRaftUpdate(ud)
		if ud.LastApplied-node.ss.getReqIndex() > node.config.SnapshotEntries {
			if err := node.save(rsm.Task{}); err != nil {
				panic(err)
			}
		}
		rec, err := node.sm.Handle(make([]rsm.Task, 0), nil)
		if err != nil {
			panic(err)
		}
		if rec.IsSnapshotTask() {
			if rec.Recover || rec.Initial {
				if _, err := node.sm.Recover(rec); err != nil {
					panic(err)
				}
			} else if rec.Save {
				if err := node.save(rsm.Task{}); err != nil {
					panic(err)
				}
			}
		}
	}
	return hasEvent
}

func singleStepNodes(nodes []*node, smList []*rsm.StateMachine,
	r *testRouter) {
	for _, node := range nodes {
		tick := node.pendingReadIndexes.getTick() + 1
		tickMsg := pb.Message{Type: pb.LocalTick, To: node.replicaID, Hint: tick}
		tickMsg.ShardID = testShardID
		r.send(tickMsg)
	}
	step(nodes)
}

func stepNodes(nodes []*node, smList []*rsm.StateMachine,
	r *testRouter, ticks uint64) {
	s := ticks + 10
	for i := uint64(0); i < s; i++ {
		for _, node := range nodes {
			tick := node.pendingReadIndexes.getTick() + 1
			tickMsg := pb.Message{
				Type:    pb.LocalTick,
				To:      node.replicaID,
				ShardID: testShardID,
				Hint:    tick,
			}
			r.send(tickMsg)
		}
		step(nodes)
	}
}

func stepNodesUntilThereIsLeader(nodes []*node, smList []*rsm.StateMachine,
	r *testRouter) {
	count := 0
	for {
		stepNodes(nodes, smList, r, 1)
		count++
		if isStableGroup(nodes) {
			stepNodes(nodes, smList, r, 10)
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
		if !node.isFollower() {
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
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir(fs)
	nodes, smList, router, ldb := getTestRaftNodes(3, false, fs)
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
	router *testRouter) (*client.Session, bool) {
	cs := client.NewSession(n.shardID, random.NewLockedRand())
	cs.PrepareForRegister()
	rs, err := n.pendingProposals.propose(cs, nil, 50)
	if err != nil {
		plog.Errorf("error: %v", err)
		return nil, false
	}
	stepNodes(nodes, smList, router, 50)
	select {
	case v := <-rs.ResultC():
		if v.Completed() && v.GetResult().Value == cs.ClientID {
			cs.PrepareForPropose()
			return cs, true
		}
		plog.Infof("unknown result/code: %v", v)
	case <-n.stopC:
		plog.Errorf("stopc triggered")
		return nil, false
	}
	plog.Errorf("failed get test client")
	return nil, false
}

func closeProposalTestClient(n *node,
	nodes []*node, smList []*rsm.StateMachine,
	router *testRouter, session *client.Session) {
	session.PrepareForUnregister()
	rs, err := n.pendingProposals.propose(session, nil, 50)
	if err != nil {
		return
	}
	stepNodes(nodes, smList, router, 50)
	select {
	case v := <-rs.ResultC():
		if v.Completed() && v.GetResult().Value == session.ClientID {
			return
		}
	case <-n.stopC:
		return
	}
}

func makeCheckedTestProposal(t *testing.T, session *client.Session,
	data []byte, timeoutInMillisecond uint64,
	nodes []*node, smList []*rsm.StateMachine, router *testRouter,
	expectedCode RequestResultCode, checkResult bool, expectedResult uint64) {
	n := mustHasLeaderNode(nodes, t)
	tick := uint64(50)
	rs, err := n.propose(session, data, tick)
	if err != nil {
		t.Fatalf("failed to make proposal")
	}
	stepNodes(nodes, smList, router, tick)
	select {
	case v := <-rs.ResultC():
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

func runRaftNodeTest(t *testing.T, quiesce bool, ordered bool,
	tf func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir(fs)
	nodes, smList, router, ldb := getTestRaftNodes(3, ordered, fs)
	if quiesce {
		for idx := range nodes {
			(nodes[idx]).qs.enabled = true
		}
		for _, node := range nodes {
			if node.qs.quiesced() {
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
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		sm := smList[0]
		for i := uint64(5); i <= 100; i++ {
			sm.SetLastApplied(i)
			hasEvent, err := n.handleEvents()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if !hasEvent {
				t.Errorf("handle events reported no event")
			}
			ud, ok, err := n.getUpdate()
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
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
		hasEvents, err := n.handleEvents()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if hasEvents {
			t.Errorf("unexpected event")
		}
		ud, ok, err := n.getUpdate()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if ok {
			t.Errorf("unexpected update, %+v", ud)
		}
	}
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestLastAppliedValueIsAlwaysOneWayIncreasing(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("not panic")
			}
		}()
		n := nodes[0]
		sm := smList[0]
		sm.SetLastApplied(1)
		if _, err := n.handleEvents(); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if _, _, err := n.getUpdate(); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
	}
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposalCanBeMadeWithMessageDrops(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestLeaderIDCanBeQueried(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		v, _, ok := n.getLeaderID()
		if !ok {
			t.Errorf("failed to get leader id")
		}
		if v < 1 || v > 3 {
			t.Errorf("unexpected leader id %d", v)
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestMembershipCanBeLocallyRead(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		m := n.sm.GetMembership()
		v := m.Addresses
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestConfigChangeOnWitnessWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		n.config.IsWitness = true
		_, err := n.requestConfigChange(pb.AddNode, 100, "noidea:9090", 0, 10)
		if err != ErrInvalidOperation {
			t.Errorf("config change not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestReadOnWitnessWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		n.config.IsWitness = true
		_, err := n.read(10)
		if err != ErrInvalidOperation {
			t.Errorf("read not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestMakingProposalOnWitnessNodeWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		n.config.IsWitness = true
		cs := client.NewNoOPSession(n.shardID, random.NewLockedRand())
		_, err := n.propose(cs, make([]byte, 1), 10)
		if err != ErrInvalidOperation {
			t.Errorf("making proposal not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposingSessionOnWitnessNodeWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		n.config.IsWitness = true
		_, err := n.proposeSession(nil, 10)
		if err != ErrInvalidOperation {
			t.Errorf("proposing session not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestRequestingSnapshotOnWitnessWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		n.config.IsWitness = true
		_, err := n.requestSnapshot(SnapshotOption{}, 10)
		if err != ErrInvalidOperation {
			t.Errorf("requesting snapshot not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposalWithClientSessionCanBeMade(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposalWithNotRegisteredClientWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestDuplicatedProposalReturnsTheSameResult(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestReproposeRespondedDataWillTimeout(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		data := []byte("test-data")
		maxLastApplied := getMaxLastApplied(smList)
		_, err := n.propose(session, data, 10)
		if err != nil {
			t.Fatalf("failed to make proposal")
		}
		stepNodes(nodes, smList, router, 10)
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
		rs, _ := n.propose(session, data, 10)
		stepNodes(nodes, smList, router, 10)
		select {
		case v := <-rs.ResultC():
			if !v.Timeout() {
				t.Errorf("didn't timeout, v: %d", v.code)
			}
		default:
			t.Errorf("failed to complete the proposal")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposalsWithIllFormedSessionAreChecked(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		s1 := client.NewSession(n.shardID, random.NewLockedRand())
		s1.SeriesID = client.SeriesIDForRegister
		_, err := n.propose(s1, nil, 10)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.shardID, random.NewLockedRand())
		s1.SeriesID = client.SeriesIDForUnregister
		_, err = n.propose(s1, nil, 10)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.shardID, random.NewLockedRand())
		s1.SeriesID = 100
		s1.ShardID = 123456
		_, err = n.propose(s1, nil, 10)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
		s1 = client.NewSession(n.shardID, random.NewLockedRand())
		s1.SeriesID = 1
		s1.ClientID = 0
		_, err = n.propose(s1, nil, 10)
		if err != ErrInvalidSession {
			t.Errorf("not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestProposalsWithCorruptedSessionWillPanic(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		s1 := client.NewSession(n.shardID, random.NewLockedRand())
		s1.SeriesID = 100
		s1.RespondedTo = 200
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("panic not triggered")
			}
		}()
		_, err := n.propose(s1, nil, 10)
		if err != nil {
			t.Fatalf("failed to make proposal %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestRaftNodeQuiesceCanBeDisabled(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	// quiesce is disabled by default
	defer cleanupTestDir(fs)
	nodes, smList, router, ldb := getTestRaftNodes(3, false, fs)
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	for _, node := range nodes {
		if node.qs.quiesced() {
			t.Errorf("node quiesced on startup")
		}
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	defer stopNodes(nodes)
	defer ldb.Close()
	// need to step more than quiesce.threshold() as the startup
	// config change messages are going to be recorded as activities
	for i := uint64(0); i <= nodes[0].qs.threshold()*2; i++ {
		singleStepNodes(nodes, smList, router)
	}
	for _, node := range nodes {
		if node.qs.quiesced() {
			t.Errorf("node is quiesced when quiesce is not enabled")
		}
	}
}

func TestNodesCanEnterQuiesce(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].qs.threshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.qs.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		// step more, nodes should stay in quiesce state.
		for i := uint64(0); i <= nodes[0].qs.threshold()*3; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.qs.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, true, false, tf, fs)
}

func TestNodesCanExitQuiesceByMakingProposal(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].qs.threshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.qs.quiesced() {
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
			if node.qs.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, true, false, tf, fs)
}

func TestNodesCanExitQuiesceByReadIndex(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].qs.threshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.qs.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		n := nodes[0]
		rs, err := n.read(10)
		if err != nil {
			t.Errorf("failed to read")
		}
		var done bool
		for i := uint64(0); i <= 5; i++ {
			singleStepNodes(nodes, smList, router)
			select {
			case <-rs.ResultC():
				done = true
			default:
			}
			if done {
				break
			}
		}
		for _, node := range nodes {
			if node.qs.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, true, false, tf, fs)
}

func TestNodesCanExitQuiesceByConfigChange(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		// need to step more than quiesce.threshold() as the startup
		// config change messages are going to be recorded as activities
		for i := uint64(0); i <= nodes[0].qs.threshold()*2; i++ {
			singleStepNodes(nodes, smList, router)
		}
		for _, node := range nodes {
			if !node.qs.quiesced() {
				t.Errorf("node failed to enter quiesced")
			}
		}
		n := nodes[0]
		done := false
		for i := 0; i < 5; i++ {
			rs, err := n.requestAddNodeWithOrderID(24680, "localhost:12345", 0, 10)
			if err != nil {
				t.Errorf("request to add node failed, %v", err)
			}
			hasResp := false
			for i := uint64(0); i < 25; i++ {
				singleStepNodes(nodes, smList, router)
				select {
				case v := <-rs.ResultC():
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
			if node.qs.quiesced() {
				t.Errorf("node failed to exit from quiesced")
			}
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, true, false, tf, fs)
}

func TestLinearizableReadCanBeMade(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		rs, err := n.propose(session, []byte("test-data"), 10)
		if err != nil {
			t.Fatalf("failed to make proposal")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		closeProposalTestClient(n, nodes, smList, router, session)
		rs, err = n.read(10)
		if err != nil {
			t.Fatalf("")
		}
		if rs.node == nil {
			t.Fatalf("rs.node not set")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func testNodeCanBeAdded(t *testing.T, fs vfs.IFS) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		router.dropRate = 3
		n := mustHasLeaderNode(nodes, t)
		rs, err := n.requestAddNodeWithOrderID(4, "a4:4", 0, 10)
		if err != nil {
			t.Fatalf("request to delete node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		for _, node := range nodes {
			if !sliceEqual([]uint64{1, 2, 3, 4}, getMemberNodes(node.sm)) {
				t.Errorf("failed to delete the node, %v", getMemberNodes(node.sm))
			}
		}
	}
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestNodeCanBeAddedWithMessageDrops(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	for i := 0; i < 10; i++ {
		testNodeCanBeAdded(t, fs)
	}
}

func TestNodeCanBeDeleted(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		rs, err := n.requestDeleteNodeWithOrderID(2, 0, 10)
		if err != nil {
			t.Fatalf("request to delete node failed")
		}
		stepNodes(nodes, smList, router, 10)
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
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
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
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		fs := vfs.GetTestFS()
		n := nodes[0]
		session, ok := getProposalTestClient(n, nodes, smList, router)
		if !ok {
			t.Errorf("failed to get session")
			return
		}
		for i := 0; i < 5; i++ {
			rs, err := n.propose(session, []byte("test-data"), 10)
			if err != nil {
				t.Fatalf("")
			}
			stepNodes(nodes, smList, router, 10)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		rs, err := n.requestAddNodeWithOrderID(4, "a4:4", 0, 10)
		if err != nil {
			t.Fatalf("request to add node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		for _, node := range nodes {
			if node.stopped() {
				t.Errorf("node %d is stopped, this is unexpected", node.replicaID)
			}
			if !sliceEqual([]uint64{1, 2, 3, 4}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
		// now bring the node 5 online
		newNodes, newSMList, newRouter, _ := doGetTestRaftNodes(4, 1, true, ldb, fs)
		if len(newNodes) != 1 {
			t.Fatalf("failed to get 1 nodes")
		}
		router.addQ(4, newRouter.qm[4])
		nodes = append(nodes, newNodes[0])
		smList = append(smList, newSMList[0])
		nodes[3].sendRaftMessage = router.send
		stepNodes(nodes, smList, router, 100)
		if smList[0].GetLastApplied() != newSMList[0].GetLastApplied() {
			t.Errorf("last applied: %d, want %d",
				newSMList[0].GetLastApplied(), smList[0].GetLastApplied())
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestNodeCanBeAddedWhenOrderIsEnforced(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		rs, err := n.requestAddNodeWithOrderID(5, "a5:5", 0, 10)
		if err != nil {
			t.Fatalf("request to add node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustReject(rs, t)
		for _, node := range nodes {
			if node.stopped() {
				t.Errorf("node %d is stopped, this is unexpected", node.replicaID)
			}
			if !sliceEqual([]uint64{1, 2, 3}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
		m := n.sm.GetMembership()
		ccid := m.ConfigChangeId
		rs, err = n.requestAddNodeWithOrderID(5, "a5:5", ccid, 10)
		if err != nil {
			t.Fatalf("request to add node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		for _, node := range nodes {
			if node.stopped() {
				t.Errorf("node %d is stopped, this is unexpected", node.replicaID)
			}
			if !sliceEqual([]uint64{1, 2, 3, 5}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
	}
	runRaftNodeTest(t, false, true, tf, fs)
}

func TestNodeCanBeDeletedWhenOrderIsEnforced(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
		n := nodes[0]
		rs, err := n.requestDeleteNodeWithOrderID(2, 0, 10)
		if err != nil {
			t.Fatalf("request to delete node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustReject(rs, t)
		for _, node := range nodes {
			if node.stopped() {
				t.Errorf("node %d is stopped, this is unexpected", node.replicaID)
			}
			if !sliceEqual([]uint64{1, 2, 3}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
		m := n.sm.GetMembership()
		ccid := m.ConfigChangeId
		rs, err = n.requestDeleteNodeWithOrderID(2, ccid, 10)
		if err != nil {
			t.Fatalf("request to add node failed")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		for _, node := range nodes {
			if node.replicaID == 2 {
				continue
			}
			if !sliceEqual([]uint64{1, 3}, getMemberNodes(node.sm)) {
				t.Errorf("node members not expected: %v", getMemberNodes(node.sm))
			}
		}
	}
	runRaftNodeTest(t, false, true, tf, fs)
}

func getSnapshotFileCount(dir string, fs vfs.IFS) (int, error) {
	fiList, err := fs.List(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, fn := range fiList {
		fi, err := fs.Stat(fs.PathJoin(dir, fn))
		if err != nil {
			return 0, err
		}
		if !fi.IsDir() {
			continue
		}
		if strings.HasPrefix(fi.Name(), "snapshot-") {
			count++
		}
	}
	return count, nil
}

func TestSnapshotCanBeMade(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
			rs, err := n.propose(session, []byte(data), 10)
			if err != nil {
				t.Fatalf("failed to make proposal")
			}
			stepNodes(nodes, smList, router, 10)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		if getMaxLastApplied(smList) != maxLastApplied+uint64(proposalCount) {
			t.Errorf("not all %d proposals applied", proposalCount)
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		// check we do have snapshots saved on disk
		for _, node := range nodes {
			sd := fmt.Sprintf(snapDir, testShardID, node.replicaID)
			dir := fs.PathJoin(raftTestTopDir, sd)
			count, err := getSnapshotFileCount(dir, fs)
			if err != nil {
				t.Fatalf("failed to get snapshot count")
			}
			if count == 0 {
				t.Errorf("no snapshot image")
			}
		}
	}
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestSnapshotCanBeMadeTwice(t *testing.T) {
	tf := func(t *testing.T, nodes []*node,
		smList []*rsm.StateMachine, router *testRouter, ldb raftio.ILogDB) {
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
			rs, err := n.propose(session, []byte(data), 10)
			if err != nil {
				t.Fatalf("failed to make proposal")
			}
			stepNodes(nodes, smList, router, 10)
			mustComplete(rs, t)
			session.ProposalCompleted()
		}
		if getMaxLastApplied(smList) != maxLastApplied+uint64(proposalCount) {
			t.Errorf("not all %d proposals applied", proposalCount)
		}
		closeProposalTestClient(n, nodes, smList, router, session)
		// check we do have snapshots saved on disk
		for _, node := range nodes {
			if err := node.save(rsm.Task{}); err != nil {
				t.Fatalf("%v", err)
			}
			if err := node.save(rsm.Task{}); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	fs := vfs.GetTestFS()
	runRaftNodeTest(t, false, false, tf, fs)
}

func TestNodesCanBeRestarted(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	defer cleanupTestDir(fs)
	nodes, smList, router, ldb := getTestRaftNodes(3, false, fs)
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
		rs, err := n.propose(session, []byte("test-data"), 10)
		if err != nil {
			t.Fatalf("")
		}
		stepNodes(nodes, smList, router, 10)
		mustComplete(rs, t)
		session.ProposalCompleted()
	}
	if getMaxLastApplied(smList) != maxLastApplied+25 {
		t.Errorf("not all %d proposals applied", 25)
	}
	closeProposalTestClient(n, nodes, smList, router, session)
	for _, node := range nodes {
		sd := fmt.Sprintf(snapDir, testShardID, node.replicaID)
		dir := fs.PathJoin(raftTestTopDir, sd)
		count, err := getSnapshotFileCount(dir, fs)
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
	nodes, smList, router, ldb = getTestRaftNodes(3, false, fs)
	defer stopNodes(nodes)
	defer ldb.Close()
	if len(nodes) != 3 {
		t.Fatalf("failed to get 3 nodes")
	}
	stepNodesUntilThereIsLeader(nodes, smList, router)
	stepNodes(nodes, smList, router, 100)
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
	timeout := v.Milliseconds()
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
			ReplicaID:       1,
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

type dummyPipeline struct{}

func (d *dummyPipeline) setCloseReady(*node)            {}
func (d *dummyPipeline) setStepReady(shardID uint64)    {}
func (d *dummyPipeline) setCommitReady(shardID uint64)  {}
func (d *dummyPipeline) setApplyReady(shardID uint64)   {}
func (d *dummyPipeline) setStreamReady(shardID uint64)  {}
func (d *dummyPipeline) setSaveReady(shardID uint64)    {}
func (d *dummyPipeline) setRecoverReady(shardID uint64) {}

func TestProcessUninitilizedNode(t *testing.T) {
	n := &node{ss: snapshotState{}, pipeline: &dummyPipeline{}}
	if !n.processUninitializedNodeStatus() {
		t.Errorf("failed to returned the recover request")
	}
	if !n.ss.recovering() {
		t.Errorf("not in recovering mode")
	}
	req, ok := n.ss.getRecoverReq()
	if !ok {
		t.Fatalf("failed to set recover req")
	}
	if !req.Initial || !req.Recover {
		t.Errorf("unexpected req")
	}
	n2 := &node{ss: snapshotState{}, initializedC: make(chan struct{})}
	n2.setInitialized()
	if n2.processUninitializedNodeStatus() {
		t.Errorf("unexpected recover from snapshot request")
	}
}

func TestProcessRecoveringNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: snapshotState{}}
	if n.processRecoverStatus() {
		t.Errorf("processRecoveringNode not skipped")
	}
}

func TestProcessTakingSnapshotNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: snapshotState{}}
	if n.processSaveStatus() {
		t.Errorf("processTakingSnapshotNode not skipped")
	}
}

func TestRecoveringFromSnapshotNodeCanComplete(t *testing.T) {
	n := &node{
		ss:           snapshotState{},
		sysEvents:    newSysEventListener(nil, nil),
		initializedC: make(chan struct{}),
	}
	n.ss.setRecovering()
	n.ss.notifySnapshotStatus(false, true, false, true, 100)
	if n.processRecoverStatus() {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.recovering() {
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
	n := &node{ss: snapshotState{}, sysEvents: newSysEventListener(nil, nil)}
	n.ss.setRecovering()
	if !n.processRecoverStatus() {
		t.Errorf("not skipped")
	}
}

func TestTakingSnapshotNodeCanComplete(t *testing.T) {
	n := &node{ss: snapshotState{}, initializedC: make(chan struct{})}
	n.ss.setSaving()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.setInitialized()
	if n.processSaveStatus() {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.saving() {
		t.Errorf("still taking snapshot")
	}
}

func TestTakingSnapshotOnUninitializedNodeWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	n := &node{ss: snapshotState{}}
	n.ss.setSaving()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.processSaveStatus()
}

func TestGetCompactionIndex(t *testing.T) {
	cfg := config.Config{
		CompactionOverhead: 234,
	}
	n := node{config: cfg}
	req1 := rsm.SSRequest{
		OverrideCompaction: true,
		CompactionOverhead: 123,
	}
	req2 := rsm.SSRequest{
		OverrideCompaction: false,
		CompactionOverhead: 456,
	}
	req3 := rsm.SSRequest{
		OverrideCompaction: true,
		CompactionIndex:    300,
	}
	if v, _ := n.getCompactionIndex(req1, 200); v != 77 {
		t.Errorf("snapshot overhead override not applied")
	}
	if v, _ := n.getCompactionIndex(req2, 500); v != 266 {
		t.Errorf("snapshot overhead override unexpectedly applied")
	}
	if v, _ := n.getCompactionIndex(req3, 500); v != 300 {
		t.Errorf("snapshot index not correctly set")
	}
	if v, ok := n.getCompactionIndex(req3, 299); v != 0 || ok {
		t.Errorf("snapshot index unexpectedly set")
	}
}

type testDummyNodeProxy struct{}

func (np *testDummyNodeProxy) StepReady()                                            {}
func (np *testDummyNodeProxy) RestoreRemotes(pb.Snapshot) error                      { return nil }
func (np *testDummyNodeProxy) ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool)     {}
func (np *testDummyNodeProxy) ApplyConfigChange(pb.ConfigChange, uint64, bool) error { return nil }
func (np *testDummyNodeProxy) ReplicaID() uint64                                     { return 1 }
func (np *testDummyNodeProxy) ShardID() uint64                                       { return 1 }
func (np *testDummyNodeProxy) ShouldStop() <-chan struct{}                           { return nil }

func TestNotReadyTakingSnapshotNodeIsSkippedWhenConcurrencyIsNotSupported(t *testing.T) {
	fs := vfs.GetTestFS()
	n := &node{ss: snapshotState{}, initializedC: make(chan struct{})}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeSM(config, &rsm.InMemStateMachine{}, nil),
		nil, config, &testDummyNodeProxy{}, fs)
	if n.concurrentSnapshot() {
		t.Errorf("concurrency not suppose to be supported")
	}
	n.ss.setSaving()
	n.setInitialized()
	if !n.processSaveStatus() {
		t.Fatalf("node not skipped")
	}
}

func TestNotReadyTakingSnapshotConcurrentNodeIsNotSkipped(t *testing.T) {
	fs := vfs.GetTestFS()
	n := &node{ss: snapshotState{}, initializedC: make(chan struct{})}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeSM(config, &rsm.ConcurrentStateMachine{}, nil),
		nil, config, &testDummyNodeProxy{}, fs)
	if !n.concurrentSnapshot() {
		t.Errorf("concurrency not supported")
	}
	n.ss.setSaving()
	n.setInitialized()
	if n.processSaveStatus() {
		t.Fatalf("node unexpectedly skipped")
	}
}

func TestIsWitnessNode(t *testing.T) {
	n1 := node{config: config.Config{}}
	if n1.isWitness() {
		t.Errorf("not expect to be witness")
	}
	n2 := node{config: config.Config{IsWitness: true}}
	if !n2.isWitness() {
		t.Errorf("not reported as witness")
	}
}

func TestSaveSnapshotAborted(t *testing.T) {
	tests := []struct {
		err     error
		aborted bool
	}{
		{sm.ErrSnapshotStopped, true},
		{sm.ErrSnapshotAborted, true},
		{nil, false},
		{sm.ErrSnapshotStreaming, false},
	}

	for idx, tt := range tests {
		if saveAborted(tt.err) != tt.aborted {
			t.Errorf("%d, saveSnapshotAborted failed", idx)
		}
	}
}

func TestLogDBMetrics(t *testing.T) {
	l := logDBMetrics{}
	l.update(true)
	if !l.isBusy() {
		t.Errorf("unexpected value")
	}
	l.update(false)
	if l.isBusy() {
		t.Errorf("unexpected value")
	}
}

func TestUninitializedNodeNotAllowedToMakeRequests(t *testing.T) {
	n := node{}
	if n.initialized() {
		t.Fatalf("already initialized")
	}
	if _, err := n.propose(nil, nil, 1); err != ErrShardNotReady {
		t.Fatalf("making proposal not rejected")
	}
	if _, err := n.proposeSession(nil, 1); err != ErrShardNotReady {
		t.Fatalf("propose session not rejected")
	}
	if _, err := n.read(1); err != ErrShardNotReady {
		t.Fatalf("read not rejected")
	}
	if err := n.requestLeaderTransfer(1); err != ErrShardNotReady {
		t.Fatalf("leader transfer request not rejected")
	}
	if _, err := n.requestSnapshot(SnapshotOption{}, 1); err != ErrShardNotReady {
		t.Fatalf("snapshot request not rejected")
	}
	if _, err := n.requestConfigChange(pb.ConfigChangeType(0),
		1, "localhost:1", 1, 1); err != ErrShardNotReady {
		t.Fatalf("config change request not rejected")
	}
}

func TestEntriesToApply(t *testing.T) {
	tests := []struct {
		inputIndex   uint64
		inputLength  uint64
		crash        bool
		resultIndex  uint64
		resultLength uint64
	}{
		{1, 5, true, 0, 0},
		{1, 10, false, 0, 0},
		{1, 11, false, 11, 1},
		{1, 20, false, 11, 10},
		{10, 6, false, 11, 5},
		{11, 5, false, 11, 5},
		{12, 5, true, 0, 0},
	}
	for idx, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r == nil && tt.crash {
					t.Fatalf("%d, didn't panic", idx)
				}
			}()
			inputs := make([]pb.Entry, 0)
			for i := tt.inputIndex; i < tt.inputIndex+tt.inputLength; i++ {
				inputs = append(inputs, pb.Entry{Index: i})
			}
			n := &node{pushedIndex: 10}
			results := pb.EntriesToApply(inputs, n.pushedIndex, true)
			if uint64(len(results)) != tt.resultLength {
				t.Errorf("%d, result len %d, want %d", idx, len(results), tt.resultLength)
			}
			if len(results) > 0 {
				if results[0].Index != tt.resultIndex {
					t.Errorf("%d, first result index %d, want %d",
						idx, results[0].Index, tt.resultIndex)
				}
			}
		}()
	}
}
