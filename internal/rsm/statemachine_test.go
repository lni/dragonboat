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

// +build !dragonboat_cppwrappertest
// +build !dragonboat_cppkvtest

package rsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/internal/raft"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/tests"
	"github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	testSnapshotterDir = "rsm_test_data_safe_to_delete"
	snapshotFileSuffix = "gbsnap"
)

func removeTestDir() {
	os.RemoveAll(testSnapshotterDir)
}

func createTestDir() {
	removeTestDir()
	os.MkdirAll(testSnapshotterDir, 0755)
}

type testNodeProxy struct {
	applySnapshot      bool
	applyConfChange    bool
	addPeer            bool
	removePeer         bool
	reject             bool
	accept             bool
	smResult           uint64
	index              uint64
	rejected           bool
	ignored            bool
	applyUpdateInvoked bool
	notifyReadClient   bool
	addPeerCount       uint64
	addObserver        bool
	addObserverCount   uint64
}

func newTestNodeProxy() *testNodeProxy {
	return &testNodeProxy{}
}

func (p *testNodeProxy) ApplyUpdate(entry pb.Entry,
	result uint64, rejected bool, ignored bool, notifyReadClient bool) {
	p.smResult = result
	p.index = entry.Index
	p.rejected = rejected
	p.ignored = ignored
	p.applyUpdateInvoked = true
	p.notifyReadClient = notifyReadClient
}

func (p *testNodeProxy) SetLastApplied(v uint64) {}

func (p *testNodeProxy) RestoreRemotes(s pb.Snapshot) {
	for k := range s.Membership.Addresses {
		_ = k
		p.addPeer = true
		p.addPeerCount++
	}
}

func (p *testNodeProxy) ApplyConfigChange(cc pb.ConfigChange) {
	p.applyConfChange = true
	if cc.Type == pb.AddNode {
		p.addPeer = true
		p.addPeerCount++
	} else if cc.Type == pb.AddObserver {
		p.addObserver = true
		p.addObserverCount++
	} else if cc.Type == pb.RemoveNode {
		p.removePeer = true
	}
}

func (p *testNodeProxy) ConfigChangeProcessed(index uint64, accept bool) {
	if accept {
		p.accept = true
	} else {
		p.reject = true
	}
}

func (p *testNodeProxy) NodeID() uint64    { return 1 }
func (p *testNodeProxy) ClusterID() uint64 { return 1 }

type testSnapshotter struct {
	index    uint64
	dataSize uint64
}

func newTestSnapshotter() *testSnapshotter {
	return &testSnapshotter{}
}

func (s *testSnapshotter) GetSnapshot(index uint64) (pb.Snapshot, error) {
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := filepath.Join(testSnapshotterDir, fn)
	address := make(map[uint64]string)
	address[1] = "localhost:1"
	address[2] = "localhost:2"
	snap := pb.Snapshot{
		Filepath: fp,
		FileSize: s.dataSize,
		Index:    index,
		Term:     2,
		Membership: pb.Membership{
			Addresses: address,
		},
	}
	return snap, nil
}

func (s *testSnapshotter) GetFilePath(index uint64) string {
	filename := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	return filepath.Join(testSnapshotterDir, filename)
}

func (s *testSnapshotter) GetMostRecentSnapshot() (pb.Snapshot, error) {
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := filepath.Join(testSnapshotterDir, fn)
	snap := pb.Snapshot{
		Filepath: fp,
		FileSize: s.dataSize,
		Index:    s.index,
		Term:     2,
	}
	return snap, nil
}

func (s *testSnapshotter) IsNoSnapshotError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "no snapshot available"
}

func (s *testSnapshotter) Save(savable IManagedStateMachine,
	meta *SnapshotMeta) (*pb.Snapshot, *server.SnapshotEnv, error) {
	s.index = meta.Index
	f := func(cid uint64, nid uint64) string {
		return testSnapshotterDir
	}
	env := server.NewSnapshotEnv(f, 1, 1, s.index, 1, server.SnapshottingMode)
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := filepath.Join(testSnapshotterDir, fn)
	writer, err := NewSnapshotWriter(fp, CurrentSnapshotVersion)
	if err != nil {
		return nil, env, err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()
	session := meta.Session.Bytes()
	sz, err := savable.SaveSnapshot(nil, writer, session, nil)
	s.dataSize = sz
	if err != nil {
		return nil, env, err
	}
	ss := &pb.Snapshot{
		Filepath:   env.GetFilepath(),
		FileSize:   sz,
		Membership: meta.Membership,
		Index:      meta.Index,
		Term:       meta.Term,
	}
	return ss, env, nil
}

func runSMTest(t *testing.T, tf func(t *testing.T, sm *StateMachine)) {
	defer leaktest.AfterTest(t)()
	store := tests.NewKVTest(1, 1)
	ds := NewNativeStateMachine(&RegularStateMachine{sm: store}, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter()
	sm := NewStateMachine(ds, snapshotter, false, nodeProxy)
	tf(t, sm)
}

func runSMTest2(t *testing.T,
	tf func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine)) {
	defer leaktest.AfterTest(t)()
	createTestDir()
	defer removeTestDir()
	store := tests.NewKVTest(1, 1)
	ds := NewNativeStateMachine(&RegularStateMachine{sm: store}, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter()
	sm := NewStateMachine(ds, snapshotter, false, nodeProxy)
	tf(t, sm, ds, nodeProxy, snapshotter, store)
}

func TestUpdatesCanBeBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	createTestDir()
	defer removeTestDir()
	store := &tests.ConcurrentUpdate{}
	ds := NewNativeStateMachine(&ConcurrentStateMachine{sm: store}, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter()
	sm := NewStateMachine(ds, snapshotter, false, nodeProxy)
	e1 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    235,
		Term:     1,
	}
	e2 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    236,
		Term:     1,
	}
	e3 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    237,
		Term:     1,
	}
	commit := Commit{
		Entries: []pb.Entry{e1, e2, e3},
	}
	sm.index = 234
	sm.CommitC() <- commit
	// two commits to handle
	batch := make([]Commit, 0, 8)
	sm.Handle(batch, nil)
	if sm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 237", sm.GetLastApplied())
	}
	count := store.UpdateCount
	if count != 3 {
		t.Fatalf("not batched as expected, batched update count %d, want 3", count)
	}
}

func TestUpdatesNotBatchedWhenNotAllNoOPUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	createTestDir()
	defer removeTestDir()
	store := &tests.ConcurrentUpdate{}
	ds := NewNativeStateMachine(&ConcurrentStateMachine{sm: store}, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter()
	sm := NewStateMachine(ds, snapshotter, false, nodeProxy)
	e1 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    235,
		Term:     1,
	}
	e2 := pb.Entry{
		ClientID: 123,
		SeriesID: client.SeriesIDForRegister,
		Index:    236,
		Term:     1,
	}
	e3 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    237,
		Term:     1,
	}
	commit := Commit{
		Entries: []pb.Entry{e1, e2, e3},
	}
	sm.index = 234
	sm.CommitC() <- commit
	// two commits to handle
	batch := make([]Commit, 0, 8)
	sm.Handle(batch, nil)
	if sm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 237", sm.GetLastApplied())
	}
	count := store.UpdateCount
	if count != 1 {
	}
}

func TestStateMachineCanBeCreated(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		if sm.CommitChanBusy() {
			t.Errorf("commitChan busy")
		}
	}
	runSMTest(t, tf)
}

func applyConfigChangeEntry(sm *StateMachine,
	configChangeID uint64, configType pb.ConfigChangeType, NodeID uint64,
	addressToAdd string, index uint64) {
	cc := pb.ConfigChange{
		ConfigChangeId: configChangeID,
		Type:           configType,
		NodeID:         NodeID,
	}
	if addressToAdd != "" {
		cc.Address = addressToAdd
	}
	data, err := cc.Marshal()
	if err != nil {
		panic(err)
	}
	e := pb.Entry{
		Cmd:   data,
		Type:  pb.ConfigChangeEntry,
		Index: index,
		Term:  1,
	}
	commit := Commit{
		Entries: []pb.Entry{e},
	}
	sm.index = index - 1
	sm.CommitC() <- commit
}

func TestBatchedLastAppliedValue(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.setBatchedLastApplied(12345)
		if sm.GetBatchedLastApplied() != 12345 {
			t.Errorf("batched last applied value can not be set/get")
		}
	}
	runSMTest(t, tf)
}

func TestLookupNotAllowedOnClosedCluster(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.aborted = true
		v, err := sm.Lookup(make([]byte, 10))
		if err != ErrClusterClosed {
			t.Errorf("unexpected err value %v", err)
		}
		if v != nil {
			t.Errorf("unexpected result")
		}
	}
	runSMTest(t, tf)
}

func TestGetMembership(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			Observers: map[uint64]string{
				200: "a200",
				300: "a300",
			},
			Removed: map[uint64]bool{
				400: true,
				500: true,
				600: true,
			},
			ConfigChangeId: 12345,
		}
		m, o, r, cid := sm.GetMembership()
		if cid != 12345 {
			t.Errorf("unexpected cid value")
		}
		if len(m) != 2 || len(o) != 2 || len(r) != 3 {
			t.Errorf("len changed")
		}
	}
	runSMTest(t, tf)
}

func TestGetMembershipNodes(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			Observers: map[uint64]string{
				200: "a200",
				300: "a300",
			},
			Removed: map[uint64]bool{
				400: true,
				500: true,
				600: true,
			},
			ConfigChangeId: 12345,
		}
		n, _, _, _ := sm.GetMembership()
		if len(n) != 2 {
			t.Errorf("unexpected len")
		}
		_, ok1 := n[100]
		_, ok2 := n[234]
		if !ok1 || !ok2 {
			t.Errorf("unexpected node id")
		}
	}
	runSMTest(t, tf)
}

func TestGetMembershipHash(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		h1 := sm.GetMembershipHash()
		sm.members.Addresses[200] = "a200"
		h2 := sm.GetMembershipHash()
		if h1 == h2 {
			t.Errorf("hash doesn't change after membership change")
		}
	}
	runSMTest(t, tf)
}

func TestGetSnapshotMetaPanicWhenThereIsNoMember(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("didn't panic")
			}
		}()
		sm.getSnapshotMeta(nil)
	}
	runSMTest(t, tf)
}

func TestGetSnapshotMeta(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		applySessionRegisterEntry(sm, 12345, 789)
		sm.index = 100
		sm.term = 101
		meta := sm.getSnapshotMeta(make([]byte, 123))
		if meta.Index != 100 || meta.Term != 101 {
			t.Errorf("index/term not recorded")
		}
		v := meta.Ctx.([]byte)
		if len(v) != 123 {
			t.Errorf("ctx not recorded")
		}
		if len(meta.Membership.Addresses) != 2 {
			t.Errorf("membership not saved %d, want 2", len(meta.Membership.Addresses))
		}
		v = meta.Session.Bytes()
		if len(v) == 0 {
			t.Errorf("sessions not saved")
		}
	}
	runSMTest(t, tf)
}

func TestHandleConfChangeAddNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddNode,
			4,
			"localhost:1010",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if !nodeProxy.addPeer {
			t.Errorf("add peer not called")
		}
		v, ok := sm.members.Addresses[4]
		if !ok {
			t.Errorf("members not updated")
		}
		if v != "localhost:1010" {
			t.Errorf("address recorded %s, want localhost:1010", v)
		}
	}
	runSMTest2(t, tf)
}

func TestAddObserverWhenNodeAlreadyAddedWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddNode,
			4,
			"localhost:1010",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		applyConfigChangeEntry(sm,
			123,
			pb.AddObserver,
			4,
			"localhost:1010",
			124)
		sm.Handle(batch, nil)
		if !nodeProxy.reject {
			t.Errorf("invalid cc not rejected")
		}
	}
	runSMTest2(t, tf)
}

func TestObserverCanBeAdded(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddObserver,
			4,
			"localhost:1010",
			123)

		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
		if !nodeProxy.addObserver {
			t.Errorf("add observer not called")
		}
	}
	runSMTest2(t, tf)
}

func TestObserverPromoteToNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddObserver,
			4,
			"localhost:1010",
			123)

		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
		if !nodeProxy.addObserver {
			t.Errorf("add observer not called")
		}
		applyConfigChangeEntry(sm,
			123,
			pb.AddNode,
			4,
			"localhost:1010",
			124)

		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 124 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.addPeer {
			t.Errorf("add peer not called")
		}
		if len(sm.members.Addresses) != 1 {
			t.Errorf("node count != 1")
		}
		if len(sm.members.Observers) != 0 {
			t.Errorf("observer count != 0")
		}
	}
	runSMTest2(t, tf)
}

func TestObserverPromoteToNodeWithDifferentAddressIsHandled(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddObserver,
			4,
			"localhost:1010",
			123)

		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
		if !nodeProxy.addObserver {
			t.Errorf("add observer not called")
		}
		applyConfigChangeEntry(sm,
			123,
			pb.AddNode,
			4,
			"localhost:1011",
			124)

		sm.Handle(batch, nil)
		v, ok := sm.members.Addresses[4]
		if !ok {
			t.Errorf("address not found")
		}
		if v != "localhost:1010" {
			t.Errorf("unexpected address")
		}
	}
	runSMTest2(t, tf)
}

func TestHandleConfChangeRemoveNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.Addresses[1] = "localhost:1010"
		applyConfigChangeEntry(sm,
			1,
			pb.RemoveNode,
			1,
			"",
			123)

		_, ok := sm.members.Addresses[1]
		if !ok {
			t.Errorf("node 1 not in members")
		}
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if !nodeProxy.removePeer {
			t.Errorf("remove peer not called")
		}
		_, ok = sm.members.Addresses[1]
		if ok {
			t.Errorf("failed to remove node 1 from members")
		}
		_, ok = sm.members.Removed[1]
		if !ok {
			t.Errorf("removed node not recorded as removed")
		}
	}
	runSMTest2(t, tf)
}

func TestOrderedConfChangeIsAccepted(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm,
			6,
			pb.RemoveNode,
			1,
			"",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if nodeProxy.reject {
			t.Errorf("rejected")
		}
		if !nodeProxy.accept {
			t.Errorf("not accepted")
		}
		if !nodeProxy.removePeer {
			t.Errorf("remove peer not called")
		}
		if sm.members.ConfigChangeId != 123 {
			t.Errorf("conf change id not updated, %d", sm.members.ConfigChangeId)
		}
	}
	runSMTest2(t, tf)
}

func TestAddingNodeOnTheSameNodeHostWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.ConfigChangeId = 6
		sm.members.Addresses[100] = "test.nodehost"
		applyConfigChangeEntry(sm,
			7,
			pb.AddNode,
			2,
			"test.nodehost",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
		if sm.members.ConfigChangeId == 123 {
			t.Errorf("conf change unexpected updated, %d", sm.members.ConfigChangeId)
		}
	}
	runSMTest2(t, tf)
}

func TestAddingRemovedNodeWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.ConfigChangeId = 6
		sm.members.Removed[2] = true
		applyConfigChangeEntry(sm,
			7,
			pb.AddNode,
			2,
			"a1",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
	}
	runSMTest2(t, tf)
}

func TestOutOfOrderConfChangeIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.ordered = true
		sm.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm,
			1,
			pb.RemoveNode,
			1,
			"",
			123)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.removePeer {
			t.Errorf("remove peer unexpectedly called")
		}
	}
	runSMTest2(t, tf)
}

func TestHandleEmptyEvent(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		e := pb.Entry{
			Type:  pb.ApplicationEntry,
			Index: 234,
			Term:  1,
		}
		commit := Commit{
			Entries: []pb.Entry{e},
		}
		sm.index = 233
		sm.CommitC() <- commit
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 234 {
			t.Errorf("last applied %d, want 234", sm.GetLastApplied())
		}
	}
	runSMTest2(t, tf)
}

func getTestKVData() []byte {
	k := "test-key"
	d := "test-value"
	u := kvpb.PBKV{
		Key: &k,
		Val: &d,
	}
	data, err := proto.Marshal(&u)
	if err != nil {
		panic(err)
	}
	return data
}

func TestHandleUpate(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		data := getTestKVData()
		e1 := pb.Entry{
			ClientID: 123,
			SeriesID: client.SeriesIDForRegister,
			Index:    235,
			Term:     1,
		}
		e2 := pb.Entry{
			ClientID: 123,
			SeriesID: 2,
			Cmd:      data,
			Index:    236,
			Term:     1,
		}
		commit := Commit{
			Entries: []pb.Entry{e1, e2},
		}
		sm.index = 234
		sm.CommitC() <- commit
		// two commits to handle
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 236 {
			t.Errorf("last applied %d, want 236", sm.GetLastApplied())
		}
		v, ok := store.(*tests.KVTest).KVStore["test-key"]
		if !ok {
			t.Errorf("value not set")
		}
		if v != "test-value" {
			t.Errorf("v: %s, want test-value", v)
		}
		result, err := sm.Lookup([]byte("test-key"))
		if err != nil {
			t.Errorf("lookup failed")
		}
		if string(result) != "test-value" {
			t.Errorf("result %s, want test-value", string(result))
		}
	}
	runSMTest2(t, tf)
}

func TestSnapshotCanBeApplied(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.Addresses[1] = "localhost:1234"
		store.(*tests.KVTest).KVStore["test-key1"] = "test-value1"
		store.(*tests.KVTest).KVStore["test-key2"] = "test-value2"
		sm.index = 3
		hash1 := sm.GetHash()
		ss, _, err := sm.SaveSnapshot()
		if err != nil {
			t.Fatalf("failed to make snapshot %v", err)
		}
		index := ss.Index
		commit := Commit{
			Index: index,
		}
		store2 := tests.NewKVTest(1, 1)
		ds2 := NewNativeStateMachine(&RegularStateMachine{sm: store2}, make(chan struct{}))
		nodeProxy2 := newTestNodeProxy()
		snapshotter2 := newTestSnapshotter()
		sm2 := NewStateMachine(ds2, snapshotter2, false, nodeProxy2)
		if len(sm2.members.Addresses) != 0 {
			t.Errorf("unexpected member length")
		}
		index2, err := sm2.RecoverFromSnapshot(commit)
		if err != nil {
			t.Errorf("apply snapshot failed %v", err)
		}
		if index2 != index {
			t.Errorf("last applied %d, want %d", index2, index)
		}
		hash2 := sm2.GetHash()
		if hash1 != hash2 {
			t.Errorf("bad hash %d, want %d, sz %d",
				hash2, hash1, len(store2.(*tests.KVTest).KVStore))
		}
		// see whether members info are recovered
		if len(sm2.members.Addresses) != 2 {
			t.Errorf("failed to restore members")
		}
		v1, ok1 := sm2.members.Addresses[1]
		v2, ok2 := sm2.members.Addresses[2]
		if !ok1 || !ok2 {
			t.Errorf("failed to save member info")
		}
		if v1 != "localhost:1" || v2 != "localhost:2" {
			t.Errorf("unexpected address")
		}
		if nodeProxy2.addPeerCount != 2 {
			t.Errorf("failed to pass on address to node proxy")
		}
	}
	runSMTest2(t, tf)
}

func TestMembersAreSavedWhenMakingSnapshot(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.Addresses[1] = "localhost:1"
		sm.members.Addresses[2] = "localhost:2"
		ss, _, err := sm.SaveSnapshot()
		if err != nil {
			t.Errorf("failed to make snapshot %v", err)
		}
		cs := ss.Membership
		if len(cs.Addresses) != 2 {
			t.Errorf("cs addresses len %d, want 2", len(cs.Addresses))
		}
		v1, ok1 := cs.Addresses[1]
		v2, ok2 := cs.Addresses[2]
		if !ok1 || !ok2 {
			t.Errorf("failed to save member info")
		}
		if v1 != "localhost:1" || v2 != "localhost:2" {
			t.Errorf("unexpected address")
		}
	}
	runSMTest2(t, tf)
}

func TestSnapshotTwiceIsHandled(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.Addresses[1] = "localhost:1"
		sm.members.Addresses[2] = "localhost:2"
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 1, 0, data)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		_, _, err := sm.SaveSnapshot()
		if err != nil {
			t.Errorf("failed to make snapshot %v", err)
		}
		_, _, err = sm.SaveSnapshot()
		if err != raft.ErrSnapshotOutOfDate {
			t.Errorf("snapshot twice completed, %v", err)
		}
	}
	runSMTest2(t, tf)
}

func applySessionRegisterEntry(sm *StateMachine,
	clientID uint64, index uint64) pb.Entry {
	e := pb.Entry{
		ClientID: clientID,
		SeriesID: client.SeriesIDForRegister,
		Index:    index,
		Term:     1,
	}
	commit := Commit{
		Entries: []pb.Entry{e},
	}
	sm.index = index - 1
	sm.CommitC() <- commit
	return e
}

func applySessionUnregisterEntry(sm *StateMachine,
	clientID uint64, index uint64) pb.Entry {
	e := pb.Entry{
		ClientID: clientID,
		SeriesID: client.SeriesIDForUnregister,
		Index:    index,
		Term:     1,
	}
	commit := Commit{
		Entries: []pb.Entry{e},
	}
	sm.CommitC() <- commit
	return e
}

func applyTestEntry(sm *StateMachine,
	clientID uint64, seriesID uint64, index uint64,
	respondedTo uint64, data []byte) pb.Entry {
	e := pb.Entry{
		ClientID:    clientID,
		SeriesID:    seriesID,
		Index:       index,
		Term:        1,
		Cmd:         data,
		RespondedTo: respondedTo,
	}
	commit := Commit{
		Entries: []pb.Entry{e},
	}
	sm.CommitC() <- commit
	return e
}

func TestSessionCanBeCreatedAndRemoved(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		clientID := uint64(12345)
		applySessionRegisterEntry(sm, clientID, 789)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != 789 {
			t.Errorf("last applied %d, want 789", sm.GetLastApplied())
		}
		nds := ds.(*NativeStateMachine)
		sessionManager := nds.sessions
		_, ok := sessionManager.getSession(RaftClientID(clientID))
		if !ok {
			t.Errorf("session not found")
		}
		if nodeProxy.smResult != clientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, clientID)
		}
		index := uint64(790)
		applySessionUnregisterEntry(sm, 12345, index)

		sm.Handle(batch, nil)
		if sm.GetLastApplied() != index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), index)
		}
		_, ok = sessionManager.getSession(RaftClientID(clientID))
		if ok {
			t.Errorf("session not removed")
		}
		if nodeProxy.smResult != clientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, clientID)
		}
	}
	runSMTest2(t, tf)
}

func TestDuplicatedSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		e := applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		nds := ds.(*NativeStateMachine)
		sessionManager := nds.sessions
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		if !ok {
			t.Errorf("session not found")
		}
		if nodeProxy.smResult != e.ClientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, e.ClientID)
		}
		e.Index = 790
		commit := Commit{
			Entries: []pb.Entry{e},
		}
		sm.CommitC() <- commit
		if nodeProxy.rejected {
			t.Errorf("rejected flag set too early")
		}
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if nodeProxy.smResult != 0 {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, 0)
		}
		if !nodeProxy.rejected {
			t.Errorf("reject flag not set")
		}
	}
	runSMTest2(t, tf)
}

func TestRemovingUnregisteredSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {

		sm.index = 789
		e := applySessionUnregisterEntry(sm, 12345, 790)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		nds := ds.(*NativeStateMachine)
		sessionManager := nds.sessions
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		if ok {
			t.Errorf("session not suppose to be there")
		}
		if nodeProxy.smResult != 0 {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, 0)
		}
		if !nodeProxy.rejected {
			t.Errorf("reject flag not set")
		}
	}
	runSMTest2(t, tf)
}

func TestUpdateFromUnregisteredClientWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		sm.index = 789
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		// handleUpdateEvent suppose to return a (result, ignored, rejected) tuple of
		// (0, false, true)
		if nodeProxy.ignored {
			t.Errorf("ignored %t, want false", nodeProxy.ignored)
		}
		if nodeProxy.smResult != 0 {
			t.Errorf("smResult %d, want 0", nodeProxy.smResult)
		}
		if !nodeProxy.rejected {
			t.Errorf("rejected %t, want true", nodeProxy.rejected)
		}
	}
	runSMTest2(t, tf)
}

func TestDuplicatedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		// check normal update is accepted and handleped
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if !nodeProxy.applyUpdateInvoked {
			t.Errorf("update not invoked")
		}
		if nodeProxy.smResult != uint64(len(data)) {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, len(data))
		}
		nodeProxy.applyUpdateInvoked = false
		storeCount := store.(*tests.KVTest).Count
		v := store.(*tests.KVTest).KVStore["test-key"]
		if v != "test-value" {
			t.Errorf("store not set")
		}
		e = applyTestEntry(sm, 12345, 1, 791, 0, data)
		plog.Infof("going to handle the second commit rec")
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if !nodeProxy.applyUpdateInvoked {
			t.Errorf("update not invoked")
		}
		if storeCount != store.(*tests.KVTest).Count {
			t.Errorf("store update invoked twice, not expected")
		}
	}
	runSMTest2(t, tf)
}

func TestRespondedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		// check normal update is accepted and handleped
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		storeCount := store.(*tests.KVTest).Count
		// update the respondedto value
		e = applyTestEntry(sm, 12345, 1, 791, 1, data)
		sm.Handle(batch, nil)
		nds := ds.(*NativeStateMachine)
		sessionManager := nds.sessions
		session, _ := sessionManager.getSession(RaftClientID(12345))
		if session.RespondedUpTo != RaftSeriesID(1) {
			t.Errorf("responded to %d, want 1", session.RespondedUpTo)
		}
		nodeProxy.applyUpdateInvoked = false
		// submit the same stuff again with a different index value
		e = applyTestEntry(sm, 12345, 1, 792, 1, data)
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if nodeProxy.applyUpdateInvoked {
			t.Errorf("update invoked, unexpected")
		}
		if storeCount != store.(*tests.KVTest).Count {
			t.Errorf("store update invoked twice, not expected")
		}
	}
	runSMTest2(t, tf)
}

func TestNoOPSessionAllowEntryToBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Commit, 0, 8)
		sm.Handle(batch, nil)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 790, 0, data)
		// check normal update is accepted and handleped
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		storeCount := store.(*tests.KVTest).Count
		// different index as the same entry is proposed again
		e = applyTestEntry(sm, 12345, client.NoOPSeriesID, 791, 0, data)
		// check normal update is accepted and handleped
		sm.Handle(batch, nil)
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if storeCount == store.(*tests.KVTest).Count {
			t.Errorf("entry not applied")
		}
	}
	runSMTest2(t, tf)
}

func TestGetSnapshotFilesReturnFiles(t *testing.T) {
	sf1 := &pb.SnapshotFile{
		Filepath: "test.data1",
		FileId:   1,
		Metadata: make([]byte, 16),
	}
	sf2 := &pb.SnapshotFile{
		Filepath: "test.data2",
		FileId:   2,
		Metadata: make([]byte, 32),
	}
	ss := pb.Snapshot{
		Files: []*pb.SnapshotFile{sf1, sf2},
	}
	sl := getSnapshotFiles(ss)
	if len(sl) != 2 {
		t.Errorf("unexpected file list size %d", len(sl))
	}
	if sl[0].FileID != 1 || len(sl[0].Metadata) != 16 || sl[0].Filepath != "test.data1" {
		t.Errorf("unexpected file value")
	}
	if sl[1].FileID != 2 || len(sl[1].Metadata) != 32 || sl[1].Filepath != "test.data2" {
		t.Errorf("unexpected file value")
	}
}
