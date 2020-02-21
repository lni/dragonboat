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

package rsm

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/tests/kvpb"
	"github.com/lni/dragonboat/v3/internal/utils/dio"
	"github.com/lni/dragonboat/v3/internal/vfs"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/leaktest"
)

const (
	testSnapshotterDir = "rsm_test_data_safe_to_delete"
	snapshotFileSuffix = "gbsnap"
)

func removeTestDir(fs vfs.IFS) {
	fs.RemoveAll(testSnapshotterDir)
}

func createTestDir(fs vfs.IFS) {
	removeTestDir(fs)
	if err := fs.MkdirAll(testSnapshotterDir, 0755); err != nil {
		panic(err)
	}
}

type testNodeProxy struct {
	applyConfChange    bool
	addPeer            bool
	removePeer         bool
	reject             bool
	accept             bool
	smResult           sm.Result
	index              uint64
	rejected           bool
	ignored            bool
	applyUpdateInvoked bool
	notifyReadClient   bool
	addPeerCount       uint64
	addObserver        bool
	addObserverCount   uint64
	nodeReady          uint64
	applyUpdateCalled  bool
	firstIndex         uint64
}

func newTestNodeProxy() *testNodeProxy {
	return &testNodeProxy{}
}

func (p *testNodeProxy) NodeReady() { p.nodeReady++ }

func (p *testNodeProxy) ShouldStop() <-chan struct{} {
	return nil
}

func (p *testNodeProxy) ApplyUpdate(entry pb.Entry,
	result sm.Result, rejected bool, ignored bool, notifyReadClient bool) {
	if !p.applyUpdateCalled {
		p.applyUpdateCalled = true
		p.firstIndex = entry.Index
	}
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
	fs       vfs.IFS
}

func newTestSnapshotter(fs vfs.IFS) *testSnapshotter {
	return &testSnapshotter{fs: fs}
}

func (s *testSnapshotter) GetSnapshot(index uint64) (pb.Snapshot, error) {
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := s.fs.PathJoin(testSnapshotterDir, fn)
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
	return s.fs.PathJoin(testSnapshotterDir, filename)
}

func (s *testSnapshotter) GetMostRecentSnapshot() (pb.Snapshot, error) {
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := s.fs.PathJoin(testSnapshotterDir, fn)
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

func (s *testSnapshotter) Stream(streamable IStreamable,
	meta *SSMeta, sink pb.IChunkSink) error {
	writer := NewChunkWriter(sink, meta)
	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()
	return streamable.StreamSnapshot(meta.Ctx, writer)
}

func (s *testSnapshotter) Save(savable ISavable,
	meta *SSMeta) (ss *pb.Snapshot, env *server.SSEnv, err error) {
	s.index = meta.Index
	f := func(cid uint64, nid uint64) string {
		return testSnapshotterDir
	}
	env = server.NewSSEnv(f, 1, 1, s.index, 1, server.SnapshottingMode, s.fs)
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := s.fs.PathJoin(testSnapshotterDir, fn)
	writer, err := NewSnapshotWriter(fp, SnapshotVersion, pb.NoCompression, s.fs)
	if err != nil {
		return nil, env, err
	}
	cw := dio.NewCountedWriter(writer)
	defer func() {
		if cerr := cw.Close(); err == nil {
			err = cerr
		}
		if ss != nil {
			ss.FileSize = cw.BytesWritten() + SnapshotHeaderSize
		}
	}()
	session := meta.Session.Bytes()
	if _, err := savable.SaveSnapshot(&SSMeta{}, cw, session, nil); err != nil {
		return nil, env, err
	}
	ss = &pb.Snapshot{
		Filepath:   env.GetFilepath(),
		Membership: meta.Membership,
		Index:      meta.Index,
		Term:       meta.Term,
	}
	return ss, env, nil
}

func (s *testSnapshotter) Load(loadableSessions ILoadableSessions,
	loadableSM ILoadableSM,
	fp string, fs []sm.SnapshotFile) error {
	reader, err := NewSnapshotReader(fp, s.fs)
	if err != nil {
		return err
	}
	defer func() {
		err = reader.Close()
	}()
	header, err := reader.GetHeader()
	if err != nil {
		return err
	}
	v := SSVersion(header.Version)
	if err := loadableSessions.LoadSessions(reader, v); err != nil {
		return err
	}
	if err := loadableSM.RecoverFromSnapshot(reader, fs); err != nil {
		return err
	}
	reader.ValidatePayload(header)
	return nil
}

func runSMTest(t *testing.T, tf func(t *testing.T, sm *StateMachine), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	store := tests.NewKVTest(1, 1)
	config := config.Config{ClusterID: 1, NodeID: 1}
	store.(*tests.KVTest).DisableLargeDelay()
	ds := NewNativeSM(config, NewRegularStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	tf(t, sm)
	reportLeakedFD(fs, t)
}

func runSMTest2(t *testing.T,
	tf func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := tests.NewKVTest(1, 1)
	config := config.Config{ClusterID: 1, NodeID: 1}
	store.(*tests.KVTest).DisableLargeDelay()
	ds := NewNativeSM(config, NewRegularStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	tf(t, sm, ds, nodeProxy, snapshotter, store)
	reportLeakedFD(fs, t)
}

func TestDefaultTaskIsNotSnapshotTask(t *testing.T) {
	task := Task{}
	if task.IsSnapshotTask() {
		t.Errorf("default task is a snapshot task")
	}
}

func TestUpdatesCanBeBatched(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ClusterID: 1, NodeID: 1}
	ds := NewNativeSM(config, NewConcurrentStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
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
	commit := Task{
		Entries: []pb.Entry{e1, e2, e3},
	}
	sm.index = 234
	sm.taskQ.Add(commit)
	// two commits to handle
	batch := make([]Task, 0, 8)
	if _, err := sm.Handle(batch, nil); err != nil {
		t.Fatalf("handle failed %v", err)
	}
	if sm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 237", sm.GetLastApplied())
	}
	count := store.UpdateCount
	if count != 3 {
		t.Fatalf("not batched as expected, batched update count %d, want 3", count)
	}
	reportLeakedFD(fs, t)
}

func TestMetadataEntryCanBeHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ClusterID: 1, NodeID: 1}
	ds := NewNativeSM(config, NewConcurrentStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	sm := NewStateMachine(ds, nil, config, nodeProxy, fs)
	e1 := pb.Entry{
		Type:  pb.MetadataEntry,
		Index: 235,
		Term:  1,
	}
	e2 := pb.Entry{
		Type:  pb.MetadataEntry,
		Index: 236,
		Term:  1,
	}
	e3 := pb.Entry{
		Type:  pb.MetadataEntry,
		Index: 237,
		Term:  1,
	}
	commit := Task{
		Entries: []pb.Entry{e1, e2, e3},
	}
	sm.index = 234
	sm.taskQ.Add(commit)
	// two commits to handle
	batch := make([]Task, 0, 8)
	if _, err := sm.Handle(batch, nil); err != nil {
		t.Fatalf("handle failed %v", err)
	}
	if sm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 237", sm.GetLastApplied())
	}
	if store.UpdateCount != 0 {
		t.Fatalf("Update() not suppose to be called")
	}
	reportLeakedFD(fs, t)
}

func testHandleBatchedSnappyEncodedEntry(t *testing.T,
	ct dio.CompressionType, fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := tests.NewConcurrentKVTest(1, 1)
	config := config.Config{ClusterID: 1, NodeID: 1}
	ds := NewNativeSM(config, NewConcurrentStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	tsm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	data1 := getTestKVData()
	data2 := getTestKVData2()
	encoded1 := GetEncodedPayload(ct, data1, make([]byte, 512))
	encoded2 := GetEncodedPayload(ct, data2, make([]byte, 512))
	e1 := pb.Entry{
		Type:     pb.EncodedEntry,
		ClientID: 123,
		SeriesID: 0,
		Cmd:      encoded1,
		Index:    236,
		Term:     1,
	}
	e2 := pb.Entry{
		Type:     pb.EncodedEntry,
		ClientID: 123,
		SeriesID: 0,
		Cmd:      encoded2,
		Index:    237,
		Term:     1,
	}
	tsm.index = 235
	entries := []pb.Entry{e1, e2}
	batch := make([]sm.Entry, 0, 8)
	if err := tsm.handleBatch(entries, batch); err != nil {
		t.Fatalf("handle failed %v", err)
	}
	if tsm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 236", tsm.GetLastApplied())
	}
	v, err := store.Lookup([]byte("test-key"))
	if err != nil {
		t.Errorf("%v", err)
	}
	if string(v.([]byte)) != "test-value" {
		t.Errorf("v: %s, want test-value", v)
	}
	v, err = store.Lookup([]byte("test-key-2"))
	if err != nil {
		t.Errorf("%v", err)
	}
	if string(v.([]byte)) != "test-value-2" {
		t.Errorf("v: %s, want test-value-2", v)
	}
	reportLeakedFD(fs, t)
}

func TestHandleBatchedSnappyEncodedEntry(t *testing.T) {
	fs := vfs.GetTestFS()
	testHandleBatchedSnappyEncodedEntry(t, dio.Snappy, fs)
	testHandleBatchedSnappyEncodedEntry(t, dio.NoCompression, fs)
}

func TestHandleAllocationCount(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	store := &tests.NoOP{NoAlloc: true}
	config := config.Config{ClusterID: 1, NodeID: 1}
	ds := NewNativeSM(config, NewRegularStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	sm.index = 1
	idx := uint64(1)
	batch := make([]Task, 0, 8)
	entries := make([]pb.Entry, 1)
	ac := testing.AllocsPerRun(1000, func() {
		idx++
		e1 := pb.Entry{
			ClientID: 123,
			SeriesID: client.NoOPSeriesID,
			Index:    idx,
			Term:     1,
		}
		entries[0] = e1
		commit := Task{
			Entries: entries,
		}
		sm.taskQ.Add(commit)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != idx {
			t.Errorf("last applied %d, want %d", sm.GetLastApplied(), idx)
		}
	})
	if ac != 0 {
		t.Fatalf("ac %f, want 0", ac)
	}
}

func TestUpdatesNotBatchedWhenNotAllNoOPUpdates(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ClusterID: 1, NodeID: 1}
	ds := NewNativeSM(config, &ConcurrentStateMachine{sm: store}, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
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
	commit := Task{
		Entries: []pb.Entry{e1, e2, e3},
	}
	sm.index = 234
	sm.taskQ.Add(commit)
	// two commits to handle
	batch := make([]Task, 0, 8)
	if _, err := sm.Handle(batch, nil); err != nil {
		t.Fatalf("handle failed %v", err)
	}
	if sm.GetLastApplied() != 237 {
		t.Errorf("last applied %d, want 237", sm.GetLastApplied())
	}
	reportLeakedFD(fs, t)
}

func TestStateMachineCanBeCreated(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		if sm.TaskChanBusy() {
			t.Errorf("commitChan busy")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
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
	commit := Task{
		Entries: []pb.Entry{e},
	}
	sm.index = index - 1
	sm.taskQ.Add(commit)
}

func TestBatchedLastAppliedValue(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.setBatchedLastApplied(12345)
		if sm.GetBatchedLastApplied() != 12345 {
			t.Errorf("batched last applied value can not be set/get")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
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
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembership(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = &pb.Membership{
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
			Witnesses: map[uint64]string{
				700: "a700",
			},
			ConfigChangeId: 12345,
		}
		m, o, w, r, cid := sm.GetMembership()
		if cid != 12345 {
			t.Errorf("unexpected cid value")
		}
		if len(m) != 2 || len(o) != 2 || len(r) != 3 || len(w) != 1 {
			t.Errorf("len changed")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembershipNodes(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = &pb.Membership{
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
		n, _, _, _, _ := sm.GetMembership()
		if len(n) != 2 {
			t.Errorf("unexpected len")
		}
		_, ok1 := n[100]
		_, ok2 := n[234]
		if !ok1 || !ok2 {
			t.Errorf("unexpected node id")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembershipHash(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		h1 := sm.GetMembershipHash()
		sm.members.members.Addresses[200] = "a200"
		h2 := sm.GetMembershipHash()
		if h1 == h2 {
			t.Errorf("hash doesn't change after membership change")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetSSMetaPanicWhenThereIsNoMember(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("didn't panic")
			}
		}()
		if _, err := sm.getSSMeta(nil, SSRequest{}); err != nil {
			t.Fatalf("get snapshot meta failed %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetSSMeta(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = &pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		applySessionRegisterEntry(sm, 12345, 789)
		sm.index = 100
		sm.term = 101
		meta, err := sm.getSSMeta(make([]byte, 123), SSRequest{})
		if err != nil {
			t.Fatalf("get snapshot meta failed %v", err)
		}
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
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
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
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if !nodeProxy.addPeer {
			t.Errorf("add peer not called")
		}
		v, ok := sm.members.members.Addresses[4]
		if !ok {
			t.Errorf("members not updated")
		}
		if v != "localhost:1010" {
			t.Errorf("address recorded %s, want localhost:1010", v)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddNodeAsObserverWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddNode,
			4,
			"localhost:1010",
			123)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("Handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		applyConfigChangeEntry(sm,
			123,
			pb.AddObserver,
			4,
			"localhost:1010",
			124)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if !nodeProxy.reject {
			t.Errorf("invalid cc not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
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

		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestObserverPromotion(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddObserver,
			4,
			"localhost:1010",
			123)

		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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

		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 124 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.addPeer {
			t.Errorf("add peer not called")
		}
		if len(sm.members.members.Addresses) != 1 {
			t.Errorf("node count != 1")
		}
		if len(sm.members.members.Observers) != 0 {
			t.Errorf("observer count != 0")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestInvalidObserverPromotionIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddObserver,
			4,
			"localhost:1010",
			123)

		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
		nodeProxy.accept = false
		applyConfigChangeEntry(sm,
			123,
			pb.AddNode,
			4,
			"localhost:1011",
			124)

		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		_, ok := sm.members.members.Addresses[4]
		if ok {
			t.Errorf("expectedly promoted observer")
		}
		if nodeProxy.accept {
			t.Errorf("unexpectedly accepted the promotion")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func testAddExistingMemberIsRejected(t *testing.T, tt pb.ConfigChangeType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[5] = "localhost:1010"
		applyConfigChangeEntry(sm,
			1,
			tt,
			4,
			"localhost:1010",
			123)

		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if nodeProxy.accept {
			t.Errorf("accept unexpectedly called")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
	}
	runSMTest2(t, tf, fs)
}

func TestAddExistingMemberIsRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	testAddExistingMemberIsRejected(t, pb.AddNode, fs)
	testAddExistingMemberIsRejected(t, pb.AddObserver, fs)
}

func testAddExistingMemberWithSameNodeIDIsRejected(t *testing.T,
	tt pb.ConfigChangeType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		if tt == pb.AddNode {
			sm.members.members.Addresses[5] = "localhost:1010"
		} else if tt == pb.AddObserver {
			sm.members.members.Observers[5] = "localhost:1010"
		} else {
			panic("unknown tt")
		}
		applyConfigChangeEntry(sm,
			1,
			tt,
			5,
			"localhost:1011",
			123)

		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if nodeProxy.accept {
			t.Errorf("accept unexpectedly called")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
	}
	runSMTest2(t, tf, fs)
}

func TestAddExistingMemberWithSameNodeIDIsRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	testAddExistingMemberWithSameNodeIDIsRejected(t, pb.AddNode, fs)
	testAddExistingMemberWithSameNodeIDIsRejected(t, pb.AddObserver, fs)
}

func TestHandleConfChangeRemoveNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1010"
		sm.members.members.Addresses[2] = "localhost:1011"
		applyConfigChangeEntry(sm,
			1,
			pb.RemoveNode,
			1,
			"",
			123)

		_, ok := sm.members.members.Addresses[1]
		if !ok {
			t.Errorf("node 1 not in members")
		}
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if !nodeProxy.accept {
			t.Errorf("accept not called")
		}
		if !nodeProxy.removePeer {
			t.Errorf("remove peer not called")
		}
		_, ok = sm.members.members.Addresses[1]
		if ok {
			t.Errorf("failed to remove node 1 from members")
		}
		_, ok = sm.members.members.Removed[1]
		if !ok {
			t.Errorf("removed node not recorded as removed")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestOrderedConfChangeIsAccepted(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm,
			6,
			pb.RemoveNode,
			1,
			"",
			123)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if nodeProxy.reject {
			t.Errorf("rejected")
		}
		if !nodeProxy.accept {
			t.Errorf("not accepted")
		}
		if !nodeProxy.removePeer {
			t.Errorf("remove peer not called")
		}
		if sm.members.members.ConfigChangeId != 123 {
			t.Errorf("conf change id not updated, %d", sm.members.members.ConfigChangeId)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRemoveOnlyNodeIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1010"
		applyConfigChangeEntry(sm,
			1,
			pb.RemoveNode,
			1,
			"",
			123)
		_, ok := sm.members.members.Addresses[1]
		if !ok {
			t.Errorf("node 1 not in members")
		}
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 123 {
			t.Errorf("last applied %d, want 123", sm.GetLastApplied())
		}
		if nodeProxy.accept {
			t.Errorf("unexpected accepted")
		}
		if nodeProxy.removePeer {
			t.Errorf("remove peer unexpected called")
		}
		_, ok = sm.members.members.Addresses[1]
		if !ok {
			t.Errorf("unexpectedly removed node 1 from members")
		}
		_, ok = sm.members.members.Removed[1]
		if ok {
			t.Errorf("removed node 1 as removed")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddingNodeOnTheSameNodeHostWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		sm.members.members.Addresses[100] = "test.nodehost"
		applyConfigChangeEntry(sm,
			7,
			pb.AddNode,
			2,
			"test.nodehost",
			123)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
		if sm.members.members.ConfigChangeId == 123 {
			t.Errorf("conf change unexpected updated, %d", sm.members.members.ConfigChangeId)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddingRemovedNodeWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		sm.members.members.Removed[2] = true
		applyConfigChangeEntry(sm,
			7,
			pb.AddNode,
			2,
			"a1",
			123)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.addPeer {
			t.Errorf("add peer unexpectedly called")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestOutOfOrderConfChangeIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.ordered = true
		sm.members.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm,
			1,
			pb.RemoveNode,
			1,
			"",
			123)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if !nodeProxy.reject {
			t.Errorf("not rejected")
		}
		if nodeProxy.removePeer {
			t.Errorf("remove peer unexpectedly called")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestHandleSyncTask(t *testing.T) {
	tf := func(t *testing.T, tsm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		task1 := Task{
			PeriodicSync: true,
		}
		task2 := Task{
			PeriodicSync: true,
		}
		tsm.index = 100
		tsm.taskQ.Add(task1)
		tsm.taskQ.Add(task2)
		if _, err := tsm.Handle(make([]Task, 0), make([]sm.Entry, 0)); err != nil {
			t.Fatalf("%v", err)
		}
		if tsm.index != 100 {
			t.Errorf("index unexpected moved")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestHandleEmptyEvent(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		e := pb.Entry{
			Type:  pb.ApplicationEntry,
			Index: 234,
			Term:  1,
		}
		commit := Task{
			Entries: []pb.Entry{e},
		}
		sm.index = 233
		sm.taskQ.Add(commit)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 234 {
			t.Errorf("last applied %d, want 234", sm.GetLastApplied())
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func getTestKVData() []byte {
	return genTestKVData("test-key", "test-value")
}

func getTestKVData2() []byte {
	return genTestKVData("test-key-2", "test-value-2")
}

func genTestKVData(k, d string) []byte {
	u := kvpb.PBKV{
		Key: k,
		Val: d,
	}
	data, err := proto.Marshal(&u)
	if err != nil {
		panic(err)
	}
	return data
}

func testHandleSnappyEncodedEntry(t *testing.T, ct dio.CompressionType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		data := getTestKVData()
		encoded := GetEncodedPayload(ct, data, make([]byte, 512))
		e1 := pb.Entry{
			Type:     pb.EncodedEntry,
			ClientID: 123,
			SeriesID: 0,
			Cmd:      encoded,
			Index:    236,
			Term:     1,
		}
		commit := Task{
			Entries: []pb.Entry{e1},
		}
		sm.index = 235
		sm.taskQ.Add(commit)
		// two commits to handle
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
	}
	runSMTest2(t, tf, fs)
}

func TestHandleSnappyEncodedEntry(t *testing.T) {
	fs := vfs.GetTestFS()
	testHandleSnappyEncodedEntry(t, dio.Snappy, fs)
	testHandleSnappyEncodedEntry(t, dio.NoCompression, fs)
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
		commit := Task{
			Entries: []pb.Entry{e1, e2},
		}
		sm.index = 234
		sm.taskQ.Add(commit)
		// two commits to handle
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
		if string(result.([]byte)) != "test-value" {
			t.Errorf("result %s, want test-value", string(result.([]byte)))
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestSnapshotCanBeApplied(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1234"
		store.(*tests.KVTest).KVStore["test-key1"] = "test-value1"
		store.(*tests.KVTest).KVStore["test-key2"] = "test-value2"
		sm.index = 3
		hash1, _ := sm.GetHash()
		ss, _, err := sm.SaveSnapshot(SSRequest{})
		if err != nil {
			t.Fatalf("failed to make snapshot %v", err)
		}
		index := ss.Index
		commit := Task{
			Index: index,
		}
		store2 := tests.NewKVTest(1, 1)
		config := config.Config{ClusterID: 1, NodeID: 1}
		store2.(*tests.KVTest).DisableLargeDelay()
		ds2 := NewNativeSM(config, NewRegularStateMachine(store2), make(chan struct{}))
		nodeProxy2 := newTestNodeProxy()
		snapshotter2 := newTestSnapshotter(fs)
		sm2 := NewStateMachine(ds2, snapshotter2, config, nodeProxy2, fs)
		if len(sm2.members.members.Addresses) != 0 {
			t.Errorf("unexpected member length")
		}
		index2, err := sm2.RecoverFromSnapshot(commit)
		if err != nil {
			t.Errorf("apply snapshot failed %v", err)
		}
		if index2 != index {
			t.Errorf("last applied %d, want %d", index2, index)
		}
		hash2, _ := sm2.GetHash()
		if hash1 != hash2 {
			t.Errorf("bad hash %d, want %d, sz %d",
				hash2, hash1, len(store2.(*tests.KVTest).KVStore))
		}
		// see whether members info are recovered
		if len(sm2.members.members.Addresses) != 2 {
			t.Errorf("failed to restore members")
		}
		v1, ok1 := sm2.members.members.Addresses[1]
		v2, ok2 := sm2.members.members.Addresses[2]
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
	runSMTest2(t, tf, fs)
}

func TestMembersAreSavedWhenMakingSnapshot(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1"
		sm.members.members.Addresses[2] = "localhost:2"
		ss, _, err := sm.SaveSnapshot(SSRequest{})
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
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestSnapshotTwiceIsHandled(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1"
		sm.members.members.Addresses[2] = "localhost:2"
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 1, 0, data)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		_, _, err := sm.SaveSnapshot(SSRequest{})
		if err != nil {
			t.Errorf("failed to make snapshot %v", err)
		}
		_, _, err = sm.SaveSnapshot(SSRequest{})
		if err != raft.ErrSnapshotOutOfDate {
			t.Errorf("snapshot twice completed, %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func applySessionRegisterEntry(sm *StateMachine,
	clientID uint64, index uint64) pb.Entry {
	e := pb.Entry{
		ClientID: clientID,
		SeriesID: client.SeriesIDForRegister,
		Index:    index,
		Term:     1,
	}
	commit := Task{
		Entries: []pb.Entry{e},
	}
	sm.index = index - 1
	sm.taskQ.Add(commit)
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
	commit := Task{
		Entries: []pb.Entry{e},
	}
	sm.taskQ.Add(commit)
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
	commit := Task{
		Entries: []pb.Entry{e},
	}
	sm.taskQ.Add(commit)
	return e
}

func TestSessionCanBeCreatedAndRemoved(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		clientID := uint64(12345)
		applySessionRegisterEntry(sm, clientID, 789)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != 789 {
			t.Errorf("last applied %d, want 789", sm.GetLastApplied())
		}
		sessionManager := sm.sessions.sessions
		_, ok := sessionManager.getSession(RaftClientID(clientID))
		if !ok {
			t.Errorf("session not found")
		}
		if nodeProxy.smResult.Value != clientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult.Value, clientID)
		}
		index := uint64(790)
		applySessionUnregisterEntry(sm, 12345, index)

		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), index)
		}
		_, ok = sessionManager.getSession(RaftClientID(clientID))
		if ok {
			t.Errorf("session not removed")
		}
		if nodeProxy.smResult.Value != clientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, clientID)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestDuplicatedSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		e := applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		sessionManager := sm.sessions.sessions
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		if !ok {
			t.Errorf("session not found")
		}
		if nodeProxy.smResult.Value != e.ClientID {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult.Value, e.ClientID)
		}
		e.Index = 790
		commit := Task{
			Entries: []pb.Entry{e},
		}
		sm.taskQ.Add(commit)
		if nodeProxy.rejected {
			t.Errorf("rejected flag set too early")
		}
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if nodeProxy.smResult.Value != 0 {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, 0)
		}
		if !nodeProxy.rejected {
			t.Errorf("reject flag not set")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRemovingUnregisteredSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		sm.index = 789
		e := applySessionUnregisterEntry(sm, 12345, 790)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		sessionManager := sm.sessions.sessions
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		if ok {
			t.Errorf("session not suppose to be there")
		}
		if nodeProxy.smResult.Value != 0 {
			t.Errorf("smResult %d, want %d", nodeProxy.smResult, 0)
		}
		if !nodeProxy.rejected {
			t.Errorf("reject flag not set")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestUpdateFromUnregisteredClientWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		sm.index = 789
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		// handleUpdateEvent suppose to return a (result, ignored, rejected) tuple of
		// (0, false, true)
		if nodeProxy.ignored {
			t.Errorf("ignored %t, want false", nodeProxy.ignored)
		}
		if nodeProxy.smResult.Value != 0 {
			t.Errorf("smResult %d, want 0", nodeProxy.smResult)
		}
		if !nodeProxy.rejected {
			t.Errorf("rejected %t, want true", nodeProxy.rejected)
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestDuplicatedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		// check normal update is accepted and handled
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		plog.Infof("Handle returned")
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if !nodeProxy.applyUpdateInvoked {
			t.Errorf("update not invoked")
		}
		if nodeProxy.smResult.Value != uint64(len(data)) {
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
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRespondedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		// check normal update is accepted and handleped
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		storeCount := store.(*tests.KVTest).Count
		// update the respondedto value
		e = applyTestEntry(sm, 12345, 1, 791, 1, data)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		sessionManager := sm.sessions.sessions
		session, _ := sessionManager.getSession(RaftClientID(12345))
		if session.RespondedUpTo != RaftSeriesID(1) {
			t.Errorf("responded to %d, want 1", session.RespondedUpTo)
		}
		nodeProxy.applyUpdateInvoked = false
		// submit the same stuff again with a different index value
		e = applyTestEntry(sm, 12345, 1, 792, 1, data)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
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
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestNoOPSessionAllowEntryToBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter, store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 790, 0, data)
		// check normal update is accepted and handleped
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		storeCount := store.(*tests.KVTest).Count
		// different index as the same entry is proposed again
		e = applyTestEntry(sm, 12345, client.NoOPSeriesID, 791, 0, data)
		// check normal update is accepted and handleped
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != e.Index {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.Index)
		}
		if storeCount == store.(*tests.KVTest).Count {
			t.Errorf("entry not applied")
		}
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
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

func TestNoOPEntryIsNotBatched(t *testing.T) {
	updates, _ := getEntryTypes([]pb.Entry{{}})
	if updates {
		t.Errorf("NoOP entry is considered as regular update entry")
	}
}

func TestRegularSessionedEntryIsNotBatched(t *testing.T) {
	e := pb.Entry{
		ClientID: client.NotSessionManagedClientID + 1,
		SeriesID: client.NoOPSeriesID + 1,
	}
	_, allNoOP := getEntryTypes([]pb.Entry{{}, e})
	if allNoOP {
		t.Errorf("regular sessioned entry not detected")
	}
}

func TestNonUpdateEntryIsNotBatched(t *testing.T) {
	cce := pb.Entry{Type: pb.ConfigChangeEntry}
	notSessionManaged := pb.Entry{ClientID: client.NotSessionManagedClientID}
	newSessionEntry := pb.Entry{SeriesID: client.SeriesIDForRegister}
	unSessionEntry := pb.Entry{SeriesID: client.SeriesIDForUnregister}
	entries := []pb.Entry{cce, notSessionManaged, newSessionEntry, unSessionEntry}
	for _, e := range entries {
		if e.IsUpdateEntry() {
			t.Errorf("incorrectly considered as update entry")
		}
	}
	for _, e := range entries {
		updates, _ := getEntryTypes([]pb.Entry{e})
		if updates {
			t.Errorf("incorrectly considered as update entry")
		}
	}
}

func TestEntryAppliedInDiskSM(t *testing.T) {
	tests := []struct {
		onDiskSM        bool
		onDiskInitIndex uint64
		index           uint64
		result          bool
	}{
		{true, 100, 50, true},
		{true, 100, 100, true},
		{true, 100, 200, false},
		{false, 100, 50, false},
		{false, 100, 100, false},
		{false, 100, 200, false},
	}
	for idx, tt := range tests {
		sm := StateMachine{onDiskSM: tt.onDiskSM, onDiskInitIndex: tt.onDiskInitIndex}
		result := sm.entryInInitDiskSM(tt.index)
		if result != tt.result {
			t.Errorf("%d failed", idx)
		}
	}
}

func TestRecoverSMRequired(t *testing.T) {
	tests := []struct {
		dummy           bool
		shrunk          bool
		init            bool
		onDiskIndex     uint64
		onDiskInitIndex uint64
		required        bool
	}{
		{true, true, true, 100, 100, false},
		{true, true, true, 200, 100, false},
		{true, true, true, 100, 200, false},
		{true, true, false, 100, 100, false},
		{true, true, false, 200, 100, false},
		{true, true, false, 100, 200, false},
		{true, false, true, 100, 100, false},
		{true, false, true, 200, 100, false},
		{true, false, true, 100, 200, false},
		{true, false, false, 100, 100, false},
		{true, false, false, 200, 100, false},
		{true, false, false, 100, 200, false},

		{false, true, true, 100, 100, false},
		{false, true, true, 200, 100, false},
		{false, true, true, 100, 200, false},
		{false, true, false, 100, 100, false},
		{false, true, false, 200, 100, false},
		{false, true, false, 100, 200, false},
		{false, false, true, 100, 100, false},
		{false, false, true, 200, 100, true},
		{false, false, true, 100, 200, false},
		{false, false, false, 100, 100, false},
		{false, false, false, 200, 100, true},
		{false, false, false, 100, 200, false},
	}
	ssIndex := uint64(200)
	for idx, tt := range tests {
		fs := vfs.GetTestFS()
		defer fs.RemoveAll(testSnapshotterDir)
		func() {
			fs.RemoveAll(testSnapshotterDir)
			if err := fs.MkdirAll(testSnapshotterDir, 0755); err != nil {
				t.Fatalf("mkdir failed %v", err)
			}
			snapshotter := newTestSnapshotter(fs)
			sm := &StateMachine{
				snapshotter:     snapshotter,
				onDiskSM:        true,
				onDiskInitIndex: tt.onDiskInitIndex,
				onDiskIndex:     tt.onDiskInitIndex,
				fs:              fs,
			}
			fp := snapshotter.GetFilePath(ssIndex)
			if tt.shrunk {
				fp = fp + ".tmp"
			}
			w, err := NewSnapshotWriter(fp, SnapshotVersion, pb.NoCompression, fs)
			if err != nil {
				t.Fatalf("failed to create snapshot writer %v", err)
			}
			sessionData := make([]byte, testSessionSize)
			storeData := make([]byte, testPayloadSize)
			rand.Read(sessionData)
			rand.Read(storeData)
			n, err := w.Write(sessionData)
			if err != nil || n != len(sessionData) {
				t.Fatalf("failed to write the session data")
			}
			m, err := w.Write(storeData)
			if err != nil || m != len(storeData) {
				t.Fatalf("failed to write the store data")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("%v", err)
			}
			if tt.shrunk {
				if err := ShrinkSnapshot(fp,
					snapshotter.GetFilePath(ssIndex), fs); err != nil {
					t.Fatalf("failed to shrink %v", err)
				}
			}
			ss := pb.Snapshot{
				Dummy:       tt.dummy,
				Index:       ssIndex,
				OnDiskIndex: tt.onDiskIndex,
			}
			defer func() {
				if !tt.dummy && !tt.init && tt.shrunk {
					if r := recover(); r == nil {
						t.Fatalf("not panic")
					}
				}
			}()
			if res := sm.recoverSMRequired(ss, tt.init); res != tt.required {
				t.Errorf("%d, result %t, want %t", idx, res, tt.required)
			}
		}()
		reportLeakedFD(fs, t)
	}
}

func TestReadyToStreamSnapshot(t *testing.T) {
	tests := []struct {
		onDisk          bool
		index           uint64
		onDiskInitIndex uint64
		ready           bool
	}{
		{true, 100, 100, true},
		{true, 200, 100, true},
		{true, 100, 200, false},
		{false, 100, 100, true},
		{false, 200, 100, true},
		{false, 100, 200, true},
	}
	for idx, tt := range tests {
		sm := &StateMachine{
			onDiskSM:        tt.onDisk,
			index:           tt.index,
			onDiskInitIndex: tt.onDiskInitIndex,
		}
		if result := sm.ReadyToStreamSnapshot(); result != tt.ready {
			t.Errorf("%d, result %t, want %t", idx, result, tt.ready)
		}
	}
}

func TestUpdateLastApplied(t *testing.T) {
	tests := []struct {
		index uint64
		term  uint64
		crash bool
	}{
		{0, 100, true},
		{101, 0, true},
		{100, 100, true},
		{101, 100, false},
		{100, 90, true},
		{100, 101, true},
		{100, 110, true},
		{101, 0, true},
		{101, 99, true},
	}
	for idx, tt := range tests {
		func() {
			sm := &StateMachine{index: 100, term: 100}
			if tt.crash {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("no panic")
					}
				}()
			}
			sm.updateLastApplied(tt.index, tt.term)
			if sm.index != tt.index {
				t.Errorf("%d, index not updated", idx)
			}
			if sm.term != tt.term {
				t.Errorf("%d, term not updated", idx)
			}
		}()
	}
}

func TestAlreadyAppliedInOnDiskSMEntryTreatedAsNoOP(t *testing.T) {
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 100,
		index:           90,
		term:            5,
	}
	ent := pb.Entry{
		ClientID: 100,
		Index:    91,
		Term:     6,
	}
	if err := sm.handleEntry(ent, false); err != nil {
		t.Fatalf("handle entry failed %v", err)
	}
	if sm.index != 91 {
		t.Errorf("index not moved")
	}
	if sm.term != 6 {
		t.Errorf("term not moved")
	}
}

type testManagedStateMachine struct {
	first        uint64
	last         uint64
	synced       bool
	nalookup     bool
	corruptIndex bool
}

func (t *testManagedStateMachine) Open() (uint64, error) { return 10, nil }
func (t *testManagedStateMachine) Update(sm.Entry) (sm.Result, error) {
	return sm.Result{}, nil
}
func (t *testManagedStateMachine) Lookup(interface{}) (interface{}, error) { return nil, nil }
func (t *testManagedStateMachine) NALookup(input []byte) ([]byte, error) {
	t.nalookup = true
	return input, nil
}
func (t *testManagedStateMachine) Sync() error {
	t.synced = true
	return nil
}
func (t *testManagedStateMachine) GetHash() (uint64, error)              { return 0, nil }
func (t *testManagedStateMachine) PrepareSnapshot() (interface{}, error) { return nil, nil }
func (t *testManagedStateMachine) SaveSnapshot(*SSMeta,
	io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error) {
	return false, nil
}
func (t *testManagedStateMachine) RecoverFromSnapshot(io.Reader, []sm.SnapshotFile) error {
	return nil
}
func (t *testManagedStateMachine) StreamSnapshot(interface{}, io.Writer) error { return nil }
func (t *testManagedStateMachine) Offloaded(From) bool                         { return false }
func (t *testManagedStateMachine) Loaded(From)                                 {}
func (t *testManagedStateMachine) ConcurrentSnapshot() bool                    { return false }
func (t *testManagedStateMachine) OnDiskStateMachine() bool                    { return false }
func (t *testManagedStateMachine) StateMachineType() pb.StateMachineType       { return 0 }
func (t *testManagedStateMachine) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	if !t.corruptIndex {
		t.first = ents[0].Index
		t.last = ents[len(ents)-1].Index
	} else {
		for idx := range ents {
			ents[idx].Index = ents[idx].Index + 1
		}
	}

	return ents, nil
}

func TestOnDiskStateMachineCanBeOpened(t *testing.T) {
	msm := &testManagedStateMachine{}
	np := newTestNodeProxy()
	sm := &StateMachine{
		onDiskSM: true,
		sm:       msm,
		node:     np,
	}
	index, err := sm.OpenOnDiskStateMachine()
	if err != nil {
		t.Errorf("open sm failed %v", err)
	}
	if index != 10 {
		t.Errorf("unexpectedly index %d", index)
	}
	if sm.onDiskInitIndex != 10 {
		t.Errorf("disk sm index not recorded")
	}
}

func TestSaveConcurrentSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	msm := &testManagedStateMachine{}
	np := newTestNodeProxy()
	createTestDir(fs)
	defer removeTestDir(fs)
	sm := &StateMachine{
		onDiskSM:    true,
		sm:          msm,
		node:        np,
		index:       100,
		term:        5,
		snapshotter: newTestSnapshotter(fs),
		sessions:    NewSessionManager(),
		members:     newMembership(1, 1, false),
		fs:          fs,
	}
	sm.members.members.Addresses[1] = "a1"
	ss, _, err := sm.saveConcurrentSnapshot(SSRequest{})
	if err != nil {
		t.Fatalf("concurrent snapshot failed %v", err)
	}
	if ss.Index != 100 {
		t.Errorf("unexpected index")
	}
	if !msm.synced {
		t.Errorf("not synced")
	}
	reportLeakedFD(fs, t)
}

func TestStreamSnapshot(t *testing.T) {
	msm := &testManagedStateMachine{}
	np := newTestNodeProxy()
	fs := vfs.GetTestFS()
	sm := &StateMachine{
		onDiskSM:    true,
		sm:          msm,
		node:        np,
		index:       100,
		term:        5,
		snapshotter: newTestSnapshotter(fs),
		sessions:    NewSessionManager(),
		members:     newMembership(1, 1, false),
		fs:          fs,
	}
	sm.members.members.Addresses[1] = "a1"
	ts := &testSink{
		chunks: make([]pb.Chunk, 0),
	}
	if err := sm.StreamSnapshot(ts); err != nil {
		t.Errorf("stream snapshot failed %v", err)
	}
	if len(ts.chunks) != 3 {
		t.Fatalf("unexpected chunk count")
	}
	if !ts.chunks[1].IsLastChunk() {
		t.Errorf("failed to get tail chunk")
	}
	if !ts.chunks[2].IsPoisonChunk() {
		t.Errorf("failed to get the poison chunk")
	}
	reportLeakedFD(fs, t)
}

func TestHandleBatchedEntriesForOnDiskSM(t *testing.T) {
	tests := []struct {
		onDiskInitIndex uint64
		index           uint64
		first           uint64
		last            uint64
		firstApplied    uint64
		lastApplied     uint64
	}{
		{100, 50, 51, 60, 0, 0},
		{100, 50, 51, 100, 0, 0},
		{100, 50, 51, 110, 101, 110},
		{100, 100, 101, 120, 101, 120},
		{100, 110, 111, 125, 111, 125},
	}
	for idx, tt := range tests {
		input := make([]pb.Entry, 0)
		for i := tt.first; i <= tt.last; i++ {
			input = append(input, pb.Entry{Index: i, Term: 100})
		}
		ents := make([]sm.Entry, 0)
		msm := &testManagedStateMachine{}
		np := newTestNodeProxy()
		sm := &StateMachine{
			onDiskSM:        true,
			onDiskInitIndex: tt.onDiskInitIndex,
			index:           tt.index,
			term:            100,
			sm:              msm,
			node:            np,
		}
		if err := sm.handleBatch(input, ents); err != nil {
			t.Fatalf("handle batched entries failed %v", err)
		}
		if msm.first != tt.firstApplied {
			t.Errorf("%d, unexpected first value, %d, %d", idx, msm.first, tt.firstApplied)
		}
		if msm.last != tt.lastApplied {
			t.Errorf("%d, unexpected last value, %d, %d", idx, msm.last, tt.lastApplied)
		}
		if sm.batchedIndex.index != tt.last {
			t.Errorf("%d, index %d, last %d", idx, sm.batchedIndex.index, tt.last)
		}
		if np.firstIndex != tt.firstApplied {
			t.Errorf("unexpected first applied index: %d, want %d", np.firstIndex, tt.firstApplied)
		}
		if np.index != tt.lastApplied {
			t.Errorf("%d, unexpected first value, %d, %d", idx, np.index, tt.lastApplied)
		}
		if sm.index != tt.last {
			t.Errorf("unexpected last applied index %d, want %d", sm.index, tt.last)
		}
	}
}

func TestCorruptedIndexValueWillBeDetected(t *testing.T) {
	ents := make([]sm.Entry, 0)
	msm := &testManagedStateMachine{corruptIndex: true}
	np := newTestNodeProxy()
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 0,
		index:           0,
		term:            100,
		sm:              msm,
		node:            np,
	}
	input := make([]pb.Entry, 0)
	for i := uint64(1); i <= 10; i++ {
		input = append(input, pb.Entry{Index: i, Term: 100})
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	if err := sm.handleBatch(input, ents); err != nil {
		t.Fatalf("handle batched entries failed %v", err)
	}
}

func TestNodeReadyIsSetWhenAnythingFromTaskQIsProcessed(t *testing.T) {
	ents := make([]sm.Entry, 0)
	batch := make([]Task, 0)
	msm := &testManagedStateMachine{}
	np := newTestNodeProxy()
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 0,
		index:           0,
		term:            0,
		sm:              msm,
		taskQ:           NewTaskQueue(),
		node:            np,
	}
	rec, err := sm.Handle(batch, ents)
	if rec.IsSnapshotTask() {
		t.Errorf("why snapshot?")
	}
	if err != nil {
		t.Fatalf("handle failed")
	}
	if np.nodeReady != 0 {
		t.Errorf("nodeReady unexpectedly updated")
	}
	sm.taskQ.Add(Task{})
	rec, err = sm.Handle(batch, ents)
	if rec.IsSnapshotTask() {
		t.Errorf("why snapshot?")
	}
	if err != nil {
		t.Fatalf("handle failed")
	}
	if np.nodeReady != 1 {
		t.Errorf("unexpected nodeReady count %d", np.nodeReady)
	}
}

func TestSyncedIndex(t *testing.T) {
	sm := &StateMachine{}
	sm.setSyncedIndex(100)
	if sm.GetSyncedIndex() != 100 {
		t.Errorf("failed to get synced index")
	}
	sm.setSyncedIndex(100)
	if sm.GetSyncedIndex() != 100 {
		t.Errorf("failed to get synced index")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	sm.setSyncedIndex(99)
}

func TestNALookup(t *testing.T) {
	msm := &testManagedStateMachine{}
	sm := &StateMachine{
		sm: msm,
	}
	input := make([]byte, 128)
	rand.Read(input)
	result, err := sm.NALookup(input)
	if err != nil {
		t.Errorf("NALookup failed %v", err)
	}
	if !bytes.Equal(input, result) {
		t.Errorf("result changed")
	}
	if !msm.nalookup {
		t.Errorf("NALookup not called")
	}
}

func TestIsDummySnapshot(t *testing.T) {
	tests := []struct {
		onDisk    bool
		exported  bool
		streaming bool
		dummy     bool
	}{
		{false, false, false, false},
		{false, true, false, false},
		{false, false, true, false},
		{true, true, false, false},
		{true, false, true, false},
		{true, false, false, true},
	}
	for idx, tt := range tests {
		s := &StateMachine{onDiskSM: tt.onDisk}
		r := SSRequest{Type: PeriodicSnapshot}
		if tt.exported {
			r.Type = ExportedSnapshot
		}
		if tt.streaming {
			r.Type = StreamSnapshot
		}
		if s.isDummySnapshot(r) != tt.dummy {
			t.Errorf("%d, is dummy test failed", idx)
		}
	}
}

func TestWitnessNodeIsNeverConsideredAsOnDiskSM(t *testing.T) {
	tests := []struct {
		onDiskSM  bool
		isWitness bool
		result    bool
	}{
		{true, true, false},
		{true, false, true},
		{false, true, false},
		{false, false, false},
	}
	for idx, tt := range tests {
		sm := &StateMachine{
			onDiskSM:  tt.onDiskSM,
			isWitness: tt.isWitness,
		}
		if sm.OnDiskStateMachine() != tt.result {
			t.Errorf("%d, got %t, want %t", idx, sm.OnDiskStateMachine(), tt.result)
		}
	}
}

func TestWitnessNodePanicWhenSavingSnapshot(t *testing.T) {
	sm := &StateMachine{isWitness: true}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	sm.SaveSnapshot(SSRequest{})
}
