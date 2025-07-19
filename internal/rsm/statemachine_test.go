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

package rsm

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/raft"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/tests"
	"github.com/lni/dragonboat/v4/internal/tests/kvpb"
	"github.com/lni/dragonboat/v4/internal/utils/dio"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	testSnapshotterDir = "rsm_test_data_safe_to_delete"
	snapshotFileSuffix = "gbsnap"
)

func removeTestDir(fs vfs.IFS) {
	if err := fs.RemoveAll(testSnapshotterDir); err != nil {
		panic(err)
	}
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
	addNonVoting       bool
	addNonVotingCount  uint64
	nodeReady          uint64
	applyUpdateCalled  bool
	firstIndex         uint64
}

func newTestNodeProxy() *testNodeProxy {
	return &testNodeProxy{}
}

func (p *testNodeProxy) StepReady() { p.nodeReady++ }

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

func (p *testNodeProxy) RestoreRemotes(s pb.Snapshot) error {
	for k := range s.Membership.Addresses {
		_ = k
		p.addPeer = true
		p.addPeerCount++
	}
	return nil
}

func (p *testNodeProxy) ApplyConfigChange(cc pb.ConfigChange,
	key uint64, rejected bool) error {
	if !rejected {
		p.applyConfChange = true
		switch cc.Type {
		case pb.AddNode:
			p.addPeer = true
			p.addPeerCount++
		case pb.AddNonVoting:
			p.addNonVoting = true
			p.addNonVotingCount++
		case pb.RemoveNode:
			p.removePeer = true
		}
	}
	p.configChangeProcessed(key, rejected)
	return nil
}

func (p *testNodeProxy) configChangeProcessed(index uint64, rejected bool) {
	if rejected {
		p.reject = true
	} else {
		p.accept = true
	}
}

func (p *testNodeProxy) ReplicaID() uint64 { return 1 }
func (p *testNodeProxy) ShardID() uint64   { return 1 }

type noopCompactor struct{}

func (noopCompactor) Compact(uint64) error { return nil }

var testCompactor = &noopCompactor{}

type testSnapshotter struct {
	index    uint64
	dataSize uint64
	fs       vfs.IFS
}

func newTestSnapshotter(fs vfs.IFS) *testSnapshotter {
	return &testSnapshotter{fs: fs}
}

func (s *testSnapshotter) GetSnapshot() (pb.Snapshot, error) {
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := s.fs.PathJoin(testSnapshotterDir, fn)
	address := make(map[uint64]string)
	address[1] = "localhost:1"
	address[2] = "localhost:2"
	ss := pb.Snapshot{
		Filepath: fp,
		FileSize: s.dataSize,
		Index:    s.index,
		Term:     2,
		Membership: pb.Membership{
			Addresses: address,
		},
	}
	ss.Load(testCompactor)
	return ss, nil
}

func (s *testSnapshotter) Shrunk(ss pb.Snapshot) (bool, error) {
	return IsShrunkSnapshotFile(s.getFilePath(ss.Index), s.fs)
}

func (s *testSnapshotter) getFilePath(index uint64) string {
	filename := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	return s.fs.PathJoin(testSnapshotterDir, filename)
}

func (s *testSnapshotter) GetSnapshotFromLogDB() (pb.Snapshot, error) {
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
	meta SSMeta, sink pb.IChunkSink) error {
	writer := NewChunkWriter(sink, meta)
	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()
	return streamable.Stream(meta.Ctx, writer)
}

func (s *testSnapshotter) Save(savable ISavable,
	meta SSMeta) (ss pb.Snapshot, env SSEnv, err error) {
	s.index = meta.Index
	f := func(cid uint64, nid uint64) string {
		return testSnapshotterDir
	}
	env = server.NewSSEnv(f, 1, 1, s.index, 1, server.SnapshotMode, s.fs)
	fn := fmt.Sprintf("snapshot-test.%s", snapshotFileSuffix)
	fp := s.fs.PathJoin(testSnapshotterDir, fn)
	writer, err := NewSnapshotWriter(fp, pb.NoCompression, s.fs)
	if err != nil {
		return pb.Snapshot{}, env, err
	}
	cw := dio.NewCountedWriter(writer)
	defer func() {
		err = firstError(err, cw.Close())
		if ss.Index > 0 {
			ss.FileSize = cw.BytesWritten() + HeaderSize
		}
	}()
	session := meta.Session.Bytes()
	if _, err := savable.Save(SSMeta{},
		cw, session, nil); err != nil {
		return pb.Snapshot{}, env, err
	}
	ss = pb.Snapshot{
		Filepath:   env.GetFilepath(),
		Membership: meta.Membership,
		Index:      meta.Index,
		Term:       meta.Term,
	}
	return ss, env, nil
}

func (s *testSnapshotter) Load(ss pb.Snapshot,
	loadable ILoadable, recoverable IRecoverable) error {
	fp := s.getFilePath(ss.Index)
	fs := make([]sm.SnapshotFile, 0)
	for _, f := range ss.Files {
		fs = append(fs, sm.SnapshotFile{
			FileID:   f.FileId,
			Filepath: f.Filepath,
			Metadata: f.Metadata,
		})
	}
	reader, header, err := NewSnapshotReader(fp, s.fs)
	if err != nil {
		return err
	}
	defer func() {
		err = reader.Close()
	}()
	v := SSVersion(header.Version)
	if err := loadable.LoadSessions(reader, v); err != nil {
		return err
	}
	if err := recoverable.Recover(reader, fs); err != nil {
		return err
	}
	return nil
}

func runSMTest(t *testing.T,
	tf func(t *testing.T, sm *StateMachine), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	store := tests.NewKVTest(1, 1)
	config := config.Config{ShardID: 1, ReplicaID: 1}
	store.(*tests.KVTest).DisableLargeDelay()
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
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
	config := config.Config{ShardID: 1, ReplicaID: 1}
	store.(*tests.KVTest).DisableLargeDelay()
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	tf(t, sm, ds, nodeProxy, snapshotter, store)
	reportLeakedFD(fs, t)
}

func TestDefaultTaskIsNotSnapshotTask(t *testing.T) {
	task := Task{}
	require.False(t, task.IsSnapshotTask())
}

func TestUpdatesCanBeBatched(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config,
		NewConcurrentStateMachine(store), make(chan struct{}))
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
	sm.lastApplied.index = 234
	sm.index = 234
	sm.taskQ.Add(commit)
	batch := make([]Task, 0, 8)
	_, err := sm.Handle(batch, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(237), sm.GetLastApplied())
	count := store.UpdateCount
	require.Equal(t, 3, count,
		"not batched as expected, batched update count %d, want 3", count)
	reportLeakedFD(fs, t)
}

func TestMetadataEntryCanBeHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config,
		NewConcurrentStateMachine(store), make(chan struct{}))
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
	sm.lastApplied.index = 234
	sm.index = 234
	sm.taskQ.Add(commit)
	batch := make([]Task, 0, 8)
	_, err := sm.Handle(batch, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(237), sm.GetLastApplied())
	require.Equal(t, 0, store.UpdateCount,
		"Update() not suppose to be called")
	reportLeakedFD(fs, t)
}

func testHandleBatchedSnappyEncodedEntry(t *testing.T,
	ct dio.CompressionType, fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := tests.NewConcurrentKVTest(1, 1)
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config,
		NewConcurrentStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	tsm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	data1 := getTestKVData()
	data2 := getTestKVData2()
	encoded1 := GetEncoded(ct, data1, make([]byte, 512))
	encoded2 := GetEncoded(ct, data2, make([]byte, 512))
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
	tsm.lastApplied.index = 235
	tsm.index = 235
	entries := []pb.Entry{e1, e2}
	batch := make([]sm.Entry, 0, 8)
	require.NoError(t, tsm.handleBatch(entries, batch))
	v, err := store.Lookup([]byte("test-key"))
	require.NoError(t, err)
	require.Equal(t, "test-value", string(v.([]byte)))
	v, err = store.Lookup([]byte("test-key-2"))
	require.NoError(t, err)
	require.Equal(t, "test-value-2", string(v.([]byte)))
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
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	sm.lastApplied.index = 1
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
		// not using testify here to avoid any allocation by such 3rd party lib
		if _, err := sm.Handle(batch, nil); err != nil {
			t.Fatalf("handle failed %v", err)
		}
		if sm.GetLastApplied() != idx {
			t.Errorf("last applied %d, want %d", sm.GetLastApplied(), idx)
		}
	})
	require.Equal(t, float64(0), ac)
}

func TestUpdatesNotBatchedWhenNotAllNoOPUpdates(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	store := &tests.ConcurrentUpdate{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config,
		&ConcurrentStateMachine{sm: store}, make(chan struct{}))
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
	sm.lastApplied.index = 234
	sm.index = 234
	sm.taskQ.Add(commit)
	batch := make([]Task, 0, 8)
	_, err := sm.Handle(batch, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(237), sm.GetLastApplied())
	reportLeakedFD(fs, t)
}

func TestStateMachineCanBeCreated(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		require.False(t, sm.TaskChanBusy())
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func applyConfigChangeEntry(sm *StateMachine,
	configChangeID uint64, configType pb.ConfigChangeType, ReplicaID uint64,
	addressToAdd string, index uint64) {
	cc := pb.ConfigChange{
		ConfigChangeId: configChangeID,
		Type:           configType,
		ReplicaID:      ReplicaID,
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
	sm.lastApplied.index = index - 1
	sm.index = index - 1
	sm.taskQ.Add(commit)
}

func TestLookupNotAllowedOnClosedShard(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.aborted = true
		v, err := sm.Lookup(make([]byte, 10))
		require.Equal(t, ErrShardClosed, err)
		require.Nil(t, v)
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembership(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			NonVotings: map[uint64]string{
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
		m := sm.GetMembership()
		require.Equal(t, uint64(12345), m.ConfigChangeId)
		require.Len(t, m.Addresses, 2)
		require.Len(t, m.NonVotings, 2)
		require.Len(t, m.Removed, 3)
		require.Len(t, m.Witnesses, 1)
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembershipNodes(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			NonVotings: map[uint64]string{
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
		m := sm.GetMembership()
		n := m.Addresses
		require.Len(t, n, 2)
		_, ok1 := n[100]
		_, ok2 := n[234]
		require.True(t, ok1)
		require.True(t, ok2)
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetMembershipHash(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		h1 := sm.GetMembershipHash()
		sm.members.members.Addresses[200] = "a200"
		h2 := sm.GetMembershipHash()
		require.NotEqual(t, h1, h2)
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetSSMetaPanicWhenThereIsNoMember(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		require.Panics(t, func() {
			_, err := sm.getSSMeta(nil, SSRequest{})
			require.NoError(t, err)
		})
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestGetSSMeta(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members.members = pb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			ConfigChangeId: 12345,
		}
		applySessionRegisterEntry(sm, 12345, 789)
		sm.lastApplied.index = 100
		sm.index = 100
		sm.term = 101
		sm.lastApplied.term = 101
		meta, err := sm.getSSMeta(make([]byte, 123), SSRequest{})
		require.NoError(t, err)
		require.Equal(t, uint64(100), meta.Index)
		require.Equal(t, uint64(101), meta.Term)
		v := meta.Ctx.([]byte)
		require.Len(t, v, 123)
		require.Len(t, meta.Membership.Addresses, 2)
		v = meta.Session.Bytes()
		require.NotEmpty(t, v)
	}
	fs := vfs.GetTestFS()
	runSMTest(t, tf, fs)
}

func TestHandleConfChangeAddNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applyConfigChangeEntry(sm, 1, pb.AddNode, 4, "localhost:1010", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.True(t, nodeProxy.accept)
		require.True(t, nodeProxy.addPeer)
		v, ok := sm.members.members.Addresses[4]
		require.True(t, ok)
		require.Equal(t, "localhost:1010", v)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddNodeAsNonVotingWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applyConfigChangeEntry(sm, 1, pb.AddNode, 4, "localhost:1010", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		applyConfigChangeEntry(sm,
			123,
			pb.AddNonVoting,
			4,
			"localhost:1010",
			124)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.True(t, nodeProxy.reject)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestNonVotingCanBeAdded(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applyConfigChangeEntry(sm,
			1,
			pb.AddNonVoting,
			4,
			"localhost:1010",
			123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.True(t, nodeProxy.accept)
		require.False(t, nodeProxy.addPeer)
		require.True(t, nodeProxy.addNonVoting)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestNonVotingPromotion(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applyConfigChangeEntry(sm, 1, pb.AddNonVoting, 4, "localhost:1010", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.True(t, nodeProxy.accept)
		require.False(t, nodeProxy.addPeer)
		require.True(t, nodeProxy.addNonVoting)
		applyConfigChangeEntry(sm, 123, pb.AddNode, 4, "localhost:1010", 124)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(124), sm.GetLastApplied())
		require.True(t, nodeProxy.addPeer)
		require.Len(t, sm.members.members.Addresses, 1)
		require.Empty(t, sm.members.members.NonVotings)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestInvalidNonVotingPromotionIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applyConfigChangeEntry(sm, 1, pb.AddNonVoting, 4, "localhost:1010", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.True(t, nodeProxy.accept)
		require.False(t, nodeProxy.addPeer)
		require.True(t, nodeProxy.addNonVoting)
		nodeProxy.accept = false
		applyConfigChangeEntry(sm, 123, pb.AddNode, 4, "localhost:1011", 124)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		_, ok := sm.members.members.Addresses[4]
		require.False(t, ok)
		require.False(t, nodeProxy.accept)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func testAddExistingMemberIsRejected(t *testing.T,
	tt pb.ConfigChangeType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[5] = "localhost:1010"
		applyConfigChangeEntry(sm, 1, tt, 4, "localhost:1010", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.False(t, nodeProxy.accept)
		require.False(t, nodeProxy.addPeer)
	}
	runSMTest2(t, tf, fs)
}

func TestAddExistingMemberIsRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	testAddExistingMemberIsRejected(t, pb.AddNode, fs)
	testAddExistingMemberIsRejected(t, pb.AddNonVoting, fs)
}

func testAddExistingMemberWithSameReplicaIDIsRejected(t *testing.T,
	tt pb.ConfigChangeType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		switch tt {
		case pb.AddNode:
			sm.members.members.Addresses[5] = "localhost:1010"
		case pb.AddNonVoting:
			sm.members.members.NonVotings[5] = "localhost:1010"
		default:
			require.Fail(t, "unknown tt")
		}
		applyConfigChangeEntry(sm, 1, tt, 5, "localhost:1011", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.False(t, nodeProxy.accept)
		require.False(t, nodeProxy.addPeer)
	}
	runSMTest2(t, tf, fs)
}

func TestAddExistingMemberWithSameReplicaIDIsRejected(t *testing.T) {
	fs := vfs.GetTestFS()
	testAddExistingMemberWithSameReplicaIDIsRejected(t, pb.AddNode, fs)
	testAddExistingMemberWithSameReplicaIDIsRejected(t, pb.AddNonVoting, fs)
}

func TestHandleConfChangeRemoveNode(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1010"
		sm.members.members.Addresses[2] = "localhost:1011"
		applyConfigChangeEntry(sm, 1, pb.RemoveNode, 1, "", 123)
		_, ok := sm.members.members.Addresses[1]
		require.True(t, ok)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.True(t, nodeProxy.accept)
		require.True(t, nodeProxy.removePeer)
		_, ok = sm.members.members.Addresses[1]
		require.False(t, ok)
		_, ok = sm.members.members.Removed[1]
		require.True(t, ok)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestOrderedConfChangeIsAccepted(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm, 6, pb.RemoveNode, 1, "", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.False(t, nodeProxy.reject)
		require.True(t, nodeProxy.accept)
		require.True(t, nodeProxy.removePeer)
		require.Equal(t, uint64(123), sm.members.members.ConfigChangeId)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRemoveOnlyNodeIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1010"
		applyConfigChangeEntry(sm, 1, pb.RemoveNode, 1, "", 123)
		_, ok := sm.members.members.Addresses[1]
		require.True(t, ok)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(123), sm.GetLastApplied())
		require.False(t, nodeProxy.accept)
		require.False(t, nodeProxy.removePeer)
		_, ok = sm.members.members.Addresses[1]
		require.True(t, ok)
		_, ok = sm.members.members.Removed[1]
		require.False(t, ok)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddingNodeOnTheSameNodeHostWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		sm.members.members.Addresses[100] = "test.nodehost"
		applyConfigChangeEntry(sm, 7, pb.AddNode, 2, "test.nodehost", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.True(t, nodeProxy.reject)
		require.False(t, nodeProxy.addPeer)
		require.NotEqual(t, uint64(123), sm.members.members.ConfigChangeId)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestAddingRemovedNodeWillBeRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.ConfigChangeId = 6
		sm.members.members.Removed[2] = true
		applyConfigChangeEntry(sm, 7, pb.AddNode, 2, "a1", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.True(t, nodeProxy.reject)
		require.False(t, nodeProxy.addPeer)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestOutOfOrderConfChangeIsRejected(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.ordered = true
		sm.members.members.ConfigChangeId = 6
		applyConfigChangeEntry(sm, 1, pb.RemoveNode, 1, "", 123)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.True(t, nodeProxy.reject)
		require.False(t, nodeProxy.removePeer)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestHandleSyncTask(t *testing.T) {
	tf := func(t *testing.T, tsm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		task1 := Task{
			PeriodicSync: true,
		}
		task2 := Task{
			PeriodicSync: true,
		}
		tsm.lastApplied.index = 100
		tsm.taskQ.Add(task1)
		tsm.taskQ.Add(task2)
		_, err := tsm.Handle(make([]Task, 0), make([]sm.Entry, 0))
		require.NoError(t, err)
		require.Equal(t, uint64(100), tsm.lastApplied.index)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestHandleEmptyEvent(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		e := pb.Entry{
			Type:  pb.ApplicationEntry,
			Index: 234,
			Term:  1,
		}
		commit := Task{
			Entries: []pb.Entry{e},
		}
		sm.lastApplied.index = 233
		sm.index = 233
		sm.taskQ.Add(commit)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(234), sm.GetLastApplied())
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
	data, err := u.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func testHandleSnappyEncodedEntry(t *testing.T,
	ct dio.CompressionType, fs vfs.IFS) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		data := getTestKVData()
		encoded := GetEncoded(ct, data, make([]byte, 512))
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
		sm.lastApplied.index = 235
		sm.index = 235
		sm.taskQ.Add(commit)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(236), sm.GetLastApplied())
		v, ok := store.(*tests.KVTest).KVStore["test-key"]
		require.True(t, ok)
		require.Equal(t, "test-value", v)
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
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
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
		sm.lastApplied.index = 234
		sm.index = 234
		sm.taskQ.Add(commit)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(236), sm.GetLastApplied())
		v, ok := store.(*tests.KVTest).KVStore["test-key"]
		require.True(t, ok)
		require.Equal(t, "test-value", v)
		result, err := sm.Lookup([]byte("test-key"))
		require.NoError(t, err)
		require.Equal(t, "test-value", string(result.([]byte)))
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestSnapshotCanBeApplied(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1234"
		store.(*tests.KVTest).KVStore["test-key1"] = "test-value1"
		store.(*tests.KVTest).KVStore["test-key2"] = "test-value2"
		sm.lastApplied.index = 3
		sm.index = 3
		hash1, err := sm.GetHash()
		require.NoError(t, err)
		ss, _, err := sm.Save(SSRequest{})
		require.NoError(t, err)
		index := ss.Index
		commit := Task{
			Index: index,
		}
		store2 := tests.NewKVTest(1, 1)
		config := config.Config{ShardID: 1, ReplicaID: 1}
		store2.(*tests.KVTest).DisableLargeDelay()
		ds2 := NewNativeSM(config, NewInMemStateMachine(store2), make(chan struct{}))
		nodeProxy2 := newTestNodeProxy()
		snapshotter2 := newTestSnapshotter(fs)
		snapshotter2.index = commit.Index
		sm2 := NewStateMachine(ds2, snapshotter2, config, nodeProxy2, fs)
		require.Empty(t, sm2.members.members.Addresses)
		ss2, err := sm2.Recover(commit)
		require.NoError(t, err)
		require.Equal(t, index, ss2.Index)
		hash2, err := sm2.GetHash()
		require.NoError(t, err)
		require.Equal(t, hash1, hash2)
		require.Len(t, sm2.members.members.Addresses, 2)
		v1, ok1 := sm2.members.members.Addresses[1]
		v2, ok2 := sm2.members.members.Addresses[2]
		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, "localhost:1", v1)
		require.Equal(t, "localhost:2", v2)
		require.Equal(t, uint64(2), nodeProxy2.addPeerCount)
	}
	runSMTest2(t, tf, fs)
}

func TestMembersAreSavedWhenMakingSnapshot(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1"
		sm.members.members.Addresses[2] = "localhost:2"
		ss, _, err := sm.Save(SSRequest{})
		require.NoError(t, err)
		cs := ss.Membership
		require.Len(t, cs.Addresses, 2)
		v1, ok1 := cs.Addresses[1]
		v2, ok2 := cs.Addresses[2]
		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, "localhost:1", v1)
		require.Equal(t, "localhost:2", v2)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestSnapshotTwiceIsHandled(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1"
		sm.members.members.Addresses[2] = "localhost:2"
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 1, 0, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		_, _, err = sm.Save(SSRequest{})
		require.NoError(t, err)
		_, _, err = sm.Save(SSRequest{})
		require.Equal(t, raft.ErrSnapshotOutOfDate, err)
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
	sm.lastApplied.index = index - 1
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
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		clientID := uint64(12345)
		applySessionRegisterEntry(sm, clientID, 789)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(789), sm.GetLastApplied())
		sessionManager := sm.sessions.lru
		_, ok := sessionManager.getSession(RaftClientID(clientID))
		require.True(t, ok)
		require.Equal(t, clientID, nodeProxy.smResult.Value)
		index := uint64(790)
		applySessionUnregisterEntry(sm, 12345, index)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, index, sm.GetLastApplied())
		_, ok = sessionManager.getSession(RaftClientID(clientID))
		require.False(t, ok)
		require.Equal(t, clientID, nodeProxy.smResult.Value)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestDuplicatedSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		e := applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		sessionManager := sm.sessions.lru
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		require.True(t, ok)
		require.Equal(t, e.ClientID, nodeProxy.smResult.Value)
		e.Index = 790
		commit := Task{
			Entries: []pb.Entry{e},
		}
		sm.taskQ.Add(commit)
		require.False(t, nodeProxy.rejected)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.Equal(t, uint64(0), nodeProxy.smResult.Value)
		require.True(t, nodeProxy.rejected)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRemovingUnregisteredSessionWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.lastApplied.index = 789
		sm.index = 789
		e := applySessionUnregisterEntry(sm, 12345, 790)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		sessionManager := sm.sessions.lru
		_, ok := sessionManager.getSession(RaftClientID(e.ClientID))
		require.False(t, ok)
		require.Equal(t, uint64(0), nodeProxy.smResult.Value)
		require.True(t, nodeProxy.rejected)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestUpdateFromUnregisteredClientWillBeReported(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		sm.lastApplied.index = 789
		sm.index = 789
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.False(t, nodeProxy.ignored)
		require.Equal(t, uint64(0), nodeProxy.smResult.Value)
		require.True(t, nodeProxy.rejected)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestDuplicatedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		plog.Infof("Handle returned")
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.True(t, nodeProxy.applyUpdateInvoked)
		require.Equal(t, uint64(len(data)), nodeProxy.smResult.Value)
		nodeProxy.applyUpdateInvoked = false
		storeCount := store.(*tests.KVTest).Count
		v := store.(*tests.KVTest).KVStore["test-key"]
		require.Equal(t, "test-value", v)
		e = applyTestEntry(sm, 12345, 1, 791, 0, data)
		plog.Infof("going to handle the second commit rec")
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.True(t, nodeProxy.applyUpdateInvoked)
		require.Equal(t, storeCount, store.(*tests.KVTest).Count)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestRespondedUpdateWillNotBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, 1, 790, 0, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		storeCount := store.(*tests.KVTest).Count
		e = applyTestEntry(sm, 12345, 1, 791, 1, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		sessionManager := sm.sessions.lru
		session, _ := sessionManager.getSession(RaftClientID(12345))
		require.Equal(t, RaftSeriesID(1), session.RespondedUpTo)
		nodeProxy.applyUpdateInvoked = false
		e = applyTestEntry(sm, 12345, 1, 792, 1, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.False(t, nodeProxy.applyUpdateInvoked)
		require.Equal(t, storeCount, store.(*tests.KVTest).Count)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestNoOPSessionAllowEntryToBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		applySessionRegisterEntry(sm, 12345, 789)
		batch := make([]Task, 0, 8)
		_, err := sm.Handle(batch, nil)
		require.NoError(t, err)
		data := getTestKVData()
		e := applyTestEntry(sm, 12345, client.NoOPSeriesID, 790, 0, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		storeCount := store.(*tests.KVTest).Count
		e = applyTestEntry(sm, 12345, client.NoOPSeriesID, 791, 0, data)
		_, err = sm.Handle(batch, nil)
		require.NoError(t, err)
		require.Equal(t, e.Index, sm.GetLastApplied())
		require.NotEqual(t, storeCount, store.(*tests.KVTest).Count)
	}
	fs := vfs.GetTestFS()
	runSMTest2(t, tf, fs)
}

func TestNoOPEntryIsNotBatched(t *testing.T) {
	updates, _ := getEntryTypes([]pb.Entry{{}})
	require.False(t, updates)
}

func TestRegularSessionedEntryIsNotBatched(t *testing.T) {
	e := pb.Entry{
		ClientID: client.NotSessionManagedClientID + 1,
		SeriesID: client.NoOPSeriesID + 1,
	}
	_, allNoOP := getEntryTypes([]pb.Entry{{}, e})
	require.False(t, allNoOP)
}

func TestNonUpdateEntryIsNotBatched(t *testing.T) {
	cce := pb.Entry{Type: pb.ConfigChangeEntry}
	notSessionManaged := pb.Entry{ClientID: client.NotSessionManagedClientID}
	newSessionEntry := pb.Entry{SeriesID: client.SeriesIDForRegister}
	unSessionEntry := pb.Entry{SeriesID: client.SeriesIDForUnregister}
	entries := []pb.Entry{cce, notSessionManaged, newSessionEntry, unSessionEntry}
	for _, e := range entries {
		require.False(t, e.IsUpdateEntry())
	}
	for _, e := range entries {
		updates, _ := getEntryTypes([]pb.Entry{e})
		require.False(t, updates)
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
	for _, tt := range tests {
		sm := StateMachine{onDiskSM: tt.onDiskSM,
			onDiskInitIndex: tt.onDiskInitIndex}
		result := sm.entryInInitDiskSM(tt.index)
		require.Equal(t, tt.result, result)
	}
}

func TestRecoverSMRequired2(t *testing.T) {
	tests := []struct {
		init          bool
		ssOnDiskIndex uint64
		onDiskIndex   uint64
		shouldPanic   bool
	}{
		{true, 100, 100, false},
		{true, 200, 100, true},
		{true, 300, 100, true},

		{true, 100, 200, false},
		{true, 200, 200, false},
		{true, 300, 200, true},

		{true, 100, 300, false},
		{true, 200, 300, false},
		{true, 300, 300, false},

		{false, 100, 100, false},
		{false, 200, 100, true},
		{false, 300, 100, true},

		{false, 100, 200, false},
		{false, 200, 200, false},
		{false, 300, 200, true},

		{false, 100, 300, false},
		{false, 200, 300, false},
		{false, 300, 300, false},
	}
	ssIndex := uint64(200)
	node := newTestNodeProxy()
	for _, tt := range tests {
		sm := &StateMachine{
			onDiskSM: true,
			node:     node,
		}
		if tt.init {
			sm.onDiskInitIndex = tt.onDiskIndex
		} else {
			sm.onDiskIndex = tt.onDiskIndex
		}
		ss := pb.Snapshot{
			Index:       ssIndex,
			OnDiskIndex: tt.ssOnDiskIndex,
		}
		f := func() {
			sm.checkPartialSnapshotApplyOnDiskSM(ss, tt.init)
		}
		if tt.shouldPanic {
			require.Panics(t, f)
		} else {
			require.NotPanics(t, f)
		}
	}
}

func TestRecoverSMRequired(t *testing.T) {
	tests := []struct {
		witness         bool
		dummy           bool
		init            bool
		onDiskIndex     uint64
		onDiskInitIndex uint64
		required        bool
	}{
		{false, false, true, 100, 100, false},
		{false, false, true, 200, 100, true},
		{false, false, true, 100, 200, false},
		{false, false, false, 100, 100, false},
		{false, false, false, 200, 100, true},
		{false, false, false, 100, 200, false},

		{false, true, true, 100, 100, false},
		{false, true, true, 200, 100, false},
		{false, true, true, 100, 200, false},
		{false, true, false, 100, 100, false},
		{false, true, false, 200, 100, false},
		{false, true, false, 100, 200, false},

		{true, false, true, 100, 100, false},
		{true, false, true, 200, 100, false},
		{true, false, true, 100, 200, false},
		{true, false, false, 100, 100, false},
		{true, false, false, 200, 100, false},
		{true, false, false, 100, 200, false},

		{true, true, true, 100, 100, false},
		{true, true, true, 200, 100, false},
		{true, true, true, 100, 200, false},
		{true, true, false, 100, 100, false},
		{true, true, false, 200, 100, false},
		{true, true, false, 100, 200, false},
	}
	ssIndex := uint64(200)
	for _, tt := range tests {
		sm := &StateMachine{
			onDiskSM:        true,
			onDiskInitIndex: tt.onDiskInitIndex,
			onDiskIndex:     tt.onDiskInitIndex,
		}
		ss := pb.Snapshot{
			Index:       ssIndex,
			OnDiskIndex: tt.onDiskIndex,
			Witness:     tt.witness,
			Dummy:       tt.dummy,
		}
		f := func() {
			require.Equal(t, tt.required, sm.recoverRequired(ss, tt.init))
		}
		if tt.dummy || tt.witness {
			require.Panics(t, f)
		} else {
			require.NotPanics(t, f)
		}
	}
}

func TestIsShrunkSnapshot(t *testing.T) {
	tests := []struct {
		shrunk   bool
		init     bool
		isShrunk bool
	}{
		{false, false, false},
		{false, true, false},

		{true, false, false},
		{true, true, true},
	}
	ssIndex := uint64(200)
	for _, tt := range tests {
		fs := vfs.GetTestFS()
		defer func() {
			require.NoError(t, fs.RemoveAll(testSnapshotterDir))
		}()
		func() {
			require.NoError(t, fs.RemoveAll(testSnapshotterDir))
			require.NoError(t, fs.MkdirAll(testSnapshotterDir, 0755))
			snapshotter := newTestSnapshotter(fs)
			sm := &StateMachine{
				snapshotter: snapshotter,
				onDiskSM:    true,
				fs:          fs,
			}
			fp := snapshotter.getFilePath(ssIndex)
			if tt.shrunk {
				fp = fp + ".tmp"
			}
			w, err := NewSnapshotWriter(fp, pb.NoCompression, fs)
			require.NoError(t, err)
			sessionData := make([]byte, testSessionSize)
			storeData := make([]byte, testPayloadSize)
			_, err = rand.Read(sessionData)
			require.NoError(t, err)
			_, err = rand.Read(storeData)
			require.NoError(t, err)
			n, err := w.Write(sessionData)
			require.NoError(t, err)
			require.Equal(t, len(sessionData), n)
			m, err := w.Write(storeData)
			require.NoError(t, err)
			require.Equal(t, len(storeData), m)
			require.NoError(t, w.Close())
			if tt.shrunk {
				err := ShrinkSnapshot(fp, snapshotter.getFilePath(ssIndex), fs)
				require.NoError(t, err)
			}
			ss := pb.Snapshot{
				Index: ssIndex,
			}
			f := func() {
				res, err := sm.isShrunkSnapshot(ss, tt.init)
				require.NoError(t, err)
				require.Equal(t, tt.isShrunk, res)
			}
			if !tt.init && tt.shrunk {
				require.Panics(t, f)
			} else {
				require.NotPanics(t, f)
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
	for _, tt := range tests {
		sm := &StateMachine{
			onDiskSM:        tt.onDisk,
			onDiskInitIndex: tt.onDiskInitIndex,
		}
		sm.lastApplied.index = tt.index
		require.Equal(t, tt.ready, sm.ReadyToStream())
	}
}

func TestAlreadyAppliedInOnDiskSMEntryTreatedAsNoOP(t *testing.T) {
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 100,
		index:           90,
	}
	sm.lastApplied.index = 90
	sm.lastApplied.term = 5
	ent := pb.Entry{
		ClientID: 100,
		Index:    91,
		Term:     6,
	}
	require.NoError(t, sm.handleEntry(ent, false))
}

type testManagedStateMachine struct {
	first          uint64
	last           uint64
	synced         bool
	nalookup       bool
	corruptIndex   bool
	concurrent     bool
	onDisk         bool
	smType         pb.StateMachineType
	prepareInvoked bool
}

func (t *testManagedStateMachine) Open() (uint64, error) { return 10, nil }
func (t *testManagedStateMachine) Update(sm.Entry) (sm.Result, error) {
	return sm.Result{}, nil
}
func (t *testManagedStateMachine) Lookup(interface{}) (interface{}, error) {
	return nil, nil
}
func (t *testManagedStateMachine) NALookup(input []byte) ([]byte, error) {
	t.nalookup = true
	return input, nil
}
func (t *testManagedStateMachine) ConcurrentLookup(interface{}) (interface{},
	error) {
	return nil, nil
}
func (t *testManagedStateMachine) NAConcurrentLookup(input []byte) ([]byte,
	error) {
	t.nalookup = true
	return input, nil
}
func (t *testManagedStateMachine) Sync() error {
	t.synced = true
	return nil
}
func (t *testManagedStateMachine) GetHash() (uint64, error) { return 0, nil }
func (t *testManagedStateMachine) Prepare() (interface{}, error) {
	t.prepareInvoked = true
	return nil, nil
}
func (t *testManagedStateMachine) Save(SSMeta,
	io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error) {
	return false, nil
}
func (t *testManagedStateMachine) Recover(io.Reader,
	[]sm.SnapshotFile) error {
	return nil
}
func (t *testManagedStateMachine) Stream(interface{}, io.Writer) error {
	return nil
}
func (t *testManagedStateMachine) Offloaded() bool             { return false }
func (t *testManagedStateMachine) Loaded()                     {}
func (t *testManagedStateMachine) Close() error                { return nil }
func (t *testManagedStateMachine) DestroyedC() <-chan struct{} { return nil }
func (t *testManagedStateMachine) Concurrent() bool {
	return t.concurrent
}
func (t *testManagedStateMachine) OnDisk() bool { return t.onDisk }
func (t *testManagedStateMachine) Type() pb.StateMachineType {
	return t.smType
}
func (t *testManagedStateMachine) BatchedUpdate(ents []sm.Entry) ([]sm.Entry,
	error) {
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
	require.NoError(t, err)
	require.Equal(t, uint64(10), index)
	require.Equal(t, uint64(10), sm.onDiskInitIndex)
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
		snapshotter: newTestSnapshotter(fs),
		sessions:    NewSessionManager(),
		members:     newMembership(1, 1, false),
		fs:          fs,
	}
	sm.lastApplied.index = 100
	sm.lastApplied.term = 5
	sm.members.members.Addresses[1] = "a1"
	ss, _, err := sm.concurrentSave(SSRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), ss.Index)
	require.True(t, msm.synced)
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
		snapshotter: newTestSnapshotter(fs),
		sessions:    NewSessionManager(),
		members:     newMembership(1, 1, false),
		fs:          fs,
	}
	sm.lastApplied.index = 100
	sm.lastApplied.term = 5
	sm.members.members.Addresses[1] = "a1"
	ts := &testSink{
		chunks: make([]pb.Chunk, 0),
	}
	require.NoError(t, sm.Stream(ts))
	require.Len(t, ts.chunks, 3)
	require.True(t, ts.chunks[1].IsLastChunk())
	require.True(t, ts.chunks[2].IsPoisonChunk())
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
			sm:              msm,
			node:            np,
		}
		sm.lastApplied.index = tt.index
		sm.lastApplied.term = 100
		require.NoError(t, sm.handleBatch(input, ents))
		require.Equal(t, tt.firstApplied, msm.first, "idx %d", idx)
		require.Equal(t, tt.lastApplied, msm.last, "idx %d", idx)
		require.Equal(t, tt.firstApplied, np.firstIndex)
		require.Equal(t, tt.lastApplied, np.index, "idx %d", idx)
		require.Equal(t, tt.index, sm.GetLastApplied(), "idx %d", idx)
	}
}

func TestCorruptedIndexValueWillBeDetected(t *testing.T) {
	ents := make([]sm.Entry, 0)
	msm := &testManagedStateMachine{corruptIndex: true}
	np := newTestNodeProxy()
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 0,
		sm:              msm,
		node:            np,
	}
	sm.lastApplied.index = 0
	sm.lastApplied.term = 100
	input := make([]pb.Entry, 0)
	for i := uint64(1); i <= 10; i++ {
		input = append(input, pb.Entry{Index: i, Term: 100})
	}
	require.Panics(t, func() {
		_ = sm.handleBatch(input, ents)
	})
}

func TestNodeReadyIsSetWhenAnythingFromTaskQIsProcessed(t *testing.T) {
	ents := make([]sm.Entry, 0)
	batch := make([]Task, 0)
	msm := &testManagedStateMachine{}
	np := newTestNodeProxy()
	sm := &StateMachine{
		onDiskSM:        true,
		onDiskInitIndex: 0,
		sm:              msm,
		taskQ:           NewTaskQueue(),
		node:            np,
	}
	rec, err := sm.Handle(batch, ents)
	require.False(t, rec.IsSnapshotTask())
	require.NoError(t, err)
	require.Equal(t, uint64(0), np.nodeReady)
	sm.taskQ.Add(Task{})
	rec, err = sm.Handle(batch, ents)
	require.False(t, rec.IsSnapshotTask())
	require.NoError(t, err)
	require.Equal(t, uint64(1), np.nodeReady)
}

func TestSyncedIndex(t *testing.T) {
	sm := &StateMachine{}
	sm.setSyncedIndex(100)
	require.Equal(t, uint64(100), sm.GetSyncedIndex())
	sm.setSyncedIndex(100)
	require.Equal(t, uint64(100), sm.GetSyncedIndex())
	require.Panics(t, func() {
		sm.setSyncedIndex(99)
	})
}

func TestNALookup(t *testing.T) {
	msm := &testManagedStateMachine{}
	sm := &StateMachine{
		sm: msm,
	}
	input := make([]byte, 128)
	_, err := rand.Read(input)
	require.NoError(t, err)
	result, err := sm.NALookup(input)
	require.NoError(t, err)
	require.True(t, bytes.Equal(input, result))
	require.True(t, msm.nalookup)
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
		r := SSRequest{Type: Periodic}
		if tt.exported {
			r.Type = Exported
		}
		if tt.streaming {
			r.Type = Streaming
		}
		require.Equal(t, tt.dummy, s.isDummySnapshot(r), "idx %d", idx)
	}
}

func TestWitnessNodePanicWhenSavingSnapshot(t *testing.T) {
	sm := &StateMachine{isWitness: true}
	require.Panics(t, func() {
		_, _, err := sm.Save(SSRequest{})
		require.NoError(t, err)
	})
}

func TestSetLastApplied(t *testing.T) {
	tests := []struct {
		index   uint64
		term    uint64
		entries []pb.Entry
		crash   bool
	}{
		{100, 5, []pb.Entry{{Index: 0, Term: 1}}, true},
		{100, 5, []pb.Entry{{Index: 1, Term: 0}}, true},
		{100, 5, []pb.Entry{{Index: 102, Term: 5}}, true},
		{100, 5, []pb.Entry{{Index: 101, Term: 4}}, true},
		{100, 5, []pb.Entry{{Index: 101, Term: 5}, {Index: 103, Term: 5}}, true},
		{100, 5, []pb.Entry{{Index: 101, Term: 6}, {Index: 102, Term: 5}}, true},
		{100, 5, []pb.Entry{{Index: 101, Term: 5}, {Index: 102, Term: 6}}, false},
	}
	for idx, tt := range tests {
		sm := StateMachine{}
		sm.lastApplied.index = tt.index
		sm.lastApplied.term = tt.term
		f := func() {
			sm.setLastApplied(tt.entries)
			require.Equal(t, tt.entries[len(tt.entries)-1].Index, sm.lastApplied.index)
			require.Equal(t, tt.entries[len(tt.entries)-1].Term, sm.lastApplied.term)
		}
		if tt.crash {
			require.Panics(t, f, "idx %d", idx)
		} else {
			require.NotPanics(t, f, "idx %d", idx)
		}
	}
}

func TestSavingDummySnapshot(t *testing.T) {
	tests := []struct {
		smType    pb.StateMachineType
		streaming bool
		export    bool
		result    bool
	}{
		{pb.RegularStateMachine, true, false, false},
		{pb.RegularStateMachine, false, true, false},
		{pb.RegularStateMachine, false, false, false},
		{pb.ConcurrentStateMachine, true, false, false},
		{pb.ConcurrentStateMachine, false, true, false},
		{pb.ConcurrentStateMachine, false, false, false},
		{pb.OnDiskStateMachine, true, false, false},
		{pb.OnDiskStateMachine, false, true, false},
		{pb.OnDiskStateMachine, false, false, true},
	}
	for idx, tt := range tests {
		sm := StateMachine{
			onDiskSM: tt.smType == pb.OnDiskStateMachine,
		}
		var rt SSReqType
		require.False(t, tt.export && tt.streaming)
		if tt.export {
			rt = Exported
		} else if tt.streaming {
			rt = Streaming
		}
		r := sm.savingDummySnapshot(SSRequest{Type: rt})
		require.Equal(t, tt.result, r, "idx %d", idx)
	}
}

func TestPrepareIsNotCalledWhenSavingDummySnapshot(t *testing.T) {
	tests := []struct {
		onDiskSM       bool
		streaming      bool
		export         bool
		prepareInvoked bool
	}{
		{true, false, false, false},
		{true, true, false, true},
		{true, false, true, true},
		{false, false, false, true},
		{false, false, true, true},
	}

	for idx, tt := range tests {
		msm := &testManagedStateMachine{
			concurrent: true,
			onDisk:     tt.onDiskSM,
			smType:     pb.ConcurrentStateMachine,
		}
		if tt.onDiskSM {
			msm.smType = pb.OnDiskStateMachine
		}
		m := membership{
			members: pb.Membership{
				Addresses: map[uint64]string{1: "localhost:1234"},
			},
		}
		sm := StateMachine{
			index:    100,
			onDiskSM: tt.onDiskSM,
			sm:       msm,
			members:  m,
			node:     &testNodeProxy{},
			sessions: NewSessionManager(),
		}
		var rt SSReqType
		require.False(t, tt.export && tt.streaming)
		if tt.export {
			rt = Exported
		} else if tt.streaming {
			rt = Streaming
		}
		meta, err := sm.prepare(SSRequest{Type: rt})
		require.NoError(t, err)
		require.Equal(t, uint64(100), meta.Index)
		require.Equal(t, tt.prepareInvoked, msm.prepareInvoked, "idx %d", idx)
	}
}

var errReturnedError = errors.New("test error")

func expectedError(err error) bool {
	return errors.Is(err, errReturnedError) && tests.HasStack(err)
}

type errorUpdateSM struct{}

func (e *errorUpdateSM) Update(i sm.Entry) (sm.Result, error) {
	return sm.Result{}, errReturnedError
}
func (e *errorUpdateSM) Lookup(q interface{}) (interface{}, error) {
	return nil, nil
}
func (e *errorUpdateSM) SaveSnapshot(io.Writer,
	sm.ISnapshotFileCollection, <-chan struct{}) error {
	return errReturnedError
}
func (e *errorUpdateSM) RecoverFromSnapshot(io.Reader,
	[]sm.SnapshotFile, <-chan struct{}) error {
	return errReturnedError
}
func (e *errorUpdateSM) Close() error { return nil }

func TestUpdateErrorIsReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	defer reportLeakedFD(fs, t)
	store := &errorUpdateSM{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	e1 := pb.Entry{
		ClientID: 123,
		SeriesID: client.NoOPSeriesID,
		Index:    235,
		Term:     1,
	}
	commit := Task{
		Entries: []pb.Entry{e1},
	}
	sm.lastApplied.index = 234
	sm.index = 234
	sm.taskQ.Add(commit)
	batch := make([]Task, 0, 8)
	_, err := sm.Handle(batch, nil)
	require.True(t, expectedError(err))
}

type errorNodeProxy struct{}

func (e *errorNodeProxy) StepReady() {}
func (e *errorNodeProxy) RestoreRemotes(pb.Snapshot) error {
	return errReturnedError
}
func (e *errorNodeProxy) ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool) {}
func (e *errorNodeProxy) ApplyConfigChange(pb.ConfigChange, uint64,
	bool) error {
	return errReturnedError
}
func (e *errorNodeProxy) ReplicaID() uint64           { return 1 }
func (e *errorNodeProxy) ShardID() uint64             { return 1 }
func (e *errorNodeProxy) ShouldStop() <-chan struct{} { return make(chan struct{}) }

func TestConfigChangeErrorIsReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	defer reportLeakedFD(fs, t)
	store := &errorUpdateSM{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
	nodeProxy := &errorNodeProxy{}
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	cc := pb.ConfigChange{
		ConfigChangeId: 1,
		Type:           pb.AddNode,
		ReplicaID:      2,
		Address:        "localhost:1222",
	}
	data, err := cc.Marshal()
	require.NoError(t, err)
	e := pb.Entry{
		Cmd:   data,
		Type:  pb.ConfigChangeEntry,
		Index: 235,
		Term:  1,
	}
	commit := Task{
		Entries: []pb.Entry{e},
	}
	sm.lastApplied.index = 234
	sm.index = 234
	sm.taskQ.Add(commit)
	batch := make([]Task, 0, 8)
	_, err = sm.Handle(batch, nil)
	require.True(t, expectedError(err))
}

func TestSaveErrorIsReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	createTestDir(fs)
	defer removeTestDir(fs)
	defer reportLeakedFD(fs, t)
	store := &errorUpdateSM{}
	config := config.Config{ShardID: 1, ReplicaID: 1}
	ds := NewNativeSM(config, NewInMemStateMachine(store), make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	snapshotter := newTestSnapshotter(fs)
	sm := NewStateMachine(ds, snapshotter, config, nodeProxy, fs)
	sm.members.members.Addresses[1] = "localhost:1234"
	sm.lastApplied.index = 234
	sm.index = 234
	_, _, err := sm.Save(SSRequest{})
	require.True(t, expectedError(err))
}

func TestRecoverErrorIsReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1234"
		sm.lastApplied.index = 3
		sm.index = 3
		ss, _, err := sm.Save(SSRequest{})
		require.NoError(t, err)
		index := ss.Index
		commit := Task{
			Index: index,
		}
		store2 := &errorUpdateSM{}
		config := config.Config{ShardID: 1, ReplicaID: 1}
		ds2 := NewNativeSM(config, NewInMemStateMachine(store2), make(chan struct{}))
		nodeProxy2 := newTestNodeProxy()
		snapshotter2 := newTestSnapshotter(fs)
		snapshotter2.index = commit.Index
		sm2 := NewStateMachine(ds2, snapshotter2, config, nodeProxy2, fs)
		_, err = sm2.Recover(commit)
		require.True(t, expectedError(err))
	}
	runSMTest2(t, tf, fs)
}

func TestRestoreRemoteErrorIsReturned(t *testing.T) {
	fs := vfs.GetTestFS()
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, snapshotter *testSnapshotter,
		store sm.IStateMachine) {
		sm.members.members.Addresses[1] = "localhost:1234"
		sm.lastApplied.index = 3
		sm.index = 3
		ss, _, err := sm.Save(SSRequest{})
		require.NoError(t, err)
		index := ss.Index
		commit := Task{
			Index: index,
		}
		store2 := tests.NewKVTest(1, 1)
		store2.(*tests.KVTest).DisableLargeDelay()
		config := config.Config{ShardID: 1, ReplicaID: 1}
		ds2 := NewNativeSM(config, NewInMemStateMachine(store2), make(chan struct{}))
		nodeProxy2 := &errorNodeProxy{}
		snapshotter2 := newTestSnapshotter(fs)
		snapshotter2.index = commit.Index
		sm2 := NewStateMachine(ds2, snapshotter2, config, nodeProxy2, fs)
		_, err = sm2.Recover(commit)
		require.True(t, expectedError(err))
	}
	runSMTest2(t, tf, fs)
}
