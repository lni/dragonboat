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

/*
Package rsm implements Replicated State Machines used in Dragonboat.

This package is internally used by Dragonboat, applications are not expected to
import this package.
*/
package rsm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/logger"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/logutil"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrTestKnobReturn is the error returned when returned earlier due to test
	// knob.
	ErrTestKnobReturn = errors.New("returned earlier due to test knob")
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot             = errors.New("failed to restore from snapshot")
	batchedEntryApply              = settings.Soft.BatchedEntryApply
	sessionBufferInitialCap uint64 = 128 * 1024
)

// SSReqType is the type of a snapshot request.
type SSReqType uint64

const (
	// PeriodicSnapshot is the value to indicate periodic snapshot.
	PeriodicSnapshot SSReqType = iota
	// UserRequestedSnapshot is the value to indicate user requested snapshot.
	UserRequestedSnapshot
	// ExportedSnapshot is the value to indicate exported snapshot.
	ExportedSnapshot
	// StreamSnapshot is the value to indicate snapshot streaming.
	StreamSnapshot
)

// SSRequest is the type for describing the details of a snapshot request.
type SSRequest struct {
	Type               SSReqType
	Key                uint64
	Path               string
	OverrideCompaction bool
	CompactionOverhead uint64
}

// IsExportedSnapshot returns a boolean value indicating whether the snapshot
// request is to create an exported snapshot.
func (r *SSRequest) IsExportedSnapshot() bool {
	return r.Type == ExportedSnapshot
}

// IsStreamingSnapshot returns a boolean value indicating whether the snapshot
// request is to stream snapshot.
func (r *SSRequest) IsStreamingSnapshot() bool {
	return r.Type == StreamSnapshot
}

// SSMeta is the metadata of a snapshot.
type SSMeta struct {
	From            uint64
	Index           uint64
	Term            uint64
	OnDiskIndex     uint64 // applied index of IOnDiskStateMachine
	Request         SSRequest
	Membership      pb.Membership
	Type            pb.StateMachineType
	Session         *bytes.Buffer
	Ctx             interface{}
	CompressionType config.CompressionType
}

// Task describes a task that need to be handled by StateMachine.
type Task struct {
	ClusterID         uint64
	NodeID            uint64
	Index             uint64
	SnapshotAvailable bool
	InitialSnapshot   bool
	SnapshotRequested bool
	StreamSnapshot    bool
	PeriodicSync      bool
	NewNode           bool
	SSRequest         SSRequest
	Entries           []pb.Entry
}

// IsSnapshotTask returns a boolean flag indicating whether it is a snapshot
// task.
func (t *Task) IsSnapshotTask() bool {
	return t.SnapshotAvailable || t.SnapshotRequested || t.StreamSnapshot
}

func (t *Task) isSyncTask() bool {
	if t.PeriodicSync && t.IsSnapshotTask() {
		panic("invalid task")
	}
	return t.PeriodicSync
}

// SMFactoryFunc is the function type for creating an IStateMachine instance
type SMFactoryFunc func(clusterID uint64,
	nodeID uint64, done <-chan struct{}) IManagedStateMachine

// INodeProxy is the interface used as proxy to a nodehost.
type INodeProxy interface {
	NodeReady()
	RestoreRemotes(pb.Snapshot)
	ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool)
	ApplyConfigChange(pb.ConfigChange)
	ConfigChangeProcessed(uint64, bool)
	NodeID() uint64
	ClusterID() uint64
	ShouldStop() <-chan struct{}
}

// ISnapshotter is the interface for the snapshotter object.
type ISnapshotter interface {
	GetSnapshot(uint64) (pb.Snapshot, error)
	GetMostRecentSnapshot() (pb.Snapshot, error)
	GetFilePath(uint64) string
	Stream(IStreamable, *SSMeta, pb.IChunkSink) error
	Save(ISavable, *SSMeta) (*pb.Snapshot, *server.SSEnv, error)
	Load(ILoadableSessions, ILoadableSM, string, []sm.SnapshotFile) error
	IsNoSnapshotError(error) bool
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	mu              sync.RWMutex
	snapshotter     ISnapshotter
	node            INodeProxy
	sm              IManagedStateMachine
	sessions        *SessionManager
	members         *membership
	index           uint64
	term            uint64
	snapshotIndex   uint64
	onDiskInitIndex uint64
	onDiskIndex     uint64
	taskQ           *TaskQueue
	onDiskSM        bool
	aborted         bool
	sct             config.CompressionType
	syncedIndex     struct {
		sync.Mutex
		index uint64
	}
	batchedIndex struct {
		sync.Mutex
		index uint64
	}
}

// NewStateMachine creates a new application state machine object.
func NewStateMachine(sm IManagedStateMachine,
	snapshotter ISnapshotter, cfg config.Config, proxy INodeProxy) *StateMachine {
	ordered := cfg.OrderedConfigChange
	ct := cfg.SnapshotCompressionType
	a := &StateMachine{
		snapshotter: snapshotter,
		sm:          sm,
		onDiskSM:    sm.OnDiskStateMachine(),
		taskQ:       NewTaskQueue(),
		node:        proxy,
		sessions:    NewSessionManager(),
		members:     newMembership(proxy.ClusterID(), proxy.NodeID(), ordered),
		sct:         ct,
	}
	return a
}

// TaskQ returns the task queue.
func (s *StateMachine) TaskQ() *TaskQueue {
	return s.taskQ
}

// TaskChanBusy returns whether the TaskC chan is busy. Busy is defined as
// having more than half of its buffer occupied.
func (s *StateMachine) TaskChanBusy() bool {
	sz := s.taskQ.Size()
	return sz*2 > taskQueueBusyCap
}

// RecoverFromSnapshot applies the snapshot.
func (s *StateMachine) RecoverFromSnapshot(t Task) (uint64, error) {
	ss, err := s.getSnapshot(t)
	if err != nil {
		return 0, err
	}
	if pb.IsEmptySnapshot(ss) {
		return 0, nil
	}
	ss.Validate()
	plog.Infof("%s called RecoverFromSnapshot, idx %d, on disk idx %d",
		s.id(), ss.Index, ss.OnDiskIndex)
	if idx, err := s.recoverFromSnapshot(ss, t.InitialSnapshot); err != nil {
		return idx, err
	}
	s.node.RestoreRemotes(ss)
	s.setBatchedLastApplied(ss.Index)
	plog.Infof("%s snapshot %d restored, members %v",
		s.id(), ss.Index, ss.Membership.Addresses)
	return ss.Index, nil
}

func (s *StateMachine) getSnapshot(t Task) (pb.Snapshot, error) {
	if !t.InitialSnapshot {
		snapshot, err := s.snapshotter.GetSnapshot(t.Index)
		if err != nil && !s.snapshotter.IsNoSnapshotError(err) {
			plog.Errorf("%s, get snapshot failed: %v", s.id(), err)
			return pb.Snapshot{}, ErrRestoreSnapshot
		}
		if s.snapshotter.IsNoSnapshotError(err) {
			plog.Errorf("%s, no snapshot", s.id())
			return pb.Snapshot{}, err
		}
		return snapshot, nil
	}
	snapshot, err := s.snapshotter.GetMostRecentSnapshot()
	if s.snapshotter.IsNoSnapshotError(err) {
		plog.Infof("%s no snapshot available during node launch", s.id())
		return pb.Snapshot{}, nil
	}
	return snapshot, nil
}

func (s *StateMachine) recoverSMRequiredForOnDiskMachine(ss pb.Snapshot, init bool) bool {
	if !s.OnDiskStateMachine() {
		plog.Panicf("non-disk based state machine shouldn't attempt this check")
	}
	if ss.Dummy {
		return false
	}
	// just a self test to see whether it is trying to recover from a shrunk
	// snapshot
	fn := s.snapshotter.GetFilePath(ss.Index)
	shrunk, err := IsShrinkedSnapshotFile(fn)
	if err != nil {
		panic(err)
	}
	if !init && shrunk {
		panic("not initial recovery but snapshot shrunk")
	}

	if init {
		if shrunk {
			return false
		}
		if ss.Imported {
			return true
		} else {
			if ss.OnDiskIndex <= s.onDiskInitIndex {
				plog.Panicf("%s, ss.OnDiskIndex (%d) <= s.onDiskInitIndex (%d)",
					s.id(), ss.OnDiskIndex, s.onDiskInitIndex)
			}
			if ss.OnDiskIndex <= s.onDiskIndex {
				plog.Panicf("%s, ss.OnDiskInit (%d) <= s.onDiskIndex (%d)",
					s.id(), ss.OnDiskIndex, s.onDiskIndex)
			}
		}
		return ss.OnDiskIndex > s.onDiskInitIndex
	}
	return ss.OnDiskIndex > s.onDiskIndex
}

func (s *StateMachine) recoverFromSnapshot(ss pb.Snapshot,
	init bool) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := ss.Index
	if s.index >= index {
		return s.index, raft.ErrSnapshotOutOfDate
	}
	if s.aborted {
		return 0, sm.ErrSnapshotStopped
	}

	if !s.OnDiskStateMachine() {
		return s.recoverStateMachine(ss, init, index)
	} else if s.recoverSMRequiredForOnDiskMachine(ss, init) {
		if index, err := s.recoverStateMachine(ss, init, index); err != nil {
			return index, err
		}

		s.onDiskIndex = ss.OnDiskIndex
		if ss.Imported && init {
			s.onDiskInitIndex = ss.OnDiskIndex
		}
		return 0, nil
	} else {
		plog.Infof("%s is on disk SM, %d vs %d, SM not restored",
			s.id(), index, s.onDiskInitIndex)
		s.applySnapshotMetadata(ss)
		return 0, nil
	}
}

func (s *StateMachine) recoverStateMachine(ss pb.Snapshot, init bool, ssIndex uint64) (uint64, error) {
	plog.Infof("%s recovering from snapshot, term %d, index %d, %s, init %t",
		s.id(), ss.Term, ssIndex, snapshotInfo(ss), init)
	fs := getSnapshotFiles(ss)
	fn := s.snapshotter.GetFilePath(ssIndex)
	if err := s.snapshotter.Load(s.sessions, s.sm, fn, fs); err != nil {
		plog.Errorf("failed to load snapshot, %s, %v", s.id(), err)
		if err == sm.ErrSnapshotStopped {
			// no more lookup allowed
			s.aborted = true
			return 0, err
		}
		return 0, ErrRestoreSnapshot
	}

	s.applySnapshotMetadata(ss)
	return 0, nil
}

func (s *StateMachine) applySnapshotMetadata(ss pb.Snapshot) {
	s.index = ss.Index
	s.term = ss.Term
	s.members.set(ss.Membership)
}

// ReadyToStreamSnapshot returns a boolean flag to indicate whether the state
// machine is ready to stream snapshot. It can not stream a full snapshot when
// membership state is catching up with the all disk SM state. however, meta
// only snapshot can be taken at any time.
func (s *StateMachine) ReadyToStreamSnapshot() bool {
	return !s.OnDiskStateMachine() || s.GetLastApplied() >= s.onDiskInitIndex
}

// OpenOnDiskStateMachine opens the on disk state machine.
func (s *StateMachine) OpenOnDiskStateMachine() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index, err := s.sm.Open()
	if err != nil {
		plog.Errorf("%s failed to open on disk SM", s.id())
		if err == sm.ErrOpenStopped {
			s.aborted = true
		}
		return 0, err
	}
	plog.Infof("%s opened disk SM, index %d", s.id(), index)
	s.onDiskInitIndex = index
	s.onDiskIndex = index
	return index, nil
}

// GetLastApplied returns the last applied value.
func (s *StateMachine) GetLastApplied() uint64 {
	s.mu.RLock()
	v := s.index
	s.mu.RUnlock()
	return v
}

// GetBatchedLastApplied returns the batched last applied value.
func (s *StateMachine) GetBatchedLastApplied() uint64 {
	s.batchedIndex.Lock()
	v := s.batchedIndex.index
	s.batchedIndex.Unlock()
	return v
}

// GetSyncedIndex returns the index value that is known to have been
// synchronized.
func (s *StateMachine) GetSyncedIndex() uint64 {
	s.syncedIndex.Lock()
	defer s.syncedIndex.Unlock()
	return s.syncedIndex.index
}

// SetBatchedLastApplied sets the batched last applied value. This method
// is mostly used in tests.
func (s *StateMachine) SetBatchedLastApplied(index uint64) {
	s.setBatchedLastApplied(index)
}

func (s *StateMachine) setSyncedIndex(index uint64) {
	s.syncedIndex.Lock()
	defer s.syncedIndex.Unlock()
	if s.syncedIndex.index > index {
		panic("s.syncedIndex.index > index")
	}
	s.syncedIndex.index = index
}

func (s *StateMachine) setBatchedLastApplied(index uint64) {
	s.batchedIndex.Lock()
	s.batchedIndex.index = index
	s.batchedIndex.Unlock()
}

// Offloaded marks the state machine as offloaded from the specified component.
func (s *StateMachine) Offloaded(from From) {
	s.sm.Offloaded(from)
}

// Loaded marks the state machine as loaded from the specified component.
func (s *StateMachine) Loaded(from From) {
	s.sm.Loaded(from)
}

// Lookup queries the local state machine.
func (s *StateMachine) Lookup(query interface{}) (interface{}, error) {
	if s.Concurrent() {
		return s.concurrentLookup(query)
	}
	return s.lookup(query)
}

func (s *StateMachine) lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	if s.aborted {
		s.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	result, err := s.sm.Lookup(query)
	s.mu.RUnlock()
	return result, err
}

func (s *StateMachine) concurrentLookup(query interface{}) (interface{}, error) {
	return s.sm.Lookup(query)
}

// NALookup queries the local state machine.
func (s *StateMachine) NALookup(query []byte) ([]byte, error) {
	if s.Concurrent() {
		return s.naConcurrentLookup(query)
	}
	return s.naLookup(query)
}

func (s *StateMachine) naLookup(query []byte) ([]byte, error) {
	s.mu.RLock()
	if s.aborted {
		s.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	result, err := s.sm.NALookup(query)
	s.mu.RUnlock()
	return result, err
}

func (s *StateMachine) naConcurrentLookup(query []byte) ([]byte, error) {
	return s.sm.NALookup(query)
}

// GetMembership returns the membership info maintained by the state machine.
func (s *StateMachine) GetMembership() *pb.Membership {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.members.get()
}

// Concurrent returns a boolean flag indicating whether the state machine is
// capable of taking concurrent snapshot.
func (s *StateMachine) Concurrent() bool {
	return s.sm.ConcurrentSnapshot()
}

// OnDiskStateMachine returns a boolean flag indicating whether it is an on
// disk state machine.
func (s *StateMachine) OnDiskStateMachine() bool {
	return s.onDiskSM
}

// SaveSnapshot creates a snapshot.
func (s *StateMachine) SaveSnapshot(req SSRequest) (*pb.Snapshot,
	*server.SSEnv, error) {
	if req.IsStreamingSnapshot() {
		panic("invalid snapshot request")
	}
	if s.Concurrent() {
		return s.saveConcurrentSnapshot(req)
	}
	return s.saveSnapshot(req)
}

// StreamSnapshot starts to stream snapshot from the current SM to a remote
// node targeted by the provided sink.
func (s *StateMachine) StreamSnapshot(sink pb.IChunkSink) error {
	return s.streamSnapshot(sink)
}

// Sync synchronizes state machine's in-core state with that on disk.
func (s *StateMachine) Sync() error {
	return s.sync()
}

// GetHash returns the state machine hash.
func (s *StateMachine) GetHash() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sm.GetHash()
}

// GetSessionHash returns the session hash.
func (s *StateMachine) GetSessionHash() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions.GetSessionHash()
}

// GetMembershipHash returns the hash of the membership instance.
func (s *StateMachine) GetMembershipHash() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.members.getHash()
}

// Handle pulls the committed record and apply it if there is any available.
func (s *StateMachine) Handle(batch []Task, apply []sm.Entry) (Task, error) {
	batch = batch[:0]
	apply = apply[:0]
	processed := false
	defer func() {
		// give the node worker a chance to run when
		//  - batched applied value has been updated
		//  - taskC has been popped
		if processed {
			s.node.NodeReady()
		}
	}()
	rec, ok := s.taskQ.Get()
	if ok {
		processed = true
		if rec.IsSnapshotTask() {
			return rec, nil
		}
		if !rec.isSyncTask() {
			batch = append(batch, rec)
		} else {
			if err := s.sync(); err != nil {
				return Task{}, err
			}
		}
		done := false
		for !done {
			rec, ok := s.taskQ.Get()
			if ok {
				if rec.IsSnapshotTask() {
					if err := s.handle(batch, apply); err != nil {
						return Task{}, err
					}
					return rec, nil
				}
				if !rec.isSyncTask() {
					batch = append(batch, rec)
				} else {
					if err := s.sync(); err != nil {
						return Task{}, err
					}
				}
			} else {
				done = true
			}
		}
	}
	return Task{}, s.handle(batch, apply)
}

func (s *StateMachine) isDummySnapshot(r SSRequest) bool {
	return s.OnDiskStateMachine() &&
		!r.IsExportedSnapshot() &&
		!r.IsStreamingSnapshot()
}

func (s *StateMachine) getSSMeta(c interface{}, r SSRequest) (*SSMeta, error) {
	if s.members.isEmpty() {
		plog.Panicf("%s has empty membership", s.id())
	}
	ct := s.sct
	// never compress dummy snapshot file
	if s.isDummySnapshot(r) {
		ct = config.NoCompression
	}
	buf := bytes.NewBuffer(make([]byte, 0, sessionBufferInitialCap))
	meta := &SSMeta{
		From:            s.node.NodeID(),
		Ctx:             c,
		Index:           s.index,
		Term:            s.term,
		OnDiskIndex:     s.onDiskIndex,
		Request:         r,
		Session:         buf,
		Membership:      s.members.getMembership(),
		Type:            s.sm.StateMachineType(),
		CompressionType: ct,
	}
	plog.Infof("%s generating a snapshot at index %d, members %v",
		s.id(), meta.Index, meta.Membership.Addresses)
	if err := s.sessions.SaveSessions(meta.Session); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *StateMachine) updateLastApplied(index uint64, term uint64) {
	if s.index+1 != index {
		plog.Panicf("%s invalid index %d, applied %d", s.id(), index, s.index)
	}
	if index == 0 || term == 0 {
		plog.Panicf("%s invalid index %d or term %d", s.id(), index, term)
	}
	if term < s.term {
		plog.Panicf("%s term %d moving backward, new term %d", s.id(), s.term, term)
	}
	s.index = index
	s.term = term
}

func (s *StateMachine) checkSnapshotStatus(req SSRequest) error {
	if s.aborted {
		return sm.ErrSnapshotStopped
	}

	// When will this condition trigger? Could we protect when changing these indexes?
	if s.index < s.snapshotIndex {
		panic("s.index < s.snapshotIndex")
	}
	if !s.OnDiskStateMachine() {
		if !req.IsExportedSnapshot() &&
			s.index > 0 && s.index == s.snapshotIndex {
			return raft.ErrSnapshotOutOfDate
		}
	}
	return nil
}

func (s *StateMachine) streamSnapshot(sink pb.IChunkSink) error {
	var err error
	var meta *SSMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepareSnapshot(SSRequest{Type: StreamSnapshot})
		return err
	}(); err != nil {
		return err
	}
	if tests.ReadyToReturnTestKnob(s.node.ShouldStop(), true, "snapshotter.Stream") {
		return ErrTestKnobReturn
	}
	return s.snapshotter.Stream(s.sm, meta, sink)
}

func (s *StateMachine) saveConcurrentSnapshot(req SSRequest) (*pb.Snapshot,
	*server.SSEnv, error) {
	var err error
	var meta *SSMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepareSnapshot(req)
		return err
	}(); err != nil {
		return nil, nil, err
	}
	if tests.ReadyToReturnTestKnob(s.node.ShouldStop(), true, "s.sync") {
		return nil, nil, ErrTestKnobReturn
	}
	if err := s.sync(); err != nil {
		return nil, nil, err
	}
	if tests.ReadyToReturnTestKnob(s.node.ShouldStop(), true, "s.doSaveSnapshot") {
		return nil, nil, ErrTestKnobReturn
	}
	return s.doSaveSnapshot(meta)
}

func (s *StateMachine) saveSnapshot(req SSRequest) (*pb.Snapshot,
	*server.SSEnv, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, err := s.prepareSnapshot(req)
	if err != nil {
		plog.Errorf("prepare snapshot failed %v", err)
		return nil, nil, err
	}
	if tests.ReadyToReturnTestKnob(s.node.ShouldStop(), true, "s.doSaveSnapshot") {
		return nil, nil, ErrTestKnobReturn
	}
	return s.doSaveSnapshot(meta)
}

func (s *StateMachine) prepareSnapshot(req SSRequest) (*SSMeta, error) {
	if err := s.checkSnapshotStatus(req); err != nil {
		return nil, err
	}
	var err error
	var ctx interface{}
	if s.Concurrent() {
		ctx, err = s.sm.PrepareSnapshot()
		if err != nil {
			return nil, err
		}
	}
	return s.getSSMeta(ctx, req)
}

func (s *StateMachine) sync() error {
	if !s.OnDiskStateMachine() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plog.Infof("%s is being synchronized, index %d", s.id(), s.index)
	if err := s.sm.Sync(); err != nil {
		return err
	}
	s.setSyncedIndex(s.index)
	return nil
}

func (s *StateMachine) doSaveSnapshot(meta *SSMeta) (*pb.Snapshot,
	*server.SSEnv, error) {
	snapshot, env, err := s.snapshotter.Save(s.sm, meta)
	if err != nil {
		plog.Errorf("%s snapshotter.Save failed %v", s.id(), err)
		return nil, env, err
	}
	s.snapshotIndex = meta.Index
	return snapshot, env, nil
}

func getEntryTypes(entries []pb.Entry) (bool, bool) {
	allUpdate := true
	allNoOP := true
	for _, v := range entries {
		if allUpdate && !v.IsUpdateEntry() {
			allUpdate = false
		}
		if allNoOP && !v.IsNoOPSession() {
			allNoOP = false
		}
	}
	return allUpdate, allNoOP
}

func (s *StateMachine) handle(batch []Task, toApply []sm.Entry) error {
	batchSupport := batchedEntryApply && s.Concurrent()
	for b := range batch {
		if batch[b].IsSnapshotTask() || batch[b].isSyncTask() {
			plog.Panicf("%s trying to handle a snapshot/sync request", s.id())
		}
		input := batch[b].Entries
		allUpdate, allNoOP := getEntryTypes(input)
		if batchSupport && allUpdate && allNoOP {
			if err := s.handleBatch(input, toApply); err != nil {
				return err
			}
		} else {
			for i := range input {
				last := b == len(batch)-1 && i == len(input)-1
				if err := s.handleEntry(input[i], last); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isEmptyResult(result sm.Result) bool {
	return result.Data == nil && result.Value == 0
}

func (s *StateMachine) entryInInitDiskSM(index uint64) bool {
	if !s.OnDiskStateMachine() {
		return false
	}
	return index <= s.onDiskInitIndex
}

func (s *StateMachine) updateOnDiskIndex(firstIndex uint64, lastIndex uint64) {
	if !s.OnDiskStateMachine() {
		return
	}
	if firstIndex > lastIndex {
		panic("firstIndex > lastIndex")
	}
	if firstIndex <= s.onDiskInitIndex {
		plog.Panicf("last entry index to apply %d, initial on disk index %d",
			firstIndex, s.onDiskInitIndex)
	}
	if firstIndex <= s.onDiskIndex {
		plog.Panicf("last entry index to apply %d, on disk index %d",
			firstIndex, s.onDiskIndex)
	}
	s.onDiskIndex = lastIndex
}

func (s *StateMachine) handleEntry(ent pb.Entry, last bool) error {
	// ConfChange also goes through the SM so the index value is updated
	if ent.IsConfigChange() {
		accepted := s.handleConfigChange(ent)
		s.node.ConfigChangeProcessed(ent.Key, accepted)
	} else {
		if !ent.IsSessionManaged() {
			if ent.IsEmpty() {
				s.handleNoOP(ent)
				s.node.ApplyUpdate(ent, sm.Result{}, false, true, last)
			} else {
				panic("not session managed, not empty")
			}
		} else {
			if ent.IsNewSessionRequest() {
				smResult := s.handleRegisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, isEmptyResult(smResult), false, last)
			} else if ent.IsEndOfSessionRequest() {
				smResult := s.handleUnregisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, isEmptyResult(smResult), false, last)
			} else {
				if !s.entryInInitDiskSM(ent.Index) {
					smResult, ignored, rejected, err := s.handleUpdate(ent)
					if err != nil {
						return err
					}
					if !ignored {
						s.node.ApplyUpdate(ent, smResult, rejected, ignored, last)
					}
				} else {
					// treat it as a NoOP entry
					s.handleNoOP(pb.Entry{Index: ent.Index, Term: ent.Term})
				}
			}
		}
	}
	index := s.GetLastApplied()
	if index != ent.Index {
		plog.Panicf("unexpected last applied value, %d, %d", index, ent.Index)
	}
	if last {
		s.setBatchedLastApplied(ent.Index)
	}
	return nil
}

func (s *StateMachine) onUpdateApplied(ent pb.Entry,
	result sm.Result, ignored bool, rejected bool, last bool) {
	if !ignored {
		s.node.ApplyUpdate(ent, result, rejected, ignored, last)
	}
}

func (s *StateMachine) handleBatch(input []pb.Entry, ents []sm.Entry) error {
	if len(ents) != 0 {
		panic("ents is not empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	skipped := 0
	for _, ent := range input {
		if !s.entryInInitDiskSM(ent.Index) {
			e := sm.Entry{
				Index: ent.Index,
				Cmd:   getEntryPayload(ent),
			}
			ents = append(ents, e)
		} else {
			skipped++
		}
		s.updateLastApplied(ent.Index, ent.Term)
	}
	if len(ents) > 0 {
		firstIndex := ents[0].Index
		lastIndex := ents[len(ents)-1].Index
		s.updateOnDiskIndex(firstIndex, lastIndex)
		results, err := s.sm.BatchedUpdate(ents)
		if err != nil {
			return err
		}
		for idx, ent := range results {
			ce := input[skipped+idx]
			if ce.Index != ent.Index {
				// probably because user modified the Index value in results
				plog.Panicf("%s alignment error, %d, %d, %d",
					s.id(), ce.Index, ent.Index, skipped)
			}
			last := ce.Index == input[len(input)-1].Index
			s.onUpdateApplied(ce, ent.Result, false, false, last)
		}
	}
	if len(input) > 0 {
		s.setBatchedLastApplied(input[len(input)-1].Index)
	}
	return nil
}

func (s *StateMachine) handleConfigChange(ent pb.Entry) bool {
	var cc pb.ConfigChange
	if err := cc.Unmarshal(ent.Cmd); err != nil {
		panic(err)
	}
	if cc.Type == pb.AddNode && len(cc.Address) == 0 {
		panic("empty address in AddNode request")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateLastApplied(ent.Index, ent.Term)
	if s.members.handleConfigChange(cc, ent.Index) {
		s.node.ApplyConfigChange(cc)
		return true
	}
	return false
}

func (s *StateMachine) handleRegisterSession(ent pb.Entry) sm.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sessions.RegisterClientID(ent.ClientID)
	if isEmptyResult(smResult) {
		plog.Errorf("%s register client failed %v", s.id(), ent)
	}
	s.updateLastApplied(ent.Index, ent.Term)
	return smResult
}

func (s *StateMachine) handleUnregisterSession(ent pb.Entry) sm.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sessions.UnregisterClientID(ent.ClientID)
	if isEmptyResult(smResult) {
		plog.Errorf("%s unregister %d failed %v", s.id(), ent.ClientID, ent)
	}
	s.updateLastApplied(ent.Index, ent.Term)
	return smResult
}

func (s *StateMachine) handleNoOP(ent pb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ent.IsEmpty() || ent.IsSessionManaged() {
		panic("handle empty event called on non-empty event")
	}
	s.updateLastApplied(ent.Index, ent.Term)
}

// result a tuple of (result, should ignore, rejected)
func (s *StateMachine) handleUpdate(ent pb.Entry) (sm.Result, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ok bool
	var session *Session
	s.updateLastApplied(ent.Index, ent.Term)
	if !ent.IsNoOPSession() {
		session, ok = s.sessions.ClientRegistered(ent.ClientID)
		if !ok {
			// client is expected to crash
			return sm.Result{}, false, true, nil
		}
		s.sessions.UpdateRespondedTo(session, ent.RespondedTo)
		v, responded, toUpdate := s.sessions.UpdateRequired(session, ent.SeriesID)
		if responded {
			// should ignore. client is expected to timeout
			return sm.Result{}, true, false, nil
		}
		if !toUpdate {
			// server responded, client never confirmed
			// return the result again but not update the sm again
			// this implements the no-more-than-once update of the SM
			return v, false, false, nil
		}
	}
	if !ent.IsNoOPSession() && session == nil {
		panic("session not found")
	}
	if session != nil {
		if _, ok := session.getResponse(RaftSeriesID(ent.SeriesID)); ok {
			panic("already has response in session")
		}
	}
	s.updateOnDiskIndex(ent.Index, ent.Index)
	cmd := getEntryPayload(ent)
	result, err := s.sm.Update(sm.Entry{Index: ent.Index, Cmd: cmd})
	if err != nil {
		return sm.Result{}, false, false, err
	}
	if session != nil {
		session.addResponse(RaftSeriesID(ent.SeriesID), result)
	}
	return result, false, false, nil
}

func (s *StateMachine) id() string {
	return logutil.DescribeSM(s.node.ClusterID(), s.node.NodeID())
}

func snapshotInfo(ss pb.Snapshot) string {
	return fmt.Sprintf("addresses %v, config change id %d",
		ss.Membership.Addresses, ss.Membership.ConfigChangeId)
}

func getSnapshotFiles(snapshot pb.Snapshot) []sm.SnapshotFile {
	sfl := make([]sm.SnapshotFile, 0)
	for _, f := range snapshot.Files {
		sf := sm.SnapshotFile{
			FileID:   f.FileId,
			Filepath: f.Filepath,
			Metadata: f.Metadata,
		}
		sfl = append(sfl, sf)
	}
	return sfl
}
