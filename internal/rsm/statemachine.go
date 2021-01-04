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

/*
Package rsm implements Replicated State Machines used in Dragonboat.

This package is internally used by Dragonboat, applications are not expected to
import this package.
*/
package rsm

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot             = errors.New("failed to restore from snapshot")
	batchedEntryApply              = settings.Soft.BatchedEntryApply
	sessionBufferInitialCap uint64 = 128 * 1024
)

// SSReqType is the type of a snapshot request.
type SSReqType uint64

const (
	// Periodic is the value to indicate periodic snapshot.
	Periodic SSReqType = iota
	// UserRequested is the value to indicate user requested snapshot.
	UserRequested
	// Exported is the value to indicate exported snapshot.
	Exported
	// Streaming is the value to indicate snapshot streaming.
	Streaming
)

// SSRequest is the type for describing the details of a snapshot request.
type SSRequest struct {
	Type               SSReqType
	Key                uint64
	Path               string
	OverrideCompaction bool
	CompactionOverhead uint64
}

// Exported returns a boolean value indicating whether the snapshot request
// is to create an exported snapshot.
func (r *SSRequest) Exported() bool {
	return r.Type == Exported
}

// Streaming returns a boolean value indicating whether the snapshot request
// is to stream snapshot.
func (r *SSRequest) Streaming() bool {
	return r.Type == Streaming
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
	ClusterID    uint64
	NodeID       uint64
	Index        uint64
	Recover      bool
	Initial      bool
	Save         bool
	Stream       bool
	PeriodicSync bool
	NewNode      bool
	SSRequest    SSRequest
	Entries      []pb.Entry
}

// IsSnapshotTask returns a boolean flag indicating whether it is a snapshot
// task.
func (t *Task) IsSnapshotTask() bool {
	return t.Recover || t.Save || t.Stream
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

// INode is the interface of a dragonboat node.
type INode interface {
	StepReady()
	RestoreRemotes(pb.Snapshot)
	ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool)
	ApplyConfigChange(pb.ConfigChange, uint64, bool)
	NodeID() uint64
	ClusterID() uint64
	ShouldStop() <-chan struct{}
}

// ISnapshotter is the interface for the snapshotter object.
type ISnapshotter interface {
	GetSnapshot(uint64) (pb.Snapshot, error)
	GetMostRecentSnapshot() (pb.Snapshot, error)
	Stream(IStreamable, *SSMeta, pb.IChunkSink) error
	Shrunk(ss pb.Snapshot) (bool, error)
	Save(ISavable, *SSMeta) (pb.Snapshot, *server.SSEnv, error)
	Load(pb.Snapshot, ILoadable, IRecoverable) error
	IsNoSnapshotError(error) bool
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	syncedIndex        uint64
	visibleLastApplied uint64
	mu                 sync.RWMutex
	snapshotter        ISnapshotter
	node               INode
	sm                 IManagedStateMachine
	sessions           *SessionManager
	members            *membership
	index              uint64
	term               uint64
	snapshotIndex      uint64
	onDiskInitIndex    uint64
	onDiskIndex        uint64
	taskQ              *TaskQueue
	onDiskSM           bool
	aborted            bool
	isWitness          bool
	sct                config.CompressionType
	fs                 vfs.IFS
}

// NewStateMachine creates a new application state machine object.
func NewStateMachine(sm IManagedStateMachine,
	snapshotter ISnapshotter,
	cfg config.Config, node INode, fs vfs.IFS) *StateMachine {
	ordered := cfg.OrderedConfigChange
	return &StateMachine{
		snapshotter: snapshotter,
		sm:          sm,
		onDiskSM:    sm.OnDisk(),
		taskQ:       NewTaskQueue(),
		node:        node,
		sessions:    NewSessionManager(),
		members:     newMembership(node.ClusterID(), node.NodeID(), ordered),
		isWitness:   cfg.IsWitness,
		sct:         cfg.SnapshotCompressionType,
		fs:          fs,
	}
}

// Type returns the state machine type.
func (s *StateMachine) Type() pb.StateMachineType {
	return s.sm.Type()
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

// DestroyedC return a chan struct{} used to indicate whether the SM has been
// fully unloaded.
func (s *StateMachine) DestroyedC() <-chan struct{} {
	return s.sm.DestroyedC()
}

// Recover applies the snapshot.
func (s *StateMachine) Recover(t Task) (uint64, error) {
	ss, err := s.getSnapshot(t)
	if err != nil {
		return 0, err
	}
	if pb.IsEmptySnapshot(ss) {
		return 0, nil
	}
	ss.Validate(s.fs)
	plog.Debugf("%s called RecoverFromSnapshot, %s, on disk idx %d",
		s.id(), s.ssid(ss.Index), ss.OnDiskIndex)
	if err := s.recover(ss, t.Initial); err != nil {
		return 0, err
	}
	s.node.RestoreRemotes(ss)
	s.SetVisibleLastApplied(ss.Index)
	plog.Debugf("%s restored %s", s.id(), s.ssid(ss.Index))
	return ss.Index, nil
}

func (s *StateMachine) getSnapshot(t Task) (pb.Snapshot, error) {
	if !t.Initial {
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
		plog.Infof("%s no snapshot available during launch", s.id())
		return pb.Snapshot{}, nil
	}
	return snapshot, nil
}

func (s *StateMachine) mustBeOnDiskSM() {
	if !s.OnDiskStateMachine() {
		panic("not an on disk sm")
	}
}

func (s *StateMachine) recoverRequired(ss pb.Snapshot, init bool) bool {
	s.mustBeOnDiskSM()
	shrunk, err := s.snapshotter.Shrunk(ss)
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
		}
		return ss.OnDiskIndex > s.onDiskInitIndex
	}
	return ss.OnDiskIndex > s.onDiskIndex
}

func (s *StateMachine) canRecoverOnDiskSnapshot(ss pb.Snapshot, init bool) {
	s.mustBeOnDiskSM()
	if ss.Imported && init {
		return
	}
	if ss.OnDiskIndex <= s.onDiskInitIndex {
		plog.Panicf("%s, ss.OnDiskIndex (%d) <= s.onDiskInitIndex (%d)",
			s.id(), ss.OnDiskIndex, s.onDiskInitIndex)
	}
	if ss.OnDiskIndex <= s.onDiskIndex {
		plog.Panicf("%s, ss.OnDiskInit (%d) <= s.onDiskIndex (%d)",
			s.id(), ss.OnDiskIndex, s.onDiskIndex)
	}
}

func (s *StateMachine) recover(ss pb.Snapshot, init bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index >= ss.Index {
		return raft.ErrSnapshotOutOfDate
	}
	if s.aborted {
		return sm.ErrSnapshotStopped
	}
	if ss.Witness || ss.Dummy {
		s.apply(ss)
		return nil
	}
	if !s.OnDiskStateMachine() {
		return s.doRecover(ss, init)
	}
	if s.recoverRequired(ss, init) {
		s.canRecoverOnDiskSnapshot(ss, init)
		if err := s.doRecover(ss, init); err != nil {
			return err
		}
		s.applyOnDisk(ss, init)
	} else {
		s.apply(ss)
	}
	return nil
}

func (s *StateMachine) doRecover(ss pb.Snapshot, init bool) error {
	index := ss.Index
	plog.Debugf("%s recovering from %s, init %t", s.id(), s.ssid(index), init)
	s.logMembership("members", index, ss.Membership.Addresses)
	s.logMembership("observers", index, ss.Membership.Observers)
	s.logMembership("witnesses", index, ss.Membership.Witnesses)
	if err := s.snapshotter.Load(ss, s.sessions, s.sm); err != nil {
		plog.Errorf("%s failed to load %s, %v", s.id(), s.ssid(index), err)
		if err == sm.ErrSnapshotStopped {
			s.aborted = true
			return err
		}
		return ErrRestoreSnapshot
	}
	s.apply(ss)
	return nil
}

func (s *StateMachine) apply(ss pb.Snapshot) {
	s.index = ss.Index
	s.term = ss.Term
	s.members.set(ss.Membership)
}

func (s *StateMachine) applyOnDisk(ss pb.Snapshot, init bool) {
	s.mustBeOnDiskSM()
	s.onDiskIndex = ss.OnDiskIndex
	if ss.Imported && init {
		s.onDiskInitIndex = ss.OnDiskIndex
	}
	s.apply(ss)
}

//TODO: add test to cover the case when ReadyToStreamSnapshot returns false

// ReadyToStream returns a boolean flag to indicate whether the state machine
// is ready to stream snapshot. It can not stream a full snapshot when
// membership state is catching up with the all disk SM state. Meta only
// snapshot can be taken at any time.
func (s *StateMachine) ReadyToStream() bool {
	if !s.OnDiskStateMachine() {
		return true
	}
	return s.GetLastApplied() >= s.onDiskInitIndex
}

func (s *StateMachine) tryInjectTestFS() {
	if nsm, ok := s.sm.(*NativeSM); ok {
		if odsm, ok := nsm.sm.(*OnDiskStateMachine); ok {
			odsm.SetTestFS(s.fs)
		}
	}
}

// OpenOnDiskStateMachine opens the on disk state machine.
func (s *StateMachine) OpenOnDiskStateMachine() (uint64, error) {
	s.mustBeOnDiskSM()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tryInjectTestFS()
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

// GetSyncedIndex returns the index value that is known to have been
// synchronized.
func (s *StateMachine) GetSyncedIndex() uint64 {
	return atomic.LoadUint64(&s.syncedIndex)
}

// GetVisibleLastApplied returns the batched last applied value.
func (s *StateMachine) GetVisibleLastApplied() uint64 {
	return atomic.LoadUint64(&s.visibleLastApplied)
}

// SetVisibleLastApplied sets the batched last applied value. This method
// is mostly used in tests.
func (s *StateMachine) SetVisibleLastApplied(index uint64) {
	if s.GetVisibleLastApplied() > index {
		panic("batched applied index moving backward")
	}
	atomic.StoreUint64(&s.visibleLastApplied, index)
}

func (s *StateMachine) setSyncedIndex(index uint64) {
	if s.GetSyncedIndex() > index {
		panic("synced index moving backward")
	}
	atomic.StoreUint64(&s.syncedIndex, index)
}

// Offloaded marks the state machine as offloaded from the specified compone.
// It returns a boolean value indicating whether the node has been fully
// unloaded after unloading from the specified compone.
func (s *StateMachine) Offloaded(from From) bool {
	return s.sm.Offloaded(from)
}

// Loaded marks the state machine as loaded from the specified compone.
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
	defer s.mu.RUnlock()
	if s.aborted {
		return nil, ErrClusterClosed
	}
	return s.sm.Lookup(query)
}

func (s *StateMachine) concurrentLookup(query interface{}) (interface{}, error) {
	return s.sm.Lookup(query)
}

// NALookup queries the local state machine.
func (s *StateMachine) NALookup(query []byte) ([]byte, error) {
	if s.Concurrent() {
		return s.naConcurrentLookup(query)
	}
	return s.nalookup(query)
}

func (s *StateMachine) nalookup(query []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.aborted {
		return nil, ErrClusterClosed
	}
	return s.sm.NALookup(query)
}

func (s *StateMachine) naConcurrentLookup(query []byte) ([]byte, error) {
	return s.sm.NALookup(query)
}

// GetMembership returns the membership info maintained by the state machine.
func (s *StateMachine) GetMembership() pb.Membership {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.members.get()
}

// Concurrent returns a boolean flag indicating whether the state machine is
// capable of taking concurrent snapshot.
func (s *StateMachine) Concurrent() bool {
	return s.sm.Concurrent()
}

// OnDiskStateMachine returns a boolean flag indicating whether it is an on
// disk state machine.
func (s *StateMachine) OnDiskStateMachine() bool {
	return s.onDiskSM && !s.isWitness
}

// Save creates a snapshot.
func (s *StateMachine) Save(req SSRequest) (pb.Snapshot, *server.SSEnv, error) {
	if req.Streaming() {
		panic("invalid snapshot request")
	}
	if s.isWitness {
		plog.Panicf("witness %s saving snapshot", s.id())
	}
	if s.Concurrent() {
		return s.concurrentSave(req)
	}
	return s.save(req)
}

// Stream starts to stream snapshot from the current SM to a remote node
// targeted by the provided sink.
func (s *StateMachine) Stream(sink pb.IChunkSink) error {
	return s.stream(sink)
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
			s.node.StepReady()
		}
	}()
	if rec, ok := s.taskQ.Get(); ok {
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
			if rec, ok := s.taskQ.Get(); ok {
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
	return s.OnDiskStateMachine() && !r.Exported() && !r.Streaming()
}

func (s *StateMachine) logMembership(name string,
	index uint64, members map[uint64]string) {
	plog.Debugf("%d %s included in %s", len(members), name, s.ssid(index))
	for nid, addr := range members {
		plog.Debugf("\t%s : %s", logutil.NodeID(nid), addr)
	}
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
		Membership:      s.members.get(),
		Type:            s.sm.Type(),
		CompressionType: ct,
	}
	plog.Debugf("%s generating %s", s.id(), s.ssid(meta.Index))
	s.logMembership("members", meta.Index, meta.Membership.Addresses)
	if err := s.sessions.SaveSessions(meta.Session); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *StateMachine) setLastApplied(index uint64, term uint64) {
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
	if s.index < s.snapshotIndex {
		panic("s.index < s.snapshotIndex")
	}
	if !s.OnDiskStateMachine() {
		if !req.Exported() && s.index > 0 && s.index == s.snapshotIndex {
			return raft.ErrSnapshotOutOfDate
		}
	}
	return nil
}

func (s *StateMachine) stream(sink pb.IChunkSink) error {
	var err error
	var meta *SSMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepare(SSRequest{Type: Streaming})
		return err
	}(); err != nil {
		return err
	}
	return s.snapshotter.Stream(s.sm, meta, sink)
}

func (s *StateMachine) concurrentSave(req SSRequest) (pb.Snapshot,
	*server.SSEnv, error) {
	var err error
	var meta *SSMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepare(req)
		return err
	}(); err != nil {
		return pb.Snapshot{}, nil, err
	}
	if err := s.sync(); err != nil {
		return pb.Snapshot{}, nil, err
	}
	return s.doSave(meta)
}

func (s *StateMachine) save(req SSRequest) (pb.Snapshot, *server.SSEnv, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, err := s.prepare(req)
	if err != nil {
		plog.Errorf("prepare snapshot failed %v", err)
		return pb.Snapshot{}, nil, err
	}
	return s.doSave(meta)
}

func (s *StateMachine) prepare(req SSRequest) (*SSMeta, error) {
	if err := s.checkSnapshotStatus(req); err != nil {
		return nil, err
	}
	var err error
	var ctx interface{}
	if s.Concurrent() {
		ctx, err = s.sm.Prepare()
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
	plog.Debugf("%s is being synchronized, index %d", s.id(), s.index)
	if err := s.sm.Sync(); err != nil {
		return err
	}
	s.setSyncedIndex(s.index)
	return nil
}

func (s *StateMachine) doSave(meta *SSMeta) (pb.Snapshot,
	*server.SSEnv, error) {
	ss, env, err := s.snapshotter.Save(s.sm, meta)
	if err != nil {
		plog.Errorf("%s snapshotter.Save failed %v", s.id(), err)
		return pb.Snapshot{}, env, err
	}
	s.snapshotIndex = meta.Index
	return ss, env, nil
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

func (s *StateMachine) setOnDiskIndex(first uint64, last uint64) {
	if !s.OnDiskStateMachine() {
		return
	}
	if first > last {
		panic("first > last")
	}
	if first <= s.onDiskInitIndex {
		plog.Panicf("first index %d, init on disk %d", first, s.onDiskInitIndex)
	}
	if first <= s.onDiskIndex {
		plog.Panicf("first index %d, on disk index %d", first, s.onDiskIndex)
	}
	s.onDiskIndex = last
}

func (s *StateMachine) handleEntry(e pb.Entry, last bool) error {
	if e.IsConfigChange() {
		s.handleConfigChange(e)
	} else {
		if !e.IsSessionManaged() {
			if e.IsEmpty() {
				s.handleNoOP(e)
				s.node.ApplyUpdate(e, sm.Result{}, false, true, last)
			} else {
				panic("not session managed, not empty")
			}
		} else {
			if e.IsNewSessionRequest() {
				smResult := s.handleRegisterSession(e)
				s.node.ApplyUpdate(e, smResult, isEmptyResult(smResult), false, last)
			} else if e.IsEndOfSessionRequest() {
				smResult := s.handleUnregisterSession(e)
				s.node.ApplyUpdate(e, smResult, isEmptyResult(smResult), false, last)
			} else {
				if !s.entryInInitDiskSM(e.Index) {
					smResult, ignored, rejected, err := s.handleUpdate(e)
					if err != nil {
						return err
					}
					if !ignored {
						s.node.ApplyUpdate(e, smResult, rejected, ignored, last)
					}
				} else {
					// treat it as a NoOP entry
					s.handleNoOP(pb.Entry{Index: e.Index, Term: e.Term})
				}
			}
		}
	}
	index := s.GetLastApplied()
	if index != e.Index {
		plog.Panicf("unexpected last applied value, %d, %d", index, e.Index)
	}
	if last {
		s.SetVisibleLastApplied(e.Index)
	}
	return nil
}

func (s *StateMachine) onUpdateApplied(e pb.Entry,
	result sm.Result, ignored bool, rejected bool, last bool) {
	if !ignored {
		s.node.ApplyUpdate(e, result, rejected, ignored, last)
	}
}

func (s *StateMachine) handleBatch(input []pb.Entry, ents []sm.Entry) error {
	if len(ents) != 0 {
		panic("ents is not empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	skipped := 0
	for _, e := range input {
		if !s.entryInInitDiskSM(e.Index) {
			ents = append(ents, sm.Entry{
				Index: e.Index,
				Cmd:   GetPayload(e),
			})
		} else {
			skipped++
		}
		s.setLastApplied(e.Index, e.Term)
	}
	if len(ents) > 0 {
		results, err := s.sm.BatchedUpdate(ents)
		if err != nil {
			return err
		}
		for idx, e := range results {
			ce := input[skipped+idx]
			if ce.Index != e.Index {
				// probably because user modified the Index value in results
				plog.Panicf("%s alignment error, %d, %d, %d",
					s.id(), ce.Index, e.Index, skipped)
			}
			last := ce.Index == input[len(input)-1].Index
			s.onUpdateApplied(ce, e.Result, false, false, last)
		}
		s.setOnDiskIndex(ents[0].Index, ents[len(ents)-1].Index)
	}
	if len(input) > 0 {
		s.SetVisibleLastApplied(input[len(input)-1].Index)
	}
	return nil
}

func (s *StateMachine) handleConfigChange(e pb.Entry) {
	var cc pb.ConfigChange
	if err := cc.Unmarshal(e.Cmd); err != nil {
		panic(err)
	}
	rejected := true
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.setLastApplied(e.Index, e.Term)
		if s.members.handleConfigChange(cc, e.Index) {
			rejected = false
		}
	}()
	s.node.ApplyConfigChange(cc, e.Key, rejected)
}

func (s *StateMachine) handleRegisterSession(e pb.Entry) sm.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sessions.RegisterClientID(e.ClientID)
	if isEmptyResult(smResult) {
		plog.Errorf("%s register client failed %v", s.id(), e)
	}
	s.setLastApplied(e.Index, e.Term)
	return smResult
}

func (s *StateMachine) handleUnregisterSession(e pb.Entry) sm.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sessions.UnregisterClientID(e.ClientID)
	if isEmptyResult(smResult) {
		plog.Errorf("%s unregister %d failed %v", s.id(), e.ClientID, e)
	}
	s.setLastApplied(e.Index, e.Term)
	return smResult
}

func (s *StateMachine) handleNoOP(e pb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.IsEmpty() || e.IsSessionManaged() {
		panic("handle empty event called on non-empty event")
	}
	s.setLastApplied(e.Index, e.Term)
}

// result a tuple of (result, should ignore, rejected)
func (s *StateMachine) handleUpdate(e pb.Entry) (sm.Result, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ok bool
	var session *Session
	s.setLastApplied(e.Index, e.Term)
	if !e.IsNoOPSession() {
		session, ok = s.sessions.ClientRegistered(e.ClientID)
		if !ok {
			// client is expected to crash
			return sm.Result{}, false, true, nil
		}
		s.sessions.UpdateRespondedTo(session, e.RespondedTo)
		v, responded, toUpdate := s.sessions.UpdateRequired(session, e.SeriesID)
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
	if !e.IsNoOPSession() && session == nil {
		panic("session not found")
	}
	if session != nil {
		if _, ok := session.getResponse(RaftSeriesID(e.SeriesID)); ok {
			panic("already has response in session")
		}
	}
	result, err := s.sm.Update(sm.Entry{Index: e.Index, Cmd: GetPayload(e)})
	if err != nil {
		return sm.Result{}, false, false, err
	}
	s.setOnDiskIndex(e.Index, e.Index)
	if session != nil {
		session.addResponse(RaftSeriesID(e.SeriesID), result)
	}
	return result, false, false, nil
}

func (s *StateMachine) id() string {
	return logutil.DescribeSM(s.node.ClusterID(), s.node.NodeID())
}

func (s *StateMachine) ssid(index uint64) string {
	return logutil.DescribeSS(s.node.ClusterID(), s.node.NodeID(), index)
}
