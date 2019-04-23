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

	"github.com/lni/dragonboat/internal/raft"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/logger"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrSaveSnapshot indicates there is error when trying to save a snapshot
	ErrSaveSnapshot = errors.New("failed to save snapshot")
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot           = errors.New("failed to restore from snapshot")
	taskChanLength        uint64 = settings.Soft.NodeTaskChanLength
	taskChanBusyThreshold uint64 = settings.Soft.NodeTaskChanLength / 2
	batchedEntryApply     bool   = settings.Soft.BatchedEntryApply
)

// SnapshotRequestType is the type of a snapshot request.
type SnapshotRequestType uint64

const (
	// PeriodicSnapshot is the value to indicate periodic snapshot.
	PeriodicSnapshot SnapshotRequestType = iota
	// UserRequestedSnapshot is the value to indicate user requested snapshot.
	UserRequestedSnapshot
	// ExportedSnapshot is the value to indicate exported snapshot.
	ExportedSnapshot
)

// SnapshotRequest is the type for decribing the details of a snapshot request.
type SnapshotRequest struct {
	Type SnapshotRequestType
	Key  uint64
	Path string
}

// IsExportedSnapshot returns a boolean value indicating whether the snapshot
// request is to create an exported snapshot.
func (sr *SnapshotRequest) IsExportedSnapshot() bool {
	return sr.Type == ExportedSnapshot
}

// SnapshotMeta is the metadata of a snapshot.
type SnapshotMeta struct {
	From       uint64
	Index      uint64
	Term       uint64
	Request    SnapshotRequest
	Membership pb.Membership
	Type       pb.StateMachineType
	Session    *bytes.Buffer
	Ctx        interface{}
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
	SnapshotRequest   SnapshotRequest
	Entries           []pb.Entry
}

func (t *Task) isSnapshotRelated() bool {
	return t.SnapshotAvailable || t.SnapshotRequested || t.StreamSnapshot
}

// SMFactoryFunc is the function type for creating an IStateMachine instance
type SMFactoryFunc func(clusterID uint64,
	nodeID uint64, done <-chan struct{}) IManagedStateMachine

// INodeProxy is the interface used as proxy to a nodehost.
type INodeProxy interface {
	RestoreRemotes(pb.Snapshot)
	ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool)
	ApplyConfigChange(pb.ConfigChange)
	ConfigChangeProcessed(uint64, bool)
	NodeID() uint64
	ClusterID() uint64
}

// ISnapshotter is the interface for the snapshotter object.
type ISnapshotter interface {
	GetSnapshot(uint64) (pb.Snapshot, error)
	GetMostRecentSnapshot() (pb.Snapshot, error)
	GetFilePath(uint64) string
	StreamSnapshot(IStreamable, *SnapshotMeta, pb.IChunkSink) error
	Save(ISavable, *SnapshotMeta) (*pb.Snapshot, *server.SnapshotEnv, error)
	Load(uint64, ILoadableSessions, ILoadableSM, string, []sm.SnapshotFile) error
	IsNoSnapshotError(error) bool
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	mu                 sync.RWMutex
	snapshotter        ISnapshotter
	node               INodeProxy
	sm                 IManagedStateMachine
	sessions           *SessionManager
	members            *membership
	index              uint64
	term               uint64
	snapshotIndex      uint64
	diskSMIndex        uint64
	taskC              chan Task
	onDiskSM           bool
	aborted            bool
	batchedLastApplied struct {
		sync.Mutex
		index uint64
	}
}

// NewStateMachine creates a new application state machine object.
func NewStateMachine(sm IManagedStateMachine,
	snapshotter ISnapshotter, ordered bool,
	proxy INodeProxy) *StateMachine {
	a := &StateMachine{
		snapshotter: snapshotter,
		sm:          sm,
		onDiskSM:    sm.OnDiskStateMachine(),
		taskC:       make(chan Task, taskChanLength),
		node:        proxy,
		sessions:    NewSessionManager(),
		members:     newMembership(proxy.ClusterID(), proxy.NodeID(), ordered),
	}
	return a
}

// TaskC returns the task channel.
func (s *StateMachine) TaskC() chan Task {
	return s.taskC
}

// TaskChanBusy returns whether the TaskC chan is busy. Busy is defined as
// having more than half of its buffer occupied.
func (s *StateMachine) TaskChanBusy() bool {
	return uint64(len(s.taskC)) > taskChanBusyThreshold
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
	plog.Infof("sm.RecoverFromSnapshot called on %s, %+v", s.describe(), ss)
	if r, idx, err := s.recoverSnapshot(ss, t.InitialSnapshot); !r {
		return idx, err
	}
	s.node.RestoreRemotes(ss)
	s.setBatchedLastApplied(ss.Index)
	plog.Infof("%s snapshot %d restored, members %v",
		s.describe(), ss.Index, ss.Membership.Addresses)
	return ss.Index, nil
}

func (s *StateMachine) getSnapshot(rec Task) (pb.Snapshot, error) {
	if !rec.InitialSnapshot {
		snapshot, err := s.snapshotter.GetSnapshot(rec.Index)
		if err != nil && !s.snapshotter.IsNoSnapshotError(err) {
			plog.Errorf("%s, get snapshot failed: %v", s.describe(), err)
			return pb.Snapshot{}, ErrRestoreSnapshot
		}
		if s.snapshotter.IsNoSnapshotError(err) {
			plog.Errorf("%s, no snapshot", s.describe())
			return pb.Snapshot{}, err
		}
		return snapshot, nil
	}
	snapshot, err := s.snapshotter.GetMostRecentSnapshot()
	if s.snapshotter.IsNoSnapshotError(err) {
		plog.Infof("%s no snapshot available during start up", s.describe())
		return pb.Snapshot{}, nil
	}
	return snapshot, nil
}

func (s *StateMachine) recoverSMRequired(ss pb.Snapshot, init bool) bool {
	if !s.OnDiskStateMachine() {
		return true
	}
	if ss.Dummy {
		return false
	}
	fn := s.snapshotter.GetFilePath(ss.Index)
	shrinked, err := IsShrinkedSnapshotFile(fn)
	if err != nil {
		panic(err)
	}
	if init && shrinked {
		return false
	}
	if !init && shrinked {
		panic("not initial recovery but snapshot shrinked")
	}
	if init && ss.Index > s.diskSMIndex {
		plog.Infof("initial recover, ss.Index %d, disk index %d, time to recover",
			ss.Index, s.diskSMIndex)
		return true
	}
	plog.Infof("disk SM, not initial recover, always recover")
	return true
}

func (s *StateMachine) recoverSnapshot(ss pb.Snapshot,
	initial bool) (bool, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := ss.Index
	if s.index >= index {
		return false, s.index, nil
	}
	if s.aborted {
		return false, 0, sm.ErrSnapshotStopped
	}
	if s.recoverSMRequired(ss, initial) {
		plog.Infof("%s recover snapshot, term %d, index %d, %s, init %t",
			s.describe(), ss.Term, index, snapshotInfo(ss), initial)
		fs := getSnapshotFiles(ss)
		fn := s.snapshotter.GetFilePath(index)
		if err := s.snapshotter.Load(index, s.sessions, s.sm, fn, fs); err != nil {
			plog.Errorf("snapshotter.load failed on %s, %v", s.describe(), err)
			if err == sm.ErrSnapshotStopped {
				// no more lookup allowed
				s.aborted = true
				return false, 0, err
			}
			return false, 0, ErrRestoreSnapshot
		}
	} else {
		plog.Infof("all disk SM %s, %d vs %d, memory SM not restored",
			s.describe(), index, s.diskSMIndex)
	}
	// set the confState and the last applied value
	s.index = index
	s.term = ss.Term
	s.members.set(ss.Membership)
	return true, 0, nil
}

// we can not stream a full snapshot when membership state is catching up with
// the all disk SM state. however, meta only snapshot can be taken at any time.
func (s *StateMachine) readyToStreamSnapshot() bool {
	if !s.OnDiskStateMachine() {
		return true
	}
	return s.GetLastApplied() >= s.diskSMIndex
}

// OpenOnDiskStateMachine opens the on disk state machine.
func (s *StateMachine) OpenOnDiskStateMachine() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index, err := s.sm.Open()
	plog.Infof("%s opened disk state machine, index %d, err %v",
		s.describe(), index, err)
	if err != nil {
		if err == sm.ErrOpenStopped {
			s.aborted = true
		}
		return 0, err
	}
	s.diskSMIndex = index
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
	s.batchedLastApplied.Lock()
	v := s.batchedLastApplied.index
	s.batchedLastApplied.Unlock()
	return v
}

// SetBatchedLastApplied sets the batched last applied value. This method
// is mostly used in tests.
func (s *StateMachine) SetBatchedLastApplied(idx uint64) {
	s.setBatchedLastApplied(idx)
}

func (s *StateMachine) setBatchedLastApplied(idx uint64) {
	s.batchedLastApplied.Lock()
	s.batchedLastApplied.index = idx
	s.batchedLastApplied.Unlock()
}

// Offloaded marks the state machine as offloaded from the specified component.
func (s *StateMachine) Offloaded(from From) {
	s.sm.Offloaded(from)
}

// Loaded marks the state machine as loaded from the specified component.
func (s *StateMachine) Loaded(from From) {
	s.sm.Loaded(from)
}

// Lookup performances local lookup on the data store.
func (s *StateMachine) Lookup(query []byte) ([]byte, error) {
	if s.sm.ConcurrentSnapshot() {
		return s.concurrentLookup(query)
	}
	return s.lookup(query)
}

func (s *StateMachine) lookup(query []byte) ([]byte, error) {
	s.mu.RLock()
	if s.aborted {
		s.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	result, err := s.sm.Lookup(query)
	s.mu.RUnlock()
	return result, err
}

func (s *StateMachine) concurrentLookup(query []byte) ([]byte, error) {
	return s.sm.Lookup(query)
}

// GetMembership returns the membership info maintained by the state machine.
func (s *StateMachine) GetMembership() (map[uint64]string,
	map[uint64]string, map[uint64]struct{}, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.members.get()
}

// ConcurrentSnapshot returns a boolean flag indicating whether the state
// machine is capable of taking concurrent snapshot.
func (s *StateMachine) ConcurrentSnapshot() bool {
	return s.sm.ConcurrentSnapshot()
}

// OnDiskStateMachine returns a boolean flag indicating whether it is an on
// disk state machine.
func (s *StateMachine) OnDiskStateMachine() bool {
	return s.onDiskSM
}

// SaveSnapshot creates a snapshot.
func (s *StateMachine) SaveSnapshot(req SnapshotRequest) (*pb.Snapshot,
	*server.SnapshotEnv, error) {
	if s.sm.ConcurrentSnapshot() {
		return s.saveConcurrentSnapshot(req)
	}
	return s.saveSnapshot(req)
}

// StreamSnapshot starts to stream snapshot from the current SM to a remote
// node targetted by the provided sink.
func (s *StateMachine) StreamSnapshot(sink pb.IChunkSink) error {
	return s.streamSnapshot(sink)
}

// GetHash returns the state machine hash.
func (s *StateMachine) GetHash() uint64 {
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
func (s *StateMachine) Handle(batch []Task, toApply []sm.Entry) (Task, bool) {
	processed := 0
	batch = batch[:0]
	toApply = toApply[:0]
	select {
	case rec := <-s.taskC:
		if rec.isSnapshotRelated() {
			return rec, true
		}
		batch = append(batch, rec)
		processed++
		done := false
		for !done {
			select {
			case rec := <-s.taskC:
				if rec.isSnapshotRelated() {
					s.handle(batch, toApply)
					return rec, true
				}
				batch = append(batch, rec)
				processed++
			default:
				done = true
			}
		}
	default:
	}
	s.handle(batch, toApply)
	return Task{}, false
}

func (s *StateMachine) getSnapshotMeta(ctx interface{},
	req SnapshotRequest) *SnapshotMeta {
	if s.members.isEmpty() {
		plog.Panicf("%s has empty membership", s.describe())
	}
	meta := &SnapshotMeta{
		From:       s.node.NodeID(),
		Ctx:        ctx,
		Index:      s.index,
		Term:       s.term,
		Request:    req,
		Session:    bytes.NewBuffer(make([]byte, 0, 128*1024)),
		Membership: s.members.getMembership(),
		Type:       s.sm.StateMachineType(),
	}
	plog.Infof("%s generating a snapshot at index %d, members %v",
		s.describe(), meta.Index, meta.Membership.Addresses)
	if _, err := s.sessions.SaveSessions(meta.Session); err != nil {
		plog.Panicf("failed to save sessions %v", err)
	}
	return meta
}

func (s *StateMachine) updateLastApplied(index uint64, term uint64) {
	if s.index+1 != index {
		plog.Panicf("%s, not sequential update, last applied %d, applying %d",
			s.describe(), s.index, index)
	}
	if index == 0 || term == 0 {
		plog.Panicf("invalid last index %d or term %d", index, term)
	}
	if term < s.term {
		plog.Panicf("term is moving backward, term %d, applying term %d",
			s.term, term)
	}
	s.index = index
	s.term = term
}

func (s *StateMachine) checkSnapshotStatus() error {
	if s.aborted {
		return sm.ErrSnapshotStopped
	}
	if s.index < s.snapshotIndex {
		panic("s.index < s.snapshotIndex")
	}
	if !s.OnDiskStateMachine() {
		if s.index > 0 && s.index == s.snapshotIndex {
			return raft.ErrSnapshotOutOfDate
		}
	}
	return nil
}

func (s *StateMachine) saveConcurrentSnapshot(req SnapshotRequest) (*pb.Snapshot,
	*server.SnapshotEnv, error) {
	var err error
	var meta *SnapshotMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepareSnapshot(req)
		return err
	}(); err != nil {
		return nil, nil, err
	}
	return s.doSaveSnapshot(meta)
}

func (s *StateMachine) streamSnapshot(sink pb.IChunkSink) error {
	var err error
	var meta *SnapshotMeta
	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		meta, err = s.prepareSnapshot(SnapshotRequest{})
		return err
	}(); err != nil {
		return err
	}
	return s.snapshotter.StreamSnapshot(s.sm, meta, sink)
}

func (s *StateMachine) saveSnapshot(req SnapshotRequest) (*pb.Snapshot,
	*server.SnapshotEnv, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, err := s.prepareSnapshot(req)
	if err != nil {
		plog.Errorf("prepare snapshot failed %v", err)
		return nil, nil, err
	}
	return s.doSaveSnapshot(meta)
}

func (s *StateMachine) prepareSnapshot(req SnapshotRequest) (*SnapshotMeta,
	error) {
	if err := s.checkSnapshotStatus(); err != nil {
		return nil, err
	}
	var err error
	var ctx interface{}
	if s.ConcurrentSnapshot() {
		ctx, err = s.sm.PrepareSnapshot()
		if err != nil {
			panic(err)
		}
	}
	return s.getSnapshotMeta(ctx, req), nil
}

func (s *StateMachine) doSaveSnapshot(meta *SnapshotMeta) (*pb.Snapshot,
	*server.SnapshotEnv, error) {
	snapshot, env, err := s.snapshotter.Save(s.sm, meta)
	if err != nil {
		plog.Errorf("save snapshot failed %v", err)
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

func (s *StateMachine) handle(batch []Task, toApply []sm.Entry) {
	batchSupport := batchedEntryApply && s.ConcurrentSnapshot()
	for b := range batch {
		if batch[b].isSnapshotRelated() {
			panic("trying to handle a snapshot request")
		}
		ents := batch[b].Entries
		allUpdate, allNoOP := getEntryTypes(ents)
		if batchSupport && allUpdate && allNoOP {
			s.handleBatchedNoOPEntries(ents, toApply)
		} else {
			for i := range ents {
				notifyRead := b == len(batch)-1 && i == len(ents)-1
				s.handleEntry(ents[i], notifyRead)
			}
		}
	}
}

func isEmptyResult(result sm.Result) bool {
	return result.Data == nil && result.Value == 0
}

func (s *StateMachine) handleEntry(ent pb.Entry, lastInBatch bool) {
	// ConfChnage also go through the SM so the index value is updated
	if ent.IsConfigChange() {
		accepted := s.handleConfigChange(ent)
		s.node.ConfigChangeProcessed(ent.Key, accepted)
	} else {
		if !ent.IsSessionManaged() {
			if ent.IsEmpty() {
				s.handleNoOP(ent)
				s.node.ApplyUpdate(ent, sm.Result{}, false, true, lastInBatch)
			} else {
				panic("not session managed, not empty")
			}
		} else {
			if ent.IsNewSessionRequest() {
				smResult := s.handleRegisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, isEmptyResult(smResult), false, lastInBatch)
			} else if ent.IsEndOfSessionRequest() {
				smResult := s.handleUnregisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, isEmptyResult(smResult), false, lastInBatch)
			} else {
				if !s.entryAppliedInDiskSM(ent.Index) {
					smResult, ignored, rejected := s.handleUpdate(ent)
					if !ignored {
						s.node.ApplyUpdate(ent, smResult, rejected, ignored, lastInBatch)
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
	if lastInBatch {
		s.setBatchedLastApplied(ent.Index)
	}
}

func (s *StateMachine) entryAppliedInDiskSM(index uint64) bool {
	if !s.OnDiskStateMachine() {
		return false
	}
	return index <= s.diskSMIndex
}

func (s *StateMachine) onUpdateApplied(ent pb.Entry,
	result sm.Result, ignored bool, rejected bool, lastInBatch bool) {
	if !ignored {
		s.node.ApplyUpdate(ent, result, rejected, ignored, lastInBatch)
	}
}

func (s *StateMachine) handleBatchedNoOPEntries(input []pb.Entry,
	toApply []sm.Entry) {
	if len(toApply) != 0 {
		panic("toApply is not empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ent := range input {
		if !s.entryAppliedInDiskSM(ent.Index) {
			toApply = append(toApply, sm.Entry{Index: ent.Index, Cmd: ent.Cmd})
		}
		s.updateLastApplied(ent.Index, ent.Term)
	}
	if len(toApply) > 0 {
		results := s.sm.BatchedUpdate(toApply)
		for idx, ent := range results {
			lastInBatch := idx == len(input)-1
			s.onUpdateApplied(input[idx], ent.Result, false, false, lastInBatch)
		}
	}
	if len(input) > 0 {
		s.setBatchedLastApplied(input[len(input)-1].Index)
	}
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
		plog.Errorf("on %s register client failed, %v", s.describe(), ent)
	}
	s.updateLastApplied(ent.Index, ent.Term)
	return smResult
}

func (s *StateMachine) handleUnregisterSession(ent pb.Entry) sm.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sessions.UnregisterClientID(ent.ClientID)
	if isEmptyResult(smResult) {
		plog.Errorf("%s unregister client %d failed, %v",
			s.describe(), ent.ClientID, ent)
	}
	s.updateLastApplied(ent.Index, ent.Term)
	return smResult
}

func (s *StateMachine) handleNoOP(ent pb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !ent.IsEmpty() {
		panic("handle empty event called on non-empty event")
	}
	s.updateLastApplied(ent.Index, ent.Term)
}

// result a tuple of (result, should ignore, rejected)
func (s *StateMachine) handleUpdate(ent pb.Entry) (sm.Result, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ok bool
	var session *Session
	s.updateLastApplied(ent.Index, ent.Term)
	if !ent.IsNoOPSession() {
		session, ok = s.sessions.ClientRegistered(ent.ClientID)
		if !ok {
			// client is expected to crash
			return sm.Result{}, false, true
		}
		s.sessions.UpdateRespondedTo(session, ent.RespondedTo)
		result, responded, updateRequired := s.sessions.UpdateRequired(session,
			ent.SeriesID)
		if responded {
			// should ignore. client is expected to timeout
			return sm.Result{}, true, false
		}
		if !updateRequired {
			// server responded, client never confirmed
			// return the result again but not update the sm again
			// this implements the no-more-than-once update of the SM
			return result, false, false
		}
	}
	if !ent.IsNoOPSession() && session == nil {
		panic("session not found")
	}
	return s.sm.Update(session, ent), false, false
}

func (s *StateMachine) describe() string {
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
