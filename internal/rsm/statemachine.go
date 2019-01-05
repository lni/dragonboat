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
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/lni/dragonboat/internal/raft"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/logger"
	pb "github.com/lni/dragonboat/raftpb"
	"github.com/lni/dragonboat/statemachine"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrSaveSnapshot indicates there is error when trying to save a snapshot
	ErrSaveSnapshot = errors.New("failed to save snapshot")
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot             = errors.New("failed to restore from snapshot")
	commitChanLength        uint64 = settings.Soft.NodeCommitChanLength
	commitChanBusyThreshold uint64 = settings.Soft.NodeCommitChanLength / 2
	batchedEntryApply       bool   = settings.Soft.BatchedEntryApply > 0
)

// Commit is the processing units that can be handled by StateMachines.
type Commit struct {
	Index             uint64
	SnapshotAvailable bool
	InitialSnapshot   bool
	SnapshotRequested bool
	Entries           []pb.Entry
}

// SMFactoryFunc is the function type for creating an IStateMachine instance
type SMFactoryFunc func(clusterID uint64,
	nodeID uint64, done <-chan struct{}) IManagedStateMachine

// INodeProxy is the interface used as proxy to a nodehost.
type INodeProxy interface {
	RestoreRemotes(pb.Snapshot)
	ApplyUpdate(pb.Entry, uint64, bool, bool, bool)
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
	Save(uint64, uint64, IManagedStateMachine) (*pb.Snapshot, *server.SnapshotEnv, error)
	IsNoSnapshotError(error) bool
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	mu                 sync.RWMutex
	snapshotter        ISnapshotter
	node               INodeProxy
	sm                 IManagedStateMachine
	lastApplied        uint64
	lastAppliedTerm    uint64
	lastSnapshotIndex  uint64
	members            *pb.Membership
	ordered            bool
	commitC            chan Commit
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
		commitC:     make(chan Commit, commitChanLength),
		ordered:     ordered,
		node:        proxy,
	}
	a.members = &pb.Membership{
		Addresses: make(map[uint64]string),
		Observers: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	return a
}

// CommitC returns the commit channel.
func (s *StateMachine) CommitC() chan Commit {
	return s.commitC
}

// CommitChanBusy returns whether the CommitC chan is busy. Busy is defined as
// having more than half of its buffer occupied.
func (s *StateMachine) CommitChanBusy() bool {
	return uint64(len(s.commitC)) > commitChanBusyThreshold
}

// RecoverFromSnapshot applies the snapshot.
func (s *StateMachine) RecoverFromSnapshot(rec Commit) (uint64, error) {
	snapshot, err := s.getSnapshot(rec)
	if err != nil {
		return 0, err
	}
	if pb.IsEmptySnapshot(snapshot) {
		return 0, nil
	}
	snapshot.Validate()
	if recovered, idx, err := s.recoverSnapshot(snapshot,
		rec.InitialSnapshot); !recovered {
		return idx, err
	}
	s.setBatchedLastApplied(snapshot.Index)
	s.node.RestoreRemotes(snapshot)
	plog.Infof("%s snapshot %d restored, members %v",
		s.describe(), snapshot.Index, snapshot.Membership.Addresses)
	return snapshot.Index, nil
}

func (s *StateMachine) getSnapshot(rec Commit) (pb.Snapshot, error) {
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

func (s *StateMachine) recoverSnapshot(ss pb.Snapshot,
	initial bool) (bool, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastApplied >= ss.Index {
		return false, s.lastApplied, nil
	}
	if s.aborted {
		return false, 0, statemachine.ErrSnapshotStopped
	}
	plog.Infof("%s restarting at term %d, index %d, %s, initial snapshot %t",
		s.describe(), ss.Term, ss.Index, snapshotInfo(ss), initial)
	snapshotFiles := getSnapshotFiles(ss)
	fn := s.snapshotter.GetFilePath(ss.Index)
	if err := s.sm.RecoverFromSnapshot(fn, snapshotFiles); err != nil {
		plog.Infof("%s called RecoverFromSnapshot %d, returned %v",
			s.describe(), ss.Index, err)
		if err == statemachine.ErrSnapshotStopped {
			// no more lookup allowed
			s.aborted = true
			return false, 0, err
		}
		return false, 0, ErrRestoreSnapshot
	}
	// set the confState and the last applied value
	s.lastApplied = ss.Index
	s.lastAppliedTerm = ss.Term
	cm := deepCopyMembership(ss.Membership)
	s.members = &cm
	return true, 0, nil
}

// GetLastApplied returns the last applied value.
func (s *StateMachine) GetLastApplied() uint64 {
	s.mu.RLock()
	v := s.lastApplied
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
	s.mu.RLock()
	if s.aborted {
		s.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	result, err := s.sm.Lookup(query)
	s.mu.RUnlock()
	return result, err
}

// GetMembership returns the membership info maintained by the state machine.
func (s *StateMachine) GetMembership() (map[uint64]string,
	map[uint64]string, map[uint64]struct{}, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	members := make(map[uint64]string)
	observers := make(map[uint64]string)
	removed := make(map[uint64]struct{})
	for nid, addr := range s.members.Addresses {
		members[nid] = addr
	}
	for nid, addr := range s.members.Observers {
		observers[nid] = addr
	}
	for nid := range s.members.Removed {
		removed[nid] = struct{}{}
	}
	return members, observers, removed, s.members.ConfigChangeId
}

// SaveSnapshot creates a snapshot.
func (s *StateMachine) SaveSnapshot() (*pb.Snapshot,
	*server.SnapshotEnv, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.aborted {
		return nil, nil, statemachine.ErrSnapshotStopped
	}
	if s.lastApplied < s.lastSnapshotIndex {
		panic("s.lastApplied < s.lastSnapshotIndex")
	}
	if s.lastApplied > 0 && s.lastApplied == s.lastSnapshotIndex {
		return nil, nil, raft.ErrSnapshotOutOfDate
	}
	snapshot, env, err := s.snapshotter.Save(s.lastApplied,
		s.lastAppliedTerm, s.sm)
	if err != nil {
		plog.Errorf("Save snapshot failed %v", err)
		return nil, env, err
	}
	if len(s.members.Addresses) > 0 {
		snapshot.Membership = deepCopyMembership(*s.members)
		plog.Infof("%s generated a snapshot at index %d, members %v",
			s.describe(), s.lastApplied, snapshot.Membership.Addresses)
	} else {
		plog.Panicf("%s has empty membership", s.describe(), s.lastApplied)
	}
	s.lastSnapshotIndex = s.lastApplied
	return snapshot, env, nil
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
	return s.sm.GetSessionHash()
}

// GetMembershipHash returns the hash of the membership instance.
func (s *StateMachine) GetMembershipHash() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.members == nil {
		return 0
	}
	nidList := make([]uint64, 0)
	for k := range s.members.Addresses {
		nidList = append(nidList, k)
	}
	sort.Slice(nidList, func(i, j int) bool { return nidList[i] < nidList[j] })
	data := make([]byte, (len(s.members.Addresses)+1)*8)
	idx := 0
	for _, k := range nidList {
		binary.LittleEndian.PutUint64(data[idx:], k)
		idx += 8
	}
	binary.LittleEndian.PutUint64(data[idx:], s.members.ConfigChangeId)
	hash := md5.New()
	if _, err := hash.Write(data); err != nil {
		panic(err)
	}
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8])
}

func (s *StateMachine) updateLastApplied(lastApplied uint64, term uint64) {
	if s.lastApplied+1 != lastApplied {
		plog.Panicf("%s, not sequential update, last applied %d, applying %d",
			s.describe(), s.lastApplied, lastApplied)
	}
	if lastApplied == 0 {
		plog.Panicf("lastApplied is 0")
	}
	if term == 0 {
		plog.Panicf("term is 0")
	}
	if term < s.lastAppliedTerm {
		plog.Panicf("term is moving backward, term %d, applying term %d",
			s.lastAppliedTerm, term)
	}
	s.lastApplied = lastApplied
	s.lastAppliedTerm = term
}

// Handle pulls the committed record and apply it if there is any available.
func (s *StateMachine) Handle(batch []Commit) (Commit, bool) {
	processed := 0
	batch = batch[:0]
	select {
	case rec := <-s.commitC:
		if rec.SnapshotAvailable || rec.SnapshotRequested {
			return rec, true
		}
		batch = append(batch, rec)
		processed++
		done := false
		for !done {
			select {
			case rec := <-s.commitC:
				if rec.SnapshotAvailable || rec.SnapshotRequested {
					s.handle(batch)
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
	s.handle(batch)
	return Commit{}, false
}

func allUpdateEntries(entries []pb.Entry) bool {
	for _, v := range entries {
		if !v.IsUpdateEntry() {
			return false
		}
	}
	return true
}

func (s *StateMachine) handle(batch []Commit) {
	for b := range batch {
		if batch[b].SnapshotAvailable || batch[b].SnapshotRequested {
			panic("trying to handle a snapshot request")
		}
		ents := batch[b].Entries
		if batchedEntryApply && allUpdateEntries(ents) {
			s.handleAllUpdateEntries(ents)
		} else {
			for i := range ents {
				notifyRead := b == len(batch)-1 && i == len(ents)-1
				s.handleCommitRec(ents[i], notifyRead)
			}
		}
	}
}

func (s *StateMachine) handleCommitRec(ent pb.Entry, lastInBatch bool) {
	// ConfChnage also go through the SM so the lastApplied value is updated
	if ent.IsConfigChange() {
		accepted := s.handleConfigChange(ent)
		s.node.ConfigChangeProcessed(ent.Key, accepted)
	} else {
		if !ent.IsSessionManaged() {
			if ent.IsEmpty() {
				s.handleNoOP(ent)
				s.node.ApplyUpdate(ent, 0, false, true, lastInBatch)
			} else {
				panic("not session managed, not empty")
			}
		} else {
			if ent.IsNewSessionRequest() {
				smResult := s.handleRegisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, smResult == 0, false, lastInBatch)
			} else if ent.IsEndOfSessionRequest() {
				smResult := s.handleUnregisterSession(ent)
				s.node.ApplyUpdate(ent, smResult, smResult == 0, false, lastInBatch)
			} else {
				smResult, ignored, rejected := s.handleUpdate(ent)
				if !ignored {
					s.node.ApplyUpdate(ent, smResult, rejected, ignored, lastInBatch)
				}
			}
		}
	}
	lastApplied := s.GetLastApplied()
	if lastApplied != ent.Index {
		plog.Panicf("unexpected last applied value, %d, %d", lastApplied, ent.Index)
	}
	if lastInBatch {
		s.setBatchedLastApplied(ent.Index)
	}
}

func (s *StateMachine) onUpdateApplied(ent pb.Entry,
	result uint64, ignored bool, rejected bool, lastInBatch bool) {
	if !ignored {
		s.node.ApplyUpdate(ent, result, rejected, ignored, lastInBatch)
	}
}

func (s *StateMachine) handleAllUpdateEntries(ents []pb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastIdx := len(ents) - 1
	for idx, entry := range ents {
		var session *Session
		var ok bool
		lastInBatch := idx == lastIdx
		s.updateLastApplied(entry.Index, entry.Term)
		if !entry.IsNoOPSession() {
			session, ok = s.sm.ClientRegistered(entry.ClientID)
			if !ok {
				s.onUpdateApplied(entry, 0, false, true, lastInBatch)
				continue
			}
			s.sm.UpdateRespondedTo(session, entry.RespondedTo)
			result, responded, updateRequired := s.sm.UpdateRequired(session,
				entry.SeriesID)
			if responded {
				s.onUpdateApplied(entry, 0, true, false, lastInBatch)
				continue
			}
			if !updateRequired {
				s.onUpdateApplied(entry, result, false, false, lastInBatch)
				continue
			}
		}
		if !entry.IsNoOPSession() && session == nil {
			panic("session is nil")
		}
		result := s.sm.Update(session, entry.SeriesID, entry.Cmd)
		s.onUpdateApplied(entry, result, false, false, lastInBatch)
	}
	if len(ents) > 0 {
		s.setBatchedLastApplied(ents[len(ents)-1].Index)
	}
}

func (s *StateMachine) isConfChangeUpToDate(cc pb.ConfigChange) bool {
	if !s.ordered || cc.Initialize {
		return true
	}
	if s.members.ConfigChangeId == cc.ConfigChangeId {
		return true
	}
	return false
}

func (s *StateMachine) isAddingRemovedNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode || cc.Type == pb.AddObserver {
		_, ok := s.members.Removed[cc.NodeID]
		return ok
	}
	return false
}

func addressEqual(addr1 string, addr2 string) bool {
	return strings.ToLower(strings.TrimSpace(addr1)) ==
		strings.ToLower(strings.TrimSpace(addr2))
}

func (s *StateMachine) isAddingExistingMember(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		plog.Infof("%s adding node %d:%s, existing members: %v",
			s.describe(), cc.NodeID, string(cc.Address), s.members.Addresses)
		for _, addr := range s.members.Addresses {
			if addressEqual(addr, string(cc.Address)) {
				return true
			}
		}
	}
	if cc.Type == pb.AddObserver {
		plog.Infof("%s adding observer %d:%s, existing members: %v",
			s.describe(), cc.NodeID, string(cc.Address), s.members.Addresses)
		for _, addr := range s.members.Observers {
			if addressEqual(addr, string(cc.Address)) {
				return true
			}
		}
	}
	return false
}

func (s *StateMachine) isAddingNodeAsObserver(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddObserver {
		_, ok := s.members.Addresses[cc.NodeID]
		return ok
	}
	return false
}

func (s *StateMachine) applyConfigChangeLocked(cc pb.ConfigChange,
	index uint64) {
	s.members.ConfigChangeId = index
	s.node.ApplyConfigChange(cc)
	switch cc.Type {
	case pb.AddNode:
		nodeAddr := string(cc.Address)
		if addr, ok := s.members.Observers[cc.NodeID]; ok {
			delete(s.members.Observers, cc.NodeID)
			if !addressEqual(nodeAddr, addr) {
				plog.Warningf("promoting observer, addr changed to %s, use %s",
					nodeAddr, addr)
			}
			nodeAddr = addr
		}
		s.members.Addresses[cc.NodeID] = nodeAddr
	case pb.AddObserver:
		if _, ok := s.members.Addresses[cc.NodeID]; ok {
			panic("not suppose to reach here")
		}
		s.members.Observers[cc.NodeID] = string(cc.Address)
	case pb.RemoveNode:
		delete(s.members.Addresses, cc.NodeID)
		delete(s.members.Observers, cc.NodeID)
		s.members.Removed[cc.NodeID] = true
	default:
		panic("unknown config change type")
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
	accepted := false
	s.mu.Lock()
	defer s.mu.Unlock()
	// order id requested by user
	ccid := cc.ConfigChangeId
	nodeBecomingObserver := s.isAddingNodeAsObserver(cc)
	alreadyMember := s.isAddingExistingMember(cc)
	addRemovedNode := s.isAddingRemovedNode(cc)
	upToDateCC := s.isConfChangeUpToDate(cc)
	s.updateLastApplied(ent.Index, ent.Term)
	if upToDateCC && !addRemovedNode && !alreadyMember && !nodeBecomingObserver {
		// current entry index, it will be recorded as the conf change id of the members
		s.applyConfigChangeLocked(cc, ent.Index)
		if cc.Type == pb.AddNode {
			plog.Infof("%s applied ConfChange Add ccid %d, node %s index %d address %s",
				s.describe(), ccid, logutil.NodeID(cc.NodeID),
				ent.Index, string(cc.Address))
		} else if cc.Type == pb.RemoveNode {
			plog.Infof("%s applied ConfChange Remove ccid %d, node %s, index %d",
				s.describe(), ccid, logutil.NodeID(cc.NodeID), ent.Index)
		} else if cc.Type == pb.AddObserver {
			plog.Infof("%s applied ConfChange Add Observer ccid %d, node %s index %d address %s",
				s.describe(), ccid, logutil.NodeID(cc.NodeID),
				ent.Index, string(cc.Address))
		} else {
			plog.Panicf("unknown cc.Type value")
		}
		accepted = true
	} else {
		if !upToDateCC {
			plog.Warningf("%s rejected out-of-order ConfChange ccid %d, type %s, index %d",
				s.describe(), ccid, cc.Type, ent.Index)
		} else if addRemovedNode {
			plog.Warningf("%s rejected adding removed node ccid %d, node id %d, index %d",
				s.describe(), ccid, cc.NodeID, ent.Index)
		} else if alreadyMember {
			plog.Warningf("%s rejected adding existing member to raft cluster ccid %d "+
				"node id %d, index %d, address %s",
				s.describe(), ccid, cc.NodeID, ent.Index, cc.Address)
		} else if nodeBecomingObserver {
			plog.Warningf("%s rejected adding existing member as observer ccid %d "+
				"node id %d, index %d, address %s",
				s.describe(), ccid, cc.NodeID, ent.Index, cc.Address)
		} else {
			plog.Panicf("config change rejected for unknown reasons")
		}
	}
	return accepted
}

func (s *StateMachine) handleRegisterSession(ent pb.Entry) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sm.RegisterClientID(ent.ClientID)
	if smResult == 0 {
		plog.Errorf("on %s register client failed, %v", s.describe(), ent)
	}
	s.updateLastApplied(ent.Index, ent.Term)
	return smResult
}

func (s *StateMachine) handleUnregisterSession(ent pb.Entry) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	smResult := s.sm.UnregisterClientID(ent.ClientID)
	if smResult == 0 {
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
func (s *StateMachine) handleUpdate(ent pb.Entry) (uint64, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result uint64
	var ok bool
	var session *Session
	s.updateLastApplied(ent.Index, ent.Term)
	if !ent.IsNoOPSession() {
		session, ok = s.sm.ClientRegistered(ent.ClientID)
		if !ok {
			// client is expected to crash
			return 0, false, true
		}
		s.sm.UpdateRespondedTo(session, ent.RespondedTo)
		result, responded, updateRequired := s.sm.UpdateRequired(session,
			ent.SeriesID)
		if responded {
			// should ignore. client is expected to timeout
			return 0, true, false
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
	result = s.sm.Update(session, ent.SeriesID, ent.Cmd)
	return result, false, false
}

func (s *StateMachine) describe() string {
	return logutil.DescribeSM(s.node.ClusterID(), s.node.NodeID())
}

func deepCopyMembership(m pb.Membership) pb.Membership {
	c := pb.Membership{
		ConfigChangeId: m.ConfigChangeId,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		Observers:      make(map[uint64]string),
	}
	for nid, addr := range m.Addresses {
		c.Addresses[nid] = addr
	}
	for nid := range m.Removed {
		c.Removed[nid] = true
	}
	for nid, addr := range m.Observers {
		c.Observers[nid] = addr
	}
	return c
}

func snapshotInfo(ss pb.Snapshot) string {
	return fmt.Sprintf("addresses %v, config change id %d",
		ss.Membership.Addresses, ss.Membership.ConfigChangeId)
}

func getSnapshotFiles(snapshot pb.Snapshot) []statemachine.SnapshotFile {
	sfl := make([]statemachine.SnapshotFile, 0)
	for _, f := range snapshot.Files {
		sf := statemachine.SnapshotFile{
			FileID:   f.FileId,
			Filepath: f.Filepath,
			Metadata: f.Metadata,
		}
		sfl = append(sfl, sf)
	}
	return sfl
}
