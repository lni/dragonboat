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

package rsm

import (
	"errors"
	"io"
	"sync"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/statemachine"
)

var (
	// ErrClusterClosed indicates that the cluster has been closed
	ErrClusterClosed = errors.New("raft cluster already closed")
)

// From identifies a component in the system.
type From uint64

var (
	// LRUMaxSessionCount is the largest number of client sessions that can be
	// concurrently managed by a LRUSession instance.
	LRUMaxSessionCount = settings.Hard.LRUMaxSessionCount
)

const (
	// FromNodeHost indicates the data store has been loaded by or offloaded from
	// nodehost.
	FromNodeHost From = iota
	// FromStepWorker indicates that the data store has been loaded by or
	// offloaded from the step worker.
	FromStepWorker
	// FromCommitWorker indicates that the data store has been loaded by or
	// offloaded from the commit worker.
	FromCommitWorker
	// FromSnapshotWorker indicates that the data store has been loaded by or
	// offloaded from the snapshot worker.
	FromSnapshotWorker
)

// OffloadedStatus is used for tracking whether the managed data store has been
// offloaded from various system components.
type OffloadedStatus struct {
	readyToDestroy              bool
	destroyed                   bool
	offloadedFromNodeHost       bool
	offloadedFromStepWorker     bool
	offloadedFromCommitWorker   bool
	offloadedFromSnapshotWorker bool
	loadedByStepWorker          bool
	loadedByCommitWorker        bool
	loadedBySnapshotWorker      bool
}

// ReadyToDestroy returns a boolean value indicating whether the the managed data
// store is ready to be destroyed.
func (o *OffloadedStatus) ReadyToDestroy() bool {
	return o.readyToDestroy
}

// Destroyed returns a boolean value indicating whether the belonging object
// has been destroyed.
func (o *OffloadedStatus) Destroyed() bool {
	return o.destroyed
}

// SetDestroyed set the destroyed flag to be true
func (o *OffloadedStatus) SetDestroyed() {
	o.destroyed = true
}

// SetLoaded marks the managed data store as loaded from the specified
// component.
func (o *OffloadedStatus) SetLoaded(from From) {
	if o.offloadedFromNodeHost {
		if from == FromStepWorker ||
			from == FromCommitWorker ||
			from == FromSnapshotWorker {
			plog.Panicf("loaded from %v after offloaded from nodehost", from)
		}
	}
	if from == FromNodeHost {
		panic("not suppose to get loaded notification from nodehost")
	} else if from == FromStepWorker {
		o.loadedByStepWorker = true
	} else if from == FromCommitWorker {
		o.loadedByCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.loadedBySnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
}

// SetOffloaded marks the managed data store as offloaded from the specified
// component.
func (o *OffloadedStatus) SetOffloaded(from From) {
	if from == FromNodeHost {
		o.offloadedFromNodeHost = true
	} else if from == FromStepWorker {
		o.offloadedFromStepWorker = true
	} else if from == FromCommitWorker {
		o.offloadedFromCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.offloadedFromSnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
	if from == FromNodeHost {
		if !o.loadedByStepWorker {
			o.offloadedFromStepWorker = true
		}
		if !o.loadedByCommitWorker {
			o.offloadedFromCommitWorker = true
		}
		if !o.loadedBySnapshotWorker {
			o.offloadedFromSnapshotWorker = true
		}
	}
	if o.offloadedFromNodeHost &&
		o.offloadedFromCommitWorker &&
		o.offloadedFromSnapshotWorker &&
		o.offloadedFromStepWorker {
		o.readyToDestroy = true
	}
}

// IManagedStateMachine is the interface used to manage data store.
type IManagedStateMachine interface {
	GetSessionHash() uint64
	UpdateRespondedTo(*Session, uint64)
	UnregisterClientID(clientID uint64) uint64
	RegisterClientID(clientID uint64) uint64
	ClientRegistered(clientID uint64) (*Session, bool)
	UpdateRequired(*Session, uint64) (uint64, bool, bool)
	Update(*Session, uint64, []byte) uint64
	Lookup([]byte) ([]byte, error)
	GetHash() uint64
	SaveSnapshot(string, statemachine.ISnapshotFileCollection) (uint64, error)
	RecoverFromSnapshot(string, []statemachine.SnapshotFile) error
	Offloaded(From)
	Loaded(From)
}

// ManagedStateMachineFactory is the factory function type for creating an
// IManagedStateMachine instance.
type ManagedStateMachineFactory func(clusterID uint64,
	nodeID uint64, stopc <-chan struct{}) IManagedStateMachine

// SessionManager is the wrapper struct that implements client session related
// functionalites used in the IManagedStateMachine interface.
type SessionManager struct {
	sessions *lrusession
}

// NewSessionManager returns a new SessionManager instance.
func NewSessionManager() SessionManager {
	return SessionManager{
		sessions: newLRUSession(LRUMaxSessionCount),
	}
}

// GetSessionHash returns an uint64 integer representing the state of the
// session manager.
func (ds *SessionManager) GetSessionHash() uint64 {
	return ds.sessions.getHash()
}

// UpdateRespondedTo updates the responded to value of the specified
// client session.
func (ds *SessionManager) UpdateRespondedTo(session *Session,
	respondedTo uint64) {
	session.clearTo(RaftSeriesID(respondedTo))
}

// RegisterClientID registers a new client, it returns the input client id
// if it is previously unknown, or 0 when the client has already been
// registered.
func (ds *SessionManager) RegisterClientID(clientID uint64) uint64 {
	es, ok := ds.sessions.getSession(RaftClientID(clientID))
	if ok {
		if es.ClientID != RaftClientID(clientID) {
			plog.Panicf("returned an expected session, got id %d, want %d",
				es.ClientID, clientID)
		}
		plog.Warningf("client ID %d already exist", clientID)
		return 0
	}
	s := newSession(RaftClientID(clientID))
	ds.sessions.addSession(RaftClientID(clientID), *s)
	return clientID
}

// UnregisterClientID removes the specified client session from the system.
// It returns the client id if the client is successfully removed, or 0
// if the client session does not exist.
func (ds *SessionManager) UnregisterClientID(clientID uint64) uint64 {
	es, ok := ds.sessions.getSession(RaftClientID(clientID))
	if !ok {
		return 0
	}
	if es.ClientID != RaftClientID(clientID) {
		plog.Panicf("returned an expected session, got id %d, want %d",
			es.ClientID, clientID)
	}
	ds.sessions.delSession(RaftClientID(clientID))
	return clientID
}

// ClientRegistered returns whether the specified client exists in the system.
func (ds *SessionManager) ClientRegistered(clientID uint64) (*Session, bool) {
	es, ok := ds.sessions.getSession(RaftClientID(clientID))
	if ok {
		if es.ClientID != RaftClientID(clientID) {
			plog.Panicf("returned an expected session, got id %d, want %d",
				es.ClientID, clientID)
		}
	}
	return es, ok
}

// UpdateRequired return a tuple of request result, responded before,
// update required.
func (ds *SessionManager) UpdateRequired(session *Session,
	seriesID uint64) (uint64, bool, bool) {
	if session.hasResponded(RaftSeriesID(seriesID)) {
		return 0, true, false
	}
	v, ok := session.getResponse(RaftSeriesID(seriesID))
	if ok {
		return v, false, false
	}
	return 0, false, true
}

// MustHaveClientSeries checks whether the session manager contains a client
// session identified as clientID and whether it has seriesID responded.
func (ds *SessionManager) MustHaveClientSeries(session *Session,
	seriesID uint64) {
	_, ok := session.getResponse(RaftSeriesID(seriesID))
	if ok {
		panic("already has response in session")
	}
}

// AddResponse adds the specified result to the session.
func (ds *SessionManager) AddResponse(session *Session,
	seriesID uint64, result uint64) {
	session.addResponse(RaftSeriesID(seriesID), result)
}

// SaveSessions saves the sessions to the provided io.writer.
func (ds *SessionManager) SaveSessions(writer io.Writer) (uint64, error) {
	return ds.sessions.save(writer)
}

// LoadSessions loads and restores sessions from io.Reader.
func (ds *SessionManager) LoadSessions(reader io.Reader) error {
	return ds.sessions.load(reader)
}

// NativeStateMachine is the IManagedStateMachine object used to manage native
// data store in Golang.
type NativeStateMachine struct {
	dataStore statemachine.IStateMachine
	done      <-chan struct{}
	// see dragonboat-doc on how this RWMutex can be entirely avoided
	mu sync.RWMutex
	OffloadedStatus
	SessionManager
}

// NewNativeStateMachine creates and returns a new NativeStateMachine object.
func NewNativeStateMachine(ds statemachine.IStateMachine,
	done <-chan struct{}) IManagedStateMachine {
	s := &NativeStateMachine{
		dataStore:      ds,
		done:           done,
		SessionManager: NewSessionManager(),
	}
	return s
}

func (ds *NativeStateMachine) closeStateMachine() {
	ds.dataStore.Close()
}

// Offloaded offloads the data store from the specified part of the system.
func (ds *NativeStateMachine) Offloaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.closeStateMachine()
		ds.SetDestroyed()
	}
}

// Loaded marks the statemachine as loaded by the specified component.
func (ds *NativeStateMachine) Loaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// Update updates the data store.
func (ds *NativeStateMachine) Update(session *Session,
	seriesID uint64, data []byte) uint64 {
	if session != nil {
		_, ok := session.getResponse(RaftSeriesID(seriesID))
		if ok {
			panic("already has response in session")
		}
	}
	v := ds.dataStore.Update(data)
	if session != nil {
		session.addResponse(RaftSeriesID(seriesID), v)
	}
	return v
}

// Lookup queries the data store.
func (ds *NativeStateMachine) Lookup(data []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	v := ds.dataStore.Lookup(data)
	ds.mu.RUnlock()
	return v, nil
}

// GetHash returns an integer value representing the state of the data store.
func (ds *NativeStateMachine) GetHash() uint64 {
	return ds.dataStore.GetHash()
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *NativeStateMachine) SaveSnapshot(fp string,
	collection statemachine.ISnapshotFileCollection) (uint64, error) {
	writer, err := NewSnapshotWriter(fp)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = writer.Close()
	}()
	smsz, err := ds.sessions.save(writer)
	if err != nil {
		return 0, err
	}
	sz, err := ds.dataStore.SaveSnapshot(writer, collection, ds.done)
	if err != nil {
		return 0, err
	}
	if err = writer.SaveHeader(smsz, sz); err != nil {
		return 0, err
	}
	return sz + smsz + SnapshotHeaderSize, nil
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *NativeStateMachine) RecoverFromSnapshot(fp string,
	files []statemachine.SnapshotFile) (err error) {
	reader, err := NewSnapshotReader(fp)
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
	reader.ValidateHeader(header)
	if err = ds.sessions.load(reader); err != nil {
		return err
	}
	if err = ds.dataStore.RecoverFromSnapshot(reader, files, ds.done); err != nil {
		plog.Errorf("statemachine.RecoverFromSnapshot returned %v", err)
		return err
	}
	reader.ValidatePayload(header)
	return err
}
