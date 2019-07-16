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
	"errors"
	"io"
	"sync"

	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	// ErrClusterClosed indicates that the cluster has been closed
	ErrClusterClosed = errors.New("raft cluster already closed")
)

// IStreamable is the interface for types that can be snapshot streamed.
type IStreamable interface {
	StreamSnapshot(interface{}, io.Writer) error
}

// ISavable is the interface for types that can its content saved as snapshots.
type ISavable interface {
	SaveSnapshot(*SSMeta,
		io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error)
}

// ILoadableSM is the interface for types that can have its state restored from
// snapshots.
type ILoadableSM interface {
	RecoverFromSnapshot(io.Reader, []sm.SnapshotFile) error
}

// ILoadableSessions is the interface for types that can load client session
// state from a snapshot.
type ILoadableSessions interface {
	LoadSessions(io.Reader, SSVersion) error
}

// IManagedStateMachine is the interface used to manage data store.
type IManagedStateMachine interface {
	Open() (uint64, error)
	Update(*Session, pb.Entry) (sm.Result, error)
	BatchedUpdate([]sm.Entry) ([]sm.Entry, error)
	Lookup(interface{}) (interface{}, error)
	NALookup([]byte) ([]byte, error)
	Sync() error
	GetHash() (uint64, error)
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(*SSMeta,
		io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error)
	RecoverFromSnapshot(io.Reader, []sm.SnapshotFile) error
	StreamSnapshot(interface{}, io.Writer) error
	Offloaded(From)
	Loaded(From)
	ConcurrentSnapshot() bool
	OnDiskStateMachine() bool
	StateMachineType() pb.StateMachineType
}

type countedWriter struct {
	w     io.Writer
	total uint64
}

func (cw *countedWriter) Write(data []byte) (int, error) {
	n, err := cw.w.Write(data)
	if err != nil {
		return 0, err
	}
	cw.total = cw.total + uint64(n)
	return n, nil
}

// ManagedStateMachineFactory is the factory function type for creating an
// IManagedStateMachine instance.
type ManagedStateMachineFactory func(clusterID uint64,
	nodeID uint64, stopc <-chan struct{}) IManagedStateMachine

// NativeSM is the IManagedStateMachine object used to manage native
// data store in Golang.
type NativeSM struct {
	sm   IStateMachine
	done <-chan struct{}
	ue   []sm.Entry
	mu   sync.RWMutex
	OffloadedStatus
}

// NewNativeSM creates and returns a new NativeSM object.
func NewNativeSM(clusterID uint64, nodeID uint64, ism IStateMachine,
	done <-chan struct{}) IManagedStateMachine {
	s := &NativeSM{
		sm:   ism,
		done: done,
		ue:   make([]sm.Entry, 1),
	}
	s.OffloadedStatus.clusterID = clusterID
	s.OffloadedStatus.nodeID = nodeID
	return s
}

// Open opens on disk state machine.
func (ds *NativeSM) Open() (uint64, error) {
	return ds.sm.Open(ds.done)
}

// Offloaded offloads the data store from the specified part of the system.
func (ds *NativeSM) Offloaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		if err := ds.sm.Close(); err != nil {
			panic(err)
		}
		ds.SetDestroyed()
	}
}

// Loaded marks the statemachine as loaded by the specified component.
func (ds *NativeSM) Loaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// ConcurrentSnapshot returns a boolean flag to indicate whether the managed
// state machine instance is capable of doing concurrent snapshots.
func (ds *NativeSM) ConcurrentSnapshot() bool {
	return ds.sm.ConcurrentSnapshot()
}

// OnDiskStateMachine returns a boolean flag indicating whether the state
// machine is an on disk state machine.
func (ds *NativeSM) OnDiskStateMachine() bool {
	return ds.sm.OnDiskStateMachine()
}

// StateMachineType returns the state machine type.
func (ds *NativeSM) StateMachineType() pb.StateMachineType {
	return ds.sm.StateMachineType()
}

// Update updates the data store.
func (ds *NativeSM) Update(session *Session,
	e pb.Entry) (sm.Result, error) {
	if session != nil {
		_, ok := session.getResponse(RaftSeriesID(e.SeriesID))
		if ok {
			panic("already has response in session")
		}
	}
	ds.ue[0] = sm.Entry{Index: e.Index, Cmd: e.Cmd}
	results, err := ds.sm.Update(ds.ue)
	ds.ue[0].Cmd = nil
	if err != nil {
		return sm.Result{}, err
	}
	if len(results) != 1 {
		panic("len(results) != 1")
	}
	if session != nil {
		session.addResponse(RaftSeriesID(e.SeriesID), results[0].Result)
	}
	return results[0].Result, nil
}

// BatchedUpdate applies committed entries in a batch to hide latency.
func (ds *NativeSM) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	il := len(ents)
	results, err := ds.sm.Update(ents)
	if err != nil {
		return nil, err
	}
	if len(results) != il {
		panic("unexpected result length")
	}
	return results, nil
}

// Lookup queries the data store.
func (ds *NativeSM) Lookup(query interface{}) (interface{}, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	v, err := ds.sm.Lookup(query)
	ds.mu.RUnlock()
	return v, err
}

// NALookup queries the data store.
func (ds *NativeSM) NALookup(query []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	v, err := ds.sm.NALookup(query)
	ds.mu.RUnlock()
	return v, err
}

// Sync synchronizes state machine's in-core state with that on disk.
func (ds *NativeSM) Sync() error {
	if !ds.sm.OnDiskStateMachine() {
		panic("sync called on non-ondisk SM")
	}
	return ds.sm.Sync()
}

// GetHash returns an integer value representing the state of the data store.
func (ds *NativeSM) GetHash() (uint64, error) {
	return ds.sm.GetHash()
}

// PrepareSnapshot makes preparation for concurrently taking snapshot.
func (ds *NativeSM) PrepareSnapshot() (interface{}, error) {
	if !ds.ConcurrentSnapshot() {
		panic("state machine is not capable of concurrent snapshotting")
	}
	return ds.sm.PrepareSnapshot()
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *NativeSM) SaveSnapshot(meta *SSMeta,
	w io.Writer, session []byte,
	collection sm.ISnapshotFileCollection) (bool, error) {
	if ds.sm.OnDiskStateMachine() && !meta.Request.IsExportedSnapshot() {
		return true, ds.saveDummySnapshot(w, session)
	}
	return false, ds.saveSnapshot(meta.Ctx, w, session, collection)
}

func (ds *NativeSM) saveDummySnapshot(w io.Writer, session []byte) error {
	if !ds.sm.OnDiskStateMachine() {
		panic("saveDummySnapshot called on non OnDiskStateMachine")
	}
	if _, err := w.Write(session); err != nil {
		return err
	}
	return nil
}

func (ds *NativeSM) saveSnapshot(ssctx interface{},
	w io.Writer, session []byte, collection sm.ISnapshotFileCollection) error {
	if _, err := w.Write(session); err != nil {
		return err
	}
	if err := ds.sm.SaveSnapshot(ssctx, w, collection, ds.done); err != nil {
		return err
	}
	return nil
}

// StreamSnapshot creates and streams snapshot to a remote node.
func (ds *NativeSM) StreamSnapshot(ssctx interface{}, w io.Writer) error {
	if _, err := w.Write(GetEmptyLRUSession()); err != nil {
		return err
	}
	return ds.sm.SaveSnapshot(ssctx, w, nil, ds.done)
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *NativeSM) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile) error {
	return ds.sm.RecoverFromSnapshot(r, files, ds.done)
}
