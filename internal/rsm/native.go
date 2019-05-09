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

	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
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
	SaveSnapshot(*SnapshotMeta,
		*SnapshotWriter, []byte, sm.ISnapshotFileCollection) (bool, uint64, error)
}

// ILoadableSM is the interface for types that can have its state restored from
// snapshots.
type ILoadableSM interface {
	RecoverFromSnapshot(uint64, *SnapshotReader, []sm.SnapshotFile) error
}

// ILoadableSessions is the interface for types that can load client session
// state from a snapshot.
type ILoadableSessions interface {
	LoadSessions(io.Reader, SnapshotVersion) error
}

// IManagedStateMachine is the interface used to manage data store.
type IManagedStateMachine interface {
	Open() (uint64, error)
	Update(*Session, pb.Entry) sm.Result
	BatchedUpdate([]sm.Entry) []sm.Entry
	Lookup([]byte) ([]byte, error)
	GetHash() uint64
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(*SnapshotMeta,
		*SnapshotWriter, []byte, sm.ISnapshotFileCollection) (bool, uint64, error)
	RecoverFromSnapshot(uint64, *SnapshotReader, []sm.SnapshotFile) error
	StreamSnapshot(interface{}, io.Writer) error
	Offloaded(From)
	Loaded(From)
	ConcurrentSnapshot() bool
	OnDiskStateMachine() bool
	StateMachineType() pb.StateMachineType
}

// ManagedStateMachineFactory is the factory function type for creating an
// IManagedStateMachine instance.
type ManagedStateMachineFactory func(clusterID uint64,
	nodeID uint64, stopc <-chan struct{}) IManagedStateMachine

// NativeStateMachine is the IManagedStateMachine object used to manage native
// data store in Golang.
type NativeStateMachine struct {
	sm   IStateMachine
	done <-chan struct{}
	mu   sync.RWMutex
	OffloadedStatus
}

// NewNativeStateMachine creates and returns a new NativeStateMachine object.
func NewNativeStateMachine(clusterID uint64, nodeID uint64, sm IStateMachine,
	done <-chan struct{}) IManagedStateMachine {
	s := &NativeStateMachine{
		sm:   sm,
		done: done,
	}
	s.clusterID = clusterID
	s.nodeID = nodeID
	return s
}

func (ds *NativeStateMachine) closeStateMachine() {
	ds.sm.Close()
}

// Open opens on disk state machine.
func (ds *NativeStateMachine) Open() (uint64, error) {
	return ds.sm.Open(ds.done)
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

// ConcurrentSnapshot returns a boolean flag to indicate whether the managed
// state machine instance is capable of doing concurrent snapshots.
func (ds *NativeStateMachine) ConcurrentSnapshot() bool {
	return ds.sm.ConcurrentSnapshot()
}

// OnDiskStateMachine returns a boolean flag indicating whether the state
// machine is an on disk state machine.
func (ds *NativeStateMachine) OnDiskStateMachine() bool {
	return ds.sm.OnDiskStateMachine()
}

// StateMachineType returns the state machine type.
func (ds *NativeStateMachine) StateMachineType() pb.StateMachineType {
	return ds.sm.StateMachineType()
}

// Update updates the data store.
func (ds *NativeStateMachine) Update(session *Session, e pb.Entry) sm.Result {
	if session != nil {
		_, ok := session.getResponse(RaftSeriesID(e.SeriesID))
		if ok {
			panic("already has response in session")
		}
	}
	entries := []sm.Entry{sm.Entry{Index: e.Index, Cmd: e.Cmd}}
	results := ds.sm.Update(entries)
	if len(results) != 1 {
		panic("len(results) != 1")
	}
	if session != nil {
		session.addResponse(RaftSeriesID(e.SeriesID), results[0].Result)
	}
	return results[0].Result
}

// BatchedUpdate applies committed entries in a batch to hide latency.
func (ds *NativeStateMachine) BatchedUpdate(ents []sm.Entry) []sm.Entry {
	il := len(ents)
	results := ds.sm.Update(ents)
	if len(results) != il {
		panic("unexpected result length")
	}
	return results
}

// Lookup queries the data store.
func (ds *NativeStateMachine) Lookup(data []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, ErrClusterClosed
	}
	v, err := ds.sm.Lookup(data)
	ds.mu.RUnlock()
	return v, err
}

// GetHash returns an integer value representing the state of the data store.
func (ds *NativeStateMachine) GetHash() uint64 {
	return ds.sm.GetHash()
}

// PrepareSnapshot makes preparation for concurrently taking snapshot.
func (ds *NativeStateMachine) PrepareSnapshot() (interface{}, error) {
	if !ds.ConcurrentSnapshot() {
		panic("state machine is not capable of concurrent snapshotting")
	}
	return ds.sm.PrepareSnapshot()
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *NativeStateMachine) SaveSnapshot(meta *SnapshotMeta,
	writer *SnapshotWriter, session []byte,
	collection sm.ISnapshotFileCollection) (bool, uint64, error) {
	if ds.sm.OnDiskStateMachine() && !meta.Request.IsExportedSnapshot() {
		sz, err := ds.saveDummySnapshot(writer, session)
		return true, sz, err
	}
	sz, err := ds.saveSnapshot(meta.Ctx, writer, session, collection)
	return false, sz, err
}

func (ds *NativeStateMachine) saveDummySnapshot(writer *SnapshotWriter,
	session []byte) (uint64, error) {
	_, err := writer.Write(session)
	if err != nil {
		return 0, err
	}
	if err = writer.Flush(); err != nil {
		return 0, err
	}
	if err := writer.SaveHeader(16, 0); err != nil {
		return 0, err
	}
	return writer.GetPayloadSize(16) + SnapshotHeaderSize, nil
}

func (ds *NativeStateMachine) saveSnapshot(
	ssctx interface{}, writer *SnapshotWriter, session []byte,
	collection sm.ISnapshotFileCollection) (uint64, error) {
	n, err := writer.Write(session)
	if err != nil {
		return 0, err
	}
	if n != len(session) {
		return 0, io.ErrShortWrite
	}
	smsz := uint64(len(session))
	cw := &countedWriter{w: writer}
	err = ds.sm.SaveSnapshot(ssctx, cw, collection, ds.done)
	if err != nil {
		return 0, err
	}
	if err = writer.Flush(); err != nil {
		return 0, err
	}
	sz := cw.total
	if err = writer.SaveHeader(smsz, sz); err != nil {
		return 0, err
	}
	actualSz := writer.GetPayloadSize(sz + smsz)
	return actualSz + SnapshotHeaderSize, nil
}

// StreamSnapshot creates and streams snapshot to a remote node.
func (ds *NativeStateMachine) StreamSnapshot(ssctx interface{},
	writer io.Writer) error {
	return ds.sm.SaveSnapshot(ssctx, writer, nil, ds.done)
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *NativeStateMachine) RecoverFromSnapshot(index uint64,
	reader *SnapshotReader,
	files []sm.SnapshotFile) error {
	return ds.sm.RecoverFromSnapshot(index, reader, files, ds.done)
}
