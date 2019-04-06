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
	"github.com/lni/dragonboat/internal/utils/logutil"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
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

var fromNames = [...]string{
	"FromNodeHost",
	"FromStepWorker",
	"FromCommitWorker",
	"FromSnapshotWorker",
}

func (f From) String() string {
	return fromNames[uint64(f)]
}

// OffloadedStatus is used for tracking whether the managed data store has been
// offloaded from various system components.
type OffloadedStatus struct {
	clusterID                   uint64
	nodeID                      uint64
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
	plog.Infof("%s offload status %t,%t,%t,%t, loaded status %t,%t,%t",
		o.describe(), o.offloadedFromNodeHost, o.offloadedFromCommitWorker,
		o.offloadedFromSnapshotWorker, o.offloadedFromStepWorker,
		o.loadedByStepWorker, o.loadedByCommitWorker, o.loadedBySnapshotWorker)
	if o.offloadedFromNodeHost &&
		o.offloadedFromCommitWorker &&
		o.offloadedFromSnapshotWorker &&
		o.offloadedFromStepWorker {
		o.readyToDestroy = true
	}
}

func (o *OffloadedStatus) describe() string {
	return logutil.DescribeNode(o.clusterID, o.nodeID)
}

type IStreamable interface {
	StreamSnapshot(interface{}, io.Writer) error
}

type ISavable interface {
	SaveSnapshot(interface{},
		*SnapshotWriter, []byte, sm.ISnapshotFileCollection) (uint64, bool, error)
}

type ILoadableSM interface {
	RecoverFromSnapshot(uint64, *SnapshotReader, []sm.SnapshotFile) error
}

type ILoadableSessions interface {
	LoadSessions(reader io.Reader) error
}

// IManagedStateMachine is the interface used to manage data store.
type IManagedStateMachine interface {
	Open() (uint64, error)
	Update(*Session, pb.Entry) uint64
	BatchedUpdate([]sm.Entry) []sm.Entry
	Lookup([]byte) ([]byte, error)
	GetHash() uint64
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(interface{},
		*SnapshotWriter, []byte, sm.ISnapshotFileCollection) (uint64, bool, error)
	RecoverFromSnapshot(uint64, *SnapshotReader, []sm.SnapshotFile) error
	StreamSnapshot(interface{}, io.Writer) error
	Offloaded(From)
	Loaded(From)
	ConcurrentSnapshot() bool
	OnDiskStateMachine() bool
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

func (ds *NativeStateMachine) OnDiskStateMachine() bool {
	return ds.sm.OnDiskStateMachine()
}

// Update updates the data store.
func (ds *NativeStateMachine) Update(session *Session, e pb.Entry) uint64 {
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
func (ds *NativeStateMachine) SaveSnapshot(
	ssctx interface{}, writer *SnapshotWriter, session []byte,
	collection sm.ISnapshotFileCollection) (uint64, bool, error) {
	if ds.sm.OnDiskStateMachine() {
		return ds.saveDummySnapshot(writer, session)
	}
	return ds.saveSnapshot(ssctx, writer, session, collection)
}

func (ds *NativeStateMachine) saveDummySnapshot(writer *SnapshotWriter,
	session []byte) (uint64, bool, error) {
	dummy := true
	_, err := writer.Write(session)
	if err != nil {
		return 0, dummy, err
	}
	if err = writer.Flush(); err != nil {
		return 0, dummy, err
	}
	if err := writer.SaveHeader(16, 0); err != nil {
		return 0, dummy, err
	}
	return writer.GetPayloadSize(16) + SnapshotHeaderSize, dummy, nil
}

func (ds *NativeStateMachine) saveSnapshot(
	ssctx interface{}, writer *SnapshotWriter, session []byte,
	collection sm.ISnapshotFileCollection) (uint64, bool, error) {
	dummy := false
	n, err := writer.Write(session)
	if err != nil {
		return 0, dummy, err
	}
	if n != len(session) {
		return 0, dummy, io.ErrShortWrite
	}
	smsz := uint64(len(session))
	sz, err := ds.sm.SaveSnapshot(ssctx, writer, collection, ds.done)
	if err != nil {
		return 0, dummy, err
	}
	if err = writer.Flush(); err != nil {
		return 0, dummy, err
	}
	if err = writer.SaveHeader(smsz, sz); err != nil {
		return 0, dummy, err
	}
	actualSz := writer.GetPayloadSize(sz + smsz)
	return actualSz + SnapshotHeaderSize, dummy, nil
}

func (ds *NativeStateMachine) StreamSnapshot(ssctx interface{},
	writer io.Writer) error {
	_, err := ds.sm.SaveSnapshot(ssctx, writer, nil, ds.done)
	return err
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *NativeStateMachine) RecoverFromSnapshot(index uint64,
	reader *SnapshotReader,
	files []sm.SnapshotFile) error {
	return ds.sm.RecoverFromSnapshot(index, reader, files, ds.done)
}
