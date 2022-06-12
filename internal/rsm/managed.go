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
	"io"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	// ErrShardClosed indicates that the shard has been closed
	ErrShardClosed = errors.New("raft shard already closed")
)

// IStreamable is the interface for types that can be snapshot streamed.
type IStreamable interface {
	Stream(interface{}, io.Writer) error
}

// ISavable is the interface for types that can its content saved as snapshots.
type ISavable interface {
	Save(SSMeta, io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error)
}

// IRecoverable is the interface for types that can have its state restored from
// snapshots.
type IRecoverable interface {
	Recover(io.Reader, []sm.SnapshotFile) error
}

// ILoadable is the interface for types that can load client session
// state from a snapshot.
type ILoadable interface {
	LoadSessions(io.Reader, SSVersion) error
}

// IManagedStateMachine is the interface used for managed state machine. A
// managed state machine contains a user state machine plus its engine state.
type IManagedStateMachine interface {
	Open() (uint64, error)
	Update(sm.Entry) (sm.Result, error)
	BatchedUpdate([]sm.Entry) ([]sm.Entry, error)
	Lookup(interface{}) (interface{}, error)
	ConcurrentLookup(interface{}) (interface{}, error)
	NALookup([]byte) ([]byte, error)
	NAConcurrentLookup([]byte) ([]byte, error)
	Sync() error
	GetHash() (uint64, error)
	Prepare() (interface{}, error)
	Save(SSMeta, io.Writer, []byte, sm.ISnapshotFileCollection) (bool, error)
	Recover(io.Reader, []sm.SnapshotFile) error
	Stream(interface{}, io.Writer) error
	Offloaded() bool
	Loaded()
	Close() error
	DestroyedC() <-chan struct{}
	Concurrent() bool
	OnDisk() bool
	Type() pb.StateMachineType
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
type ManagedStateMachineFactory func(shardID uint64,
	replicaID uint64, stopc <-chan struct{}) IManagedStateMachine

// NativeSM is the IManagedStateMachine object used to manage native
// data store in Golang.
type NativeSM struct {
	sm   IStateMachine
	done <-chan struct{}
	OffloadedStatus
	ue     []sm.Entry
	config config.Config
	mu     sync.RWMutex
}

var _ IManagedStateMachine = (*NativeSM)(nil)
var _ ISavable = (*NativeSM)(nil)
var _ IStreamable = (*NativeSM)(nil)
var _ IRecoverable = (*NativeSM)(nil)

// NewNativeSM creates and returns a new NativeSM object.
func NewNativeSM(config config.Config, ism IStateMachine,
	done <-chan struct{}) *NativeSM {
	s := &NativeSM{
		config: config,
		sm:     ism,
		done:   done,
		ue:     make([]sm.Entry, 1),
	}
	s.OffloadedStatus.DestroyedC = make(chan struct{})
	s.OffloadedStatus.shardID = config.ShardID
	s.OffloadedStatus.replicaID = config.ReplicaID
	return s
}

// Open opens on disk state machine.
func (ds *NativeSM) Open() (uint64, error) {
	return ds.sm.Open(ds.done)
}

// Offloaded offloads the data store from a user component.
func (ds *NativeSM) Offloaded() bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.SetOffloaded() == 0
}

// Loaded marks the statemachine as loaded by the specified component.
func (ds *NativeSM) Loaded() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded()
}

// Close closes the underlying user state machine and set the destroyed flag.
func (ds *NativeSM) Close() error {
	if err := ds.sm.Close(); err != nil {
		return err
	}
	ds.SetDestroyed()
	return nil
}

// DestroyedC returns a chan struct{} used to indicate whether the SM has been
// fully offloaded.
func (ds *NativeSM) DestroyedC() <-chan struct{} {
	return ds.OffloadedStatus.DestroyedC
}

// Concurrent returns a boolean flag to indicate whether the managed state
// machine instance is capable of doing concurrent snapshots.
func (ds *NativeSM) Concurrent() bool {
	return ds.sm.Concurrent()
}

// OnDisk returns a boolean flag indicating whether the state machine is an on
// disk state machine.
func (ds *NativeSM) OnDisk() bool {
	return ds.sm.OnDisk()
}

// Type returns the state machine type.
func (ds *NativeSM) Type() pb.StateMachineType {
	return ds.sm.Type()
}

// Update updates the data store.
func (ds *NativeSM) Update(e sm.Entry) (sm.Result, error) {
	ds.ue[0] = e
	results, err := ds.sm.Update(ds.ue)
	if err != nil {
		return sm.Result{}, err
	}
	if len(results) != 1 {
		panic("len(results) != 1")
	}
	v := results[0].Result
	ds.ue[0] = sm.Entry{}
	return v, nil
}

// BatchedUpdate applies committed entries in a batch to hide latency.
func (ds *NativeSM) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	inputLen := len(ents)
	results, err := ds.sm.Update(ents)
	if err != nil {
		return nil, err
	}
	if len(results) != inputLen {
		panic("unexpected result length")
	}
	return results, nil
}

// Lookup queries the data store.
func (ds *NativeSM) Lookup(query interface{}) (interface{}, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if ds.destroyed {
		return nil, ErrShardClosed
	}
	return ds.sm.Lookup(query)
}

// ConcurrentLookup queries the data store without obtaining the NativeSM.mu.
func (ds *NativeSM) ConcurrentLookup(query interface{}) (interface{}, error) {
	return ds.sm.Lookup(query)
}

// NALookup queries the data store.
func (ds *NativeSM) NALookup(query []byte) ([]byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if ds.destroyed {
		return nil, ErrShardClosed
	}
	return ds.sm.NALookup(query)
}

// NAConcurrentLookup queries the data store without obtaining the NativeSM.mu.
func (ds *NativeSM) NAConcurrentLookup(query []byte) ([]byte, error) {
	return ds.sm.NALookup(query)
}

// Sync synchronizes state machine's in-core state with that on disk.
func (ds *NativeSM) Sync() error {
	return ds.sm.Sync()
}

// GetHash returns an integer value representing the state of the data store.
func (ds *NativeSM) GetHash() (uint64, error) {
	return ds.sm.GetHash()
}

// Prepare makes preparation for concurrently taking snapshot.
func (ds *NativeSM) Prepare() (interface{}, error) {
	return ds.sm.Prepare()
}

// Save saves the state of the data store to the specified writer.
func (ds *NativeSM) Save(meta SSMeta,
	w io.Writer, session []byte, c sm.ISnapshotFileCollection) (bool, error) {
	if ds.config.IsWitness || (ds.sm.OnDisk() && !meta.Request.Exported()) {
		return true, ds.saveDummy(w, session)
	}
	return false, ds.save(meta.Ctx, w, session, c)
}

func (ds *NativeSM) saveDummy(w io.Writer, session []byte) error {
	if !ds.config.IsWitness && !ds.sm.OnDisk() {
		panic("saveDummySnapshot called on non OnDiskStateMachine")
	}
	if _, err := w.Write(session); err != nil {
		return err
	}
	return nil
}

func (ds *NativeSM) save(ctx interface{},
	w io.Writer, session []byte, c sm.ISnapshotFileCollection) error {
	if _, err := w.Write(session); err != nil {
		return err
	}
	if err := ds.sm.Save(ctx, w, c, ds.done); err != nil {
		return err
	}
	return nil
}

// Stream creates and streams snapshot to a remote node.
func (ds *NativeSM) Stream(ctx interface{}, w io.Writer) error {
	return ds.save(ctx, w, GetEmptyLRUSession(), nil)
}

// Recover recovers the state of the data store from the specified reader.
func (ds *NativeSM) Recover(r io.Reader, files []sm.SnapshotFile) error {
	return ds.sm.Recover(r, files, ds.done)
}
