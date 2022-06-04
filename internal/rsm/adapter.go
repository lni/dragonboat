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

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// IStateMachine is an adapter interface for underlying sm.IStateMachine,
// sm.IConcurrentStateMachine and sm.IOnDIskStateMachine instances.
type IStateMachine interface {
	Open(<-chan struct{}) (uint64, error)
	Update(entries []sm.Entry) ([]sm.Entry, error)
	Lookup(query interface{}) (interface{}, error)
	NALookup(query []byte) ([]byte, error)
	Sync() error
	Prepare() (interface{}, error)
	Save(interface{},
		io.Writer, sm.ISnapshotFileCollection, <-chan struct{}) error
	Recover(io.Reader, []sm.SnapshotFile, <-chan struct{}) error
	Close() error
	GetHash() (uint64, error)
	Concurrent() bool
	OnDisk() bool
	Type() pb.StateMachineType
}

// InMemStateMachine is a regular state machine not capable of concurrent
// access from multiple goroutines.
type InMemStateMachine struct {
	sm sm.IStateMachine
	h  sm.IHash
	na sm.IExtended
}

var _ IStateMachine = (*InMemStateMachine)(nil)

// NewInMemStateMachine creates a new InMemStateMachine instance.
func NewInMemStateMachine(s sm.IStateMachine) *InMemStateMachine {
	i := &InMemStateMachine{sm: s}
	if h, ok := s.(sm.IHash); ok {
		i.h = h
	}
	if na, ok := s.(sm.IExtended); ok {
		i.na = na
	}
	return i
}

// Open opens the state machine.
func (i *InMemStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	panic("Open() called on InMemStateMachine")
}

// Update updates the state machine.
func (i *InMemStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if len(entries) != 1 {
		panic("len(entries) != 1")
	}
	var err error
	entries[0].Result, err = i.sm.Update(entries[0])
	return entries, errors.WithStack(err)
}

// Lookup queries the state machine.
func (i *InMemStateMachine) Lookup(query interface{}) (interface{}, error) {
	return i.sm.Lookup(query)
}

// NALookup queries the state machine.
func (i *InMemStateMachine) NALookup(query []byte) ([]byte, error) {
	if i.na == nil {
		return nil, sm.ErrNotImplemented
	}
	return i.na.NALookup(query)
}

// Sync synchronizes all in-core state with that on disk.
func (i *InMemStateMachine) Sync() error {
	panic("Sync not implemented in InMemStateMachine")
}

// Prepare makes preparations for taking concurrent snapshot.
func (i *InMemStateMachine) Prepare() (interface{}, error) {
	panic("Prepare not implemented in InMemStateMachine")
}

// Save saves the snapshot.
func (i *InMemStateMachine) Save(ctx interface{},
	w io.Writer, fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	if ctx != nil {
		panic("snapshot ctx is not nil")
	}
	return errors.WithStack(i.sm.SaveSnapshot(w, fc, stopc))
}

// Recover recovers the state machine from a snapshot.
func (i *InMemStateMachine) Recover(r io.Reader,
	fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	return errors.WithStack(i.sm.RecoverFromSnapshot(r, fs, stopc))
}

// Close closes the state machine.
func (i *InMemStateMachine) Close() error {
	return errors.WithStack(i.sm.Close())
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (i *InMemStateMachine) GetHash() (uint64, error) {
	if i.h == nil {
		return 0, sm.ErrNotImplemented
	}
	h, err := i.h.GetHash()
	return h, errors.WithStack(err)
}

// Concurrent returns a boolean flag indicating whether the state machine is
// capable of taking concurrent snapshot.
func (i *InMemStateMachine) Concurrent() bool {
	return false
}

// OnDisk returns a boolean flag indicating whether this is an on disk state
// machine.
func (i *InMemStateMachine) OnDisk() bool {
	return false
}

// Type returns the type of the state machine.
func (i *InMemStateMachine) Type() pb.StateMachineType {
	return pb.RegularStateMachine
}

// ConcurrentStateMachine is an IStateMachine type capable of taking concurrent
// snapshots.
type ConcurrentStateMachine struct {
	sm sm.IConcurrentStateMachine
	h  sm.IHash
	na sm.IExtended
}

// NewConcurrentStateMachine creates a new ConcurrentStateMachine instance.
func NewConcurrentStateMachine(s sm.IConcurrentStateMachine) *ConcurrentStateMachine {
	v := &ConcurrentStateMachine{sm: s}
	if h, ok := s.(sm.IHash); ok {
		v.h = h
	}
	if na, ok := s.(sm.IExtended); ok {
		v.na = na
	}
	return v
}

// Open opens the state machine.
func (s *ConcurrentStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	panic("Open() not implemented ConcurrentStateMachine")
}

// Update updates the state machine.
func (s *ConcurrentStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	results, err := s.sm.Update(entries)
	return results, errors.WithStack(err)
}

// Lookup queries the state machine.
func (s *ConcurrentStateMachine) Lookup(query interface{}) (interface{}, error) {
	return s.sm.Lookup(query)
}

// NALookup queries the state machine.
func (s *ConcurrentStateMachine) NALookup(query []byte) ([]byte, error) {
	if s.na == nil {
		return nil, sm.ErrNotImplemented
	}
	return s.na.NALookup(query)
}

// Sync synchronizes all in-core state with that on disk.
func (s *ConcurrentStateMachine) Sync() error {
	panic("Sync not implemented in ConcurrentStateMachine")
}

// Prepare makes preparations for taking concurrent snapshot.
func (s *ConcurrentStateMachine) Prepare() (interface{}, error) {
	results, err := s.sm.PrepareSnapshot()
	return results, errors.WithStack(err)
}

// Save saves the snapshot.
func (s *ConcurrentStateMachine) Save(ctx interface{},
	w io.Writer, fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	return errors.WithStack(s.sm.SaveSnapshot(ctx, w, fc, stopc))
}

// Recover recovers the state machine from a snapshot.
func (s *ConcurrentStateMachine) Recover(r io.Reader,
	fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	return errors.WithStack(s.sm.RecoverFromSnapshot(r, fs, stopc))
}

// Close closes the state machine.
func (s *ConcurrentStateMachine) Close() error {
	return errors.WithStack(s.sm.Close())
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (s *ConcurrentStateMachine) GetHash() (uint64, error) {
	if s.h == nil {
		return 0, sm.ErrNotImplemented
	}
	h, err := s.h.GetHash()
	return h, errors.WithStack(err)
}

// Concurrent returns a boolean flag indicating whether the state machine is
// capable of taking concurrent snapshot.
func (s *ConcurrentStateMachine) Concurrent() bool {
	return true
}

// OnDisk returns a boolean flag indicating whether this is a on disk state
// machine.
func (s *ConcurrentStateMachine) OnDisk() bool {
	return false
}

// Type returns the type of the state machine.
func (s *ConcurrentStateMachine) Type() pb.StateMachineType {
	return pb.ConcurrentStateMachine
}

// ITestFS is an interface implemented by test SMs.
type ITestFS interface {
	SetTestFS(fs config.IFS)
}

// OnDiskStateMachine is the type to represent an on disk state machine.
type OnDiskStateMachine struct {
	sm     sm.IOnDiskStateMachine
	h      sm.IHash
	na     sm.IExtended
	opened bool
}

// NewOnDiskStateMachine creates and returns an on disk state machine.
func NewOnDiskStateMachine(s sm.IOnDiskStateMachine) *OnDiskStateMachine {
	r := &OnDiskStateMachine{sm: s}
	if h, ok := s.(sm.IHash); ok {
		r.h = h
	}
	if na, ok := s.(sm.IExtended); ok {
		r.na = na
	}
	return r
}

// SetTestFS injects the specified fs to the test SM.
func (s *OnDiskStateMachine) SetTestFS(fs config.IFS) {
	if tfs, ok := s.sm.(ITestFS); ok {
		plog.Infof("the underlying SM support test fs injection")
		tfs.SetTestFS(fs)
	}
}

// Open opens the state machine.
func (s *OnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	if s.opened {
		panic("Open invoked again")
	}
	s.opened = true
	applied, err := s.sm.Open(stopc)
	return applied, errors.WithStack(err)
}

// Update updates the state machine.
func (s *OnDiskStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	s.ensureOpened()
	results, err := s.sm.Update(entries)
	return results, errors.WithStack(err)
}

// Lookup queries the state machine.
func (s *OnDiskStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.ensureOpened()
	return s.sm.Lookup(query)
}

// NALookup queries the state machine.
func (s *OnDiskStateMachine) NALookup(query []byte) ([]byte, error) {
	s.ensureOpened()
	return s.na.NALookup(query)
}

// Sync synchronizes all in-core state with that on disk.
func (s *OnDiskStateMachine) Sync() error {
	s.ensureOpened()
	return errors.WithStack(s.sm.Sync())
}

// Prepare makes preparations for taking concurrent snapshot.
func (s *OnDiskStateMachine) Prepare() (interface{}, error) {
	s.ensureOpened()
	results, err := s.sm.PrepareSnapshot()
	return results, errors.WithStack(err)
}

// Save saves the snapshot.
func (s *OnDiskStateMachine) Save(ctx interface{},
	w io.Writer, fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	s.ensureOpened()
	return errors.WithStack(s.sm.SaveSnapshot(ctx, w, stopc))
}

// Recover recovers the state machine from a snapshot.
func (s *OnDiskStateMachine) Recover(r io.Reader,
	fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	s.ensureOpened()
	return errors.WithStack(s.sm.RecoverFromSnapshot(r, stopc))
}

// Close closes the state machine.
func (s *OnDiskStateMachine) Close() error {
	return errors.WithStack(s.sm.Close())
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (s *OnDiskStateMachine) GetHash() (uint64, error) {
	s.ensureOpened()
	if s.h == nil {
		return 0, sm.ErrNotImplemented
	}
	h, err := s.h.GetHash()
	return h, errors.WithStack(err)
}

// Concurrent returns a boolean flag indicating whether the state machine is
// capable of taking concurrent snapshot.
func (s *OnDiskStateMachine) Concurrent() bool {
	return true
}

// OnDisk returns a boolean flag indicating whether this is an on disk state
// machine.
func (s *OnDiskStateMachine) OnDisk() bool {
	return true
}

// Type returns the type of the state machine.
func (s *OnDiskStateMachine) Type() pb.StateMachineType {
	return pb.OnDiskStateMachine
}

func (s *OnDiskStateMachine) ensureOpened() {
	if !s.opened {
		panic("not opened")
	}
}
