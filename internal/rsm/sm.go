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
	"io"

	sm "github.com/lni/dragonboat/statemachine"
)

// IStateMachine is an adapter interface for underlying IStateMachine or
// IConcurrentStateMachine instances.
type IStateMachine interface {
	Open(<-chan struct{}) (uint64, error)
	Update(entries []sm.Entry) []sm.Entry
	Lookup(query []byte) ([]byte, error)
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(interface{},
		io.Writer,
		sm.ISnapshotFileCollection, <-chan struct{}) error
	RecoverFromSnapshot(uint64, io.Reader, []sm.SnapshotFile, <-chan struct{}) error
	Close()
	GetHash() uint64
	ConcurrentSnapshot() bool
	OnDiskStateMachine() bool
}

// RegularStateMachine is a regular state machine not capable of taking
// concurrent snapshots.
type RegularStateMachine struct {
	sm sm.IStateMachine
}

// NewRegularStateMachine creates a new RegularStateMachine instance.
func NewRegularStateMachine(sm sm.IStateMachine) *RegularStateMachine {
	return &RegularStateMachine{sm: sm}
}

// Open opens the state machine.
func (sm *RegularStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	panic("Open() called on RegularStateMachine")
}

// Update updates the state machine.
func (sm *RegularStateMachine) Update(entries []sm.Entry) []sm.Entry {
	if len(entries) != 1 {
		panic("len(entries) != 1")
	}
	entries[0].Result = sm.sm.Update(entries[0].Cmd)
	return entries
}

// Lookup queries the state machine.
func (sm *RegularStateMachine) Lookup(query []byte) ([]byte, error) {
	return sm.sm.Lookup(query), nil
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (sm *RegularStateMachine) PrepareSnapshot() (interface{}, error) {
	panic("PrepareSnapshot called on RegularStateMachine")
}

// SaveSnapshot saves the snapshot.
func (sm *RegularStateMachine) SaveSnapshot(ctx interface{},
	w io.Writer,
	fc sm.ISnapshotFileCollection,
	stopc <-chan struct{}) error {
	if ctx != nil {
		panic("ctx is not nil")
	}
	return sm.sm.SaveSnapshot(w, fc, stopc)
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (sm *RegularStateMachine) RecoverFromSnapshot(index uint64,
	r io.Reader, fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	return sm.sm.RecoverFromSnapshot(r, fs, stopc)
}

// Close closes the state machine.
func (sm *RegularStateMachine) Close() {
	sm.sm.Close()
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (sm *RegularStateMachine) GetHash() uint64 {
	return sm.sm.GetHash()
}

// ConcurrentSnapshot returns a boolean flag indicating whether the state
// machine is capable of taking concurrent snapshot.
func (sm *RegularStateMachine) ConcurrentSnapshot() bool {
	return false
}

func (sm *RegularStateMachine) OnDiskStateMachine() bool {
	return false
}

// ConcurrentStateMachine is an IStateMachine type capable of taking concurrent
// snapshots.
type ConcurrentStateMachine struct {
	sm sm.IConcurrentStateMachine
}

// NewConcurrentStateMachine creates a new ConcurrentStateMachine instance.
func NewConcurrentStateMachine(sm sm.IConcurrentStateMachine) *ConcurrentStateMachine {
	return &ConcurrentStateMachine{sm: sm}
}

// Open opens the state machine.
func (sm *ConcurrentStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	panic("Open() called on RegularStateMachine")
}

// Update updates the state machine.
func (sm *ConcurrentStateMachine) Update(entries []sm.Entry) []sm.Entry {
	return sm.sm.Update(entries)
}

// Lookup queries the state machine.
func (sm *ConcurrentStateMachine) Lookup(query []byte) ([]byte, error) {
	return sm.sm.Lookup(query)
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (sm *ConcurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	return sm.sm.PrepareSnapshot()
}

// SaveSnapshot saves the snapshot.
func (sm *ConcurrentStateMachine) SaveSnapshot(ctx interface{},
	w io.Writer, fc sm.ISnapshotFileCollection,
	stopc <-chan struct{}) error {
	return sm.sm.SaveSnapshot(ctx, w, fc, stopc)
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (sm *ConcurrentStateMachine) RecoverFromSnapshot(index uint64,
	r io.Reader, fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	return sm.sm.RecoverFromSnapshot(r, fs, stopc)
}

// Close closes the state machine.
func (sm *ConcurrentStateMachine) Close() {
	sm.sm.Close()
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (sm *ConcurrentStateMachine) GetHash() uint64 {
	return sm.sm.GetHash()
}

// ConcurrentSnapshot returns a boolean flag indicating whether the state
// machine is capable of taking concurrent snapshot.
func (sm *ConcurrentStateMachine) ConcurrentSnapshot() bool {
	return true
}

// OnDiskStateMachine returns a boolean flag indicating whether this is a on
// disk state machine.
func (sm *ConcurrentStateMachine) OnDiskStateMachine() bool {
	return false
}

// OnDiskStateMachine is the type to represent an on disk state machine.
type OnDiskStateMachine struct {
	sm           sm.IOnDiskStateMachine
	opened       bool
	initialIndex uint64
	applied      uint64
}

// NewOnDiskStateMachine creates and returns an on disk state machine.
func NewOnDiskStateMachine(sm sm.IOnDiskStateMachine) *OnDiskStateMachine {
	return &OnDiskStateMachine{sm: sm}
}

// Open opens the state machine.
func (sm *OnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	if sm.opened {
		panic("Open() called more than once on OnDiskStateMachine")
	}
	sm.opened = true
	applied, err := sm.sm.Open(stopc)
	if err != nil {
		return 0, err
	}
	sm.initialIndex = applied
	sm.applied = applied
	return applied, nil
}

// Update updates the state machine.
func (sm *OnDiskStateMachine) Update(entries []sm.Entry) []sm.Entry {
	if !sm.opened {
		panic("Update called before open()")
	}
	if len(entries) > 0 {
		if entries[len(entries)-1].Index <= sm.initialIndex {
			plog.Panicf("last entry index to apply %d, initial index %d",
				entries[len(entries)-1].Index, sm.initialIndex)
		}
		if entries[len(entries)-1].Index <= sm.applied {
			plog.Panicf("last entry index to apply %d, applied %d",
				entries[len(entries)-1].Index, sm.applied)
		}
		sm.applied = entries[len(entries)-1].Index
	}
	return sm.sm.Update(entries)
}

// Lookup queries the state machine.
func (sm *OnDiskStateMachine) Lookup(query []byte) ([]byte, error) {
	if !sm.opened {
		panic("lookup called when not opened")
	}
	return sm.sm.Lookup(query)
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (sm *OnDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	if !sm.opened {
		panic("prepare snapshot called when not opened")
	}
	return sm.sm.PrepareSnapshot()
}

// SaveSnapshot saves the snapshot.
func (sm *OnDiskStateMachine) SaveSnapshot(ctx interface{},
	w io.Writer, fc sm.ISnapshotFileCollection,
	stopc <-chan struct{}) error {
	if !sm.opened {
		panic("save snapshot called when not opened")
	}
	return sm.sm.SaveSnapshot(ctx, w, stopc)
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (sm *OnDiskStateMachine) RecoverFromSnapshot(index uint64,
	r io.Reader, fs []sm.SnapshotFile, stopc <-chan struct{}) error {
	if !sm.opened {
		panic("recover from snapshot called when not opened")
	}
	if index <= sm.applied {
		plog.Panicf("recover snapshot moving applied index backwards, %d, %d",
			index, sm.applied)
	}
	sm.applied = index
	return sm.sm.RecoverFromSnapshot(r, stopc)
}

// Close closes the state machine.
func (sm *OnDiskStateMachine) Close() {
	sm.sm.Close()
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (sm *OnDiskStateMachine) GetHash() uint64 {
	return sm.sm.GetHash()
}

// ConcurrentSnapshot returns a boolean flag indicating whether the state
// machine is capable of taking concurrent snapshot.
func (sm *OnDiskStateMachine) ConcurrentSnapshot() bool {
	return true
}

// OnDiskStateMachine returns a boolean flag indicating whether this is an on
// disk state machine.
func (sm *OnDiskStateMachine) OnDiskStateMachine() bool {
	return true
}
