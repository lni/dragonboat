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

package tests

import (
	"encoding/binary"
	"io"
	"sync/atomic"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

// TestUpdate is a IStateMachine used for testing purposes.
type TestUpdate struct {
	val uint32
}

// Update updates the state machine.
func (c *TestUpdate) Update(data []byte) (sm.Result, error) {
	atomic.StoreUint32(&c.val, 1)
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Millisecond)
	}
	atomic.StoreUint32(&c.val, 0)
	return sm.Result{Value: 100}, nil
}

// Lookup queries the state machine.
func (c *TestUpdate) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 4)
	v := atomic.LoadUint32(&c.val)
	binary.LittleEndian.PutUint32(result, v)
	return result, nil
}

// SaveSnapshot saves the snapshot.
func (c *TestUpdate) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	panic("not implemented")
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (c *TestUpdate) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, stopc <-chan struct{}) error {
	panic("not implemented")
}

// Close closes the state machine.
func (c *TestUpdate) Close() error {
	return nil
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (c *TestUpdate) GetHash() (uint64, error) {
	return 0, nil
}

// ConcurrentUpdate is a IConcurrentStateMachine used for testing purposes.
type ConcurrentUpdate struct {
	UpdateCount int
	val         uint32
}

// Update updates the state machine.
func (c *ConcurrentUpdate) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if c.UpdateCount == 0 {
		c.UpdateCount = len(entries)
	}
	for i := 0; i < 10; i++ {
		atomic.AddUint32(&c.val, 1)
		time.Sleep(1 * time.Millisecond)
	}
	entries[0].Result = sm.Result{Value: 100}
	return entries, nil
}

// Lookup queries the state machine.
func (c *ConcurrentUpdate) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 4)
	v := atomic.LoadUint32(&c.val)
	binary.LittleEndian.PutUint32(result, v)
	return result, nil
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (c *ConcurrentUpdate) PrepareSnapshot() (interface{}, error) {
	panic("not implemented")
}

// SaveSnapshot saves the snapshot.
func (c *ConcurrentUpdate) SaveSnapshot(ctx interface{},
	w io.Writer,
	fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	panic("not implemented")
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (c *ConcurrentUpdate) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, stopc <-chan struct{}) error {
	panic("not implemented")
}

// Close closes the state machine.
func (c *ConcurrentUpdate) Close() error {
	return nil
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (c *ConcurrentUpdate) GetHash() (uint64, error) {
	return 0, nil
}

// TestSnapshot is a IConcurrentStateMachine used for testing purposes.
type TestSnapshot struct {
	val uint32
}

// Update updates the state machine.
func (c *TestSnapshot) Update(data []byte) (sm.Result, error) {
	return sm.Result{Value: uint64(atomic.LoadUint32(&c.val))}, nil
}

// Lookup queries the state machine.
func (c *TestSnapshot) Lookup(query interface{}) (interface{}, error) {
	return nil, nil
}

// SaveSnapshot saves the snapshot.
func (c *TestSnapshot) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	atomic.StoreUint32(&c.val, 0)
	for i := 0; i < 100; i++ {
		atomic.StoreUint32(&c.val, 1)
		time.Sleep(time.Millisecond)
	}
	atomic.StoreUint32(&c.val, 0)
	data := make([]byte, 4)
	_, err := w.Write(data)
	if err != nil {
		panic(err)
	}
	return nil
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (c *TestSnapshot) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, stopc <-chan struct{}) error {
	panic("not implemented")
}

// Close closes the state machine.
func (c *TestSnapshot) Close() error {
	return nil
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (c *TestSnapshot) GetHash() (uint64, error) {
	return 0, nil
}

// ConcurrentSnapshot is a IConcurrentStateMachine used for testing purposes.
type ConcurrentSnapshot struct {
	val uint32
}

// Update updates the state machine.
func (c *ConcurrentSnapshot) Update(entries []sm.Entry) ([]sm.Entry, error) {
	entries[0].Result = sm.Result{Value: uint64(atomic.LoadUint32(&c.val))}
	return entries, nil
}

// Lookup queries the state machine.
func (c *ConcurrentSnapshot) Lookup(query interface{}) (interface{}, error) {
	return nil, sm.ErrSnapshotStopped
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (c *ConcurrentSnapshot) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}

// SaveSnapshot saves the snapshot.
func (c *ConcurrentSnapshot) SaveSnapshot(ctx interface{},
	w io.Writer,
	fc sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	atomic.StoreUint32(&c.val, 0)
	for i := 0; i < 100; i++ {
		atomic.StoreUint32(&c.val, 1)
		time.Sleep(time.Millisecond)
	}
	atomic.StoreUint32(&c.val, 0)
	data := make([]byte, 4)
	_, err := w.Write(data)
	if err != nil {
		panic(err)
	}
	return nil
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (c *ConcurrentSnapshot) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, stopc <-chan struct{}) error {
	panic("not implemented")
}

// Close closes the state machine.
func (c *ConcurrentSnapshot) Close() error {
	return nil
}

// GetHash returns the uint64 hash value representing the state of a state
// machine.
func (c *ConcurrentSnapshot) GetHash() (uint64, error) {
	return 0, nil
}
