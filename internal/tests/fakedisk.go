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
	"fmt"
	"io"
	"sync/atomic"
	"time"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

// FakeDiskSM is a test state machine.
type FakeDiskSM struct {
	SlowOpen       uint32
	initialApplied uint64
	count          uint64
}

// NewFakeDiskSM creates a new fake disk sm for testing purpose.
func NewFakeDiskSM(initialApplied uint64) *FakeDiskSM {
	return &FakeDiskSM{initialApplied: initialApplied}
}

// Open opens the state machine.
func (f *FakeDiskSM) Open(stopc <-chan struct{}) (uint64, error) {
	for atomic.LoadUint32(&f.SlowOpen) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	return f.initialApplied, nil
}

// Update updates the state machine.
func (f *FakeDiskSM) Update(ents []sm.Entry) ([]sm.Entry, error) {
	for _, e := range ents {
		if e.Index <= f.initialApplied {
			panic("already applied index received again")
		} else {
			f.count = f.count + 1
			e.Result = sm.Result{Value: f.count}
		}
	}
	return ents, nil
}

// Lookup queries the state machine.
func (f *FakeDiskSM) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, f.count)
	return result, nil
}

// PrepareSnapshot prepares snapshotting.
func (f *FakeDiskSM) PrepareSnapshot() (interface{}, error) {
	pit := &FakeDiskSM{initialApplied: f.initialApplied, count: f.count}
	return pit, nil
}

// Sync synchronize all in-core state.
func (f *FakeDiskSM) Sync() error {
	return nil
}

// SaveSnapshot saves the state to a snapshot.
func (f *FakeDiskSM) SaveSnapshot(ctx interface{},
	w io.Writer, stopc <-chan struct{}) error {
	pit := ctx.(*FakeDiskSM)
	fmt.Printf("saving initial %d, count %d\n", pit.initialApplied, pit.count)
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, pit.initialApplied)
	if _, err := w.Write(v); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(v, pit.count)
	if _, err := w.Write(v); err != nil {
		return err
	}
	return nil
}

// RecoverFromSnapshot recovers the state of the state machine from a snapshot.
func (f *FakeDiskSM) RecoverFromSnapshot(r io.Reader,
	stopc <-chan struct{}) error {
	v := make([]byte, 8)
	if _, err := io.ReadFull(r, v); err != nil {
		return err
	}
	f.initialApplied = binary.LittleEndian.Uint64(v)
	if _, err := io.ReadFull(r, v); err != nil {
		return err
	}
	f.count = binary.LittleEndian.Uint64(v)
	fmt.Printf("loading initial %d, count %d\n", f.initialApplied, f.count)
	return nil
}

// Close closes the state machine.
func (f *FakeDiskSM) Close() error {
	return nil
}

// GetHash returns the hash of the state.
func (f *FakeDiskSM) GetHash() (uint64, error) {
	return 0, nil
}

type SimDiskSM struct {
	applied uint64
}

func NewSimDiskSM(applied uint64) *SimDiskSM {
	return &SimDiskSM{applied: applied}
}

func (s *SimDiskSM) Open(stopc <-chan struct{}) (uint64, error) {
	return s.applied, nil
}

func (s *SimDiskSM) Update(ents []sm.Entry) ([]sm.Entry, error) {
	fmt.Printf("updated called %v\n", ents)
	for _, e := range ents {
		s.applied = e.Index
		e.Result = sm.Result{Value: e.Index}
	}
	return ents, nil
}

func (s *SimDiskSM) Lookup(query interface{}) (interface{}, error) {
	result := s.applied
	return result, nil
}

func (s *SimDiskSM) PrepareSnapshot() (interface{}, error) {
	v := &SimDiskSM{applied: s.applied}
	return v, nil
}

func (s *SimDiskSM) SaveSnapshot(ctx interface{},
	w io.Writer, stopc <-chan struct{}) error {
	pit := ctx.(*SimDiskSM)
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, pit.applied)
	_, err := w.Write(v)
	return err
}

func (s *SimDiskSM) RecoverFromSnapshot(r io.Reader,
	stopc <-chan struct{}) error {
	v := make([]byte, 8)
	if _, err := io.ReadFull(r, v); err != nil {
		return err
	}
	s.applied = binary.LittleEndian.Uint64(v)
	return nil
}

func (s *SimDiskSM) Sync() error {
	return nil
}

func (s *SimDiskSM) Close() error {
	return nil
}
