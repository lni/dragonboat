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
	"bytes"
	"testing"

	"github.com/lni/dragonboat/internal/tests"
	sm "github.com/lni/dragonboat/statemachine"
)

func TestOnDiskSMCanBeOpened(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	idx, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != applied {
		t.Errorf("unexpected idx %d", idx)
	}
	if od.initialIndex != applied {
		t.Errorf("initial index not recorded %d, want %d", od.initialIndex, applied)
	}
	if od.applied != applied {
		t.Errorf("applied not recorded %d, want %d", od.applied, applied)
	}
}

func TestOnDiskSMCanNotBeOpenedMoreThanOnce(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	idx, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != applied {
		t.Errorf("unexpected idx %d", idx)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := od.Open(nil); err != nil {
		t.Fatalf("open failed %v", err)
	}
}

func TestOnDiskSMRecordAppliedIndex(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	idx, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != applied {
		t.Errorf("unexpected idx %d", idx)
	}
	if od.applied != applied {
		t.Errorf("applied not recorded %d, want %d", od.applied, applied)
	}
	entries := []sm.Entry{
		{Index: applied + 1},
		{Index: applied + 2},
		{Index: applied + 3},
	}
	od.Update(entries)
	if od.applied != applied+uint64(len(entries)) {
		t.Errorf("applied value not recorded")
	}
}

func TestUpdateAnUnopenedOnDiskSMWillPanic(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	entries := []sm.Entry{
		{Index: applied + 1},
		{Index: applied + 2},
		{Index: applied + 3},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	od.Update(entries)
}

func TestUpdateOnDiskSMWithAppliedIndexWillPanic(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	entries := []sm.Entry{{Index: applied + 1}}
	od.Update(entries)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	od.Update(entries)
}

func TestUpdateOnDiskSMWithIndexLessThanInitialIndexWillPanic(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	od.applied = od.applied + 1
	entries := []sm.Entry{{Index: applied}}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	od.Update(entries)
}

func TestLookupCalledBeforeOnDiskSMIsOpenedWillPanic(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := od.Lookup(nil); err != nil {
		t.Fatalf("lookup failed %v", err)
	}
}

func TestLookupCanBeCalledOnceOnDiskSMIsOpened(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	_, err = od.Lookup(nil)
	if err != nil {
		t.Errorf("lookup failed %v", err)
	}
}

func TestRecoverFromSnapshotCanComplete(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	buf := make([]byte, 16)
	reader := bytes.NewBuffer(buf)
	stopc := make(chan struct{})
	if err := od.RecoverFromSnapshot(applied+1, reader, nil, stopc); err != nil {
		t.Errorf("recover from snapshot failed %v", err)
	}
}

func TestRecoverFromSnapshotWillPanicWhenIndexIsLessThanApplied(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	buf := make([]byte, 16)
	reader := bytes.NewBuffer(buf)
	stopc := make(chan struct{})
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if err := od.RecoverFromSnapshot(applied-1, reader, nil, stopc); err != nil {
		t.Fatalf("recover failed %v", err)
	}
}
