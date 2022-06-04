// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

	"github.com/lni/dragonboat/v4/internal/tests"
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
	if err := od.Recover(reader, nil, stopc); err != nil {
		t.Errorf("recover from snapshot failed %v", err)
	}
}
