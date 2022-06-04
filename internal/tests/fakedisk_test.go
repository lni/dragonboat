// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"bytes"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestSimDiskSMCanBeOpened(t *testing.T) {
	sm := NewSimDiskSM(102)
	v, err := sm.Open(nil)
	if err != nil {
		t.Fatalf("open failed %v", err)
	}
	if v != 102 {
		t.Fatalf("unexpected return value %d, want 102", v)
	}
}

func TestSimDiskSMCanBeUpdatedAndQueried(t *testing.T) {
	m := NewSimDiskSM(1)
	ents := []sm.Entry{{Index: 2}, {Index: 3}, {Index: 4}}
	_, _ = m.Update(ents)
	if m.applied != 4 {
		t.Errorf("sm not updated")
	}
	v, err := m.Lookup(nil)
	if err != nil {
		t.Errorf("lookup failed %v", err)
	}
	if v.(uint64) != 4 {
		t.Errorf("unexpected result %d, want 4", v.(uint64))
	}
}

func TestSimDiskSnapshotWorks(t *testing.T) {
	m := NewSimDiskSM(1)
	ents := []sm.Entry{{Index: 2}, {Index: 3}, {Index: 4}}
	_, _ = m.Update(ents)
	if m.applied != 4 {
		t.Errorf("sm not updated")
	}
	ctx, err := m.PrepareSnapshot()
	if err != nil {
		t.Fatalf("prepare snapshot failed %v", err)
	}
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if err := m.SaveSnapshot(ctx, buf, nil); err != nil {
		t.Fatalf("save snapshot failed %v", err)
	}
	reader := bytes.NewBuffer(buf.Bytes())
	m2 := NewSimDiskSM(100)
	v, err := m2.Lookup(nil)
	if err != nil {
		t.Errorf("lookup failed %v", err)
	}
	if v.(uint64) != 100 {
		t.Errorf("unexpected result %d, want 100", v.(uint64))
	}
	if err := m2.RecoverFromSnapshot(reader, nil); err != nil {
		t.Fatalf("recover from snapshot failed %v", err)
	}
	v, err = m2.Lookup(nil)
	if err != nil {
		t.Errorf("lookup failed %v", err)
	}
	if v.(uint64) != 4 {
		t.Errorf("unexpected result %d, want 4", v.(uint64))
	}
}
