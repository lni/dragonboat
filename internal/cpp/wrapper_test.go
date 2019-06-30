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

package cpp

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/tests/kvpb"
	"github.com/lni/dragonboat/v3/internal/utils/leaktest"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

func TestManagedObjectCanBeAddedReturnedAndRemoved(t *testing.T) {
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if GetManagedObjectCount() != 0 {
		t.Errorf("object count not 0")
	}
	oid := AddManagedObject(ds)
	o, ok := GetManagedObject(oid)
	if !ok {
		t.Errorf("failed to get the object back")
	}
	if o != ds {
		t.Errorf("pointer value changed")
	}
	if GetManagedObjectCount() != 1 {
		t.Errorf("object count not 1")
	}
	RemoveManagedObject(oid)
	if GetManagedObjectCount() != 0 {
		t.Errorf("object count not 0")
	}
	_, ok = GetManagedObject(oid)
	if ok {
		t.Errorf("object not removed")
	}
}

func TestStateMachineWrapperCanBeCreatedAndDestroyed(t *testing.T) {
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	ds.(*StateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	ds.(*StateMachineWrapper).SetOffloaded(rsm.FromStepWorker)
	ds.(*StateMachineWrapper).SetOffloaded(rsm.FromCommitWorker)
	ds.(*StateMachineWrapper).SetOffloaded(rsm.FromSnapshotWorker)
	ds.(*StateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	if !ds.(*StateMachineWrapper).ReadyToDestroy() {
		t.Errorf("destroyed: %t, want true", ds.(*StateMachineWrapper).ReadyToDestroy())
	}
}

func TestOffloadedWorksAsExpected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	tests := []struct {
		val       rsm.From
		destroyed bool
	}{
		{rsm.FromStepWorker, false},
		{rsm.FromCommitWorker, false},
		{rsm.FromSnapshotWorker, false},
		{rsm.FromNodeHost, true},
	}
	for _, tt := range tests {
		ds.Offloaded(tt.val)
		if ds.(*StateMachineWrapper).Destroyed() != tt.destroyed {
			t.Errorf("ds.destroyed %t, want %t",
				ds.(*StateMachineWrapper).Destroyed(), tt.destroyed)
		}
	}
}

func TestCppStateMachineCanBeUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*StateMachineWrapper).destroy()
}

func TestCppWrapperCanBeUpdatedAndLookedUp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*StateMachineWrapper).destroy()
	e1 := pb.Entry{Index: 1, Cmd: []byte("test-data-1")}
	e2 := pb.Entry{Index: 2, Cmd: []byte("test-data-2")}
	e3 := pb.Entry{Index: 3, Cmd: []byte("test-data-3")}
	v1, _ := ds.Update(nil, e1)
	v2, _ := ds.Update(nil, e2)
	v3, _ := ds.Update(nil, e3)
	if v2.Value != v1.Value+1 || v3.Value != v2.Value+1 {
		t.Errorf("Unexpected update result")
	}
	result, err := ds.Lookup([]byte("test-lookup-data"))
	if err != nil {
		t.Errorf("failed to lookup")
	}
	v4 := binary.LittleEndian.Uint32(result.([]byte))
	if uint64(v4) != v3.Value {
		t.Errorf("returned %d, want %d", v4, v3)
	}
}

func TestCppWrapperCanUseProtobuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*StateMachineWrapper).destroy()
	k := "test-key"
	d := "test-value"
	kv := kvpb.PBKV{
		Key: k,
		Val: d,
	}
	data, err := kv.Marshal()
	if err != nil {
		panic(err)
	}
	e := pb.Entry{Index: 1, Cmd: data}
	if _, err := ds.Update(nil, e); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestCppSnapshotWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	defer os.Remove(fp)
	ds := NewStateMachineWrapper(1, 1, "example", nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*StateMachineWrapper).destroy()
	e1 := pb.Entry{Index: 1, Cmd: []byte("test-data-1")}
	e2 := pb.Entry{Index: 2, Cmd: []byte("test-data-2")}
	e3 := pb.Entry{Index: 3, Cmd: []byte("test-data-3")}
	v1, _ := ds.Update(nil, e1)
	v2, _ := ds.Update(nil, e2)
	v3, _ := ds.Update(nil, e3)
	if v2.Value != v1.Value+1 || v3.Value != v2.Value+1 {
		t.Errorf("Unexpected update result")
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	sessions := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	_, sz, err := ds.SaveSnapshot(nil, writer, sessions.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writter %v", err)
	}
	f, err := os.Open(fp)
	if err != nil {
		t.Errorf("failed to open file %v", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Errorf("failed to get file stat")
	}
	if uint64(fi.Size()) != sz {
		t.Errorf("sz %d, want %d", fi.Size(), sz)
	}
	ds2 := NewStateMachineWrapper(1, 1, "example", nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*StateMachineWrapper).destroy()
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		err = reader.Close()
	}()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	reader.ValidateHeader(header)
	err = ds2.RecoverFromSnapshot(reader, nil)
	if err != nil {
		t.Errorf("failed to recover from snapshot %v", err)
	}
	reader.ValidatePayload(header)
	h, _ := ds.GetHash()
	h2, _ := ds.GetHash()
	if h != h2 {
		t.Errorf("hash does not match")
	}
}
