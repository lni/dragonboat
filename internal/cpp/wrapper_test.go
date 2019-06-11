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

package cpp

import (
	"bytes"
	"encoding/binary"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	pb "github.com/lni/dragonboat/raftpb"
	"os"
	"testing"
)

func TestManagedObjectCanBeAddedReturnedAndRemoved(t *testing.T) {
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
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
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	ds2 := NewStateMachineWrapperFromPlugin(2, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateConcurrentStateMachine",
		pb.ConcurrentStateMachine, nil)
	ds3 := NewStateMachineWrapperFromPlugin(3, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds1 == nil {
		t.Errorf("failed to return the regular data store object")
	}
	if ds2 == nil {
		t.Errorf("failed to return the concurrent data store object")
	}
	if ds3 == nil {
		t.Errorf("failed to return the on-disk data store object")
	}
	ds3.Open()
	ds1.(*RegularStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	ds1.(*RegularStateMachineWrapper).SetOffloaded(rsm.FromStepWorker)
	ds1.(*RegularStateMachineWrapper).SetOffloaded(rsm.FromCommitWorker)
	ds1.(*RegularStateMachineWrapper).SetOffloaded(rsm.FromSnapshotWorker)
	ds1.(*RegularStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	if !ds1.(*RegularStateMachineWrapper).ReadyToDestroy() {
		t.Errorf("ds1.destroyed: %t, want true", ds1.(*RegularStateMachineWrapper).ReadyToDestroy())
	}
	ds2.(*ConcurrentStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	ds2.(*ConcurrentStateMachineWrapper).SetOffloaded(rsm.FromStepWorker)
	ds2.(*ConcurrentStateMachineWrapper).SetOffloaded(rsm.FromCommitWorker)
	ds2.(*ConcurrentStateMachineWrapper).SetOffloaded(rsm.FromSnapshotWorker)
	ds2.(*ConcurrentStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	if !ds2.(*ConcurrentStateMachineWrapper).ReadyToDestroy() {
		t.Errorf("ds2.destroyed: %t, want true", ds2.(*ConcurrentStateMachineWrapper).ReadyToDestroy())
	}
	ds3.(*OnDiskStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	ds3.(*OnDiskStateMachineWrapper).SetOffloaded(rsm.FromStepWorker)
	ds3.(*OnDiskStateMachineWrapper).SetOffloaded(rsm.FromCommitWorker)
	ds3.(*OnDiskStateMachineWrapper).SetOffloaded(rsm.FromSnapshotWorker)
	ds3.(*OnDiskStateMachineWrapper).SetOffloaded(rsm.FromNodeHost)
	if !ds3.(*OnDiskStateMachineWrapper).ReadyToDestroy() {
		t.Errorf("ds3.destroyed: %t, want true", ds3.(*OnDiskStateMachineWrapper).ReadyToDestroy())
	}
}

func TestOffloadedWorksAsExpected(t *testing.T) {
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	ds2 := NewStateMachineWrapperFromPlugin(2, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateConcurrentStateMachine",
		pb.ConcurrentStateMachine, nil)
	ds3 := NewStateMachineWrapperFromPlugin(3, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds1 == nil {
		t.Errorf("failed to return the regular data store object")
	}
	if ds2 == nil {
		t.Errorf("failed to return the concurrent data store object")
	}
	if ds3 == nil {
		t.Errorf("failed to return the on-disk data store object")
	}
	ds3.Open()
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
		ds1.Offloaded(tt.val)
		ds2.Offloaded(tt.val)
		ds3.Offloaded(tt.val)
		if ds1.(*RegularStateMachineWrapper).Destroyed() != tt.destroyed {
			t.Errorf("ds1.destroyed %t, want %t",
				ds1.(*RegularStateMachineWrapper).Destroyed(), tt.destroyed)
		}
		if ds2.(*ConcurrentStateMachineWrapper).Destroyed() != tt.destroyed {
			t.Errorf("ds2.destroyed %t, want %t",
				ds2.(*ConcurrentStateMachineWrapper).Destroyed(), tt.destroyed)
		}
		if ds3.(*OnDiskStateMachineWrapper).Destroyed() != tt.destroyed {
			t.Errorf("ds3.destroyed %t, want %t",
				ds1.(*OnDiskStateMachineWrapper).Destroyed(), tt.destroyed)
		}
	}
}

func TestCppWrapperCanBeUpdatedAndLookedUp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	ds2 := NewStateMachineWrapperFromPlugin(2, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateConcurrentStateMachine",
		pb.ConcurrentStateMachine, nil)
	ds3 := NewStateMachineWrapperFromPlugin(3, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	initialApplied := uint64(123)
	ds3.Open()
	if ds1 == nil {
		t.Errorf("failed to return the data store object")
	}
	if ds2 == nil {
		t.Errorf("failed to return the concurrent data store object")
	}
	if ds3 == nil {
		t.Errorf("failed to return the on-disk data store object")
	}
	defer ds1.(*RegularStateMachineWrapper).destroy()
	defer ds2.(*ConcurrentStateMachineWrapper).destroy()
	defer ds3.(*OnDiskStateMachineWrapper).destroy()
	e1 := pb.Entry{Index: 1, Cmd: []byte("test-data-1")}
	e2 := pb.Entry{Index: 2, Cmd: []byte("test-data-2")}
	e3 := pb.Entry{Index: 3, Cmd: []byte("test-data-3")}
	v1, _ := ds1.Update(nil, e1)
	v2, _ := ds1.Update(nil, e2)
	v3, _ := ds1.Update(nil, e3)
	if v2.Value != v1.Value+1 || v3.Value != v2.Value+1 {
		t.Errorf("Unexpected update result from regular data store")
	}
	result, err := ds1.Lookup([]byte("test-lookup-data"))
	if err != nil {
		t.Errorf("failed to lookup regular data store")
	}
	v4 := binary.LittleEndian.Uint64(result.([]byte))
	if v4 != v3.Value {
		t.Errorf("regular data store returned %d, want %d", v4, v3)
	}
	v1, _ = ds2.Update(nil, e1)
	v2, _ = ds2.Update(nil, e2)
	v3, _ = ds2.Update(nil, e3)
	if v2.Value != v1.Value+1 || v3.Value != v2.Value+1 {
		t.Errorf("Unexpected update result from concurrent data store")
	}
	result, err = ds2.Lookup([]byte("test-lookup-data"))
	if err != nil {
		t.Errorf("failed to lookup concurrent data store")
	}
	v4 = binary.LittleEndian.Uint64(result.([]byte))
	if v4 != v3.Value {
		t.Errorf("concurrent data store returned %d, want %d", v4, v3)
	}
	e1.Index = initialApplied + 1
	e2.Index = initialApplied + 2
	e3.Index = initialApplied + 3
	v1, _ = ds3.Update(nil, e1)
	v2, _ = ds3.Update(nil, e2)
	v3, _ = ds3.Update(nil, e3)
	if v2.Value != v1.Value+1 || v3.Value != v2.Value+1 {
		t.Errorf("Unexpected update result from on-disk data store")
	}
	result, err = ds3.Lookup([]byte("test-lookup-data"))
	if err != nil {
		t.Errorf("failed to lookup on-disk data store")
	}
	v4 = binary.LittleEndian.Uint64(result.([]byte))
	if v4 != 3 {
		t.Errorf("on-disk data store returned %d, want %d", v4, 3)
	}
}

func TestCppWrapperCanUseProtobuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*RegularStateMachineWrapper).destroy()
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

func TestCppWrapperSnapshotWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	defer os.Remove(fp)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*RegularStateMachineWrapper).destroy()
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
		t.Fatalf("failed to close the snapshot writer %v", err)
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
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*RegularStateMachineWrapper).destroy()
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
	err = ds2.RecoverFromSnapshot(0, reader, nil)
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

func TestRegularSMCanRecoverFromExportedSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	defer ds1.(*RegularStateMachineWrapper).destroy()

	entry := pb.Entry{Index: 1, Cmd: []byte("test-data-1")}
	count, err := ds1.Update(nil, entry)
	if err != nil {
		t.Fatalf("update failed %v", err)
	}
	if count.Value != 1 {
		t.Fatalf("initial update returned %v, want 1", count.Value)
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.ExportedSnapshot,
		},
		Type:    pb.RegularStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to new snapshot writer %v", err)
	}
	_, sz, err := ds1.SaveSnapshot(&meta, writer, meta.Session.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
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
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateRegularStateMachine",
		pb.RegularStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*RegularStateMachineWrapper).destroy()
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
	err = ds2.RecoverFromSnapshot(0, reader, nil)
	if err != nil {
		t.Fatalf("failed to recover from snapshot %v", err)
	}
	yacount, err := ds2.Lookup([]byte{0})
	if v := binary.LittleEndian.Uint64(yacount.([]byte)); v != 1 {
		t.Fatalf("lookup recovered data store returned %v, want 1", v)
	}
	h, _ := ds1.GetHash()
	h2, _ := ds2.GetHash()
	if h != h2 {
		t.Fatalf("hash does not match")
	}
}

func TestConcurrentSMCanRecoverFromExportedSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateConcurrentStateMachine",
		pb.ConcurrentStateMachine, nil)
	defer ds1.(*ConcurrentStateMachineWrapper).destroy()

	entry := pb.Entry{Index: 1, Cmd: []byte("test-data-1")}
	count, err := ds1.Update(nil, entry)
	if err != nil {
		t.Fatalf("update failed %v", err)
	}
	if count.Value != 1 {
		t.Fatalf("initial update returned %v, want 1", count.Value)
	}
	ctx, _ := ds1.PrepareSnapshot()
	if len(ctx.([]byte)) != 8 {
		t.Fatalf("failed to prepare snapshot")
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.ExportedSnapshot,
		},
		Type:    pb.ConcurrentStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		Ctx:     ctx,
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to new snapshot writer %v", err)
	}
	_, sz, err := ds1.SaveSnapshot(&meta, writer, meta.Session.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
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
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateConcurrentStateMachine",
		pb.ConcurrentStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*ConcurrentStateMachineWrapper).destroy()
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
	err = ds2.RecoverFromSnapshot(0, reader, nil)
	if err != nil {
		t.Fatalf("failed to recover from snapshot %v", err)
	}
	yacount, err := ds2.Lookup([]byte{0})
	if v := binary.LittleEndian.Uint64(yacount.([]byte)); v != 1 {
		t.Fatalf("lookup recovered data store returned %v, want 1", v)
	}
	h, _ := ds1.GetHash()
	h2, _ := ds2.GetHash()
	if h != h2 {
		t.Fatalf("hash does not match")
	}
}

func TestOnDiskSMCanBeOpened(t *testing.T) {
	defer leaktest.AfterTest(t)()
	initialApplied := uint64(123)
	// initialApplied = 123 is hard coded in the testing on-disk statemachine
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	idx, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != initialApplied {
		t.Errorf("unexpected idx %d", idx)
	}
	if ds.(*OnDiskStateMachineWrapper).initialIndex != initialApplied {
		t.Errorf("initial index not recorded %d, want %d",
			ds.(*OnDiskStateMachineWrapper).initialIndex, initialApplied)
	}
	if ds.(*OnDiskStateMachineWrapper).applied != initialApplied {
		t.Errorf("applied not recorded %d, want %d",
			ds.(*OnDiskStateMachineWrapper).applied, initialApplied)
	}
}

func TestOnDiskSMCanNotOpenedMoreThanOnce(t *testing.T) {
	defer leaktest.AfterTest(t)
	initialApplied := uint64(123)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	idx, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != initialApplied {
		t.Errorf("unexpected idx %d", idx)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := ds.Open(); err != nil {
		t.Fatalf("open failed %v", err)
	}
}

func TestOnDiskSMRecordAppliedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)
	initialApplied := uint64(123)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	idx, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != initialApplied {
		t.Errorf("unexpected idx %d", idx)
	}
	if ds.(*OnDiskStateMachineWrapper).applied != initialApplied {
		t.Errorf("applied not recorded %d, want %d",
			ds.(*OnDiskStateMachineWrapper).applied, initialApplied)
	}
	entry := pb.Entry{Index: initialApplied + 1, Cmd: []byte("test-data-1")}
	if _, err := ds.Update(nil, entry); err != nil {
		t.Fatalf("update failed %v", err)
	}
	if ds.(*OnDiskStateMachineWrapper).applied != initialApplied+1 {
		t.Errorf("applied value not recorded")
	}
}

func TestUpdateAnUnopenedOnDiskSMWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)
	applied := uint64(123)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	entry := pb.Entry{Index: applied + 1, Cmd: []byte("test-data-1")}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := ds.Update(nil, entry); err != nil {
		t.Fatalf("update failed %v", err)
	}
}

func TestUpdateOnDiskSMWithAppliedIndexWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)
	applied := uint64(123)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	idx, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != applied {
		t.Errorf("unexpected idx %d", idx)
	}
	entry := pb.Entry{Index: applied + 1, Cmd: []byte("test-data-1")}
	count, err := ds.Update(nil, entry)
	if err != nil {
		t.Fatalf("update failed %v", err)
	}
	if count.Value != 1 {
		t.Fatalf("initial update returned %v, want 1", count.Value)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := ds.Update(nil, entry); err != nil {
		t.Fatalf("update failed %v", err)
	}
}

func TestUpdateOnDiskSMWithIndexLessThanInitialIndexWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)
	applied := uint64(123)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	idx, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	if idx != applied {
		t.Errorf("unexpected idx %d", idx)
	}
	entry := pb.Entry{Index: applied - 1, Cmd: []byte("test-data-1")}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := ds.Update(nil, entry); err != nil {
		t.Fatalf("update failed %v", err)
	}
}

func TestLookupAnUnopenedOnDiskSMWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	if _, err := ds.Lookup(nil); err != nil {
		t.Fatalf("lookup failed %v", err)
	}
}

func TestLookupCanBeCalledOnceOnDiskSMIsOpened(t *testing.T) {
	defer leaktest.AfterTest(t)
	ds := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds.(*OnDiskStateMachineWrapper).destroy()
	_, err := ds.Open()
	if err != nil {
		t.Fatalf("failed to open %v", err)
	}
	count, err := ds.Lookup([]byte{0})
	if err != nil {
		t.Errorf("lookup failed %v", err)
	}
	if v := binary.LittleEndian.Uint64(count.([]byte)); v != 0 {
		t.Errorf("initial lookup returned %v, want 0", v)
	}
}

func TestOnDiskSMCanRecoverFromExportedSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	initialApplied := uint64(123)
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds1.(*OnDiskStateMachineWrapper).destroy()
	_, err := ds1.Open()
	if err != nil {
		t.Fatalf("failed to open ds1 %v", err)
	}
	entry := pb.Entry{Index: initialApplied + 1, Cmd: []byte("test-data-1")}
	count, err := ds1.Update(nil, entry)
	if err != nil {
		t.Fatalf("update failed %v", err)
	}
	if count.Value != 1 {
		t.Fatalf("initial update returned %v, want 1", count.Value)
	}
	ctx, _ := ds1.PrepareSnapshot()
	if len(ctx.([]byte)) != 16 {
		t.Fatalf("failed to prepare snapshot")
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.ExportedSnapshot,
		},
		Type:    pb.OnDiskStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		Ctx:     ctx,
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to new snapshot writer %v", err)
	}
	_, sz, err := ds1.SaveSnapshot(&meta, writer, meta.Session.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
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
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*OnDiskStateMachineWrapper).destroy()
	_, err = ds2.Open()
	if err != nil {
		t.Fatalf("failed to open ds2 %v", err)
	}
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
	err = ds2.RecoverFromSnapshot(initialApplied+1, reader, nil)
	if err != nil {
		t.Fatalf("failed to recover from snapshot %v", err)
	}
	yacount, err := ds2.Lookup([]byte{0})
	if v := binary.LittleEndian.Uint64(yacount.([]byte)); v != 1 {
		t.Fatalf("lookup recovered data store returned %v, want 1", v)
	}
	h, _ := ds1.GetHash()
	h2, _ := ds2.GetHash()
	if h != h2 {
		t.Fatalf("hash does not match")
	}
}

func TestOnDiskSMRecoverFromBackwardSnapshotWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	initialApplied := uint64(123)
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds1.(*OnDiskStateMachineWrapper).destroy()
	_, err := ds1.Open()
	if err != nil {
		t.Fatalf("failed to open ds1 %v", err)
	}
	ctx, _ := ds1.PrepareSnapshot()
	if len(ctx.([]byte)) != 16 {
		t.Fatalf("failed to prepare snapshot")
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.ExportedSnapshot,
		},
		Type:    pb.OnDiskStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		Ctx:     ctx,
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to new snapshot writer %v", err)
	}
	_, sz, err := ds1.SaveSnapshot(&meta, writer, meta.Session.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
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
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*OnDiskStateMachineWrapper).destroy()
	_, err = ds2.Open()
	if err != nil {
		t.Fatalf("failed to open ds2 %v", err)
	}
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	err = ds2.RecoverFromSnapshot(initialApplied-1, reader, nil)
	if err != nil {
		t.Fatalf("recover from snapshot failed %v", err)
	}
}

func TestOnDiskSMCanSaveDummySnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds1.(*OnDiskStateMachineWrapper).destroy()
	_, err := ds1.Open()
	if err != nil {
		t.Fatalf("failed to open ds1 %v", err)
	}
	ctx, _ := ds1.PrepareSnapshot()
	if len(ctx.([]byte)) != 16 {
		t.Fatalf("failed to prepare snapshot")
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.UserRequestedSnapshot,
		},
		Type:    pb.OnDiskStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		Ctx:     ctx,
	}
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to new snapshot writer %v", err)
	}
	_, _, err = ds1.SaveSnapshot(&meta, writer, meta.Session.Bytes(), nil)
	if err != nil {
		t.Errorf("failed to save snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
	}
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
}

type testSink struct {
	chunks     []pb.SnapshotChunk
	sendFailed bool
	stopped    bool
}

func (s *testSink) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	if s.sendFailed || s.stopped {
		return !s.sendFailed, s.stopped
	}
	s.chunks = append(s.chunks, chunk)
	return true, false
}

func (s *testSink) ClusterID() uint64 {
	return 1
}

func (s *testSink) ToNodeID() uint64 {
	return 1
}

func TestOnDiskSMCanStreamSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)
	initialApplied := uint64(123)
	ds1 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	defer ds1.(*OnDiskStateMachineWrapper).destroy()
	_, err := ds1.Open()
	if err != nil {
		t.Fatalf("failed to open ds1 %v", err)
	}
	entry := pb.Entry{Index: initialApplied + 1, Cmd: []byte("test-data-1")}
	count, err := ds1.Update(nil, entry)
	if err != nil {
		t.Fatalf("update failed %v", err)
	}
	if count.Value != 1 {
		t.Fatalf("initial update returned %v, want 1", count.Value)
	}
	ctx, _ := ds1.PrepareSnapshot()
	if len(ctx.([]byte)) != 16 {
		t.Fatalf("failed to prepare snapshot")
	}
	meta := rsm.SnapshotMeta{
		Request: rsm.SnapshotRequest{
			Type: rsm.ExportedSnapshot,
		},
		Type:    pb.OnDiskStateMachine,
		Session: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		Ctx:     ctx,
	}
	sink := testSink{
		chunks:     make([]pb.SnapshotChunk, 0),
		sendFailed: false,
		stopped:    false,
	}
	writer := rsm.NewChunkWriter(&sink, &meta)
	err = ds1.StreamSnapshot(ctx, writer)
	if err != nil {
		t.Errorf("failed to stream snapshot, %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close the snapshot writer %v", err)
	}
	fp := "cpp_test_snapshot_file_safe_to_delete.snap"
	f, err := os.Create(fp)
	if err != nil {
		t.Errorf("failed to create temp snapshot file")
	}
	for _, chunk := range sink.chunks {
		f.Write(chunk.Data)
	}
	f.Close()
	f, err = os.Open(fp)
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to new snapshot reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get snapshot header")
	}
	reader.ValidateHeader(header)
	buf := make([]byte, 16)
	// the snapshot header is followed by extra 16 bytes for sessions
	// the first 8 bytes indicate that there are at most 4096 struct session
	// the next 8 bytes indicate the actual number of struct session
	// because on-disk-sm does not support sessions, the 16 extra bytes
	// in streamed snapshot are just used as place holders
	reader.Read(buf)
	if binary.LittleEndian.Uint64(buf[0:8]) != 4096 || binary.LittleEndian.Uint64(buf[8:]) != 0 {
		t.Errorf("sessions not empty")
	}
	ds2 := NewStateMachineWrapperFromPlugin(1, 1,
		"./dragonboat-cpp-plugin-example.so",
		"CreateOnDiskStateMachine",
		pb.OnDiskStateMachine, nil)
	if ds2 == nil {
		t.Errorf("failed to return the data store object")
	}
	defer ds2.(*OnDiskStateMachineWrapper).destroy()
	_, err = ds2.Open()
	if err != nil {
		t.Fatalf("failed to open ds2 %v", err)
	}
	err = ds2.RecoverFromSnapshot(initialApplied+1, reader, nil)
	if err != nil {
		t.Fatalf("failed to recover from snapshot %v", err)
	}
	yacount, err := ds2.Lookup([]byte{0})
	if v := binary.LittleEndian.Uint64(yacount.([]byte)); v != 1 {
		t.Fatalf("lookup recovered data store returned %v, want 1", v)
	}
	h, _ := ds1.GetHash()
	h2, _ := ds2.GetHash()
	if h != h2 {
		t.Fatalf("hash does not match")
	}
}
