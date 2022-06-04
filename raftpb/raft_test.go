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

package raftpb

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestStateMachineTypeHaveExpectedValues(t *testing.T) {
	if sm.Type(UnknownStateMachine) != 0 ||
		sm.Type(RegularStateMachine) != sm.RegularStateMachine ||
		sm.Type(ConcurrentStateMachine) != sm.ConcurrentStateMachine ||
		sm.Type(OnDiskStateMachine) != sm.OnDiskStateMachine {
		t.Errorf("unexpected sm type value")
	}
}

func TestBootstrapValidateHandlesJoiningNode(t *testing.T) {
	bootstrap := Bootstrap{Join: true}
	if !bootstrap.Validate(nil, true, UnknownStateMachine) {
		t.Errorf("incorrect result")
	}
	if !bootstrap.Validate(nil, false, UnknownStateMachine) {
		t.Errorf("incorrect result")
	}
	bootstrap = Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	bootstrap.Addresses[100] = "address1"
	if bootstrap.Validate(nil, true, UnknownStateMachine) {
		t.Errorf("incorrect result not reported")
	}
}

func TestCorruptedBootstrapValueIsChecked(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("didn't panic")
	}()
	bootstrap := Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	bootstrap.Validate(nil, true, UnknownStateMachine)
}

func TestInconsistentInitialMembersAreCheckedAndReported(t *testing.T) {
	bootstrap := Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	bootstrap.Addresses[100] = "address1"
	bootstrap.Addresses[200] = "address2"
	bootstrap.Addresses[300] = "address3"
	if !bootstrap.Validate(nil, false, UnknownStateMachine) {
		t.Errorf("unexpected validation result")
	}
	nodes1 := make(map[uint64]string)
	if !bootstrap.Validate(nodes1, false, UnknownStateMachine) {
		t.Errorf("restarting node should be allowed")
	}
	nodes1[100] = "address1"
	if bootstrap.Validate(nodes1, false, UnknownStateMachine) {
		t.Errorf("inconsistent members not reported")
	}
	nodes1[200] = "address2"
	nodes1[300] = "address3"
	if !bootstrap.Validate(nodes1, false, UnknownStateMachine) {
		t.Errorf("correct members incorrected flagged")
	}
	nodes1[300] = "address4"
	if bootstrap.Validate(nodes1, false, UnknownStateMachine) {
		t.Errorf("inconsistent members not reported")
	}
}

func TestInconsistentStateMachineTypeIsDetected(t *testing.T) {
	tests := []struct {
		bt     StateMachineType
		ct     StateMachineType
		result bool
	}{
		{UnknownStateMachine, UnknownStateMachine, true},
		{UnknownStateMachine, RegularStateMachine, true},
		{UnknownStateMachine, ConcurrentStateMachine, true},
		{UnknownStateMachine, OnDiskStateMachine, true},
		{RegularStateMachine, RegularStateMachine, true},
		{RegularStateMachine, ConcurrentStateMachine, false},
		{RegularStateMachine, OnDiskStateMachine, false},
		{ConcurrentStateMachine, RegularStateMachine, false},
		{ConcurrentStateMachine, ConcurrentStateMachine, true},
		{ConcurrentStateMachine, OnDiskStateMachine, false},
		{OnDiskStateMachine, RegularStateMachine, false},
		{OnDiskStateMachine, ConcurrentStateMachine, false},
		{OnDiskStateMachine, OnDiskStateMachine, true},
	}
	for idx, tt := range tests {
		addr := make(map[uint64]string)
		addr[100] = "addr1"
		bs := Bootstrap{Type: tt.bt, Addresses: addr}
		if bs.Validate(nil, false, tt.ct) != tt.result {
			t.Errorf("%d, validation failed", idx)
		}
	}
}

func TestIsConfigChange(t *testing.T) {
	e1 := Entry{Type: ConfigChangeEntry}
	e2 := Entry{Type: ApplicationEntry}
	if !e1.IsConfigChange() {
		t.Errorf("expected to be a conf change entry")
	}
	if e2.IsConfigChange() {
		t.Errorf("not suppose to be a conf change entry")
	}
	e3 := Entry{}
	if e3.IsConfigChange() {
		t.Errorf("default entry not suppose to be a config change entry")
	}
}

func TestNoOPEntryIsNotUpdateEntry(t *testing.T) {
	e := &Entry{}
	if e.IsUpdateEntry() {
		t.Errorf("noop entry is update entry")
	}
}

func TestNoOPEntryIsNotSessionManaged(t *testing.T) {
	e := &Entry{}
	if e.IsSessionManaged() {
		t.Errorf("noop entry is session managed")
	}
}

func TestIsEmpty(t *testing.T) {
	entries := []Entry{
		{Type: ConfigChangeEntry},
		{ClientID: 12345},
		{Cmd: make([]byte, 1)},
	}
	for idx, ent := range entries {
		if ent.IsEmpty() {
			t.Errorf("entry %d not expected to be empty", idx)
		}
	}
	entries = []Entry{
		{
			Type:     ApplicationEntry,
			ClientID: client.NotSessionManagedClientID,
		},
		{},
	}
	for idx, ent := range entries {
		if !ent.IsEmpty() {
			t.Errorf("entry idx %d is not empty", idx)
		}
	}
}

func TestIsSessionManaged(t *testing.T) {
	e1 := Entry{Type: ConfigChangeEntry}
	e2 := Entry{
		Type:     ApplicationEntry,
		ClientID: client.NotSessionManagedClientID,
	}
	e3 := Entry{ClientID: 12345}
	if e1.IsSessionManaged() || e2.IsSessionManaged() {
		t.Errorf("not suppose to be session managed")
	}
	if !e3.IsSessionManaged() {
		t.Errorf("not session managed")
	}
	e4 := Entry{}
	if e4.IsSessionManaged() {
		t.Errorf("not suppose to be session managed")
	}
}

func TestIsNoOPSession(t *testing.T) {
	e1 := Entry{SeriesID: client.NoOPSeriesID}
	if !e1.IsNoOPSession() {
		t.Errorf("not considered as noop session")
	}
	e2 := Entry{SeriesID: client.NoOPSeriesID + 1}
	if e2.IsNoOPSession() {
		t.Errorf("still considered as noop session")
	}
	e3 := Entry{}
	if !e3.IsNoOPSession() {
		t.Errorf("not a noop session")
	}
}

func TestIsNewSessionRequest(t *testing.T) {
	entries := []Entry{
		{Type: ConfigChangeEntry},
		{Cmd: make([]byte, 1)},
		{ClientID: client.NotSessionManagedClientID},
		{SeriesID: client.SeriesIDForRegister + 1},
		{},
	}
	for idx, ent := range entries {
		if ent.IsNewSessionRequest() {
			t.Errorf("%d is not suppose to be a IsNewSessionRequest %+v", idx, ent)
		}
	}
	ent := Entry{
		Type:     ApplicationEntry,
		ClientID: 123456,
		SeriesID: client.SeriesIDForRegister,
	}
	if !ent.IsNewSessionRequest() {
		t.Errorf("not a new session request")
	}
}

func TestIsEndOfSessionRequest(t *testing.T) {
	entries := []Entry{
		{Type: ConfigChangeEntry},
		{Cmd: make([]byte, 1)},
		{ClientID: client.NotSessionManagedClientID},
		{SeriesID: client.SeriesIDForUnregister - 1},
		{},
	}
	for idx, ent := range entries {
		if ent.IsEndOfSessionRequest() {
			t.Errorf("%d is not suppose to be a IsEndOfSessionRequest %+v", idx, ent)
		}
	}
	ent := Entry{
		Type:     ApplicationEntry,
		ClientID: 123456,
		SeriesID: client.SeriesIDForUnregister,
	}
	if !ent.IsEndOfSessionRequest() {
		t.Errorf("not a new session request")
	}
}

func TestEntrySizeUpperLimit(t *testing.T) {
	max64 := uint64(math.MaxUint64)
	e1 := Entry{
		Term:        max64,
		Index:       max64,
		Type:        1,
		Key:         max64,
		ClientID:    max64,
		SeriesID:    max64,
		RespondedTo: max64,
		Cmd:         make([]byte, 1024),
	}
	if e1.SizeUpperLimit() < e1.Size() {
		t.Errorf("size upper limit < size")
	}
	e1.Cmd = nil
	if e1.SizeUpperLimit() < e1.Size() {
		t.Errorf("size upper limit < size")
	}
	e2 := Entry{}
	if e2.SizeUpperLimit() < e2.Size() {
		t.Errorf("size upper limit < size")
	}
	e2.Cmd = make([]byte, 1024)
	if e2.SizeUpperLimit() < e2.Size() {
		t.Errorf("size upper limit < size")
	}
}

func TestEntryBatchSizeUpperLimit(t *testing.T) {
	max64 := uint64(math.MaxUint64)
	e1 := Entry{
		Term:        max64,
		Index:       max64,
		Type:        1,
		Key:         max64,
		ClientID:    max64,
		SeriesID:    max64,
		RespondedTo: max64,
		Cmd:         make([]byte, 1024),
	}
	eb := EntryBatch{
		Entries: make([]Entry, 0),
	}
	if eb.Size() > eb.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e1)
	}
	if eb.Size() > eb.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	e1.Cmd = nil
	eb.Entries = make([]Entry, 0)
	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e1)
	}
	if eb.Size() > eb.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	e2 := Entry{}
	eb.Entries = make([]Entry, 0)
	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e2)
	}
	if eb.Size() > eb.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
}

func getMaxSizedMsg() Message {
	max64 := uint64(math.MaxUint64)
	msg := Message{
		Type:     NoOP,
		To:       max64,
		From:     max64,
		ShardID:  max64,
		Term:     max64,
		LogTerm:  max64,
		LogIndex: max64,
		Commit:   max64,
		Reject:   true,
		Hint:     max64,
		HintHigh: max64,
	}
	e1 := Entry{
		Term:        max64,
		Index:       max64,
		Type:        1,
		Key:         max64,
		ClientID:    max64,
		SeriesID:    max64,
		RespondedTo: max64,
		Cmd:         make([]byte, 1024),
	}
	for i := 0; i < 1024; i++ {
		msg.Entries = append(msg.Entries, e1)
	}
	msg.Snapshot.Filepath = "longfilepathisherexxxxxxxxxxxxxxxxx"
	msg.Snapshot.FileSize = max64
	msg.Snapshot.Index = max64
	msg.Snapshot.Term = max64
	return msg
}

func TestMessageSizeUpperLimit(t *testing.T) {
	msg := getMaxSizedMsg()
	if msg.Size() > msg.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	msg2 := Message{}
	if msg2.Size() > msg2.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
}

func TestMessageBatchSizeUpperLimit(t *testing.T) {
	max64 := uint64(math.MaxUint64)
	max32 := uint32(math.MaxUint32)
	msg := getMaxSizedMsg()
	mb := MessageBatch{
		DeploymentId:  max64,
		BinVer:        max32,
		SourceAddress: "longaddressisherexxxxxxxxxxxxxxxxxxxxxxxxx",
	}
	for i := 0; i < 1024; i++ {
		mb.Requests = append(mb.Requests, msg)
	}
	if mb.Size() > mb.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	mb2 := MessageBatch{}
	if mb2.Size() > mb2.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
	mb2.DeploymentId = max64
	mb2.BinVer = max32
	mb2.SourceAddress = "longaddressisherexxxxxxxxxxxxxxxxxxxxxxxxxx"
	if mb2.Size() > mb2.SizeUpperLimit() {
		t.Errorf("size > size upper limit")
	}
}

func TestGetEntrySliceInMemSize(t *testing.T) {
	e0 := Entry{}
	e16 := Entry{Cmd: make([]byte, 16)}
	e64 := Entry{Cmd: make([]byte, 64)}
	tests := []struct {
		ents []Entry
		size uint64
	}{
		{[]Entry{}, 0},
		{[]Entry{e0}, 80},
		{[]Entry{e16}, 96},
		{[]Entry{e64}, 144},
		{[]Entry{e0, e64}, 224},
		{[]Entry{e0, e16, e64}, 320},
	}
	for idx, tt := range tests {
		result := GetEntrySliceInMemSize(tt.ents)
		if result != tt.size {
			t.Errorf("%d, result %d, want %d", idx, result, tt.size)
		}
	}
}

func TestMetadataEntry(t *testing.T) {
	me := Entry{
		Type:  MetadataEntry,
		Index: 200,
		Term:  5,
	}
	if !me.IsEmpty() {
		t.Errorf("IsEmpty returned false")
	}
	if me.IsSessionManaged() {
		t.Errorf("IsSessionManaged returned true")
	}
	if !me.IsNoOPSession() {
		t.Errorf("IsNoOPSession returned false")
	}
	if me.IsNewSessionRequest() || me.IsEndOfSessionRequest() {
		t.Errorf("not suppose to be session related")
	}
	if me.IsUpdateEntry() {
		t.Errorf("IsUpdateEntry returned true")
	}
}

func TestEntryCanBeMarshalledAndUnmarshalled(t *testing.T) {
	cmd := make([]byte, 1024)
	rand.Read(cmd)
	e := Entry{
		Type:        MetadataEntry,
		Index:       200,
		Term:        5,
		Key:         12345678,
		ClientID:    7654321,
		RespondedTo: 13579,
		Cmd:         cmd,
	}
	m, err := e.Marshal()
	if err != nil {
		t.Fatalf("%v", err)
	}
	e2 := Entry{}
	if err := e2.Unmarshal(m); err != nil {
		t.Fatalf("%v", err)
	}
	if !reflect.DeepEqual(&e, &e2) {
		t.Fatalf("entry changed")
	}
	sh1 := (*reflect.SliceHeader)(unsafe.Pointer(&e.Cmd))
	sh2 := (*reflect.SliceHeader)(unsafe.Pointer(&e2.Cmd))
	if !(sh2.Data+uintptr(sh2.Len) <= sh1.Data ||
		sh2.Data >= sh1.Data+uintptr(sh1.Len)) {
		t.Fatalf("overlapping slice")
	}
}

func TestRaftDataStatusCanBeMarshaled(t *testing.T) {
	r := &RaftDataStatus{
		Address:             "mydomain.com:12345",
		BinVer:              uint32(123),
		HardHash:            uint64(1234567890),
		LogdbType:           "mytype",
		Hostname:            "myhostname",
		DeploymentId:        uint64(1234567890),
		EntryBatchSize:      uint64(123456789),
		AddressByNodeHostId: true,
	}
	check := func(v *RaftDataStatus) {
		data, err := v.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal %v", err)
		}
		r2 := &RaftDataStatus{}
		if err := r2.Unmarshal(data); err != nil {
			t.Fatalf("failed to unmarshal %v", err)
		}
		if !reflect.DeepEqual(v, r2) {
			t.Fatalf("data changed, %+v\n%+v", *v, *r2)
		}
	}
	check(r)
	r.AddressByNodeHostId = false
	check(r)
}

func TestMessageCanDrop(t *testing.T) {
	tests := []struct {
		t       MessageType
		canDrop bool
	}{
		{LocalTick, true},
		{Election, true},
		{LeaderHeartbeat, true},
		{ConfigChangeEvent, true},
		{NoOP, true},
		{Ping, true},
		{Pong, true},
		{Propose, true},
		{SnapshotStatus, false},
		{Unreachable, false},
		{CheckQuorum, true},
		{BatchedReadIndex, true},
		{Replicate, true},
		{ReplicateResp, true},
		{RequestVote, true},
		{RequestVoteResp, true},
		{InstallSnapshot, false},
		{Heartbeat, true},
		{HeartbeatResp, true},
		{ReadIndex, true},
		{ReadIndexResp, true},
		{Quiesce, true},
		{SnapshotReceived, true},
		{LeaderTransfer, true},
		{TimeoutNow, true},
		{RateLimit, true},
	}
	for idx, tt := range tests {
		m := Message{Type: tt.t}
		if m.CanDrop() != tt.canDrop {
			t.Errorf("%d, can drop %t, want %t", idx, m.CanDrop(), tt.canDrop)
		}
	}
}
