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
	"crypto/rand"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestStateMachineTypeHaveExpectedValues(t *testing.T) {
	require.Equal(t, sm.Type(0), sm.Type(UnknownStateMachine))
	require.Equal(t, sm.Type(sm.RegularStateMachine),
		sm.Type(RegularStateMachine))
	require.Equal(t, sm.Type(sm.ConcurrentStateMachine),
		sm.Type(ConcurrentStateMachine))
	require.Equal(t, sm.Type(sm.OnDiskStateMachine),
		sm.Type(OnDiskStateMachine))
}

func TestBootstrapValidateHandlesJoiningNode(t *testing.T) {
	bootstrap := Bootstrap{Join: true}
	require.True(t, bootstrap.Validate(nil, true, UnknownStateMachine))
	require.True(t, bootstrap.Validate(nil, false, UnknownStateMachine))

	bootstrap = Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	bootstrap.Addresses[100] = "address1"
	require.False(t, bootstrap.Validate(nil, true, UnknownStateMachine))
}

func TestCorruptedBootstrapValueIsChecked(t *testing.T) {
	bootstrap := Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	require.Panics(t, func() {
		bootstrap.Validate(nil, true, UnknownStateMachine)
	})
}

func TestInconsistentInitialMembersAreCheckedAndReported(t *testing.T) {
	bootstrap := Bootstrap{Join: false, Addresses: make(map[uint64]string)}
	bootstrap.Addresses[100] = "address1"
	bootstrap.Addresses[200] = "address2"
	bootstrap.Addresses[300] = "address3"
	require.True(t, bootstrap.Validate(nil, false, UnknownStateMachine))

	nodes1 := make(map[uint64]string)
	require.True(t, bootstrap.Validate(nodes1, false, UnknownStateMachine))

	nodes1[100] = "address1"
	require.False(t, bootstrap.Validate(nodes1, false, UnknownStateMachine))

	nodes1[200] = "address2"
	nodes1[300] = "address3"
	require.True(t, bootstrap.Validate(nodes1, false, UnknownStateMachine))

	nodes1[300] = "address4"
	require.False(t, bootstrap.Validate(nodes1, false, UnknownStateMachine))
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
		require.Equal(t, tt.result, bs.Validate(nil, false, tt.ct),
			"test case %d failed", idx)
	}
}

func TestIsConfigChange(t *testing.T) {
	e1 := Entry{Type: ConfigChangeEntry}
	e2 := Entry{Type: ApplicationEntry}
	require.True(t, e1.IsConfigChange())
	require.False(t, e2.IsConfigChange())

	e3 := Entry{}
	require.False(t, e3.IsConfigChange())
}

func TestNoOPEntryIsNotUpdateEntry(t *testing.T) {
	e := &Entry{}
	require.False(t, e.IsUpdateEntry())
}

func TestNoOPEntryIsNotSessionManaged(t *testing.T) {
	e := &Entry{}
	require.False(t, e.IsSessionManaged())
}

func TestIsEmpty(t *testing.T) {
	entries := []Entry{
		{Type: ConfigChangeEntry},
		{ClientID: 12345},
		{Cmd: make([]byte, 1)},
	}
	for idx, ent := range entries {
		require.False(t, ent.IsEmpty(), "entry %d should not be empty", idx)
	}

	entries = []Entry{
		{
			Type:     ApplicationEntry,
			ClientID: client.NotSessionManagedClientID,
		},
		{},
	}
	for idx, ent := range entries {
		require.True(t, ent.IsEmpty(), "entry idx %d should be empty", idx)
	}
}

func TestIsSessionManaged(t *testing.T) {
	e1 := Entry{Type: ConfigChangeEntry}
	e2 := Entry{
		Type:     ApplicationEntry,
		ClientID: client.NotSessionManagedClientID,
	}
	e3 := Entry{ClientID: 12345}

	require.False(t, e1.IsSessionManaged())
	require.False(t, e2.IsSessionManaged())
	require.True(t, e3.IsSessionManaged())

	e4 := Entry{}
	require.False(t, e4.IsSessionManaged())
}

func TestIsNoOPSession(t *testing.T) {
	e1 := Entry{SeriesID: client.NoOPSeriesID}
	require.True(t, e1.IsNoOPSession())

	e2 := Entry{SeriesID: client.NoOPSeriesID + 1}
	require.False(t, e2.IsNoOPSession())

	e3 := Entry{}
	require.True(t, e3.IsNoOPSession())
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
		require.False(t, ent.IsNewSessionRequest(),
			"%d is not supposed to be a IsNewSessionRequest %+v", idx, ent)
	}

	ent := Entry{
		Type:     ApplicationEntry,
		ClientID: 123456,
		SeriesID: client.SeriesIDForRegister,
	}
	require.True(t, ent.IsNewSessionRequest())
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
		require.False(t, ent.IsEndOfSessionRequest(),
			"%d is not supposed to be a IsEndOfSessionRequest %+v", idx, ent)
	}

	ent := Entry{
		Type:     ApplicationEntry,
		ClientID: 123456,
		SeriesID: client.SeriesIDForUnregister,
	}
	require.True(t, ent.IsEndOfSessionRequest())
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
	require.GreaterOrEqual(t, e1.SizeUpperLimit(), e1.Size())

	e1.Cmd = nil
	require.GreaterOrEqual(t, e1.SizeUpperLimit(), e1.Size())

	e2 := Entry{}
	require.GreaterOrEqual(t, e2.SizeUpperLimit(), e2.Size())

	e2.Cmd = make([]byte, 1024)
	require.GreaterOrEqual(t, e2.SizeUpperLimit(), e2.Size())
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
	require.LessOrEqual(t, eb.Size(), eb.SizeUpperLimit())

	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e1)
	}
	require.LessOrEqual(t, eb.Size(), eb.SizeUpperLimit())

	e1.Cmd = nil
	eb.Entries = make([]Entry, 0)
	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e1)
	}
	require.LessOrEqual(t, eb.Size(), eb.SizeUpperLimit())

	e2 := Entry{}
	eb.Entries = make([]Entry, 0)
	for i := 0; i < 1024; i++ {
		eb.Entries = append(eb.Entries, e2)
	}
	require.LessOrEqual(t, eb.Size(), eb.SizeUpperLimit())
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
	require.LessOrEqual(t, msg.Size(), msg.SizeUpperLimit())

	msg2 := Message{}
	require.LessOrEqual(t, msg2.Size(), msg2.SizeUpperLimit())
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
	require.LessOrEqual(t, mb.Size(), mb.SizeUpperLimit())

	mb2 := MessageBatch{}
	require.LessOrEqual(t, mb2.Size(), mb2.SizeUpperLimit())

	mb2.DeploymentId = max64
	mb2.BinVer = max32
	mb2.SourceAddress = "longaddressisherexxxxxxxxxxxxxxxxxxxxxxxxxx"
	require.LessOrEqual(t, mb2.Size(), mb2.SizeUpperLimit())
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
		require.Equal(t, tt.size, result, "test case %d", idx)
	}
}

func TestMetadataEntry(t *testing.T) {
	me := Entry{
		Type:  MetadataEntry,
		Index: 200,
		Term:  5,
	}
	require.True(t, me.IsEmpty())
	require.False(t, me.IsSessionManaged())
	require.True(t, me.IsNoOPSession())
	require.False(t, me.IsNewSessionRequest() || me.IsEndOfSessionRequest())
	require.False(t, me.IsUpdateEntry())
}

func isDifferentInstance(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return true // empty slices can't alias each other
	}

	aPtr := unsafe.Pointer(&a[0])
	bPtr := unsafe.Pointer(&b[0])

	return aPtr != bPtr
}

func TestEntryCanBeMarshalledAndUnmarshalled(t *testing.T) {
	cmd := make([]byte, 1024)
	_, err := rand.Read(cmd)
	require.NoError(t, err)
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
	require.NoError(t, err)

	e2 := Entry{}
	err = e2.Unmarshal(m)
	require.NoError(t, err)

	require.True(t, reflect.DeepEqual(&e, &e2))
	require.True(t, isDifferentInstance(e.Cmd, e2.Cmd))
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
		require.NoError(t, err)

		r2 := &RaftDataStatus{}
		err = r2.Unmarshal(data)
		require.NoError(t, err)

		require.True(t, reflect.DeepEqual(v, r2))
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
		require.Equal(t, tt.canDrop, m.CanDrop(), "test case %d", idx)
	}
}
