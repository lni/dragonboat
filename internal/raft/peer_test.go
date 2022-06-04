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

//
// Some tests in this file were ported from etcd raft.
// updates have been made to reflect interface & implementation differences
//
// Copyright 2015 The etcd Authors
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

package raft

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func ne(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
}

// Campaign starts the campaign procedure.
func (p *Peer) Campaign() {
	if err := p.raft.Handle(pb.Message{Type: pb.Election}); err != nil {
		panic(err)
	}
}

func getTestMembership(nodes []uint64) pb.Membership {
	m := pb.Membership{
		Addresses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	for _, nid := range nodes {
		m.Addresses[nid] = ""
	}
	return m
}

func TestRaftAPINodeStep(t *testing.T) {
	for i := range pb.MessageType_name {
		s := NewTestLogDB()
		rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
		rawNode.raft.preVote = true
		rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply

		msgt := pb.MessageType(i)
		// stepping on non-local messages should be fine
		if !isLocalMessageType(msgt) &&
			msgt != pb.SnapshotReceived && msgt != pb.TimeoutNow {
			ne(rawNode.Handle(pb.Message{Type: msgt, Term: rawNode.raft.term}), t)
		}
	}
}

func TestRaftAPIRequestLeaderTransfer(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	ne(p.RequestLeaderTransfer(1), t)
}

func TestRaftAPIRTT(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	tick := p.raft.electionTick
	ne(p.Tick(), t)
	if p.raft.electionTick != tick+1 {
		t.Errorf("tick not updated")
	}
	ne(p.QuiescedTick(), t)
	if p.raft.electionTick != tick+2 {
		t.Errorf("tick not updated 2")
	}
}

func TestRaftAPIReportUnreachable(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{
		{ReplicaID: 1, Address: "1"},
		{ReplicaID: 2, Address: "2"},
	}, true, true)
	if len(p.raft.remotes) != 2 {
		t.Errorf("remotes len %d, want 2", len(p.raft.remotes))
	}
	p.raft.state = leader
	p.raft.remotes[2].state = remoteReplicate
	ne(p.ReportUnreachableNode(2), t)
	if p.raft.remotes[2].state != remoteRetry {
		t.Errorf("remote not set to retry state")
	}
}

func TestRaftAPIReportSnapshotStatus(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{
		{ReplicaID: 1, Address: "1"},
		{ReplicaID: 2, Address: "2"},
	}, true, true)
	if len(p.raft.remotes) != 2 {
		t.Errorf("remotes len %d, want 2", len(p.raft.remotes))
	}
	p.raft.state = leader
	p.raft.remotes[2].state = remoteSnapshot
	ne(p.ReportSnapshotStatus(2, false), t)
	if p.raft.remotes[2].state != remoteWait {
		t.Errorf("remote not set to wait, %s", p.raft.remotes[2].state)
	}
}

func testRaftAPIProposeAndConfigChange(cct pb.ConfigChangeType, nid uint64, t *testing.T) {
	s := NewTestLogDB()
	var err error
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	proposed := false
	var (
		lastIndex uint64
		ccdata    []byte
	)
	for {
		ud, err = rawNode.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		// Once we are the leader, propose a command and a ConfigChange.
		if !proposed && rawNode.raft.leaderID == rawNode.raft.replicaID {
			ne(rawNode.ProposeEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
			cc := pb.ConfigChange{Type: cct, ReplicaID: nid}
			ccdata, err = cc.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			ne(rawNode.ProposeConfigChange(cc, 128), t)

			proposed = true
		}
		rawNode.Commit(ud)

		// Exit when we have four entries: one ConfigChange, one no-op for the election,
		// our proposed command and proposed ConfigChange.
		_, lastIndex = s.GetRange()
		if lastIndex >= 4 {
			break
		}
	}

	entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 2)
	}
	if !bytes.Equal(entries[0].Cmd, []byte("somedata")) {
		t.Errorf("entries[0].Cmd = %v, want %v", entries[0].Cmd, []byte("somedata"))
	}
	if entries[1].Type != pb.ConfigChangeEntry {
		t.Fatalf("type = %v, want %v", entries[1].Type, pb.ConfigChangeEntry)
	}
	if entries[1].Key != 128 {
		t.Errorf("key not recorded")
	}
	if !bytes.Equal(entries[1].Cmd, ccdata) {
		t.Errorf("data = %v, want %v", entries[1].Cmd, ccdata)
	}
}

func TestRaftAPIProposeAndConfigChange(t *testing.T) {
	testRaftAPIProposeAndConfigChange(pb.AddNode, NoLeader, t)
	testRaftAPIProposeAndConfigChange(pb.AddNode, 2, t)
	testRaftAPIProposeAndConfigChange(pb.RemoveNode, 2, t)
	testRaftAPIProposeAndConfigChange(pb.AddNonVoting, 2, t)
}

func TestGetUpdateIncludeLastAppliedValue(t *testing.T) {
	s := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	ud, err := rawNode.GetUpdate(true, 1232)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ud.LastApplied != 1232 {
		t.Errorf("unexpected last applied value %d, want 1232", ud.LastApplied)
	}
	uc := getUpdateCommit(ud)
	if uc.LastApplied != 1232 {
		t.Errorf("unexpected last applied value %d, want 1232", uc.LastApplied)
	}
}

func TestRaftMoreEntriesToApplyControl(t *testing.T) {
	s := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	for {
		ud, err = rawNode.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.replicaID {
			rawNode.Commit(ud)
			break
		}
		rawNode.Commit(ud)
	}
	cc := pb.ConfigChange{Type: pb.AddNode, ReplicaID: 1}
	ne(rawNode.ProposeConfigChange(cc, 128), t)
	if !rawNode.HasUpdate(true) {
		t.Errorf("HasUpdate returned false")
	}
	ud, err = rawNode.GetUpdate(false, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(ud.CommittedEntries) > 0 {
		t.Errorf("unexpected returned %d committed entries", len(ud.CommittedEntries))
	}
	ud, err = rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(ud.CommittedEntries) == 0 {
		t.Errorf("failed to returned committed entries")
	}
}

func TestRaftAPIProposeAddDuplicateNode(t *testing.T) {
	s := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	for {
		ud, err = rawNode.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.replicaID {
			rawNode.Commit(ud)
			break
		}
		rawNode.Commit(ud)
	}

	proposeConfigChangeAndApply := func(cc pb.ConfigChange, key uint64) {
		ne(rawNode.ProposeConfigChange(cc, key), t)
		ud, err = rawNode.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		for _, entry := range ud.CommittedEntries {
			if entry.Type == pb.ConfigChangeEntry {
				var cc pb.ConfigChange
				if err := cc.Unmarshal(entry.Cmd); err != nil {
					t.Fatalf("%v", err)
				}
				ne(rawNode.ApplyConfigChange(cc), t)
			}
		}
		rawNode.Commit(ud)
	}

	cc1 := pb.ConfigChange{Type: pb.AddNode, ReplicaID: 1}
	ccdata1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfigChangeAndApply(cc1, 128)

	// try to add the same node again
	proposeConfigChangeAndApply(cc1, 129)

	// the new node join should be ok
	cc2 := pb.ConfigChange{Type: pb.AddNode, ReplicaID: 2}
	ccdata2, err := cc2.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfigChangeAndApply(cc2, 130)

	_, lastIndex := s.GetRange()

	// the last three entries should be: ConfigChange cc1, cc1, cc2
	entries, err := s.Entries(lastIndex-2, lastIndex+1, noLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 3)
	}
	if !bytes.Equal(entries[0].Cmd, ccdata1) {
		t.Errorf("entries[0].Cmd = %v, want %v", entries[0].Cmd, ccdata1)
	}
	if !bytes.Equal(entries[2].Cmd, ccdata2) {
		t.Errorf("entries[2].Cmd = %v, want %v", entries[2].Cmd, ccdata2)
	}
	cc3 := pb.ConfigChange{Type: pb.RemoveNode, ReplicaID: 2}
	ne(rawNode.ApplyConfigChange(cc3), t)
	cc4 := pb.ConfigChange{Type: pb.AddNonVoting, ReplicaID: 3}
	ne(rawNode.ApplyConfigChange(cc4), t)
	cc5 := pb.ConfigChange{Type: pb.RemoveNode, ReplicaID: NoLeader}
	ne(rawNode.ApplyConfigChange(cc5), t)
}

func TestRaftAPIRejectConfigChange(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	p.raft.setPendingConfigChange()
	if !p.raft.hasPendingConfigChange() {
		t.Errorf("pending config change flag not set")
	}
	ne(p.RejectConfigChange(), t)
	if p.raft.hasPendingConfigChange() {
		t.Errorf("pending config change flag not cleared")
	}
}

func TestRaftAPINotifyRaftLastApplied(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	p.NotifyRaftLastApplied(123)
	if p.raft.getApplied() != 123 {
		t.Errorf("applied not set")
	}
}

func TestRaftAPIReadIndex(t *testing.T) {
	msgs := []pb.Message{}
	appendStep := func(r *raft, m pb.Message) error {
		msgs = append(msgs, m)
		return nil
	}
	wrs := []pb.ReadyToRead{{Index: uint64(1), SystemCtx: getTestSystemCtx(12345)}}

	s := NewTestLogDB()
	c := newTestConfig(1, 10, 1)
	rawNode := Launch(c, s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	rawNode.raft.readyToRead = wrs
	// ensure the ReadyToReads can be read out
	hasReady := rawNode.HasUpdate(true)
	if !hasReady {
		t.Errorf("HasReady() returns %t, want %t", hasReady, true)
	}
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !reflect.DeepEqual(ud.ReadyToReads, wrs) {
		t.Errorf("ReadyToReads = %d, want %d", ud.ReadyToReads, wrs)
	}
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)
	// ensure raft.readyToRead is reset after advance
	if len(rawNode.raft.readyToRead) > 0 {
		t.Errorf("readyToRead = %v", rawNode.raft.readyToRead)
	}

	wrequestCtx := getTestSystemCtx(23456)
	rawNode.Campaign()
	for {
		ud, err = rawNode.GetUpdate(true, 0)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.replicaID {
			rawNode.Commit(ud)
			// Once we are the leader, issue a ReadIndex request
			rawNode.raft.handle = appendStep
			ne(rawNode.ReadIndex(wrequestCtx), t)
			break
		}
		rawNode.Commit(ud)
	}
	// ensure that MTReadIndex message is sent to the underlying raft
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), 1)
	}
	if msgs[0].Type != pb.ReadIndex {
		t.Errorf("msg type = %d, want %d", msgs[0].Type, pb.ReadIndex)
	}
	if msgs[0].Hint != wrequestCtx.Low || msgs[0].HintHigh != wrequestCtx.High {
		t.Errorf("data = %d, want %d", msgs[0].Hint, wrequestCtx)
	}
}

func TestRaftAPIStatus(t *testing.T) {
	storage := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1), storage, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	status := getLocalStatus(rawNode.raft)
	if status.ReplicaID != 1 {
		t.Errorf("expected status struct, got nil")
	}
}

func TestRaftAPIInvalidReplicaIDCausePanicInLaunch(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	Launch(config.Config{}, nil, nil, nil, true, true)
}

func TestRaftAPIInvalidInputToLaunchCausePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	storage := NewTestLogDB()
	Launch(newTestConfig(1, 10, 1), storage, nil, []PeerAddress{}, true, true)
}

func TestRaftAPIDuplicatedAddressCausePanicInLaunch(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	storage := NewTestLogDB()
	Launch(newTestConfig(1, 10, 1), storage, nil, []PeerAddress{
		{ReplicaID: 1, Address: "111"},
		{ReplicaID: 2, Address: "111"},
	}, true, true)
}

func TestRaftAPILaunch(t *testing.T) {
	cc := pb.ConfigChange{Type: pb.AddNode, ReplicaID: 1, Initialize: true}
	ccdata, err := cc.Marshal()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	wants := []pb.Update{
		{
			ReplicaID: 1,
			State:     pb.State{Term: 1, Commit: 1, Vote: 0},
			EntriesToSave: []pb.Entry{
				{Type: pb.ConfigChangeEntry, Term: 1, Index: 1, Cmd: ccdata},
			},
			CommittedEntries: []pb.Entry{
				{Type: pb.ConfigChangeEntry, Term: 1, Index: 1, Cmd: ccdata},
			},
			UpdateCommit: pb.UpdateCommit{Processed: 1, StableLogTo: 1, StableLogTerm: 1},
			LeaderUpdate: pb.LeaderUpdate{LeaderID: 0, Term: 1},
		},
		{
			ReplicaID:        1,
			State:            pb.State{Term: 2, Commit: 3, Vote: 1},
			EntriesToSave:    []pb.Entry{{Term: 2, Index: 3, Cmd: []byte("foo")}},
			CommittedEntries: []pb.Entry{{Term: 2, Index: 3, Cmd: []byte("foo")}},
			UpdateCommit:     pb.UpdateCommit{Processed: 3, StableLogTo: 3, StableLogTerm: 2},
		},
	}

	storage := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1), storage, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	ud.Messages = nil
	if !reflect.DeepEqual(ud, wants[0]) {
		t.Fatalf("#%d: g = %+v,\n             w   %+v", 1, ud, wants[0])
	} else {
		if err := storage.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		rawNode.Commit(ud)
	}
	if err := storage.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	ud, err = rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := storage.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	ne(rawNode.ProposeEntries([]pb.Entry{{Cmd: []byte("foo")}}), t)
	ud, err = rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	ud.Messages = nil
	if !reflect.DeepEqual(ud, wants[1]) {
		t.Errorf("#%d: g = %+v,\n             w   %+v", 2, ud, wants[1])
	} else {
		if err := storage.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		rawNode.Commit(ud)
	}

	if rawNode.HasUpdate(true) {
		t.Errorf("unexpected Ready")
	}
}

func TestRaftAPIRestart(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Cmd: []byte("foo")},
	}
	st := pb.State{Term: 1, Commit: 1}

	want := pb.Update{
		ReplicaID: 1,
		State:     emptyState,
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
		UpdateCommit:     pb.UpdateCommit{Processed: 1},
		FastApply:        true,
		LeaderUpdate:     pb.LeaderUpdate{LeaderID: 0, Term: 1},
	}

	storage := NewTestLogDB()
	storage.SetState(st)
	if err := storage.Append(entries); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode := Launch(newTestConfig(1, 10, 1), storage, nil, nil, true, false)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	ud.Messages = nil
	if !reflect.DeepEqual(ud, want) {
		t.Errorf("g = %+v,\n             w   %+v", ud, want)
	}
	rawNode.Commit(ud)
	if rawNode.HasUpdate(true) {
		t.Errorf("unexpected Ready")
	}
}

func TestRaftAPIRestartFromSnapshot(t *testing.T) {
	snap := pb.Snapshot{
		Membership: getTestMembership([]uint64{1, 2}),
		Index:      2,
		Term:       1,
	}
	entries := []pb.Entry{
		{Term: 1, Index: 3, Cmd: []byte("foo")},
	}
	st := pb.State{Term: 1, Commit: 3}

	want := pb.Update{
		ReplicaID: 1,
		State:     emptyState,
		// commit up to commit index in st
		CommittedEntries: entries,
		UpdateCommit:     pb.UpdateCommit{Processed: 3},
		FastApply:        true,
		LeaderUpdate:     pb.LeaderUpdate{LeaderID: 0, Term: 1},
	}

	s := NewTestLogDB()
	s.SetState(st)
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatalf("%v", err)
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, nil, true, false)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	ud.Messages = nil
	if !reflect.DeepEqual(ud, want) {
		t.Errorf("g = %+v,\n             w   %+v", ud, want)
	} else {
		rawNode.Commit(ud)
	}
	if rawNode.HasUpdate(true) {
		t.Errorf("unexpected Ready: %+v", rawNode.HasUpdate(true))
	}
}

func TestRaftAPIStepOnLocalMessageWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	storage := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1), storage, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	ne(p.Handle(pb.Message{Type: pb.LocalTick}), t)
}

func TestRaftAPIGetUpdateCommit(t *testing.T) {
	ud := pb.Update{
		CommittedEntries: []pb.Entry{
			{Index: 100, Term: 2},
			{Index: 101, Term: 3},
		},
		EntriesToSave: []pb.Entry{
			{Index: 102, Term: 3},
			{Index: 103, Term: 4},
		},
		Snapshot:    pb.Snapshot{Index: 105},
		LastApplied: 99,
	}
	uc := getUpdateCommit(ud)
	if uc.StableSnapshotTo != 105 {
		t.Errorf("stable snapshot to incorrect")
	}
	if uc.Processed != 105 {
		t.Errorf("applied to")
	}
	if uc.StableLogTo != 103 || uc.StableLogTerm != 4 {
		t.Errorf("stable log to/term")
	}
	if uc.LastApplied != 99 {
		t.Errorf("last applied %d, want 99", uc.LastApplied)
	}
}

func TestCheckLaunchRequest(t *testing.T) {
	expectPanicFn := func(f func()) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("panic not triggered")
			}
		}()
		f()
	}
	expectPanicFn(func() {
		checkLaunchRequest(config.Config{}, nil, false, false)
	})
	expectPanicFn(func() {
		checkLaunchRequest(config.Config{ReplicaID: 1}, nil, true, true)
	})
	addr := make([]PeerAddress, 0)
	addr = append(addr, PeerAddress{Address: "1"})
	addr = append(addr, PeerAddress{Address: "1"})
	expectPanicFn(func() {
		checkLaunchRequest(config.Config{ReplicaID: 1}, addr, false, false)
	})
}

func TestValidateUpdate(t *testing.T) {
	tests := []struct {
		commit              uint64
		firstCommittedIndex uint64
		committedLength     uint64
		firstSaveIndex      uint64
		saveLength          uint64
		panic               bool
	}{
		{0, 1, 2, 0, 0, false},
		{1, 1, 2, 0, 0, true},
		{2, 1, 2, 0, 0, false},
		{0, 1, 2, 1, 2, false},
		{0, 1, 2, 1, 1, true},
		{0, 1, 2, 3, 1, false},
		{0, 0, 0, 1, 2, false},
		{0, 1, 2, 0, 0, false},
	}
	for idx, tt := range tests {
		tidx := idx
		ud := pb.Update{}
		ud.Commit = tt.commit
		if tt.committedLength > 0 {
			lastIndex := tt.firstCommittedIndex + tt.committedLength - 1
			for i := tt.firstCommittedIndex; i <= lastIndex; i++ {
				e := pb.Entry{
					Index: i,
				}
				ud.CommittedEntries = append(ud.CommittedEntries, e)
			}
		}
		if tt.saveLength > 0 {
			lastIndex := tt.firstSaveIndex + tt.saveLength - 1
			for i := tt.firstSaveIndex; i <= lastIndex; i++ {
				e := pb.Entry{
					Index: i,
				}
				ud.EntriesToSave = append(ud.EntriesToSave, e)
			}
		}
		func() {
			if tt.panic {
				defer func() {
					if r := recover(); r == nil {
						t.Fatalf("%d, failed to panic", tidx)
					}
				}()
			}
			validateUpdate(ud)
		}()
	}
}

func TestSetFastApply(t *testing.T) {
	tests := []struct {
		hasSnapshot         bool
		firstCommittedIndex uint64
		committedLength     uint64
		firstSaveIndex      uint64
		saveLength          uint64
		fastApply           bool
	}{
		{true, 0, 0, 0, 0, false},
		{true, 0, 0, 1, 2, false},
		{true, 1, 2, 0, 0, false},
		{true, 1, 2, 1, 2, false},
		{true, 1, 2, 1, 1, false},
		{true, 1, 1, 1, 2, false},
		{false, 1, 2, 1, 2, false},
		{false, 1, 2, 0, 0, true},
		{false, 0, 0, 1, 2, true},
		{false, 1, 2, 2, 3, false},
		{false, 1, 1, 2, 3, true},
	}

	for idx, tt := range tests {
		tidx := idx
		ud := pb.Update{FastApply: true}
		if tt.hasSnapshot {
			ud.Snapshot = pb.Snapshot{
				Index: 1,
			}
		}
		if tt.committedLength > 0 {
			lastIndex := tt.firstCommittedIndex + tt.committedLength - 1
			for i := tt.firstCommittedIndex; i <= lastIndex; i++ {
				e := pb.Entry{
					Index: i,
				}
				ud.CommittedEntries = append(ud.CommittedEntries, e)
			}
		}
		if tt.saveLength > 0 {
			lastIndex := tt.firstSaveIndex + tt.saveLength - 1
			for i := tt.firstSaveIndex; i <= lastIndex; i++ {
				e := pb.Entry{
					Index: i,
				}
				ud.EntriesToSave = append(ud.EntriesToSave, e)
			}
		}
		ud = setFastApply(ud)
		if ud.FastApply != tt.fastApply {
			t.Fatalf("%d, fast apply not expected", tidx)
		}
	}
}

func TestRaftAPIQueryRaftLog(t *testing.T) {
	s := NewTestLogDB()
	var err error
	rawNode := Launch(newTestConfig(1, 10, 1), s, nil, []PeerAddress{{ReplicaID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud, err := rawNode.GetUpdate(true, 0)
	assert.NoError(t, err)
	assert.NoError(t, s.Append(ud.EntriesToSave))
	rawNode.Commit(ud)

	rawNode.Campaign()
	var lastIndex uint64
	for {
		ud, err = rawNode.GetUpdate(true, 0)
		assert.NoError(t, err)
		assert.NoError(t, s.Append(ud.EntriesToSave))
		// Once we are the leader, propose a command and a ConfigChange.
		if rawNode.raft.leaderID == rawNode.raft.replicaID {
			ne(rawNode.ProposeEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
		}
		rawNode.Commit(ud)
		// Exit when we have four entries: one ConfigChange, one no-op for the election,
		// our proposed command and proposed ConfigChange.
		_, lastIndex = s.GetRange()
		if lastIndex >= 4 {
			break
		}
	}
	entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
	assert.NoError(t, err)
	assert.NoError(t, rawNode.QueryRaftLog(lastIndex-1, lastIndex+1, noLimit))
	assert.NotNil(t, rawNode.raft.logQueryResult)
	ud, err = rawNode.GetUpdate(true, 0)
	assert.NoError(t, err)
	assert.NotNil(t, rawNode.raft.logQueryResult)
	assert.False(t, ud.LogQueryResult.IsEmpty())
	assert.Nil(t, ud.LogQueryResult.Error)
	rawNode.Commit(ud)
	assert.Nil(t, rawNode.raft.logQueryResult)
	assert.Equal(t, entries, ud.LogQueryResult.Entries)
}
