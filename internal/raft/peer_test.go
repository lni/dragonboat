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

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftpb"
)

// Campaign starts the campaign procedure.
func (rc *Peer) Campaign() {
	rc.raft.Handle(raftpb.Message{Type: raftpb.Election})
}

func getTestMembership(nodes []uint64) raftpb.Membership {
	m := raftpb.Membership{
		Addresses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	for _, nid := range nodes {
		m.Addresses[nid] = ""
	}
	return m
}

func TestRaftAPINodeStep(t *testing.T) {
	for i := range raftpb.MessageType_name {
		s := NewTestLogDB()
		rawNode := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
		rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply

		msgt := raftpb.MessageType(i)
		// stepping on non-local messages should be fine
		if !isLocalMessageType(msgt) &&
			msgt != raftpb.SnapshotReceived && msgt != raftpb.TimeoutNow {
			rawNode.Handle(raftpb.Message{Type: msgt})
		}
	}
}

func TestLeaderIDCanBeReportedBackToPeer(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	p.raft.hasNotAppliedConfigChange = p.raft.testOnlyHasConfigChangeToApply
	p.raft.becomeFollower(1, 12)
	if p.GetLeaderID() != 12 {
		t.Errorf("leader id not reported back")
	}
	p.raft.becomeCandidate()
	p.raft.becomeLeader()
	if p.GetLeaderID() != 1 {
		t.Errorf("leader id not reported back")
	}
}

func TestRaftAPIRequestLeaderTransfer(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	p.RequestLeaderTransfer(1)
}

func TestRaftAPIRTT(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	tick := p.raft.electionTick
	p.Tick()
	if p.raft.electionTick != tick+1 {
		t.Errorf("tick not updated")
	}
	p.QuiescedTick()
	if p.raft.electionTick != tick+2 {
		t.Errorf("tick not updated")
	}
}

func TestRaftAPIReportUnreachable(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{
		{NodeID: 1, Address: "1"},
		{NodeID: 2, Address: "2"},
	}, true, true)
	if len(p.raft.remotes) != 2 {
		t.Errorf("remotes len %d, want 2", len(p.raft.remotes))
	}
	p.raft.state = leader
	p.raft.remotes[2].state = remoteReplicate
	p.ReportUnreachableNode(2)
	if p.raft.remotes[2].state != remoteRetry {
		t.Errorf("remote not set to retry state")
	}
}

func TestRaftAPIReportSnapshotStatus(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{
		{NodeID: 1, Address: "1"},
		{NodeID: 2, Address: "2"},
	}, true, true)
	if len(p.raft.remotes) != 2 {
		t.Errorf("remotes len %d, want 2", len(p.raft.remotes))
	}
	p.raft.state = leader
	p.raft.remotes[2].state = remoteSnapshot
	p.ReportSnapshotStatus(2, false)
	if p.raft.remotes[2].state != remoteWait {
		t.Errorf("remote not set to wait, %s", p.raft.remotes[2].state)
	}
}

func testRaftAPIProposeAndConfigChange(cct raftpb.ConfigChangeType, nid uint64, t *testing.T) {
	s := NewTestLogDB()
	var err error
	rawNode := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
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
		ud = rawNode.GetUpdate(true, 0)
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		// Once we are the leader, propose a command and a ConfigChange.
		if !proposed && rawNode.raft.leaderID == rawNode.raft.nodeID {
			rawNode.ProposeEntries([]raftpb.Entry{{Cmd: []byte("somedata")}})
			cc := raftpb.ConfigChange{Type: cct, NodeID: nid}
			ccdata, err = cc.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			rawNode.ProposeConfigChange(cc, 128)

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
	if entries[1].Type != raftpb.ConfigChangeEntry {
		t.Fatalf("type = %v, want %v", entries[1].Type, raftpb.ConfigChangeEntry)
	}
	if entries[1].Key != 128 {
		t.Errorf("key not recorded")
	}
	if !bytes.Equal(entries[1].Cmd, ccdata) {
		t.Errorf("data = %v, want %v", entries[1].Cmd, ccdata)
	}
}

func TestRaftAPIProposeAndConfigChange(t *testing.T) {
	testRaftAPIProposeAndConfigChange(raftpb.AddNode, NoLeader, t)
	testRaftAPIProposeAndConfigChange(raftpb.AddNode, 2, t)
	testRaftAPIProposeAndConfigChange(raftpb.RemoveNode, 2, t)
	testRaftAPIProposeAndConfigChange(raftpb.AddObserver, 2, t)
}

func TestGetUpdateIncludeLastAppliedValue(t *testing.T) {
	s := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	ud := rawNode.GetUpdate(true, 1232)
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
	rawNode := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	for {
		ud = rawNode.GetUpdate(true, 0)
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.nodeID {
			rawNode.Commit(ud)
			break
		}
		rawNode.Commit(ud)
	}
	cc := raftpb.ConfigChange{Type: raftpb.AddNode, NodeID: 1}
	rawNode.ProposeConfigChange(cc, 128)
	if !rawNode.HasUpdate(true) {
		t.Errorf("HasUpdate returned false")
	}
	ud = rawNode.GetUpdate(false, 0)
	if len(ud.CommittedEntries) > 0 {
		t.Errorf("unexpected returned %d committed entries", len(ud.CommittedEntries))
	}
	ud = rawNode.GetUpdate(true, 0)
	if len(ud.CommittedEntries) == 0 {
		t.Errorf("failed to returned committed entries")
	}
}

func TestRaftAPIProposeAddDuplicateNode(t *testing.T) {
	s := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
	if err := s.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.Campaign()
	for {
		ud = rawNode.GetUpdate(true, 0)
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.nodeID {
			rawNode.Commit(ud)
			break
		}
		rawNode.Commit(ud)
	}

	proposeConfigChangeAndApply := func(cc raftpb.ConfigChange, key uint64) {
		rawNode.ProposeConfigChange(cc, key)
		ud = rawNode.GetUpdate(true, 0)
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		for _, entry := range ud.CommittedEntries {
			if entry.Type == raftpb.ConfigChangeEntry {
				var cc raftpb.ConfigChange
				if err := cc.Unmarshal(entry.Cmd); err != nil {
					t.Fatalf("%v", err)
				}
				rawNode.ApplyConfigChange(cc)
			}
		}
		rawNode.Commit(ud)
	}

	cc1 := raftpb.ConfigChange{Type: raftpb.AddNode, NodeID: 1}
	ccdata1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfigChangeAndApply(cc1, 128)

	// try to add the same node again
	proposeConfigChangeAndApply(cc1, 129)

	// the new node join should be ok
	cc2 := raftpb.ConfigChange{Type: raftpb.AddNode, NodeID: 2}
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
	cc3 := raftpb.ConfigChange{Type: raftpb.RemoveNode, NodeID: 2}
	rawNode.ApplyConfigChange(cc3)
	cc4 := raftpb.ConfigChange{Type: raftpb.AddObserver, NodeID: 3}
	rawNode.ApplyConfigChange(cc4)
	cc5 := raftpb.ConfigChange{Type: raftpb.RemoveNode, NodeID: NoLeader}
	rawNode.ApplyConfigChange(cc5)
}

func TestRaftAPIRejectConfigChange(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	p.raft.setPendingConfigChange()
	if !p.raft.hasPendingConfigChange() {
		t.Errorf("pending config change flag not set")
	}
	p.RejectConfigChange()
	if p.raft.hasPendingConfigChange() {
		t.Errorf("pending config change flag not cleared")
	}
}

func TestRaftAPINotifyRaftLastApplied(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s), s, []PeerAddress{{NodeID: 1}}, true, true)
	p.NotifyRaftLastApplied(123)
	if p.raft.getApplied() != 123 {
		t.Errorf("applied not set")
	}
}

func TestRaftAPIDumpRaftInfoToLog(t *testing.T) {
	s := NewTestLogDB()
	p := Launch(newTestConfig(1, 10, 1, s),
		s, []PeerAddress{{NodeID: 1, Address: "1"}}, true, true)
	m := make(map[uint64]string)
	m[1] = "1"
	p.DumpRaftInfoToLog(m)
}

func TestRaftAPIReadIndex(t *testing.T) {
	msgs := []raftpb.Message{}
	appendStep := func(r *raft, m raftpb.Message) {
		msgs = append(msgs, m)
	}
	wrs := []raftpb.ReadyToRead{{Index: uint64(1), SystemCtx: getTestSystemCtx(12345)}}

	s := NewTestLogDB()
	c := newTestConfig(1, 10, 1, s)
	rawNode := Launch(c, s, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	rawNode.raft.readyToRead = wrs
	// ensure the ReadyToReads can be read out
	hasReady := rawNode.HasUpdate(true)
	if !hasReady {
		t.Errorf("HasReady() returns %t, want %t", hasReady, true)
	}
	ud := rawNode.GetUpdate(true, 0)
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
		ud = rawNode.GetUpdate(true, 0)
		if err := s.Append(ud.EntriesToSave); err != nil {
			t.Fatalf("%v", err)
		}
		if rawNode.raft.leaderID == rawNode.raft.nodeID {
			rawNode.Commit(ud)
			// Once we are the leader, issue a ReadIndex request
			rawNode.raft.handle = appendStep
			rawNode.ReadIndex(wrequestCtx)
			break
		}
		rawNode.Commit(ud)
	}
	// ensure that MTReadIndex message is sent to the underlying raft
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), 1)
	}
	if msgs[0].Type != raftpb.ReadIndex {
		t.Errorf("msg type = %d, want %d", msgs[0].Type, raftpb.ReadIndex)
	}
	if msgs[0].Hint != wrequestCtx.Low || msgs[0].HintHigh != wrequestCtx.High {
		t.Errorf("data = %d, want %d", msgs[0].Hint, wrequestCtx)
	}
}

func TestRaftAPIStatus(t *testing.T) {
	storage := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1, storage), storage, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	status := rawNode.LocalStatus()
	if status.NodeID != 1 {
		t.Errorf("expected status struct, got nil")
	}
}

func TestRaftAPIInvalidNodeIDCausePanicInLaunch(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	cfg := &config.Config{}
	Launch(cfg, nil, nil, true, true)
}

func TestRaftAPIInvalidInputToLaunchCausePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	storage := NewTestLogDB()
	Launch(newTestConfig(1, 10, 1, storage), storage, []PeerAddress{}, true, true)
}

func TestRaftAPIDuplicatedAddressCausePanicInLaunch(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not called")
	}()
	storage := NewTestLogDB()
	Launch(newTestConfig(1, 10, 1, storage), storage, []PeerAddress{
		{NodeID: 1, Address: "111"},
		{NodeID: 2, Address: "111"},
	}, true, true)
}

func TestRaftAPILaunch(t *testing.T) {
	cc := raftpb.ConfigChange{Type: raftpb.AddNode, NodeID: 1, Initialize: true}
	ccdata, err := cc.Marshal()
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	wants := []raftpb.Update{
		{
			NodeID: 1,
			State:  raftpb.State{Term: 1, Commit: 1, Vote: 0},
			EntriesToSave: []raftpb.Entry{
				{Type: raftpb.ConfigChangeEntry, Term: 1, Index: 1, Cmd: ccdata},
			},
			CommittedEntries: []raftpb.Entry{
				{Type: raftpb.ConfigChangeEntry, Term: 1, Index: 1, Cmd: ccdata},
			},
			UpdateCommit: raftpb.UpdateCommit{Processed: 1, StableLogTo: 1, StableLogTerm: 1},
		},
		{
			NodeID:           1,
			State:            raftpb.State{Term: 2, Commit: 3, Vote: 1},
			EntriesToSave:    []raftpb.Entry{{Term: 2, Index: 3, Cmd: []byte("foo")}},
			CommittedEntries: []raftpb.Entry{{Term: 2, Index: 3, Cmd: []byte("foo")}},
			UpdateCommit:     raftpb.UpdateCommit{Processed: 3, StableLogTo: 3, StableLogTerm: 2},
		},
	}

	storage := NewTestLogDB()
	rawNode := Launch(newTestConfig(1, 10, 1, storage), storage, []PeerAddress{{NodeID: 1}}, true, true)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
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
	ud = rawNode.GetUpdate(true, 0)
	if err := storage.Append(ud.EntriesToSave); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode.Commit(ud)

	rawNode.ProposeEntries([]raftpb.Entry{{Cmd: []byte("foo")}})
	ud = rawNode.GetUpdate(true, 0)
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
		t.Errorf("unexpected Ready: %+v", rawNode.GetUpdate(true, 0))
	}
}

func TestRaftAPIRestart(t *testing.T) {
	entries := []raftpb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Cmd: []byte("foo")},
	}
	st := raftpb.State{Term: 1, Commit: 1}

	want := raftpb.Update{
		NodeID: 1,
		State:  emptyState,
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
		UpdateCommit:     raftpb.UpdateCommit{Processed: 1},
	}

	storage := NewTestLogDB()
	storage.SetState(st)
	if err := storage.Append(entries); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode := Launch(newTestConfig(1, 10, 1, storage), storage, nil, true, false)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
	ud.Messages = nil
	if !reflect.DeepEqual(ud, want) {
		t.Errorf("g = %+v,\n             w   %+v", ud, want)
	}
	rawNode.Commit(ud)
	if rawNode.HasUpdate(true) {
		t.Errorf("unexpected Ready: %+v", rawNode.GetUpdate(true, 0))
	}
}

func TestRaftAPIRestartFromSnapshot(t *testing.T) {
	snap := raftpb.Snapshot{
		Membership: getTestMembership([]uint64{1, 2}),
		Index:      2,
		Term:       1,
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 3, Cmd: []byte("foo")},
	}
	st := raftpb.State{Term: 1, Commit: 3}

	want := raftpb.Update{
		NodeID: 1,
		State:  emptyState,
		// commit up to commit index in st
		CommittedEntries: entries,
		UpdateCommit:     raftpb.UpdateCommit{Processed: 3},
	}

	s := NewTestLogDB()
	s.SetState(st)
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatalf("%v", err)
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("%v", err)
	}
	rawNode := Launch(newTestConfig(1, 10, 1, s), s, nil, true, false)
	rawNode.raft.hasNotAppliedConfigChange = rawNode.raft.testOnlyHasConfigChangeToApply
	ud := rawNode.GetUpdate(true, 0)
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
	p := Launch(newTestConfig(1, 10, 1, storage), storage, []PeerAddress{{NodeID: 1}}, true, true)
	p.Handle(raftpb.Message{Type: raftpb.LocalTick})
}

func TestRaftAPIGetUpdateCommit(t *testing.T) {
	ud := raftpb.Update{
		CommittedEntries: []raftpb.Entry{
			{Index: 100, Term: 2},
			{Index: 101, Term: 3},
		},
		EntriesToSave: []raftpb.Entry{
			{Index: 102, Term: 3},
			{Index: 103, Term: 4},
		},
		Snapshot:    raftpb.Snapshot{Index: 105},
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
		checkLaunchRequest(&config.Config{}, nil, false, false)
	})
	expectPanicFn(func() {
		checkLaunchRequest(&config.Config{NodeID: 1}, nil, true, true)
	})
	addr := make([]PeerAddress, 0)
	addr = append(addr, PeerAddress{Address: "1"})
	addr = append(addr, PeerAddress{Address: "1"})
	expectPanicFn(func() {
		checkLaunchRequest(&config.Config{NodeID: 1}, addr, false, false)
	})
}
