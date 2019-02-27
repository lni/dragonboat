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

package raft

import (
	"reflect"
	"sort"
	"testing"

	pb "github.com/lni/dragonboat/raftpb"
)

//
// Most test here are more focused on individual features/actions in our raft
// implementation. They are more like unit tests. raft_etcd_test.go contains
// tests ported from etcd raft, those tests are more like raft protocol level
// integration tests.
//

func TestOneNodeWithHigherTermAndOneNodeWithMostRecentLogCanCompleteElection(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	a.becomeFollower(1, NoLeader)
	b.becomeFollower(1, NoLeader)
	c.becomeFollower(1, NoLeader)
	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true
	// cause a network partition to isolate node 3
	nt := newNetwork(a, b, c)
	nt.cut(1, 3)
	nt.cut(2, 3)
	// start a few elections to bump the term value
	for i := 0; i < 4; i++ {
		nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data2")}}})
	sm := nt.peers[1].(*raft)
	if sm.log.committed != 3 {
		t.Errorf("peer 1 committed index: %d, want %d", sm.log.committed, 3)
	}
	sm = nt.peers[2].(*raft)
	if sm.log.committed != 3 {
		t.Errorf("peer 2 committed index: %d, want %d", sm.log.committed, 3)
	}
	sm = nt.peers[1].(*raft)
	if sm.state != leader {
		t.Errorf("peer 1 state: %s, want %s", sm.state, leader)
	}
	sm = nt.peers[2].(*raft)
	if sm.state != follower {
		t.Errorf("peer 2 state: %s, want %s", sm.state, follower)
	}
	sm = nt.peers[3].(*raft)
	if sm.state != candidate {
		t.Errorf("peer 3 state: %s, want %s", sm.state, candidate)
	}
	// check whether the term values are expected
	// a.Term == 1
	// b.Term == 1
	// c.Term == 100
	sm = nt.peers[1].(*raft)
	if sm.term != 2 {
		t.Errorf("peer 1 term: %d, want %d", sm.term, 2)
	}
	sm = nt.peers[2].(*raft)
	if sm.term != 2 {
		t.Errorf("peer 2 term: %d, want %d", sm.term, 2)
	}
	sm = nt.peers[3].(*raft)
	if sm.term != 5 {
		t.Errorf("peer 3 term: %d, want %d", sm.term, 5)
	}
	nt.recover()
	nt.cut(1, 2)
	nt.cut(1, 3)
	for i := 0; i <= 2; i++ {
		// call for election
		nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})
		nt.send(pb.Message{From: 2, To: 2, Type: pb.Election})

		// do we have a leader
		sma := nt.peers[2].(*raft)
		smb := nt.peers[3].(*raft)
		if sma.state != leader && smb.state != leader {
			if i == 2 {
				t.Errorf("no leader")
			}
		} else {
			break
		}
	}
	sm = nt.peers[2].(*raft)
	if sm.state != leader {
		t.Errorf("peer 2 state: %s, want %s", sm.state, leader)
	}
	sm = nt.peers[3].(*raft)
	if sm.state != follower {
		t.Errorf("peer 3 state: %s, want %s", sm.state, follower)
	}
}

func TestRaftHelperMethods(t *testing.T) {
	v := NodeID(100)
	v2 := ClusterID(100)
	if v != "n00100" || v2 != "c00100" {
		t.Errorf("unexpected node id / cluster id value")
	}
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	r.checkHandlerMap()
	addrMap := make(map[uint64]string)
	addrMap[1] = "address1"
	addrMap[2] = "address2"
	addrMap[3] = "address3"
	r.dumpRaftInfoToLog(addrMap)
	status := getLocalStatus(r)
	if status.IsLeader() || !status.IsFollower() || status.NodeID != 1 {
		t.Errorf("unexpected status value")
	}
}

func TestBecomeFollowerDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	if r.term != 2 {
		t.Errorf("term not set")
	}
	if r.leaderID != 3 {
		t.Errorf("leader id not set")
	}
	if r.state != follower {
		t.Errorf("not become follower")
	}
}

func TestBecomeCandidatePanicWhenNodeIsLeader(t *testing.T) {
	ready := false
	defer func() {
		if r := recover(); r != nil {
			if !ready {
				t.Errorf("panic too early")
			}
			return
		}
		t.Errorf("not panic")
	}()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	r.becomeCandidate()
	r.becomeLeader()
	ready = true
	r.becomeCandidate()
}

func TestBecomeLeaderPanicWhenNodeIsFollower(t *testing.T) {
	ready := false
	defer func() {
		if r := recover(); r != nil {
			if !ready {
				t.Errorf("panic too early")
			}
			return
		}
		t.Errorf("not panic")
	}()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	ready = true
	r.becomeLeader()
}

func TestBecomeCandidateDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	term := r.term
	r.becomeCandidate()
	if r.term != term+1 {
		t.Errorf("term didn't increase")
	}
	if r.state != candidate {
		t.Errorf("not in candidate state")
	}
	if r.vote != r.nodeID {
		t.Errorf("vote not set")
	}
}

func TestObserverWillNotStartElection(t *testing.T) {
	p := newTestObserver(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isObserver() {
		t.Errorf("not an observer")
	}
	if len(p.remotes) != 0 {
		t.Errorf("p.romotes len: %d", len(p.remotes))
	}
	for i := uint64(0); i < p.randomizedElectionTimeout*10; i++ {
		p.tick()
	}
	// gRequestVote won't be sent
	if len(p.msgs) != 0 {
		t.Errorf("unexpected msg found %+v", p.msgs)
	}
}

func TestObserverWillNotVoteInElection(t *testing.T) {
	p := newTestObserver(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isObserver() {
		t.Errorf("not an observer")
	}
	p.Handle(pb.Message{From: 2, To: 1, Type: pb.RequestVote, LogTerm: 100, LogIndex: 100})
	if len(p.msgs) != 0 {
		t.Errorf("observer is voting")
	}
}

func TestObserverCanBePromotedToVotingMember(t *testing.T) {
	p := newTestObserver(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isObserver() {
		t.Errorf("not an observer")
	}
	p.addNode(1)
	if p.isObserver() {
		t.Errorf("not promoted to regular node")
	}
	if len(p.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p.remotes))
	}
	if len(p.observers) != 0 {
		t.Errorf("observers len: %d, want 0", len(p.observers))
	}
}

func TestObserverCanActAsRegularNodeAfterPromotion(t *testing.T) {
	p := newTestObserver(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isObserver() {
		t.Errorf("not an observer")
	}
	p.addNode(1)
	if p.isObserver() {
		t.Errorf("not promoted to regular node")
	}
	for i := uint64(0); i <= p.randomizedElectionTimeout; i++ {
		p.tick()
	}
	if p.state != leader {
		t.Errorf("failed to start election")
	}
}

func TestObserverReplication(t *testing.T) {
	p1 := newTestObserver(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestObserver(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isObserver() {
		t.Errorf("p1 is still observer")
	}
	if !p2.isObserver() {
		t.Errorf("p2 is not observer")
	}
	nt := newNetwork(p1, p2)
	if len(p1.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p1.remotes))
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		p1.tick()
	}
	if p1.state != leader {
		t.Errorf("failed to start election")
	}
	committed := p1.log.committed
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})
	if committed+1 != p1.log.committed {
		t.Errorf("entry not committed")
	}
	// the no-op blank entry appended after p1 becomes the leader is also replicated
	if committed+1 != p2.log.committed {
		t.Errorf("entry not committed on observer: %d", p2.log.committed)
	}
	if committed+1 != p1.observers[2].match {
		t.Errorf("match value not expected: %d", p1.observers[2].match)
	}
}

func TestObserverCanPropose(t *testing.T) {
	p1 := newTestObserver(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestObserver(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isObserver() {
		t.Errorf("p1 is still observer")
	}
	if !p2.isObserver() {
		t.Errorf("p2 is not observer")
	}
	nt := newNetwork(p1, p2)
	if len(p1.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p1.remotes))
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if p1.state != leader {
		t.Errorf("failed to start election")
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		p1.tick()
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	if !p2.isObserver() {
		t.Errorf("not observer")
	}
	committed := p1.log.committed
	for i := 0; i < 10; i++ {
		nt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})
	}
	if committed+10 != p1.log.committed {
		t.Errorf("entry not committed")
	}
	// the no-op blank entry appended after p1 becomes the leader is also replicated
	if committed+10 != p2.log.committed {
		t.Errorf("entry not committed on observer: %d", p2.log.committed)
	}
	if committed+10 != p1.observers[2].match {
		t.Errorf("match value not expected: %d", p1.observers[2].match)
	}
}

func TestObserverCanReadIndexQuorum1(t *testing.T) {
	p1 := newTestObserver(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestObserver(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isObserver() {
		t.Errorf("p1 is still observer")
	}
	if !p2.isObserver() {
		t.Errorf("p2 is not observer")
	}
	nt := newNetwork(p1, p2)
	if len(p1.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p1.remotes))
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if p1.state != leader {
		t.Errorf("failed to start election")
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		p1.tick()
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	if !p2.isObserver() {
		t.Errorf("not observer")
	}
	committed := p1.log.committed
	for i := 0; i < 10; i++ {
		nt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})
	}
	if committed+10 != p1.log.committed {
		t.Errorf("entry not committed")
	}
	nt.send(pb.Message{From: 2, To: 2, Type: pb.ReadIndex, Hint: 12345})
	if len(p2.readyToRead) != 1 {
		t.Fatalf("ready to read len is 0")
	}
	if p2.readyToRead[0].Index != p1.log.committed {
		t.Errorf("unexpected ready to read index")
	}
}

func TestObserverCanReadIndexQuorum2(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestRaft(2, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p3 := newTestObserver(3, []uint64{1, 2}, []uint64{3}, 10, 1, NewTestLogDB())
	p1.addObserver(3)
	p2.addObserver(3)
	nt := newNetwork(p1, p2, p3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if p1.state != leader {
		t.Errorf("failed to start election")
	}
	if p2.state != follower {
		t.Errorf("not a follower")
	}
	if !p3.isObserver() {
		t.Errorf("not an observer")
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		p1.tick()
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	committed := p1.log.committed
	for i := 0; i < 10; i++ {
		nt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})
	}
	if committed+10 != p1.log.committed {
		t.Errorf("entry not committed")
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.ReadIndex, Hint: 12345})
	if len(p3.readyToRead) != 1 {
		t.Fatalf("ready to read len is not 1")
	}
	if p3.readyToRead[0].Index != p1.log.committed {
		t.Errorf("unexpected ready to read index")
	}
}

func TestObserverCanReceiveSnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Observers: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestObserver(3, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	if !p1.isObserver() {
		t.Errorf("not an observer")
	}
	p1.Handle(pb.Message{From: 2, To: 1, Type: pb.InstallSnapshot, Snapshot: ss})
	if p1.log.committed != 20 {
		t.Errorf("snapshot not applied")
	}
}

func TestObserverCanReceiveHeartbeatMessage(t *testing.T) {
	p1 := newTestObserver(2, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	m := pb.Message{
		From:     1,
		To:       2,
		Type:     pb.Replicate,
		LogIndex: 0,
		LogTerm:  0,
		Commit:   0,
		Entries:  make([]pb.Entry, 0),
	}
	m.Entries = append(m.Entries, pb.Entry{Index: 1, Term: 1, Cmd: []byte("test-data1")})
	m.Entries = append(m.Entries, pb.Entry{Index: 2, Term: 1, Cmd: []byte("test-data2")})
	m.Entries = append(m.Entries, pb.Entry{Index: 3, Term: 1, Cmd: []byte("test-data3")})
	p1.Handle(m)
	if p1.log.lastIndex() != 3 {
		t.Errorf("last index unexpected: %d, want 3", p1.log.lastIndex())
	}
	if p1.log.committed != 0 {
		t.Errorf("unexpected committed value %d, want 0", p1.log.committed)
	}
	hbm := pb.Message{
		Type:   pb.Heartbeat,
		Commit: 3,
		From:   1,
		To:     2,
	}
	p1.Handle(hbm)
	if p1.log.committed != 3 {
		t.Errorf("unexpected committed value %d, want 3", p1.log.committed)
	}
}

func TestObserverCanBeRestored(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Observers: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.Observers[3] = "a3"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestObserver(3, []uint64{1, 2}, []uint64{3}, 10, 1, NewTestLogDB())
	if ok := p1.restore(ss); !ok {
		t.Errorf("failed to restore")
	}
}

func TestObserverCanBePromotedBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Observers: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestObserver(1, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	if !p1.isObserver() {
		t.Errorf("not an observer")
	}
	if ok := p1.restore(ss); !ok {
		t.Errorf("failed to restore")
	}
	p1.restoreRemotes(ss)
	if p1.isObserver() {
		t.Errorf("observer not promoted")
	}
}

func TestObserverCanNotMoveNodeBackToObserverBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Observers: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.Observers[3] = "a3"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	defer func() {
		if r := recover(); r == nil {
			panic("restore didn't cause panic")
		}
	}()
	if ok := p1.restore(ss); ok {
		t.Errorf("restore unexpectedly completed")
	}
}

func TestObserverCanBeAdded(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	if len(p1.observers) != 0 {
		t.Errorf("unexpected observer record")
	}
	p1.addObserver(2)
	if len(p1.observers) != 1 {
		t.Errorf("observer not added")
	}
	if p1.isObserver() {
		t.Errorf("unexpectedly changed to observer")
	}
}

func TestObserverCanBeRemoved(t *testing.T) {
	p1 := newTestObserver(1, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	if len(p1.observers) != 1 {
		t.Errorf("unexpected observer count")
	}
	p1.removeNode(2)
	if len(p1.observers) != 0 {
		t.Errorf("observer not removed")
	}
}

func TestFollowerTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	for i := 0; i < 9; i++ {
		if r.timeForElection() {
			t.Errorf("time for election unexpected became true")
		}
		r.tick()
	}
	if len(r.msgs) != 1 {
		t.Fatalf("unexpected message count %+v", r.msgs)
	}
	if r.msgs[0].Type != pb.RequestVote {
		t.Errorf("gRequestVote not sent")
	}
}

func TestLeaderTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	for i := 0; i < 10; i++ {
		r.tick()
	}
	if len(r.msgs) != 10 {
		t.Errorf("unexpected msg count")
	}
	for _, msg := range r.msgs {
		if msg.Type != pb.Heartbeat {
			t.Errorf("unexpected msg type")
		}
	}
}

func TestTimeForElection(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	if r.randomizedElectionTimeout < 5 || r.randomizedElectionTimeout > 10 {
		t.Errorf("unexpected randomized election timeout %d",
			r.randomizedElectionTimeout)
	}
	r.electionTick = r.randomizedElectionTimeout - 1
	if r.timeForElection() {
		t.Errorf("unexpected time for election result")
	}
	r.electionTick = r.randomizedElectionTimeout
	if !r.timeForElection() {
		t.Errorf("time for election result didn't return true")
	}
}

func TestLeaderChecksQuorumEveryElectionTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.checkQuorum = true
	for i := 0; i < 5; i++ {
		r.tick()
	}
	if r.state == leader {
		t.Errorf("leader didn't step down")
	}
}

func TestQuiescedTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	for i := 0; i < 200; i++ {
		r.quiescedTick()
	}
	if len(r.msgs) != 0 {
		t.Errorf("unexpectedly generated outgoing message")
	}
	r = newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	for i := 0; i < 200; i++ {
		r.quiescedTick()
	}
	if len(r.msgs) != 0 {
		t.Errorf("unexpectedly generated outgoing message")
	}
}

func TestSetRandomizedElectionTimeout(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	for i := 0; i < 100; i++ {
		r.setRandomizedElectionTimeout()
		if r.randomizedElectionTimeout < 5 && r.randomizedElectionTimeout > 10 {
			t.Errorf("unexpected randomizedElectionTimeout value, %d", r.randomizedElectionTimeout)
		}
	}
}

func TestMultiNodeClusterCampaign(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	r.campaign()
	if len(r.msgs) != 2 {
		t.Fatalf("unexpected message count")
	}
	for _, msg := range r.msgs {
		if msg.Type != pb.RequestVote {
			t.Errorf("unexpected message %+v", msg)
		}
	}
	if r.state != candidate {
		t.Errorf("unexpected state %s", r.state)
	}
	if r.vote != 1 {
		t.Errorf("vote not recorded")
	}
}

func TestSingleNodeClusterCampaign(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.campaign()
	if len(r.msgs) != 0 {
		t.Fatalf("unexpected message count")
	}
	if r.state != leader {
		t.Errorf("didn't become leader")
	}
}

func TestNoOPWithHigherTermIsSentToLeaderWhenLeaderHasLowerTerm(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	r.checkQuorum = true
	r.Handle(pb.Message{Type: pb.Heartbeat, Term: 2})
	msgs := r.msgs
	if len(msgs) == 0 {
		t.Fatalf("no message sent")
	}
	m := msgs[0]
	if m.Type != pb.NoOP {
		t.Errorf("no gNoOP sent")
	}
	if m.Term != 10 {
		t.Errorf("gNoOP not sent with high term")
	}
}

func TestIsLeaderMessage(t *testing.T) {
	msgs := []struct {
		mt     pb.MessageType
		leader bool
	}{
		{pb.Election, false},
		{pb.LeaderHeartbeat, false},
		{pb.Propose, false},
		{pb.SnapshotStatus, false},
		{pb.Unreachable, false},
		{pb.CheckQuorum, false},
		{pb.BatchedReadIndex, false},
		{pb.LocalTick, false},
		{pb.Replicate, true},
		{pb.ReplicateResp, false},
		{pb.RequestVote, false},
		{pb.RequestVoteResp, false},
		{pb.InstallSnapshot, true},
		{pb.Heartbeat, true},
		{pb.HeartbeatResp, false},
		{pb.ReadIndex, false},
		{pb.ReadIndexResp, true},
		{pb.Quiesce, false},
		{pb.ConfigChangeEvent, false},
		{pb.Ping, false},
		{pb.Pong, false},
		{pb.SnapshotReceived, false},
		{pb.LeaderTransfer, false},
		{pb.TimeoutNow, true},
		{pb.NoOP, false},
	}

	for _, tt := range msgs {
		if res := isLeaderMessage(tt.mt); res != tt.leader {
			t.Errorf("%s, result %t, want %t", tt.mt, res, tt.leader)
		}
	}
}

func TestDropRequestVoteMessageFromRemovedNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)

	tests := []struct {
		checkQuorum bool
		term        uint64
		drop        bool
	}{
		{false, 20, false},
		{true, 20, true},
		{true, 10, false},
	}
	for idx, tt := range tests {
		r.checkQuorum = tt.checkQuorum
		m := pb.Message{
			Type: pb.RequestVote,
			Term: tt.term,
			From: 2,
		}
		if r.dropRequestVoteFromHighTermNode(m) != tt.drop {
			t.Errorf("%d, got %t, want %t", idx, r.dropRequestVoteFromHighTermNode(m), tt.drop)
		}
	}
}

func TestOnMessageTermNotMatched(t *testing.T) {
	tests := []struct {
		checkQuorum bool
		term        uint64
		msgType     pb.MessageType
		leaderID    uint64
		notMatched  bool
		msgCount    int
	}{
		{false, 20, pb.Replicate, 3, false, 0},
		{false, 20, pb.ReplicateResp, NoLeader, false, 0},
		{false, 10, pb.Replicate, 2, false, 0},
		{false, 10, pb.ReplicateResp, 2, false, 0},
		{true, 5, pb.Replicate, 2, true, 1},
		{true, 5, pb.ReplicateResp, 2, true, 0},
		{false, 5, pb.ReplicateResp, 2, true, 0},
	}
	for idx, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
		r.becomeFollower(10, 2)
		r.checkQuorum = tt.checkQuorum
		msg := pb.Message{
			Type: tt.msgType,
			Term: tt.term,
			From: 3,
		}
		if r.onMessageTermNotMatched(msg) != tt.notMatched {
			t.Fatalf("%d, incorrect not matched result", idx)
		}
		if r.leaderID != tt.leaderID {
			t.Errorf("%d, leader ID %d, want %d", idx, r.leaderID, tt.leaderID)
		}
		if len(r.msgs) != tt.msgCount {
			t.Errorf("%d, msg count %d, want %d", idx, len(r.msgs), tt.msgCount)
		}
	}
}

func testZeroTermRequestVoteMessageCausePanic(t *testing.T, r *raft) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r.finalizeMessageTerm(pb.Message{Type: pb.RequestVote})
}

func testNonZeroTermOtherMessageCausePanic(t *testing.T, r *raft) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r.finalizeMessageTerm(pb.Message{Type: pb.NoOP, Term: 1})
}

func TestFinalizeMessageTerm(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	testZeroTermRequestVoteMessageCausePanic(t, r)
	testNonZeroTermOtherMessageCausePanic(t, r)
	msg := r.finalizeMessageTerm(pb.Message{Type: pb.NoOP})
	if msg.Term != 10 {
		t.Errorf("term not set")
	}
}

func TestSendSetMessageFromAndTerm(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	r.send(pb.Message{Type: pb.NoOP})
	if len(r.msgs) != 1 {
		t.Errorf("message not sent")
	}
	m := r.msgs[0]
	if m.From != 1 || m.Term != 10 {
		t.Errorf("from or term not set")
	}
}

func TestMakeInstallSnapshotMessage(t *testing.T) {
	st := NewTestLogDB()
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, st)
	r.becomeCandidate()
	r.becomeLeader()
	ss := pb.Snapshot{Index: 100, Term: 2}
	if err := st.ApplySnapshot(ss); err != nil {
		t.Errorf("apply snapshot failed %v", err)
	}
	msg := pb.Message{}
	if idx := r.makeInstallSnapshotMessage(2, &msg); idx != 100 {
		t.Errorf("unexpected index %d", idx)
	}
	if msg.Type != pb.InstallSnapshot || msg.Snapshot.Index != 100 ||
		msg.Snapshot.Term != 2 {
		t.Errorf("unexpected message values")
	}
}

func TestMakeReplicateMessage(t *testing.T) {
	st := NewTestLogDB()
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, st)
	r.becomeCandidate()
	r.becomeLeader()
	noop := pb.Entry{}
	ents := []pb.Entry{
		{Index: 2, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Index: 3, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	sz := noop.SizeUpperLimit() + ents[0].SizeUpperLimit() + ents[1].SizeUpperLimit() + 1
	msg, err := r.makeReplicateMessage(2, 1, uint64(sz))
	if err != nil {
		t.Errorf("make Replicate failed %v", err)
	}
	if msg.Type != pb.Replicate || msg.To != 2 {
		t.Errorf("failed to make Replicate")
	}
	// NoOP entry plus two above
	if len(msg.Entries) != 3 {
		t.Errorf("unexpected entry list length, got %d, want 3, %+v",
			len(msg.Entries), msg.Entries)
	}
	noopEntry := pb.Entry{Index: 1, Term: 1, Type: pb.ApplicationEntry}
	noopEntrySize := uint64(noopEntry.SizeUpperLimit())
	msg, err = r.makeReplicateMessage(2, 1, noopEntrySize+uint64(ents[1].SizeUpperLimit()))
	if err != nil {
		t.Errorf("failed to get gReplicate")
	}
	if len(msg.Entries) != 2 {
		t.Errorf("unexpected entry list length, %+v", msg)
	}
}

func TestBroadcastReplicateMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.broadcastReplicateMessage()
	count := 0
	for _, msg := range r.msgs {
		if msg.Type == pb.Replicate {
			count++
		}
	}
	if count != 2 {
		t.Errorf("unexpected gReplicate count %d", count)
	}
}

func TestBroadcastHeartbeatMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.broadcastHeartbeatMessage()
	count := 0
	for _, msg := range r.msgs {
		if msg.Type == pb.Heartbeat {
			count++
		}
	}
	if count != 2 {
		t.Errorf("unexpected gReplicate count %d", count)
	}
}

func TestBroadcastHeartbeatMessageWithHint(t *testing.T) {
	ctx := pb.SystemCtx{
		Low:  101,
		High: 1001,
	}
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.broadcastHeartbeatMessageWithHint(ctx)
	count := 0
	for _, msg := range r.msgs {
		if msg.Type == pb.Heartbeat {
			count++
		}
		if msg.Hint != ctx.Low || msg.HintHigh != ctx.High {
			t.Errorf("ctx not carried in the message")
		}
	}
	if count != 2 {
		t.Errorf("unexpected gReplicate count %d", count)
	}
}

func TestSendTimeoutNowMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.sendTimeoutNowMessage(2)
	if len(r.msgs) != 1 {
		t.Fatalf("msg count is not 1")
	}
	if r.msgs[0].Type != pb.TimeoutNow || r.msgs[0].To != 2 {
		t.Errorf("unexpected message, %+v", r.msgs[0])
	}
}

func TestSendHeartbeatMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	hint := pb.SystemCtx{Low: 100, High: 200}
	r.remotes[2].match = 100
	r.log.committed = 200
	r.sendHeartbeatMessage(2, hint, false)
	msgs := r.msgs
	if len(msgs) != 1 {
		t.Fatalf("unexpected msgs list length")
	}
	m := msgs[0]
	if m.Type != pb.Heartbeat || m.Commit != 100 || m.Hint != 100 || m.HintHigh != 200 {
		t.Errorf("unexpected msg %+v", m)
	}
}

func TestQuorumValue(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	if r.quorum() != 1 {
		t.Errorf("quorum %d", r.quorum())
	}
	r = newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	if r.quorum() != 2 {
		t.Errorf("quorum %d", r.quorum())
	}
	r = newTestRaft(1, []uint64{1, 2, 3, 4, 5}, 5, 1, NewTestLogDB())
	if r.quorum() != 3 {
		t.Errorf("quorum %d", r.quorum())
	}
}

func TestIsSingleNodeQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	if !r.isSingleNodeQuorum() {
		t.Errorf("is single node returned incorrect result")
	}
	r = newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	if r.isSingleNodeQuorum() {
		t.Errorf("is single node returned incorrect result")
	}
}

func TestAppendEntries(t *testing.T) {
	st := NewTestLogDB()
	r := newTestRaft(1, []uint64{1}, 5, 1, st)
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Index: 2, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Index: 3, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	if r.log.committed != 3 {
		t.Errorf("unexpected committed value %d", r.log.committed)
	}
	if r.remotes[1].match != 3 {
		t.Errorf("remotes' match is not updated %d", r.remotes[1].match)
	}
}

// got hit twice on ResetRemotes, just unbelievable...
func TestResetRemotes(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	r.remotes[1].next = 100
	r.remotes[1].match = 50
	r.remotes[2].next = 200
	r.remotes[2].match = 52
	r.remotes[3].next = 300
	r.remotes[3].match = 53
	r.resetRemotes()
	if r.remotes[1].next != 4 || r.remotes[2].next != 4 || r.remotes[3].next != 4 {
		t.Errorf("unexpected next value, %+v", r.remotes)
	}
	if r.remotes[1].match != 3 {
		t.Errorf("unexpected match value, %d", r.remotes[1].match)
	}
}

func TestSelfRemoved(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	if r.selfRemoved() {
		t.Errorf("unexpectedly self removed")
	}
	delete(r.remotes, 1)
	if !r.selfRemoved() {
		t.Errorf("self removed not report removed")
	}
}

func TestSetRemote(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.setRemote(4, 100, 101)
	if len(r.remotes) != 4 {
		t.Errorf("remote not set")
	}
	_, ok := r.remotes[4]
	if !ok {
		t.Errorf("node 4 not added")
	}
	if len(r.matched) != 4 {
		t.Errorf("match values not reset")
	}
}

func TestDeleteRemote(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.deleteRemote(3)
	if len(r.matched) != 2 {
		t.Errorf("match values not deleted")
	}
	if len(r.remotes) != 2 {
		t.Errorf("remote not deleted")
	}
	_, ok := r.remotes[3]
	if ok {
		t.Errorf("node 3 not deleted")
	}
}

func TestCampaignSendExpectedMessages(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	r.becomeFollower(r.term+1, NoLeader)
	r.campaign()
	msgs := r.msgs
	if len(msgs) != 2 {
		t.Errorf("unexpected message count")
	}
	for idx, m := range msgs {
		if m.Type != pb.RequestVote || m.LogIndex != 3 || m.LogTerm != 1 {
			t.Errorf("%d, unexpected msg %+v", idx, m)
		}
	}
}

func TestHandleVoteResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	v1 := r.handleVoteResp(1, false)
	v2 := r.handleVoteResp(2, true)
	v3 := r.handleVoteResp(3, false)
	v4 := r.handleVoteResp(2, false)
	if v1 != 1 || v2 != 1 || v3 != 2 || v4 != 2 {
		t.Errorf("unexpected count")
	}
}

func TestCanGrantVote(t *testing.T) {
	from := uint64(2)
	term := uint64(1)
	tests := []struct {
		vote uint64
		mt   uint64
		ok   bool
	}{
		{NoNode, 1, true},
		{NoNode, 2, true},
		{from, 1, true},
		{from, 2, true},
		{3, 1, false},
		{3, 5, true},
	}

	for idx, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
		r.term = term
		r.vote = tt.vote
		msg := pb.Message{From: from, Term: tt.mt}
		ok := r.canGrantVote(msg)
		if ok != tt.ok {
			t.Errorf("%d, unexpected can grant vote result", idx)
		}
	}
}

func TestHasConfigChangeToApply(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.hasNotAppliedConfigChange = nil
	r.log.committed = 10
	r.setApplied(5)
	if !r.hasConfigChangeToApply() {
		t.Errorf("unexpected r.hasConfigChangeToApply result")
	}
	r.setApplied(10)
	if r.hasConfigChangeToApply() {
		t.Errorf("unexpected r.hasConfigChangeToApply result")
	}
	r.setApplied(12)
	if r.hasConfigChangeToApply() {
		t.Errorf("unexpected r.hasConfigChangeToApply result")
	}
}

func TestPendingConfigChangeFlag(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	if r.hasPendingConfigChange() {
		t.Errorf("unexpected value")
	}
	r.setPendingConfigChange()
	if !r.hasPendingConfigChange() {
		t.Errorf("flag not set")
	}
	r.clearPendingConfigChange()
	if r.hasPendingConfigChange() {
		t.Errorf("unexpected value")
	}
}

func TestGetPendingConfigChangeCount(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	for i := 0; i < 5; i++ {
		ents := []pb.Entry{
			{Type: pb.ApplicationEntry, Cmd: make([]byte, maxEntriesToApplySize)},
			{Type: pb.ConfigChangeEntry},
		}
		r.appendEntries(ents)
	}
	count := r.getPendingConfigChangeCount()
	if count != 5 {
		t.Errorf("count %d, want 5", count)
	}
}

func TestIsRequestLeaderMessage(t *testing.T) {
	tests := []struct {
		msgType   pb.MessageType
		reqMsg    bool
		leaderMsg bool
	}{
		{pb.Propose, true, false},
		{pb.ReadIndex, true, false},
		{pb.Replicate, false, true},
		{pb.ReplicateResp, false, false},
		{pb.RequestVote, false, false},
		{pb.RequestVoteResp, false, false},
		{pb.Heartbeat, false, true},
		{pb.HeartbeatResp, false, false},
		{pb.InstallSnapshot, false, true},
		{pb.ReadIndexResp, false, true},
		{pb.TimeoutNow, false, true},
		{pb.LeaderTransfer, false, false},
	}
	for _, tt := range tests {
		if isRequestMessage(tt.msgType) != tt.reqMsg {
			t.Errorf("incorrect is request message result %s", tt.msgType)
		}
		if isLeaderMessage(tt.msgType) != tt.leaderMsg {
			t.Errorf("incorrect is leader message result %s", tt.msgType)
		}
	}
}

func TestAddNodeDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ConfigChangeEntry},
	}
	r.setPendingConfigChange()
	r.appendEntries(ents)
	if !r.hasPendingConfigChange() {
		t.Errorf("pending config change flag not set")
	}
	r.addNode(3)
	if r.hasPendingConfigChange() {
		t.Errorf("pending config change flag not cleared")
	}
	if len(r.remotes) != 3 {
		t.Errorf("remotes not expanded")
	}
	if r.remotes[3].match != 0 || r.remotes[3].next != 5 {
		t.Errorf("unexpected remotes %+v", r.remotes[3])
	}
}

func TestRemoveNodeDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.setPendingConfigChange()
	r.removeNode(2)
	if len(r.remotes) != 1 || len(r.matched) != 1 {
		t.Errorf("remotes and matched not resized")
	}
	if r.hasPendingConfigChange() {
		t.Errorf("pending config change flag not cleared")
	}
}

func TestHasCommittedEntryAtCurrentTerm(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(1, NoLeader)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpectedly set hasCommittedEntryAtCurrentTerm")
	}
	r.becomeCandidate()
	r.becomeLeader()
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
	r.remotes[2].tryUpdate(r.log.lastIndex())
	if !r.tryCommit() {
		t.Errorf("failed to commit")
	}
	if !r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
}

func TestHandleLeaderCheckQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.handleLeaderCheckQuorum(pb.Message{})
	if r.state != follower {
		t.Errorf("node didn't step down")
	}
	r = newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.remotes[1].setActive()
	r.remotes[2].setActive()
	r.handleLeaderCheckQuorum(pb.Message{})
	if r.state != leader {
		t.Errorf("node didn't step down")
	}
}

func TestReadyToReadList(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	if len(r.readyToRead) != 0 {
		t.Errorf("unexpected initial count")
	}
	r.addReadyToRead(100, pb.SystemCtx{})
	r.addReadyToRead(200, pb.SystemCtx{})
	r.addReadyToRead(300, pb.SystemCtx{})
	if len(r.readyToRead) != 3 {
		t.Errorf("unexpected count")
	}
	r.clearReadyToRead()
	if len(r.readyToRead) != 0 {
		t.Errorf("unexpected count")
	}
}

func TestRestoreSnapshotIgnoreDelayedSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	ss := pb.Snapshot{Index: 3, Term: 1}
	if r.restore(ss) {
		t.Errorf("nothing to restore")
	}
}

func TestSnapshotCommitEntries(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	if r.log.committed != 0 {
		t.Errorf("unexpected commit, %d", r.log.committed)
	}
	ss := pb.Snapshot{Index: 2, Term: 1}
	if r.restore(ss) {
		t.Errorf("not expect to restore")
	}
	if r.log.committed != 2 {
		t.Errorf("commit not moved")
	}
}

func TestSnapshotCanBeRestored(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	ss := pb.Snapshot{Index: 4, Term: 1}
	if !r.restore(ss) {
		t.Errorf("snapshot unexpectedly ignored")
	}
	if r.log.lastIndex() != 4 {
		t.Errorf("last index not moved, %d", r.log.lastIndex())
	}
	if r.log.lastTerm() != 1 {
		t.Errorf("last term %d", r.log.lastTerm())
	}
}

func TestRestoreRemote(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	r.appendEntries(ents)
	ss := pb.Snapshot{}
	ss.Membership.Addresses = make(map[uint64]string)
	ss.Membership.Addresses[1] = ""
	ss.Membership.Addresses[2] = ""
	ss.Membership.Addresses[3] = ""
	r.restoreRemotes(ss)
	if len(r.remotes) != 3 {
		t.Errorf("remotes length unexpected %d", len(r.remotes))
	}
	if len(r.matched) != 3 {
		t.Errorf("matchValue not reset")
	}
	for nid, rm := range r.remotes {
		if nid == 1 {
			if rm.match != 3 {
				t.Errorf("match not moved, %d", rm.match)
			}
		}
		if rm.next != 4 {
			t.Errorf("next not moved, %d", rm.next)
		}
	}
}

func TestAppliedValueCanBeSet(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.setApplied(12345)
	if r.getApplied() != 12345 {
		t.Errorf("applied value not set")
	}
}

func TestUnrolledBubbleSortMatchValue(t *testing.T) {
	tests := []struct {
		vals []uint64
	}{
		{[]uint64{1, 1, 1}},
		{[]uint64{1, 1, 2}},
		{[]uint64{1, 2, 2}},
		{[]uint64{2, 3, 1}},
		{[]uint64{3, 2, 1}},
		{[]uint64{3, 3, 1}},
	}
	for idx, tt := range tests {
		r := &raft{matched: make([]uint64, len(tt.vals))}
		copy(r.matched, tt.vals)
		r.sortMatchValues()
		sort.Slice(tt.vals, func(i, j int) bool {
			return tt.vals[i] < tt.vals[j]
		})
		if !reflect.DeepEqual(tt.vals, r.matched) {
			t.Errorf("%d, sort failed, %v, want %v", idx, r.matched, tt.vals)
		}
	}
}

func TestDoubleCheckTermMatched(t *testing.T) {
	r := raft{term: 10}
	r.doubleCheckTermMatched(0)
	r.doubleCheckTermMatched(10)
	tt := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("panic not triggered")
			}
		}()
		r.doubleCheckTermMatched(1)
	}
	tt()
}

func TestEnterRetryState(t *testing.T) {
	tests := []struct {
		initType  remoteStateType
		finalType remoteStateType
	}{
		{remoteRetry, remoteRetry},
		{remoteReplicate, remoteRetry},
		{remoteSnapshot, remoteSnapshot},
	}
	for idx, tt := range tests {
		r := raft{}
		rm := &remote{state: tt.initType}
		r.enterRetryState(rm)
		if rm.state != tt.finalType {
			t.Errorf("%d, unexpected type %s, want %s", idx, r.state, tt.finalType)
		}
	}
}

func TestHandleCandidatePropose(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	msg := pb.Message{
		Type:    pb.Replicate,
		Entries: []pb.Entry{{Cmd: []byte("test-data")}},
	}
	r.handleCandidatePropose(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpectedly sent message")
	}
}

func TestCandidateBecomeFollowerOnRecivingLeaderMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	tests := []struct {
		fn      func(msg pb.Message)
		msgType pb.MessageType
	}{
		{r.handleCandidateReplicate, pb.Replicate},
		{r.handleCandidateInstallSnapshot, pb.InstallSnapshot},
		{r.handleCandidateHeartbeat, pb.Heartbeat},
	}
	for _, tt := range tests {
		r.becomeCandidate()
		r.term = 2
		if r.leaderID != NoLeader {
			t.Errorf("leader not cleared")
		}
		msg := pb.Message{
			Type: tt.msgType,
			From: 2,
		}
		tt.fn(msg)
		if r.state != follower {
			t.Errorf("not a follower")
		}
		if r.term != 2 {
			t.Errorf("term changed, term %d want 2", r.term)
		}
		if r.leaderID != 2 {
			t.Errorf("leader id not set")
		}
	}
}

func TestHandleCandidateHeartbeat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	msg := pb.Message{
		Type:   pb.Heartbeat,
		From:   2,
		Commit: 3,
	}
	ents := make([]pb.Entry, 0)
	for i := uint64(0); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	// yes, this is a bit hacky
	r.log.inmem.merge(ents)
	r.handleCandidateHeartbeat(msg)
	if r.log.committed != 3 {
		t.Errorf("committed %d, want 3", r.log.committed)
	}
}

func TestHandleCandidateInstallSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ents := make([]pb.Entry, 0)
	for i := uint64(0); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	r.log.inmem.merge(ents)
	ss := pb.Snapshot{
		Index: 10,
		Term:  1,
	}
	m := pb.Message{From: 2, To: 1, Type: pb.InstallSnapshot, Snapshot: ss}
	r.handleCandidateInstallSnapshot(m)
	if r.log.committed != 10 {
		t.Errorf("gSnapshot not applied")
	}
}

func TestHandleCandidateReplicate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ents := make([]pb.Entry, 0)
	for i := uint64(1); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	m := pb.Message{
		LogIndex: 0,
		LogTerm:  0,
		Entries:  ents,
		Type:     pb.Replicate,
		Term:     0,
	}
	r.handleCandidateReplicate(m)
	if r.log.lastIndex() != ents[len(ents)-1].Index {
		t.Errorf("entries not appended, last index %d, want %d",
			r.log.lastIndex(), ents[len(ents)-1].Index)
	}
}

func TestHandleCandidateRequestVoteResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	for i := uint64(1); i <= uint64(3); i++ {
		m := pb.Message{
			Type:   pb.RequestVoteResp,
			From:   i,
			Reject: false,
		}
		r.handleCandidateRequestVoteResp(m)
	}
	if r.state != leader {
		t.Errorf("didn't become leader")
	}
	count := 0
	for _, msg := range r.msgs {
		if msg.Type == pb.Replicate {
			count++
		}
	}
	if count != 2 {
		t.Errorf("gReplicate count %d, want 2", count)
	}
}

func TestHandleCandidateRequestVoteRespRejected(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	for i := uint64(2); i <= uint64(3); i++ {
		m := pb.Message{
			Type:   pb.RequestVoteResp,
			From:   i,
			Reject: true,
		}
		r.handleCandidateRequestVoteResp(m)
	}
	if r.state != follower {
		t.Errorf("didn't become follower")
	}
	if len(r.msgs) != 0 {
		t.Errorf("unexpectedly sent message")
	}
}

func TestFollowerResetElectionTickOnReceivingLeaderMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	tests := []struct {
		fn      func(msg pb.Message)
		msgType pb.MessageType
	}{
		{r.handleFollowerReplicate, pb.Replicate},
		{r.handleFollowerInstallSnapshot, pb.InstallSnapshot},
		{r.handleFollowerHeartbeat, pb.Heartbeat},
		{r.handleFollowerReadIndexResp, pb.ReadIndexResp},
	}
	for _, tt := range tests {
		r.becomeFollower(r.term, 2)
		r.electionTick = 2
		m := pb.Message{
			Type: tt.msgType,
			From: 3,
		}
		tt.fn(m)
		if r.leaderID != 3 {
			t.Errorf("leader id not set")
		}
		if r.electionTick != 0 {
			t.Errorf("election tick not reset")
		}
	}
}

func TestFollowerRedirectReadIndexMessageToLeader(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	m := pb.Message{
		Type: pb.ReadIndex,
	}
	r.handleFollowerReadIndex(m)
	if len(r.msgs) != 1 {
		t.Fatalf("failed to redirect the message")
	}
	if r.msgs[0].Type != pb.ReadIndex || r.msgs[0].To != 2 {
		t.Errorf("unexpected message sent %v", r.msgs[0])
	}
}

func TestHandleFollowerReplicate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	ents := make([]pb.Entry, 0)
	for i := uint64(1); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	m := pb.Message{
		LogIndex: 0,
		LogTerm:  0,
		Entries:  ents,
		Type:     pb.Replicate,
		Term:     0,
	}
	r.handleFollowerReplicate(m)
	if r.log.lastIndex() != ents[len(ents)-1].Index {
		t.Errorf("entries not appended, last index %d, want %d",
			r.log.lastIndex(), ents[len(ents)-1].Index)
	}
}

func TestHandleFollowerHeartbeat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	msg := pb.Message{
		Type:   pb.Heartbeat,
		From:   2,
		Commit: 3,
	}
	ents := make([]pb.Entry, 0)
	for i := uint64(0); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	r.log.inmem.merge(ents)
	r.handleFollowerHeartbeat(msg)
	if r.log.committed != 3 {
		t.Errorf("committed %d, want 3", r.log.committed)
	}
}

func TestHandleFollowerInstallSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	ents := make([]pb.Entry, 0)
	for i := uint64(0); i < uint64(10); i++ {
		ents = append(ents, pb.Entry{Index: i, Term: 1})
	}
	r.log.inmem.merge(ents)
	ss := pb.Snapshot{
		Index: 10,
		Term:  1,
	}
	m := pb.Message{From: 2, To: 1, Type: pb.InstallSnapshot, Snapshot: ss}
	r.handleFollowerInstallSnapshot(m)
	if r.log.committed != 10 {
		t.Errorf("gSnapshot not applied")
	}
}

func TestHandleFollowerReadIndexResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	msg := pb.Message{
		Type:     pb.ReadIndexResp,
		Hint:     101,
		HintHigh: 1002,
		LogIndex: 100,
	}
	r.handleFollowerReadIndexResp(msg)
	if len(r.readyToRead) != 1 {
		t.Fatalf("ready to read not updated")
	}
	rr := r.readyToRead[0]
	if rr.Index != 100 || rr.SystemCtx.Low != 101 || rr.SystemCtx.High != 1002 {
		t.Errorf("unexpected ready to read content")
	}
}

func TestHandleFollowerTimeoutNow(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	m := pb.Message{
		Type: pb.TimeoutNow,
	}
	r.handleFollowerTimeoutNow(m)
	if r.state != candidate {
		t.Errorf("not become candidate")
	}
	if len(r.msgs) != 1 {
		t.Fatalf("no message sent")
	}
	if r.msgs[0].Type != pb.RequestVote {
		t.Errorf("no gRequestVote sent")
	}
}

func TestHandleFollowerLeaderTransfer(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(0, 2)
	m := pb.Message{
		Type: pb.LeaderTransfer,
	}
	r.handleFollowerLeaderTransfer(m)
	if len(r.msgs) != 1 {
		t.Fatalf("gLeaderTransfer not redirected")
	}
	if r.msgs[0].Type != pb.LeaderTransfer || r.msgs[0].To != 2 {
		t.Errorf("unexpected msg")
	}
}

func TestLeaderIgnoreElection(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.Election,
	}
	r.handleNodeElection(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected message sent")
	}
	if r.state != leader {
		t.Errorf("no longer a leader")
	}
}

func TestElectionIgnoredWhenConfigChangeIsPending(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	r.log.committed = 10
	r.setApplied(5)
	r.hasNotAppliedConfigChange = nil
	if !r.hasConfigChangeToApply() {
		t.Fatalf("no config change to apply")
	}
	msg := pb.Message{
		Type: pb.Election,
	}
	r.handleNodeElection(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected message sent")
	}
	if r.state != follower {
		t.Errorf("state change to %s", r.state)
	}
}

func TestHandlegElection(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	msg := pb.Message{
		Type: pb.Election,
	}
	r.handleNodeElection(msg)
	if len(r.msgs) != 1 {
		t.Fatalf("unexpected message count %d", len(r.msgs))
	}
	if r.msgs[0].Type != pb.RequestVote || r.msgs[0].To != 2 {
		t.Errorf("didn't send out gRequestVote")
	}
	if r.state != candidate {
		t.Errorf("not a candidate")
	}
}

func TestHandleLeaderHeartbeatMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.LeaderHeartbeat,
	}
	r.handleLeaderHeartbeat(msg)
	count := 0
	for _, msg := range r.msgs {
		if msg.Type == pb.Heartbeat && (msg.To == 2 || msg.To == 3) {
			count++
		}
	}
	if count != 2 {
		t.Errorf("didn't send heartbeat to all other nodes")
	}
}

func TestLeaderStepDownWithoutQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.CheckQuorum,
	}
	r.handleLeaderCheckQuorum(msg)
	if r.state == leader {
		t.Errorf("leader didn't step down")
	}
}

func TestLeaderIgnoreCheckQuorumWhenHasQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.CheckQuorum,
	}
	for _, rp := range r.remotes {
		rp.setActive()
	}
	r.handleLeaderCheckQuorum(msg)
	if r.state != leader {
		t.Errorf("leader unexpectedly stepped down")
	}
}

func TestHandleLeaderUnreachable(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.Unreachable,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteReplicate
	r.handleLeaderUnreachable(msg, rp)
	if rp.state != remoteRetry {
		t.Errorf("not in retry state")
	}
}

func TestSnapshotStatusMessageIgnoredWhenNotInSnapshotState(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.SnapshotStatus,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteReplicate
	r.handleLeaderSnapshotStatus(msg, rp)
	if rp.state == remoteRetry {
		t.Errorf("unexpectedly in retry state")
	}
}

func TestHandleLeaderSnapshotStatus(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.SnapshotStatus,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteSnapshot
	r.handleLeaderSnapshotStatus(msg, rp)
	if rp.state != remoteWait {
		t.Errorf("not in wait state")
	}
	if !rp.isPaused() {
		t.Errorf("not paused")
	}
}

func TestHandleLeaderTransfer(t *testing.T) {
	tests := []struct {
		target       uint64
		transferring bool
		match        bool
		ignored      bool
	}{
		{1, false, false, true},
		{1, true, false, true},
		{2, false, true, false},
		{2, false, false, true},
		{2, true, false, true},
	}
	for idx, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
		r.becomeCandidate()
		r.becomeLeader()
		msg := pb.Message{
			Type: pb.LeaderTransfer,
			Hint: tt.target,
		}
		if tt.transferring {
			r.leaderTransferTarget = 3
		}
		rp, ok := r.remotes[2]
		if !ok {
			t.Fatalf("failed to get remote")
		}
		if tt.match {
			rp.match = r.log.lastIndex()
		}
		r.handleLeaderTransfer(msg, rp)
		if tt.ignored {
			if len(r.msgs) != 0 {
				t.Errorf("unexpectedly sent msg")
			}
		} else {
			if len(r.msgs) != 1 {
				t.Fatalf("%d, unexpected msg count %d, want 1", idx, len(r.msgs))
			}
			if r.msgs[0].Type != pb.TimeoutNow || r.msgs[0].To != tt.target {
				t.Errorf("unexpected msg %v", r.msgs[0])
			}
		}
	}
}

func TestHandleLeaderHeartbeatResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type: pb.HeartbeatResp,
		From: 2,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Fatalf("failed to get remote")
	}
	rp.setNotActive()
	//rp.pause()
	r.handleLeaderHeartbeatResp(msg, rp)
	if !rp.isActive() {
		t.Errorf("not active")
	}
}

func TestLeaderIgnoreReadIndexWhenSelfRemoved(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	r.deleteRemote(r.nodeID)
	if !r.selfRemoved() {
		t.Fatalf("not self removed")
	}
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	r.handleLeaderReadIndex(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected msg sent")
	}
	if len(r.readyToRead) != 0 {
		t.Errorf("readyToRead has unexpected rec")
	}
	if len(r.readIndex.pending) != 0 || len(r.readIndex.queue) != 0 {
		t.Errorf("unexpected readIndex content")
	}
}
func TestLeaderReadIndexOnSingleNodeCluster(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	r.handleLeaderReadIndex(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected msg sent")
	}
	if len(r.readyToRead) != 1 {
		t.Fatalf("readyToRead has unexpected rec")
	}
	if r.readyToRead[0].Index != r.log.committed ||
		r.readyToRead[0].SystemCtx.Low != 101 ||
		r.readyToRead[0].SystemCtx.High != 1002 {
		t.Errorf("unexpected ready to read stat")
	}
	if len(r.readIndex.pending) != 0 || len(r.readIndex.queue) != 0 {
		t.Errorf("unexpected readIndex content")
	}
}

func TestLeaderIgnoreReadIndexWhenClusterCommittedIsUnknown(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	r.becomeLeader()
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	r.handleLeaderReadIndex(msg)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected msg sent")
	}
	if len(r.readyToRead) != 0 {
		t.Errorf("readyToRead has unexpected rec")
	}
	if len(r.readIndex.pending) != 0 || len(r.readIndex.queue) != 0 {
		t.Errorf("unexpected readIndex content")
	}
}

func TestHandleLeaderReadIndex(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeFollower(1, NoLeader)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpectedly set hasCommittedEntryAtCurrentTerm")
	}
	r.becomeCandidate()
	r.becomeLeader()
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
	r.remotes[2].tryUpdate(r.log.lastIndex())
	if !r.tryCommit() {
		t.Errorf("failed to commit")
	}
	if !r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	count := 0
	r.handleLeaderReadIndex(msg)
	for _, m := range r.msgs {
		if m.Type == pb.Heartbeat && (m.To == 2 || m.To == 3) &&
			m.Hint == 101 && m.HintHigh == 1002 {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected heartbeat messages not sent")
	}
	if len(r.readIndex.pending) != 1 || len(r.readIndex.queue) != 1 {
		t.Errorf("readIndex not updated")
	}
}
