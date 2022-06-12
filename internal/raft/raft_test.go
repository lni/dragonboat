// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/settings"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

//
// Most test here are more focused on individual features/actions in our raft
// implementation. They are more like unit tests. raft_etcd_test.go contains
// tests ported from etcd raft, those tests are more like raft protocol level
// integration tests.
//

func TestInitializeRaft(t *testing.T) {
	m := pb.Membership{
		Addresses: map[uint64]string{
			5: "",
			6: "",
			7: "",
		},
		NonVotings: map[uint64]string{
			3: "",
			4: "",
		},
		Witnesses: map[uint64]string{
			1: "",
			2: "",
		},
		Removed: make(map[uint64]bool),
	}

	logdb := &TestLogDB{
		entries: make([]pb.Entry, 0),
		snapshot: pb.Snapshot{
			Membership: m,
		},
	}

	node := newRaft(newTestConfig(1, 10, 1), logdb)
	if len(node.remotes) != 3 {
		t.Errorf("remotes length not expected: %d", len(node.remotes))
	}
	if len(node.nonVotings) != 2 {
		t.Errorf("nonVotings length not expected: %d", len(node.nonVotings))
	}
	if len(node.witnesses) != 2 {
		t.Errorf("witnesses length not expected: %d", len(node.witnesses))
	}
}

func TestMustBeLeaderPanicWhenNotLeader(t *testing.T) {
	tests := []struct {
		st          State
		shouldPanic bool
	}{
		{follower, true},
		{candidate, true},
		{leader, false},
		{nonVoting, true},
	}
	for idx, tt := range tests {
		r := raft{state: tt.st}
		func() {
			defer func() {
				r := recover()
				if r == nil {
					if tt.shouldPanic {
						t.Errorf("%d, failed to panic", idx)
					}
				}
			}()
			r.mustBeLeader()
		}()
	}
}

func TestConfigViolationWillPanic(t *testing.T) {
	tests := []struct {
		name       string
		config     config.Config
		shouldFail bool
	}{
		{"Zero node id", newTestConfig(0, 10, 1), true},
		{"Zero heartbeat", newTestConfig(1, 10, 0), true},
		{"Zero election rtt", newTestConfig(1, 0, 1), true},
		{"Too low election rtt", newTestConfig(1, 3, 2), true},
		{"Good config", newTestConfig(1, 10, 1), false},
		{"Rate limit too small", newRateLimitedTestConfig(1, 10, 1, 15), true},
		{"Good rate limit config", newRateLimitedTestConfig(1, 10, 1, settings.EntryNonCmdFieldsSize+5), false},
	}

	for _, test := range tests {
		func() {
			defer func() {
				if r := recover(); test.shouldFail == (r == nil) {
					t.Errorf("Test %v failed: panic expectation is %v however get recover result %v",
						test.name, test.shouldFail, r)
				}
			}()
			newRaft(test.config, NewTestLogDB())
		}()
	}
}

func TestNilLogdbWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should have panic with nil logdb.")
		}
	}()
	newRaft(newTestConfig(1, 10, 1), nil)
}

func TestTryCommitResetsMatchArray(t *testing.T) {
	p := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	p.becomeCandidate()
	ne(p.becomeLeader(), t)
	p.matched = nil
	if _, err := p.tryCommit(); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(p.matched) != 3 {
		t.Errorf("tryCommit didn't resize the match array")
	}
}

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
	v := ReplicaID(100)
	v2 := ShardID(100)
	if v != "n00100" || v2 != "c00100" {
		t.Errorf("unexpected node id / shard id value")
	}
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	r.checkHandlerMap()
	addrMap := make(map[uint64]string)
	addrMap[1] = "address1"
	addrMap[2] = "address2"
	addrMap[3] = "address3"
	status := getLocalStatus(r)
	if status.IsLeader() || !status.IsFollower() || status.ReplicaID != 1 {
		t.Errorf("unexpected status value")
	}
}

func TestBecomePreVoteCandidateFromCandidate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.preVote = true
	r.becomeFollower(2, 3)
	r.becomeCandidate()
	r.becomePreVoteCandidate()
	if r.term != 3 {
		t.Errorf("term changed")
	}
	if r.state != preVoteCandidate {
		t.Errorf("state not set to preVoteCandidate")
	}
}

func TestBecomePreVoteCandidate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.preVote = true
	r.becomeFollower(2, 3)
	if r.term != 2 {
		t.Errorf("term not set")
	}
	r.becomePreVoteCandidate()
	if r.term != 2 {
		t.Errorf("term changed")
	}
	if r.state != preVoteCandidate {
		t.Errorf("state not set to preVoteCandidate")
	}
	if r.leaderID != NoLeader {
		t.Errorf("leader unexpectedly set")
	}
}

func TestBecomeFollowerDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.electionTick = 100
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
	if r.electionTick != 0 {
		t.Errorf("election tick not reset, %d", r.electionTick)
	}
}

func TestBecomeFollowerKE(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.electionTick = 100
	r.electionTimeout = 101
	r.becomeFollowerKE(2, 3)
	if r.term != 2 {
		t.Errorf("term not set")
	}
	if r.leaderID != 3 {
		t.Errorf("leader id not set")
	}
	if r.state != follower {
		t.Errorf("not become follower")
	}
	if r.electionTick != 100 {
		t.Errorf("election tick changed, %d", r.electionTick)
	}
	if r.electionTimeout != 101 {
		t.Errorf("election timeout changed, %d", r.electionTimeout)
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
	ne(r.becomeLeader(), t)
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
	ne(r.becomeLeader(), t)
}

func TestBecomeCandidateDragonboat(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.becomeFollower(2, 3)
	r.electionTick = 100
	term := r.term
	r.becomeCandidate()
	if r.term != term+1 {
		t.Errorf("term didn't increase")
	}
	if r.state != candidate {
		t.Errorf("not in candidate state")
	}
	if r.vote != r.replicaID {
		t.Errorf("vote not set")
	}
	if r.electionTick != 0 {
		t.Errorf("election tick not reset, %d", r.electionTick)
	}
}

func TestNonVotingWillNotStartElection(t *testing.T) {
	p := newTestNonVoting(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	if len(p.remotes) != 0 {
		t.Errorf("p.romotes len: %d", len(p.remotes))
	}
	for i := uint64(0); i < p.randomizedElectionTimeout*10; i++ {
		ne(p.tick(), t)
	}
	// gRequestVote won't be sent
	if len(p.msgs) != 0 {
		t.Errorf("unexpected msg found %+v", p.msgs)
	}
}

func TestNonVotingsPreVoteWillNotBeCounted(t *testing.T) {
	p := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p.preVote = true
	p.addNode(1)
	p.becomeCandidate()
	ne(p.Handle(pb.Message{From: 2, To: 1, Type: pb.RequestPreVoteResp}), t)
	if len(p.votes) != 0 {
		t.Errorf("vote from nonvoting not dropped")
	}
}

func TestNonVotingsVoteWillNotBeCounted(t *testing.T) {
	p := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p.addNode(1)
	p.becomeCandidate()
	ne(p.Handle(pb.Message{From: 2, To: 1, Type: pb.RequestVoteResp}), t)
	if len(p.votes) != 0 {
		t.Errorf("vote from nonvoting not dropped")
	}
}

func TestNonVotingCanBePromotedToVotingMember(t *testing.T) {
	p := newTestNonVoting(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	p.addNode(1)
	if p.isNonVoting() {
		t.Errorf("not promoted to regular node")
	}
	if len(p.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p.remotes))
	}
	if len(p.nonVotings) != 0 {
		t.Errorf("nonVotings len: %d, want 0", len(p.nonVotings))
	}
}

func TestNonVotingCanActAsRegularNodeAfterPromotion(t *testing.T) {
	p := newTestNonVoting(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	p.addNode(1)
	if p.isNonVoting() {
		t.Errorf("not promoted to regular node")
	}
	for i := uint64(0); i <= p.randomizedElectionTimeout; i++ {
		ne(p.tick(), t)
	}
	if p.state != leader {
		t.Errorf("failed to start election")
	}
}

// nonVotings will not be asked to vote, however a node used to be a non-voting node
// may not realize its own promotion and thus can still receive RequestVote from
// candidates that are aware of such promotion. in this case, nonVotings have to
// cast their votes
func TestNonVotingCanVote(t *testing.T) {
	testNonVotingCanVote(t, false)
	testNonVotingCanVote(t, true)
}

func testNonVotingCanVote(t *testing.T, preVote bool) {
	a := newTestNonVoting(1, nil, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestNonVoting(2, nil, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestNonVoting(3, nil, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	if preVote {
		a.preVote = true
		b.preVote = true
		c.preVote = true
	}
	if !c.isNonVoting() {
		t.Errorf("not nonvoting")
	}
	a.addNode(1)
	b.addNode(2)
	if a.isNonVoting() || b.isNonVoting() {
		t.Errorf("unepected nonvoting")
	}
	if a.state != follower {
		t.Errorf("not a follower")
	}
	if b.state != follower {
		t.Errorf("not a follower")
	}
	nt := newNetwork(a, b, c)
	nt.isolate(1)
	nt.send(pb.Message{From: 2, To: 2, Type: pb.Election})
	if !b.isLeader() {
		t.Errorf("not leader")
	}
	if b.term == a.term || a.leaderID != NoLeader {
		t.Errorf("a not isolated")
	}
}

func TestNonVotingReplication(t *testing.T) {
	p1 := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestNonVoting(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isNonVoting() {
		t.Errorf("p1 is still nonvoting")
	}
	if !p2.isNonVoting() {
		t.Errorf("p2 is not nonvoting")
	}
	nt := newNetwork(p1, p2)
	if len(p1.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(p1.remotes))
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		ne(p1.tick(), t)
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
		t.Errorf("entry not committed on nonvoting: %d", p2.log.committed)
	}
	if committed+1 != p1.nonVotings[2].match {
		t.Errorf("match value not expected: %d", p1.nonVotings[2].match)
	}
}

func TestNonVotingCanPropose(t *testing.T) {
	p1 := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestNonVoting(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isNonVoting() {
		t.Errorf("p1 is still nonvoting")
	}
	if !p2.isNonVoting() {
		t.Errorf("p2 is not nonvoting")
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
		ne(p1.tick(), t)
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	if !p2.isNonVoting() {
		t.Errorf("not nonvoting")
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
		t.Errorf("entry not committed on nonvoting: %d", p2.log.committed)
	}
	if committed+10 != p1.nonVotings[2].match {
		t.Errorf("match value not expected: %d", p1.nonVotings[2].match)
	}
}

func TestNonVotingCanReadIndexQuorum1(t *testing.T) {
	p1 := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestNonVoting(2, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p1.addNode(1)
	p2.addNode(1)
	if p1.isNonVoting() {
		t.Errorf("p1 is still nonvoting")
	}
	if !p2.isNonVoting() {
		t.Errorf("p2 is not nonvoting")
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
		ne(p1.tick(), t)
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	if !p2.isNonVoting() {
		t.Errorf("not nonvoting")
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

func TestNonVotingCanReadIndexQuorum2(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p2 := newTestRaft(2, []uint64{1, 2}, 10, 1, NewTestLogDB())
	p3 := newTestNonVoting(3, []uint64{1, 2}, []uint64{3}, 10, 1, NewTestLogDB())
	p1.addNonVoting(3)
	p2.addNonVoting(3)
	nt := newNetwork(p1, p2, p3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if p1.state != leader {
		t.Errorf("failed to start election")
	}
	if p2.state != follower {
		t.Errorf("not a follower")
	}
	if !p3.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	for i := uint64(0); i <= p1.randomizedElectionTimeout; i++ {
		ne(p1.tick(), t)
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

func TestNonVotingCanReceiveSnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestNonVoting(3, []uint64{1}, []uint64{2, 3}, 10, 1, NewTestLogDB())
	if !p1.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	ne(p1.Handle(pb.Message{From: 2, To: 1, Type: pb.InstallSnapshot, Snapshot: ss}), t)
	if p1.log.committed != 20 {
		t.Errorf("snapshot not applied")
	}
}

func TestNonVotingCanReceiveHeartbeatMessage(t *testing.T) {
	p1 := newTestNonVoting(2, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
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
	ne(p1.Handle(m), t)
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
	ne(p1.Handle(hbm), t)
	if p1.log.committed != 3 {
		t.Errorf("unexpected committed value %d, want 3", p1.log.committed)
	}
}

func TestNonVotingCanBeRestored(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.NonVotings[3] = "a3"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestNonVoting(3, []uint64{1, 2}, []uint64{3}, 10, 1, NewTestLogDB())
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("failed to restore")
	}
}

func TestNonVotingCanBePromotedBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	if !p1.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("failed to restore")
	}
	p1.restoreRemotes(ss)
	if p1.isNonVoting() {
		t.Errorf("nonvoting not promoted")
	}
}

func TestCorrectNonVotingCanBePromotedBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.NonVotings[1] = "a1"
	members.Addresses[2] = "a2"
	members.Addresses[3] = "a3"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestNonVoting(1, []uint64{2}, []uint64{1, 3}, 10, 1, NewTestLogDB())
	if !p1.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	_, ok := p1.nonVotings[1]
	if !ok {
		t.Errorf("not a non-voting node")
	}
	_, ok = p1.nonVotings[3]
	if !ok {
		t.Errorf("not a non-voting node")
	}
	p1.restoreRemotes(ss)
	if !p1.isNonVoting() {
		t.Errorf("nonvoting p1 unexpectedly promoted")
	}
}

func TestNonVotingCanNotMoveNodeBackToNonVotingBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.NonVotings[3] = "a3"
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
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("restore unexpectedly completed")
	}
}

func TestNonVotingCanBeAdded(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	if len(p1.nonVotings) != 0 {
		t.Errorf("unexpected nonvoting record")
	}
	p1.addNonVoting(2)
	if len(p1.nonVotings) != 1 {
		t.Errorf("nonvoting not added")
	}
	if p1.isNonVoting() {
		t.Errorf("unexpectedly changed to nonvoting")
	}
}

func TestNonVotingCanBeRemoved(t *testing.T) {
	p1 := newTestNonVoting(1, nil, []uint64{1, 2}, 10, 1, NewTestLogDB())
	if len(p1.nonVotings) != 2 {
		t.Errorf("unexpected nonvoting count")
	}
	ne(p1.removeNode(2), t)
	if len(p1.nonVotings) != 1 {
		t.Errorf("nonvoting not removed")
	}
	_, ok := p1.nonVotings[2]
	if ok {
		t.Errorf("nonvoting node 2 not removed")
	}
}

func TestWitnessCanNotBecomeNonVoting(t *testing.T) {
	_, witness, _ := setUpLeaderAndWitness(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("witness became follower")
		}
	}()
	witness.becomeNonVoting(1, 1)
}

func TestWitnessCanNotBecomeFollower(t *testing.T) {
	_, witness, _ := setUpLeaderAndWitness(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("witness became follower")
		}
	}()
	witness.becomeFollower(1, 1)
}

func TestWitnessCanNotBecomeCandidate(t *testing.T) {
	_, witness, _ := setUpLeaderAndWitness(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("witness became candidate")
		}
	}()
	witness.becomeCandidate()
}

func TestWitnessWillNotStartElection(t *testing.T) {
	p := newTestWitness(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isWitness() {
		t.Errorf("not a witness")
	}
	if len(p.remotes) != 0 {
		t.Errorf("p.romotes len: %d", len(p.remotes))
	}
	for i := uint64(0); i < p.randomizedElectionTimeout*10; i++ {
		ne(p.tick(), t)
	}
	// RequestVote won't be sent
	if len(p.msgs) != 0 {
		t.Errorf("unexpected msg found %+v", p.msgs)
	}
}

func TestWitnessWillVoteInElection(t *testing.T) {
	p := newTestWitness(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isWitness() {
		t.Errorf("not a witness")
	}
	ne(p.Handle(pb.Message{From: 2, To: 1, Type: pb.RequestVote, LogTerm: 100, LogIndex: 100}), t)
	if len(p.msgs) != 1 {
		t.Fatalf("witness is not voting")
	}
	if p.msgs[0].Type != pb.RequestVoteResp {
		t.Errorf("witness didn't send vote resp")
	}
}

func TestWitnessCannotBePromotedToFullMember(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should panic while promoting from witness")
		}
	}()
	replicaID := uint64(1)
	p := newTestWitness(replicaID, nil, []uint64{1}, 10, 1, NewTestLogDB())
	if !p.isWitness() {
		t.Errorf("not an witness")
	}
	p.addNode(replicaID)
}

func TestNonWitnessWouldPanicWhenRemoteSnapshotAssumeAsWitness(t *testing.T) {
	members := pb.Membership{
		Addresses:  make(map[uint64]string),
		NonVotings: make(map[uint64]string),
		Removed:    make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestNonVoting(1, []uint64{1}, []uint64{1}, 10, 1, NewTestLogDB())
	if !p1.isNonVoting() {
		t.Errorf("not a non-voting node")
	}
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("failed to restore")
	}
	p1.restoreRemotes(ss)
	if p1.isNonVoting() {
		t.Errorf("nonvoting not promoted")
	}

	p1.witnesses[2] = &remote{}
	defer func() {
		if r := recover(); r == nil {
			panic("assumed witness not promotion not causing panic")
		}
	}()
	p1.restoreRemotes(ss)
}

func TestWitnessReplication(t *testing.T) {
	leader, witness, nt := setUpLeaderAndWitness(t)
	committed := leader.log.committed
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})
	expectedIndex := committed + 1
	if expectedIndex != leader.log.committed {
		t.Errorf("entry not committed on leader: %d", witness.log.committed)
	}
	// the no-op blank entry appended after p1 becomes the leader is also replicated
	if expectedIndex != witness.log.committed {
		t.Errorf("entry not committed on witness: %d", witness.log.committed)
	}
	if expectedIndex != leader.witnesses[2].match {
		t.Errorf("match value expected: %d, actual: %d", expectedIndex, leader.witnesses[2].match)
	}
}

func TestApplicationMessageSentToWitnessIsEmpty(t *testing.T) {
	_, witness, _ := setUpLeaderAndWitness(t)
	expectedEntry := pb.Entry{
		Type:  pb.MetadataEntry,
		Term:  1,
		Index: 1,
		Cmd:   nil,
	}
	witnessEntries, err := witness.log.getEntries(1, 2, math.MaxUint64)
	if err != nil {
		t.Errorf("Encounter error during get entries: %v", err)
	}
	if !reflect.DeepEqual(expectedEntry, witnessEntries[0]) {
		t.Errorf("Found entry not matching. Expected: %v, actual: %v", expectedEntry, witnessEntries[0])
	}
}

func TestConfigChangeMessageSentToWitnessIsEmpty(t *testing.T) {
	leader, witness, nt := setUpLeaderAndWitness(t)
	configChangEntry := pb.Entry{
		Term:  1,
		Index: 2,
		Type:  pb.ConfigChangeEntry,
		Cmd:   []byte("test-data"),
	}
	leader.log.append([]pb.Entry{configChangEntry})
	// Send config change to witness.
	leader.broadcastReplicateMessage()
	if len(leader.msgs) != 1 {
		t.Errorf("Expecting 1 election message, actually get %v", len(leader.msgs))
	}
	nt.send(leader.msgs[0])
	witnessEntries, err := witness.log.getEntries(1, 3, math.MaxUint64)
	if err != nil {
		t.Errorf("Encounter error during get entries: %v", err)
	}
	if !reflect.DeepEqual(configChangEntry, witnessEntries[1]) {
		t.Errorf("Found entry not matching. Expected: %v, actual: %v", configChangEntry, witnessEntries[1])
	}
}

func TestWitnessSnapshot(t *testing.T) {
	leader, _, _ := setUpLeaderAndWitness(t)
	ss := pb.Snapshot{Index: 10, Term: 2}
	if err := leader.log.logdb.ApplySnapshot(ss); err != nil {
		t.Errorf("apply snapshot failed %v", err)
	}
	msg := pb.Message{}
	if idx := leader.makeInstallSnapshotMessage(2, &msg); idx != 10 {
		t.Errorf("unexpected index %d", idx)
	}
	if msg.Type != pb.InstallSnapshot || msg.Snapshot.Index != 10 ||
		msg.Snapshot.Term != 2 || !msg.Snapshot.Witness || msg.Snapshot.Dummy {
		t.Errorf("unexpected message values")
	}
}

func TestNonWitnessCanNotAddItselfAsWitness(t *testing.T) {
	p := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	defer func() {
		if r := recover(); r == nil {
			panic("added non witness node as witness")
		}
	}()
	p.addWitness(1)
}

func TestWitnessCanNotBeAddedAsNode(t *testing.T) {
	_, witness, _ := setUpLeaderAndWitness(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("witness added as a node")
		}
	}()
	witness.addNode(2)
}

func setUpLeaderAndWitness(t *testing.T) (*raft, *raft, *network) {
	leader := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	witness := newTestWitness(2, nil, []uint64{2}, 10, 1, NewTestLogDB())
	leader.addWitness(2)
	witness.addNode(1)
	if !witness.isWitness() {
		t.Errorf("Assumed witness is not witness")
	}
	nt := newNetwork(leader, witness)
	if len(leader.remotes) != 1 {
		t.Errorf("remotes len: %d, want 1", len(leader.remotes))
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if !leader.isLeader() {
		t.Errorf("failed to start election")
	}
	for i := uint64(0); i <= leader.randomizedElectionTimeout; i++ {
		ne(leader.tick(), t)
		nt.send(pb.Message{From: 1, To: 1, Type: pb.NoOP})
	}
	if !witness.isWitness() {
		t.Errorf("not witness")
	}
	return leader, witness, nt
}

func TestWitnessCannotReadIndex(t *testing.T) {
	witness := newTestWitness(1, nil, []uint64{1}, 10, 1, NewTestLogDB())
	nt := newNetwork(witness)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.ReadIndex, Hint: 12345})
	if len(witness.readyToRead) != 0 {
		t.Errorf("ready to read len is not 0")
	}
}

func TestWitnessCanReceiveSnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Witnesses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestWitness(3, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	if !p1.isWitness() {
		t.Errorf("not a witness")
	}
	ne(p1.Handle(pb.Message{From: 2, To: 1, Type: pb.InstallSnapshot, Snapshot: ss}), t)
	if p1.log.committed != 20 {
		t.Errorf("snapshot not applied")
	}
	if len(p1.msgs) != 1 {
		t.Fatalf("failed to send ss resp")
	}
	if p1.msgs[len(p1.msgs)-1].LogIndex != 20 {
		t.Errorf("unexpected log index")
	}
}

func TestWitnessCanReceiveHeartbeatMessage(t *testing.T) {
	p1 := newTestWitness(2, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	m := pb.Message{
		From:     1,
		To:       2,
		Type:     pb.Replicate,
		LogIndex: 0,
		LogTerm:  0,
		Commit:   0,
		Entries:  make([]pb.Entry, 0),
	}
	m.Entries = append(m.Entries, pb.Entry{Index: 1, Term: 1, Type: pb.MetadataEntry})
	m.Entries = append(m.Entries, pb.Entry{Index: 2, Term: 1, Type: pb.MetadataEntry})
	m.Entries = append(m.Entries, pb.Entry{Index: 3, Term: 1, Type: pb.MetadataEntry})

	ne(p1.Handle(m), t)
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
	ne(p1.Handle(hbm), t)
	if p1.log.committed != 3 {
		t.Errorf("unexpected committed value %d, want 3", p1.log.committed)
	}
}

func TestWitnessCanBeRestored(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Witnesses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.Witnesses[3] = "a3"
	ss := pb.Snapshot{
		Index:      20,
		Term:       20,
		Membership: members,
	}
	p1 := newTestWitness(3, []uint64{1, 2}, []uint64{3}, 10, 1, NewTestLogDB())
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("failed to restore")
	}
}

func TestWitnessCanNotMoveNodeBackToWitnessBySnapshot(t *testing.T) {
	members := pb.Membership{
		Addresses: make(map[uint64]string),
		Witnesses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	members.Addresses[1] = "a1"
	members.Addresses[2] = "a2"
	members.Witnesses[3] = "a3"
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
	ok, err := p1.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("restore unexpectedly completed")
	}
}

func TestWitnessCanBeAdded(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	if len(p1.witnesses) != 0 {
		t.Errorf("unexpected witness record")
	}
	p1.addWitness(2)
	if len(p1.witnesses) != 1 {
		t.Errorf("witness not added")
	}
	if p1.isWitness() {
		t.Errorf("unexpectedly changed to nonvoting")
	}
}

func TestWitnessCanBeRemoved(t *testing.T) {
	p1 := newTestWitness(1, []uint64{1}, []uint64{2}, 10, 1, NewTestLogDB())
	if len(p1.witnesses) != 1 {
		t.Errorf("unexpected witness count")
	}
	ne(p1.removeNode(2), t)
	if len(p1.witnesses) != 0 {
		t.Errorf("witness not removed")
	}
}

func TestFollowerTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	for i := 0; i < 9; i++ {
		if r.timeForElection() {
			t.Errorf("time for election unexpected became true")
		}
		ne(r.tick(), t)
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
	ne(r.becomeLeader(), t)
	for i := 0; i < 10; i++ {
		ne(r.tick(), t)
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
	ne(r.becomeLeader(), t)
	r.checkQuorum = true
	for i := 0; i < 5; i++ {
		ne(r.tick(), t)
	}
	if r.state == leader {
		t.Errorf("leader didn't step down")
	}
}

func TestQuiescedTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
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

func TestMultiNodeShardCampaign(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	ne(r.campaign(), t)
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

func TestSingleNodeShardCampaign(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	ne(r.campaign(), t)
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
	ne(r.Handle(pb.Message{Type: pb.Heartbeat, Term: 2}), t)
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

func TestNoOPIsSentOnSmallTermRejectedRequestPreVote(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.preVote = true
	r.becomeFollower(10, 2)
	ne(r.Handle(pb.Message{Type: pb.RequestPreVote, Term: 9, From: 2}), t)
	if len(r.msgs) != 1 {
		t.Fatalf("no message sent")
	}
	m := pb.Message{Type: pb.NoOP, To: 2, From: 1, Term: 10}
	if !reflect.DeepEqual(m, r.msgs[0]) {
		t.Errorf("not expected message")
	}
}

func TestPreVoteRespWithHigherTerm(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.preVote = true
	r.becomeFollower(10, 2)
	ne(r.Handle(pb.Message{Type: pb.RequestPreVoteResp, Term: 11, From: 2}), t)
	if r.term != 10 {
		t.Errorf("term unexpected changed")
	}
	ne(r.Handle(pb.Message{Type: pb.RequestPreVoteResp, Term: 20, From: 2, Reject: true}), t)
	if r.term != 20 {
		t.Errorf("term not set")
	}
}

func TestDropRequestVoteMessageFromHighTermNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.preVote = true
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
	for _, mt := range []pb.MessageType{pb.RequestVote, pb.RequestPreVote} {
		for idx, tt := range tests {
			r.checkQuorum = tt.checkQuorum
			m := pb.Message{
				Type: mt,
				Term: tt.term,
				From: 2,
			}
			if r.dropRequestVoteFromHighTermNode(m) != tt.drop {
				t.Errorf("%d, got %t, want %t", idx, r.dropRequestVoteFromHighTermNode(m), tt.drop)
			}
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
	ne(r.becomeLeader(), t)
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
	ne(r.becomeLeader(), t)
	noop := pb.Entry{}
	ents := []pb.Entry{
		{Index: 2, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Index: 3, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
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
	ne(r.becomeLeader(), t)
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
	ne(r.becomeLeader(), t)
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
	ne(r.becomeLeader(), t)
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
	ne(r.becomeLeader(), t)
	hint := pb.SystemCtx{Low: 100, High: 200}
	match := uint64(100)
	r.remotes[2].match = match
	r.log.committed = 200
	r.sendHeartbeatMessage(2, hint, match)
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
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Index: 2, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Index: 3, Term: 1, Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
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
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
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

func TestFollowerSelfRemoved(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	if r.selfRemoved() {
		t.Errorf("unexpectedly self removed")
	}
	delete(r.remotes, 1)
	if !r.selfRemoved() {
		t.Errorf("self removed not report removed")
	}
}

func TestNonVotingSelfRemoved(t *testing.T) {
	r := newTestNonVoting(1, []uint64{}, []uint64{1}, 5, 1, NewTestLogDB())
	if r.selfRemoved() {
		t.Errorf("unexpectedly self removed")
	}
	delete(r.nonVotings, 1)
	if !r.selfRemoved() {
		t.Errorf("self removed not report removed")
	}
}

func TestWitnessSelfRemoved(t *testing.T) {
	r := newTestWitness(1, []uint64{}, []uint64{1}, 5, 1, NewTestLogDB())
	if r.selfRemoved() {
		t.Errorf("unexpectedly self removed")
	}
	delete(r.witnesses, 1)
	if !r.selfRemoved() {
		t.Errorf("self removed not report removed")
	}
}

func TestFullMemberWithOneWitnessCouldMakeProgressWithOneMemberDrop(t *testing.T) {
	p1 := newTestRaft(1, []uint64{1, 2, 3, 4}, 10, 1, NewTestLogDB())
	p2 := newTestRaft(1, []uint64{1, 2, 3, 4}, 10, 1, NewTestLogDB())
	p3 := newTestRaft(1, []uint64{1, 2, 3, 4}, 10, 1, NewTestLogDB())
	p4 := newTestWitness(1, []uint64{1, 2, 3, 4}, []uint64{4}, 10, 1, NewTestLogDB())
	nt := newNetwork(p1, p2, p3, p4)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if !p1.isLeader() {
		t.Errorf("p1 should be leader")
	}
	if !p4.isWitness() {
		t.Errorf("p4 should be witness")
	}

	committed := p1.log.committed
	nt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})

	peers := []*raft{p1, p2, p3, p4}
	for _, p := range peers {
		if p.log.committed != committed+1 {
			t.Errorf("new propose should have committed for member %v", p.replicaID)
		}
	}

	// Partition a full member
	nt.isolate(3)
	nt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("test-data")}}})

	for _, p := range peers {
		// Only p3 will lag behind.
		if p.log.committed != committed+2 && p.replicaID != 3 {
			t.Errorf("new propose should have committed for member %v", p.replicaID)
		}
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
}

func TestDeleteRemote(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.deleteRemote(3)
	_, ok := r.remotes[3]
	if ok {
		t.Errorf("node 3 not deleted")
	}
	if len(r.remotes) != 2 {
		t.Errorf("remote not deleted")
	}
}

func TestCampaignSendExpectedMessages(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
	r.becomeFollower(r.term+1, NoLeader)
	ne(r.campaign(), t)
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
	v1 := r.handleVoteResp(1, false, false)
	v2 := r.handleVoteResp(2, true, false)
	v3 := r.handleVoteResp(3, false, false)
	v4 := r.handleVoteResp(2, false, false)
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

func TestElectionTickResetAfterGrantVote(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	r.electionTick = 101
	m := pb.Message{
		Type: pb.RequestVote,
		From: 2,
		To:   1,
		Term: 3,
	}
	ne(r.Handle(m), t)
	if r.vote != 2 {
		t.Fatalf("failed to grant the vote")
	}
	if r.electionTick != 0 {
		t.Errorf("electionTick not reset")
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
	ne(r.becomeLeader(), t)
	for i := 0; i < 5; i++ {
		ents := []pb.Entry{
			{Type: pb.ApplicationEntry, Cmd: make([]byte, maxEntriesToApplySize)},
			{Type: pb.ConfigChangeEntry},
		}
		ne(r.appendEntries(ents), t)
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
		{pb.LeaderTransfer, true, false},
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
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ConfigChangeEntry},
	}
	r.setPendingConfigChange()
	ne(r.appendEntries(ents), t)
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
	ne(r.removeNode(2), t)
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
	ne(r.becomeLeader(), t)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
	r.remotes[2].tryUpdate(r.log.lastIndex())
	ok, err := r.tryCommit()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("failed to commit")
	}
	if !r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
}

func TestHandleLeaderCheckQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ne(r.handleLeaderCheckQuorum(pb.Message{}), t)
	if r.state != follower {
		t.Errorf("node didn't step down")
	}
	r = newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.remotes[1].setActive()
	r.remotes[2].setActive()
	ne(r.handleLeaderCheckQuorum(pb.Message{}), t)
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
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
	ss := pb.Snapshot{Index: 3, Term: 1}
	ok, err := r.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("nothing to restore")
	}
}

func TestSnapshotCommitEntries(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
	if r.log.committed != 0 {
		t.Errorf("unexpected commit, %d", r.log.committed)
	}
	ss := pb.Snapshot{Index: 2, Term: 1}
	ok, err := r.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("not expect to restore")
	}
	if r.log.committed != 2 {
		t.Errorf("commit not moved")
	}
}

func TestSnapshotCanBeRestored(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
	ss := pb.Snapshot{Index: 4, Term: 1}
	ok, err := r.restore(ss)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Errorf("snapshot unexpectedly ignored")
	}
	if r.log.lastIndex() != 4 {
		t.Errorf("last index not moved, %d", r.log.lastIndex())
	}
	term, err := r.log.lastTerm()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if term != 1 {
		t.Errorf("last term %d", term)
	}
}

func TestRestoreRemote(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
		{Type: pb.ApplicationEntry, Cmd: make([]byte, 16)},
	}
	ne(r.appendEntries(ents), t)
	ss := pb.Snapshot{}
	ss.Membership.Addresses = make(map[uint64]string)
	ss.Membership.Addresses[1] = ""
	ss.Membership.Addresses[2] = ""
	ss.Membership.Addresses[3] = ""
	ss.Membership.NonVotings = make(map[uint64]string)
	ss.Membership.NonVotings[4] = ""
	ss.Membership.NonVotings[5] = ""
	ss.Membership.Witnesses = make(map[uint64]string)
	ss.Membership.Witnesses[6] = ""
	ss.Membership.Witnesses[7] = ""
	r.restoreRemotes(ss)
	if len(r.remotes) != 3 {
		t.Errorf("remotes length unexpected %d", len(r.remotes))
	}
	if len(r.nonVotings) != 2 {
		t.Errorf("nonVotings length unexpected %d", len(r.nonVotings))
	}
	if len(r.witnesses) != 2 {
		t.Errorf("witnesses length unexpected %d", len(r.witnesses))
	}
	if len(r.nodesSorted()) != 7 {
		t.Errorf("total node length unexpected %d", len(r.nodesSorted()))
	}

	if len(r.matched) != 5 {
		t.Errorf("matchValue not reset as %d", len(r.matched))
	}
	for nid, rm := range r.votingMembers() {
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
		vals := tt.vals
		sort.Slice(tt.vals, func(i, j int) bool {
			return vals[i] < vals[j]
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
	ne(r.handleCandidatePropose(msg), t)
	if len(r.msgs) != 0 {
		t.Errorf("unexpectedly sent message")
	}
}

func TestCandidateBecomeFollowerOnRecivingLeaderMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	tests := []struct {
		fn      func(msg pb.Message) error
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
		ne(tt.fn(msg), t)
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
	ne(r.handleCandidateHeartbeat(msg), t)
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
	ne(r.handleCandidateInstallSnapshot(m), t)
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
	ne(r.handleCandidateReplicate(m), t)
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
		ne(r.handleCandidateRequestVoteResp(m), t)
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
		ne(r.handleCandidateRequestVoteResp(m), t)
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
		fn      func(msg pb.Message) error
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
		ne(tt.fn(m), t)
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
	r.becomeFollower(10, 2)
	m := pb.Message{
		Type: pb.ReadIndex,
	}
	ne(r.handleFollowerReadIndex(m), t)
	if len(r.msgs) != 1 {
		t.Fatalf("failed to redirect the message")
	}
	if r.msgs[0].Type != pb.ReadIndex || r.msgs[0].To != 2 {
		t.Errorf("unexpected message sent %v", r.msgs[0])
	}
	if r.msgs[0].Term != 0 {
		t.Errorf("term unexpectedly set")
	}
}

func TestFollowerRedirectProposeMessageToLeader(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	m := pb.Message{
		Type: pb.Propose,
	}
	ne(r.handleFollowerPropose(m), t)
	if len(r.msgs) != 1 {
		t.Fatalf("failed to redirect the message")
	}
	if r.msgs[0].Type != pb.Propose || r.msgs[0].To != 2 {
		t.Errorf("unexpected message sent %v", r.msgs[0])
	}
	if r.msgs[0].Term != 0 {
		t.Errorf("term unexpectedly set")
	}
}

func TestFollowerRedirectLeaderTransferMessageToLeader(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(10, 2)
	m := pb.Message{
		Type: pb.LeaderTransfer,
	}
	ne(r.handleFollowerLeaderTransfer(m), t)
	if len(r.msgs) != 1 {
		t.Fatalf("failed to redirect the message")
	}
	if r.msgs[0].Type != pb.LeaderTransfer || r.msgs[0].To != 2 {
		t.Errorf("unexpected message sent %v", r.msgs[0])
	}
	if r.msgs[0].Term != 0 {
		t.Errorf("term unexpectedly set")
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
	ne(r.handleFollowerReplicate(m), t)
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
	ne(r.handleFollowerHeartbeat(msg), t)
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
	ne(r.handleFollowerInstallSnapshot(m), t)
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
	ne(r.handleFollowerReadIndexResp(msg), t)
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
	ne(r.handleFollowerTimeoutNow(m), t)
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
	ne(r.handleFollowerLeaderTransfer(m), t)
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
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.Election,
	}
	ne(r.handleNodeElection(msg), t)
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
	ne(r.handleNodeElection(msg), t)
	if len(r.msgs) != 0 {
		t.Errorf("unexpected message sent")
	}
	if r.state != follower {
		t.Errorf("state change to %s", r.state)
	}
}

func TestHandleElection(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	msg := pb.Message{
		Type: pb.Election,
	}
	ne(r.handleNodeElection(msg), t)
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

func TestRequestVoteMessageWontResetElectionTick(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	r.electionTick = 101
	r.electionTimeout = 102
	msg := pb.Message{
		Type: pb.RequestVote,
		From: 2,
		To:   1,
		Term: 3,
	}
	r.onMessageTermNotMatched(msg)
	if r.electionTick != 101 || r.electionTimeout != 102 {
		t.Errorf("election tick changed")
	}
	msg = pb.Message{
		Type: pb.Replicate,
		Term: 5,
	}
	r.onMessageTermNotMatched(msg)
	if r.electionTick != 0 {
		t.Fatalf("election tick not reset")
	}
}

func TestLeaderStepDownAfterRemoved(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	msg := pb.Message{
		Type: pb.Election,
	}
	ne(r.handleNodeElection(msg), t)
	if r.state != leader {
		t.Errorf("not a leader")
	}
	ne(r.removeNode(2), t)
	if r.state != leader {
		t.Errorf("no longer a leader, %s", r.state)
	}
	ne(r.removeNode(1), t)
	if r.state != follower {
		t.Errorf("not a follower, %s", r.state)
	}
	if r.leaderID != NoLeader {
		t.Errorf("unexpected leader id %d", r.leaderID)
	}
}

func TestLeaderStepDownAfterRemovedBySnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, 2)
	msg := pb.Message{
		Type: pb.Election,
	}
	ne(r.handleNodeElection(msg), t)
	if r.state != leader {
		t.Errorf("not a leader")
	}
	ss := pb.Snapshot{
		Membership: pb.Membership{
			Addresses: map[uint64]string{2: "a2", 1: "a1"},
		},
	}
	r.restoreRemotes(ss)
	if r.state != leader {
		t.Errorf("no longer a leader, %s", r.state)
	}
	ss = pb.Snapshot{
		Membership: pb.Membership{
			Addresses: map[uint64]string{2: "a2"},
		},
	}
	r.restoreRemotes(ss)
	if r.state != follower {
		t.Errorf("not a follower, %s", r.state)
	}
	if r.leaderID != NoLeader {
		t.Errorf("unexpected leader id %d", r.leaderID)
	}
}

func TestHandleLeaderHeartbeatMessage(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.LeaderHeartbeat,
	}
	ne(r.handleLeaderHeartbeat(msg), t)
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
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.CheckQuorum,
	}
	ne(r.handleLeaderCheckQuorum(msg), t)
	if r.state == leader {
		t.Errorf("leader didn't step down")
	}
}

func TestLeaderIgnoreCheckQuorumWhenHasQuorum(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.CheckQuorum,
	}
	for _, rp := range r.remotes {
		rp.setActive()
	}
	ne(r.handleLeaderCheckQuorum(msg), t)
	if r.state != leader {
		t.Errorf("leader unexpectedly stepped down")
	}
}

func TestHandleLeaderUnreachable(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.Unreachable,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteReplicate
	ne(r.handleLeaderUnreachable(msg, rp), t)
	if rp.state != remoteRetry {
		t.Errorf("not in retry state")
	}
}

func TestSnapshotStatusMessageIgnoredWhenNotInSnapshotState(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.SnapshotStatus,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteReplicate
	ne(r.handleLeaderSnapshotStatus(msg, rp), t)
	if rp.state == remoteRetry {
		t.Errorf("unexpectedly in retry state")
	}
}

func TestHandleLeaderSnapshotStatus(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type: pb.SnapshotStatus,
	}
	rp, ok := r.remotes[2]
	if !ok {
		t.Errorf("rp not found")
	}
	rp.state = remoteSnapshot
	ne(r.handleLeaderSnapshotStatus(msg, rp), t)
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
		ne(r.becomeLeader(), t)
		msg := pb.Message{
			Type: pb.LeaderTransfer,
			Hint: tt.target,
		}
		if tt.transferring {
			r.leaderTransferTarget = 3
		}
		rp, ok := r.remotes[tt.target]
		if !ok {
			t.Fatalf("failed to get remote")
		}
		if tt.match {
			rp.match = r.log.lastIndex()
		}
		ne(r.handleLeaderTransfer(msg), t)
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
	ne(r.becomeLeader(), t)
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
	ne(r.handleLeaderHeartbeatResp(msg, rp), t)
	if !rp.isActive() {
		t.Errorf("not active")
	}
}

func TestLeaderReadIndexOnSingleNodeShard(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	ne(r.handleLeaderReadIndex(msg), t)
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

func TestLeaderIgnoreReadIndexWhenShardCommittedIsUnknown(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
	}
	ne(r.handleLeaderReadIndex(msg), t)
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
	ne(r.becomeLeader(), t)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpected hasCommittedEntryAtCurrentTerm result")
	}
	r.remotes[2].tryUpdate(r.log.lastIndex())
	ok, err := r.tryCommit()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
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
	ne(r.handleLeaderReadIndex(msg), t)
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

func TestWitnessReadIndex(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())

	r.becomeFollower(1, NoLeader)
	if r.hasCommittedEntryAtCurrentTerm() {
		t.Errorf("unexpectedly set hasCommittedEntryAtCurrentTerm")
	}
	r.becomeCandidate()
	ne(r.becomeLeader(), t)

	r.addWitness(2)

	msg := pb.Message{
		Type:     pb.ReadIndex,
		Hint:     101,
		HintHigh: 1002,
		From:     2,
	}

	ne(r.handleLeaderReadIndex(msg), t)

	if len(r.readIndex.pending) != 0 || len(r.readIndex.queue) != 0 {
		t.Errorf("readIndex updated unexpectedly")
	}
}

func TestVotingMemberLengthMismatchWillResetMatchArray(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeFollower(1, NoLeader)
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.remotes[2].tryUpdate(r.log.lastIndex())
	if len(r.matched) != 3 {
		t.Errorf("Match array length unexpected %v", len(r.matched))
	}
	// Changing the number of total voting members
	r.witnesses[4] = &remote{}
	ok, err := r.tryCommit()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("Should fail commit")
	}
	if len(r.matched) != 4 {
		t.Errorf("Match array should already be reset to 4 however get %v", len(r.matched))
	}
}

func testNodeUpdatesItsRateLimiterHeartbeat(isLeader bool, t *testing.T) {
	r := newRateLimitedTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	if isLeader {
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
	} else {
		r.becomeFollower(0, 2)
	}
	hbt := r.rl.GetTick()
	for i := uint64(0); i < r.electionTimeout; i++ {
		ne(r.tick(), t)
	}
	if r.rl.GetTick() != hbt+1 {
		t.Errorf("rl heartbeat not updated, %d want %d",
			r.rl.GetTick(), hbt+1)
	}
}

func TestLeaderNodeUpdatesItsRateLimiterHeartbeat(t *testing.T) {
	testNodeUpdatesItsRateLimiterHeartbeat(true, t)
}

func TestFollowerNodeUpdatesItsRateLimiterHeartbeat(t *testing.T) {
	testNodeUpdatesItsRateLimiterHeartbeat(false, t)
}

func TestResetClearsFollowerRateLimitState(t *testing.T) {
	r := newRateLimitedTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	rl := r.rl
	ne(r.handleLeaderRateLimit(pb.Message{From: 2, Hint: testRateLimit + 1}), t)
	if !rl.RateLimited() {
		t.Errorf("not rate limited")
	}
	r.reset(2, true)
	for i := uint64(0); i <= server.ChangeTickThreashold; i++ {
		rl.Tick()
	}
	if rl.RateLimited() {
		t.Errorf("still rate limited")
	}
}

func TestLeaderRateLimitMessageIsHandledByLeader(t *testing.T) {
	r := newRateLimitedTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	rl := r.rl
	if rl.RateLimited() {
		t.Errorf("unexpectedly already rate limited")
	}
	ne(r.handleLeaderRateLimit(pb.Message{From: 2, Hint: testRateLimit + 1}), t)
	if !rl.RateLimited() {
		t.Errorf("not rate limited")
	}
}

func TestRateLimitMessageIsNeverSentByLeader(t *testing.T) {
	r := newRateLimitedTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, testRateLimit+1)},
	}
	ne(r.appendEntries(ents), t)
	rl := r.rl
	if !rl.RateLimited() {
		t.Errorf("not rate limited")
	}
	for i := uint64(0); i < r.electionTimeout; i++ {
		ne(r.tick(), t)
	}
	for _, msg := range r.msgs {
		if msg.Type == pb.RateLimit {
			t.Fatalf("rate limit message unexpected sent")
		}
	}
}

func testRateLimitMessageIsSentByNonLeader(leaderID uint64,
	rateLimitSent bool, t *testing.T) {
	r := newRateLimitedTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	r.becomeFollower(2, leaderID)
	ents := []pb.Entry{
		{Type: pb.ApplicationEntry, Cmd: make([]byte, testRateLimit+1)},
	}
	ne(r.appendEntries(ents), t)
	rl := r.rl
	if !rl.RateLimited() {
		t.Errorf("not rate limited")
	}
	for i := uint64(0); i < r.electionTimeout; i++ {
		ne(r.tick(), t)
	}
	sent := false
	for _, msg := range r.msgs {
		if msg.Type == pb.RateLimit {
			sent = true
		}
	}
	if sent != rateLimitSent {
		t.Fatalf("sent %t, want %t", sent, rateLimitSent)
	}
}

func TestRateLimitMessageIsSentByNonLeader(t *testing.T) {
	testRateLimitMessageIsSentByNonLeader(2, true, t)
	testRateLimitMessageIsSentByNonLeader(NoLeader, false, t)
}

func TestInMemoryEntriesSliceCanBeResized(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	oldcap := cap(r.log.inmem.entries)
	if oldcap != 0 {
		t.Errorf("unexpected cap val: %d", oldcap)
	}
	r.log.inmem.shrunk = true
	for i := uint64(0); i < inMemGcTimeout; i++ {
		ne(r.tick(), t)
	}
	if uint64(cap(r.log.inmem.entries)) != entrySliceSize {
		t.Errorf("not resized")
	}
}

func TestFirstQuiescedTickResizesInMemoryEntriesSlice(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 5, 1, NewTestLogDB())
	oldcap := cap(r.log.inmem.entries)
	if oldcap != 0 {
		t.Errorf("unexpected cap val: %d", oldcap)
	}
	r.quiescedTick()
	if uint64(cap(r.log.inmem.entries)) != entrySliceSize {
		t.Errorf("not resized, cap: %d", oldcap)
	}
	r.log.inmem.entries = make([]pb.Entry, 0)
	r.quiescedTick()
	if cap(r.log.inmem.entries) != 0 {
		t.Errorf("unexpectedly resized again")
	}
}

func TestDelayedSnapshotAckCanBeSet(t *testing.T) {
	p := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	p.becomeCandidate()
	ne(p.becomeLeader(), t)
	rp := p.remotes[3]
	rp.state = remoteSnapshot
	ne(p.handleLeaderSnapshotStatus(pb.Message{
		Type:   pb.SnapshotStatus,
		Hint:   10,
		Reject: true,
	}, rp), t)
	if !rp.delayed.rejected || rp.delayed.ctick != 10 {
		t.Errorf("delayed snapshot ack not set")
	}
}

func TestCheckDelayedSnapshotAck(t *testing.T) {
	p := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	p.becomeCandidate()
	ne(p.becomeLeader(), t)
	if p.snapshotting {
		t.Errorf("unexpected snapshotting flag")
	}
	rp := p.remotes[3]
	rp.state = remoteSnapshot
	rp.snapshotIndex = 100
	ne(p.handleLeaderSnapshotStatus(pb.Message{
		Type:   pb.SnapshotStatus,
		Hint:   10,
		Reject: true,
	}, rp), t)
	if !p.snapshotting {
		t.Errorf("snapshotting flag not set")
	}
	for i := 0; i < 10; i++ {
		ne(p.tick(), t)
		if i != 9 {
			if !p.snapshotting {
				t.Errorf("snapshotting flag not set")
			}
		} else {
			if p.snapshotting {
				t.Errorf("snapshotting flag not cleared")
			}
		}
	}
	if rp.delayed.rejected || rp.delayed.ctick != 0 {
		t.Errorf("pending not cleared")
	}
	if rp.state != remoteWait || rp.snapshotIndex != 0 {
		t.Errorf("not in remote wait state, %s", rp.state)
	}
}

func TestElectionWithPreVote(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	a.preVote = true
	b.preVote = true
	c.preVote = true

	nt := newNetwork(a, b, c)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if a.state != leader {
		t.Errorf("state = %s, want %s", a.state, leader)
	}
	if b.state != follower {
		t.Errorf("state = %s, want %s", b.state, follower)
	}
	if c.state != follower {
		t.Errorf("state = %s, want %s", c.state, follower)
	}
}

func TestInconsistentRaftConfig(t *testing.T) {
	tests := []struct {
		mt      pb.MessageType
		prevote bool
		result  bool
	}{
		{pb.RequestVote, true, false},
		{pb.RequestVote, false, false},
		{pb.RequestPreVote, true, false},
		{pb.RequestPreVote, false, true},
		{pb.RequestPreVoteResp, true, false},
		{pb.RequestPreVoteResp, false, true},
	}

	for idx, tt := range tests {
		r := raft{preVote: tt.prevote}
		result := r.inconsistentRaftConfig(pb.Message{Type: tt.mt})
		if result != tt.result {
			t.Errorf("%d, unexpected result", idx)
		}
	}
}

func TestCastVoteToDifferentNodesIsAllowed(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	a.preVote = true
	a.becomeFollower(10, 3)
	ne(a.Handle(pb.Message{Type: pb.RequestPreVote, From: 2, Term: 11}), t)
	if len(a.msgs) != 1 {
		t.Fatalf("no message")
	}
	expected := pb.Message{Type: pb.RequestPreVoteResp, From: 1, To: 2, Term: 11}
	if !reflect.DeepEqual(expected, a.msgs[0]) {
		t.Errorf("unexpected msg")
	}
	a.msgs = nil
	ne(a.Handle(pb.Message{Type: pb.RequestPreVote, From: 3, Term: 11}), t)
	if len(a.msgs) != 1 {
		t.Fatalf("no message")
	}
	expected = pb.Message{Type: pb.RequestPreVoteResp, From: 1, To: 3, Term: 11}
	if !reflect.DeepEqual(expected, a.msgs[0]) {
		t.Errorf("unexpected msg")
	}
}

func TestHandleLogQuery(t *testing.T) {
	p := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	assert.NoError(t, p.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}))
	assert.Equal(t, leader, p.state)
	committed := p.log.committed
	for i := 0; i < 10; i++ {
		assert.NoError(t, p.Handle(pb.Message{
			From:    1,
			To:      1,
			Type:    pb.Propose,
			Entries: []pb.Entry{{Cmd: []byte("test-data")}},
		}))
	}
	assert.Equal(t, committed+10, p.log.committed)
	assert.NoError(t, p.Handle(pb.Message{
		Type: pb.LogQuery,
		From: 1,
		To:   committed + 11,
		Hint: math.MaxUint64,
	}))
	entries, err := p.log.getEntries(1, 12, math.MaxUint64)
	assert.NoError(t, err)
	expected := &pb.LogQueryResult{
		FirstIndex: p.log.firstIndex(),
		LastIndex:  p.log.committed + 1,
		Error:      nil,
		Entries:    entries,
	}
	assert.Equal(t, expected, p.logQueryResult)
}

func TestHandleLogQueryWillPanicWhenRepeatedlyCalled(t *testing.T) {
	p := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	assert.NoError(t, p.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}))
	assert.Equal(t, leader, p.state)
	committed := p.log.committed
	for i := 0; i < 10; i++ {
		assert.NoError(t, p.Handle(pb.Message{
			From:    1,
			To:      1,
			Type:    pb.Propose,
			Entries: []pb.Entry{{Cmd: []byte("test-data")}},
		}))
	}
	assert.Equal(t, committed+10, p.log.committed)
	assert.Nil(t, p.logQueryResult)
	assert.NoError(t, p.Handle(pb.Message{
		Type: pb.LogQuery,
		From: 1,
		To:   committed + 11,
		Hint: math.MaxUint64,
	}))
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	assert.NotNil(t, p.logQueryResult)
	assert.NoError(t, p.Handle(pb.Message{
		Type: pb.LogQuery,
		From: 1,
		To:   committed + 11,
		Hint: math.MaxUint64,
	}))
}

func TestHandleLogQueryCanHandleRangeError(t *testing.T) {
	p := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	assert.NoError(t, p.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}))
	assert.Equal(t, leader, p.state)
	committed := p.log.committed
	for i := 0; i < 10; i++ {
		assert.NoError(t, p.Handle(pb.Message{
			From:    1,
			To:      1,
			Type:    pb.Propose,
			Entries: []pb.Entry{{Cmd: []byte("test-data")}},
		}))
	}
	assert.Equal(t, committed+10, p.log.committed)
	assert.NoError(t, p.Handle(pb.Message{
		Type: pb.LogQuery,
		From: 13,
		To:   14,
		Hint: math.MaxUint64,
	}))
	expected := &pb.LogQueryResult{
		FirstIndex: p.log.firstIndex(),
		LastIndex:  p.log.committed + 1,
		Error:      ErrCompacted,
		Entries:    nil,
	}
	assert.Equal(t, expected, p.logQueryResult)
}

func TestSetLeaderIDWillSetLeaderInfo(t *testing.T) {
	r := raft{term: 200}
	r.setLeaderID(100)
	assert.Equal(t, uint64(100), r.leaderID)
	assert.Equal(t, &pb.LeaderUpdate{LeaderID: 100, Term: 200}, r.leaderUpdate)
}
