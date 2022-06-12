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

/*
This file contains tests which verify that the scenarios described
in the raft paper (https://ramcloud.stanford.edu/raft.pdf) are
handled by the raft implementation correctly. Each test focuses on
several sentences written in the paper. This could help us to prevent
most implementation bugs.

Each test is composed of three parts: init, test and check.
Init part uses simple and understandable way to simulate the init state.
Test part uses Step function to generate the scenario. Check part checks
outgoing messages and state.
*/

//
// raft_paper_test.go is ported from etcd raft for testing purposes.
// updates have been made to reflect the interface & implementation differences
//

package raft

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/lni/dragonboat/v4/logger"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestFollowerUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, follower)
}
func TestCandidateUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, candidate)
}
func TestLeaderUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, leader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
func testUpdateTermFromMessage(t *testing.T, state State) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	switch state {
	case follower:
		r.becomeFollower(1, 2)
	case candidate:
		r.becomeCandidate()
	case leader:
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
	}

	ne(r.Handle(pb.Message{Type: pb.Replicate, Term: 2}), t)

	if r.term != 2 {
		t.Errorf("term = %d, want %d", r.term, 2)
	}
	if r.state != follower {
		t.Errorf("state = %v, want %v", r.state, follower)
	}
}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
func TestRejectStaleTermMessage(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) error {
		called = true
		return nil
	}
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.handle = fakeStep
	r.loadState(pb.State{Term: 2})

	ne(r.Handle(pb.Message{Type: pb.Replicate, Term: r.term - 1}), t)

	if called {
		t.Errorf("stepFunc called = %v, want %v", called, false)
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	if r.state != follower {
		t.Errorf("state = %s, want %s", r.state, follower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers.
// Reference: section 5.2
func TestLeaderBcastBeat(t *testing.T) {
	// heartbeat interval
	hi := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	for i := 0; i < 10; i++ {
		ne(r.appendEntries([]pb.Entry{{Index: uint64(i) + 1}}), t)
	}

	for i := 0; i < hi; i++ {
		ne(r.tick(), t)
	}

	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.Heartbeat},
		{From: 1, To: 3, Term: 1, Type: pb.Heartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

func TestFollowerStartElection(t *testing.T) {
	testNonleaderStartElection(t, follower)
}
func TestCandidateStartNewElection(t *testing.T) {
	testNonleaderStartElection(t, candidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the shard.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
func testNonleaderStartElection(t *testing.T, state State) {
	// election timeout
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewTestLogDB())
	switch state {
	case follower:
		r.becomeFollower(1, 2)
	case candidate:
		r.becomeCandidate()
	}

	for i := 1; i < 2*et; i++ {
		ne(r.tick(), t)
	}

	if r.term != 2 {
		t.Errorf("term = %d, want 2", r.term)
	}
	if r.state != candidate {
		t.Errorf("state = %s, want %s", r.state, candidate)
	}
	if !r.votes[r.replicaID] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 2, Type: pb.RequestVote},
		{From: 1, To: 3, Term: 2, Type: pb.RequestVote},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
func TestLeaderElectionInOneRoundRPC(t *testing.T) {
	tests := []struct {
		size  int
		votes map[uint64]bool
		state State
	}{
		// win the election when receiving votes from a majority of the servers
		{1, map[uint64]bool{}, leader},
		{3, map[uint64]bool{2: true, 3: true}, leader},
		{3, map[uint64]bool{2: true}, leader},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, leader},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, leader},
		{5, map[uint64]bool{2: true, 3: true}, leader},

		// return to follower state if it receives vote denial from a majority
		{3, map[uint64]bool{2: false, 3: false}, follower},
		{5, map[uint64]bool{2: false, 3: false, 4: false, 5: false}, follower},
		{5, map[uint64]bool{2: true, 3: false, 4: false, 5: false}, follower},

		// stay in candidate if it does not obtain the majority
		{3, map[uint64]bool{}, candidate},
		{5, map[uint64]bool{2: true}, candidate},
		{5, map[uint64]bool{2: false, 3: false}, candidate},
		{5, map[uint64]bool{}, candidate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, NewTestLogDB())

		ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}), t)
		for id, vote := range tt.votes {
			ne(r.Handle(pb.Message{From: id, To: 1, Type: pb.RequestVoteResp, Reject: !vote}), t)
		}

		if r.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, r.state, tt.state)
		}
		if g := r.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
func TestFollowerVote(t *testing.T) {
	tests := []struct {
		vote    uint64
		nvote   uint64
		wreject bool
	}{
		{NoLeader, 1, false},
		{NoLeader, 2, false},
		{1, 1, false},
		{2, 2, false},
		{1, 2, true},
		{2, 1, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		r.loadState(pb.State{Term: 1, Vote: tt.vote})

		ne(r.Handle(pb.Message{From: tt.nvote, To: 1, Term: 1, Type: pb.RequestVote}), t)

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, Type: pb.RequestVoteResp, Reject: tt.wreject},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %v, want %v", i, msgs, wmsgs)
		}
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
func TestCandidateFallback(t *testing.T) {
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, Type: pb.Replicate},
		{From: 2, To: 1, Term: 2, Type: pb.Replicate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}), t)
		if r.state != candidate {
			t.Fatalf("unexpected state = %s, want %s", r.state, candidate)
		}

		ne(r.Handle(tt), t)

		if g := r.state; g != follower {
			t.Errorf("#%d: state = %s, want %s", i, g, follower)
		}
		if g := r.term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func TestFollowerElectionTimeoutRandomized(t *testing.T) {
	//SetLogger(discardLogger)
	//defer SetLogger(defaultLogger)
	plog.SetLevel(logger.WARNING)
	defer plog.SetLevel(logger.INFO)
	testNonleaderElectionTimeoutRandomized(t, follower)
}
func TestCandidateElectionTimeoutRandomized(t *testing.T) {
	//SetLogger(discardLogger)
	//defer SetLogger(defaultLogger)
	plog.SetLevel(logger.WARNING)
	defer plog.SetLevel(logger.INFO)
	testNonleaderElectionTimeoutRandomized(t, candidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
func testNonleaderElectionTimeoutRandomized(t *testing.T, state State) {
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewTestLogDB())
	timeouts := make(map[int]bool)
	for round := 0; round < 50*et; round++ {
		switch state {
		case follower:
			r.becomeFollower(r.term+1, 2)
		case candidate:
			r.becomeCandidate()
		}

		time := 0
		for len(r.readMessages()) == 0 {
			ne(r.tick(), t)
			time++
		}
		timeouts[time] = true
	}

	for d := et + 1; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

func TestFollowersElectioinTimeoutNonconflict(t *testing.T) {
	//SetLogger(discardLogger)
	//defer SetLogger(defaultLogger)
	plog.SetLevel(logger.WARNING)
	defer plog.SetLevel(logger.INFO)
	testNonleadersElectionTimeoutNonconflict(t, follower)
}
func TestCandidatesElectionTimeoutNonconflict(t *testing.T) {
	//SetLogger(discardLogger)
	//defer SetLogger(defaultLogger)
	plog.SetLevel(logger.WARNING)
	defer plog.SetLevel(logger.INFO)
	testNonleadersElectionTimeoutNonconflict(t, candidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state State) {
	et := 10
	size := 5
	rs := make([]*raft, size)
	ids := idsBySize(size)
	for k := range rs {
		rs[k] = newTestRaft(ids[k], ids, et, 1, NewTestLogDB())
	}
	conflicts := 0
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case follower:
				r.becomeFollower(r.term+1, NoLeader)
			case candidate:
				r.becomeCandidate()
			}
		}

		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				ne(r.tick(), t)
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		if timeoutNum > 1 {
			conflicts++
		}
	}

	if g := float64(conflicts) / 1000; g > 0.3 {
		t.Errorf("probability of conflicts = %v, want <= 0.3", g)
	}
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
func TestLeaderCommitEntry(t *testing.T) {
	s := NewTestLogDB()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	commitNoopEntry(r, s)
	li := r.log.lastIndex()
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}}), t)

	for _, m := range r.readMessages() {
		ne(r.Handle(acceptAndReply(m)), t)
	}

	if g := r.log.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Cmd: []byte("some data")}}
	g, err := r.log.entriesToApply()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %x, want %x", m.To, w)
		}
		if m.Type != pb.Replicate {
			t.Errorf("type = %v, want %v", m.Type, pb.Replicate)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit(t *testing.T) {
	tests := []struct {
		size      int
		acceptors map[uint64]bool
		wack      bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := NewTestLogDB()
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, s)
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
		commitNoopEntry(r, s)
		li := r.log.lastIndex()
		ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}}), t)

		for _, m := range r.readMessages() {
			if tt.acceptors[m.To] {
				ne(r.Handle(acceptAndReply(m)), t)
			}
		}

		if g := r.log.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestLeaderCommitPrecedingEntries(t *testing.T) {
	tests := [][]pb.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(tt); err != nil {
			t.Fatalf("%v", err)
		}
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.loadState(pb.State{Term: 2})
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
		ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}}), t)

		for _, m := range r.readMessages() {
			ne(r.Handle(acceptAndReply(m)), t)
		}

		li := uint64(len(tt))
		wents := append(tt, pb.Entry{Term: 3, Index: li + 1}, pb.Entry{Term: 3, Index: li + 2, Cmd: []byte("some data")})
		g, err := r.log.entriesToApply()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestFollowerCommitEntry(t *testing.T) {
	tests := []struct {
		ents   []pb.Entry
		commit uint64
	}{
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Cmd: []byte("some data")},
			},
			1,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Cmd: []byte("some data")},
				{Term: 1, Index: 2, Cmd: []byte("some data2")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Cmd: []byte("some data2")},
				{Term: 1, Index: 2, Cmd: []byte("some data")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Cmd: []byte("some data")},
				{Term: 1, Index: 2, Cmd: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		r.becomeFollower(1, 2)

		ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.Replicate, Term: 1, Entries: tt.ents, Commit: tt.commit}), t)

		if g := r.log.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := tt.ents[:int(tt.commit)]
		g, err := r.log.entriesToApply()
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextEnts = %v, want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckReplicate tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
func TestFollowerCheckReplicate(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term        uint64
		index       uint64
		windex      uint64
		wreject     bool
		wrejectHint uint64
	}{
		// match with committed entries
		{0, 0, 1, false, 0},
		{ents[0].Term, ents[0].Index, 1, false, 0},
		// match with uncommitted entries
		{ents[1].Term, ents[1].Index, 2, false, 0},

		// unmatch with existing entry
		{ents[0].Term, ents[1].Index, ents[1].Index, true, 2},
		// unexisting entry
		{ents[1].Term + 1, ents[1].Index + 1, ents[1].Index + 1, true, 2},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(ents); err != nil {
			t.Fatalf("%v", err)
		}
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.loadState(pb.State{Commit: 1})
		r.becomeFollower(2, 2)

		ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.Replicate, Term: 2, LogTerm: tt.term, LogIndex: tt.index}), t)

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: 2, Type: pb.ReplicateResp, Term: 2, LogIndex: tt.windex, Reject: tt.wreject, Hint: tt.wrejectHint},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %+v, want %+v", i, msgs, wmsgs)
		}
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestFollowerAppendEntries(t *testing.T) {
	tests := []struct {
		index, term uint64
		ents        []pb.Entry
		wents       []pb.Entry
		wunstable   []pb.Entry
	}{
		{
			2, 2,
			[]pb.Entry{{Term: 3, Index: 3}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			[]pb.Entry{{Term: 3, Index: 3}},
		},
		{
			1, 1,
			[]pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
		{
			0, 0,
			[]pb.Entry{{Term: 1, Index: 1}},
			[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			nil,
		},
		{
			0, 0,
			[]pb.Entry{{Term: 3, Index: 1}},
			[]pb.Entry{{Term: 3, Index: 1}},
			[]pb.Entry{{Term: 3, Index: 1}},
		},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}); err != nil {
			t.Fatalf("%v", err)
		}
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.becomeFollower(2, 2)

		ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.Replicate, Term: 2, LogTerm: tt.term, LogIndex: tt.index, Entries: tt.ents}), t)

		if g := getAllEntries(r.log); !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, tt.wents)
		}
		if g := r.log.entriesToSave(); !reflect.DeepEqual(g, tt.wunstable) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, g, tt.wunstable)
		}
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
func TestLeaderSyncFollowerLog(t *testing.T) {
	ents := []pb.Entry{
		{},
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5},
		{Term: 5, Index: 6}, {Term: 5, Index: 7},
		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
	}
	term := uint64(8)
	tests := [][]pb.Entry{
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10}, {Term: 6, Index: 11},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
			{Term: 7, Index: 11}, {Term: 7, Index: 12},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
			{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9}, {Term: 3, Index: 10}, {Term: 3, Index: 11},
		},
	}
	for i, tt := range tests {
		leadStorage := NewTestLogDB()
		if err := leadStorage.Append(ents); err != nil {
			t.Fatalf("%v", err)
		}
		lead := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, leadStorage)
		lead.loadState(pb.State{Commit: lead.log.lastIndex(), Term: term})
		followerStorage := NewTestLogDB()
		if err := followerStorage.Append(tt); err != nil {
			t.Fatalf("%v", err)
		}
		follower := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, followerStorage)
		follower.loadState(pb.State{Term: term - 1})
		// It is necessary to have a three-node shard.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		n := newNetwork(lead, follower, nopStepper)
		n.send(pb.Message{From: 1, To: 1, Type: pb.Election})
		// The election occurs in the term after the one we loaded with
		// lead.loadState above.
		n.send(pb.Message{From: 3, To: 1, Type: pb.RequestVoteResp, Term: term + 1})

		n.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})

		if g := diffu(ltoa(lead.log), ltoa(follower.log)); g != "" {
			t.Errorf("#%d: log diff:\n%s", i, g)
		}
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest(t *testing.T) {
	tests := []struct {
		ents  []pb.Entry
		wterm uint64
	}{
		{[]pb.Entry{{Term: 1, Index: 1}}, 2},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		ne(r.Handle(pb.Message{
			From: 2, To: 1, Type: pb.Replicate, Term: tt.wterm - 1, LogTerm: 0, LogIndex: 0, Entries: tt.ents,
		}), t)
		r.readMessages()

		for i := uint64(1); i < r.electionTimeout*2; i++ {
			ne(r.nonLeaderTick(), t)
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		for i, m := range msgs {
			if m.Type != pb.RequestVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.RequestVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}
			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.LogIndex != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.LogIndex, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]pb.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(tt.ents); err != nil {
			t.Fatalf("%v", err)
		}
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)

		ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.RequestVote, Term: 3, LogTerm: tt.logterm, LogIndex: tt.index}), t)

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.Type != pb.RequestVoteResp {
			t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.RequestVoteResp)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
func TestLeaderOnlyCommitsLogFromCurrentTerm(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(ents); err != nil {
			t.Fatalf("%v", err)
		}
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
		r.loadState(pb.State{Term: 2})
		// become leader at term 3
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
		r.readMessages()
		// propose a entry to current term
		ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}}), t)

		ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.ReplicateResp, Term: r.term, LogIndex: tt.index}), t)
		if r.log.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.log.committed, tt.wcommit)
		}
	}
}

func TestLeaderStartReplication(t *testing.T) {
	s := NewTestLogDB()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	commitNoopEntry(r, s)
	li := r.log.lastIndex()

	ents := []pb.Entry{{Cmd: []byte("some data")}}
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: ents}), t)
	if g := r.log.lastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.log.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Cmd: []byte("some data")}}
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.Replicate, LogIndex: li, LogTerm: 1, Entries: wents, Commit: li},
		{From: 1, To: 3, Term: 1, Type: pb.Replicate, LogIndex: li, LogTerm: 1, Entries: wents, Commit: li},
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v, want %+v", msgs, wmsgs)
	}
	r.log.inmem.savedTo = li
	if g := r.log.entriesToSave(); !reflect.DeepEqual(g, wents) {
		t.Errorf("ents = %+v, want %+v", g, wents)
	}
}

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func commitNoopEntry(r *raft, s ILogDB) {
	if r.state != leader {
		panic("it should only be used when it is the leader")
	}
	r.broadcastReplicateMessage()
	// simulate the response of Replicate
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.Type != pb.Replicate || len(m.Entries) != 1 || m.Entries[0].Cmd != nil {
			panic("not a message to append noop entry")
		}
		if err := r.Handle(acceptAndReply(m)); err != nil {
			panic(err)
		}
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	if err := s.Append(r.log.entriesToSave()); err != nil {
		panic(err)
	}
	term, err := r.log.lastTerm()
	if err != nil {
		panic(err)
	}
	r.log.commitUpdate(pb.UpdateCommit{
		Processed:     r.log.committed,
		StableLogTo:   r.log.lastIndex(),
		StableLogTerm: term,
	})
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.Type != pb.Replicate {
		panic("type should be Replicate")
	}
	return pb.Message{
		From:     m.To,
		To:       m.From,
		Term:     m.Term,
		Type:     pb.ReplicateResp,
		LogIndex: m.LogIndex + uint64(len(m.Entries)),
	}
}
