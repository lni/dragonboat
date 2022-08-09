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

//
// raft_etcd_test.go is ported from etcd rafto.
// updates have been made to reflect interface & implementation differences
// between dragonboat and etcd raft. some irrelevant tests were removed.
//

package raft

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	testRateLimit = uint64(1024 * 128)
)

func (r *raft) testOnlyHasConfigChangeToApply() bool {
	entries, err := r.log.getEntriesToApply(noLimit)
	if err != nil {
		panic(err)
	}
	if r.log.committed > r.log.processed && len(entries) > 0 {
		return countConfigChange(entries) > 0
	}

	return false
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := fileutil.CreateTemp("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func ltoa(l *entryLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.processed)
	for i, e := range getAllEntries(l) {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

// nextEnts returns the appliable entries and updates the applied index
func nextEnts(r *raft, s ILogDB) (ents []pb.Entry) {
	// Transfer all unstable entries to "stable" storage.
	if err := s.Append(r.log.entriesToSave()); err != nil {
		panic(err)
	}
	term, err := r.log.lastTerm()
	if err != nil {
		panic(err)
	}
	r.log.commitUpdate(pb.UpdateCommit{
		StableLogTo:   r.log.lastIndex(),
		StableLogTerm: term,
	})
	ents, err = r.log.entriesToApply()
	if err != nil {
		panic(err)
	}
	r.log.commitUpdate(pb.UpdateCommit{
		Processed: r.log.committed,
	})
	return ents
}

type stateMachine interface {
	Handle(m pb.Message) error
	readMessages() []pb.Message
}

func (r *raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

func checkLeaderTransferState(t *testing.T, r *raft, state State, lead uint64) {
	if r.state != state || r.leaderID != lead {
		t.Fatalf("after transferring, node has state %v lead %v, want state %v lead %v", r.state, r.leaderID, state, lead)
	}
	if r.leaderTransferTarget != NoNode {
		t.Fatalf("after transferring, node has leadTransferee %v, want leadTransferee %v", r.leaderTransferTarget, NoNode)
	}
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
func TestLeaderTransferToUpToDateNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	if lead.leaderID != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leaderID)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Hint: 2, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, follower, 2)
	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.send(pb.Message{From: 1, To: 2, Hint: 1, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
func TestLeaderTransferToUpToDateNodeFromFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	if lead.leaderID != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leaderID)
	}
	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 2, Hint: 2, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, follower, 2)
	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.send(pb.Message{From: 1, To: 1, Hint: 1, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

// TestLeaderTransferWithPreVote ensures transferring leader still works
// even the current leader is still under its leader lease
func TestLeaderTransferWithPreVote(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := uint64(1); i < 4; i++ {
		r := nt.peers[i].(*raft)
		r.checkQuorum = true
		r.preVote = true
		setRandomizedElectionTimeout(r, r.electionTimeout+i)
	}
	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*raft)
	for i := uint64(0); i < f.electionTimeout; i++ {
		ne(f.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	if lead.leaderID != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leaderID)
	}
	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Hint: 2, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, follower, 2)
	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.send(pb.Message{From: 1, To: 2, Hint: 1, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
func TestLeaderTransferWithCheckQuorum(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := uint64(1); i < 4; i++ {
		r := nt.peers[i].(*raft)
		r.checkQuorum = true
		setRandomizedElectionTimeout(r, r.electionTimeout+i)
	}
	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*raft)
	for i := uint64(0); i < f.electionTimeout; i++ {
		ne(f.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	if lead.leaderID != 1 {
		t.Fatalf("after election leader is %x, want 1", lead.leaderID)
	}
	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Hint: 2, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, follower, 2)
	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.send(pb.Message{From: 1, To: 2, Hint: 1, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

func TestLeaderTransferToSlowFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.recover()
	lead := nt.peers[1].(*raft)
	if lead.remotes[3].match != 1 {
		t.Fatalf("node 1 has match %x for node 3, want %x", lead.remotes[3].match, 1)
	}
	// Transfer leadership to 3 when node 3 is lack of log.
	// this will not actually transfer the leadership. on receiving the
	// MsgLeaderTransfer, dragonboat's raft implementation won't force a MsgAppend msg
	// to be sent
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.state != leader || lead.leaderID != 1 {
		t.Errorf("leadership transferred to unexpected node")
	}
	if !lead.leaderTransfering() {
		t.Errorf("leader transfer flag is gone")
	}
	lead.abortLeaderTransfer()
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, follower, 3)
}

func TestLeaderTransferAfterSnapshot(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	lead := nt.peers[1].(*raft)
	nextEnts(lead, nt.storage[1])
	m := getTestMembership(lead.nodesSorted())
	ss, err := nt.storage[1].(*TestLogDB).getSnapshot(lead.log.processed, &m)
	if err != nil {
		t.Fatalf("failed to get snapshot")
	}
	if err := nt.storage[1].CreateSnapshot(ss); err != nil {
		t.Fatalf("%v", err)
	}
	if err := nt.storage[1].Compact(lead.log.processed); err != nil {
		t.Fatalf("%v", err)
	}
	nt.recover()
	if lead.remotes[3].match != 1 {
		t.Fatalf("node 1 has match %x for node 3, want %x", lead.remotes[3].match, 1)
	}
	// Transfer leadership to 3 when node 3 is lack of snapshot.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	// Send pb.HeartbeatResp to leader to trigger a snapshot for node 3.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.HeartbeatResp})
	checkLeaderTransferState(t, lead, follower, 3)
}

func TestLeaderTransferToSelf(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, Hint: 1, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

func TestLeaderTransferToNonExistingNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	lead := nt.peers[1].(*raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, Hint: 4, Type: pb.LeaderTransfer})
	checkLeaderTransferState(t, lead, leader, 1)
}

func TestLeaderTransferTimeout(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	lead := nt.peers[1].(*raft)
	// Transfer leadership to isolated node, wait for timeout.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	for i := uint64(0); i < lead.heartbeatTimeout; i++ {
		ne(lead.tick(), t)
	}
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	for i := uint64(0); i < lead.electionTimeout; i++ {
		ne(lead.tick(), t)
	}
	checkLeaderTransferState(t, lead, leader, 1)
}

func TestLeaderTransferIgnoreProposal(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	lead := nt.peers[1].(*raft)
	// Transfer leadership to isolated node to let transfer pending, then send proposal.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	matched := lead.remotes[2].match
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	if lead.remotes[2].match != matched {
		t.Fatalf("node 1 has match %x, want %x", lead.remotes[2].match, matched)
	}
}

func TestLeaderTransferReceiveHigherTermVote(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	lead := nt.peers[1].(*raft)
	// Transfer leadership to isolated node to let transfer pending.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	nt.send(pb.Message{From: 2, To: 2, Type: pb.Election, LogIndex: 1, Term: 2})
	checkLeaderTransferState(t, lead, follower, 2)
}

func TestLeaderTransferRemoveNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.ignore(pb.TimeoutNow)
	lead := nt.peers[1].(*raft)
	// The leadTransferee is removed when leadship transferring.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	ne(lead.removeNode(3), t)
	checkLeaderTransferState(t, lead, leader, 1)
}

func TestNewLeaderTransferCanNotOverrideOngoingLeaderTransfer(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	lead := nt.peers[1].(*raft)
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	ot := lead.electionTick
	nt.send(pb.Message{From: 1, To: 1, Hint: 1, Type: pb.LeaderTransfer})
	// the above MsgLeaderTransfer should be ignored
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	if lead.electionTick != ot {
		t.Fatalf("election tick changed unexpectedly")
	}
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
func TestLeaderTransferSecondTransferToSameNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.isolate(3)
	lead := nt.peers[1].(*raft)
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	if lead.leaderTransferTarget != 3 {
		t.Fatalf("wait transferring, leadTransferee = %v, want %v", lead.leaderTransferTarget, 3)
	}
	for i := uint64(0); i < lead.heartbeatTimeout; i++ {
		ne(lead.tick(), t)
	}
	// Second transfer leadership request to the same node.
	nt.send(pb.Message{From: 3, To: 1, Hint: 3, Type: pb.LeaderTransfer})
	for i := uint64(0); i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		ne(lead.tick(), t)
	}
	checkLeaderTransferState(t, lead, leader, 1)
}

// TestRemoteResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
func TestRemoteResumeByHeartbeatResp(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.remotes[2].retryToWait()

	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.LeaderHeartbeat}), t)
	if r.remotes[2].state != remoteWait {
		t.Errorf("st = %s, want true", r.remotes[2].state)
	}

	r.remotes[2].becomeReplicate()
	ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.HeartbeatResp}), t)
	if r.remotes[2].state == remoteWait {
		t.Errorf("paused = %s, want false", r.remotes[2].state)
	}
}

func TestRemotePaused(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)

	ms := r.readMessages()
	if len(ms) != 1 {
		t.Errorf("len(ms) = %d, want 1", len(ms))
	}
}

func TestLeaderElection(t *testing.T) {
	testLeaderElection(t)
}

/*
func TestLeaderElectionPreVote(t *testing.T) {
	testLeaderElection(t, true)
}
*/

func testLeaderElection(t *testing.T) {
	var cfg func(config.Config)
	tests := []struct {
		*network
		state   State
		expTerm uint64
	}{
		{newNetworkWithConfig(cfg, nil, nil, nil), leader, 1},
		{newNetworkWithConfig(cfg, nil, nil, nopStepper), leader, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), candidate, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), candidate, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), leader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfig(cfg,
			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
			follower, 1},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
		sm := tt.network.peers[1].(*raft)
		var expState State
		var expTerm uint64
		expState = tt.state
		expTerm = tt.expTerm

		if sm.state != expState {
			t.Errorf("#%d: state = %s, want %s", i, sm.state, expState)
		}
		if g := sm.term; g != expTerm {
			t.Errorf("#%d: term = %d, want %d", i, g, expTerm)
		}
	}
}

func TestLeaderCycle(t *testing.T) {
	testLeaderCycle(t)
}

// testLeaderCycle verifies that each node in a shard can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
func testLeaderCycle(t *testing.T) {
	var cfg func(config.Config)
	n := newNetworkWithConfig(cfg, nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		n.send(pb.Message{From: campaignerID, To: campaignerID, Type: pb.Election})

		for _, peer := range n.peers {
			sm := peer.(*raft)
			if sm.replicaID == campaignerID && sm.state != leader {
				t.Errorf("campaigning node %d state = %v, want leader",
					sm.replicaID, sm.state)
			} else if sm.replicaID != campaignerID && sm.state != follower {
				t.Errorf("after campaign of node %d, "+
					"node %d had state = %v, want follower",
					campaignerID, sm.replicaID, sm.state)
			}
		}
	}
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
func TestLeaderElectionOverwriteNewerLogs(t *testing.T) {
	testLeaderElectionOverwriteNewerLogs(t)
}

func testLeaderElectionOverwriteNewerLogs(t *testing.T) {
	var cfg func(config.Config)
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	n := newNetworkWithConfig(cfg,
		entsWithConfig(cfg, 1),     // Node 1: Won first election
		entsWithConfig(cfg, 1),     // Node 2: Got logs from node 1
		entsWithConfig(cfg, 2),     // Node 3: Won second election
		votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
		votedWithConfig(cfg, 3, 2)) // Node 5: Voted but didn't get logs

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
	n.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	sm1 := n.peers[1].(*raft)
	if sm1.state != follower {
		t.Errorf("state = %s, want follower", sm1.state)
	}
	if sm1.term != 2 {
		t.Errorf("term = %d, want 2", sm1.term)
	}

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	if sm1.state != leader {
		t.Errorf("state = %s, want leader", sm1.state)
	}
	if sm1.term != 3 {
		t.Errorf("term = %d, want 3", sm1.term)
	}

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for i := range n.peers {
		sm := n.peers[i].(*raft)
		entries := getAllEntries(sm.log)
		if len(entries) != 2 {
			t.Fatalf("node %d: len(entries) == %d, want 2", i, len(entries))
		}
		if entries[0].Term != 1 {
			t.Errorf("node %d: term at index 1 == %d, want 1", i, entries[0].Term)
		}
		if entries[1].Term != 3 {
			t.Errorf("node %d: term at index 2 == %d, want 3", i, entries[1].Term)
		}
	}
}

func TestVoteFromAnyState(t *testing.T) {
	testVoteFromAnyState(t, pb.RequestVote)
}

func testVoteFromAnyState(t *testing.T, vt pb.MessageType) {
	for st := State(0); st < numStates; st++ {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		r.term = 1

		switch st {
		case follower:
			r.becomeFollower(r.term, 3)
		case candidate:
			r.becomeCandidate()
		case leader:
			r.becomeCandidate()
			ne(r.becomeLeader(), t)
		}

		// Note that setting our state above may have advanced r.term
		// past its initial value.
		origTerm := r.term
		newTerm := r.term + 1

		msg := pb.Message{
			From:     2,
			To:       1,
			Type:     vt,
			Term:     newTerm,
			LogTerm:  newTerm,
			LogIndex: 42,
		}
		ne(r.Handle(msg), t)
		if len(r.msgs) != 1 {
			t.Errorf("%s,%s: %d response messages, want 1: %+v", vt, st, len(r.msgs), r.msgs)
		} else {
			resp := r.msgs[0]
			if resp.Type != pb.RequestVoteResp {
				t.Errorf("%s,%s: response message is %s, want %s",
					vt, st, resp.Type, pb.RequestVoteResp)
			}
			if resp.Reject {
				t.Errorf("%s,%s: unexpected rejection", vt, st)
			}
		}

		// If this was a real vote, we reset our state and term.
		if vt == pb.RequestVote {
			if r.state != follower {
				t.Errorf("%s,%s: state %s, want %s", vt, st, r.state, follower)
			}
			if r.term != newTerm {
				t.Errorf("%s,%s: term %d, want %d", vt, st, r.term, newTerm)
			}
			if r.vote != 2 {
				t.Errorf("%s,%s: vote %d, want 2", vt, st, r.vote)
			}
		} else {
			// In a prevote, nothing changes.
			if r.state != st {
				t.Errorf("%s,%s: state %s, want %s", vt, st, r.state, st)
			}
			if r.term != origTerm {
				t.Errorf("%s,%s: term %d, want %d", vt, st, r.term, origTerm)
			}
			// if st == follower or statePreCandidate, r hasn't voted yet.
			// In candidate or leader, it's voted for itself.
			if r.vote != NoLeader && r.vote != 1 {
				t.Errorf("%s,%s: vote %d, want %d or 1", vt, st, r.vote, NoLeader)
			}
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}},
				{From: 1, To: 2, Type: pb.Election},
				{From: 1, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*raft)

			if sm.log.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.log.committed, tt.wcommitted)
			}

			ents := []pb.Entry{}
			for _, e := range nextEnts(sm, tt.network.storage[j]) {
				if e.Cmd != nil {
					ents = append(ents, e)
				}
			}
			props := []pb.Message{}
			for _, m := range tt.msgs {
				if m.Type == pb.Propose {
					props = append(props, m)
				}
			}
			for k, m := range props {
				if !bytes.Equal(ents[k].Cmd, m.Entries[0].Cmd) {
					t.Errorf("#%d.%d: data = %d, want %d", i, j, ents[k].Cmd, m.Entries[0].Cmd)
				}
			}
		}
	}
}

func TestSingleNodeCommit(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.log.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 3)
	}
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 1)
	}

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(pb.Replicate)

	// elect 2 as the new leader with term 2
	tt.send(pb.Message{From: 2, To: 2, Type: pb.Election})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*raft)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 1)
	}

	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 2, To: 2, Type: pb.LeaderHeartbeat})
	// append an entry at current term
	tt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})
	// expect the committed to be advanced
	if sm.log.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 5)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 1)
	}

	// network recovery
	tt.recover()

	// elect 1 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: 2, To: 2, Type: pb.Election})

	if sm.log.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 4)
	}
}

func TestDuelingCandidates(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	if sm.state != leader {
		t.Errorf("state = %s, want %s", sm.state, leader)
	}

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = nt.peers[3].(*raft)
	if sm.state != candidate {
		t.Errorf("state = %s, want %s", sm.state, candidate)
	}

	nt.recover()

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	wlog := &entryLog{
		logdb:     &TestLogDB{entries: []pb.Entry{{Cmd: nil, Term: 1, Index: 1}}},
		committed: 1,
		inmem:     inMemory{markerIndex: 2},
	}
	tests := []struct {
		sm       *raft
		state    State
		term     uint64
		entryLog *entryLog
	}{
		{a, follower, 2, wlog},
		{b, follower, 2, wlog},
		{c, follower, 2, newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.entryLog)
		if sm, ok := nt.peers[1+uint64(i)].(*raft); ok {
			l := ltoa(sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestDuelingPreCandidates(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	a.preVote = true
	b.preVote = true
	c.preVote = true
	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	if sm.state != leader {
		t.Errorf("state = %s, want %s", sm.state, leader)
	}

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = nt.peers[3].(*raft)
	if sm.state != follower {
		t.Errorf("state = %s, want %s", sm.state, follower)
	}

	nt.recover()

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	wlog := &entryLog{
		logdb:     &TestLogDB{entries: []pb.Entry{{Cmd: nil, Term: 1, Index: 1}}},
		committed: 1,
		inmem:     inMemory{markerIndex: 2},
	}
	tests := []struct {
		sm       *raft
		state    State
		term     uint64
		entryLog *entryLog
	}{
		{a, leader, 1, wlog},
		{b, follower, 1, wlog},
		{c, follower, 1, newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.entryLog)
		if sm, ok := nt.peers[1+uint64(i)].(*raft); ok {
			l := ltoa(sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	tt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	// heal the partition
	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 3, To: 3, Type: pb.LeaderHeartbeat})

	data := []byte("force follower")
	// send a proposal to 3 to flush out a MsgAppend to 1
	tt.send(pb.Message{From: 3, To: 3, Type: pb.Propose, Entries: []pb.Entry{{Cmd: data}}})
	// send heartbeat; flush out commit
	tt.send(pb.Message{From: 3, To: 3, Type: pb.LeaderHeartbeat})

	a := tt.peers[1].(*raft)
	if g := a.state; g != follower {
		t.Errorf("state = %s, want %s", g, follower)
	}
	if g := a.term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wantLog := ltoa(&entryLog{
		logdb: &TestLogDB{
			entries: []pb.Entry{{Cmd: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Cmd: data}},
		},
		inmem:     inMemory{markerIndex: 3},
		committed: 2,
	})
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.log)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	sm := tt.peers[1].(*raft)
	if sm.state != leader {
		t.Errorf("state = %d, want %d", sm.state, leader)
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	tt.send(pb.Message{From: 2, To: 2, Type: pb.Election})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(pb.Message{From: 2, To: 1, Type: pb.Replicate, Term: 2, Entries: []pb.Entry{{Index: 3, Term: 2}}})
	// commit a new entry
	tt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}})

	ilog := &entryLog{
		logdb: &TestLogDB{
			entries: []pb.Entry{
				{Cmd: nil, Term: 1, Index: 1},
				{Cmd: nil, Term: 2, Index: 2}, {Cmd: nil, Term: 3, Index: 3},
				{Cmd: []byte("somedata"), Term: 3, Index: 4},
			},
		},
		inmem:     inMemory{markerIndex: 5},
		committed: 4,
	}
	base := ltoa(ilog)
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// TestOldMessagesReply - optimization - reply with new term.

func TestProposal(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for j, tt := range tests {
		success := tt.success
		jj := j
		ttt := tt
		send := func(m pb.Message) {
			defer func() {
				// only recover is we expect it to panic so
				// panics we don't expect go up.
				if !success {
					e := recover()
					if e != nil {
						t.Logf("#%d: err: %s", jj, e)
					}
				}
			}()
			ttt.send(m)
		}

		data := []byte("somedata")

		// promote 0 the leader
		send(pb.Message{From: 1, To: 1, Type: pb.Election})
		send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: data}}})

		wantLog := newEntryLog(NewTestLogDB(), server.NewInMemRateLimiter(0))
		if tt.success {
			wantLog = &entryLog{
				logdb: &TestLogDB{
					entries: []pb.Entry{{Cmd: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Cmd: data}},
				},
				inmem:     inMemory{markerIndex: 3},
				committed: 2}
		}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.log)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.network.peers[1].(*raft)
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", j, g, 1)
		}
	}
}

func TestProposalByProxy(t *testing.T) {
	data := []byte("somedata")
	tests := []*network{
		newNetwork(nil, nil, nil),
		newNetwork(nil, nil, nopStepper),
	}

	for j, tt := range tests {
		// promote 0 the leader
		tt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

		// propose via follower
		tt.send(pb.Message{From: 2, To: 2, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}})

		wantLog := &entryLog{
			logdb: &TestLogDB{
				entries: []pb.Entry{{Cmd: nil, Term: 1, Index: 1}, {Term: 1, Cmd: data, Index: 2}},
			},
			inmem:     inMemory{markerIndex: 3},
			committed: 2}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.log)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.peers[1].(*raft)
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", j, g, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []uint64
		logs    []pb.Entry
		smTerm  uint64
		w       uint64
	}{
		// single
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 1}}, 1, 1},
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 1}}, 2, 0},
		{[]uint64{2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{1}, []pb.Entry{{Index: 1, Term: 2}}, 2, 1},

		// odd
		// quorum 1, term according to log is 1, node's term is 1, advance the
		// committed value to 1
		{[]uint64{2, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		// quorum 1, term according to log is 1, node's term is 2, do nothing,
		// committed value remains 0
		{[]uint64{2, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},

		// even
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append(tt.logs); err != nil {
			t.Fatalf("%v", err)
		}
		storage.(*TestLogDB).state = pb.State{Term: tt.smTerm}

		sm := newTestRaft(1, []uint64{1}, 5, 1, storage)
		for j := 0; j < len(tt.matches); j++ {
			sm.setRemote(uint64(j)+1, tt.matches[j], tt.matches[j]+1)
		}
		sm.state = leader
		if _, err := sm.tryCommit(); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if g := sm.log.committed; g != tt.w {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.w)
		}
	}
}

func TestPastElectionTimeout(t *testing.T) {
	tests := []struct {
		elapse       int
		wprobability float64
		round        bool
	}{
		{5, 0, false},
		{10, 0.1, true},
		{13, 0.4, true},
		{15, 0.6, true},
		{18, 0.9, true},
		{20, 1, false},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
		sm.electionTick = uint64(tt.elapse)
		c := 0
		for j := 0; j < 10000; j++ {
			sm.setRandomizedElectionTimeout()
			if sm.timeForElection() {
				c++
			}
		}
		got := float64(c) / 10000.0
		if tt.round {
			got = math.Floor(got*10+0.5) / 10.0
		}
		if got != tt.wprobability {
			t.Errorf("#%d: probability = %v, want %v", i, got, tt.wprobability)
		}
	}
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) error {
		called = true
		return nil
	}
	sm := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	sm.handle = fakeStep
	sm.term = 2
	ne(sm.Handle(pb.Message{Type: pb.Replicate, Term: sm.term - 1}), t)
	if called {
		t.Errorf("stepFunc called = %v , want %v", called, false)
	}
}

// TestHandleMTReplicate ensures:
//  1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
//  2. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it; append any new entries not already in the log.
//  3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMTReplicate(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// lni
		// LogTerm and Index are the previous Log term/index.
		// Term is the term of the node. Commit is the log committed value (leader commit).

		// Ensure 1
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 3, LogIndex: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 3, LogIndex: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 1, LogIndex: 1, Commit: 1}, 2, 1, false},
		// conflict found, delete all existing logs, append the new one
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 0, LogIndex: 0, Commit: 1, Entries: []pb.Entry{{Index: 1, Term: 2}}}, 1, 1, false},
		// no conflict, both new logs appended, leaderCommit determines the commit value, last index in log is 4
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 2, LogIndex: 2, Commit: 3, Entries: []pb.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		// no conflict, one log appeneded, last index in log determines the commit value 3, last index in log is 3
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 2, LogIndex: 2, Commit: 4, Entries: []pb.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		// no conflict, no new entry. last index in log determines the commit value (2), last index in log is 2.
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 1, LogIndex: 1, Commit: 4, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		{pb.Message{Type: pb.Replicate, Term: 1, LogTerm: 1, LogIndex: 1, Commit: 3}, 2, 1, false},                                           // match entry 1, commit up to last new entry 1
		{pb.Message{Type: pb.Replicate, Term: 1, LogTerm: 1, LogIndex: 1, Commit: 3, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 2, LogIndex: 2, Commit: 3}, 2, 2, false},                                           // match entry 2, commit up to last new entry 2
		{pb.Message{Type: pb.Replicate, Term: 2, LogTerm: 2, LogIndex: 2, Commit: 4}, 2, 2, false},                                           // commit up to log.last()
	}

	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}); err != nil {
			t.Fatalf("%v", err)
		}
		sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
		sm.becomeFollower(2, NoLeader)

		ne(sm.handleReplicateMessage(tt.m), t)
		if sm.log.lastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.log.lastIndex(), tt.wIndex)
		}
		if sm.log.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.log.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Reject != tt.wReject {
			t.Errorf("#%d: reject = %v, want %v", i, m[0].Reject, tt.wReject)
		}
	}
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
func TestHandleHeartbeat(t *testing.T) {
	commit := uint64(2)
	tests := []struct {
		m       pb.Message
		wCommit uint64
	}{
		{pb.Message{From: 2, To: 1, Type: pb.Heartbeat, Term: 2, Commit: commit + 1}, commit + 1},
		{pb.Message{From: 2, To: 1, Type: pb.Heartbeat, Term: 2, Commit: commit - 1}, commit}, // do not decrease commit
	}

	// heatbeat from the leader tells the follower to performan the commitTo operation.
	// other than boundary checkings, nothing else got checked.
	for i, tt := range tests {
		storage := NewTestLogDB()
		if err := storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}); err != nil {
			t.Fatalf("%v", err)
		}
		sm := newTestRaft(1, []uint64{1, 2}, 5, 1, storage)
		sm.becomeFollower(2, 2)
		sm.log.commitTo(commit)
		ne(sm.handleHeartbeatMessage(tt.m), t)
		if sm.log.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.log.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Type != pb.HeartbeatResp {
			t.Errorf("#%d: type = %v, want MsgHeartbeatResp", i, m[0].Type)
		}
	}
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
func TestHandleHeartbeatResp(t *testing.T) {
	storage := NewTestLogDB()
	if err := storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}); err != nil {
		t.Fatalf("%v", err)
	}
	sm := newTestRaft(1, []uint64{1, 2}, 5, 1, storage)
	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)
	sm.log.commitTo(sm.log.lastIndex())
	// A heartbeat response from a node that is behind; re-send MTReplicate
	ne(sm.Handle(pb.Message{From: 2, Type: pb.HeartbeatResp}), t)
	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].Type != pb.Replicate {
		t.Errorf("type = %v, want MTReplicate", msgs[0].Type)
	}
	// A second heartbeat response generates another MTReplicate re-send
	ne(sm.Handle(pb.Message{From: 2, Type: pb.HeartbeatResp}), t)
	msgs = sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].Type != pb.Replicate {
		t.Errorf("type = %v, want MTReplicate", msgs[0].Type)
	}
	// Once we have an MTReplicateResp, heartbeats no longer send MTReplicate.
	ne(sm.Handle(pb.Message{
		From:     2,
		Type:     pb.ReplicateResp,
		LogIndex: msgs[0].LogIndex + uint64(len(msgs[0].Entries)),
	}), t)
	// Consume the message sent in response to MTReplicateResp
	sm.readMessages()

	ne(sm.Handle(pb.Message{From: 2, Type: pb.HeartbeatResp}), t)
	msgs = sm.readMessages()
	if len(msgs) != 0 {
		t.Fatalf("len(msgs) = %d, want 0: %+v", len(msgs), msgs)
	}
}

// TestMTReplicateRespWaitReset verifies the resume behavior of a leader
// MTReplicateResp.
func TestMTReplicateRespWaitReset(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())
	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	// The new leader has just emitted a new Term 4 entry; consume those messages
	// from the outgoing queue.
	sm.broadcastReplicateMessage()
	sm.readMessages()

	// Node 2 acks the first entry, making it committed.
	ne(sm.Handle(pb.Message{
		From:     2,
		Type:     pb.ReplicateResp,
		LogIndex: 1,
	}), t)
	if sm.log.committed != 1 {
		t.Fatalf("expected committed to be 1, got %d", sm.log.committed)
	}
	// Also consume the MTReplicate messages that update Commit on the followers.
	sm.readMessages()

	// A new command is now proposed on node 1.
	ne(sm.Handle(pb.Message{
		From:    1,
		Type:    pb.Propose,
		Entries: []pb.Entry{{}},
	}), t)

	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppendResp, but node 3 is still waiting.
	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != pb.Replicate || msgs[0].To != 2 {
		t.Errorf("expected MTReplicate to node 2, got %v to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}

	// lni
	// initially node 3 is in the probe state, the received MTReplicateResp message makes
	// the node to enter replicate state.
	if sm.remotes[3].state != remoteWait {
		t.Fatalf("expected state probe, got %d (%s)", sm.remotes[3].state, sm.remotes[3].state)
	}

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
	ne(sm.Handle(pb.Message{
		From:     3,
		Type:     pb.ReplicateResp,
		LogIndex: 1,
	}), t)

	// lni
	// now node 3 should be in replicate state
	if sm.remotes[3].state != remoteReplicate {
		t.Fatalf("expected state replicate, got %d (%s)", sm.remotes[3].state, sm.remotes[3].state)
	}

	msgs = sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != pb.Replicate || msgs[0].To != 3 {
		t.Errorf("expected MTReplicate to node 3, got %v to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].Index != 2 {
		t.Errorf("expected to send entry 2, but got %v", msgs[0].Entries)
	}
}

func TestRecvMsgVote(t *testing.T) {
	testRecvMsgVote(t, pb.RequestVote)
}

func testRecvMsgVote(t *testing.T, msgType pb.MessageType) {
	tests := []struct {
		state   State
		i, term uint64
		voteFor uint64
		wreject bool
	}{
		{follower, 0, 0, NoLeader, true},
		{follower, 0, 1, NoLeader, true},
		{follower, 0, 2, NoLeader, true},
		{follower, 0, 3, NoLeader, false},

		{follower, 1, 0, NoLeader, true},
		{follower, 1, 1, NoLeader, true},
		{follower, 1, 2, NoLeader, true},
		{follower, 1, 3, NoLeader, false},

		{follower, 2, 0, NoLeader, true},
		{follower, 2, 1, NoLeader, true},
		{follower, 2, 2, NoLeader, false},
		{follower, 2, 3, NoLeader, false},

		{follower, 3, 0, NoLeader, true},
		{follower, 3, 1, NoLeader, true},
		{follower, 3, 2, NoLeader, false},
		{follower, 3, 3, NoLeader, false},

		{follower, 3, 2, 2, false},
		{follower, 3, 2, 1, true},

		{leader, 3, 3, 1, true},
		//{statePreCandidate, 3, 3, 1, true},
		{candidate, 3, 3, 1, true},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
		sm.state = tt.state
		sm.vote = tt.voteFor
		sm.log = &entryLog{
			logdb: &TestLogDB{entries: []pb.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 2}}},
			inmem: inMemory{markerIndex: 3},
		}

		ne(sm.Handle(pb.Message{Type: msgType, From: 2, LogIndex: tt.i, LogTerm: tt.term}), t)

		msgs := sm.readMessages()
		if g := len(msgs); g != 1 {
			t.Fatalf("#%d: len(msgs) = %d, want 1", i, g)
		}
		if g := msgs[0].Reject; g != tt.wreject {
			t.Errorf("#%d, m.Reject = %v, want %v", i, g, tt.wreject)
		}
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   State
		to     State
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{follower, follower, true, 1, NoLeader},
		//{follower, statePreCandidate, true, 0, NoLeader},
		{follower, candidate, true, 1, NoLeader},
		{follower, leader, false, 0, NoLeader},

		//{statePreCandidate, follower, true, 0, NoLeader},
		//{statePreCandidate, statePreCandidate, true, 0, NoLeader},
		//{statePreCandidate, candidate, true, 1, NoLeader},
		//{statePreCandidate, leader, true, 0, 1},

		{candidate, follower, true, 0, NoLeader},
		//{candidate, statePreCandidate, true, 0, NoLeader},
		{candidate, candidate, true, 1, NoLeader},
		{candidate, leader, true, 0, 1},

		{leader, follower, true, 1, NoLeader},
		//{leader, statePreCandidate, false, 0, NoLeader},
		{leader, candidate, false, 1, NoLeader},
		{leader, leader, true, 0, 1},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				// lni
				// when tt.wallow is false, panic is expected, recover from
				// it and assert tt.wallow to be true.
				if r := recover(); r != nil {
					if tt.wallow {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			sm := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
			sm.state = tt.from

			switch tt.to {
			case follower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case candidate:
				sm.becomeCandidate()
			case leader:
				ne(sm.becomeLeader(), t)
			}

			if sm.term != tt.wterm {
				t.Errorf("%d: term = %d, want %d", i, sm.term, tt.wterm)
			}
			if sm.leaderID != tt.wlead {
				t.Errorf("%d: lead = %d, want %d", i, sm.leaderID, tt.wlead)
			}
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state State

		wstate State
		wterm  uint64
		windex uint64
	}{
		{follower, follower, 3, 0},
		//{statePreCandidate, follower, 3, 0},
		{candidate, follower, 3, 0},
		{leader, follower, 3, 1},
	}

	tmsgTypes := [...]pb.MessageType{pb.RequestVote, pb.Replicate}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		switch tt.state {
		case follower:
			sm.becomeFollower(1, NoLeader)
		case candidate:
			sm.becomeCandidate()
		case leader:
			sm.becomeCandidate()
			ne(sm.becomeLeader(), t)
		}

		for j, msgType := range tmsgTypes {
			ne(sm.Handle(pb.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm}), t)

			if sm.state != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.state, tt.wstate)
			}
			if sm.term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.term, tt.wterm)
			}
			if sm.log.lastIndex() != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, sm.log.lastIndex(), tt.windex)
			}
			if uint64(len(getAllEntries(sm.log))) != tt.windex {
				t.Errorf("#%d.%d len(ents) = %v , want %v", i, j, len(getAllEntries(sm.log)), tt.windex)
			}
			wlead := uint64(2)
			if msgType == pb.RequestVote {
				wlead = NoLeader
			}
			if sm.leaderID != wlead {
				t.Errorf("#%d, sm.lead = %d, want %d", i, sm.leaderID, NoLeader)
			}
		}
	}
}

func TestLeaderStepdownWhenQuorumActive(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())

	sm.checkQuorum = true

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	for i := uint64(0); i < sm.electionTimeout+1; i++ {
		ne(sm.Handle(pb.Message{From: 2, Type: pb.HeartbeatResp, Term: sm.term}), t)
		ne(sm.tick(), t)
	}

	if sm.state != leader {
		t.Errorf("state = %v, want %v", sm.state, leader)
	}
}

func TestLeaderStepdownWhenQuorumLost(t *testing.T) {
	sm := newTestRaft(1, []uint64{1, 2, 3}, 5, 1, NewTestLogDB())

	sm.checkQuorum = true

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	for i := uint64(0); i < sm.electionTimeout+1; i++ {
		ne(sm.tick(), t)
	}

	if sm.state != follower {
		t.Errorf("state = %v, want %v", sm.state, follower)
	}
}

func TestLeaderSupersedingWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	if a.state != leader {
		t.Errorf("state = %s, want %s", a.state, leader)
	}

	if c.state != follower {
		t.Errorf("state = %s, want %s", c.state, follower)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})
	// Peer b rejected c's vote since its electionRTT had not reached to electionTimeout
	if c.state != candidate {
		t.Errorf("state = %s, want %s", c.state, candidate)
	}

	// Letting b's electionRTT reach to electionTimeout
	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	if c.state != leader {
		t.Errorf("state = %s, want %s", c.state, leader)
	}
}

// lni
// check this three tests
func TestLeaderElectionWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	if a.state != leader {
		t.Errorf("state = %s, want %s", a.state, leader)
	}

	if c.state != follower {
		t.Errorf("state = %s, want %s", c.state, follower)
	}

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)
	for i := uint64(0); i < a.electionTimeout; i++ {
		ne(a.tick(), t)
	}
	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	if a.state != follower {
		t.Errorf("state = %s, want %s", a.state, follower)
	}

	if c.state != leader {
		t.Errorf("state = %s, want %s", c.state, leader)
	}
}

func TestFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	testFreeStuckCandidateWithCheckQuorum(t)
}

/*
func TestFreeStuckCandidateWithCheckQuorumAndPreVote(t *testing.T) {
	testFreeStuckCandidateWithCheckQuorum(t, true)
}
*/

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
func testFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true
	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	nt.isolate(1)
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	if b.state != follower {
		t.Errorf("state = %s, want %s", b.state, follower)
	}

	if c.state != candidate {
		t.Errorf("state = %s, want %s", c.state, candidate)
	}

	if c.term != b.term+1 {
		t.Errorf("term = %d, want %d", c.term, b.term+1)
	}

	// Vote again for safety
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})

	if b.state != follower {
		t.Errorf("state = %s, want %s", b.state, follower)
	}

	if c.state != candidate {
		t.Errorf("state = %s, want %s", c.state, candidate)
	}

	if c.term != b.term+2 {
		t.Errorf("term = %d, want %d", c.term, b.term+2)
	}

	nt.recover()
	nt.send(pb.Message{From: 1, To: 3, Type: pb.Heartbeat, Term: a.term})

	// Disrupt the leader so that the stuck peer is freed
	if a.state != follower {
		t.Errorf("state = %s, want %s", a.state, follower)
	}

	if c.term != a.term {
		t.Errorf("term = %d, want %d", c.term, a.term)
	}

	// Vote again, should become leader this time
	nt.send(pb.Message{From: 3, To: 3, Type: pb.Election})
	if c.state != leader {
		t.Errorf("peer 3 state: %s, want %s", c.state, leader)
	}
}

func TestNonPromotableVoterWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1}, 10, 1, NewTestLogDB())

	a.checkQuorum = true
	b.checkQuorum = true

	nt := newNetwork(a, b)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b.deleteRemote(2)

	if !b.selfRemoved() {
		t.Fatalf("promotable = %v, want false", b.selfRemoved())
	}

	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	if a.state != leader {
		t.Errorf("state = %s, want %s", a.state, leader)
	}

	if b.state != follower {
		t.Errorf("state = %s, want %s", b.state, follower)
	}

	if b.leaderID != 1 {
		t.Errorf("lead = %d, want 1", b.leaderID)
	}
}

func TestReadOnlyOptionSafe(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := uint64(0); i < b.electionTimeout; i++ {
		ne(b.tick(), t)
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	if a.state != leader {
		t.Fatalf("state = %s, want %s", a.state, leader)
	}

	tests := []struct {
		sm        *raft
		proposals int
		wri       uint64
		wctx      pb.SystemCtx
	}{
		{a, 10, 11, getTestSystemCtx(10001)},
		{b, 10, 21, getTestSystemCtx(10002)},
		{c, 10, 31, getTestSystemCtx(10003)},
		{a, 10, 41, getTestSystemCtx(10004)},
		{b, 10, 51, getTestSystemCtx(10005)},
		{c, 10, 61, getTestSystemCtx(10006)},
	}

	for i, tt := range tests {
		for j := 0; j < tt.proposals; j++ {
			nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
		}

		nt.send(pb.Message{From: tt.sm.replicaID, To: tt.sm.replicaID, Type: pb.ReadIndex, Hint: tt.wctx.Low, HintHigh: tt.wctx.High})

		r := tt.sm
		if len(r.readyToRead) == 0 {
			t.Errorf("#%d: len(readyToRead) = 0, want non-zero", i)
		}
		rs := r.readyToRead[0]
		if rs.Index != tt.wri {
			t.Errorf("#%d: readIndex = %d, want %d", i, rs.Index, tt.wri)
		}

		if rs.SystemCtx != tt.wctx {
			t.Errorf("#%d: requestCtx = %d, want %d", i, rs.SystemCtx, tt.wctx)
		}
		r.readyToRead = nil
	}
}

func TestLeaderAppResp(t *testing.T) {
	// initial progress: match = 0; next = 3
	tests := []struct {
		index  uint64
		reject bool
		// progress
		wmatch uint64
		wnext  uint64
		// message
		wmsgNum    int
		windex     uint64
		wcommitted uint64
	}{
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit; decrease next and send probing msg, which is just a r.sendAppend()
		{2, false, 2, 4, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		{0, false, 0, 3, 0, 0, 0}, // ignore heartbeat replies
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		sm.log = &entryLog{
			logdb: &TestLogDB{entries: []pb.Entry{{Index: 1, Term: 0}, {Index: 2, Term: 1}}},
			inmem: inMemory{markerIndex: 3},
		}
		sm.becomeCandidate()
		ne(sm.becomeLeader(), t)
		sm.readMessages()
		ne(sm.Handle(pb.Message{From: 2, Type: pb.ReplicateResp, LogIndex: tt.index, Term: sm.term, Reject: tt.reject, Hint: tt.index}), t)

		p := sm.remotes[2]
		if p.match != tt.wmatch {
			t.Errorf("#%d match = %d, want %d", i, p.match, tt.wmatch)
		}
		if p.next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.next, tt.wnext)
		}

		msgs := sm.readMessages()

		if len(msgs) != tt.wmsgNum {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wmsgNum)
		}
		for j, msg := range msgs {
			if msg.LogIndex != tt.windex {
				t.Errorf("#%d.%d index = %d, want %d", i, j, msg.LogIndex, tt.windex)
			}
			if msg.Commit != tt.wcommitted {
				t.Errorf("#%d.%d commit = %d, want %d", i, j, msg.Commit, tt.wcommitted)
			}
		}
	}
}

// When the leader receives a heartbeat tick, it should
// send a MsgAppend with m.Index = 0, m.LogTerm=0 and empty entries.
func TestBcastBeat(t *testing.T) {
	offset := uint64(1000)
	// make a state machine with log.offset = 1000
	s := pb.Snapshot{
		Index:      offset,
		Term:       1,
		Membership: getTestMembership([]uint64{1, 2, 3}),
	}
	storage := NewTestLogDB()
	if err := storage.ApplySnapshot(s); err != nil {
		t.Fatalf("%v", err)
	}
	sm := newTestRaft(1, nil, 10, 1, storage)
	sm.term = 1

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)
	for i := 0; i < 10; i++ {
		ne(sm.appendEntries([]pb.Entry{{Index: uint64(i) + 1}}), t)
	}
	// slow follower
	sm.remotes[2].match, sm.remotes[2].next = 5, 6
	// normal follower
	sm.remotes[3].match, sm.remotes[3].next = sm.log.lastIndex(), sm.log.lastIndex()+1

	ne(sm.Handle(pb.Message{Type: pb.LeaderHeartbeat}), t)
	msgs := sm.readMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %v, want 2", len(msgs))
	}
	wantCommitMap := map[uint64]uint64{
		2: min(sm.log.committed, sm.remotes[2].match),
		3: min(sm.log.committed, sm.remotes[3].match),
	}
	for i, m := range msgs {
		if m.Type != pb.Heartbeat {
			t.Fatalf("#%d: type = %v, want = %v", i, m.Type, pb.Heartbeat)
		}
		if m.LogIndex != 0 {
			t.Fatalf("#%d: prevIndex = %d, want %d", i, m.LogIndex, 0)
		}
		if m.LogTerm != 0 {
			t.Fatalf("#%d: prevTerm = %d, want %d", i, m.LogTerm, 0)
		}
		if wantCommitMap[m.To] == 0 {
			t.Fatalf("#%d: unexpected to %d", i, m.To)
		} else {
			if m.Commit != wantCommitMap[m.To] {
				t.Fatalf("#%d: commit = %d, want %d", i, m.Commit, wantCommitMap[m.To])
			}
			delete(wantCommitMap, m.To)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("#%d: len(entries) = %d, want 0", i, len(m.Entries))
		}
	}
}

// tests the output of the state machine when receiving MsgLeaderHeartbeat
func TestRecvMsgLeaderHeartbeat(t *testing.T) {
	tests := []struct {
		state State
		wMsg  int
	}{
		// leader should send MsgHeartbeat
		{leader, 2},
		// candidate and follower should ignore MsgLeaderHeartbeat
		{candidate, 0},
		{follower, 0},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
		sm.log = &entryLog{logdb: &TestLogDB{entries: []pb.Entry{{Index: 1, Term: 0}, {Index: 2, Term: 1}}}}
		sm.term = 1
		sm.state = tt.state
		ne(sm.Handle(pb.Message{From: 1, To: 1, Type: pb.LeaderHeartbeat}), t)

		msgs := sm.readMessages()
		if len(msgs) != tt.wMsg {
			t.Errorf("%d: len(msgs) = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.Type != pb.Heartbeat {
				t.Errorf("%d: msg.type = %v, want %v", i, m.Type, pb.Heartbeat)
			}
		}
	}
}

func TestLeaderIncreaseNext(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	tests := []struct {
		// progress
		state remoteStateType
		next  uint64

		wnext uint64
	}{
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{remoteReplicate, 2, uint64(len(previousEnts) + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{remoteRetry, 2, 2},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
		sm.log.append(previousEnts)
		sm.becomeCandidate()
		ne(sm.becomeLeader(), t)
		sm.remotes[2].state = tt.state
		sm.remotes[2].next = tt.next
		ne(sm.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)

		p := sm.remotes[2]
		if p.next != tt.wnext {
			t.Errorf("#%d next = %d, want %d", i, p.next, tt.wnext)
		}
	}
}

func TestSendAppendForRemoteRetry(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.readMessages()
	r.remotes[2].becomeRetry()

	// each round is a heartbeat
	for i := 0; i < 3; i++ {
		if i == 0 {
			// we expect that raft will only send out one msgAPP on the first
			// loop. After that, the follower is paused until a heartbeat response is
			// received.
			ne(r.appendEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
			r.sendReplicateMessage(2)
			msg := r.readMessages()
			if len(msg) != 1 {
				t.Errorf("len(msg) = %d, want %d", len(msg), 1)
			}
			if msg[0].LogIndex != 0 {
				t.Errorf("index = %d, want %d", msg[0].LogIndex, 0)
			}
		}

		if r.remotes[2].state != remoteWait {
			t.Errorf("paused = %s, want remoteWait", r.remotes[2].state)
		}
		for j := 0; j < 10; j++ {
			ne(r.appendEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
			r.sendReplicateMessage(2)
			if l := len(r.readMessages()); l != 0 {
				t.Errorf("len(msg) = %d, want %d", l, 0)
			}
		}

		// do a heartbeat
		for j := uint64(0); j < r.heartbeatTimeout; j++ {
			ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.LeaderHeartbeat}), t)
		}
		if r.remotes[2].state != remoteWait {
			t.Errorf("paused = %v, want remoteWait", r.remotes[2].state)
		}

		// consume the heartbeat
		msg := r.readMessages()
		if len(msg) != 1 {
			t.Errorf("len(msg) = %d, want %d", len(msg), 1)
		}
		if msg[0].Type != pb.Heartbeat {
			t.Errorf("type = %v, want %v", msg[0].Type, pb.Heartbeat)
		}
	}

	// a heartbeat response will allow another message to be sent
	ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.HeartbeatResp}), t)
	msg := r.readMessages()
	if len(msg) != 1 {
		t.Errorf("len(msg) = %d, want %d", len(msg), 1)
	}
	if msg[0].LogIndex != 0 {
		t.Errorf("index = %d, want %d", msg[0].LogIndex, 0)
	}
	if r.remotes[2].state != remoteWait {
		t.Errorf("paused = %s, want remoteWait", r.remotes[2].state)
	}
}

// optimisitically send entries out
func TestSendAppendForRemoteReplicate(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.readMessages()
	r.remotes[2].becomeReplicate()

	for i := 0; i < 10; i++ {
		ne(r.appendEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
		r.sendReplicateMessage(2)
		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Errorf("len(msg) = %d, want %d", len(msgs), 1)
		}
	}
}

// paused, no msgapp when entries are locally appended
func TestSendAppendForRemoteSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.readMessages()
	r.remotes[2].becomeSnapshot(10)

	for i := 0; i < 10; i++ {
		ne(r.appendEntries([]pb.Entry{{Cmd: []byte("somedata")}}), t)
		r.sendReplicateMessage(2)
		msgs := r.readMessages()
		if len(msgs) != 0 {
			t.Errorf("len(msg) = %d, want %d", len(msgs), 0)
		}
	}
}

// MsgUnreachable puts remote peer into probe state.
func TestRecvMsgUnreachable(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	s := NewTestLogDB()
	if err := s.Append(previousEnts); err != nil {
		t.Fatalf("%v", err)
	}
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, s)
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	r.readMessages()
	// set node 2 to state replicate
	r.remotes[2].match = 3
	r.remotes[2].becomeReplicate()
	r.remotes[2].tryUpdate(5)

	ne(r.Handle(pb.Message{From: 2, To: 1, Type: pb.Unreachable}), t)

	if r.remotes[2].state != remoteRetry {
		t.Errorf("state = %s, want %s", r.remotes[2].state, remoteRetry)
	}
	if wnext := r.remotes[2].match + 1; r.remotes[2].next != wnext {
		t.Errorf("next = %d, want %d", r.remotes[2].next, wnext)
	}
}

func getNodesFromMembership(ss pb.Membership) []uint64 {
	v := make([]uint64, 0)
	for nid := range ss.Addresses {
		v = append(v, nid)
	}
	return v
}

func sliceEqual(s1 []uint64, s2 []uint64) bool {
	if len(s1) != len(s2) {
		return false
	}
	sort.Slice(s1, func(i, j int) bool { return s1[i] < s1[j] })
	sort.Slice(s2, func(i, j int) bool { return s2[i] < s2[j] })
	for idx, v := range s1 {
		if v != s2[idx] {
			return false
		}
	}

	return true
}

// nodes info is in the snapshot.
func TestRestore(t *testing.T) {
	s := pb.Snapshot{
		Index:      11, // magic number
		Term:       11, // magic number
		Membership: getTestMembership([]uint64{1, 2, 3}),
	}

	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	ok, err := sm.restore(s)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !ok {
		t.Fatal("restore fail, want succeed")
	}

	if sm.log.lastIndex() != s.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.log.lastIndex(), s.Index)
	}
	if mustTerm(sm.log.term(s.Index)) != s.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.log.term(s.Index)), s.Term)
	}

	if sliceEqual(sm.nodesSorted(), getNodesFromMembership(s.Membership)) {
		t.Errorf("snapshot remotes info restored too earlier: %v", sm.nodesSorted())
	}

	sm.restoreRemotes(s)
	sg := sm.nodesSorted()
	if !sliceEqual(sg, getNodesFromMembership(s.Membership)) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, getNodesFromMembership(s.Membership))
	}
	ok, err = sm.restore(s)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Fatal("restore succeed, want fail")
	}
}

func TestRestoreIgnoreSnapshot(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	commit := uint64(1)
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.log.append(previousEnts)
	sm.log.commitTo(commit)

	s := pb.Snapshot{
		Index:      commit,
		Term:       1,
		Membership: getTestMembership([]uint64{1, 2}),
	}

	// ignore snapshot
	ok, err := sm.restore(s)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("restore = %t, want %t", ok, false)
	}
	if sm.log.committed != commit {
		t.Errorf("commit = %d, want %d", sm.log.committed, commit)
	}

	// lni
	// s.Index, s.term match the log. restore is not required.
	// but it will fast forward the committed value.
	// ignore snapshot and fast forward commit
	s.Index = commit + 1
	ok, err = sm.restore(s)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if ok {
		t.Errorf("restore = %t, want %t", ok, false)
	}
	if sm.log.committed != commit+1 {
		t.Errorf("commit = %d, want %d", sm.log.committed, commit+1)
	}
}

func TestProvideSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Index:      11, // magic number
		Term:       11, // magic number
		Membership: getTestMembership([]uint64{1, 2}),
	}
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	if _, err := sm.restore(s); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(s)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	// force set the next of node 2, so that node 2 needs a snapshot
	sm.remotes[2].next = sm.log.firstIndex()
	ne(sm.Handle(pb.Message{From: 2, To: 1, Type: pb.ReplicateResp, LogIndex: sm.remotes[2].next - 1, Reject: true}), t)

	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != pb.InstallSnapshot {
		t.Errorf("m.Type = %v, want %v", m.Type, pb.InstallSnapshot)
	}
}

func TestIgnoreProvidingSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Index:      11, // magic number
		Term:       11, // magic number
		Membership: getTestMembership([]uint64{1, 2}),
	}
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	if _, err := sm.restore(s); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(s)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm.remotes[2].next = sm.log.firstIndex() - 1
	sm.remotes[2].active = false

	ne(sm.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)

	msgs := sm.readMessages()
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := pb.Snapshot{
		Index:      11, // magic number
		Term:       11, // magic number
		Membership: getTestMembership([]uint64{1, 2}),
	}
	m := pb.Message{Type: pb.InstallSnapshot, From: 1, Term: 2, Snapshot: s}

	sm := newTestRaft(2, []uint64{1, 2}, 10, 1, NewTestLogDB())
	ne(sm.Handle(m), t)

	if sm.leaderID != uint64(1) {
		t.Errorf("sm.lead = %d, want 1", sm.leaderID)
	}

	// TODO(bdarnell): what should this test?
}

func TestSlowNodeRestore(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	nt.isolate(3)
	for j := 0; j <= 100; j++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	}
	lead := nt.peers[1].(*raft)
	nextEnts(lead, nt.storage[1])
	m := getTestMembership(lead.nodesSorted())
	ss, err := nt.storage[1].(*TestLogDB).getSnapshot(lead.log.processed, &m)
	if err != nil {
		t.Fatalf("failed to get snapshot")
	}
	if err := nt.storage[1].CreateSnapshot(ss); err != nil {
		t.Fatalf("%v", err)
	}
	if err := nt.storage[1].Compact(lead.log.processed); err != nil {
		t.Fatalf("%v", err)
	}
	follower := nt.peers[3].(*raft)
	nt.recover()
	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	for {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.LeaderHeartbeat})
		if lead.remotes[3].active {
			break
		}
	}

	// trigger a snapshot
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	// trigger a commit
	nt.send(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{}}})
	if follower.log.committed != lead.log.committed {
		t.Errorf("follower.committed = %d, want %d", follower.log.committed, lead.log.committed)
	}
}

// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConfigChange to be true.
func TestStepConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	index := r.log.lastIndex()
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Type: pb.ConfigChangeEntry}}}), t)
	if g := r.log.lastIndex(); g != index+1 {
		t.Errorf("index = %d, want %d", g, index+1)
	}
	if !r.pendingConfigChange {
		t.Errorf("pendingConfigChange = %v, want true", r.pendingConfigChange)
	}
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
func TestStepIgnoreConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.becomeCandidate()
	ne(r.becomeLeader(), t)
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Type: pb.ConfigChangeEntry}}}), t)
	index := r.log.lastIndex()
	pendingConfigChange := r.pendingConfigChange
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Type: pb.ConfigChangeEntry}}}), t)
	wents := []pb.Entry{{Type: pb.ApplicationEntry, Term: 1, Index: 3, Cmd: nil}}
	ents, err := r.log.entries(index+1, noLimit)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !reflect.DeepEqual(ents, wents) {
		t.Errorf("ents = %+v, want %+v", ents, wents)
	}
	if r.pendingConfigChange != pendingConfigChange {
		t.Errorf("pendingConfigChange = %v, want %v", r.pendingConfigChange, pendingConfigChange)
	}
}

// TestRecoverPendingConfig tests that new leader recovers its pendingConfigChange flag
// based on uncommitted entries.
func TestRecoverPendingConfig(t *testing.T) {
	tests := []struct {
		entType  pb.EntryType
		wpending bool
	}{
		{pb.ApplicationEntry, false},
		{pb.ConfigChangeEntry, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
		ne(r.appendEntries([]pb.Entry{{Type: tt.entType}}), t)
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
		if r.pendingConfigChange != tt.wpending {
			t.Errorf("#%d: pendingConfigChange = %v, want %v", i, r.pendingConfigChange, tt.wpending)
		}
	}
}

// TestRecoverDoublePendingConfig tests that new leader will panic if
// there exist two uncommitted config entries.
func TestRecoverDoublePendingConfig(t *testing.T) {
	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("expect panic, but nothing happens")
			}
		}()
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
		ne(r.appendEntries([]pb.Entry{{Type: pb.ConfigChangeEntry}}), t)
		ne(r.appendEntries([]pb.Entry{{Type: pb.ConfigChangeEntry}}), t)
		r.becomeCandidate()
		ne(r.becomeLeader(), t)
	}()
}

// TestAddNode tests that addNode could update pendingConfigChange and nodes correctly.
func TestAddNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewTestLogDB())
	r.pendingConfigChange = true
	r.addNode(2)
	if r.pendingConfigChange {
		t.Errorf("pendingConfigChange = %v, want false", r.pendingConfigChange)
	}
	nodes := r.nodesSorted()
	wnodes := []uint64{1, 2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
}

// TestRemoveNode tests that removeNode could update pendingConfigChange, nodes and
// and removed list correctly.
func TestRemoveNode(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewTestLogDB())
	r.pendingConfigChange = true
	ne(r.removeNode(2), t)
	if r.pendingConfigChange {
		t.Errorf("pendingConfigChange = %v, want false", r.pendingConfigChange)
	}
	w := []uint64{1}
	if g := r.nodesSorted(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	// remove all nodes from shard
	ne(r.removeNode(1), t)
	w = []uint64{}
	if g := r.nodesSorted(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
}

// lni
// promotoable probably means the local node has not been removed?
func TestPromotable(t *testing.T) {
	id := uint64(1)
	tests := []struct {
		peers []uint64
		wp    bool
	}{
		{[]uint64{1}, true},
		{[]uint64{1, 2, 3}, true},
		{[]uint64{}, false},
		{[]uint64{2, 3}, false},
	}
	for i, tt := range tests {
		r := newTestRaft(id, tt.peers, 5, 1, NewTestLogDB())
		if g := !r.selfRemoved(); g != tt.wp {
			t.Errorf("#%d: promotable = %v, want %v", i, g, tt.wp)
		}
	}
}

func TestRaftNodes(t *testing.T) {
	tests := []struct {
		ids  []uint64
		wids []uint64
	}{
		{
			[]uint64{1, 2, 3},
			[]uint64{1, 2, 3},
		},
		{
			[]uint64{3, 2, 1},
			[]uint64{1, 2, 3},
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, tt.ids, 10, 1, NewTestLogDB())
		if !reflect.DeepEqual(r.nodesSorted(), tt.wids) {
			t.Errorf("#%d: nodes = %+v, want %+v", i, r.nodesSorted(), tt.wids)
		}
	}
}

func TestCampaignWhileLeader(t *testing.T) {
	testCampaignWhileLeader(t)
}

func testCampaignWhileLeader(t *testing.T) {
	s := NewTestLogDB()
	peers := []uint64{1}
	cfg := newTestConfig(1, 5, 1)
	r := newRaft(cfg, s)
	r.setTestPeers(peers)
	if r.state != follower {
		t.Errorf("expected new node to be follower but got %s", r.state)
	}
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}), t)
	if r.state != leader {
		t.Errorf("expected single-node election to become leader but got %s", r.state)
	}
	term := r.term
	ne(r.Handle(pb.Message{From: 1, To: 1, Type: pb.Election}), t)
	if r.state != leader {
		t.Errorf("expected to remain leader but got %s", r.state)
	}
	if r.term != term {
		t.Errorf("expected to remain in term %v but got %v", term, r.term)
	}
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
func TestCommitAfterRemoveNode(t *testing.T) {
	// Create a shard with two nodes.
	s := NewTestLogDB()
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, s)
	r.becomeCandidate()
	ne(r.becomeLeader(), t)

	// Begin to remove the second node.
	cc := pb.ConfigChange{
		Type:      pb.RemoveNode,
		ReplicaID: 2,
	}
	ccData, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	ne(r.Handle(pb.Message{
		Type: pb.Propose,
		Entries: []pb.Entry{
			{Type: pb.ConfigChangeEntry, Cmd: ccData},
		},
	}), t)
	// Stabilize the log and make sure nothing is committed yet.
	if ents := nextEnts(r, s); len(ents) > 0 {
		t.Fatalf("unexpected committed entries: %v", ents)
	}
	ccIndex := r.log.lastIndex()

	// While the config change is pending, make another proposal.
	ne(r.Handle(pb.Message{
		Type: pb.Propose,
		Entries: []pb.Entry{
			{Type: pb.ApplicationEntry, Cmd: []byte("hello")},
		},
	}), t)

	// Node 2 acknowledges the config change, committing it.
	ne(r.Handle(pb.Message{
		Type:     pb.ReplicateResp,
		From:     2,
		LogIndex: ccIndex,
	}), t)
	ents := nextEnts(r, s)
	if len(ents) != 2 {
		t.Fatalf("expected two committed entries, got %v", ents)
	}
	if ents[0].Type != pb.ApplicationEntry || ents[0].Cmd != nil {
		t.Fatalf("expected ents[0] to be empty, but got %v", ents[0])
	}
	if ents[1].Type != pb.ConfigChangeEntry {
		t.Fatalf("expected ents[1] to be EntryConfChange, got %v", ents[1])
	}

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	ne(r.removeNode(2), t)
	ents = nextEnts(r, s)
	if len(ents) != 1 || ents[0].Type != pb.ApplicationEntry ||
		string(ents[0].Cmd) != "hello" {
		t.Fatalf("expected one committed ApplicationEntry, got %v", ents)
	}
}

var (
	testingSnap = pb.Snapshot{
		Index:      11, // magic number
		Term:       11, // magic number
		Membership: getTestMembership([]uint64{1, 2}),
	}
)

func TestSendingSnapshotSetPendingSnapshot(t *testing.T) {
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	if _, err := sm.restore(testingSnap); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(testingSnap)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	// force set the next of node 1, so that
	// node 1 needs a snapshot
	sm.remotes[2].next = sm.log.firstIndex()

	ne(sm.Handle(pb.Message{From: 2, To: 1, Type: pb.ReplicateResp, LogIndex: sm.remotes[2].next - 1, Reject: true}), t)
	if sm.remotes[2].snapshotIndex != 11 {
		t.Fatalf("PendingSnapshot = %d, want 11", sm.remotes[2].snapshotIndex)
	}
}

func TestPendingSnapshotPauseReplication(t *testing.T) {
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	if _, err := sm.restore(testingSnap); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(testingSnap)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	sm.remotes[2].becomeSnapshot(11)

	ne(sm.Handle(pb.Message{From: 1, To: 1, Type: pb.Propose, Entries: []pb.Entry{{Cmd: []byte("somedata")}}}), t)
	msgs := sm.readMessages()
	if len(msgs) != 0 {
		t.Fatalf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestSnapshotFailure(t *testing.T) {
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	if _, err := sm.restore(testingSnap); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(testingSnap)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	sm.remotes[2].next = 1
	sm.remotes[2].becomeSnapshot(11)

	ne(sm.Handle(pb.Message{From: 2, To: 1, Type: pb.SnapshotStatus, Reject: true}), t)
	if sm.remotes[2].snapshotIndex != 0 {
		t.Fatalf("PendingSnapshot = %d, want 0", sm.remotes[2].snapshotIndex)
	}
	if sm.remotes[2].next != 1 {
		t.Fatalf("Next = %d, want 1", sm.remotes[2].next)
	}
	if sm.remotes[2].state != remoteWait {
		t.Errorf("Paused = %s, want remoteWait", sm.remotes[2].state)
	}
}

func TestSnapshotSucceed(t *testing.T) {
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	if _, err := sm.restore(testingSnap); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(testingSnap)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	sm.remotes[2].next = 1
	sm.remotes[2].becomeSnapshot(11)

	ne(sm.Handle(pb.Message{From: 2, To: 1, Type: pb.SnapshotStatus, Reject: false}), t)
	if sm.remotes[2].snapshotIndex != 0 {
		t.Fatalf("PendingSnapshot = %d, want 0", sm.remotes[2].snapshotIndex)
	}
	if sm.remotes[2].next != 12 {
		t.Fatalf("Next = %d, want 12", sm.remotes[2].next)
	}
	if sm.remotes[2].state != remoteWait {
		t.Errorf("Paused = %s, want remoteWait", sm.remotes[2].state)
	}
}

func TestSnapshotAbort(t *testing.T) {
	storage := NewTestLogDB()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	if _, err := sm.restore(testingSnap); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	sm.restoreRemotes(testingSnap)

	sm.becomeCandidate()
	ne(sm.becomeLeader(), t)

	sm.remotes[2].next = 1
	sm.remotes[2].becomeSnapshot(11)

	// A successful msgAppResp that has a higher/equal index than the
	// pending snapshot should abort the pending snapshot.
	ne(sm.Handle(pb.Message{From: 2, To: 1, Type: pb.ReplicateResp, LogIndex: 11}), t)
	if sm.remotes[2].snapshotIndex != 0 {
		t.Fatalf("PendingSnapshot = %d, want 0", sm.remotes[2].snapshotIndex)
	}
	if sm.remotes[2].next != 12 {
		t.Fatalf("Next = %d, want 12", sm.remotes[2].next)
	}
}

func entsWithConfig(configFunc func(config.Config), terms ...uint64) *raft {
	storage := NewTestLogDB()
	for i, term := range terms {
		if err := storage.Append([]pb.Entry{{Index: uint64(i + 1), Term: term}}); err != nil {
			panic(err)
		}
	}
	cfg := newTestConfig(1, 5, 1)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg, storage)
	sm.reset(terms[len(terms)-1], true)
	return sm
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
func votedWithConfig(configFunc func(config.Config), vote, term uint64) *raft {
	storage := NewTestLogDB()
	storage.SetState(pb.State{Vote: vote, Term: term})
	cfg := newTestConfig(1, 5, 1)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg, storage)
	sm.reset(term, true)
	return sm
}

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]ILogDB
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(config.Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]ILogDB, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = NewTestLogDB()
			cfg := newTestConfig(id, 10, 1)
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg, nstorage[id])
			sm.setTestPeers(peerAddrs)
			sm.hasNotAppliedConfigChange = sm.testOnlyHasConfigChangeToApply
			npeers[id] = sm
		case *raft:
			nonVotings := make(map[uint64]bool)
			witnesses := make(map[uint64]bool)
			for i := range v.nonVotings {
				nonVotings[i] = true
			}
			for i := range v.witnesses {
				witnesses[i] = true
			}

			v.replicaID = id
			v.remotes = make(map[uint64]*remote)
			v.nonVotings = make(map[uint64]*remote)
			v.witnesses = make(map[uint64]*remote)
			for i := 0; i < size; i++ {
				if _, ok := nonVotings[peerAddrs[i]]; ok {
					v.nonVotings[peerAddrs[i]] = &remote{}
				} else if _, ok := witnesses[peerAddrs[i]]; ok {
					v.witnesses[peerAddrs[i]] = &remote{}
				} else {
					v.remotes[peerAddrs[i]] = &remote{}
				}
			}
			v.reset(v.term, true)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[pb.MessageType]bool),
	}
}

func (nw *network) send(msgs ...pb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		if err := p.Handle(m); err != nil {
			panic(err)
		}
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 1)
	nw.drop(other, one, 1)
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0)
			nw.drop(nid, id, 1.0)
		}
	}
}

func (nw *network) ignore(t pb.MessageType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[pb.MessageType]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	mm := []pb.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case pb.Election:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

func (blackHole) Handle(pb.Message) error    { return nil }
func (blackHole) readMessages() []pb.Message { return nil }

var nopStepper = &blackHole{}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
func setRandomizedElectionTimeout(r *raft, v uint64) {
	r.randomizedElectionTimeout = v
}

func newTestConfig(id uint64, election, heartbeat int) config.Config {
	return newRateLimitedTestConfig(id, election, heartbeat, 0)
}

func newRateLimitedTestConfig(id uint64, election, heartbeat int, maxLogSize int) config.Config {
	return config.Config{
		ReplicaID:       id,
		ElectionRTT:     uint64(election),
		HeartbeatRTT:    uint64(heartbeat),
		MaxInMemLogSize: uint64(maxLogSize),
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, logdb ILogDB) *raft {
	r := newRaft(newTestConfig(id, election, heartbeat), logdb)
	if len(r.remotes) == 0 {
		for _, p := range peers {
			r.remotes[p] = &remote{next: 1}
		}
	}
	r.hasNotAppliedConfigChange = r.testOnlyHasConfigChangeToApply
	return r
}

func newRateLimitedTestRaft(id uint64, peers []uint64, election, heartbeat int, logdb ILogDB) *raft {
	cfg := config.Config{
		ReplicaID:       id,
		ElectionRTT:     uint64(election),
		HeartbeatRTT:    uint64(heartbeat),
		MaxInMemLogSize: testRateLimit,
	}
	r := newRaft(cfg, logdb)
	if len(r.remotes) == 0 {
		for _, p := range peers {
			r.remotes[p] = &remote{next: 1}
		}
	}
	r.hasNotAppliedConfigChange = r.testOnlyHasConfigChangeToApply
	return r
}

func newTestNonVoting(id uint64, peers []uint64, nonVotings []uint64, election, heartbeat int, logdb ILogDB) *raft {
	found := false
	for _, p := range nonVotings {
		if p == id {
			found = true
		}
	}
	if !found {
		panic("nonVoting node id not included in the nonVotings list")
	}
	cfg := newTestConfig(id, election, heartbeat)
	cfg.IsNonVoting = true
	r := newRaft(cfg, logdb)
	if len(r.remotes) == 0 {
		for _, p := range peers {
			r.remotes[p] = &remote{next: 1}
		}
	}
	if len(r.nonVotings) == 0 {
		for _, p := range nonVotings {
			r.nonVotings[p] = &remote{next: 1}
		}
	}
	r.hasNotAppliedConfigChange = r.testOnlyHasConfigChangeToApply
	return r
}

func newTestWitness(id uint64, peers []uint64, witnesses []uint64, election, heartbeat int, logdb ILogDB) *raft {
	cfg := newTestConfig(id, election, heartbeat)
	cfg.IsWitness = true
	r := newRaft(cfg, logdb)
	if len(r.remotes) == 0 {
		for _, p := range peers {
			r.remotes[p] = &remote{next: 1}
		}
	}
	if len(r.witnesses) == 0 {
		for _, p := range witnesses {
			r.witnesses[p] = &remote{next: 1}
		}
	}
	r.hasNotAppliedConfigChange = r.testOnlyHasConfigChangeToApply
	return r
}
