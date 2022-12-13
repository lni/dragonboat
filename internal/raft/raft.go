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

/*
Package raft is a distributed consensus package that implements the Raft
protocol.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package raft

import (
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/logger"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("raft")
)

const (
	// NoLeader is the flag used to indcate that there is no leader or the leader
	// is unknown.
	NoLeader uint64 = 0
	// NoNode is the flag used to indicate that the node id field is not set.
	NoNode          uint64 = 0
	noLimit         uint64 = math.MaxUint64
	numMessageTypes uint64 = 29
)

var (
	emptyState     = pb.State{}
	maxEntrySize   = settings.Soft.MaxEntrySize
	inMemGcTimeout = settings.Soft.InMemGCTimeout
)

// State is the state of a raft node defined in the raft thesis.
type State uint64

const (
	follower State = iota
	candidate
	preVoteCandidate
	leader
	nonVoting
	witness
	numStates
)

var stateNames = [...]string{
	"Follower",
	"Candidate",
	"PreVoteCandidate",
	"Leader",
	"NonVoting",
	"Witness",
}

func (st State) String() string {
	return stateNames[uint64(st)]
}

// ReplicaID returns a human friendly form of ReplicaID for logging purposes.
func ReplicaID(replicaID uint64) string {
	return logutil.ReplicaID(replicaID)
}

// ShardID returns a human friendly form of ShardID for logging purposes.
func ShardID(shardID uint64) string {
	return logutil.ShardID(shardID)
}

type handlerFunc func(pb.Message) error
type stepFunc func(*raft, pb.Message) error

// Status is the struct that captures the status of a raft node.
type Status struct {
	ReplicaID uint64
	ShardID   uint64
	Applied   uint64
	LeaderID  uint64
	NodeState State
	pb.State
}

// IsLeader returns a boolean value indicating whether the node is leader.
func (s *Status) IsLeader() bool {
	return s.NodeState == leader
}

// IsFollower returns a boolean value indicating whether the node is a follower.
func (s *Status) IsFollower() bool {
	return s.NodeState == follower
}

// getLocalStatus gets a copy of the current raft status.
func getLocalStatus(r *raft) Status {
	return Status{
		ReplicaID: r.replicaID,
		ShardID:   r.shardID,
		NodeState: r.state,
		Applied:   r.log.processed,
		LeaderID:  r.leaderID,
		State:     r.raftState(),
	}
}

//
// Struct raft implements the raft protocol published in Diego Ongarno's PhD
// thesis. Almost all features covered in Diego Ongarno's thesis have been
// implemented, including -
//  * leader election with pre-vote
//  * log replication
//  * flow control
//  * membership configuration change
//  * snapshotting and streaming
//  * log compaction
//  * ReadIndex protocol for read-only queries
//  * leadership transfer
//  * non-voting members
//  * witness members
//  * idempotent updates
//  * quorum check
//  * batching
//  * pipelining
//  * witness
//
// Features currently being worked on -
//  * pre-vote
//

//
// This implementation made references to etcd raft's design in the following
// aspects:
//  * it models the raft protocol state as a state machine
//  * restricting to at most one pending leadership change request at a time
//  * replication flow control
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
//

//
// When compared with etcd raft, this implementation is quite different,
// including in areas that we made reference to etcd raft -
// * brand new implementation
// * better bootstrapping procedure
// * log entries are partitioned based on whether they are required in
//   immediate future rather than whether they have been persisted to disk
// * zero disk read when replicating raft log entries
// * committed entries are applied in a fully asynchronous manner
// * snapshots are applied in a fully asynchronous manner
// * replication messages can be asynchronously serialized and sent
// * pagination support when applying committed entries
// * making proposals are fully batched
// * ReadIndex protocol implementation are fully batched
// * unsafe read-only queries that rely on local clock is not supported
// * non-voting members are implemented as a special raft state
// * non-voting members can initiate both new proposal and ReadIndex requests
// * simplified flow control
//

var dn = logutil.DescribeNode

type raft struct {
	handlers                  [numStates][numMessageTypes]handlerFunc
	events                    server.IRaftEventListener
	hasNotAppliedConfigChange func() bool
	votes                     map[uint64]bool
	handle                    stepFunc
	log                       *entryLog
	rl                        *server.InMemRateLimiter
	remotes                   map[uint64]*remote
	nonVotings                map[uint64]*remote
	witnesses                 map[uint64]*remote
	logQueryResult            *pb.LogQueryResult
	leaderUpdate              *pb.LeaderUpdate
	readIndex                 *readIndex
	matched                   []uint64
	msgs                      []pb.Message
	droppedReadIndexes        []pb.SystemCtx
	droppedEntries            []pb.Entry
	readyToRead               []pb.ReadyToRead
	prevLeader                server.LeaderInfo
	state                     State
	leaderTransferTarget      uint64
	leaderID                  uint64
	shardID                   uint64
	replicaID                 uint64
	term                      uint64
	applied                   uint64
	vote                      uint64
	tickCount                 uint64
	electionTick              uint64
	heartbeatTick             uint64
	heartbeatTimeout          uint64
	electionTimeout           uint64
	randomizedElectionTimeout uint64
	snapshotting              bool
	checkQuorum               bool
	quiesce                   bool
	isLeaderTransferTarget    bool
	pendingConfigChange       bool
	preVote                   bool
}

func newRaft(c config.Config, logdb ILogDB) *raft {
	if err := c.Validate(); err != nil {
		panic(err)
	}
	if logdb == nil {
		panic("logdb is nil")
	}
	rl := server.NewInMemRateLimiter(c.MaxInMemLogSize)
	r := &raft{
		shardID:          c.ShardID,
		replicaID:        c.ReplicaID,
		leaderID:         NoLeader,
		msgs:             make([]pb.Message, 0),
		droppedEntries:   make([]pb.Entry, 0),
		log:              newEntryLog(logdb, rl),
		remotes:          make(map[uint64]*remote),
		nonVotings:       make(map[uint64]*remote),
		witnesses:        make(map[uint64]*remote),
		electionTimeout:  c.ElectionRTT,
		heartbeatTimeout: c.HeartbeatRTT,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		readIndex:        newReadIndex(),
		rl:               rl,
	}
	plog.Infof("%s raft log rate limit enabled: %t, %d",
		dn(r.shardID, r.replicaID), r.rl.Enabled(), c.MaxInMemLogSize)
	st, members := logdb.NodeState()
	for p := range members.Addresses {
		r.remotes[p] = &remote{next: 1}
	}
	for p := range members.NonVotings {
		r.nonVotings[p] = &remote{next: 1}
	}
	for p := range members.Witnesses {
		r.witnesses[p] = &remote{next: 1}
	}
	r.resetMatchValueArray()
	if !pb.IsEmptyState(st) {
		r.loadState(st)
	}
	// Set node initial state.
	if c.IsNonVoting {
		r.state = nonVoting
		r.becomeNonVoting(r.term, NoLeader)
	} else if c.IsWitness {
		r.state = witness
		r.becomeWitness(r.term, NoLeader)
	} else {
		// see first paragraph section 5.2 of the raft paper
		r.becomeFollower(r.term, NoLeader)
	}
	r.initializeHandlerMap()
	r.checkHandlerMap()
	r.handle = defaultHandle
	return r
}

func (r *raft) setTestPeers(peers []uint64) {
	if len(r.remotes) == 0 {
		for _, p := range peers {
			r.remotes[p] = &remote{next: 1}
		}
	}
}

func (r *raft) setApplied(applied uint64) {
	r.applied = applied
}

func (r *raft) getApplied() uint64 {
	return r.applied
}

func (r *raft) resetMatchValueArray() {
	r.matched = make([]uint64, r.numVotingMembers())
}

func (r *raft) describe() string {
	li := r.log.lastIndex()
	t, err := r.log.term(li)
	if err != nil && !errors.Is(err, ErrCompacted) {
		plog.Panicf("%s failed to get term, %v", dn(r.shardID, r.replicaID), err)
	}
	// first, last, term, committed, applied
	fmtstr := "[f:%d,l:%d,t:%d,c:%d,a:%d] %s t%d"
	return fmt.Sprintf(fmtstr,
		r.log.firstIndex(), r.log.lastIndex(), t, r.log.committed,
		r.log.processed, dn(r.shardID, r.replicaID), r.term)
}

func (r *raft) isCandidate() bool {
	return r.state == candidate
}

func (r *raft) isLeader() bool {
	return r.state == leader
}

func (r *raft) isNonVoting() bool {
	return r.state == nonVoting
}

func (r *raft) isWitness() bool {
	return r.state == witness
}

func (r *raft) mustBeLeader() {
	if !r.isLeader() {
		plog.Panicf("%s is not leader", r.describe())
	}
}

func (r *raft) setLeaderID(leaderID uint64) {
	r.leaderID = leaderID
	r.leaderUpdate = &pb.LeaderUpdate{
		LeaderID: leaderID,
		Term:     r.term,
	}
	if r.events != nil {
		if (r.term == 0 && leaderID == NoLeader) ||
			leaderID != r.prevLeader.LeaderID || r.term != r.prevLeader.Term {
			info := server.LeaderInfo{
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				LeaderID:  leaderID,
				Term:      r.term,
			}
			r.prevLeader = info
			r.events.LeaderUpdated(info)
		}
	}
}

func (r *raft) leaderTransfering() bool {
	return r.leaderTransferTarget != NoNode && r.isLeader()
}

func (r *raft) abortLeaderTransfer() {
	r.leaderTransferTarget = NoNode
}

func (r *raft) numVotingMembers() int {
	return len(r.remotes) + len(r.witnesses)
}

func (r *raft) quorum() int {
	return r.numVotingMembers()/2 + 1
}

func (r *raft) isSingleNodeQuorum() bool {
	return r.quorum() == 1
}

func (r *raft) leaderHasQuorum() bool {
	c := 0

	for nid, member := range r.votingMembers() {
		if nid == r.replicaID || member.isActive() {
			c++
			member.setNotActive()
		}
	}
	return c >= r.quorum()
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, r.numVotingMembers()+len(r.nonVotings))
	for id := range r.remotes {
		nodes = append(nodes, id)
	}
	for id := range r.nonVotings {
		nodes = append(nodes, id)
	}
	for id := range r.witnesses {
		nodes = append(nodes, id)
	}
	return nodes
}

func (r *raft) nodesSorted() []uint64 {
	nodes := r.nodes()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

func (r *raft) votingMembers() map[uint64]*remote {
	nodes := make(map[uint64]*remote, r.numVotingMembers())
	for id, rm := range r.remotes {
		nodes[id] = rm
	}
	for id, wt := range r.witnesses {
		nodes[id] = wt
	}
	return nodes
}

func (r *raft) raftState() pb.State {
	return pb.State{
		Term:   r.term,
		Vote:   r.vote,
		Commit: r.log.committed,
	}
}

func (r *raft) loadState(st pb.State) {
	if st.Commit < r.log.committed || st.Commit > r.log.lastIndex() {
		plog.Panicf("%s got out of range state, st.commit %d, range[%d,%d]",
			r.describe(), st.Commit, r.log.committed, r.log.lastIndex())
	}
	r.log.committed = st.Commit
	r.term = st.Term
	r.vote = st.Vote
}

func (r *raft) restore(ss pb.Snapshot) (bool, error) {
	if ss.Index <= r.log.committed {
		plog.Warningf("%s, restore aborted, ss.Index <= committed", r.describe())
		return false, nil
	}
	if !r.isNonVoting() {
		for nid := range ss.Membership.NonVotings {
			if nid == r.replicaID {
				plog.Panicf("%s converting to nonVoting, index %d, committed %d, %+v",
					r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}
	if !r.isWitness() {
		for nid := range ss.Membership.Witnesses {
			if nid == r.replicaID {
				plog.Panicf("%s converting to witness, index %d, committed %d, %+v",
					r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}
	// p52 of the raft thesis
	match, err := r.log.matchTerm(ss.Index, ss.Term)
	if err != nil {
		return false, err
	}
	if match {
		// a snapshot at index X implies that X has been committed
		r.log.commitTo(ss.Index)
		return false, nil
	}
	plog.Infof("%s starts to restore snapshot index %d term %d",
		r.describe(), ss.Index, ss.Term)
	r.log.restore(ss)
	return true, nil
}

func (r *raft) restoreRemotes(ss pb.Snapshot) {
	r.remotes = make(map[uint64]*remote)
	for id := range ss.Membership.Addresses {
		if id == r.replicaID && r.isNonVoting() {
			r.becomeFollower(r.term, r.leaderID)
		}
		if _, ok := r.witnesses[id]; ok {
			plog.Panicf("Assumed witness could not promote to full member")
		}
		match := uint64(0)
		next := r.log.lastIndex() + 1
		if id == r.replicaID {
			match = next - 1
		}
		r.setRemote(id, match, next)
		plog.Debugf("%s restored remote progress of %s [%s]",
			r.describe(), ReplicaID(id), r.remotes[id])
	}
	if r.selfRemoved() && r.isLeader() {
		r.becomeFollower(r.term, NoLeader)
	}
	r.nonVotings = make(map[uint64]*remote)
	for id := range ss.Membership.NonVotings {
		match := uint64(0)
		next := r.log.lastIndex() + 1
		if id == r.replicaID {
			match = next - 1
		}
		r.setNonVoting(id, match, next)
		plog.Debugf("%s restored nonVoting progress of %s [%s]",
			r.describe(), ReplicaID(id), r.nonVotings[id])
	}
	r.witnesses = make(map[uint64]*remote)
	for id := range ss.Membership.Witnesses {
		match := uint64(0)
		next := r.log.lastIndex() + 1
		if id == r.replicaID {
			match = next - 1
		}
		r.setWitness(id, match, next)
		plog.Debugf("%s restored witness progress of %s [%s]",
			r.describe(), ReplicaID(id), r.witnesses[id])
	}
	r.resetMatchValueArray()
}

//
// tick related functions
//

func (r *raft) timeForElection() bool {
	return r.electionTick >= r.randomizedElectionTimeout
}

func (r *raft) timeForHeartbeat() bool {
	return r.heartbeatTick >= r.heartbeatTimeout
}

// p69 of the raft thesis mentions that check quorum is performed when an
// election timeout elapses
func (r *raft) timeForCheckQuorum() bool {
	return r.electionTick >= r.electionTimeout
}

// p29 of the raft thesis mentions that leadership transfer should abort
// when an election timeout elapses
func (r *raft) timeToAbortLeaderTransfer() bool {
	return r.leaderTransfering() && r.electionTick >= r.electionTimeout
}

func (r *raft) timeForRateLimitCheck() bool {
	return r.tickCount%r.electionTimeout == 0
}

func (r *raft) timeForInMemGC() bool {
	return r.tickCount%inMemGcTimeout == 0
}

func (r *raft) tick() error {
	r.quiesce = false
	r.tickCount++
	// this is to work around the language limitation described in
	// https://github.com/golang/go/issues/9618
	if r.timeForInMemGC() {
		r.log.inmem.tryResize()
	}
	if r.isLeader() {
		return r.leaderTick()
	}
	return r.nonLeaderTick()
}

func (r *raft) nonLeaderTick() error {
	if r.isLeader() {
		panic("noleader tick called on leader node")
	}
	r.electionTick++
	if r.timeForRateLimitCheck() {
		if r.rl.Enabled() {
			r.rl.Tick()
			r.sendRateLimitMessage()
		}
	}
	// section 4.2.1 of the raft thesis
	// non-voting member or witness will not participate in election
	if r.isNonVoting() || r.isWitness() {
		return nil
	}
	// 6th paragraph section 5.2 of the raft paper
	if !r.selfRemoved() && r.timeForElection() {
		r.electionTick = 0
		if err := r.Handle(pb.Message{
			From: r.replicaID,
			Type: pb.Election,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *raft) leaderTick() error {
	r.mustBeLeader()
	r.electionTick++
	if r.timeForRateLimitCheck() {
		if r.rl.Enabled() {
			r.rl.Tick()
		}
	}
	timeToAbortLeaderTransfer := r.timeToAbortLeaderTransfer()
	if r.timeForCheckQuorum() {
		r.electionTick = 0
		if r.checkQuorum {
			if err := r.Handle(pb.Message{
				From: r.replicaID,
				Type: pb.CheckQuorum,
			}); err != nil {
				return err
			}
		}
	}
	if timeToAbortLeaderTransfer {
		r.abortLeaderTransfer()
	}
	r.heartbeatTick++
	if r.timeForHeartbeat() {
		r.heartbeatTick = 0
		if err := r.Handle(pb.Message{
			From: r.replicaID,
			Type: pb.LeaderHeartbeat,
		}); err != nil {
			return err
		}
	}
	return r.checkPendingSnapshotAck()
}

func (r *raft) quiescedTick() {
	if !r.quiesce {
		r.quiesce = true
		r.log.inmem.resize()
	}
	r.electionTick++
}

func (r *raft) setRandomizedElectionTimeout() {
	randTime := random.LockGuardedRand.Uint64() % r.electionTimeout
	r.randomizedElectionTimeout = r.electionTimeout + randTime
}

//
// send and broadcast functions
//

func (r *raft) finalizeMessageTerm(m pb.Message) pb.Message {
	if m.Term == 0 && m.Type == pb.RequestVote {
		plog.Panicf("%s sending RequestVote with 0 term", r.describe())
	}
	if m.Term > 0 &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		plog.Panicf("%s term unexpectedly set for message type %d",
			r.describe(), m.Type)
	}
	if !isRequestMessage(m.Type) &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		m.Term = r.term
	}
	return m
}

func (r *raft) send(m pb.Message) {
	m.From = r.replicaID
	m = r.finalizeMessageTerm(m)
	r.msgs = append(r.msgs, m)
}

func (r *raft) sendRateLimitMessage() {
	if r.isLeader() {
		plog.Panicf("leader node called sendRateLimitMessage")
	}
	if r.leaderID == NoLeader {
		plog.Infof("%s rate limit message skipped, no leader", r.describe())
		return
	}
	if !r.rl.Enabled() {
		return
	}
	mv := uint64(0)
	if r.rl.RateLimited() {
		inmemSz := r.rl.Get()
		notCommitedSz := getEntrySliceSize(r.log.getUncommittedEntries())
		mv = max(inmemSz-notCommitedSz, 0)
	}
	r.send(pb.Message{
		Type: pb.RateLimit,
		To:   r.leaderID,
		Hint: mv,
	})
}

func (r *raft) makeInstallSnapshotMessage(to uint64, m *pb.Message) uint64 {
	m.To = to
	m.Type = pb.InstallSnapshot
	snapshot := r.log.snapshot()
	if pb.IsEmptySnapshot(snapshot) {
		plog.Panicf("%s got an empty snapshot", r.describe())
	}
	// For witness, snapshot message will be marked as dummy snapshot.
	if _, ok := r.witnesses[to]; ok {
		snapshot = makeWitnessSnapshot(snapshot)
	}
	m.Snapshot = snapshot
	return snapshot.Index
}

func makeWitnessSnapshot(snapshot pb.Snapshot) pb.Snapshot {
	result := snapshot
	result.Filepath = ""
	result.FileSize = 0
	result.Files = nil
	result.Witness = true
	result.Dummy = false
	return result
}

func (r *raft) makeReplicateMessage(to uint64,
	next uint64, maxSize uint64) (pb.Message, error) {
	term, err := r.log.term(next - 1)
	if err != nil {
		return pb.Message{}, err
	}
	entries, err := r.log.entries(next, maxSize)
	if err != nil {
		return pb.Message{}, err
	}
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		expected := next - 1 + uint64(len(entries))
		if lastIndex != expected {
			plog.Panicf("%s expected last index in Replicate %d, got %d",
				r.describe(), expected, lastIndex)
		}
	}
	// Don't send actual log entry to witness as they won't replicate real message,
	// unless there is a config change.
	if _, ok := r.witnesses[to]; ok {
		entries = makeMetadataEntries(entries)
	}
	return pb.Message{
		To:       to,
		Type:     pb.Replicate,
		LogIndex: next - 1,
		LogTerm:  term,
		Entries:  entries,
		Commit:   r.log.committed,
	}, nil
}

func makeMetadataEntries(entries []pb.Entry) []pb.Entry {
	me := make([]pb.Entry, 0, len(entries))
	for _, ent := range entries {
		if ent.Type != pb.ConfigChangeEntry {
			me = append(me, pb.Entry{
				Type:  pb.MetadataEntry,
				Index: ent.Index,
				Term:  ent.Term,
			})
		} else {
			me = append(me, ent)
		}
	}
	return me
}

func (r *raft) sendReplicateMessage(to uint64) {
	var rp *remote
	if v, ok := r.remotes[to]; ok {
		rp = v
	} else if v, ok := r.nonVotings[to]; ok {
		rp = v
	} else {
		rp, ok = r.witnesses[to]
		if !ok {
			plog.Panicf("%s failed to get the remote instance", r.describe())
		}
	}
	if rp.isPaused() {
		return
	}
	m, err := r.makeReplicateMessage(to, rp.next, maxEntrySize)
	if err != nil {
		// log not available due to compaction, send snapshot
		if !rp.isActive() {
			plog.Warningf("%s, %s is not active, sending snapshot is skipped",
				r.describe(), ReplicaID(to))
			return
		}
		index := r.makeInstallSnapshotMessage(to, &m)
		plog.Infof("%s is sending snapshot (%d) to %s, r.Next %d, r.Match %d, %v",
			r.describe(), index, ReplicaID(to), rp.next, rp.match, err)
		rp.becomeSnapshot(index)
	} else if len(m.Entries) > 0 {
		lastIndex := m.Entries[len(m.Entries)-1].Index
		rp.progress(lastIndex)
	}
	r.send(m)
}

func (r *raft) broadcastReplicateMessage() {
	r.mustBeLeader()
	for nid := range r.nonVotings {
		if nid == r.replicaID {
			plog.Panicf("%s nonVoting is broadcasting Replicate msg", r.describe())
		}
	}
	for _, nid := range r.nodes() {
		if nid != r.replicaID {
			r.sendReplicateMessage(nid)
		}
	}
}

func (r *raft) sendHeartbeatMessage(to uint64,
	hint pb.SystemCtx, match uint64) {
	commit := min(match, r.log.committed)
	r.send(pb.Message{
		To:       to,
		Type:     pb.Heartbeat,
		Commit:   commit,
		Hint:     hint.Low,
		HintHigh: hint.High,
	})
}

// p72 of the raft thesis describe how to use Heartbeat message in the ReadIndex
// protocol.
func (r *raft) broadcastHeartbeatMessage() {
	r.mustBeLeader()
	if r.readIndex.hasPendingRequest() {
		ctx := r.readIndex.peepCtx()
		r.broadcastHeartbeatMessageWithHint(ctx)
	} else {
		r.broadcastHeartbeatMessageWithHint(pb.SystemCtx{})
	}
}

func (r *raft) broadcastHeartbeatMessageWithHint(ctx pb.SystemCtx) {
	zeroCtx := pb.SystemCtx{}
	for id, rm := range r.votingMembers() {
		if id != r.replicaID {
			r.sendHeartbeatMessage(id, ctx, rm.match)
		}
	}
	if ctx == zeroCtx {
		for id, rm := range r.nonVotings {
			r.sendHeartbeatMessage(id, zeroCtx, rm.match)
		}
	}
}

func (r *raft) sendTimeoutNowMessage(replicaID uint64) {
	r.send(pb.Message{
		Type: pb.TimeoutNow,
		To:   replicaID,
	})
}

//
// log append and commit
//

func (r *raft) sortMatchValues() {
	// unrolled bubble sort, sort.Slice is not allocation free
	if len(r.matched) == 3 {
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
		if r.matched[1] > r.matched[2] {
			v := r.matched[1]
			r.matched[1] = r.matched[2]
			r.matched[2] = v
		}
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
	} else if len(r.matched) == 1 {
		return
	} else {
		sort.Slice(r.matched, func(i, j int) bool {
			return r.matched[i] < r.matched[j]
		})
	}
}

func (r *raft) tryCommit() (bool, error) {
	r.mustBeLeader()
	if r.numVotingMembers() != len(r.matched) {
		r.resetMatchValueArray()
	}
	idx := 0
	for _, v := range r.remotes {
		r.matched[idx] = v.match
		idx++
	}
	for _, v := range r.witnesses {
		r.matched[idx] = v.match
		idx++
	}
	r.sortMatchValues()
	q := r.matched[r.numVotingMembers()-r.quorum()]
	// see p8 raft paper
	// "Raft never commits log entries from previous terms by counting replicas.
	// Only log entries from the leader’s current term are committed by counting
	// replicas"
	return r.log.tryCommit(q, r.term)
}

func (r *raft) appendEntries(entries []pb.Entry) error {
	lastIndex := r.log.lastIndex()
	for i := range entries {
		entries[i].Term = r.term
		entries[i].Index = lastIndex + 1 + uint64(i)
	}
	r.log.append(entries)
	r.remotes[r.replicaID].tryUpdate(r.log.lastIndex())
	if r.isSingleNodeQuorum() {
		if _, err := r.tryCommit(); err != nil {
			return err
		}
	}
	return nil
}

//
// state transition related functions
//

func (r *raft) toFollowerState(term uint64, leaderID uint64,
	resetElectionTimeout bool) {
	if r.isWitness() {
		panic("transitioning to follower from witness state")
	}
	r.state = follower
	r.reset(term, resetElectionTimeout)
	r.setLeaderID(leaderID)
	plog.Infof("%s became follower", r.describe())
}

func (r *raft) becomeNonVoting(term uint64, leaderID uint64) {
	if !r.isNonVoting() {
		panic("transitioning to nonVoting state from other states")
	}
	if r.isWitness() {
		panic("transitioning to nonVoting from witness state")
	}
	r.reset(term, true)
	r.setLeaderID(leaderID)
	plog.Infof("%s became nonVoting", r.describe())
}

func (r *raft) becomeWitness(term uint64, leaderID uint64) {
	if !r.isWitness() {
		panic("transitioning to witness state from non-witness")
	}
	r.reset(term, true)
	r.setLeaderID(leaderID)
	plog.Infof("%s became witness", r.describe())
}

func (r *raft) becomeFollower(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, true)
}

func (r *raft) becomeFollowerKE(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, false)
}

func (r *raft) becomePreVoteCandidate() {
	if !r.preVote {
		panic("becomePreVoteCandidate called when preVote not enabled")
	}
	if r.isLeader() {
		panic("transitioning to candidate state from leader")
	}
	if r.isNonVoting() {
		panic("nonVoting is becoming candidate")
	}
	if r.isWitness() {
		panic("witness is becoming candidate")
	}
	r.state = preVoteCandidate
	r.reset(r.term, true)
	r.setLeaderID(NoLeader)
	plog.Warningf("%s became PreVote candidate", r.describe())
}

func (r *raft) becomeCandidate() {
	if r.isLeader() {
		panic("transitioning to candidate state from leader")
	}
	if r.isNonVoting() {
		panic("nonVoting is becoming candidate")
	}
	if r.isWitness() {
		panic("witness is becoming candidate")
	}
	r.state = candidate
	// 2nd paragraph section 5.2 of the raft paper
	r.reset(r.term+1, true)
	r.setLeaderID(NoLeader)
	r.vote = r.replicaID
	plog.Warningf("%s became candidate", r.describe())
}

func (r *raft) becomeLeader() error {
	// need a state transition machine
	if !r.isLeader() && !r.isCandidate() {
		plog.Panicf("transitioning to leader state from %v", r.state.String())
	}
	r.state = leader
	r.reset(r.term, true)
	r.setLeaderID(r.replicaID)
	r.preLeaderPromotionHandleConfigChange()
	plog.Infof("%s became leader", r.describe())
	// p72 of the raft thesis
	return r.appendEntries([]pb.Entry{{Type: pb.ApplicationEntry, Cmd: nil}})
}

func (r *raft) reset(term uint64, resetElectionTimeout bool) {
	if r.term != term {
		r.term = term
		r.vote = NoLeader
	}
	if r.rl.Enabled() {
		r.rl.Reset()
	}
	if resetElectionTimeout {
		r.electionTick = 0
		r.setRandomizedElectionTimeout()
	}
	r.votes = make(map[uint64]bool)
	r.heartbeatTick = 0
	r.readIndex = newReadIndex()
	r.clearPendingConfigChange()
	r.abortLeaderTransfer()
	r.resetRemotes()
	r.resetNonVotings()
	r.resetWitnesses()
	r.resetMatchValueArray()
}

func (r *raft) preLeaderPromotionHandleConfigChange() {
	n := r.getPendingConfigChangeCount()
	if n > 1 {
		plog.Panicf("%s multiple uncommitted config change entries", r.describe())
	} else if n == 1 {
		plog.Infof("%s becoming leader with pending ConfigChange", r.describe())
		r.setPendingConfigChange()
	}
}

// see section 5.3 of the raft paper
// "When a leader first comes to power, it initializes all nextIndex values to
// the index just after the last one in its log"
func (r *raft) resetRemotes() {
	for id := range r.remotes {
		r.remotes[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.remotes[id].match = r.log.lastIndex()
		}
	}
}

func (r *raft) resetNonVotings() {
	for id := range r.nonVotings {
		r.nonVotings[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.nonVotings[id].match = r.log.lastIndex()
		}
	}
}

func (r *raft) resetWitnesses() {
	for id := range r.witnesses {
		r.witnesses[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.witnesses[id].match = r.log.lastIndex()
		}
	}
}

//
// election related functions
//

func (r *raft) handleVoteResp(from uint64, rejected bool, preVote bool) int {
	mname := "RequestVoteResp"
	if preVote {
		mname = "RequestPreVoteResp"
	}
	if rejected {
		plog.Warningf("%s received %s rejection from %s",
			r.describe(), mname, ReplicaID(from))
	} else {
		plog.Warningf("%s received %s from %s",
			r.describe(), mname, ReplicaID(from))
	}
	votedFor := 0
	if _, ok := r.votes[from]; !ok {
		r.votes[from] = !rejected
	}
	for _, v := range r.votes {
		if v {
			votedFor++
		}
	}
	return votedFor
}

func (r *raft) preVoteCampaign() error {
	r.becomePreVoteCandidate()
	r.handleVoteResp(r.replicaID, false, true)
	if r.isSingleNodeQuorum() {
		return r.campaign()
	}
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     r.term + 1,
			To:       k,
			Type:     pb.RequestPreVote,
			LogIndex: index,
			LogTerm:  lastTerm,
		})
		plog.Warningf("%s sent RequestPreVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

func (r *raft) campaign() error {
	r.becomeCandidate()
	term := r.term
	if r.events != nil {
		info := server.CampaignInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Term:      term,
		}
		r.events.CampaignLaunched(info)
	}
	r.handleVoteResp(r.replicaID, false, false)
	if r.isSingleNodeQuorum() {
		return r.becomeLeader()
	}
	var hint uint64
	if r.isLeaderTransferTarget {
		hint = r.replicaID
		r.isLeaderTransferTarget = false
	}
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     term,
			To:       k,
			Type:     pb.RequestVote,
			LogIndex: index,
			LogTerm:  lastTerm,
			Hint:     hint,
		})
		plog.Warningf("%s sent RequestVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

//
// membership management
//

func (r *raft) selfRemoved() bool {
	if r.isNonVoting() {
		_, ok := r.nonVotings[r.replicaID]
		return !ok
	}
	if r.isWitness() {
		_, ok := r.witnesses[r.replicaID]
		return !ok
	}
	_, ok := r.remotes[r.replicaID]
	return !ok
}

func (r *raft) addNode(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && r.isWitness() {
		plog.Panicf("%s is witness", r.describe())
	}
	if _, ok := r.remotes[replicaID]; ok {
		// already a voting member
		return
	}
	if rp, ok := r.nonVotings[replicaID]; ok {
		// promoting to full member with inherited progress info
		r.deleteNonVoting(replicaID)
		r.remotes[replicaID] = rp
		// local peer promoted, become follower
		if replicaID == r.replicaID {
			r.becomeFollower(r.term, r.leaderID)
		}
	} else if _, ok := r.witnesses[replicaID]; ok {
		panic("could not promote witness to full member")
	} else {
		r.setRemote(replicaID, 0, r.log.lastIndex()+1)
	}
}

func (r *raft) addNonVoting(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && !r.isNonVoting() {
		plog.Panicf("%s is not a nonVoting", r.describe())
	}
	if _, ok := r.nonVotings[replicaID]; ok {
		return
	}
	r.setNonVoting(replicaID, 0, r.log.lastIndex()+1)
}

func (r *raft) addWitness(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && !r.isWitness() {
		plog.Panicf("%s is not witness", r.describe())
	}
	if _, ok := r.witnesses[replicaID]; ok {
		return
	}
	r.setWitness(replicaID, 0, r.log.lastIndex()+1)
}

func (r *raft) removeNode(replicaID uint64) error {
	r.deleteRemote(replicaID)
	r.deleteNonVoting(replicaID)
	r.deleteWitness(replicaID)
	r.clearPendingConfigChange()
	// step down as leader once it is removed
	if r.replicaID == replicaID && r.isLeader() {
		r.becomeFollower(r.term, NoLeader)
	}
	if r.leaderTransfering() && r.leaderTransferTarget == replicaID {
		r.abortLeaderTransfer()
	}
	if r.isLeader() && r.numVotingMembers() > 0 {
		ok, err := r.tryCommit()
		if err != nil {
			return err
		}
		if ok {
			r.broadcastReplicateMessage()
		}
	}
	return nil
}

func (r *raft) deleteRemote(replicaID uint64) {
	delete(r.remotes, replicaID)
}

func (r *raft) deleteNonVoting(replicaID uint64) {
	delete(r.nonVotings, replicaID)
}

func (r *raft) deleteWitness(replicaID uint64) {
	delete(r.witnesses, replicaID)
}

func (r *raft) setRemote(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set remote %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.remotes[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

func (r *raft) setNonVoting(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set nonVoting %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.nonVotings[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

func (r *raft) setWitness(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set witness %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.witnesses[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

// helper methods required for the membership change implementation
//
// p33-35 of the raft thesis describes a simple membership change protocol which
// requires only one node can be added or removed at a time. its safety is
// guarded by the fact that when there is only one node to be added or removed
// at a time, the old and new quorum are guaranteed to overlap.
// the protocol described in the raft thesis requires the membership change
// entry to be executed as soon as it is appended. this also introduces an extra
// troublesome step to roll back to an old membership configuration when
// necessary.
// similar to etcd raft, we treat such membership change entry as regular
// entries that are only executed after being committed (by the old quorum).
// to do that, however, we need to further restrict the leader to only has at
// most one pending not applied membership change entry in its log. this is to
// avoid the situation that two pending membership change entries are committed
// in one go with the same quorum while they actually require different quorums.
// consider the following situation -
// for a 3 nodes shard with existing members X, Y and Z, let's say we first
// propose a membership change to add a new node A, before A gets committed and
// applied, say we propose another membership change to add a new node B. When
// B gets committed, A will be committed as well, both will be using the 3 node
// membership quorum meaning both entries concerning A and B will become
// committed when any two of the X, Y, Z shard have them replicated. this thus
// violates the safety requirement as B will require 3 out of the 4 nodes (X,
// Y, Z, A) to have it replicated before it can be committed.
// we use the following pendingConfigChange flag to help tracking whether there
// is already a pending membership change entry in the log waiting to be
// executed.
func (r *raft) setPendingConfigChange() {
	r.pendingConfigChange = true
}

func (r *raft) hasPendingConfigChange() bool {
	return r.pendingConfigChange
}

func (r *raft) clearPendingConfigChange() {
	r.pendingConfigChange = false
}

func (r *raft) getPendingConfigChangeCount() int {
	idx := r.log.committed + 1
	count := 0
	for {
		ents, err := r.log.entries(idx, maxEntriesToApplySize)
		if err != nil {
			plog.Panicf("%s failed to get entries %v", r.describe(), err)
		}
		if len(ents) == 0 {
			return count
		}
		count += countConfigChange(ents)
		idx = ents[len(ents)-1].Index + 1
	}
}

//
// handler for various message types
//

func (r *raft) handleHeartbeatMessage(m pb.Message) error {
	r.log.commitTo(m.Commit)
	r.send(pb.Message{
		To:       m.From,
		Type:     pb.HeartbeatResp,
		Hint:     m.Hint,
		HintHigh: m.HintHigh,
	})
	return nil
}

func (r *raft) handleInstallSnapshotMessage(m pb.Message) error {
	plog.Debugf("%s called handleInstallSnapshotMessage with snapshot from %s",
		r.describe(), ReplicaID(m.From))
	index, term := m.Snapshot.Index, m.Snapshot.Term
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	ok, err := r.restore(m.Snapshot)
	if err != nil {
		return err
	}
	if ok {
		plog.Debugf("%s restored snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.lastIndex()
	} else {
		plog.Debugf("%s rejected snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.committed
		if r.events != nil {
			info := server.SnapshotInfo{
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				Index:     m.Snapshot.Index,
				Term:      m.Snapshot.Term,
				From:      m.From,
			}
			r.events.SnapshotRejected(info)
		}
	}
	r.send(resp)
	return nil
}

func (r *raft) handleReplicateMessage(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	if m.LogIndex < r.log.committed {
		resp.LogIndex = r.log.committed
		r.send(resp)
		return nil
	}
	ok, err := r.log.matchTerm(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if ok {
		if _, err := r.log.tryAppend(m.LogIndex, m.Entries); err != nil {
			return err
		}
		lastIdx := m.LogIndex + uint64(len(m.Entries))
		r.log.commitTo(min(lastIdx, m.Commit))
		resp.LogIndex = lastIdx
	} else {
		plog.Debugf("%s rejected Replicate index %d term %d from %s",
			r.describe(), m.LogIndex, m.Term, ReplicaID(m.From))
		resp.Reject = true
		resp.LogIndex = m.LogIndex
		resp.Hint = r.log.lastIndex()
		if r.events != nil {
			info := server.ReplicationInfo{
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				Index:     m.LogIndex,
				Term:      m.LogTerm,
				From:      m.From,
			}
			r.events.ReplicationRejected(info)
		}
	}
	r.send(resp)
	return nil
}

//
// Step related functions
//

func isPreVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestPreVote || t == pb.RequestPreVoteResp
}

func isRequestVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestVote || t == pb.RequestPreVote
}

func isRequestMessage(t pb.MessageType) bool {
	return t == pb.Propose || t == pb.ReadIndex || t == pb.LeaderTransfer
}

func isLeaderMessage(t pb.MessageType) bool {
	return t == pb.Replicate || t == pb.InstallSnapshot ||
		t == pb.Heartbeat || t == pb.TimeoutNow || t == pb.ReadIndexResp
}

func (r *raft) dropRequestVoteFromHighTermNode(m pb.Message) bool {
	if !isRequestVoteMessage(m.Type) || !r.checkQuorum || m.Term <= r.term {
		return false
	}
	// see p42 of the raft thesis
	if m.Hint == m.From {
		plog.Debugf("%s, RequestVote with leader transfer hint received from %s",
			r.describe(), ReplicaID(m.From))
		return false
	}
	if r.isLeader() && !r.quiesce && r.electionTick >= r.electionTimeout {
		panic("r.electionTick >= r.electionTimeout on leader")
	}
	// we got a RequestVote with higher term, but we recently had heartbeat msg
	// from leader within the minimum election timeout and that leader is known
	// to have quorum. we thus drop such RequestVote to minimize interruption by
	// network partitioned nodes with higher term.
	// this idea is from the last paragraph of the section 6 of the raft paper
	if r.leaderID != NoLeader && r.electionTick < r.electionTimeout {
		return true
	}
	return false
}

func isPreVoteMessageWithExpectedHigherTerm(m pb.Message) bool {
	return m.Type == pb.RequestPreVote ||
		(m.Type == pb.RequestPreVoteResp && !m.Reject)
}

// onMessageTermNotMatched handles the situation in which the incoming
// message has a term value different from local node's term. it returns a
// boolean flag indicating whether the message should be ignored.
// see the 3rd paragraph, section 5.1 of the raft paper for details.
func (r *raft) onMessageTermNotMatched(m pb.Message) bool {
	if m.Term == 0 || m.Term == r.term {
		return false
	}
	if r.dropRequestVoteFromHighTermNode(m) {
		plog.Warningf("%s dropped RequestVote at term %d from %s, leader available",
			r.describe(), m.Term, ReplicaID(m.From))
		return true
	}
	if m.Term > r.term {
		if !isPreVoteMessageWithExpectedHigherTerm(m) {
			plog.Warningf("%s received %s with higher term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
			leaderID := NoLeader
			if isLeaderMessage(m.Type) {
				leaderID = m.From
			}
			if r.isNonVoting() {
				r.becomeNonVoting(m.Term, leaderID)
			} else if r.isWitness() {
				r.becomeWitness(m.Term, leaderID)
			} else {
				if m.Type == pb.RequestVote {
					plog.Warningf("%s become followerKE after receiving higher term from %s",
						r.describe(), ReplicaID(m.From))
					// not to reset the electionTick value to avoid the risk of having the
					// local node not being to campaign at all. if the local node generates
					// the tick much slower than other nodes (e.g. bad config, hardware
					// clock issue, bad scheduling, overloaded etc.), it may lose the chance
					// to ever start a campaign unless we keep its electionTick value here.
					r.becomeFollowerKE(m.Term, leaderID)
				} else {
					plog.Warningf("%s become follower after receiving higher term from %s",
						r.describe(), ReplicaID(m.From))
					r.becomeFollower(m.Term, leaderID)
				}
			}
		}
	} else if m.Term < r.term {
		if m.Type == pb.RequestPreVote ||
			(isLeaderMessage(m.Type) && (r.checkQuorum || r.preVote)) {
			// see test TestFreeStuckCandidateWithCheckQuorum for details
			r.send(pb.Message{To: m.From, Type: pb.NoOP})
		} else {
			plog.Infof("%s ignored %s with lower term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
		}
		return true
	}
	return false
}

func (r *raft) inconsistentRaftConfig(m pb.Message) bool {
	return !r.preVote && isPreVoteMessage(m.Type)
}

func (r *raft) Handle(m pb.Message) error {
	if r.inconsistentRaftConfig(m) {
		panic("received preVote message when preVote is not enabled")
	}
	if !r.onMessageTermNotMatched(m) {
		if !isPreVoteMessage(m.Type) {
			r.doubleCheckTermMatched(m.Term)
		}
		return r.handle(r, m)
	}
	plog.Infof("%s dropped %s from %s, term %d, term not matched",
		r.describe(), m.Type, ReplicaID(m.From), m.Term)
	return nil
}

func (r *raft) hasConfigChangeToApply() bool {
	// this is a hack to make it easier to port etcd raft tests
	// check those *_etcd_test.go for details
	if r.hasNotAppliedConfigChange != nil {
		return r.hasNotAppliedConfigChange()
	}
	// TODO:
	// with the current entry log implementation, the simplification below is no
	// longer required, we can now actually scan the committed but not applied
	// portion of the log as they are now all in memory.
	return r.log.committed > r.getApplied()
}

func (r *raft) canGrantVote(m pb.Message) bool {
	return r.vote == NoNode || r.vote == m.From || m.Term > r.term
}

//
// handlers for nodes in any state
//

func (r *raft) handleNodeElection(m pb.Message) error {
	if !r.isLeader() {
		// there can be multiple pending membership change entries committed but not
		// applied on this node. say with a shard of X, Y and Z, there are two
		// such entries for adding node A and B are committed but not applied
		// available on X. If X is allowed to start a new election, it can become the
		// leader with a vote from any one of the node Y or Z. Further proposals made
		// by the new leader X in the next term will require a quorum of 2 which can
		// have no overlap with the committed quorum of 3. this violates the safety
		// requirement of raft.
		// ignore the Election message when there is membership configure change
		// committed but not applied
		if r.hasConfigChangeToApply() {
			plog.Warningf("%s campaign skipped, pending config change",
				r.describe())
			if r.events != nil {
				info := server.CampaignInfo{
					ShardID:   r.shardID,
					ReplicaID: r.replicaID,
					Term:      r.term,
				}
				r.events.CampaignSkipped(info)
			}
			return nil
		}
		// prevote is enabled, but the user explicitly requested the leadership to
		// be transferred, so skip the pre-vote stage
		if r.preVote && !r.isLeaderTransferTarget {
			plog.Debugf("%s will start a preVote campaign", r.describe())
			return r.preVoteCampaign()
		}
		plog.Debugf("%s will start a campaign", r.describe())
		return r.campaign()
	}
	plog.Debugf("%s is leader, ignored Election", r.describe())
	return nil
}

func (r *raft) handleNodeRequestPreVote(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.RequestPreVoteResp,
	}
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if m.Term < r.term {
		panic("m.term < r.term")
	}
	if m.Term > r.term && isUpToDate {
		resp.Term = m.Term
		plog.Warningf("%s cast preVote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
	} else {
		// m.Term == r.term || !isUpToDate
		plog.Warningf("%s rejected preVote %s index %d term %d,logterm %d, utd %t",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm, isUpToDate)
		resp.Term = r.term
		resp.Reject = true
	}
	r.send(resp)
	return nil
}

func (r *raft) handleNodeRequestVote(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.RequestVoteResp,
	}
	// 3rd paragraph section 5.2 of the raft paper
	canGrant := r.canGrantVote(m)
	// 2nd paragraph section 5.4 of the raft paper
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if canGrant && isUpToDate {
		plog.Warningf("%s cast vote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
		r.electionTick = 0
		r.vote = m.From
	} else {
		plog.Warningf("%s rejected vote %s index%d term%d,logterm%d,grant%v,utd%v",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term,
			m.LogTerm, canGrant, isUpToDate)
		resp.Reject = true
	}
	r.send(resp)
	return nil
}

func (r *raft) handleNodeConfigChange(m pb.Message) error {
	if m.Reject {
		r.clearPendingConfigChange()
	} else {
		cctype := (pb.ConfigChangeType)(m.HintHigh)
		nodeid := m.Hint
		switch cctype {
		case pb.AddNode:
			r.addNode(nodeid)
		case pb.RemoveNode:
			if err := r.removeNode(nodeid); err != nil {
				return err
			}
		case pb.AddNonVoting:
			r.addNonVoting(nodeid)
		case pb.AddWitness:
			r.addWitness(nodeid)
		default:
			panic("unexpected config change type")
		}
	}
	return nil
}

func (r *raft) handleLogQuery(m pb.Message) error {
	if r.logQueryResult == nil {
		entries, err := r.log.getCommittedEntries(m.From, m.To, m.Hint)
		r.logQueryResult = &pb.LogQueryResult{
			FirstIndex: r.log.firstIndex(),
			LastIndex:  r.log.committed + 1,
			Error:      err,
			Entries:    entries,
		}
	} else {
		panic("log query result is not nil")
	}
	return nil
}

func (r *raft) handleLocalTick(m pb.Message) error {
	if m.Reject {
		r.quiescedTick()
		return nil
	}
	return r.tick()
}

func (r *raft) handleRestoreRemote(m pb.Message) error {
	r.restoreRemotes(m.Snapshot)
	return nil
}

//
// message handler functions used by leader
//

func (r *raft) handleLeaderHeartbeat(m pb.Message) error {
	r.broadcastHeartbeatMessage()
	return nil
}

// p69 of the raft thesis
func (r *raft) handleLeaderCheckQuorum(m pb.Message) error {
	r.mustBeLeader()
	if !r.leaderHasQuorum() {
		plog.Warningf("%s has lost quorum", r.describe())
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

func (r *raft) handleLeaderPropose(m pb.Message) error {
	r.mustBeLeader()
	if r.leaderTransfering() {
		plog.Warningf("%s dropped proposal, leader transferring", r.describe())
		r.reportDroppedProposal(m)
		return nil
	}
	for i, e := range m.Entries {
		if e.Type == pb.ConfigChangeEntry {
			if r.hasPendingConfigChange() {
				plog.Warningf("%s dropped config change, pending change", r.describe())
				r.reportDroppedConfigChange(m.Entries[i])
				m.Entries[i] = pb.Entry{Type: pb.ApplicationEntry}
			}
			r.setPendingConfigChange()
		}
	}
	if err := r.appendEntries(m.Entries); err != nil {
		return err
	}
	r.broadcastReplicateMessage()
	return nil
}

// p72 of the raft thesis
func (r *raft) hasCommittedEntryAtCurrentTerm() bool {
	if r.term == 0 {
		panic("not suppose to reach here")
	}
	lastCommittedTerm, err := r.log.term(r.log.committed)
	if err != nil && !errors.Is(err, ErrCompacted) {
		plog.Panicf("%s failed to get term, %v", r.describe(), err)
	}
	return lastCommittedTerm == r.term
}

func (r *raft) clearReadyToRead() {
	r.readyToRead = r.readyToRead[:0]
}

func (r *raft) addReadyToRead(index uint64, ctx pb.SystemCtx) {
	r.readyToRead = append(r.readyToRead,
		pb.ReadyToRead{
			Index:     index,
			SystemCtx: ctx,
		})
}

// section 6.4 of the raft thesis
func (r *raft) handleLeaderReadIndex(m pb.Message) error {
	r.mustBeLeader()
	ctx := pb.SystemCtx{
		High: m.HintHigh,
		Low:  m.Hint,
	}
	if _, wok := r.witnesses[m.From]; wok {
		plog.Errorf("%s dropped ReadIndex, witness node %d", r.describe(), m.From)
	} else if !r.isSingleNodeQuorum() {
		if !r.hasCommittedEntryAtCurrentTerm() {
			// leader doesn't know the commit value of the shard
			// see raft thesis section 6.4, this is the first step of the ReadIndex
			// protocol.
			plog.Warningf("%s dropped ReadIndex, not ready", r.describe())
			r.reportDroppedReadIndex(m)
			return nil
		}
		r.readIndex.addRequest(r.log.committed, ctx, m.From)
		r.broadcastHeartbeatMessageWithHint(ctx)
	} else {
		r.addReadyToRead(r.log.committed, ctx)
		_, ook := r.nonVotings[m.From]
		if m.From != r.replicaID && ook {
			r.send(pb.Message{
				To:       m.From,
				Type:     pb.ReadIndexResp,
				LogIndex: r.log.committed,
				Hint:     m.Hint,
				HintHigh: m.HintHigh,
				Commit:   m.Commit,
			})
		}
	}
	return nil
}

func (r *raft) handleLeaderReplicateResp(m pb.Message, rp *remote) error {
	r.mustBeLeader()
	rp.setActive()
	if !m.Reject {
		paused := rp.isPaused()
		if rp.tryUpdate(m.LogIndex) {
			rp.respondedTo()
			ok, err := r.tryCommit()
			if err != nil {
				return nil
			}
			if ok {
				r.broadcastReplicateMessage()
			} else if paused {
				r.sendReplicateMessage(m.From)
			}
			// according to the leadership transfer protocol listed on the p29 of the
			// raft thesis
			if r.leaderTransfering() && m.From == r.leaderTransferTarget &&
				r.log.lastIndex() == rp.match {
				r.sendTimeoutNowMessage(r.leaderTransferTarget)
			}
		}
	} else {
		// the replication flow control code is derived from etcd raft, it resets
		// nextIndex to match + 1. it is thus even more conservative than the raft
		// thesis's approach of nextIndex = nextIndex - 1 mentioned on the p21 of
		// the thesis.
		if rp.decreaseTo(m.LogIndex, m.Hint) {
			r.enterRetryState(rp)
			r.sendReplicateMessage(m.From)
		}
	}
	return nil
}

func (r *raft) handleLeaderHeartbeatResp(m pb.Message, rp *remote) error {
	r.mustBeLeader()
	rp.setActive()
	rp.waitToRetry()
	if rp.match < r.log.lastIndex() {
		r.sendReplicateMessage(m.From)
	}
	// heartbeat response contains leadership confirmation requested as part of
	// the ReadIndex protocol.
	if m.Hint != 0 {
		r.handleReadIndexLeaderConfirmation(m)
	}
	return nil
}

func (r *raft) handleLeaderTransfer(m pb.Message) error {
	r.mustBeLeader()
	target := m.Hint
	plog.Debugf("%s called handleLeaderTransfer, target %d", r.describe(), target)
	if target == NoNode {
		plog.Panicf("%s leader transfer target not set", r.describe())
	}
	if r.leaderTransfering() {
		plog.Warningf("LeaderTransfer ignored, leader transfer is ongoing")
		return nil
	}
	if r.replicaID == target {
		plog.Warningf("received LeaderTransfer with target pointing to itself")
		return nil
	}
	rp, ok := r.remotes[target]
	if !ok {
		plog.Warningf("unknown LeaderTransfer target")
		return nil
	}
	r.leaderTransferTarget = target
	r.electionTick = 0
	// fast path below
	// or wait for the target node to catch up, see p29 of the raft thesis
	if rp.match == r.log.lastIndex() {
		r.sendTimeoutNowMessage(target)
	}
	return nil
}

func (r *raft) handleReadIndexLeaderConfirmation(m pb.Message) {
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	ris := r.readIndex.confirm(ctx, m.From, r.quorum())
	for _, s := range ris {
		if s.from == NoNode || s.from == r.replicaID {
			r.addReadyToRead(s.index, s.ctx)
		} else {
			r.send(pb.Message{
				To:       s.from,
				Type:     pb.ReadIndexResp,
				LogIndex: s.index,
				Hint:     m.Hint,
				HintHigh: m.HintHigh,
			})
		}
	}
}

func (r *raft) handleLeaderSnapshotStatus(m pb.Message, rp *remote) error {
	if rp.state != remoteSnapshot {
		return nil
	}
	if m.Hint == 0 {
		if m.Reject {
			rp.clearPendingSnapshot()
			plog.Warningf("%s snapshot failed, %s is now in wait state",
				r.describe(), ReplicaID(m.From))
		} else {
			plog.Debugf("%s snapshot succeeded, %s in wait state now, next %d",
				r.describe(), ReplicaID(m.From), rp.next)
		}
		rp.becomeWait()
	} else {
		rp.setSnapshotAck(m.Hint, m.Reject)
		r.snapshotting = true
	}
	return nil
}

func (r *raft) handleLeaderUnreachable(m pb.Message, rp *remote) error {
	plog.Debugf("%s received Unreachable, %s entered retry state",
		r.describe(), ReplicaID(m.From))
	r.enterRetryState(rp)
	return nil
}

func (r *raft) handleLeaderRateLimit(m pb.Message) error {
	if r.rl.Enabled() {
		r.rl.SetFollowerState(m.From, m.Hint)
	} else {
		plog.Warningf("%s dropped rate limit msg, rl disabled", r.describe())
	}
	return nil
}

func (r *raft) enterRetryState(rp *remote) {
	if rp.state == remoteReplicate {
		rp.becomeRetry()
	}
}

func (r *raft) checkPendingSnapshotAck() error {
	if r.isLeader() && r.snapshotting {
		check := func(m map[uint64]*remote) error {
			for from, rp := range m {
				if rp.state == remoteSnapshot {
					if rp.delayed.tick() {
						if err := r.Handle(pb.Message{
							Type:   pb.SnapshotStatus,
							From:   from,
							Reject: rp.delayed.rejected,
							Hint:   0,
						}); err != nil {
							return err
						}
						rp.clearSnapshotAck()
					} else {
						r.snapshotting = true
					}
				}
			}
			return nil
		}
		r.snapshotting = false
		if err := check(r.remotes); err != nil {
			return err
		}
		if err := check(r.nonVotings); err != nil {
			return err
		}
		if err := check(r.witnesses); err != nil {
			return err
		}
	}
	return nil
}

//
// message handlers used by nonVoting, re-route them to follower handlers
//

func (r *raft) handleNonVotingReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

func (r *raft) handleNonVotingHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

func (r *raft) handleNonVotingSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
}

func (r *raft) handleNonVotingPropose(m pb.Message) error {
	return r.handleFollowerPropose(m)
}

func (r *raft) handleNonVotingReadIndex(m pb.Message) error {
	return r.handleFollowerReadIndex(m)
}

func (r *raft) handleNonVotingReadIndexResp(m pb.Message) error {
	return r.handleFollowerReadIndexResp(m)
}

//
// message handlers used by witness, re-route them to follower handlers
//

func (r *raft) handleWitnessReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

func (r *raft) handleWitnessHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

func (r *raft) handleWitnessSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
}

//
// message handlers used by follower
//

func (r *raft) handleFollowerPropose(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped proposal, no leader", r.describe())
		r.reportDroppedProposal(m)
		return nil
	}
	m.To = r.leaderID
	// the message might be queued by the transport layer, this violates the
	// requirement of the entryQueue.get() func. copy the m.Entries to its
	// own space.
	m.Entries = newEntrySlice(m.Entries)
	r.send(m)
	return nil
}

func (r *raft) leaderIsAvailable() {
	r.electionTick = 0
}

func (r *raft) handleFollowerReplicate(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleReplicateMessage(m)
}

func (r *raft) handleFollowerHeartbeat(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleHeartbeatMessage(m)
}

func (r *raft) handleFollowerReadIndex(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped ReadIndex, no leader", r.describe())
		r.reportDroppedReadIndex(m)
		return nil
	}
	m.To = r.leaderID
	r.send(m)
	return nil
}

func (r *raft) handleFollowerLeaderTransfer(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped LeaderTransfer, no leader", r.describe())
		return nil
	}
	m.To = r.leaderID
	r.send(m)
	return nil
}

func (r *raft) handleFollowerReadIndexResp(m pb.Message) error {
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	r.addReadyToRead(m.LogIndex, ctx)
	return nil
}

func (r *raft) handleFollowerInstallSnapshot(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleInstallSnapshotMessage(m)
}

func (r *raft) handleFollowerTimeoutNow(m pb.Message) error {
	// the last paragraph, p29 of the raft thesis mentions that this is nothing
	// different from the clock moving forward quickly
	plog.Debugf("%s TimeoutNow received", r.describe())
	r.electionTick = r.randomizedElectionTimeout
	r.isLeaderTransferTarget = true
	if err := r.tick(); err != nil {
		return err
	}
	if r.isLeaderTransferTarget {
		r.isLeaderTransferTarget = false
	}
	return nil
}

//
// handler functions used by candidate
//

func (r *raft) doubleCheckTermMatched(msgTerm uint64) {
	if msgTerm != 0 && r.term != msgTerm {
		plog.Panicf("%s mismatched term found", r.describe())
	}
}

func (r *raft) handleCandidatePropose(m pb.Message) error {
	plog.Warningf("%s dropped proposal, no leader", r.describe())
	r.reportDroppedProposal(m)
	return nil
}

func (r *raft) handleCandidateReadIndex(m pb.Message) error {
	plog.Warningf("%s dropped read index, no leader", r.describe())
	r.reportDroppedReadIndex(m)
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, ctx)
	return nil
}

// when any of the following three methods
// handleCandidateReplicate
// handleCandidateInstallSnapshot
// handleCandidateHeartbeat
// is called, it implies that m.Term == r.term and there is a leader
// for that term. see 4th paragraph section 5.2 of the raft paper
func (r *raft) handleCandidateReplicate(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleReplicateMessage(m)
}

func (r *raft) handleCandidateInstallSnapshot(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleInstallSnapshotMessage(m)
}

func (r *raft) handleCandidateHeartbeat(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleHeartbeatMessage(m)
}

func (r *raft) handleCandidateRequestVoteResp(m pb.Message) error {
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestVoteResp from nonVoting")
		return nil
	}
	count := r.handleVoteResp(m.From, m.Reject, false)
	plog.Warningf("%s received %d votes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())
	// 3rd paragraph section 5.2 of the raft paper
	if count == r.quorum() {
		if err := r.becomeLeader(); err != nil {
			return err
		}
		// get the NoOP entry committed ASAP
		r.broadcastReplicateMessage()
	} else if len(r.votes)-count == r.quorum() {
		// etcd raft does this, it is not stated in the raft paper
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

//
// handler functions for preVote candidate
//

func (r *raft) handlePreVoteCandidateRequestPreVoteResp(m pb.Message) error {
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestPreVoteResp from nonVoting")
		return nil
	}
	count := r.handleVoteResp(m.From, m.Reject, true)
	plog.Warningf("%s received %d preVotes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())
	if count == r.quorum() {
		if err := r.campaign(); err != nil {
			return err
		}
	} else if len(r.votes)-count == r.quorum() {
		// etcd raft does this, it is not stated in the raft paper
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

func (r *raft) reportDroppedConfigChange(e pb.Entry) {
	r.droppedEntries = append(r.droppedEntries, e)
}

func (r *raft) reportDroppedProposal(m pb.Message) {
	r.droppedEntries = append(r.droppedEntries, newEntrySlice(m.Entries)...)
	if r.events != nil {
		info := server.ProposalInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Entries:   m.Entries,
		}
		r.events.ProposalDropped(info)
	}
}

func (r *raft) reportDroppedReadIndex(m pb.Message) {
	sysctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, sysctx)
	if r.events != nil {
		info := server.ReadIndexInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
		}
		r.events.ReadIndexDropped(info)
	}
}

func lw(r *raft, f func(m pb.Message, rp *remote) error) handlerFunc {
	w := func(nm pb.Message) error {
		if npr, ok := r.remotes[nm.From]; ok {
			return f(nm, npr)
		} else if nob, ok := r.nonVotings[nm.From]; ok {
			return f(nm, nob)
		} else if wob, ok := r.witnesses[nm.From]; ok {
			return f(nm, wob)
		} else {
			plog.Warningf("%s no remote for %s", r.describe(), ReplicaID(nm.From))
			return nil
		}
	}
	return w
}

func defaultHandle(r *raft, m pb.Message) error {
	if f := r.handlers[r.state][m.Type]; f != nil {
		return f(m)
	}
	return nil
}

func (r *raft) initializeHandlerMap() {
	// candidate
	r.handlers[candidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[candidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[candidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[candidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[candidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[candidate][pb.RequestVoteResp] = r.handleCandidateRequestVoteResp
	r.handlers[candidate][pb.Election] = r.handleNodeElection
	r.handlers[candidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[candidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[candidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[candidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[candidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[candidate][pb.LogQuery] = r.handleLogQuery
	// prevote candidate
	r.handlers[preVoteCandidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[preVoteCandidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[preVoteCandidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[preVoteCandidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[preVoteCandidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[preVoteCandidate][pb.RequestPreVoteResp] = r.handlePreVoteCandidateRequestPreVoteResp
	r.handlers[preVoteCandidate][pb.Election] = r.handleNodeElection
	r.handlers[preVoteCandidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[preVoteCandidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[preVoteCandidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[preVoteCandidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[preVoteCandidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[preVoteCandidate][pb.LogQuery] = r.handleLogQuery
	// follower
	r.handlers[follower][pb.Propose] = r.handleFollowerPropose
	r.handlers[follower][pb.Replicate] = r.handleFollowerReplicate
	r.handlers[follower][pb.Heartbeat] = r.handleFollowerHeartbeat
	r.handlers[follower][pb.ReadIndex] = r.handleFollowerReadIndex
	r.handlers[follower][pb.LeaderTransfer] = r.handleFollowerLeaderTransfer
	r.handlers[follower][pb.ReadIndexResp] = r.handleFollowerReadIndexResp
	r.handlers[follower][pb.InstallSnapshot] = r.handleFollowerInstallSnapshot
	r.handlers[follower][pb.Election] = r.handleNodeElection
	r.handlers[follower][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[follower][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[follower][pb.TimeoutNow] = r.handleFollowerTimeoutNow
	r.handlers[follower][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[follower][pb.LocalTick] = r.handleLocalTick
	r.handlers[follower][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[follower][pb.LogQuery] = r.handleLogQuery
	// leader
	r.handlers[leader][pb.LeaderHeartbeat] = r.handleLeaderHeartbeat
	r.handlers[leader][pb.CheckQuorum] = r.handleLeaderCheckQuorum
	r.handlers[leader][pb.Propose] = r.handleLeaderPropose
	r.handlers[leader][pb.ReadIndex] = r.handleLeaderReadIndex
	r.handlers[leader][pb.ReplicateResp] = lw(r, r.handleLeaderReplicateResp)
	r.handlers[leader][pb.HeartbeatResp] = lw(r, r.handleLeaderHeartbeatResp)
	r.handlers[leader][pb.SnapshotStatus] = lw(r, r.handleLeaderSnapshotStatus)
	r.handlers[leader][pb.Unreachable] = lw(r, r.handleLeaderUnreachable)
	r.handlers[leader][pb.LeaderTransfer] = r.handleLeaderTransfer
	r.handlers[leader][pb.Election] = r.handleNodeElection
	r.handlers[leader][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[leader][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[leader][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[leader][pb.LocalTick] = r.handleLocalTick
	r.handlers[leader][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[leader][pb.RateLimit] = r.handleLeaderRateLimit
	r.handlers[leader][pb.LogQuery] = r.handleLogQuery
	// nonVoting
	r.handlers[nonVoting][pb.Heartbeat] = r.handleNonVotingHeartbeat
	r.handlers[nonVoting][pb.Replicate] = r.handleNonVotingReplicate
	r.handlers[nonVoting][pb.InstallSnapshot] = r.handleNonVotingSnapshot
	r.handlers[nonVoting][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[nonVoting][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[nonVoting][pb.Propose] = r.handleNonVotingPropose
	r.handlers[nonVoting][pb.ReadIndex] = r.handleNonVotingReadIndex
	r.handlers[nonVoting][pb.ReadIndexResp] = r.handleNonVotingReadIndexResp
	r.handlers[nonVoting][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[nonVoting][pb.LocalTick] = r.handleLocalTick
	r.handlers[nonVoting][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[nonVoting][pb.LogQuery] = r.handleLogQuery
	// witness
	r.handlers[witness][pb.Heartbeat] = r.handleWitnessHeartbeat
	r.handlers[witness][pb.Replicate] = r.handleWitnessReplicate
	r.handlers[witness][pb.InstallSnapshot] = r.handleWitnessSnapshot
	r.handlers[witness][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[witness][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[witness][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[witness][pb.LocalTick] = r.handleLocalTick
	r.handlers[witness][pb.SnapshotReceived] = r.handleRestoreRemote
}

func (r *raft) checkHandlerMap() {
	// following states/types are not supposed to have handler filled in
	checks := []struct {
		stateType State
		msgType   pb.MessageType
	}{
		{leader, pb.Heartbeat},
		{leader, pb.Replicate},
		{leader, pb.InstallSnapshot},
		{leader, pb.ReadIndexResp},
		{leader, pb.RequestPreVoteResp},
		{follower, pb.ReplicateResp},
		{follower, pb.HeartbeatResp},
		{follower, pb.SnapshotStatus},
		{follower, pb.Unreachable},
		{follower, pb.RequestPreVoteResp},
		{candidate, pb.ReplicateResp},
		{candidate, pb.HeartbeatResp},
		{candidate, pb.SnapshotStatus},
		{candidate, pb.Unreachable},
		{candidate, pb.RequestPreVoteResp},
		{preVoteCandidate, pb.ReplicateResp},
		{preVoteCandidate, pb.HeartbeatResp},
		{preVoteCandidate, pb.SnapshotStatus},
		{preVoteCandidate, pb.Unreachable},
		{nonVoting, pb.Election},
		{nonVoting, pb.RequestVoteResp},
		{nonVoting, pb.ReplicateResp},
		{nonVoting, pb.HeartbeatResp},
		{nonVoting, pb.RequestPreVoteResp},
		{witness, pb.Election},
		{witness, pb.Propose},
		{witness, pb.ReadIndex},
		{witness, pb.ReadIndexResp},
		{witness, pb.RequestVoteResp},
		{witness, pb.ReplicateResp},
		{witness, pb.HeartbeatResp},
		{witness, pb.RequestPreVoteResp},
		{witness, pb.LogQuery},
	}
	for _, tt := range checks {
		f := r.handlers[tt.stateType][tt.msgType]
		if f != nil {
			panic("unexpected msg handler")
		}
	}
}
