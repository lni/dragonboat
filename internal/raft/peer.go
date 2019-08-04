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
// Peer.go is the interface used by the upper layer to access functionalities
// provided by the raft protocol. It translates all incoming requests to raftpb
// messages and pass them to the raft protocol implementation to be handled.
// Such a state machine style design together with the iterative style interface
// here is derived from etcd.
// Compared to etcd raft, we strictly model all inputs to the raft protocol as
// messages including those used to advance the raft state.
//

package raft

import (
	"sort"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/server"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

// PeerAddress is the basic info for a peer in the Raft cluster.
type PeerAddress struct {
	NodeID  uint64
	Address string
}

// Peer is the interface struct for interacting with the underlying Raft
// protocol implementation.
type Peer struct {
	raft      *raft
	prevState pb.State
}

// Launch starts or restarts a Raft node.
func Launch(config *config.Config,
	logdb ILogDB, events server.IRaftEventListener,
	addresses []PeerAddress, initial bool, newNode bool) *Peer {
	checkLaunchRequest(config, addresses, initial, newNode)
	r := newRaft(config, logdb)
	rc := &Peer{raft: r}
	rc.raft.events = events
	_, lastIndex := logdb.GetRange()
	if newNode && !config.IsObserver {
		r.becomeFollower(1, NoLeader)
	}
	plog.Infof("raft node created, %s, lastIndex %d, initial %t, newNode %t",
		r.describe(), lastIndex, initial, newNode)
	if initial && newNode {
		bootstrap(r, addresses)
	}
	if lastIndex == 0 {
		rc.prevState = emptyState
	} else {
		rc.prevState = r.raftState()
	}
	return rc
}

// Tick moves the logical clock forward by one tick.
func (rc *Peer) Tick() {
	rc.raft.Handle(pb.Message{
		Type:   pb.LocalTick,
		Reject: false,
	})
}

// QuiescedTick moves the logical clock forward by one tick in quiesced mode.
func (rc *Peer) QuiescedTick() {
	rc.raft.Handle(pb.Message{
		Type:   pb.LocalTick,
		Reject: true,
	})
}

// RequestLeaderTransfer makes a request to transfer the leadership to the
// specified target node.
func (rc *Peer) RequestLeaderTransfer(target uint64) {
	rc.raft.Handle(pb.Message{
		Type: pb.LeaderTransfer,
		To:   rc.raft.nodeID,
		From: target,
		Hint: target,
	})
}

// ProposeEntries proposes specified entries in a batched mode using a single
// MTPropose message.
func (rc *Peer) ProposeEntries(ents []pb.Entry) {
	rc.raft.Handle(pb.Message{
		Type:    pb.Propose,
		From:    rc.raft.nodeID,
		Entries: ents,
	})
}

// ProposeConfigChange proposes a raft membership change.
func (rc *Peer) ProposeConfigChange(configChange pb.ConfigChange, key uint64) {
	data, err := configChange.Marshal()
	if err != nil {
		panic(err)
	}
	rc.raft.Handle(pb.Message{
		Type:    pb.Propose,
		Entries: []pb.Entry{{Type: pb.ConfigChangeEntry, Cmd: data, Key: key}},
	})
}

// ApplyConfigChange applies a raft membership change to the local raft node.
func (rc *Peer) ApplyConfigChange(cc pb.ConfigChange) {
	if cc.NodeID == NoLeader {
		rc.raft.clearPendingConfigChange()
		return
	}
	rc.raft.Handle(pb.Message{
		Type:     pb.ConfigChangeEvent,
		Reject:   false,
		Hint:     cc.NodeID,
		HintHigh: uint64(cc.Type),
	})
}

// RejectConfigChange rejects the currently pending raft membership change.
func (rc *Peer) RejectConfigChange() {
	rc.raft.Handle(pb.Message{
		Type:   pb.ConfigChangeEvent,
		Reject: true,
	})
}

// RestoreRemotes applies the remotes info obtained from the specified snapshot.
func (rc *Peer) RestoreRemotes(ss pb.Snapshot) {
	rc.raft.Handle(pb.Message{
		Type:     pb.SnapshotReceived,
		Snapshot: ss,
	})
}

// ReportUnreachableNode marks the specified node as not reachable.
func (rc *Peer) ReportUnreachableNode(nodeID uint64) {
	rc.raft.Handle(pb.Message{
		Type: pb.Unreachable,
		From: nodeID,
	})
}

// ReportSnapshotStatus reports the status of the snapshot to the local raft
// node.
func (rc *Peer) ReportSnapshotStatus(nodeID uint64, reject bool) {
	rc.raft.Handle(pb.Message{
		Type:   pb.SnapshotStatus,
		From:   nodeID,
		Reject: reject,
	})
}

// Handle processes the given message.
func (rc *Peer) Handle(m pb.Message) {
	if IsLocalMessageType(m.Type) {
		panic("local message sent to Step")
	}
	_, rok := rc.raft.remotes[m.From]
	_, ook := rc.raft.observers[m.From]
	if rok || ook || !isResponseMessageType(m.Type) {
		rc.raft.Handle(m)
	}
}

// GetUpdate returns the current state of the Peer.
func (rc *Peer) GetUpdate(moreEntriesToApply bool,
	lastApplied uint64) pb.Update {
	return getUpdate(rc.raft, rc.prevState, moreEntriesToApply, lastApplied)
}

// RateLimited returns a boolean flag indicating whether the Raft node is rate
// limited.
func (rc *Peer) RateLimited() bool {
	return rc.raft.rl.RateLimited()
}

// HasUpdate returns a boolean value indicating whether there is any Update
// ready to be processed.
func (rc *Peer) HasUpdate(moreEntriesToApply bool) bool {
	r := rc.raft
	if pst := r.raftState(); !pb.IsEmptyState(pst) &&
		!pb.IsStateEqual(pst, rc.prevState) {
		return true
	}
	if r.log.inmem.snapshot != nil &&
		!pb.IsEmptySnapshot(*r.log.inmem.snapshot) {
		return true
	}
	if len(r.msgs) > 0 {
		return true
	}
	if len(r.log.entriesToSave()) > 0 {
		return true
	}
	if moreEntriesToApply && r.log.hasEntriesToApply() {
		return true
	}
	if len(r.readyToRead) != 0 {
		return true
	}
	if len(r.droppedEntries) > 0 || len(r.droppedReadIndexes) > 0 {
		return true
	}
	return false
}

// Commit commits the Update state to mark it as processed.
func (rc *Peer) Commit(ud pb.Update) {
	rc.raft.msgs = nil
	rc.raft.droppedEntries = nil
	rc.raft.droppedReadIndexes = nil
	if !pb.IsEmptyState(ud.State) {
		rc.prevState = ud.State
	}
	if ud.UpdateCommit.ReadyToRead > 0 {
		rc.raft.clearReadyToRead()
	}
	rc.entryLog().commitUpdate(ud.UpdateCommit)
}

// LocalStatus returns the current local status of the raft node.
func (rc *Peer) LocalStatus() Status {
	return getLocalStatus(rc.raft)
}

// ReadIndex starts a ReadIndex operation. The ReadIndex protocol is defined in
// the section 6.4 of the Raft thesis.
func (rc *Peer) ReadIndex(ctx pb.SystemCtx) {
	rc.raft.Handle(pb.Message{
		Type:     pb.ReadIndex,
		Hint:     ctx.Low,
		HintHigh: ctx.High,
	})
}

// DumpRaftInfoToLog prints the raft state to log for debugging purposes.
func (rc *Peer) DumpRaftInfoToLog(addrMap map[uint64]string) {
	rc.raft.dumpRaftInfoToLog(addrMap)
}

// NotifyRaftLastApplied passes on the lastApplied index confirmed by the RSM to
// the raft state machine.
func (rc *Peer) NotifyRaftLastApplied(lastApplied uint64) {
	rc.raft.setApplied(lastApplied)
}

// HasEntryToApply returns a boolean flag indicating whether there are more
// entries ready to be applied.
func (rc *Peer) HasEntryToApply() bool {
	return rc.entryLog().hasEntriesToApply()
}

func (rc *Peer) entryLog() *entryLog {
	return rc.raft.log
}

func checkLaunchRequest(config *config.Config,
	addresses []PeerAddress, initial bool, newNode bool) {
	if config.NodeID == 0 {
		panic("config.NodeID must not be zero")
	}
	plog.Infof("initial node: %t, new node %t", initial, newNode)
	if initial && newNode && len(addresses) == 0 {
		panic("addresses must be specified")
	}
	uniqueAddressList := make(map[string]struct{})
	for _, addr := range addresses {
		uniqueAddressList[string(addr.Address)] = struct{}{}
	}
	if len(uniqueAddressList) != len(addresses) {
		plog.Panicf("duplicated address found %v", addresses)
	}
}

func bootstrap(r *raft, addresses []PeerAddress) {
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].NodeID < addresses[j].NodeID
	})
	ents := make([]pb.Entry, len(addresses))
	for i, peer := range addresses {
		plog.Infof("%s inserting a bootstrap ConfigChangeAddNode, %d, %s",
			r.describe(), peer.NodeID, string(peer.Address))
		cc := pb.ConfigChange{
			Type:       pb.AddNode,
			NodeID:     peer.NodeID,
			Initialize: true,
			Address:    peer.Address,
		}
		data, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		ents[i] = pb.Entry{
			Type:  pb.ConfigChangeEntry,
			Term:  1,
			Index: uint64(i + 1),
			Cmd:   data,
		}
	}
	r.log.append(ents)
	r.log.committed = uint64(len(ents))
	for _, peer := range addresses {
		r.addNode(peer.NodeID)
	}
}

func getUpdateCommit(ud pb.Update) pb.UpdateCommit {
	uc := pb.UpdateCommit{
		ReadyToRead: uint64(len(ud.ReadyToReads)),
		LastApplied: ud.LastApplied,
	}
	if len(ud.CommittedEntries) > 0 {
		uc.Processed = ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
	}
	if len(ud.EntriesToSave) > 0 {
		lastEntry := ud.EntriesToSave[len(ud.EntriesToSave)-1]
		uc.StableLogTo, uc.StableLogTerm = lastEntry.Index, lastEntry.Term
	}
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		uc.StableSnapshotTo = ud.Snapshot.Index
		uc.Processed = max(uc.Processed, uc.StableSnapshotTo)
	}
	return uc
}

func getUpdate(r *raft,
	ppst pb.State, moreEntriesToApply bool, lastApplied uint64) pb.Update {
	ud := pb.Update{
		ClusterID:     r.clusterID,
		NodeID:        r.nodeID,
		EntriesToSave: r.log.entriesToSave(),
		Messages:      r.msgs,
		LastApplied:   lastApplied,
	}
	if moreEntriesToApply {
		ud.CommittedEntries = r.log.entriesToApply()
	}
	if len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		ud.MoreCommittedEntries = r.log.hasMoreEntriesToApply(lastIndex)
	}
	if pst := r.raftState(); !pb.IsStateEqual(pst, ppst) {
		ud.State = pst
	}
	if r.log.inmem.snapshot != nil {
		ud.Snapshot = *r.log.inmem.snapshot
	}
	if len(r.readyToRead) > 0 {
		ud.ReadyToReads = r.readyToRead
	}
	if len(r.droppedEntries) > 0 {
		ud.DroppedEntries = r.droppedEntries
	}
	if len(r.droppedReadIndexes) > 0 {
		ud.DroppedReadIndexes = r.droppedReadIndexes
	}
	ud.UpdateCommit = getUpdateCommit(ud)
	return ud
}
