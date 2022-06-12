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

//go:build dragonboat_monkeytest
// +build dragonboat_monkeytest

package dragonboat

import (
	"sync/atomic"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
)

func ApplyMonkeySettings() {
	transport.ApplyMonkeySettings()
}

//
// code here is used in testing only.
//

// MemFS is a in memory vfs intended to be used in testing. User applications
// can usually ignore such vfs related types and fields.
type MemFS = vfs.MemFS

// GetTestFS returns a vfs instance that can be used in testing. User
// applications can usually ignore such vfs related types and fields.
func GetTestFS() config.IFS {
	return vfs.GetTestFS()
}

// Shards returns a list of raft nodes managed by the nodehost instance.
func (nh *NodeHost) Shards() []*node {
	result := make([]*node, 0)
	nh.mu.RLock()
	nh.mu.shards.Range(func(k, v interface{}) bool {
		result = append(result, v.(*node))
		return true
	})
	nh.mu.RUnlock()
	return result
}

func SetPendingProposalShards(sz uint64) {
	pendingProposalShards = sz
}

func SetTaskBatchSize(sz uint64) {
	taskBatchSize = sz
}

func SetIncomingProposalsMaxLen(sz uint64) {
	incomingProposalsMaxLen = sz
}

func SetIncomingReadIndexMaxLen(sz uint64) {
	incomingReadIndexMaxLen = sz
}

func SetReceiveQueueLen(v uint64) {
	receiveQueueLen = v
}

func (nh *NodeHost) Stopped() bool {
	return atomic.LoadInt32(&nh.closed) != 0
}

func (nh *NodeHost) SetTransportDropBatchHook(f transport.SendMessageBatchFunc) {
	nh.transport.(*transport.Transport).SetPreSendBatchHook(f)
}

func (nh *NodeHost) SetPreStreamChunkSendHook(f transport.StreamChunkSendFunc) {
	nh.transport.(*transport.Transport).SetPreStreamChunkSendHook(f)
}

func (nh *NodeHost) GetLogDB() raftio.ILogDB {
	return nh.mu.logdb
}

func (n *node) GetLastApplied() uint64 {
	return n.sm.GetLastApplied()
}

func (n *node) DumpRaftInfoToLog() {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	n.dumpRaftInfoToLog()
}

func (n *node) IsLeader() bool {
	return n.isLeader()
}

func (n *node) IsFollower() bool {
	return n.isFollower()
}

func (n *node) GetStateMachineHash() uint64 {
	return n.getStateMachineHash()
}

func (n *node) GetSessionHash() uint64 {
	return n.getSessionHash()
}

func (n *node) GetMembershipHash() uint64 {
	return n.getMembershipHash()
}

func (n *node) GetRateLimiter() *server.InMemRateLimiter {
	return n.p.GetRateLimiter()
}

func (n *node) GetInMemLogSize() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.p.GetInMemLogSize()
}

func (n *node) getStateMachineHash() uint64 {
	if v, err := n.sm.GetHash(); err != nil {
		panic(err)
	} else {
		return v
	}
}

func (n *node) getSessionHash() uint64 {
	return n.sm.GetSessionHash()
}

func (n *node) getMembershipHash() uint64 {
	return n.sm.GetMembershipHash()
}

func (n *node) dumpRaftInfoToLog() {
	addrMap := make(map[uint64]string)
	m := n.sm.GetMembership()
	for replicaID := range m.Addresses {
		if replicaID == n.replicaID {
			addrMap[replicaID] = n.getRaftAddress()
		} else {
			v, _, err := n.nodeRegistry.Resolve(n.shardID, replicaID)
			if err == nil {
				addrMap[replicaID] = v
			}
		}
	}
	n.p.DumpRaftInfoToLog(addrMap)
}

// PartitionNode puts the node into test partition mode. All connectivity to
// the outside world should be stopped.
func (nh *NodeHost) PartitionNode() {
	plog.Infof("entered partition test mode")
	atomic.StoreInt32(&nh.partitioned, 1)
}

// RestorePartitionedNode removes the node from test partition mode. No other
// change is going to be made on the local node. It is up to the local node it
// self to repair/restore any other state.
func (nh *NodeHost) RestorePartitionedNode() {
	plog.Infof("restored from partition test mode")
	atomic.StoreInt32(&nh.partitioned, 0)
}

// IsPartitioned indicates whether the local node is in partitioned mode. This
// function is only implemented in the monkey test build mode. It always
// returns false in a regular build.
func (nh *NodeHost) IsPartitioned() bool {
	return nh.isPartitioned()
}

// IsPartitioned indicates whether the local node is in partitioned mode.
func (nh *NodeHost) isPartitioned() bool {
	return atomic.LoadInt32(&nh.partitioned) == 1
}
