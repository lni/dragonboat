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

// +build dragonboat_monkeytest dragonboat_slowtest

package dragonboat

import (
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/dragonboat/v3/raftio"
)

//
// code here is used in testing only.
//

// Clusters returns a list of raft nodes managed by the nodehost instance.
func (nh *NodeHost) Clusters() []*node {
	result := make([]*node, 0)
	nh.clusterMu.RLock()
	nh.clusterMu.clusters.Range(func(k, v interface{}) bool {
		result = append(result, v.(*node))
		return true
	})
	nh.clusterMu.RUnlock()

	return result
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

func (nh *NodeHost) SetTransportDropBatchHook(f transport.SendMessageBatchFunc) {
	nh.transport.(*transport.Transport).SetPreSendMessageBatchHook(f)
}

func (nh *NodeHost) SetPreStreamChunkSendHook(f transport.StreamChunkSendFunc) {
	nh.transport.(*transport.Transport).SetPreStreamChunkSendHook(f)
}

func (nh *NodeHost) GetLogDB() raftio.ILogDB {
	return nh.logdb
}

func (n *node) GetLastApplied() uint64 {
	return n.sm.GetLastApplied()
}

func (n *node) DumpRaftInfoToLog() {
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

func (n *node) GetRateLimiter() *server.RateLimiter {
	return n.p.GetRateLimiter()
}

func (n *node) GetInMemLogSize() uint64 {
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
	if n.p != nil {
		addrMap := make(map[uint64]string)
		nodes, _, _, _ := n.sm.GetMembership()
		for nodeID := range nodes {
			if nodeID == n.nodeID {
				addrMap[nodeID] = n.raftAddress
			} else {
				v, _, err := n.nodeRegistry.Resolve(n.clusterID, nodeID)
				if err == nil {
					addrMap[nodeID] = v
				}
			}
		}
		n.p.DumpRaftInfoToLog(addrMap)
	}
}

// Set how many snapshot worker to use in step engine. This function is
// expected to be called only during monkeytest, it doesn't exist when the
// monkeytest tag is not set.
func SetSnapshotWorkerCount(count uint64) {
	snapshotWorkerCount = count
}

// Set how many worker to use in step engine. This function is
// expected to be called only during monkeytest, it doesn't exist when the
// monkeytest tag is not set.
func SetWorkerCount(count uint64) {
	workerCount = count
}

// Set how many task worker to use in step engine. This function is
// expected to be called only during monkeytest, it doesn't exist when the
// monkeytest tag is not set.
func SetTaskWorkerCount(count uint64) {
	taskWorkerCount = count
}

// testParitionState struct is used to manage the state whether nodehost is in
// network partitioned state during testing. nodehost is in completely
// isolated state without any connectivity to the outside world when in such
// partitioned state. this is the actual implementation used in monkey tests.
type testPartitionState struct {
	testPartitioned uint32
}

// PartitionNode puts the node into test partition mode. All connectivity to
// the outside world should be stopped.
func (p *testPartitionState) PartitionNode() {
	plog.Infof("entered partition test mode")
	atomic.StoreUint32(&p.testPartitioned, 1)
}

// RestorePartitionedNode removes the node from test partition mode. No other
// change is going to be made on the local node. It is up to the local node it
// self to repair/restore any other state.
func (p *testPartitionState) RestorePartitionedNode() {
	atomic.StoreUint32(&p.testPartitioned, 0)
	plog.Infof("restored from partition test mode")
}

// IsPartitioned indicates whether the local node is in partitioned mode. This
// function is only implemented in the monkey test build mode. It always
// returns false in a regular build.
func (p *testPartitionState) IsPartitioned() bool {
	return p.isPartitioned()
}

// IsPartitioned indicates whether the local node is in partitioned mode.
func (p *testPartitionState) isPartitioned() bool {
	return atomic.LoadUint32(&p.testPartitioned) == 1
}
