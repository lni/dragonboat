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

// +build dragonboat_monkeytest dragonboat_slowtest

package dragonboat

import (
	"sync/atomic"

	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/raftio"
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

func DisableLogUnreachable() {
	logUnreachable = false
}

func SetIncomingProposalsMaxLen(sz uint64) {
	incomingProposalsMaxLen = sz
}

func SetIncomingReadIndexMaxLen(sz uint64) {
	incomingReadIndexMaxLen = sz
}

func SetReceiveQueueSize(sz uint64) {
	receiveQueueSize = sz
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

func (rn *node) GetLastApplied() uint64 {
	return rn.sm.GetLastApplied()
}

func (rn *node) DumpRaftInfoToLog() {
	rn.dumpRaftInfoToLog()
}

func (rn *node) IsLeader() bool {
	return rn.isLeader()
}

func (rn *node) IsFollower() bool {
	return rn.isFollower()
}

func (rn *node) GetStateMachineHash() uint64 {
	return rn.getStateMachineHash()
}

func (rn *node) GetSessionHash() uint64 {
	return rn.getSessionHash()
}

func (rn *node) GetMembershipHash() uint64 {
	return rn.getMembershipHash()
}

func (rn *node) GetRateLimiter() *server.RateLimiter {
	return rn.node.GetRateLimiter()
}

func (rn *node) GetInMemLogSize() uint64 {
	return rn.node.GetInMemLogSize()
}

func (rc *node) getStateMachineHash() uint64 {
	return rc.sm.GetHash()
}

func (rc *node) getSessionHash() uint64 {
	return rc.sm.GetSessionHash()
}

func (rc *node) getMembershipHash() uint64 {
	return rc.sm.GetMembershipHash()
}

func (rc *node) dumpRaftInfoToLog() {
	if rc.node != nil {
		addrMap := make(map[uint64]string)
		nodes, _, _, _ := rc.sm.GetMembership()
		for nodeID := range nodes {
			if nodeID == rc.nodeID {
				addrMap[nodeID] = rc.raftAddress
			} else {
				v, _, err := rc.nodeRegistry.Resolve(rc.clusterID, nodeID)
				if err == nil {
					addrMap[nodeID] = v
				}
			}
		}
		rc.node.DumpRaftInfoToLog(addrMap)
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

// readyToReturnTestKnob is a test knob that returns a boolean value indicating
// whether the system is being shutdown. In production, this function always
// return false without check the stopC chan.
func readyToReturnTestKnob(stopC chan struct{}, pos string) bool {
	if stopC == nil {
		return false
	}
	select {
	case <-stopC:
		plog.Infof("test knob set, returning early before %s", pos)
		return true
	default:
		return false
	}
}
