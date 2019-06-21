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

// +build !dragonboat_slowtest
// +build !dragonboat_monkeytest

package drummer

import (
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/v3/internal/drummer/drummerpb"
)

func TestZeroTickZeroFirstObservedNodeIsConsideredAsDailed(t *testing.T) {
	n := node{
		Tick:          0,
		FirstObserved: 0,
	}
	if !n.failed(1) {
		t.Errorf("node is not reported as failed")
	}
	n.Tick = 1
	if n.failed(1) {
		t.Errorf("node is not expected to be considered as failed")
	}
	n.Tick = 0
	n.FirstObserved = 1
	if n.failed(1) {
		t.Errorf("node is not expected to be considered as failed")
	}
}

func TestZombieNodeWithoutClustedrInfoWillBeReported(t *testing.T) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 100,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)

	ci2 := pb.ClusterInfo{
		ClusterId: 1,
		NodeId:    4,
		Pending:   true,
	}
	nhi2 := pb.NodeHostInfo{
		RaftAddress: "a4",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci2},
	}
	mc.update(nhi2)

	if len(mc.NodesToKill) != 1 {
		t.Fatalf("nodes to kill not updated")
	}

	ntk := mc.NodesToKill[0]
	if ntk.ClusterID != 1 {
		t.Errorf("cluster id %d, want 1", ntk.ClusterID)
	}
	if ntk.NodeID != 4 {
		t.Errorf("node id %d, want 4", ntk.NodeID)
	}
	if ntk.Address != "a4" {
		t.Errorf("address %s, want a4", ntk.Address)
	}
}

func TestZombieNodeWillBeReported(t *testing.T) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 100,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)

	ci2 := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            4,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "a4"},
		ConfigChangeIndex: 50,
	}
	nhi2 := pb.NodeHostInfo{
		RaftAddress: "a4",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci2},
	}
	mc.update(nhi2)

	if len(mc.NodesToKill) != 1 {
		t.Fatalf("nodes to kill not updated")
	}
	ntk := mc.NodesToKill[0]
	if ntk.ClusterID != 1 {
		t.Errorf("cluster id %d, want 1", ntk.ClusterID)
	}
	if ntk.NodeID != 4 {
		t.Errorf("node id %d, want 4", ntk.NodeID)
	}
	if ntk.Address != "a4" {
		t.Errorf("address %s, want a4", ntk.Address)
	}
	gntk := mc.getToKillNodes()
	if len(mc.NodesToKill) != 0 {
		t.Errorf("failed to clear the ntk list")
	}
	if len(gntk) != 1 {
		t.Errorf("failed to get the ntk list")
	}
	grntk := gntk[0]
	if grntk.ClusterID != ntk.ClusterID ||
		grntk.NodeID != ntk.NodeID ||
		grntk.Address != ntk.Address {
		t.Errorf("unexpected value")
	}
}

func TestMultiClusterUpdateCanAddCluster(t *testing.T) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)

	if len(mc.Clusters) != 1 {
		t.Errorf("sz = %d, want %d", len(mc.Clusters), 1)
	}

	v, ok := mc.Clusters[1]
	if !ok {
		t.Error("cluster id suppose to be 1")
	}
	if v.ClusterID != 1 {
		t.Errorf("cluster id %d, want 1", v.ClusterID)
	}
	if v.ConfigChangeIndex != 1 {
		t.Errorf("ConfigChangeIndex = %d, want 1", v.ConfigChangeIndex)
	}
	if len(v.Nodes) != 3 {
		t.Errorf("nodes sz = %d, want 3", len(v.Nodes))
	}
	if v.Nodes[2].Tick != 100 {
		t.Errorf("tick = %d, want 100", v.Nodes[2].Tick)
	}
	if v.Nodes[1].Tick != 0 {
		t.Errorf("tick = %d, want 0", v.Nodes[1].Tick)
	}
	if v.Nodes[1].FirstObserved != 100 {
		t.Errorf("first observed = %d, want 100", v.Nodes[1].FirstObserved)
	}
	if v.Nodes[2].Address != "a2" {
		t.Errorf("address = %s, want a2", v.Nodes[2].Address)
	}
}

func TestMultiClusterUpdateCanUpdateCluster(t *testing.T) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)

	// higher ConfigChangeIndex will be accepted
	uci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          false,
		Nodes:             map[uint64]string{2: "a2", 3: "a3", 4: "a4", 5: "a5"},
		ConfigChangeIndex: 2,
	}
	unhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    200,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{uci},
	}
	mc.update(unhi)
	v := mc.Clusters[1]
	if v.ConfigChangeIndex != 2 {
		t.Errorf("ConfigChangeIndex = %d, want 2", v.ConfigChangeIndex)
	}
	if len(v.Nodes) != 4 {
		t.Errorf("nodes sz = %d, want 4", len(v.Nodes))
	}
	// node 1 expected to be gone
	hasNode1 := false
	for _, n := range v.Nodes {
		if n.NodeID == 1 {
			hasNode1 = true
		}
	}
	if hasNode1 {
		t.Error("node 1 is not deleted")
	}

	if v.Nodes[2].Tick != 200 {
		t.Errorf("tick = %d, want 200", v.Nodes[2].Tick)
	}

	// lower ConfigChangeIndex will be ignored
	uci = pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          false,
		Nodes:             map[uint64]string{1: "a1", 2: "a2"},
		ConfigChangeIndex: 1,
	}
	unhi = pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    200,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{uci},
	}
	mc.update(unhi)
	if v.ConfigChangeIndex != 2 {
		t.Errorf("ConfigChangeIndex = %d, want 2", v.ConfigChangeIndex)
	}
	if len(v.Nodes) != 4 {
		t.Errorf("nodes sz = %d, want 4", len(v.Nodes))
	}
}

func testMultiClusterTickRegionUpdate(t *testing.T,
	pending bool, incomplete bool) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
		Pending:           false,
		Incomplete:        false,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)

	if mc.Clusters[1].Nodes[2].Tick != 100 {
		t.Errorf("tick not set")
	}
	nhi.ClusterInfo[0].Pending = pending
	nhi.ClusterInfo[0].Incomplete = incomplete
	nhi.LastTick = 200
	nhi.Region = "region-new"
	mc.update(nhi)
	if mc.Clusters[1].Nodes[2].Tick != 200 {
		t.Errorf("tick not updated")
	}
}

func TestMultiClusterTickRegionUpdate(t *testing.T) {
	testMultiClusterTickRegionUpdate(t, false, false)
	testMultiClusterTickRegionUpdate(t, false, true)
	testMultiClusterTickRegionUpdate(t, true, false)
}

func TestMultiClusterDeepCopy(t *testing.T) {
	mc := newMultiCluster()
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	mc.update(nhi)
	dcmc := mc.deepCopy()

	mc.Clusters[1].Nodes[2].IsLeader = false
	if !dcmc.Clusters[1].Nodes[2].IsLeader {
		t.Errorf("is leader = %t, want true", dcmc.Clusters[1].Nodes[2].IsLeader)
	}

	// restore the value
	mc.Clusters[1].Nodes[2].IsLeader = true
	if !reflect.DeepEqual(mc, dcmc) {
		t.Error("Deep copied MultiCluster is not reflect.DeepEqual")
	}
}

func TestNodeFailed(t *testing.T) {
	expected := []struct {
		nodeTick          uint64
		nodeFirstObserved uint64
		tick              uint64
		failed            bool
	}{
		{1, 1, 1 + nodeHostTTL, false},
		{1, 1, 2 + nodeHostTTL, true},
	}

	for idx, v := range expected {
		n := node{
			Tick:          v.nodeTick,
			FirstObserved: v.nodeFirstObserved,
		}
		f := n.failed(v.tick)
		if f != v.failed {
			t.Errorf("%d, failed %t, want %t", idx, f, v.failed)
		}
	}
}

func TestNodeWaitingToBeStarted(t *testing.T) {
	expected := []struct {
		nodeTick          uint64
		nodeFirstObserved uint64
		tick              uint64
		wst               bool
	}{
		{0, 1, 1 + 10*nodeHostTTL, true},
		{1, 1, 1 + nodeHostTTL, false},
		{1, 1, nodeHostTTL, false},
	}

	for idx, v := range expected {
		n := node{
			Tick:          v.nodeTick,
			FirstObserved: v.nodeFirstObserved,
		}
		w := n.waitingToBeStarted(v.tick)
		if w != v.wst {
			t.Errorf("%d, wst %t, want %t", idx, w, v.wst)
		}
	}
}

func TestClusterNodesStatus(t *testing.T) {
	base := uint64(100000)
	expected := []struct {
		nodeTick          uint64
		nodeFirstObserved uint64
		failed            bool
		wst               bool
	}{
		{0, base - nodeHostTTL, false, true},
		{0, base - nodeHostTTL - 1, false, true},
		{base - nodeHostTTL - 1, 0, true, false},
		{base - nodeHostTTL, 0, false, false},
	}

	c := &cluster{}
	c.Nodes = make(map[uint64]*node)
	for idx, v := range expected {
		n := &node{
			Tick:          v.nodeTick,
			FirstObserved: v.nodeFirstObserved,
		}

		if n.failed(base) != v.failed {
			t.Errorf("%d, failed %t, want %t", idx, n.failed(base), v.failed)
		}
		if n.waitingToBeStarted(base) != v.wst {
			t.Errorf("%d, wst %t, want %t", idx, n.waitingToBeStarted(base), v.wst)
		}

		c.Nodes[uint64(idx)] = n
	}

	if c.quorum() != 3 {
		t.Errorf("quorum = %d, want 3", c.quorum())
	}
	if len(c.getOkNodes(base)) != 1 {
		t.Errorf("oknode sz = %d, want 1", len(c.getOkNodes(base)))
	}
	if len(c.getFailedNodes(base)) != 1 {
		t.Errorf("failed nodes sz = %d, want 1", len(c.getFailedNodes(base)))
	}
	if len(c.getNodesToStart(base)) != 2 {
		t.Errorf("nodes to start sz = %d, want 2", len(c.getNodesToStart(base)))
	}
	if c.available(base) {
		t.Errorf("available = %t, want false", c.available(base))
	}
}

func TestClusterReqair(t *testing.T) {
	base := uint64(100000)
	expected := []struct {
		nodeTick          uint64
		nodeFirstObserved uint64
		failed            bool
		wst               bool
	}{
		{0, base - nodeHostTTL, false, true},
		{0, base - nodeHostTTL - 1, false, true},
		{base - nodeHostTTL - 1, 0, true, false},
		{base - nodeHostTTL, 0, false, false},
	}

	c := &cluster{}
	c.Nodes = make(map[uint64]*node)
	for idx, v := range expected {
		n := &node{
			Tick:          v.nodeTick,
			FirstObserved: v.nodeFirstObserved,
		}

		if n.failed(base) != v.failed {
			t.Errorf("%d, failed %t, want %t", idx, n.failed(base), v.failed)
		}
		if n.waitingToBeStarted(base) != v.wst {
			t.Errorf("%d, wst %t, want %t", idx, n.waitingToBeStarted(base), v.wst)
		}

		c.Nodes[uint64(idx)] = n
	}

	mc := &multiCluster{}
	mc.Clusters = make(map[uint64]*cluster)
	mc.Clusters[1] = c
	crl := mc.getClusterForRepair(base)
	if len(crl) != 1 {
		t.Errorf("cluster repair sz:%d, want 1", len(crl))
	}

	if crl[0].cluster != c {
		t.Errorf("cluster not pointing to correct struct")
	}

	if len(crl[0].failedNodes) != 1 {
		t.Errorf("failed nodes sz = %d, want 1", len(crl[0].failedNodes))
	}
	if len(crl[0].okNodes) != 1 {
		t.Errorf("ok nodes sz = %d, want 1", len(crl[0].okNodes))
	}
	if len(crl[0].nodesToStart) != 2 {
		t.Errorf("wst nodes sz = %d, want 2", len(crl[0].nodesToStart))
	}

	cr := crl[0]
	if cr.quorum() != 3 {
		t.Errorf("quorum = %d, want 3", cr.quorum())
	}
	if cr.available() {
		t.Errorf("available = %t, want false", cr.available())
	}
	if cr.addRequired() {
		t.Errorf("add required = %t, want false", cr.addRequired())
	}
	if !cr.createRequired() {
		t.Errorf("create required = %t, want true", cr.createRequired())
	}
	if !cr.hasNodesToStart() {
		t.Errorf("has node to start = %t, want true", cr.hasNodesToStart())
	}
}
