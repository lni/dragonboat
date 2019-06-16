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
	"math/rand"
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/random"
)

func getCluster() []*pb.Cluster {
	c := &pb.Cluster{
		Members:   []uint64{1, 2, 3},
		ClusterId: 100,
		AppName:   "noop",
	}
	return []*pb.Cluster{c}
}

type testNodeInfo struct {
	address     string
	tick        uint64
	region      string
	hasCluster  bool
	clusterID   uint64
	nodeID      uint64
	allNodes    []uint64
	addressList []string
}

func getTestNodeHostInfo(nhi []testNodeInfo) []pb.NodeHostInfo {
	result := make([]pb.NodeHostInfo, 0)
	for _, n := range nhi {
		nh := pb.NodeHostInfo{
			RaftAddress: n.address,
			LastTick:    n.tick,
			Region:      n.region,
		}
		if n.hasCluster {
			ci := pb.ClusterInfo{
				ClusterId: n.clusterID,
				NodeId:    n.nodeID,
			}
			nodeMap := make(map[uint64]string)
			for idx, nid := range n.allNodes {
				nodeMap[nid] = n.addressList[idx]
			}
			ci.Nodes = nodeMap
			nh.ClusterInfo = make([]pb.ClusterInfo, 0)
			nh.ClusterInfo = append(nh.ClusterInfo, ci)
		}
		result = append(result, nh)
	}
	return result
}

func getTestMultiCluster(nhList []pb.NodeHostInfo) *multiCluster {
	mc := newMultiCluster()
	for _, nh := range nhList {
		mc.update(nh)
	}
	return mc
}

func getTestMultiNodeHost(nhList []pb.NodeHostInfo) *multiNodeHost {
	mnh := newMultiNodeHost()
	for _, nh := range nhList {
		mnh.update(nh)
	}
	return mnh
}

//
// without any running cluster
//
func getNodeHostInfoListWithoutAnyCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 100, "region-1", false, 0, 0, nil, nil},
		{"a2", 100, "region-2", false, 0, 0, nil, nil},
		{"a3", 100, "region-3", false, 0, 0, nil, nil},
		{"a4", 100, "region-4", false, 0, 0, nil, nil},
	}
	return getTestNodeHostInfo(nhi)
}

//
// with one just restarted node, no node on that restarted node
//

func getNodeHostInfoListWithOneRestartedNode() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a2", 500, "region-2", true, 100, 2, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a3", 500, "region-3", false, 0, 0, nil, nil},
		{"a4", 500, "region-4", false, 0, 0, nil, nil},
	}
	nh := getTestNodeHostInfo(nhi)
	nh[2].PlogInfoIncluded = true
	nh[2].PlogInfo = append(nh[2].PlogInfo, pb.LogInfo{ClusterId: 100, NodeId: 3})
	return nh
}

//
// with one failed node and there was cluster on that node.
//

func getNodeHostInfoListWithOneFailedCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a2", 500, "region-2", true, 100, 2, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a3", 100, "region-3", true, 100, 3, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a4", 500, "region-4", false, 0, 0, nil, nil},
	}
	return getTestNodeHostInfo(nhi)
}

//
// with one added node
//

func getNodeHostInfoListWithOneAddedCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a2", 500, "region-2", true, 100, 2, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a3", 100, "region-3", true, 100, 3, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a4", 500, "region-4", false, 0, 0, nil, nil},
	}
	return getTestNodeHostInfo(nhi)
}

//
// with one ready to be deleted
//

func getNodeHostInfoListWithOneReadyForDeleteCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a2", 500, "region-2", true, 100, 2, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a3", 100, "region-3", true, 100, 3, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
		{"a4", 500, "region-4", true, 100, 4, []uint64{1, 2, 3, 4}, []string{"a1", "a2", "a3", "a4"}},
	}
	return getTestNodeHostInfo(nhi)
}

//
// Restore cluster
//

func getNodeHostListWithUnavailableCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a2", 100, "region-2", true, 100, 2, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a3", 100, "region-3", true, 100, 3, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a4", 500, "region-4", false, 0, 0, nil, nil},
	}
	return getTestNodeHostInfo(nhi)
}

func getNodeHostListWithReadyToBeRestoredCluster() []pb.NodeHostInfo {
	nhi := []testNodeInfo{
		{"a1", 500, "region-1", true, 100, 1, []uint64{1, 2, 3}, []string{"a1", "a2", "a3"}},
		{"a2", 500, "region-2", false, 0, 0, nil, nil},
		{"a3", 500, "region-3", false, 0, 0, nil, nil},
		{"a4", 500, "region-4", false, 0, 0, nil, nil},
	}
	results := getTestNodeHostInfo(nhi)
	// add LogInfo
	results[1].PlogInfoIncluded = true
	results[1].PlogInfo = append(results[1].PlogInfo, pb.LogInfo{ClusterId: 100, NodeId: 2})
	results[2].PlogInfoIncluded = true
	results[2].PlogInfo = append(results[2].PlogInfo, pb.LogInfo{ClusterId: 100, NodeId: 3})
	return results
}

//
// other helpers for testing
//

func getRequestedRegion() pb.Regions {
	r := pb.Regions{
		Region: []string{"region-1", "region-2", "region-3"},
		Count:  []uint64{1, 1, 1},
	}
	return r
}

func stringIn(val string, list []string) bool {
	for _, v := range list {
		if v == val {
			return true
		}
	}
	return false
}

func uint64In(val uint64, list []uint64) bool {
	for _, v := range list {
		if v == val {
			return true
		}
	}
	return false
}

func TestSchedulerLaunchRequest(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(100)
	nhList := getNodeHostInfoListWithoutAnyCluster()
	mnh := getTestMultiNodeHost(nhList)
	mc := getTestMultiCluster(nhList)
	clusters := getCluster()
	regions := getRequestedRegion()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	reqs, err := s.getLaunchRequests(clusters, &regions)
	if err != nil {
		t.Errorf("expected to get launch request, error returned: %s", err.Error())
	}
	if len(reqs) != 3 {
		t.Errorf("len(reqs)=%d, want 3", len(reqs))
	}
	for _, req := range reqs {
		if req.Change.Type != pb.Request_CREATE {
			t.Errorf("change type %d, want %d", req.Change.Type, pb.Request_CREATE)
		}
		if req.Change.ClusterId != 100 {
			t.Errorf("cluster id %d, want 100", req.Change.ClusterId)
		}
		if len(req.Change.Members) != 3 {
			t.Errorf("len(req.Members)=%d, want 3", len(req.Change.Members))
		}
		if len(req.NodeIdList) != 3 {
			t.Errorf("len(req.NodeIdList)=%d, want 3", len(req.NodeIdList))
		}
		if len(req.AddressList) != 3 {
			t.Errorf("len(req.Address)=%d, want 3", len(req.AddressList))
		}
		if !stringIn(req.RaftAddress, []string{"a1", "a2", "a3"}) {
			t.Errorf("unexpected address selected %s", req.RaftAddress)
		}
		if !uint64In(req.InstantiateNodeId, []uint64{1, 2, 3}) {
			t.Errorf("unexpected node id selected %d", req.InstantiateNodeId)
		}
		for _, addr := range req.AddressList {
			if !stringIn(addr, []string{"a1", "a2", "a3"}) {
				t.Error("unexpected to be included")
			}
		}
		for _, nid := range req.NodeIdList {
			if !uint64In(nid, []uint64{1, 2, 3}) {
				t.Error("unexpected to be included")
			}
		}
	}
}

func TestSchedulerAddRequestDuringRepair(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(500)
	nhList := getNodeHostInfoListWithOneFailedCluster()
	mnh := getTestMultiNodeHost(nhList)
	mc := getTestMultiCluster(nhList)
	mnh.syncClusterInfo(mc)
	clusters := getCluster()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	if len(s.clustersToRepair) != 1 {
		t.Errorf("len(s.clustersToRepair)=%d, want 1", len(s.clustersToRepair))
	}
	ignoreClusters := make(map[uint64]struct{})
	reqs, err := s.repairClusters(ignoreClusters)
	if err != nil {
		t.Errorf("error not expected")
	}
	if len(reqs) != 1 {
		t.Errorf("len(reqs)=%d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Change.Type != pb.Request_ADD {
		t.Errorf("type %d, want %d", req.Change.Type, pb.Request_ADD)
	}
	if req.Change.ClusterId != 100 {
		t.Errorf("cluster id %d, want 100", req.Change.ClusterId)
	}
	if len(req.Change.Members) != 1 {
		t.Errorf("len(req.Change.Members)=%d, want 1", len(req.Change.Members))
	}
	if uint64In(req.Change.Members[0], []uint64{1, 2, 3}) {
		t.Errorf("re-used node id")
	}
	if req.RaftAddress != "a1" && req.RaftAddress != "a2" {
		t.Errorf("not sending to existing member, address %s", req.RaftAddress)
	}
	if req.AddressList[0] != "a4" {
		t.Errorf("not selecting expected node, %s", req.AddressList[0])
	}
}

func TestSchedulerCreateRequestDuringRepair(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(500)
	nhList := getNodeHostInfoListWithOneAddedCluster()
	mnh := getTestMultiNodeHost(nhList)
	mc := getTestMultiCluster(nhList)
	clusters := getCluster()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	if len(s.clustersToRepair) != 1 {
		t.Errorf("len(s.clustersToRepair)=%d, want 1", len(s.clustersToRepair))
	}
	ignoreClusters := make(map[uint64]struct{})
	reqs, err := s.repairClusters(ignoreClusters)
	if err != nil {
		t.Errorf("error not expected")
	}
	if len(reqs) != 1 {
		t.Errorf("len(reqs)=%d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Change.Type != pb.Request_CREATE {
		t.Errorf("type %d, want %d", req.Change.Type, pb.Request_CREATE)
	}
	if req.Change.ClusterId != 100 {
		t.Errorf("cluster id %d, want 100", req.Change.ClusterId)
	}
	if len(req.Change.Members) != 4 {
		t.Errorf("len(req.Change.Members)=%d, want 4", len(req.Change.Members))
	}
	expectedNodeID := []uint64{1, 2, 3, 4}
	if !uint64In(1, expectedNodeID) || !uint64In(2, expectedNodeID) ||
		!uint64In(3, expectedNodeID) || !uint64In(4, expectedNodeID) {
		t.Errorf("unexpected node id list")
	}
	if req.AppName != "noop" {
		t.Errorf("app name %s, want noop", req.AppName)
	}
	if req.InstantiateNodeId != 4 {
		t.Errorf("req.InstantiateNodeId=%d, want 4", req.InstantiateNodeId)
	}
	if req.RaftAddress != "a4" {
		t.Errorf("raft address=%s, want a4", req.RaftAddress)
	}
	if !reflect.DeepEqual(req.NodeIdList, req.Change.Members) {
		t.Errorf("expect req.NodeIdList == req.Change.Members")
	}
	if len(req.AddressList) != 4 {
		t.Errorf("len(req.AddressList)=%d, want 4", len(req.AddressList))
	}
	if !stringIn("a1", req.AddressList) || !stringIn("a2", req.AddressList) ||
		!stringIn("a3", req.AddressList) || !stringIn("a4", req.AddressList) {
		t.Errorf("unexpected address list element")
	}
}

func TestSchedulerDeleteRequestDuringRepair(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(500)
	nhList := getNodeHostInfoListWithOneReadyForDeleteCluster()
	mnh := getTestMultiNodeHost(nhList)
	mc := getTestMultiCluster(nhList)
	clusters := getCluster()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	if len(s.clustersToRepair) != 1 {
		t.Errorf("len(s.clustersToRepair)=%d, want 1", len(s.clustersToRepair))
	}
	ignoreClusters := make(map[uint64]struct{})
	reqs, err := s.repairClusters(ignoreClusters)
	if err != nil {
		t.Errorf("error not expected")
	}
	if len(reqs) != 1 {
		t.Errorf("len(reqs)=%d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Change.Type != pb.Request_DELETE {
		t.Errorf("type %d, want %d", req.Change.Type, pb.Request_CREATE)
	}
	if req.Change.ClusterId != 100 {
		t.Errorf("cluster id %d, want 100", req.Change.ClusterId)
	}
	if len(req.Change.Members) != 1 {
		t.Errorf("len(req.Change.Members)=%d, want 1", len(req.Change.Members))
	}
	if req.Change.Members[0] != 3 {
		t.Errorf("to delete node id %d, want 3", req.Change.Members[0])
	}
	if req.RaftAddress != "a1" && req.RaftAddress != "a2" && req.RaftAddress != "a4" {
		t.Errorf("req.RaftAddress=%s, want a1, a2 or a4", req.RaftAddress)
	}
}

func TestRestoreUnavailableCluster(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(500)
	nhList := getNodeHostListWithUnavailableCluster()
	mc := getTestMultiCluster(nhList)
	newNhList := getNodeHostListWithReadyToBeRestoredCluster()
	mnh := getTestMultiNodeHost(newNhList)
	clusters := getCluster()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	reqs, err := s.restore()
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(reqs) != 2 {
		t.Errorf("reqs sz: %d, want 2", len(reqs))
	}
	for idx := 0; idx < 2; idx++ {
		if reqs[idx].Change.Type != pb.Request_CREATE {
			t.Errorf("change type %d, want %d", reqs[idx].Change.Type, pb.Request_CREATE)
		}
		if reqs[idx].Change.ClusterId != 100 {
			t.Errorf("cluster id %d, want 100", reqs[idx].Change.ClusterId)
		}
		if len(reqs[idx].Change.Members) != 3 {
			t.Errorf("member sz: %d, want 3", len(reqs[idx].Change.Members))
		}
		if reqs[idx].InstantiateNodeId != 2 && reqs[idx].InstantiateNodeId != 3 {
			t.Errorf("new id %d, want 2 or 3", reqs[idx].InstantiateNodeId)
		}
		if reqs[idx].RaftAddress != "a2" && reqs[idx].RaftAddress != "a3" {
			t.Errorf("new address %s, want a2 or a3", reqs[idx].RaftAddress)
		}
		if len(reqs[idx].NodeIdList) != 3 || len(reqs[idx].AddressList) != 3 {
			t.Errorf("cluster info incomplete")
		}
	}
}

func TestRestoreFailedCluster(t *testing.T) {
	config := GetClusterConfig()
	tick := uint64(500)
	nhList := getNodeHostInfoListWithOneFailedCluster()
	mc := getTestMultiCluster(nhList)
	newNhList := getNodeHostInfoListWithOneRestartedNode()
	mnh := getTestMultiNodeHost(newNhList)
	clusters := getCluster()
	s := newSchedulerWithContext(nil, config, tick, clusters, mc, mnh)
	reqs, err := s.restore()
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(s.clustersToRepair) != 1 {
		t.Fatalf("suppose to have one cluster need to be repaired")
	}
	if s.clustersToRepair[0].needToBeRestored() {
		t.Errorf("not suppose to be marked as needToBeRestored")
	}
	if len(reqs) != 1 {
		t.Errorf("got %d reqs, want 1", len(reqs))
	}
	if reqs[0].Change.Type != pb.Request_CREATE {
		t.Errorf("expected to have a CREATE request")
	}
	if reqs[0].Change.ClusterId != 100 {
		t.Errorf("cluster id %d, want 100", reqs[0].Change.ClusterId)
	}
	if len(reqs[0].Change.Members) != 3 {
		t.Errorf("member sz: %d, want 3", len(reqs[0].Change.Members))
	}
	if reqs[0].InstantiateNodeId != 3 {
		t.Errorf("new id %d, want 3", reqs[0].InstantiateNodeId)
	}
	if reqs[0].RaftAddress != "a3" {
		t.Errorf("new address %s, want a3", reqs[0].RaftAddress)
	}
	if len(reqs[0].NodeIdList) != 3 || len(reqs[0].AddressList) != 3 {
		t.Errorf("cluster info incomplete")
	}
}

func TestKillZombieNodes(t *testing.T) {
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
	s := &scheduler{
		nodesToKill: mc.getToKillNodes(),
	}
	reqs := s.killZombieNodes()
	if len(reqs) != 1 {
		t.Fatalf("failed to generate kill zombie request")
	}
	req := reqs[0]
	if req.Change.Type != pb.Request_KILL {
		t.Errorf("unexpected type %s", req.Change.Type)
	}
	if req.Change.ClusterId != 1 {
		t.Errorf("cluster id %d, want 1", req.Change.ClusterId)
	}
	if req.Change.Members[0] != 4 {
		t.Errorf("node id %d, want 4", req.Change.Members[0])
	}
	if req.RaftAddress != "a4" {
		t.Errorf("address %s, want a4", req.RaftAddress)
	}
}

func TestLaunchRequestsForLargeNumberOfClustersCanBeDelivered(t *testing.T) {
	numOfClusters := 10000
	cl := make([]*pb.Cluster, 0)
	for i := 0; i < numOfClusters; i++ {
		c := &pb.Cluster{
			AppName:   random.String(32),
			ClusterId: rand.Uint64(),
			Members: []uint64{
				rand.Uint64(),
				rand.Uint64(),
				rand.Uint64(),
				rand.Uint64(),
				rand.Uint64(),
			},
		}
		cl = append(cl, c)
	}
	regions := pb.Regions{
		Region: []string{
			random.String(64),
			random.String(64),
			random.String(64),
			random.String(64),
			random.String(64),
		},
		Count: []uint64{1, 1, 1, 1, 1},
	}
	nhList := make([]*nodeHostSpec, 0)
	for i := 0; i < 5; i++ {
		nh := &nodeHostSpec{
			Address:    random.String(260),
			RPCAddress: random.String(260),
			Region:     regions.Region[i],
			Clusters:   make(map[uint64]struct{}),
		}
		nhList = append(nhList, nh)
	}
	s := scheduler{}
	s.randomSrc = random.NewLockedRand()
	s.nodeHostList = nhList
	s.config = getDefaultClusterConfig()
	reqs, err := s.getLaunchRequests(cl, &regions)
	if err != nil {
		t.Errorf("failed to get launch requests, %v", err)
	}
	for i := 0; i < 5; i++ {
		addr := nhList[i].Address
		selected := make([]pb.NodeHostRequest, 0)
		for _, req := range reqs {
			if req.RaftAddress == addr {
				selected = append(selected, req)
			}
		}
		rc := pb.NodeHostRequestCollection{
			Requests: selected,
		}
		sz := uint64(rc.Size())
		plog.Infof("for nodehost %d, size : %d", i, sz)
		if sz > settings.Soft.MaxDrummerServerMsgSize ||
			sz > settings.Soft.MaxDrummerClientMsgSize {
			t.Errorf("message is too big to be delivered")
		}
	}
}
