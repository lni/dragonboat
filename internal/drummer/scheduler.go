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

package drummer

import (
	"errors"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/random"
)

var (
	// errNotEnoughNodeHost indicates that there are not enough node host
	// instances currently available.
	errNotEnoughNodeHost = errors.New("not enough node host")
	unknownRegion        = settings.Soft.UnknownRegionName
)

type scheduler struct {
	server           *server
	randomSrc        random.Source
	config           pb.Config
	clusters         []*pb.Cluster
	tick             uint64
	regions          *pb.Regions
	multiCluster     *multiCluster
	multiNodeHost    *multiNodeHost
	clustersToRepair []clusterRepair
	nodeHostList     []*nodeHostSpec
	nodesToKill      []nodeToKill
}

func newScheduler(server *server, config pb.Config) *scheduler {
	s := &scheduler{
		server:    server,
		config:    config,
		randomSrc: server.randSrc,
	}
	// set these redundant fields to be empty
	s.config.RaftClusterAddresses = ""
	s.config.DrummerAddress = ""
	s.config.DrummerWALDirectory = ""
	s.config.DrummerNodeHostDirectory = ""
	return s
}

func newSchedulerWithContext(server *server,
	config pb.Config, tick uint64, clusters []*pb.Cluster,
	mc *multiCluster, mnh *multiNodeHost) *scheduler {
	s := &scheduler{
		server:           server,
		config:           config,
		clusters:         clusters,
		tick:             tick,
		multiCluster:     mc,
		multiNodeHost:    mnh,
		clustersToRepair: mc.getClusterForRepair(tick),
		nodeHostList:     mnh.toArray(),
	}
	if server == nil {
		s.randomSrc = random.NewLockedRand()
	} else {
		s.randomSrc = server.randSrc
	}
	return s
}

//
// scheduler context
//

func (s *scheduler) updateSchedulerContext(sc *schedulerContext) {
	s.tick = sc.Tick
	s.clusters = make([]*pb.Cluster, 0)
	for _, c := range sc.Clusters {
		s.clusters = append(s.clusters, c)
	}
	s.multiCluster = sc.ClusterImage
	s.multiNodeHost = sc.NodeHostImage
	s.regions = sc.Regions
	s.clustersToRepair = s.multiCluster.getClusterForRepair(s.tick)
	s.nodeHostList = s.multiNodeHost.toArray()
	s.nodesToKill = s.multiCluster.getToKillNodes()
}

func (s *scheduler) hasRunningCluster() bool {
	return s.multiCluster.size() > 0
}

//
// Launch clusters related
//

func (s *scheduler) launch() ([]pb.NodeHostRequest, error) {
	requests, err := s.getLaunchRequests(s.clusters, s.regions)
	if err != nil {
		return nil, err
	}
	return requests, nil
}

func (s *scheduler) getLaunchRequests(clusters []*pb.Cluster,
	regions *pb.Regions) ([]pb.NodeHostRequest, error) {
	result := make([]pb.NodeHostRequest, 0)
	plog.Infof("hosts available for launch requests:")
	for _, nh := range s.nodeHostList {
		plog.Infof("address %s, region %s", nh.Address, nh.Region)
	}
	plog.Infof("regions content %v", regions)
	for _, cluster := range clusters {
		selected := make([]*nodeHostSpec, 0)
		for idx, reg := range regions.Region {
			cnt := int(regions.Count[idx])
			randSelector := newRandomRegionSelector(reg,
				cluster.ClusterId, s.tick, nodeHostTTL, s.randomSrc)
			regionNodes := randSelector.findSuitableNodeHost(s.nodeHostList, cnt)
			if len(regionNodes) != cnt {
				plog.Errorf("failed to get enough node host for cluster %d region %s",
					cluster.ClusterId, reg)
			}
			selected = append(selected, regionNodes...)
		}
		// don't have enough suitable nodes
		if len(selected) < len(cluster.Members) {
			// FIXME: check whether this setup is actually aborted.
			return nil, errors.New("not enough nodehost in suitable regions")
		}
		nodeIDList := make([]uint64, 0)
		addressList := make([]string, 0)
		for idx, m := range cluster.Members {
			nodeIDList = append(nodeIDList, m)
			addressList = append(addressList, selected[idx].Address)
		}
		change := pb.Request{
			Type:      pb.Request_CREATE,
			ClusterId: cluster.ClusterId,
			Members:   nodeIDList,
		}
		for idx, targetNode := range selected {
			req := pb.NodeHostRequest{
				Change:            change,
				NodeIdList:        nodeIDList,
				AddressList:       addressList,
				InstantiateNodeId: cluster.Members[idx],
				RaftAddress:       targetNode.Address,
				Join:              false,
				Restore:           false,
				AppName:           cluster.AppName,
				Config:            s.config,
			}
			result = append(result, req)
		}
	}
	return result, nil
}

//
// Repair clusters related
//

func (s *scheduler) repairClusters(restored map[uint64]struct{}) ([]pb.NodeHostRequest, error) {
	plog.Infof("toRepair sz: %d", len(s.clustersToRepair))
	result := make([]pb.NodeHostRequest, 0)
	for _, cc := range s.clustersToRepair {
		_, ok := restored[cc.clusterID]
		if ok {
			plog.Infof("cluster %s is being restored, skipping repair task",
				cc.describe())
			continue
		}
		plog.Infof("drummer cluster status %s, ok %d failed %d waiting %d",
			cc.describe(), len(cc.okNodes),
			len(cc.failedNodes), len(cc.nodesToStart))
		expectedClusterSize := s.getClusterSize(cc.clusterID)
		plog.Infof("delete required %t, create required %t, add required %t",
			cc.deleteRequired(expectedClusterSize),
			cc.createRequired(), cc.addRequired())
		if cc.deleteRequired(expectedClusterSize) {
			toDeleteNode := cc.failedNodes[0]
			reqs, err := s.getRepairDeleteRequest(toDeleteNode, cc)
			if err != nil {
				return nil, err
			}
			plog.Infof("scheduler generated a delete request for %s on %s",
				toDeleteNode.describe(), toDeleteNode.Address)
			result = append(result, reqs...)
		} else if cc.createRequired() {
			toCreateNode := cc.nodesToStart[0]
			appName := s.getAppName(cc.clusterID)
			reqs, err := s.getRepairCreateRequest(toCreateNode, *cc.cluster, appName)
			if err != nil {
				return nil, err
			}
			plog.Infof("scheduler generated a create request for %s on %s",
				toCreateNode.describe(), toCreateNode.Address)
			result = append(result, reqs...)
		} else if cc.addRequired() {
			failedNode := cc.failedNodes[0]
			reqs, err := s.getRepairAddRequest(failedNode, cc)
			if err != nil {
				return nil, err
			}
			plog.Infof("scheduler generated a add request for failed node %s on %s,"+
				"new node id %d, new node address %s, target nodehost %s",
				failedNode.describe(),
				failedNode.Address,
				reqs[0].Change.Members[0],
				reqs[0].AddressList[0],
				reqs[0].RaftAddress)
			result = append(result, reqs...)
		}
	}
	return result, nil
}

// for an identified failed node, get an ADD request to add a new node
func (s *scheduler) getRepairAddRequest(failedNode node,
	ctr clusterRepair) ([]pb.NodeHostRequest, error) {
	selected := s.getReplacementNode(failedNode)
	if len(selected) != 1 {
		plog.Warningf("failed to find a replacement node for the failed node %s",
			failedNode.describe())
		return nil, errNotEnoughNodeHost
	}
	selectedNode := selected[0]
	okNodeCount := len(ctr.okNodes)
	existingMemberAddress := ctr.okNodes[s.randomSrc.Int()%okNodeCount].Address
	newNodeID := s.randomSrc.Uint64()
	change := pb.Request{
		Type:         pb.Request_ADD,
		ClusterId:    ctr.clusterID,
		Members:      []uint64{newNodeID},
		ConfChangeId: ctr.cluster.ConfigChangeIndex,
	}
	req := pb.NodeHostRequest{
		Change:      change,
		RaftAddress: existingMemberAddress,
		AddressList: []string{selectedNode.Address},
	}
	return []pb.NodeHostRequest{req}, nil
}

func (s *scheduler) getReplacementNode(failedNode node) []*nodeHostSpec {
	// see whether we can find one in the same region
	var region string
	nodeHostSpec, ok := s.multiNodeHost.Nodehosts[failedNode.Address]
	if !ok {
		plog.Errorf("failed to get region")
		region = unknownRegion
	} else {
		region = nodeHostSpec.Region
	}
	regionSelector := newRandomRegionSelector(region,
		failedNode.ClusterID, s.tick, nodeHostTTL, s.randomSrc)
	selected := regionSelector.findSuitableNodeHost(s.nodeHostList, 1)
	if len(selected) == 1 {
		return selected
	}
	plog.Warningf("failed to find a nodehost in region %s", region)
	// get a random one
	randSelector := newRandomSelector(failedNode.ClusterID,
		s.tick, nodeHostTTL, s.randomSrc)
	return randSelector.findSuitableNodeHost(s.nodeHostList, 1)
}

// start the node which was previously added to the raft cluster
func (s *scheduler) getRepairCreateRequest(newNode node,
	ci cluster, appName string) ([]pb.NodeHostRequest, error) {
	return s.getCreateRequest(newNode, ci, appName, true, false)
}

// get the request to delete the specified node from the raft cluster
func (s *scheduler) getRepairDeleteRequest(nodeToDelete node,
	ctr clusterRepair) ([]pb.NodeHostRequest, error) {
	okNodeCount := len(ctr.okNodes)
	existingMemberAddress := ctr.okNodes[s.randomSrc.Int()%okNodeCount].Address
	change := pb.Request{
		Type:         pb.Request_DELETE,
		ClusterId:    ctr.clusterID,
		Members:      []uint64{nodeToDelete.NodeID},
		ConfChangeId: ctr.cluster.ConfigChangeIndex,
	}
	req := pb.NodeHostRequest{
		Change:      change,
		RaftAddress: existingMemberAddress,
	}
	return []pb.NodeHostRequest{req}, nil
}

// there are multiple reasons why we need such a local kill request to
// remove the specified node from the nodehost on the receiving end -
// 1. in the current raft implementation, there is no guarantee that
//    the node being deleted will be removed from its hosting nodehost.
//    consider the situation in which the remote object for the node being
//    deleted is removed from the leader before the delete config change
//    entry can be replicated onto the node being deleted.
// 2. drummer CREATE requests can be sitting in the pipeline for a while
//		when the selected nodehost to start this new node is down. drummer
//    might delete the node before the nodehost picks up the CREATE request
//    and start the node.
// in both of these two situations, the concerned nodes becomes so called
// zombie, it is not going to cause real trouble for the raft cluster,
// but having it hanging around is not that desirable.
func (s *scheduler) killZombieNodes() []pb.NodeHostRequest {
	results := make([]pb.NodeHostRequest, 0)
	for _, ntk := range s.nodesToKill {
		req := s.getKillRequest(ntk.ClusterID, ntk.NodeID, ntk.Address)
		results = append(results, req)
		plog.Infof("scheduler generated a kill request for %s on %s",
			logutil.DescribeNode(ntk.ClusterID, ntk.NodeID), ntk.Address)
	}
	return results
}

func (s *scheduler) getKillRequest(clusterID uint64,
	nodeID uint64, addr string) pb.NodeHostRequest {
	kc := pb.Request{
		Type:      pb.Request_KILL,
		ClusterId: clusterID,
		Members:   []uint64{nodeID},
	}
	killreq := pb.NodeHostRequest{
		Change:      kc,
		RaftAddress: addr,
	}
	return killreq
}

func (s *scheduler) getCreateRequest(newNode node,
	ci cluster, appName string, join bool,
	restore bool) ([]pb.NodeHostRequest, error) {
	nodeIDList := make([]uint64, 0)
	addressList := make([]string, 0)
	for _, n := range ci.Nodes {
		nodeIDList = append(nodeIDList, n.NodeID)
		addressList = append(addressList, n.Address)
	}
	change := pb.Request{
		Type:      pb.Request_CREATE,
		ClusterId: ci.ClusterID,
		Members:   nodeIDList,
	}
	req := pb.NodeHostRequest{
		Change:            change,
		NodeIdList:        nodeIDList,
		AddressList:       addressList,
		InstantiateNodeId: newNode.NodeID,
		RaftAddress:       newNode.Address,
		Join:              join,
		Restore:           restore,
		AppName:           appName,
		Config:            s.config,
	}
	return []pb.NodeHostRequest{req}, nil
}

//
// Restore unavailable clusters related
//
func (s *scheduler) restore() ([]pb.NodeHostRequest, error) {
	ureqs, done, err := s.restoreUnavailableClusters()
	if err != nil {
		return nil, err
	}
	reqs, err := s.restoreFailed(done)
	if err != nil {
		return nil, err
	}
	return append(ureqs, reqs...), nil
}

func (s *scheduler) restoreUnavailableClusters() ([]pb.NodeHostRequest,
	map[uint64]struct{}, error) {
	result := make([]pb.NodeHostRequest, 0)
	idMap := make(map[uint64]struct{})
	for _, cc := range s.clustersToRepair {
		if !cc.needToBeRestored() {
			continue
		}
		nodesToRestore, ok := cc.readyToBeRestored(s.multiNodeHost, s.tick)
		if ok {
			plog.Infof("cluster %s is unavailable but can be restored (%d)",
				cc.describe(), len(nodesToRestore))
			for _, toRestoreNode := range nodesToRestore {
				appName := s.getAppName(cc.clusterID)
				reqs, err := s.getRestoreCreateRequest(toRestoreNode,
					*cc.cluster, appName)
				if err != nil {
					plog.Warningf("failed to get restore request %v", err)
					return nil, nil, err
				}
				plog.Infof("scheduler created a restore requeset for %s on %s",
					toRestoreNode.describe(), toRestoreNode.Address)
				result = append(result, reqs...)
				idMap[cc.clusterID] = struct{}{}
			}
		} else {
			plog.Warningf("cluster %s is unavailable and can not be restored",
				cc.describe())
			cc.logUnableToRestoreCluster(s.multiNodeHost, s.tick)
		}
	}
	return result, idMap, nil
}

func (s *scheduler) restoreFailed(done map[uint64]struct{}) ([]pb.NodeHostRequest,
	error) {
	result := make([]pb.NodeHostRequest, 0)
	for _, cc := range s.clustersToRepair {
		_, ok := done[cc.clusterID]
		if cc.needToBeRestored() || ok {
			continue
		}
		nodesToRestore := cc.canBeRestored(s.multiNodeHost, s.tick)
		plog.Infof("cluster %s is unavailable but can be restored (%d)",
			cc.describe(), len(nodesToRestore))
		for _, toRestoreNode := range nodesToRestore {
			appName := s.getAppName(cc.clusterID)
			reqs, err := s.getRestoreCreateRequest(toRestoreNode,
				*cc.cluster, appName)
			if err != nil {
				return nil, err
			}
			plog.Infof("scheduler created a restore requeset for %s on %s",
				toRestoreNode.describe(), toRestoreNode.Address)
			result = append(result, reqs...)
		}
	}
	if len(result) > 0 {
		plog.Infof("trying to restore %d failed nodes", len(result))
	}
	return result, nil
}

func (s *scheduler) getRestoreCreateRequest(newNode node,
	ci cluster, appName string) ([]pb.NodeHostRequest, error) {
	return s.getCreateRequest(newNode, ci, appName, false, true)
}

//
// helper functions
//

func (s *scheduler) getClusterSize(clusterID uint64) int {
	for _, c := range s.clusters {
		if c.ClusterId == clusterID {
			return len(c.Members)
		}
	}
	panic("failed to locate the cluster")
}

func (s *scheduler) getAppName(clusterID uint64) string {
	for _, c := range s.clusters {
		if c.ClusterId == clusterID {
			return c.AppName
		}
	}
	panic("failed to locate the cluster")
}
