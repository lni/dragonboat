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
	"sort"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
)

var (
	// nodeHostTTL defines the number of seconds without heartbeat required to
	// consider a node host as dead.
	nodeHostTTL = settings.Soft.NodeHostTTL
)

type node struct {
	ClusterID     uint64
	NodeID        uint64
	Address       string
	IsLeader      bool
	Tick          uint64
	FirstObserved uint64
}

type cluster struct {
	ClusterID         uint64
	ConfigChangeIndex uint64
	Nodes             map[uint64]*node
}

type multiCluster struct {
	Clusters    map[uint64]*cluster
	NodesToKill []nodeToKill
}

type clusterRepair struct {
	clusterID    uint64
	cluster      *cluster
	failedNodes  []node
	okNodes      []node
	nodesToStart []node
}

type nodeToKill struct {
	ClusterID uint64
	NodeID    uint64
	Address   string
}

func (cr *clusterRepair) describe() string {
	return logutil.ClusterID(cr.clusterID)
}

func (cr *clusterRepair) quorum() int {
	count := len(cr.failedNodes) + len(cr.okNodes) + len(cr.nodesToStart)
	return count/2 + 1
}

func (cr *clusterRepair) available() bool {
	return len(cr.okNodes) >= cr.quorum()
}

func (cr *clusterRepair) addRequired() bool {
	return len(cr.failedNodes) > 0 &&
		len(cr.nodesToStart) == 0 && cr.available()
}

func (cr *clusterRepair) createRequired() bool {
	return len(cr.nodesToStart) > 0
}

func (cr *clusterRepair) deleteRequired(expectedClusterSize int) bool {
	return cr.available() && len(cr.failedNodes) > 0 &&
		len(cr.failedNodes)+len(cr.okNodes) > expectedClusterSize
}

func (cr *clusterRepair) hasNodesToStart() bool {
	return len(cr.nodesToStart) > 0
}

func (cr *clusterRepair) needToBeRestored() bool {
	if cr.available() || len(cr.nodesToStart) > 0 {
		return false
	}
	return true
}

func (cr *clusterRepair) readyToBeRestored(mnh *multiNodeHost,
	currentTick uint64) ([]node, bool) {
	if !cr.needToBeRestored() {
		return nil, false
	}
	nl := make([]node, 0)
	for _, n := range cr.failedNodes {
		spec, ok := mnh.Nodehosts[n.Address]
		if ok && spec.available(currentTick) && spec.hasLog(n.ClusterID, n.NodeID) {
			nl = append(nl, n)
		}
	}
	return nl, len(cr.okNodes)+len(nl) >= cr.quorum()
}

func (cr *clusterRepair) canBeRestored(mnh *multiNodeHost,
	currentTick uint64) []node {
	nl := make([]node, 0)
	for _, n := range cr.failedNodes {
		spec, ok := mnh.Nodehosts[n.Address]
		if ok && spec.available(currentTick) && spec.hasLog(n.ClusterID, n.NodeID) {
			nl = append(nl, n)
		}
	}
	return nl
}

func (cr *clusterRepair) logUnableToRestoreCluster(mnh *multiNodeHost,
	currentTick uint64) {
	for _, n := range cr.failedNodes {
		spec, ok := mnh.Nodehosts[n.Address]
		if ok {
			available := spec.available(currentTick)
			hasLog := spec.hasLog(n.ClusterID, n.NodeID)
			plog.Debugf("node %s available: %t", n.Address, available)
			plog.Debugf("node %s hasLog for %s: %t", n.Address,
				logutil.DescribeNode(n.ClusterID, n.NodeID), hasLog)
		} else {
			plog.Debugf("failed node %s not in mnh.nodehosts", n.Address)
		}
	}
}

// EntityFailed returns whether the timeline indicates that the entity has
// failed.
func EntityFailed(lastTick uint64, currentTick uint64) bool {
	return currentTick-lastTick > nodeHostTTL
}

func (n *node) failed(tick uint64) bool {
	if n.Tick == 0 {
		return n.FirstObserved == 0
	}
	return EntityFailed(n.Tick, tick)
}

func (n *node) describe() string {
	return logutil.DescribeNode(n.ClusterID, n.NodeID)
}

func (n *node) waitingToBeStarted(tick uint64) bool {
	return n.Tick == 0 && !n.failed(tick)
}

func (c *cluster) describe() string {
	return logutil.ClusterID(c.ClusterID)
}

func (c *cluster) quorum() int {
	return len(c.Nodes)/2 + 1
}

func (c *cluster) available(currentTick uint64) bool {
	return len(c.getOkNodes(currentTick)) >= c.quorum()
}

func (c *cluster) getOkNodes(tick uint64) []node {
	result := make([]node, 0)
	for _, n := range c.Nodes {
		if !n.failed(tick) && !n.waitingToBeStarted(tick) {
			result = append(result, *n)
		}
	}
	return result
}

func (c *cluster) getNodesToStart(tick uint64) []node {
	result := make([]node, 0)
	for _, n := range c.Nodes {
		if n.waitingToBeStarted(tick) {
			result = append(result, *n)
		}
	}
	return result
}

func (c *cluster) getFailedNodes(tick uint64) []node {
	result := make([]node, 0)
	for _, n := range c.Nodes {
		if n.failed(tick) {
			result = append(result, *n)
		}
	}
	return result
}

func (c *cluster) deepCopy() *cluster {
	nc := &cluster{
		ClusterID:         c.ClusterID,
		ConfigChangeIndex: c.ConfigChangeIndex,
	}
	nc.Nodes = make(map[uint64]*node)
	for k, v := range c.Nodes {
		nc.Nodes[k] = &node{
			ClusterID:     v.ClusterID,
			NodeID:        v.NodeID,
			Address:       v.Address,
			IsLeader:      v.IsLeader,
			Tick:          v.Tick,
			FirstObserved: v.FirstObserved,
		}
	}
	return nc
}

func newMultiCluster() *multiCluster {
	return &multiCluster{
		Clusters: make(map[uint64]*cluster),
	}
}

func (mc *multiCluster) deepCopy() *multiCluster {
	c := &multiCluster{
		Clusters: make(map[uint64]*cluster),
	}
	for k, v := range mc.Clusters {
		c.Clusters[k] = v.deepCopy()
	}
	return c
}

func (mc *multiCluster) getToKillNodes() []nodeToKill {
	result := make([]nodeToKill, 0)
	for _, ntk := range mc.NodesToKill {
		n := nodeToKill{
			ClusterID: ntk.ClusterID,
			NodeID:    ntk.NodeID,
			Address:   ntk.Address,
		}
		result = append(result, n)
	}
	mc.NodesToKill = mc.NodesToKill[:0]
	return result
}

func (mc *multiCluster) size() int {
	return len(mc.Clusters)
}

func (mc *multiCluster) getUnavailableClusters(currentTick uint64) []cluster {
	cl := make([]cluster, 0)
	for _, c := range mc.Clusters {
		if !c.available(currentTick) {
			cl = append(cl, *c)
		}
	}
	return cl
}

func (mc *multiCluster) getClusterForRepair(currentTick uint64) []clusterRepair {
	crl := make([]clusterRepair, 0)
	for _, c := range mc.Clusters {
		failedNodes := c.getFailedNodes(currentTick)
		okNodes := c.getOkNodes(currentTick)
		toStartNodes := c.getNodesToStart(currentTick)
		if len(failedNodes) == 0 && len(toStartNodes) == 0 {
			continue
		}
		sort.Slice(failedNodes, func(i, j int) bool {
			return failedNodes[i].NodeID < failedNodes[j].NodeID
		})
		sort.Slice(okNodes, func(i, j int) bool {
			return okNodes[i].NodeID < okNodes[j].NodeID
		})
		sort.Slice(toStartNodes, func(i, j int) bool {
			return toStartNodes[i].NodeID < toStartNodes[j].NodeID
		})
		cr := clusterRepair{
			cluster:      c,
			clusterID:    c.ClusterID,
			failedNodes:  failedNodes,
			okNodes:      okNodes,
			nodesToStart: toStartNodes,
		}
		crl = append(crl, cr)
	}
	return crl
}

func (mc *multiCluster) update(nhi pb.NodeHostInfo) {
	toKill := mc.doUpdate(nhi)
	for _, ntk := range toKill {
		n := nodeToKill{
			ClusterID: ntk.ClusterId,
			NodeID:    ntk.NodeId,
			Address:   nhi.RaftAddress,
		}
		mc.NodesToKill = append(mc.NodesToKill, n)
	}
	mc.syncLeaderInfo(nhi)
}

func (mc *multiCluster) syncLeaderInfo(nhi pb.NodeHostInfo) {
	for _, ci := range nhi.ClusterInfo {
		cid := ci.ClusterId
		c, ok := mc.Clusters[cid]
		if !ok {
			continue
		}
		if c.ConfigChangeIndex > ci.ConfigChangeIndex {
			continue
		}
		n, ok := c.Nodes[ci.NodeId]
		if !ok {
			continue
		}
		if !ci.IsLeader && n.IsLeader {
			n.IsLeader = false
		} else if ci.IsLeader && !n.IsLeader {
			for _, cn := range c.Nodes {
				cn.IsLeader = false
			}
			n.IsLeader = true
		}
	}
}

func (mc *multiCluster) reset() {
	mc.NodesToKill = mc.NodesToKill[:0]
	mc.Clusters = make(map[uint64]*cluster)
}

func (mc *multiCluster) updateNodeTick(nhi pb.NodeHostInfo) {
	for _, cluster := range nhi.ClusterInfo {
		cid := cluster.ClusterId
		nid := cluster.NodeId
		if ec, ok := mc.Clusters[cid]; ok {
			if n, ok := ec.Nodes[nid]; ok {
				n.Tick = nhi.LastTick
			}
		}
	}
}

// doUpdate updates the local records of known clusters using recently received
// NodeHostInfo received from nodehost, it returns a list of ClusterInfo
// considered as zombie clusters that are expected to be killed.
func (mc *multiCluster) doUpdate(nhi pb.NodeHostInfo) []pb.ClusterInfo {
	toKill := make([]pb.ClusterInfo, 0)
	for _, currentCluster := range nhi.ClusterInfo {
		cid := currentCluster.ClusterId
		plog.Debugf("updating NodeHostInfo for %s, pending %t, incomplete %t",
			logutil.DescribeNode(currentCluster.ClusterId, currentCluster.NodeId),
			currentCluster.Pending, currentCluster.Incomplete)
		if currentCluster.Pending {
			if ec, ok := mc.Clusters[cid]; ok {
				if len(ec.Nodes) > 0 && ec.ConfigChangeIndex > 0 &&
					ec.killRequestRequired(currentCluster) {
					toKill = append(toKill, currentCluster)
				}
			}
			continue
		}
		if !currentCluster.Incomplete {
			if ec, ok := mc.Clusters[cid]; !ok {
				// create not in the map, create it
				mc.Clusters[cid] = mc.getCluster(currentCluster, nhi.LastTick)
			} else {
				// cluster record is there, sync the info
				rejected := ec.syncCluster(currentCluster, nhi.LastTick)
				killRequired := ec.killRequestRequired(currentCluster)
				if rejected && killRequired {
					toKill = append(toKill, currentCluster)
				}
			}
		} else {
			// for this particular cluster, we don't have the full info
			// check whether the cluster/node are available, if true, update
			// the tick value
			ec, ca := mc.Clusters[cid]
			if ca {
				if len(ec.Nodes) > 0 && ec.ConfigChangeIndex > 0 &&
					ec.killRequestRequired(currentCluster) {
					toKill = append(toKill, currentCluster)
				}
			}
		}
	}
	// update nodes tick
	mc.updateNodeTick(nhi)
	return toKill
}

func (mc *multiCluster) GetCluster(clusterID uint64) *cluster {
	return mc.getClusterInfo(clusterID)
}

func (c *cluster) killRequestRequired(ci pb.ClusterInfo) bool {
	if c.ConfigChangeIndex <= ci.ConfigChangeIndex {
		return false
	}
	for nid := range c.Nodes {
		if nid == ci.NodeId {
			return false
		}
	}
	return true
}

func (mc *multiCluster) getClusterInfo(clusterID uint64) *cluster {
	c, ok := mc.Clusters[clusterID]
	if !ok {
		return nil
	}
	return c.deepCopy()
}

func (mc *multiCluster) getCluster(ci pb.ClusterInfo, tick uint64) *cluster {
	c := &cluster{
		ClusterID:         ci.ClusterId,
		ConfigChangeIndex: ci.ConfigChangeIndex,
		Nodes:             make(map[uint64]*node),
	}
	for nodeID, address := range ci.Nodes {
		n := &node{
			NodeID:        nodeID,
			Address:       address,
			ClusterID:     ci.ClusterId,
			FirstObserved: tick,
		}
		c.Nodes[nodeID] = n
	}
	n, ok := c.Nodes[ci.NodeId]
	if ok {
		n.IsLeader = ci.IsLeader
	}
	return c
}

func (c *cluster) syncCluster(ci pb.ClusterInfo, lastTick uint64) bool {
	if c.ConfigChangeIndex > ci.ConfigChangeIndex {
		return true
	}
	if c.ConfigChangeIndex == ci.ConfigChangeIndex {
		if len(c.Nodes) != len(ci.Nodes) {
			plog.Panicf("same config change index with different node count")
		}
		for nid := range ci.Nodes {
			_, ok := c.Nodes[nid]
			if !ok {
				plog.Panicf("different node id list")
			}
		}
	}
	if c.ConfigChangeIndex < ci.ConfigChangeIndex {
		plog.Debugf("cluster %s is being updated, %v, %v",
			c.describe(), c.Nodes, ci)
	}
	// we don't just re-create and populate the Nodes map because the
	// FirstObserved value need to be kept after the sync.
	c.ConfigChangeIndex = ci.ConfigChangeIndex
	toRemove := make([]uint64, 0)
	for nid := range c.Nodes {
		_, ok := ci.Nodes[nid]
		if !ok {
			toRemove = append(toRemove, nid)
		}
	}
	for _, nid := range toRemove {
		plog.Debugf("cluster %s is removing node %d due to config change %d",
			c.describe(), nid, ci.ConfigChangeIndex)
		delete(c.Nodes, nid)
	}
	for nodeID, address := range ci.Nodes {
		if _, ok := c.Nodes[nodeID]; !ok {
			n := &node{
				NodeID:        nodeID,
				Address:       address,
				ClusterID:     ci.ClusterId,
				FirstObserved: lastTick,
			}
			c.Nodes[nodeID] = n
			plog.Debugf("cluster %s is adding node %d:%s due to config change %d",
				c.describe(), nodeID, address, ci.ConfigChangeIndex)
		} else {
			n := c.Nodes[nodeID]
			if n.Address != address {
				plog.Panicf("changing node address in drummer")
			}
		}
	}
	addrMap := make(map[string]struct{})
	for _, n := range c.Nodes {
		_, ok := addrMap[n.Address]
		if ok {
			plog.Panicf("duplicated addr on drummer")
		} else {
			addrMap[n.Address] = struct{}{}
		}
	}
	return false
}
