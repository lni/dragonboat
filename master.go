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

package dragonboat

import (
	"github.com/lni/dragonboat/raftio"
)

// ClusterInfo provides Raft cluster details. The optional Master client
// periodically sends a list of ClusterInfo to master servers to notify master
// the details of all Raft clusters available on each NodeHost instance.
// ClusterInfo is an optional struct used by Master servers.
type ClusterInfo struct {
	// ClusterID is the cluster ID of the Raft cluster node
	ClusterID uint64
	// NodeID is the node ID of the Raft cluster node
	NodeID uint64
	// IsLeader indicates whether this is a leader node
	IsLeader bool
	// Nodes is a map of member node IDs to Raft addresses
	Nodes map[uint64]string
	// ConfigChangeIndex is the current config change index of the Raft node.
	// ConfigChangeIndex is increased each time when a new membership change
	// is applied
	ConfigChangeIndex uint64
	// Pending is a boolean flag indicating whether details of the cluster
	// node is still not available. The Pending flag is set to true usually
	// because the node has not had anything applied
	Pending bool
	// Incomplete is a boolean flag indicating whether the ClusterInfo record
	// has the Nodes map intentionally omitted to save bandwidth
	Incomplete bool
}

// NodeHostInfo provides info about the NodeHost, including its managed Raft
// clusters and Raft logs saved in its local persistent storage.
type NodeHostInfo struct {
	// RaftAddress is the address of the NodeHost. This is the address for
	// exchanging Raft messages between Nodehosts
	RaftAddress string
	// APIAddress is the API address for making RPC calls to NodeHost.
	// Such RPC calls are usually used to make Raft proposals and linearizable
	// reads.
	APIAddress string
	// Region is the region of the NodeHost.
	Region string
	// ClusterInfo is a list of all Raft clusters managed by the NodeHost
	ClusterInfoList []ClusterInfo
	// ClusterIDList is a list of cluster IDs for all Raft clusters managed by
	// the NodeHost
	ClusterIDList []uint64
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost. This list will be empty when LogInfoIncluded is
	// set to false.
	LogInfo []raftio.NodeInfo
}
