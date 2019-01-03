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
	"context"

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
	// NodeHostAddress is the address of the NodeHost. This is the address for
	// exchanging Raft messages between Nodehosts
	NodeHostAddress string
	// NodeHostAPIAddress is the API address for making RPC calls to NodeHost.
	// Such RPC calls are usually used to make Raft proposals and linearizable
	// reads.
	NodeHostAPIAddress string
	// Region is the region of the NodeHost.
	Region string
	// ClusterInfo is a list of all Raft clusters managed by the NodeHost
	ClusterInfoList []ClusterInfo
	// ClusterIDList is a list of cluster IDs for all Raft clusters managed by
	// the NodeHost
	ClusterIDList []uint64
	// LogInfoIncluded is a boolean flag indicating whether the LogInfo contains
	// a list of raftio.NodeInfo values representing all Raft logs stored on
	// the NodeHost.
	LogInfoIncluded bool
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost. This list will be empty when LogInfoIncluded is
	// set to false.
	LogInfo []raftio.NodeInfo
}

// IMasterClient is the interface to be implemented for connecting NodeHosts
// with Master servers. Note both Master server and IMasterClient are optional.
//
// Consider a typical deployment in which you have say dozens of NodeHost
// instances spawned across many servers, each hosting hundreds of Raft nodes.
// To ensure availability, you have to assume that some NodeHost instances can
// fail at certain time. Your system need to be able to automatically react to
// such failures so affected Raft groups can continue to have the quorums
// available. Master is a simple concept that can be used to help providing such
// capabilities.
//
// In a Master server implementation, NodeHosts periodically contact
// the Master servers via their IMasterClient clients to report their own
// availability and the details of those managed Raft nodes. The NodeHost
// itself and all Raft nodes managed by that NodeHost are considered as dead
// once the NodeHost instance fails to contact the Master for a defined
// period of time. The Master servers then request other NodeHosts to spawn
// new Raft nodes to replace the unavailable ones. Master servers are expected
// to have its own mechanisms to ensure its own high availability.
//
// In a more advanced design, you can also implement features such as moving
// certain Raft nodes to selected NodeHost instances to re-balance load.
//
// Our Drummer server, available at github.com/lni/dragonboat/drummer, is a
// Master server reference implementation extensively used in our tests.
// Its IMasterClient client, available at
// github.com/lni/dragonboat/drummer/client/nodehost.go is a reference
// IMasterClient implementation that interacts with the Drummer server.
type IMasterClient interface {
	// Name returns the unique type name of the IMasterClient.
	Name() string
	// Stop stops the MasterClient instance.
	Stop()
	// GetDeploymentID returns the deployment ID from master servers.
	// Each unique multi-raft setup managed by Master servers is identified by a
	// uint64 deployment ID, Raft nodes with different deployment ID won't be able
	// to communicate with each other. It a fool-proof feature helping to prevent
	// corruption to your Raft log data by accidentally mis-configtured NodeHosts.
	// GetDeploymentID is called shortly after the launch of the NodeHost instance,
	// all Raft nodes managed by that NodeHost will be associated with the
	// deployment ID returned by GetDeploymentID. Master server is required to
	// always return the same the Deployment ID value during its entire life cycle.
	// Note that the deployment ID concept is optional, you can return a constant
	// uint64 value if you don't want to actually use this fool-proof this
	// feature.
	GetDeploymentID(ctx context.Context, url string) (uint64, error)
	// SendNodeHostInfo is called by NodeHost periodically to notify Master
	// servers the details of those Raft clusters managed by the NodeHost. This
	// also gives the IMasterClient an opportunity to receive requests from Master
	// servers to restore, repair or re-balance Raft clusters. Such received
	// requests should be internally queued to be handled periodically when
	// HandleMasterRequests is called.
	SendNodeHostInfo(ctx context.Context, url string, nhi NodeHostInfo) error
	// HandleMasterRequests is called by NodeHost periodically to handle pending
	// master requests received and queued when the SendNodeHostInfo method is
	// called.
	HandleMasterRequests(ctx context.Context) error
}

// newMasterClient creates an instance of the default master client.
func newMasterClient(nh *NodeHost,
	factory MasterClientFactoryFunc) IMasterClient {
	if factory != nil {
		return factory(nh)
	}
	panic("MasterClientFactoryFunc not specified")
}
