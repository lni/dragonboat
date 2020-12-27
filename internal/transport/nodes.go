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

package transport

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	// ErrUnknownTarget is the error returned when the target address of the node
	// is unknown.
	ErrUnknownTarget = errors.New("target address unknown")
)

// INodeRegistry is the local registry interface used to keep all known
// nodes in the system..
type INodeRegistry interface {
	Stop()
	Add(clusterID uint64, nodeID uint64, url string)
	AddRemote(uint64, uint64, string)
	Remove(clusterID uint64, nodeID uint64)
	RemoveCluster(clusterID uint64)
	Resolve(clusterID uint64, nodeID uint64) (string, string, error)
}

var _ INodeRegistry = (*Nodes)(nil)
var _ INodeAddressResolver = (*Nodes)(nil)

type record struct {
	address string
	key     string
}

// Nodes is used to manage all known node addresses in the multi raft system.
// The transport layer uses this address registry to locate nodes.
type Nodes struct {
	partitioner server.IPartitioner
	validate    config.TargetValidator
	mu          struct {
		sync.Mutex
		addr map[raftio.NodeInfo]record
	}
	nmu struct {
		sync.Mutex
		nodes map[raftio.NodeInfo]string
	}
}

// NewNodeRegistry returns a new Nodes object.
func NewNodeRegistry(streamConnections uint64, v config.TargetValidator) *Nodes {
	n := &Nodes{validate: v}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	n.mu.addr = make(map[raftio.NodeInfo]record)
	n.nmu.nodes = make(map[raftio.NodeInfo]string)
	return n
}

// Stop stops the node registry.
func (n *Nodes) Stop() {}

// AddRemote remembers the specified address obtained from the source of the
// incoming message.
func (n *Nodes) AddRemote(clusterID uint64, nodeID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		plog.Panicf("invalid target %s", target)
	}
	n.nmu.Lock()
	defer n.nmu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := n.nmu.nodes[key]
	if !ok {
		n.nmu.nodes[key] = target
	} else {
		if v != target {
			plog.Panicf("inconsistent target for %s, %s:%s",
				logutil.DescribeNode(clusterID, nodeID), v, target)
		}
	}
}

func (n *Nodes) getConnectionKey(addr string, clusterID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	idx := n.partitioner.GetPartitionID(clusterID)
	return fmt.Sprintf("%s-%d", addr, idx)
}

func (n *Nodes) getFromRemote(clusterID uint64, nodeID uint64) (string, error) {
	n.nmu.Lock()
	defer n.nmu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := n.nmu.nodes[key]
	if !ok {
		return "", ErrUnknownTarget
	}
	return v, nil
}

// Add add a new node.
func (n *Nodes) Add(clusterID uint64, nodeID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		plog.Panicf("invalid target %s", target)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if _, ok := n.mu.addr[key]; !ok {
		n.mu.addr[key] = record{
			address: target,
			key:     n.getConnectionKey(target, clusterID),
		}
	}
}

// Remove removes a remote from the node registry.
func (n *Nodes) Remove(clusterID uint64, nodeID uint64) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.mu.addr, key)
	}()
	func() {
		n.nmu.Lock()
		defer n.nmu.Unlock()
		delete(n.nmu.nodes, key)
	}()
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *Nodes) RemoveCluster(clusterID uint64) {
	func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		for k := range n.mu.addr {
			if k.ClusterID == clusterID {
				delete(n.mu.addr, k)
			}
		}
	}()
	func() {
		n.nmu.Lock()
		defer n.nmu.Unlock()
		for k := range n.nmu.nodes {
			if k.ClusterID == clusterID {
				delete(n.nmu.nodes, k)
			}
		}
	}()
}

// Resolve looks up the Addr of the specified node.
func (n *Nodes) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	n.mu.Lock()
	addr, ok := n.mu.addr[key]
	n.mu.Unlock()
	if !ok {
		na, err := n.getFromRemote(clusterID, nodeID)
		if err != nil {
			return "", "", errors.New("addr not found")
		}
		n.Add(clusterID, nodeID, na)
		return na, n.getConnectionKey(na, clusterID), nil
	}
	return addr.address, addr.key, nil
}
