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
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
)

// INodeRegistry is the local registry interface used to keep all known
// nodes in the multi raft system.
type INodeRegistry interface {
	AddNode(clusterID uint64, nodeID uint64, url string)
	RemoveNode(clusterID uint64, nodeID uint64)
	RemoveCluster(clusterID uint64)
	Resolve(clusterID uint64, nodeID uint64) (string, string, error)
}

type addr struct {
	network string
	address string
	port    int
}

func newAddr(v string) (*addr, error) {
	in := strings.TrimSpace(v)
	parts := strings.Split(in, ":")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		plog.Panicf("invalid address, %s", v)
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil || p < 0 || p > 65535 {
		plog.Panicf("invalid port %s", parts[1])
	}
	a := &addr{
		network: "tcp",
		address: parts[0],
		port:    p,
	}
	return a, nil
}

func (a *addr) String() string {
	return fmt.Sprintf("%s:%d", a.address, a.port)
}

type record struct {
	address string
	key     string
}

// Nodes is used to manage all known node addresses in the multi raft system.
// The transport layer uses this address registry to locate nodes.
type Nodes struct {
	partitioner server.IPartitioner
	mu          struct {
		sync.Mutex
		addr map[raftio.NodeInfo]record
	}
	nmu struct {
		sync.Mutex
		nodes map[raftio.NodeInfo]string
	}
}

// NewNodes returns a new Nodes object.
func NewNodes(streamConnections uint64) *Nodes {
	n := &Nodes{}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	n.mu.addr = make(map[raftio.NodeInfo]record)
	n.nmu.nodes = make(map[raftio.NodeInfo]string)
	return n
}

// AddRemoteAddress remembers the specified address obtained from the source
// of the incoming message.
func (n *Nodes) AddRemoteAddress(clusterID uint64,
	nodeID uint64, address string) {
	if len(address) == 0 {
		panic("empty address")
	}
	if nodeID == 0 {
		panic("invalid node id")
	}
	n.nmu.Lock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := n.nmu.nodes[key]
	if !ok {
		n.nmu.nodes[key] = address
	} else {
		if v != address {
			plog.Panicf("inconsistent addr for %s, %s:%s",
				logutil.DescribeNode(clusterID, nodeID), v, address)
		}
	}
	n.nmu.Unlock()
}

func (n *Nodes) getConnectionKey(addr string, clusterID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	idx := n.partitioner.GetPartitionID(clusterID)
	return fmt.Sprintf("%s-%d", addr, idx)
}

func (n *Nodes) getAddressFromRemoteList(clusterID uint64,
	nodeID uint64) (string, error) {
	n.nmu.Lock()
	defer n.nmu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := n.nmu.nodes[key]
	if !ok {
		return "", errors.New("no address")
	}
	return v, nil
}

// AddNode add a new node.
func (n *Nodes) AddNode(clusterID uint64, nodeID uint64, url string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if v, err := newAddr(url); err != nil {
		panic(err)
	} else {
		if _, ok := n.mu.addr[key]; !ok {
			rec := record{
				address: v.String(),
				key:     n.getConnectionKey(v.String(), clusterID),
			}
			n.mu.addr[key] = rec
		}
	}
}

// RemoveNode removes a remote from the node registry.
func (n *Nodes) RemoveNode(clusterID uint64, nodeID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := raftio.GetNodeInfo(clusterID, nodeID)
	delete(n.mu.addr, key)
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *Nodes) RemoveCluster(clusterID uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	toRemove := make([]raftio.NodeInfo, 0)
	for k := range n.mu.addr {
		if k.ClusterID == clusterID {
			toRemove = append(toRemove, k)
		}
	}
	for _, key := range toRemove {
		delete(n.mu.addr, key)
	}
}

// RemoveAllPeers removes all remotes.
func (n *Nodes) RemoveAllPeers() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mu.addr = make(map[raftio.NodeInfo]record)
}

// Resolve looks up the Addr of the specified node.
func (n *Nodes) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	n.mu.Lock()
	addr, ok := n.mu.addr[key]
	n.mu.Unlock()
	if !ok {
		na, err := n.getAddressFromRemoteList(clusterID, nodeID)
		if err != nil {
			return "", "", errors.New("cluster id/node id not found")
		}
		n.AddNode(clusterID, nodeID, na)
		return na, n.getConnectionKey(na, clusterID), nil
	}
	return addr.address, addr.key, nil
}

// ReverseResolve does the reverse lookup for the specified address. A list
// of node raftio.NodeInfos are returned for nodes that match the specified address
func (n *Nodes) ReverseResolve(addr string) []raftio.NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	affected := make([]raftio.NodeInfo, 0)
	for k, v := range n.mu.addr {
		altV := ""
		u, err := url.Parse(v.address)
		if err == nil {
			altV = u.Host
		}
		if v.address == addr || (len(altV) > 0 && altV == addr) {
			affected = append(affected, k)
		}
	}
	return affected
}
