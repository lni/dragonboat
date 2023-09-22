// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/raftio"
)

var (
	// ErrUnknownTarget is the error returned when the target address of the node
	// is unknown.
	ErrUnknownTarget = errors.New("target address unknown")
)

// IResolver converts the (shard id, replica id) tuple to network address.
type IResolver interface {
	Resolve(uint64, uint64) (string, string, error)
	Add(uint64, uint64, string)
}

var _ raftio.INodeRegistry = (*Registry)(nil)
var _ IResolver = (*Registry)(nil)

// Registry is used to manage all known node addresses in the multi raft system.
// The transport layer uses this address registry to locate nodes.
type Registry struct {
	partitioner server.IPartitioner
	validate    config.TargetValidator
	addr        sync.Map // map of raftio.NodeInfo => string
}

// NewNodeRegistry returns a new Registry object.
func NewNodeRegistry(streamConnections uint64, v config.TargetValidator) *Registry {
	n := &Registry{validate: v}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	return n
}

// Close closes the registry.
func (n *Registry) Close() error { return nil }

// Add adds the specified replica and its target info to the registry.
func (n *Registry) Add(shardID uint64, replicaID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		plog.Panicf("invalid target %s", target)
	}
	key := raftio.GetNodeInfo(shardID, replicaID)
	v, ok := n.addr.LoadOrStore(key, target)
	if ok {
		if v.(string) != target {
			plog.Panicf("inconsistent target for %s, %s:%s",
				logutil.DescribeNode(shardID, replicaID), v, target)
		}
	}
}

func (n *Registry) getConnectionKey(addr string, shardID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, n.partitioner.GetPartitionID(shardID))
}

// Remove removes a remote from the node registry.
func (n *Registry) Remove(shardID uint64, replicaID uint64) {
	n.addr.Delete(raftio.GetNodeInfo(shardID, replicaID))
}

// RemoveShard removes info associated with the specified shard.
func (n *Registry) RemoveShard(shardID uint64) {
	var toRemove []raftio.NodeInfo
	n.addr.Range(func(k, v interface{}) bool {
		ni := k.(raftio.NodeInfo)
		if ni.ShardID == shardID {
			toRemove = append(toRemove, ni)
		}
		return true
	})
	for _, v := range toRemove {
		n.addr.Delete(v)
	}
}

// Resolve looks up the address of the specified node.
func (n *Registry) Resolve(shardID uint64, replicaID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(shardID, replicaID)
	addr, ok := n.addr.Load(key)
	if !ok {
		return "", "", ErrUnknownTarget
	}
	return addr.(string), n.getConnectionKey(addr.(string), shardID), nil
}
