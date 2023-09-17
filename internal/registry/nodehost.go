// Copyright 2018-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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

// NodeHostRegistry is a NodeHost info registry backed by gossip.
type NodeHostRegistry struct {
	store *metaStore
	view  *view
}

// NumOfShards returns the number of shards known to the current NodeHost
// instance.
func (r *NodeHostRegistry) NumOfShards() int {
	return r.view.shardCount()
}

// GetMeta returns gossip metadata associated with the specified NodeHost
// instance.
func (r *NodeHostRegistry) GetMeta(nhID string) ([]byte, bool) {
	m, ok := r.store.get(nhID)
	if !ok {
		return nil, false
	}
	return m.Data, true
}

// GetShardInfo returns the shard info for the specified shard if it is
// available in the gossip view.
func (r *NodeHostRegistry) GetShardInfo(shardID uint64) (ShardView, bool) {
	r.view.mu.Lock()
	defer r.view.mu.Unlock()

	ci, ok := r.view.mu.shards[shardID]
	if !ok {
		return ShardView{}, false
	}
	result := ci
	result.Replicas = make(map[uint64]string)
	for shardID, target := range ci.Replicas {
		result.Replicas[shardID] = target
	}
	return result, true
}
