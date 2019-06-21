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

package logdb

import (
	"sync"

	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type rdbcache struct {
	nodeInfo       map[raftio.NodeInfo]struct{}
	ps             map[raftio.NodeInfo]pb.State
	lastEntryBatch map[raftio.NodeInfo]pb.EntryBatch
	maxIndex       map[raftio.NodeInfo]uint64
	mu             sync.Mutex
}

func newRDBCache() *rdbcache {
	return &rdbcache{
		nodeInfo:       make(map[raftio.NodeInfo]struct{}),
		ps:             make(map[raftio.NodeInfo]pb.State),
		lastEntryBatch: make(map[raftio.NodeInfo]pb.EntryBatch),
		maxIndex:       make(map[raftio.NodeInfo]uint64),
	}
}

func (r *rdbcache) setNodeInfo(clusterID uint64, nodeID uint64) bool {
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.nodeInfo[key]
	if !ok {
		r.nodeInfo[key] = struct{}{}
	}
	return !ok
}

func (r *rdbcache) setState(clusterID uint64,
	nodeID uint64, st pb.State) bool {
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.ps[key]
	if !ok {
		r.ps[key] = st
		return true
	}
	if pb.IsStateEqual(v, st) {
		return false
	}
	r.ps[key] = st
	return true
}

func (r *rdbcache) setMaxIndex(clusterID uint64,
	nodeID uint64, maxIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	r.maxIndex[key] = maxIndex
}

func (r *rdbcache) getMaxIndex(clusterID uint64,
	nodeID uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	v, ok := r.maxIndex[key]
	if !ok {
		return 0, false
	}
	return v, true
}

func (r *rdbcache) setLastEntryBatch(clusterID uint64,
	nodeID uint64, eb pb.EntryBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	oeb, ok := r.lastEntryBatch[key]
	if !ok {
		oeb = pb.EntryBatch{Entries: make([]pb.Entry, 0, len(eb.Entries))}
	} else {
		oeb.Entries = oeb.Entries[:0]
	}
	oeb.Entries = append(oeb.Entries, eb.Entries...)
	r.lastEntryBatch[key] = oeb
}

func (r *rdbcache) getLastEntryBatch(clusterID uint64,
	nodeID uint64, lb pb.EntryBatch) (pb.EntryBatch, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	v, ok := r.lastEntryBatch[key]
	if !ok {
		return v, false
	}
	lb.Entries = lb.Entries[:0]
	lb.Entries = append(lb.Entries, v.Entries...)
	return lb, true
}
