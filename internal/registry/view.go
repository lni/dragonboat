// Copyright 2017-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math/rand"
	"sync"

	"github.com/pierrec/lz4/v4"

	"github.com/lni/dragonboat/v4/internal/raft"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	binaryEnc = binary.BigEndian
)

// ShardInfo is a record for representing the state of a Raft shard based
// on the knowledge of the local NodeHost instance.
type ShardInfo struct {
	// Replicas is a map of member replica IDs to their Raft addresses.
	Replicas map[uint64]string
	// ShardID is the shard ID of the Raft shard.
	ShardID uint64
	// ReplicaID is the replica ID of the Raft replica.
	ReplicaID uint64
	// ConfigChangeIndex is the current config change index of the Raft node.
	// ConfigChangeIndex is Raft Log index of the last applied membership
	// change entry.
	ConfigChangeIndex uint64
	// StateMachineType is the type of the state machine.
	StateMachineType sm.Type
	// IsLeader indicates whether this is a leader node.
	// Deprecated: Use LeaderID and Term instead.
	IsLeader bool
	// LeaderID is the replica ID of the current leader
	LeaderID uint64
	// Term is the term of the current leader
	Term uint64
	// IsNonVoting indicates whether this is a non-voting nonVoting node.
	IsNonVoting bool
	// IsWitness indicates whether this is a witness node without actual log.
	IsWitness bool
	// Pending is a boolean flag indicating whether details of the shard node
	// is not available. The Pending flag is set to true usually because the node
	// has not had anything applied yet.
	Pending bool
}

// ShardView is the view of a shard from gossip's point of view at a certain
// point in time.
type ShardView struct {
	ShardID           uint64
	Replicas          map[uint64]string
	ConfigChangeIndex uint64
	LeaderID          uint64
	Term              uint64
}

func toShardViewList(input []ShardInfo) []ShardView {
	result := make([]ShardView, 0)
	for _, ci := range input {
		cv := ShardView{
			ShardID:           ci.ShardID,
			Replicas:          ci.Replicas,
			ConfigChangeIndex: ci.ConfigChangeIndex,
			LeaderID:          ci.LeaderID,
			Term:              ci.Term,
		}
		result = append(result, cv)
	}
	return result
}

type exchanged struct {
	DeploymentID uint64
	ShardInfo    []ShardView
}

// view contains dynamic information on shards, it can change after an
// election or a raft configuration change.
type view struct {
	deploymentID uint64
	// shardID -> ShardView
	mu struct {
		sync.Mutex
		shards map[uint64]ShardView
	}
}

func newView(deploymentID uint64) *view {
	v := &view{
		deploymentID: deploymentID,
	}
	v.mu.shards = make(map[uint64]ShardView)
	return v
}

func (v *view) shardCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.mu.shards)
}

func mergeShardView(current ShardView, update ShardView) ShardView {
	if current.ConfigChangeIndex < update.ConfigChangeIndex {
		current.Replicas = update.Replicas
		current.ConfigChangeIndex = update.ConfigChangeIndex
	}
	// we only keep which replica is the last known leader
	if update.LeaderID != raft.NoLeader {
		if current.LeaderID == raft.NoLeader || update.Term > current.Term {
			current.LeaderID = update.LeaderID
			current.Term = update.Term
		}
	}

	return current
}

func (v *view) update(updates []ShardView) {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, u := range updates {
		current, ok := v.mu.shards[u.ShardID]
		if !ok {
			current = ShardView{ShardID: u.ShardID}
		}
		v.mu.shards[u.ShardID] = mergeShardView(current, u)
	}
}

func (v *view) toShuffledList() []ShardView {
	ci := make([]ShardView, 0)
	func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		for _, v := range v.mu.shards {
			ci = append(ci, v)
		}
	}()

	rand.Shuffle(len(ci), func(i, j int) { ci[i], ci[j] = ci[j], ci[i] })
	return ci
}

func getCompressedData(deploymentID uint64, l []ShardView, n int) []byte {
	if n == 0 {
		return nil
	}
	exchanged := exchanged{
		DeploymentID: deploymentID,
		ShardInfo:    l[:n],
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(exchanged); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	compressed := make([]byte, lz4.CompressBlockBound(len(data))+4)
	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(data, compressed[4:])
	if err != nil {
		panic(err)
	}
	binaryEnc.PutUint32(compressed, uint32(len(data)))
	return compressed[:n+4]
}

func (v *view) getFullSyncData() []byte {
	l := v.toShuffledList()
	return getCompressedData(v.deploymentID, l, len(l))
}

func (v *view) getGossipData(limit int) []byte {
	l := v.toShuffledList()
	if len(l) == 0 {
		return nil
	}
	// binary search to find the cut
	i, j := 1, len(l)
	for i < j {
		h := i + (j-i)/2
		data := getCompressedData(v.deploymentID, l, h)
		if len(data) < limit {
			i = h + 1
		} else {
			j = h
		}
	}

	for i > 0 {
		result := getCompressedData(v.deploymentID, l, i)
		if len(result) < limit {
			return result
		}
		i--
	}
	return nil
}

func (v *view) updateFrom(data []byte) {
	if len(data) <= 4 {
		panic("unexpected size")
	}
	sz := binaryEnc.Uint32(data)
	dst := make([]byte, sz)
	n, err := lz4.UncompressBlock(data[4:], dst)
	if err != nil {
		return
	}
	dst = dst[:n]
	buf := bytes.NewBuffer(dst)
	dec := gob.NewDecoder(buf)
	exchanged := exchanged{}
	if err := dec.Decode(&exchanged); err != nil {
		return
	}
	if exchanged.DeploymentID != v.deploymentID {
		return
	}
	v.update(exchanged.ShardInfo)
}
