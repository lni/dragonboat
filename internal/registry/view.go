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
	"encoding/gob"
	"math/rand"
	"sync"

	"github.com/pierrec/lz4/v4"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

// ClusterInfo is a record for representing the state of a Raft cluster based
// on the knowledge of the local NodeHost instance.
type ClusterInfo struct {
	// Nodes is a map of member node IDs to their Raft addresses.
	Nodes map[uint64]string
	// ClusterID is the cluster ID of the Raft cluster node.
	ClusterID uint64
	// NodeID is the node ID of the Raft cluster node.
	NodeID uint64
	// ConfigChangeIndex is the current config change index of the Raft node.
	// ConfigChangeIndex is Raft Log index of the last applied membership
	// change entry.
	ConfigChangeIndex uint64
	// StateMachineType is the type of the state machine.
	StateMachineType sm.Type
	// IsLeader indicates whether this is a leader node.
	// Deprecated: Use LeaderID and Term instead.
	IsLeader bool
	// LeaderID is the node ID of the current leader
	LeaderID uint64
	// Term is the term of the current leader
	Term uint64
	// IsNonVoting indicates whether this is a non-voting nonVoting node.
	IsNonVoting bool
	// IsWitness indicates whether this is a witness node without actual log.
	IsWitness bool
	// Pending is a boolean flag indicating whether details of the cluster node
	// is not available. The Pending flag is set to true usually because the node
	// has not had anything applied yet.
	Pending bool
}

type sharedInfo struct {
	DeploymentID uint64
	ClusterInfo  []ClusterInfo
}

type view struct {
	deploymentID uint64
	// clusterID -> ClusterInfo
	mu struct {
		sync.Mutex
		nodehosts map[uint64]ClusterInfo
	}
}

func newView(deploymentID uint64) *view {
	v := &view{
		deploymentID: deploymentID,
	}
	v.mu.nodehosts = make(map[uint64]ClusterInfo)
	return v
}

func (v *view) nodeHostCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.mu.nodehosts)
}

func (v *view) update(u []ClusterInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, ci := range u {
		if ci.Pending {
			continue
		}
		current, ok := v.mu.nodehosts[ci.ClusterID]
		if !ok {
			v.mu.nodehosts[ci.ClusterID] = ci
		} else {
			if ci.ConfigChangeIndex < current.ConfigChangeIndex {
				continue
			}
			v.mu.nodehosts[ci.ClusterID] = ci
		}
	}
}

func (v *view) toShuffledList() []ClusterInfo {
	ci := make([]ClusterInfo, 0)
	func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		for _, v := range v.mu.nodehosts {
			ci = append(ci, v)
		}
	}()

	rand.Shuffle(len(ci), func(i, j int) { ci[i], ci[j] = ci[j], ci[i] })
	return ci
}

func getCompressedData(deploymentID uint64, l []ClusterInfo, n int) []byte {
	if n == 0 {
		return nil
	}
	si := sharedInfo{
		DeploymentID: deploymentID,
		ClusterInfo:  l[:n],
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(si); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	compressed := make([]byte, lz4.CompressBlockBound(len(data)))
	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(data, compressed)
	if err != nil {
		panic(err)
	}
	return compressed[:n]
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
	dst := make([]byte, 64*1024)
	n, err := lz4.UncompressBlock(data, dst)
	if err != nil {
		return
	}
	dst = dst[:n]
	buf := bytes.NewBuffer(dst)
	dec := gob.NewDecoder(buf)
	si := sharedInfo{}
	if err := dec.Decode(&si); err != nil {
		return
	}
	if si.DeploymentID != v.deploymentID {
		return
	}
	v.update(si.ClusterInfo)
}
