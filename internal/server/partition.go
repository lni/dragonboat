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

package server

var (
	defaultClusterIDMod uint64 = 512
)

// IPartitioner is the interface for partitioning clusters.
type IPartitioner interface {
	GetPartitionID(clusterID uint64) uint64
}

// FixedPartitioner is the IPartitioner with fixed capacity and naive
// partitioning strategy.
type FixedPartitioner struct {
	capacity uint64
}

// NewFixedPartitioner creates a new FixedPartitioner instance.
func NewFixedPartitioner(capacity uint64) *FixedPartitioner {
	return &FixedPartitioner{capacity: capacity}
}

// GetPartitionID returns the partition ID for the specified raft cluster.
func (p *FixedPartitioner) GetPartitionID(clusterID uint64) uint64 {
	return clusterID % p.capacity
}

// DoubleFixedPartitioner is the IPartitioner with two fixed capacity and naive
// partitioning strategy.
type DoubleFixedPartitioner struct {
	capacity    uint64
	workerCount uint64
}

// NewDoubleFixedPartitioner creates a new DoubleFixedPartitioner instance.
func NewDoubleFixedPartitioner(capacity uint64,
	workerCount uint64) *DoubleFixedPartitioner {
	return &DoubleFixedPartitioner{
		capacity:    capacity,
		workerCount: workerCount,
	}
}

// GetPartitionID returns the partition ID for the specified raft cluster.
func (p *DoubleFixedPartitioner) GetPartitionID(clusterID uint64) uint64 {
	return (clusterID % p.workerCount) % p.capacity
}
