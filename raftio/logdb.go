// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

package raftio

import (
	"github.com/cockroachdb/errors"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	// ErrNoSavedLog indicates no saved log.
	ErrNoSavedLog = errors.New("no saved log")
	// ErrNoBootstrapInfo indicates that there is no saved bootstrap info.
	ErrNoBootstrapInfo = errors.New("no bootstrap info")
)

// Metrics is the metrics of the LogDB.
type Metrics struct {
	// Busy indicates whether the LogDB is busy and not suitable for saving new
	// data into the store.
	Busy bool
}

// NodeInfo is used to identify a Raft node.
type NodeInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

// RaftState is the persistent Raft state found in the Log DB.
type RaftState struct {
	// State is the Raft state persistent to the disk
	State pb.State
	// FirstIndex is the index of the first entry to iterate
	FirstIndex uint64
	// EntryCount is the number of entries to iterate
	EntryCount uint64
}

// GetNodeInfo returns a NodeInfo instance with the specified shard ID
// and replica ID.
func GetNodeInfo(shardID uint64, replicaID uint64) NodeInfo {
	return NodeInfo{ShardID: shardID, ReplicaID: replicaID}
}

// ILogDB is the interface implemented by the log DB for persistently store
// Raft states, log entries and other Raft metadata.
type ILogDB interface {
	// Name returns the type name of the ILogDB instance.
	Name() string
	// Close closes the ILogDB instance.
	Close() error
	// BinaryFormat returns an constant uint32 value representing the binary
	// format version compatible with the ILogDB instance.
	BinaryFormat() uint32
	// ListNodeInfo lists all available NodeInfo found in the log DB.
	ListNodeInfo() ([]NodeInfo, error)
	// SaveBootstrapInfo saves the specified bootstrap info to the log DB.
	SaveBootstrapInfo(shardID uint64,
		replicaID uint64, bootstrap pb.Bootstrap) error
	// GetBootstrapInfo returns saved bootstrap info from log DB. It returns
	// ErrNoBootstrapInfo when there is no previously saved bootstrap info for
	// the specified node.
	GetBootstrapInfo(shardID uint64, replicaID uint64) (pb.Bootstrap, error)
	// SaveRaftState atomically saves the Raft states, log entries and snapshots
	// metadata found in the pb.Update list to the log DB. shardID is a 1-based
	// ID of the worker invoking the SaveRaftState method, as each worker
	// accesses the log DB from its own thread, SaveRaftState will never be
	// concurrently called with the same shardID.
	SaveRaftState(updates []pb.Update, shardID uint64) error
	// IterateEntries returns the continuous Raft log entries of the specified
	// Raft node between the index value range of [low, high) up to a max size
	// limit of maxSize bytes. It returns the located log entries, their total
	// size in bytes and the occurred error.
	IterateEntries(ents []pb.Entry,
		size uint64, shardID uint64, replicaID uint64, low uint64,
		high uint64, maxSize uint64) ([]pb.Entry, uint64, error)
	// ReadRaftState returns the persistented raft state found in Log DB.
	ReadRaftState(shardID uint64,
		replicaID uint64, lastIndex uint64) (RaftState, error)
	// RemoveEntriesTo removes entries with indexes between (0, index].
	RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error
	// CompactEntriesTo reclaims underlying storage space used for storing
	// entries up to the specified index.
	CompactEntriesTo(shardID uint64,
		replicaID uint64, index uint64) (<-chan struct{}, error)
	// SaveSnapshots saves all snapshot metadata found in the pb.Update list.
	SaveSnapshots([]pb.Update) error
	// GetSnapshot returns the most recent snapshot associated with the specified
	// shard.
	GetSnapshot(shardID uint64, replicaID uint64) (pb.Snapshot, error)
	// RemoveNodeData removes all data associated with the specified node.
	RemoveNodeData(shardID uint64, replicaID uint64) error
	// ImportSnapshot imports the specified snapshot by creating all required
	// metadata in the logdb.
	ImportSnapshot(snapshot pb.Snapshot, replicaID uint64) error
}
