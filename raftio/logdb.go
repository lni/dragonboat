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

package raftio

import (
	"errors"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	// ErrNoSavedLog indicates no saved log.
	ErrNoSavedLog = errors.New("no saved log")
	// ErrNoBootstrapInfo indicates that there is no saved bootstrap info.
	ErrNoBootstrapInfo = errors.New("no bootstrap info")
)

// NodeInfo is used to identify a Raft node.
type NodeInfo struct {
	ClusterID uint64
	NodeID    uint64
}

// RaftState is the persistent Raft state found in the Log DB.
type RaftState struct {
	// State is the Raft state persistent to the disk
	State *pb.State
	// FirstIndex is the index of the first entry to iterate
	FirstIndex uint64
	// EntryCount is the number of entries to iterate
	EntryCount uint64
}

// GetNodeInfo returns a NodeInfo instance with the specified cluster ID
// and node ID.
func GetNodeInfo(cid uint64, nid uint64) NodeInfo {
	return NodeInfo{ClusterID: cid, NodeID: nid}
}

// IReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
type IReusableKey interface {
	SetEntryBatchKey(clusterID uint64, nodeID uint64, index uint64)
	// SetEntryKey sets the key to be an entry key for the specified Raft node
	// with the specified entry index.
	SetEntryKey(clusterID uint64, nodeID uint64, index uint64)
	// SetStateKey sets the key to be an persistent state key suitable
	// for the specified Raft cluster node.
	SetStateKey(clusterID uint64, nodeID uint64)
	// SetMaxIndexKey sets the key to be the max possible index key for the
	// specified Raft cluster node.
	SetMaxIndexKey(clusterID uint64, nodeID uint64)
	// Key returns the underlying byte slice of the key.
	Key() []byte
	// Release releases the key instance so it can be reused in the future.
	Release()
}

// IContext is the per thread context used in the logdb module.
// IContext is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing ILogDB.
type IContext interface {
	// Destroy destroys the IContext instance.
	Destroy()
	// Reset resets the IContext instance, all previous returned keys and
	// buffers will be put back to the IContext instance and be ready to
	// be used for the next iteration.
	Reset()
	// GetKey returns a reusable key.
	GetKey() IReusableKey
	// GetValueBuffer returns a byte buffer with at least sz bytes in length.
	GetValueBuffer(sz uint64) []byte
	// GetUpdates return a raftpb.Update slice,
	GetUpdates() []pb.Update
	// GetWriteBatch returns a write batch or transaction instance.
	GetWriteBatch() interface{}
	// GetEntryBatch returns an entry batch instance.
	GetEntryBatch() pb.EntryBatch
	// GetLastEntryBatch returns an entry batch instance.
	GetLastEntryBatch() pb.EntryBatch
}

// ILogDB is the interface implemented by the log DB for persistently store
// Raft states, log entries and other Raft metadata.
type ILogDB interface {
	// Name returns the type name of the ILogDB instance.
	Name() string
	// Close closes the ILogDB instance.
	Close()
	// BinaryFormat returns an constant uint32 value representing the binary
	// format version compatible with the ILogDB instance.
	BinaryFormat() uint32
	// GetLogDBThreadContext returns a new IContext instance.
	GetLogDBThreadContext() IContext
	// ListNodeInfo lists all available NodeInfo found in the log DB.
	ListNodeInfo() ([]NodeInfo, error)
	// SaveBootstrapInfo saves the specified bootstrap info to the log DB.
	SaveBootstrapInfo(clusterID uint64,
		nodeID uint64, bootstrap pb.Bootstrap) error
	// GetBootstrapInfo returns saved bootstrap info from log DB. It returns
	// ErrNoBootstrapInfo when there is no previously saved bootstrap info for
	// the specified node.
	GetBootstrapInfo(clusterID uint64, nodeID uint64) (*pb.Bootstrap, error)
	// SaveRaftState atomically saves the Raft states, log entries and snapshots
	// metadata found in the pb.Update list to the log DB.
	SaveRaftState(updates []pb.Update, ctx IContext) error
	// IterateEntries returns the continuous Raft log entries of the specified
	// Raft node between the index value range of [low, high) up to a max size
	// limit of maxSize bytes. It returns the located log entries, their total
	// size in bytes and the occurred error.
	IterateEntries(ents []pb.Entry,
		size uint64, clusterID uint64, nodeID uint64, low uint64,
		high uint64, maxSize uint64) ([]pb.Entry, uint64, error)
	// ReadRaftState returns the persistented raft state found in Log DB.
	ReadRaftState(clusterID uint64,
		nodeID uint64, lastIndex uint64) (*RaftState, error)
	// RemoveEntriesTo removes entries associated with the specified Raft node up
	// to the specified index.
	RemoveEntriesTo(clusterID uint64, nodeID uint64, index uint64) error
	// SaveSnapshots saves all snapshot metadata found in the pb.Update list.
	SaveSnapshots([]pb.Update) error
	// DeleteSnapshot removes the specified snapshot metadata from the log DB.
	DeleteSnapshot(clusterID uint64, nodeID uint64, index uint64) error
	// ListSnapshots lists available snapshots associated with the specified
	// Raft node for index range (0, index].
	ListSnapshots(clusterID uint64,
		nodeID uint64, index uint64) ([]pb.Snapshot, error)
	// RemoveNodeData removes all data associated with the specified node.
	RemoveNodeData(clusterID uint64, nodeID uint64) error
	// ImportSnapshot imports the specified snapshot by creating all required
	// metadata in the logdb.
	ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error
}
