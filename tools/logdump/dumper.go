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

package main

import (
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

// NodeSummary summarizes a single Raft node's data found in the LogDB.
type NodeSummary struct {
	ClusterID, NodeID                 uint64
	Term, Vote, Commit                uint64
	FirstIndex, LastIndex, EntryCount uint64
	SnapshotCnt                       int
	Bootstrap                         string
}

// ScanResult holds all data returned by Scan.
type ScanResult struct {
	State           pb.State
	Snapshots       []pb.Snapshot
	Entries         []pb.Entry
	From, To, Limit uint64
	FirstIndex      uint64
	LastIndex       uint64
	EntryCount      uint64
}

// Dumper wraps an ILogDB to provide high-level inspection methods.
type Dumper struct {
	db raftio.ILogDB
}

// NewDumper creates a new Dumper wrapping the given ILogDB.
func NewDumper(db raftio.ILogDB) *Dumper {
	return &Dumper{db: db}
}

// Close closes the underlying LogDB.
func (d *Dumper) Close() error {
	return d.db.Close()
}

// Describe returns a summary of all Raft nodes in the LogDB, optionally
// filtered by cluster and/or node ID. A zero-valued filter is ignored.
func (d *Dumper) Describe(clusterFilter, nodeFilter uint64) ([]NodeSummary, error) {
	nodes, err := d.db.ListNodeInfo()
	if err != nil {
		return nil, errors.Wrapf(err, "list node info")
	}

	var result []NodeSummary
	for _, ni := range nodes {
		if clusterFilter != 0 && ni.ClusterID != clusterFilter {
			continue
		}
		if nodeFilter != 0 && ni.NodeID != nodeFilter {
			continue
		}
		ns := NodeSummary{
			ClusterID: ni.ClusterID,
			NodeID:    ni.NodeID,
		}

		// Read persisted Raft state (term, vote, commit, entry range).
		// Find the latest snapshot to use as the snapshotIndex parameter.
		snaps, err := d.db.ListSnapshots(ni.ClusterID, ni.NodeID, math.MaxUint64)
		if err != nil {
			return nil, errors.Wrapf(err, "list snapshots for cluster=%d node=%d", ni.ClusterID, ni.NodeID)
		}
		ns.SnapshotCnt = len(snaps)

		snapshotIndex := uint64(0)
		if len(snaps) > 0 {
			// Use the latest snapshot's index as snapshotIndex
			snapshotIndex = snaps[len(snaps)-1].Index
		}

		// Read persisted Raft state (term, vote, commit, entry range).
		rs, err := d.db.ReadRaftState(ni.ClusterID, ni.NodeID, snapshotIndex)
		if err == nil {
			ns.Term = rs.State.Term
			ns.Vote = rs.State.Vote
			ns.Commit = rs.State.Commit
			ns.FirstIndex = rs.FirstIndex
			ns.EntryCount = rs.EntryCount
			if rs.EntryCount > 0 {
				ns.LastIndex = rs.FirstIndex + rs.EntryCount - 1
			}
		} else if err != raftio.ErrNoSavedLog {
			return nil, errors.Wrapf(err, "read raft state for cluster=%d node=%d snapshotIndex=%d", ni.ClusterID, ni.NodeID, snapshotIndex)
		}

		// Read bootstrap info.
		bootstrap, err := d.db.GetBootstrapInfo(ni.ClusterID, ni.NodeID)
		if err == nil {
			ns.Bootstrap = fmt.Sprintf("join=%v, %d addrs, sm=%s",
				bootstrap.Join, len(bootstrap.Addresses), bootstrap.Type.String())
		} else if err == raftio.ErrNoBootstrapInfo {
			ns.Bootstrap = "(none)"
		} else {
			return nil, errors.Wrapf(err, "get bootstrap info for cluster=%d node=%d", ni.ClusterID, ni.NodeID)
		}

		result = append(result, ns)
	}
	return result, nil
}

// GetEntries retrieves the Raft state, snapshots, and log entries for the
// specified node. The entry range is clamped to the available log and an
// optional limit is applied to the returned entries.
func (d *Dumper) Scan(clusterID, nodeID, from, to, limit uint64) (*ScanResult, error) {
	// Find the latest snapshot with index < from to use as snapshotIndex.
	snapshotIndex := uint64(0)
	snaps, err := d.db.ListSnapshots(clusterID, nodeID, math.MaxUint64)
	if err != nil {
		return nil, errors.Wrapf(err, "list snapshots for cluster=%d node=%d", clusterID, nodeID)
	}
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Index < from {
			snapshotIndex = snaps[i].Index
			break
		}
	}

	// Read persisted Raft state to discover the available entry range.
	rs, err := d.db.ReadRaftState(clusterID, nodeID, snapshotIndex)
	if err != nil {
		return nil, errors.Wrapf(err, "read raft state for cluster=%d node=%d snapshotIndex=%d", clusterID, nodeID, snapshotIndex)
	}

	res := &ScanResult{
		From:       from,
		To:         to,
		Limit:      limit,
		State:      rs.State,
		FirstIndex: rs.FirstIndex,
		EntryCount: rs.EntryCount,
	}
	if rs.EntryCount > 0 {
		res.LastIndex = rs.FirstIndex + rs.EntryCount - 1
	}

	// Clamp from/to to the available range.
	if from == 0 || from < rs.FirstIndex {
		from = rs.FirstIndex
	}
	if to == 0 {
		to = from + rs.EntryCount
	}
	if to > rs.FirstIndex+rs.EntryCount {
		to = rs.FirstIndex + rs.EntryCount
	}
	if limit != 0 && to > from+limit {
		to = from + limit
	}
	res.From = from
	res.To = to

	// List snapshots within the requested range.
	res.Snapshots, err = d.db.ListSnapshots(clusterID, nodeID, to)
	if err != nil {
		return nil, errors.Wrapf(err, "list snapshots for cluster=%d node=%d", clusterID, nodeID)
	}

	// Iterate entries in [from, to).
	if from < to {
		entries, _, err := d.db.IterateEntries(nil, 0, clusterID, nodeID, from, to, math.MaxUint64)
		if err != nil {
			return nil, errors.Wrapf(err, "iterate entries for cluster=%d node=%d from=%d to=%d", clusterID, nodeID, from, to)
		}
		res.Entries = entries
	}

	return res, nil
}
