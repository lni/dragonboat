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

package server

import (
	pb "github.com/lni/dragonboat/v4/raftpb"
)

const (
	// NoLeader is the flag used to indcate that there is no leader or the leader
	// is unknown.
	NoLeader uint64 = 0
)

// LeaderInfo contains leader info.
type LeaderInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Term      uint64
	LeaderID  uint64
}

// CampaignInfo contains campaign info.
type CampaignInfo struct {
	ShardID   uint64
	ReplicaID uint64
	PreVote   bool
	Term      uint64
}

// SnapshotInfo contains info of a snapshot.
type SnapshotInfo struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
	Term      uint64
}

// ReplicationInfo contains info of a replication message.
type ReplicationInfo struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
	Term      uint64
}

// ProposalInfo contains info on proposals.
type ProposalInfo struct {
	Entries   []pb.Entry
	ShardID   uint64
	ReplicaID uint64
}

// ReadIndexInfo contains info on read index requests.
type ReadIndexInfo struct {
	ShardID   uint64
	ReplicaID uint64
}

// IRaftEventListener is the event listener used by the Raft implementation.
type IRaftEventListener interface {
	LeaderUpdated(info LeaderInfo)
	CampaignLaunched(info CampaignInfo)
	CampaignSkipped(info CampaignInfo)
	SnapshotRejected(info SnapshotInfo)
	ReplicationRejected(info ReplicationInfo)
	ProposalDropped(info ProposalInfo)
	ReadIndexDropped(info ReadIndexInfo)
}

// SystemEventType is the type of system events.
type SystemEventType uint64

const (
	// NodeHostShuttingDown ...
	NodeHostShuttingDown SystemEventType = iota
	// NodeReady ...
	NodeReady
	// NodeUnloaded ...
	NodeUnloaded
	// NodeDeleted
	NodeDeleted
	// MembershipChanged ...
	MembershipChanged
	// ConnectionEstablished ...
	ConnectionEstablished
	// ConnectionFailed ...
	ConnectionFailed
	// SendSnapshotStarted ...
	SendSnapshotStarted
	// SendSnapshotCompleted ...
	SendSnapshotCompleted
	// SendSnapshotAborted ...
	SendSnapshotAborted
	// SnapshotReceived ...
	SnapshotReceived
	// SnapshotRecovered ...
	SnapshotRecovered
	// SnapshotCreated ...
	SnapshotCreated
	// SnapshotCompacted ...
	SnapshotCompacted
	// LogCompacted ...
	LogCompacted
	// LogDBCompacted ...
	LogDBCompacted
)

// SystemEvent is an system event record published by the system that can be
// handled by a raftio.ISystemEventListener.
type SystemEvent struct {
	Address            string
	Type               SystemEventType
	ShardID            uint64
	ReplicaID          uint64
	From               uint64
	Index              uint64
	SnapshotConnection bool
}
