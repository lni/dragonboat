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

const (
	// NoLeader is a special leader ID value to indicate that there is currently
	// no leader or leader ID is unknown.
	NoLeader uint64 = 0
)

// LeaderInfo contains info on Raft leader.
type LeaderInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Term      uint64
	LeaderID  uint64
}

// IRaftEventListener is the interface to allow users to get notified for
// certain Raft events.
type IRaftEventListener interface {
	LeaderUpdated(info LeaderInfo)
}

// EntryInfo contains info on log entries.
type EntryInfo struct {
	ShardID   uint64
	ReplicaID uint64
	Index     uint64
}

// SnapshotInfo contains info of the snapshot.
type SnapshotInfo struct {
	ShardID   uint64
	ReplicaID uint64
	From      uint64
	Index     uint64
}

// ConnectionInfo contains info of the connection.
type ConnectionInfo struct {
	Address            string
	SnapshotConnection bool
}

// ISystemEventListener is the system event listener used by the NodeHost.
type ISystemEventListener interface {
	NodeHostShuttingDown()
	NodeUnloaded(info NodeInfo)
	NodeDeleted(info NodeInfo)
	NodeReady(info NodeInfo)
	MembershipChanged(info NodeInfo)
	ConnectionEstablished(info ConnectionInfo)
	ConnectionFailed(info ConnectionInfo)
	SendSnapshotStarted(info SnapshotInfo)
	SendSnapshotCompleted(info SnapshotInfo)
	SendSnapshotAborted(info SnapshotInfo)
	SnapshotReceived(info SnapshotInfo)
	SnapshotRecovered(info SnapshotInfo)
	SnapshotCreated(info SnapshotInfo)
	SnapshotCompacted(info SnapshotInfo)
	LogCompacted(info EntryInfo)
	LogDBCompacted(info EntryInfo)
}
