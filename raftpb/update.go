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

package raftpb

import (
	"bytes"
	"encoding/binary"
	"io"
)

// LeaderUpdate describes updated leader
type LeaderUpdate struct {
	LeaderID uint64
	Term     uint64
}

// LogQueryResult is the result of log query.
type LogQueryResult struct {
	FirstIndex uint64
	LastIndex  uint64
	Error      error
	Entries    []Entry
}

// IsEmpty returns a boolean value indicating whether the LogQueryResult is
// empty.
func (r *LogQueryResult) IsEmpty() bool {
	return r.FirstIndex == 0 && r.LastIndex == 0 &&
		r.Error == nil && len(r.Entries) == 0
}

// SystemCtx is used to identify a ReadIndex operation.
type SystemCtx struct {
	Low  uint64
	High uint64
}

// ReadyToRead is used to indicate that a previous batch of ReadIndex requests
// are now ready for read once the entry specified by the Index value is applied in
// the state machine.
type ReadyToRead struct {
	Index     uint64
	SystemCtx SystemCtx
}

// UpdateCommit is used to describe how to commit the Update instance to
// progress the state of raft.
type UpdateCommit struct {
	// the last index known to be pushed to rsm for execution.
	Processed uint64
	// the last index confirmed to be executed.
	LastApplied      uint64
	StableLogTo      uint64
	StableLogTerm    uint64
	StableSnapshotTo uint64
	ReadyToRead      uint64
}

// Update is a collection of state, entries and messages that are expected to be
// processed by raft's upper layer to progress the raft node modelled as state
// machine.
type Update struct {
	ShardID   uint64
	ReplicaID uint64
	// The current persistent state of a raft node. It must be stored onto
	// persistent storage before any non-replication can be sent to other nodes.
	// isStateEqual(emptyState) returns true when the state is empty.
	State
	// whether CommittedEntries can be applied without waiting for the Update
	// to be persisted to disk
	FastApply bool
	// EntriesToSave are entries waiting to be stored onto persistent storage.
	EntriesToSave []Entry
	// CommittedEntries are entries already committed in raft and ready to be
	// applied by dragonboat applications.
	CommittedEntries []Entry
	// Whether there are more committed entries ready to be applied.
	MoreCommittedEntries bool
	// Snapshot is the metadata of the snapshot ready to be applied.
	Snapshot Snapshot
	// ReadyToReads provides a list of ReadIndex requests ready for local read.
	ReadyToReads []ReadyToRead
	// Messages is a list of outgoing messages to be sent to remote nodes.
	// As stated above, replication messages can be immediately sent, all other
	// messages must be sent after the persistent state and entries are saved
	// onto persistent storage.
	Messages []Message
	// LastApplied is the actual last applied index reported by the RSM.
	LastApplied uint64
	// UpdateCommit contains info on how the Update instance can be committed
	// to actually progress the state of raft.
	UpdateCommit UpdateCommit
	// DroppedEntries is a list of entries dropped when no leader is available
	DroppedEntries []Entry
	// DroppedReadIndexes is a list of read index requests  dropped when no leader
	// is available.
	DroppedReadIndexes []SystemCtx
	LogQueryResult     LogQueryResult
	LeaderUpdate       LeaderUpdate
}

// HasUpdate returns a boolean value indicating whether the returned Update
// instance actually has any update to be processed.
func (u *Update) HasUpdate() bool {
	return !IsEmptyState(u.State) ||
		!IsEmptySnapshot(u.Snapshot) ||
		len(u.EntriesToSave) > 0 ||
		len(u.CommittedEntries) > 0 ||
		len(u.Messages) > 0 ||
		len(u.ReadyToReads) > 0 ||
		len(u.DroppedEntries) > 0
}

// MarshalTo encodes the fields that need to be persisted to the specified
// buffer.
func (u *Update) MarshalTo(buf []byte) (int, error) {
	n1 := binary.PutUvarint(buf, u.ShardID)
	n2 := binary.PutUvarint(buf[n1:], u.ReplicaID)
	offset := n1 + n2
	if IsEmptyState(u.State) {
		buf[offset] = 0
		offset++
	} else {
		buf[offset] = 1
		offset++
		n, err := u.State.MarshalTo(buf[offset+4:])
		if err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint32(buf[offset:], uint32(n))
		offset += (n + 4)
	}
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(u.EntriesToSave)))
	offset += 4
	for _, e := range u.EntriesToSave {
		n, err := e.MarshalTo(buf[offset+4:])
		if err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint32(buf[offset:], uint32(n))
		offset += (n + 4)
	}
	if IsEmptySnapshot(u.Snapshot) {
		buf[offset] = 0
		offset++
	} else {
		buf[offset] = 1
		offset++
		n, err := u.Snapshot.MarshalTo(buf[offset+4:])
		if err != nil {
			return 0, err
		}
		binary.LittleEndian.PutUint32(buf[offset:], uint32(n))
		offset += (n + 4)
	}
	return offset, nil
}

type countedByteReader struct {
	reader io.ByteReader
	count  int
}

func (r *countedByteReader) ReadByte() (byte, error) {
	v, err := r.reader.ReadByte()
	r.count++
	return v, err
}

// Unmarshal decodes the Update state from the input buf.
func (u *Update) Unmarshal(buf []byte) error {
	r := &countedByteReader{
		reader: bytes.NewReader(buf),
	}
	var err error
	u.ShardID, err = binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	u.ReplicaID, err = binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	offset := r.count
	if buf[offset] == 0 {
		offset++
	} else {
		offset++
		l := binary.LittleEndian.Uint32(buf[offset:])
		if err := u.State.Unmarshal(buf[offset+4 : offset+4+int(l)]); err != nil {
			return err
		}
		offset += (4 + int(l))
	}
	count := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	if count > 0 {
		u.EntriesToSave = make([]Entry, count)
	}
	for i := uint32(0); i < count; i++ {
		l := binary.LittleEndian.Uint32(buf[offset:])
		var entry Entry
		if err := entry.Unmarshal(buf[offset+4 : offset+4+int(l)]); err != nil {
			return err
		}
		u.EntriesToSave[i] = entry
		offset += (4 + int(l))
	}
	if buf[offset] == 1 {
		offset++
		l := binary.LittleEndian.Uint32(buf[offset:])
		if err := u.Snapshot.Unmarshal(buf[offset+4 : offset+4+int(l)]); err != nil {
			return err
		}
	}
	return nil
}

// SizeUpperLimit returns the upper limit of the estimated size of marshalled
// Update instance.
func (u *Update) SizeUpperLimit() int {
	sz := 2 + 4 + 16
	sz += int(GetEntrySliceSize(u.EntriesToSave))
	sz += u.State.SizeUpperLimit()
	if !IsEmptySnapshot(u.Snapshot) {
		sz += u.Snapshot.Size()
	} else {
		sz += 48
	}
	return sz
}
