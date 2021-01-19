// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/lni/goutils/stringutil"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
)

var (
	plog                = logger.GetLogger("raftpb")
	panicOnSizeMismatch = settings.Soft.PanicOnSizeMismatch
	emptyState          = State{}
)

const (
	// NoNode is the flag used to indicate that the node id field is not set.
	NoNode uint64 = 0
)

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
	ClusterID uint64
	NodeID    uint64
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
}

// HasUpdate returns a boolean value indicating whether the returned Update
// instance actually has any update to be processed.
func (ud *Update) HasUpdate() bool {
	return !IsEmptyState(ud.State) ||
		!IsEmptySnapshot(ud.Snapshot) ||
		len(ud.EntriesToSave) > 0 ||
		len(ud.CommittedEntries) > 0 ||
		len(ud.Messages) > 0 ||
		len(ud.ReadyToReads) > 0 ||
		len(ud.DroppedEntries) > 0
}

// IsEmptyState returns a boolean flag indicating whether the given State is
// empty.
func IsEmptyState(st State) bool {
	return isStateEqual(st, emptyState)
}

// IsEmptySnapshot returns a boolean flag indicating whether the given snapshot
// is and empty dummy record.
func IsEmptySnapshot(s Snapshot) bool {
	return s.Index == 0
}

// IsStateEqual returns whether two input state instances are equal.
func IsStateEqual(a State, b State) bool {
	return isStateEqual(a, b)
}

func isStateEqual(a State, b State) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsProposal returns a boolean value indicating whether the entry is a
// regular update entry.
func (e *Entry) IsProposal() bool {
	return e.Type == ApplicationEntry ||
		e.Type == EncodedEntry || e.Type == MetadataEntry
}

// IsConfigChange returns a boolean value indicating whether the entry is for
// config change.
func (e *Entry) IsConfigChange() bool {
	return e.Type == ConfigChangeEntry
}

// IsEmpty returns a boolean value indicating whether the entry is Empty.
func (e *Entry) IsEmpty() bool {
	if e.IsConfigChange() {
		return false
	}
	if e.IsSessionManaged() {
		return false
	}
	return len(e.Cmd) == 0
}

// IsSessionManaged returns a boolean value indicating whether the entry is
// session managed.
func (e *Entry) IsSessionManaged() bool {
	if e.IsConfigChange() {
		return false
	}
	if e.ClientID == client.NotSessionManagedClientID {
		return false
	}
	return true
}

// IsNoOPSession returns a boolean value indicating whether the entry is NoOP
// session managed.
func (e *Entry) IsNoOPSession() bool {
	return e.SeriesID == client.NoOPSeriesID
}

// IsNewSessionRequest returns a boolean value indicating whether the entry is
// for reqeusting a new client.
func (e *Entry) IsNewSessionRequest() bool {
	return !e.IsConfigChange() &&
		len(e.Cmd) == 0 &&
		e.ClientID != client.NotSessionManagedClientID &&
		e.SeriesID == client.SeriesIDForRegister
}

// IsEndOfSessionRequest returns a boolean value indicating whether the entry
// is for requesting the session to come to an end.
func (e *Entry) IsEndOfSessionRequest() bool {
	return !e.IsConfigChange() &&
		len(e.Cmd) == 0 &&
		e.ClientID != client.NotSessionManagedClientID &&
		e.SeriesID == client.SeriesIDForUnregister
}

// IsUpdateEntry returns a boolean flag indicating whether the entry is a
// regular application entry not used for session management.
func (e *Entry) IsUpdateEntry() bool {
	return !e.IsConfigChange() && e.IsSessionManaged() &&
		!e.IsNewSessionRequest() && !e.IsEndOfSessionRequest()
}

// NewBootstrapInfo creates and returns a new bootstrap record.
func NewBootstrapInfo(join bool,
	smType StateMachineType, nodes map[uint64]string) Bootstrap {
	bootstrap := Bootstrap{
		Join:      join,
		Addresses: make(map[uint64]string),
		Type:      smType,
	}
	for nid, addr := range nodes {
		bootstrap.Addresses[nid] = stringutil.CleanAddress(addr)
	}
	return bootstrap
}

// Validate checks whether the incoming nodes parameter and the join flag is
// valid given the recorded bootstrap infomration in Log DB.
func (b *Bootstrap) Validate(nodes map[uint64]string,
	join bool, smType StateMachineType) bool {
	if b.Type != UnknownStateMachine && b.Type != smType {
		plog.Errorf("recorded sm type %s, got %s", b.Type, smType)
		return false
	}
	if !b.Join && len(b.Addresses) == 0 {
		panic("invalid non-join bootstrap record with 0 address")
	}
	if b.Join && len(nodes) > 0 {
		plog.Errorf("restarting previously joined node, member list %v", nodes)
		return false
	}
	if join && len(b.Addresses) > 0 {
		plog.Errorf("joining node when it is an initial member")
		return false
	}
	valid := true
	if len(nodes) > 0 {
		if len(nodes) != len(b.Addresses) {
			valid = false
		}
		for nid, addr := range nodes {
			ba, ok := b.Addresses[nid]
			if !ok {
				valid = false
			}
			if strings.Compare(ba, stringutil.CleanAddress(addr)) != 0 {
				valid = false
			}
		}
	}
	if !valid {
		plog.Errorf("inconsistent node list, bootstrap %v, incoming %v",
			b.Addresses, nodes)
	}
	return valid
}

func checkFileSize(path string, size uint64, fs vfs.IFS) {
	var er func(format string, args ...interface{})
	if panicOnSizeMismatch {
		er = plog.Panicf
	} else {
		er = plog.Errorf
	}
	fi, err := fs.Stat(path)
	if err != nil {
		plog.Panicf("failed to access %s", path)
	}
	if size != uint64(fi.Size()) {
		er("file %s size %d, expect %d", path, fi.Size(), size)
	}
}

// Validate validates the snapshot instance.
func (snapshot *Snapshot) Validate(fs vfs.IFS) bool {
	if len(snapshot.Filepath) == 0 || snapshot.FileSize == 0 {
		return false
	}
	checkFileSize(snapshot.Filepath, snapshot.FileSize, fs)
	for _, f := range snapshot.Files {
		if len(f.Filepath) == 0 || f.FileSize == 0 {
			return false
		}
		checkFileSize(f.Filepath, f.FileSize, fs)
	}
	return true
}

// Filename returns the filename of the external snapshot file.
func (f *SnapshotFile) Filename() string {
	return fmt.Sprintf("external-file-%d", f.FileId)
}

// GetEntrySliceSize returns the upper limit of the entry slice size.
func GetEntrySliceSize(ents []Entry) uint64 {
	sz := uint64(0)
	for _, e := range ents {
		sz += uint64(e.SizeUpperLimit())
	}
	return sz
}

// GetEntrySliceInMemSize returns the in memory size of the specified entry
// slice. Size 24 bytes used to hold ents itself is not counted.
func GetEntrySliceInMemSize(ents []Entry) uint64 {
	sz := uint64(0)
	if len(ents) == 0 {
		return 0
	}
	stSz := uint64(unsafe.Sizeof(ents[0]))
	for _, e := range ents {
		sz += uint64(len(e.Cmd))
		sz += stSz
	}
	return sz
}

// IChunkSink is the snapshot chunk sink for handling snapshot chunks being
// streamed.
type IChunkSink interface {
	// return (sent, stopped)
	Receive(chunk Chunk) (bool, bool)
	Stop()
	ClusterID() uint64
	ToNodeID() uint64
}

var (
	// LastChunkCount is the special chunk count value used to indicate that the
	// chunk is the last one.
	LastChunkCount uint64 = math.MaxUint64
	// PoisonChunkCount is the special chunk count value used to indicate that
	// the processing goroutine should return.
	PoisonChunkCount uint64 = math.MaxUint64 - 1
)

// IsLastChunk returns a boolean value indicating whether the chunk is the last
// chunk of a snapshot.
func (c Chunk) IsLastChunk() bool {
	return c.ChunkCount == LastChunkCount || c.ChunkCount == c.ChunkId+1
}

// IsLastFileChunk returns a boolean value indicating whether the chunk is the
// last chunk of a snapshot file.
func (c Chunk) IsLastFileChunk() bool {
	return c.FileChunkId+1 == c.FileChunkCount
}

// IsPoisonChunk returns a boolean value indicating whether the chunk is a
// special poison chunk.
func (c Chunk) IsPoisonChunk() bool {
	return c.ChunkCount == PoisonChunkCount
}

// CanDrop returns a boolean value indicating whether the message can be
// safely dropped.
func (m *Message) CanDrop() bool {
	return m.Type != InstallSnapshot &&
		m.Type != Unreachable && m.Type != SnapshotStatus
}
