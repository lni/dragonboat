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

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
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
func (m *Entry) IsProposal() bool {
	return m.Type == ApplicationEntry ||
		m.Type == EncodedEntry || m.Type == MetadataEntry
}

// IsConfigChange returns a boolean value indicating whether the entry is for
// config change.
func (m *Entry) IsConfigChange() bool {
	return m.Type == ConfigChangeEntry
}

// IsEmpty returns a boolean value indicating whether the entry is Empty.
func (m *Entry) IsEmpty() bool {
	if m.IsConfigChange() {
		return false
	}
	if m.IsSessionManaged() {
		return false
	}
	return len(m.Cmd) == 0
}

// IsSessionManaged returns a boolean value indicating whether the entry is
// session managed.
func (m *Entry) IsSessionManaged() bool {
	if m.IsConfigChange() {
		return false
	}
	if m.ClientID == client.NotSessionManagedClientID {
		return false
	}
	return true
}

// IsNoOPSession returns a boolean value indicating whether the entry is NoOP
// session managed.
func (m *Entry) IsNoOPSession() bool {
	return m.SeriesID == client.NoOPSeriesID
}

// IsNewSessionRequest returns a boolean value indicating whether the entry is
// for reqeusting a new client.
func (m *Entry) IsNewSessionRequest() bool {
	return !m.IsConfigChange() &&
		len(m.Cmd) == 0 &&
		m.ClientID != client.NotSessionManagedClientID &&
		m.SeriesID == client.SeriesIDForRegister
}

// IsEndOfSessionRequest returns a boolean value indicating whether the entry
// is for requesting the session to come to an end.
func (m *Entry) IsEndOfSessionRequest() bool {
	return !m.IsConfigChange() &&
		len(m.Cmd) == 0 &&
		m.ClientID != client.NotSessionManagedClientID &&
		m.SeriesID == client.SeriesIDForUnregister
}

// IsUpdateEntry returns a boolean flag indicating whether the entry is a
// regular application entry not used for session management.
func (m *Entry) IsUpdateEntry() bool {
	return !m.IsConfigChange() && m.IsSessionManaged() &&
		!m.IsNewSessionRequest() && !m.IsEndOfSessionRequest()
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
func (m *SnapshotFile) Filename() string {
	return fmt.Sprintf("external-file-%d", m.FileId)
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
	Close() error
	ShardID() uint64
	ToReplicaID() uint64
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
func (m Chunk) IsLastChunk() bool {
	return m.ChunkCount == LastChunkCount || m.ChunkCount == m.ChunkId+1
}

// IsLastFileChunk returns a boolean value indicating whether the chunk is the
// last chunk of a snapshot file.
func (m Chunk) IsLastFileChunk() bool {
	return m.FileChunkId+1 == m.FileChunkCount
}

// IsPoisonChunk returns a boolean value indicating whether the chunk is a
// special poison chunk.
func (m Chunk) IsPoisonChunk() bool {
	return m.ChunkCount == PoisonChunkCount
}

// CanDrop returns a boolean value indicating whether the message can be
// safely dropped.
func (m *Message) CanDrop() bool {
	return m.Type != InstallSnapshot &&
		m.Type != Unreachable && m.Type != SnapshotStatus
}

// Marshaler is the interface for instances that can be marshalled.
type Marshaler interface {
	Marshal() ([]byte, error)
	MarshalTo([]byte) (int, error)
}

// Unmarshaler is the interface for instances that can be unmarshalled.
type Unmarshaler interface {
	Unmarshal([]byte) error
}

// MustMarshal marshals the input object or panic if there is any error.
func MustMarshal(m Marshaler) []byte {
	data, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

// MustMarshalTo marshals the input object to the specified buffer or panic
// if there is any error.
func MustMarshalTo(m Marshaler, result []byte) []byte {
	sz, err := m.MarshalTo(result)
	if err != nil {
		panic(err)
	}
	return result[:sz]
}

// MustUnmarshal unmarshals the specified object using the provided marshalled
// data. MustUnmarshal will panic if there is any error.
func MustUnmarshal(m Unmarshaler, data []byte) {
	if err := m.Unmarshal(data); err != nil {
		panic(err)
	}
}
