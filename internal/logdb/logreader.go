// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//
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
//
//
// The implementation of the LogReader struct below is influenced by
// CockroachDB's replicaRaftStorage.

package logdb

import (
	"fmt"
	"sync"

	"github.com/lni/dragonboat/v3/internal/raft"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

// LogReader is the struct used to manage logs that have already been persisted
// into LogDB. This implementation is influenced by CockroachDB's
// replicaRaftStorage.
type LogReader struct {
	sync.Mutex
	clusterID   uint64
	nodeID      uint64
	logdb       raftio.ILogDB
	state       pb.State
	snapshot    pb.Snapshot
	markerIndex uint64
	markerTerm  uint64
	length      uint64
}

// NewLogReader creates and returns a new LogReader instance.
func NewLogReader(clusterID uint64,
	nodeID uint64, logdb raftio.ILogDB) *LogReader {
	l := &LogReader{
		logdb:     logdb,
		clusterID: clusterID,
		nodeID:    nodeID,
		length:    1,
	}
	return l
}

func (lr *LogReader) id() string {
	return fmt.Sprintf("logreader %s index %d term %d length %d",
		logutil.DescribeNode(lr.clusterID, lr.nodeID),
		lr.markerIndex, lr.markerTerm, lr.length)
}

// NodeState returns the initial state.
func (lr *LogReader) NodeState() (pb.State, pb.Membership) {
	lr.Lock()
	defer lr.Unlock()
	return lr.state, lr.snapshot.Membership
}

// Entries returns persisted entries between [low, high) with a total limit of
// up to maxSize bytes.
func (lr *LogReader) Entries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, error) {
	ents, size, err := lr.entries(low, high, maxSize)
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && size > maxSize && len(ents) > 1 {
		return ents[:len(ents)-1], nil
	} else if maxSize == 0 && size > maxSize && len(ents) > 1 {
		return ents[:1], nil
	}
	return ents, nil
}

func (lr *LogReader) entries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.entriesLocked(low, high, maxSize)
}

func (lr *LogReader) entriesLocked(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	if low > high {
		return nil, 0, fmt.Errorf("high (%d) < low (%d)", high, low)
	}
	if low <= lr.markerIndex {
		return nil, 0, raft.ErrCompacted
	}
	if high > lr.lastIndex()+1 {
		plog.Errorf("%s, low %d high %d, lastIndex %d",
			lr.id(), low, high, lr.lastIndex())
		return nil, 0, raft.ErrUnavailable
	}
	ents := make([]pb.Entry, 0, high-low)
	size := uint64(0)
	hitIndex := low
	ents, size, err := lr.logdb.IterateEntries(ents, size, lr.clusterID,
		lr.nodeID, hitIndex, high, maxSize)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(ents)) == high-low || size > maxSize {
		return ents, size, nil
	}
	if len(ents) > 0 {
		if ents[0].Index > low {
			return nil, 0, raft.ErrCompacted
		}
		expected := ents[len(ents)-1].Index + 1
		if lr.lastIndex() <= expected {
			plog.Errorf("%s, %v, low %d high %d, expected %d, lastIndex %d",
				lr.id(), raft.ErrUnavailable, low, high, expected, lr.lastIndex())
			return nil, 0, raft.ErrUnavailable
		}
		return nil, 0, fmt.Errorf("gap found between [%d:%d) at %d",
			low, high, expected)
	}
	plog.Warningf("%s failed to get anything from logreader", lr.id())
	return nil, 0, raft.ErrUnavailable
}

// Term returns the term of the entry specified by the entry index.
func (lr *LogReader) Term(index uint64) (uint64, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.termLocked(index)
}

func (lr *LogReader) termLocked(index uint64) (uint64, error) {
	if index == lr.markerIndex {
		t := lr.markerTerm
		return t, nil
	}
	ents, _, err := lr.entriesLocked(index, index+1, 0)
	if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// GetRange returns the index range of all logs managed by the LogReader
// instance.
func (lr *LogReader) GetRange() (uint64, uint64) {
	lr.Lock()
	defer lr.Unlock()
	return lr.firstIndex(), lr.lastIndex()
}

func (lr *LogReader) firstIndex() uint64 {
	return lr.markerIndex + 1
}

func (lr *LogReader) lastIndex() uint64 {
	return lr.markerIndex + lr.length - 1
}

// Snapshot returns the metadata of the lastest snapshot.
func (lr *LogReader) Snapshot() pb.Snapshot {
	lr.Lock()
	defer lr.Unlock()
	return lr.snapshot
}

// ApplySnapshot applies the specified snapshot.
func (lr *LogReader) ApplySnapshot(snapshot pb.Snapshot) error {
	lr.Lock()
	defer lr.Unlock()
	if lr.snapshot.Index >= snapshot.Index {
		return raft.ErrSnapshotOutOfDate
	}
	lr.snapshot = snapshot
	lr.markerIndex = snapshot.Index
	lr.markerTerm = snapshot.Term
	lr.length = 1
	return nil
}

// CreateSnapshot keeps the metadata of the specified snapshot.
func (lr *LogReader) CreateSnapshot(snapshot pb.Snapshot) error {
	lr.Lock()
	defer lr.Unlock()
	if lr.snapshot.Index >= snapshot.Index {
		return raft.ErrSnapshotOutOfDate
	}
	lr.snapshot = snapshot
	return nil
}

// Append marks the specified entries as persisted and make them available from
// logreader.
func (lr *LogReader) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) > 0 {
		if entries[0].Index+uint64(len(entries))-1 != entries[len(entries)-1].Index {
			panic("gap in entries")
		}
	}
	lr.SetRange(entries[0].Index, uint64(len(entries)))
	return nil
}

// SetRange updates the LogReader to reflect what is available in it.
func (lr *LogReader) SetRange(firstIndex uint64, length uint64) {
	if length == 0 {
		return
	}
	lr.Lock()
	defer lr.Unlock()
	first := lr.firstIndex()
	last := firstIndex + length - 1
	if last < first {
		return
	}
	if first > firstIndex {
		cut := first - firstIndex
		firstIndex = first
		length -= cut
	}
	offset := firstIndex - lr.markerIndex
	switch {
	case lr.length > offset:
		lr.length = offset + length
	case lr.length == offset:
		lr.length = lr.length + length
	default:
		panic("missing log entry")
	}
}

// SetState sets the persistent state.
func (lr *LogReader) SetState(s pb.State) {
	lr.Lock()
	defer lr.Unlock()
	lr.state = s
}

// Compact compacts raft log entries up to index.
func (lr *LogReader) Compact(index uint64) error {
	lr.Lock()
	defer lr.Unlock()
	if index < lr.markerIndex {
		return raft.ErrCompacted
	}
	if index > lr.lastIndex() {
		return raft.ErrUnavailable
	}
	term, err := lr.termLocked(index)
	if err != nil {
		return err
	}
	i := index - lr.markerIndex
	lr.length = lr.length - i
	lr.markerIndex = index
	lr.markerTerm = term
	return nil
}
