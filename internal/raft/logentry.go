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

package raft

import (
	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	maxEntriesToApplySize = settings.Soft.MaxApplyEntrySize
)

// ErrCompacted is the error returned to indicate that the requested entries
// are no longer in the LogDB due to compaction.
var ErrCompacted = errors.New("entry compacted")

// ErrSnapshotOutOfDate is the error returned to indicate that the concerned
// snapshot is considered as out of date.
var ErrSnapshotOutOfDate = errors.New("snapshot out of date")

// ErrUnavailable is the error returned to indicate that requested entries are
// not available in LogDB.
var ErrUnavailable = errors.New("entry unavailable")

// ILogDB is a read-only interface to the underlying persistent storage to
// allow the raft package to access raft state, entries, snapshots stored in
// the persistent storage. Entries stored in the persistent storage accessible
// via ILogDB is usually not required in normal cases.
type ILogDB interface {
	// GetRange returns the range of the entries in LogDB.
	GetRange() (uint64, uint64)
	// SetRange updates the ILogDB to extend the entry range known to the ILogDB.
	SetRange(index uint64, length uint64)
	// NodeState returns the state of the node persistent in LogDB.
	NodeState() (pb.State, pb.Membership)
	// SetState sets the persistent state known to ILogDB.
	SetState(ps pb.State)
	// CreateSnapshot sets the snapshot known to ILogDB
	CreateSnapshot(ss pb.Snapshot) error
	// ApplySnapshot makes the snapshot known to ILogDB and also update the entry
	// range known to ILogDB.
	ApplySnapshot(ss pb.Snapshot) error
	// Term returns the entry term of the specified entry.
	Term(index uint64) (uint64, error)
	// Entries returns entries between [low, high) with total size of entries
	// limited to maxSize bytes.
	Entries(low uint64, high uint64, maxSize uint64) ([]pb.Entry, error)
	// Snapshot returns the metadata for the most recent snapshot known to the
	// LogDB.
	Snapshot() pb.Snapshot
	// Compact performs entry range compaction on ILogDB up to the entry
	// specified by index.
	Compact(index uint64) error
	// Append makes the given entries known to the ILogDB instance. This is
	// usually not how entries are persisted.
	Append(entries []pb.Entry) error
}

// entryLog is the entry log used by Raft. It splits entries into two parts -
// those likely to be access in immediate future and those unlikely to be used
// any time soon in normal fast path.
type entryLog struct {
	logdb     ILogDB
	inmem     inMemory
	committed uint64
	// committed entries already returned as Updated to be applied.
	processed uint64
}

func newEntryLog(logdb ILogDB, rl *server.InMemRateLimiter) *entryLog {
	firstIndex, lastIndex := logdb.GetRange()
	l := &entryLog{
		logdb:     logdb,
		inmem:     newInMemory(lastIndex, rl),
		committed: firstIndex - 1,
		processed: firstIndex - 1,
	}
	return l
}

func (l *entryLog) firstIndex() uint64 {
	index, ok := l.inmem.getSnapshotIndex()
	if ok {
		return index + 1
	}

	index, _ = l.logdb.GetRange()
	return index
}

func (l *entryLog) lastIndex() uint64 {
	if index, ok := l.inmem.getLastIndex(); ok {
		return index
	}
	_, index := l.logdb.GetRange()
	return index

}

func (l *entryLog) termEntryRange() (uint64, uint64) {
	// for firstIndex(), when it is determined by the inmem, what we actually
	// want to return is the snapshot index, l.firstIndex() - 1 is thus required
	// when it is determined by the logdb component, other than actual entries
	// we have a marker entry with known index/term (but not type or data),
	// use l.firstIndex()-1 to include this marker element.
	// as we don't have the type/data of the marker entry, it is only used in
	// term(), we can not pull its value and send it to the RSM for execution.
	return l.firstIndex() - 1, l.lastIndex()
}

func (l *entryLog) entryRange() (uint64, uint64, bool) {
	if l.inmem.snapshot != nil && len(l.inmem.entries) == 0 {
		return 0, 0, false
	}
	return l.firstIndex(), l.lastIndex(), true
}

func (l *entryLog) lastTerm() (uint64, error) {
	t, err := l.term(l.lastIndex())
	if err != nil {
		return 0, err
	}
	return t, nil
}

func (l *entryLog) term(index uint64) (uint64, error) {
	first, last := l.termEntryRange()
	if index < first || index > last {
		return 0, nil
	}
	if t, ok := l.inmem.getTerm(index); ok {
		return t, nil
	}
	t, err := l.logdb.Term(index)
	if err != nil {
		return 0, err
	}
	return t, nil
}

func (l *entryLog) checkBound(low uint64, high uint64) error {
	if low > high {
		plog.Panicf("input low %d > high %d", low, high)
	}
	first, last, ok := l.entryRange()
	if !ok {
		return ErrCompacted
	}
	if low < first {
		return ErrCompacted
	}
	if high > last+1 {
		plog.Panicf("requested range [%d,%d) is out of bound [%d,%d]",
			low, high, first, last)
	}
	return nil
}

func (l *entryLog) getUncommittedEntries() []pb.Entry {
	lastIndex := l.inmem.markerIndex + uint64(len(l.inmem.entries))
	return l.getEntriesFromInMem([]pb.Entry{}, l.committed+1, lastIndex)
}

func (l *entryLog) getEntriesFromLogDB(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, bool, error) {
	if low >= l.inmem.markerIndex {
		return nil, true, nil
	}

	upperBound := min(high, l.inmem.markerIndex)
	ents, err := l.logdb.Entries(low, upperBound, maxSize)
	if err != nil {
		return nil, false, err
	}
	if uint64(len(ents)) > upperBound-low {
		plog.Panicf("uint64(len(ents)) > upperBound-low")
	}
	return ents, uint64(len(ents)) == upperBound-low, nil
}

func (l *entryLog) getEntriesFromInMem(ents []pb.Entry,
	low uint64, high uint64) []pb.Entry {
	if high <= l.inmem.markerIndex {
		return ents
	}
	lowerBound := max(low, l.inmem.markerIndex)
	inmem := l.inmem.getEntries(lowerBound, high)
	if len(inmem) > 0 {
		if len(ents) > 0 {
			checkEntriesToAppend(ents, inmem)
			return append(ents, inmem...)
		}
		return inmem
	}
	return ents
}

func (l *entryLog) getEntries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, error) {
	err := l.checkBound(low, high)
	if err != nil {
		return nil, err
	}
	if low == high {
		return nil, nil
	}
	ents, checkInMem, err := l.getEntriesFromLogDB(low, high, maxSize)
	if err != nil {
		return nil, err
	}
	if !checkInMem {
		return ents, nil
	}
	return limitSize(l.getEntriesFromInMem(ents, low, high), maxSize), nil
}

func (l *entryLog) entries(start uint64, maxSize uint64) ([]pb.Entry, error) {
	if start > l.lastIndex() {
		return nil, nil
	}
	return l.getEntries(start, l.lastIndex()+1, maxSize)
}

// TODO: double check whether the inmem.snapshot can be used in upper layer
func (l *entryLog) snapshot() pb.Snapshot {
	if l.inmem.snapshot != nil {
		return *l.inmem.snapshot
	}
	return l.logdb.Snapshot()
}

func (l *entryLog) firstNotAppliedIndex() uint64 {
	return max(l.processed+1, l.firstIndex())
}

func (l *entryLog) toApplyIndexLimit() uint64 {
	return l.committed + 1
}

func (l *entryLog) hasEntriesToApply() bool {
	return l.toApplyIndexLimit() > l.firstNotAppliedIndex()
}

func (l *entryLog) hasMoreEntriesToApply(appliedTo uint64) bool {
	return l.committed > appliedTo
}

func (l *entryLog) entriesToApply() ([]pb.Entry, error) {
	return l.getEntriesToApply(maxEntriesToApplySize)
}

func (l *entryLog) getEntriesToApply(limit uint64) ([]pb.Entry, error) {
	if l.hasEntriesToApply() {
		ents, err := l.getEntries(l.firstNotAppliedIndex(),
			l.toApplyIndexLimit(), limit)
		if err != nil {
			return nil, err
		}
		return ents, nil
	}
	return nil, nil
}

func (l *entryLog) getCommittedEntries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, error) {
	if low < l.firstIndex() || low > l.committed {
		return nil, ErrCompacted
	}
	high = min(high, l.committed+1)
	if low == high {
		return nil, nil
	}
	return l.getEntries(low, high, maxSize)
}

func (l *entryLog) entriesToSave() []pb.Entry {
	return l.inmem.entriesToSave()
}

func (l *entryLog) tryAppend(index uint64, ents []pb.Entry) (bool, error) {
	conflictIndex, err := l.getConflictIndex(ents)
	if err != nil {
		return false, err
	}
	if conflictIndex != 0 {
		if conflictIndex <= l.committed {
			plog.Panicf("entry %d conflicts with committed entry, committed %d",
				conflictIndex, l.committed)
		}
		l.append(ents[conflictIndex-index-1:])
		return true, nil
	}
	return false, nil
}

func (l *entryLog) append(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	if entries[0].Index <= l.committed {
		plog.Panicf("committed entries being changed, committed %d, first idx %d",
			l.committed, entries[0].Index)
	}
	l.inmem.merge(entries)
}

func (l *entryLog) getConflictIndex(entries []pb.Entry) (uint64, error) {
	for _, e := range entries {
		match, err := l.matchTerm(e.Index, e.Term)
		if err != nil {
			return 0, err
		}
		if !match {
			return e.Index, nil
		}
	}
	return 0, nil
}

func (l *entryLog) commitTo(index uint64) {
	if index <= l.committed {
		return
	}
	if index > l.lastIndex() {
		plog.Panicf("invalid commitTo index %d, lastIndex() %d",
			index, l.lastIndex())
	}
	if index < l.committed {
		plog.Panicf("committed value moving backwards index %d, committed %d",
			index, l.committed)
	}
	l.committed = index
}

func (l *entryLog) commitUpdate(cu pb.UpdateCommit) {
	l.inmem.commitUpdate(cu)
	if cu.Processed > 0 {
		if cu.Processed < l.processed || cu.Processed > l.committed {
			plog.Panicf("invalid ApplyReturnedTo %d, current applied %d, committed %d",
				cu.Processed, l.processed, l.committed)
		}
		l.processed = cu.Processed
	}
	if cu.LastApplied > 0 {
		if cu.LastApplied > l.committed {
			plog.Panicf("invalid last applied %d, committed %d",
				cu.LastApplied, l.committed)
		}
		if cu.LastApplied > l.processed {
			plog.Panicf("invalid last applied %d, processed %d",
				cu.LastApplied, l.processed)
		}
		l.inmem.appliedLogTo(cu.LastApplied)
	}
}

func (l *entryLog) matchTerm(index uint64, term uint64) (bool, error) {
	lt, err := l.term(index)
	if err != nil {
		return false, err
	}
	return lt == term, nil
}

func (l *entryLog) upToDate(index uint64, term uint64) (bool, error) {
	lastTerm, err := l.term(l.lastIndex())
	if err != nil {
		return false, err
	}
	if term >= lastTerm {
		if term > lastTerm {
			return true, nil
		}
		return index >= l.lastIndex(), nil
	}
	return false, nil
}

func (l *entryLog) tryCommit(index uint64, term uint64) (bool, error) {
	if index <= l.committed {
		return false, nil
	}
	lterm, err := l.term(index)
	if errors.Is(err, ErrCompacted) {
		lterm = 0
	} else if err != nil {
		return false, err
	}
	if index > l.committed && lterm == term {
		l.commitTo(index)
		return true, nil
	}
	return false, nil
}

func (l *entryLog) restore(s pb.Snapshot) {
	l.inmem.restore(s)
	if s.Index < l.committed {
		plog.Panicf("committed value moving backwards ss index %d, committed %d",
			s.Index, l.committed)
	}
	l.committed = s.Index
	l.processed = s.Index
}
