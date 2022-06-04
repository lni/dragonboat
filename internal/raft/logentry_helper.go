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

package raft

import (
	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/internal/server"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// LogTestHelper is a helper type used for testing logEntry.
type LogTestHelper struct {
	el *entryLog
}

// NewLog creates and returns a new LogTestHelper instance used for testing
// purpose.
func NewLog(logdb ILogDB) *LogTestHelper {
	return &LogTestHelper{
		el: newEntryLog(logdb, server.NewInMemRateLimiter(0)),
	}
}

// GetConflictIndex ...
func (l *LogTestHelper) GetConflictIndex(ents []pb.Entry) (uint64, error) {
	return l.el.getConflictIndex(ents)
}

// Term ...
func (l *LogTestHelper) Term(index uint64) (uint64, error) {
	return l.el.term(index)
}

// MatchTerm ...
func (l *LogTestHelper) MatchTerm(index uint64, term uint64) (bool, error) {
	return l.el.matchTerm(index, term)
}

// FirstIndex ...
func (l *LogTestHelper) FirstIndex() uint64 {
	return l.el.firstIndex()
}

// LastIndex ...
func (l *LogTestHelper) LastIndex() uint64 {
	return l.el.lastIndex()
}

// UpToDate ...
func (l *LogTestHelper) UpToDate(index uint64, term uint64) (bool, error) {
	return l.el.upToDate(index, term)
}

// Append ...
func (l *LogTestHelper) Append(ents []pb.Entry) error {
	l.el.append(ents)
	return nil
}

// AllEntries ...
func (l *LogTestHelper) AllEntries() []pb.Entry {
	ents, err := l.el.entries(l.el.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if errors.Is(err, ErrCompacted) {
		return l.AllEntries()
	}
	panic(err)
}

// Entries ...
func (l *LogTestHelper) Entries(start uint64,
	maxsize uint64) ([]pb.Entry, error) {
	return l.el.entries(start, maxsize)
}

// EntriesToSave ...
func (l *LogTestHelper) EntriesToSave() []pb.Entry {
	return l.el.entriesToSave()
}

// UnstableOffset ...
func (l *LogTestHelper) UnstableOffset() uint64 {
	return l.el.inmem.markerIndex
}

// SetCommitted ...
func (l *LogTestHelper) SetCommitted(v uint64) {
	l.el.committed = v
}

// GetCommitted ...
func (l *LogTestHelper) GetCommitted() uint64 {
	return l.el.committed
}

// TryAppend ...
func (l *LogTestHelper) TryAppend(index uint64, logTerm uint64,
	committed uint64, ents []pb.Entry) (uint64, bool, error) {
	match, err := l.el.matchTerm(index, logTerm)
	if err != nil {
		return 0, false, err
	}
	if match {
		if _, err := l.el.tryAppend(index, ents); err != nil {
			return 0, false, err
		}
		lastIndex := index + uint64(len(ents))
		l.el.commitTo(min(lastIndex, committed))
		return lastIndex, true, nil
	}
	return 0, false, nil
}

// GetEntries ...
func (l *LogTestHelper) GetEntries(low uint64, high uint64,
	maxsize uint64) ([]pb.Entry, error) {
	return l.el.getEntries(low, high, maxsize)
}

// TryCommit ...
func (l *LogTestHelper) TryCommit(index uint64, term uint64) (bool, error) {
	return l.el.tryCommit(index, term)
}

// AppliedTo ...
func (l *LogTestHelper) AppliedTo(index uint64) {
	l.el.commitUpdate(pb.UpdateCommit{Processed: index})
}

// HasEntriesToApply ...
func (l *LogTestHelper) HasEntriesToApply() bool {
	return l.el.hasEntriesToApply()
}

// EntriesToApply ...
func (l *LogTestHelper) EntriesToApply() ([]pb.Entry, error) {
	return l.el.entriesToApply()
}

// CheckBound ...
func (l *LogTestHelper) CheckBound(low uint64, high uint64) error {
	return l.el.checkBound(low, high)
}
