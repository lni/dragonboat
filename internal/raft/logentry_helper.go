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

package raft

import (
	"github.com/lni/dragonboat/v3/internal/server"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

// LogTestHelper is a helper type used for testing logEntry.
type LogTestHelper struct {
	el *entryLog
}

// NewLog creates and returns a new LogTestHelper instance used for testing
// purpose.
func NewLog(logdb ILogDB) *LogTestHelper {
	return &LogTestHelper{
		el: newEntryLog(logdb, server.NewRateLimiter(0)),
	}
}

func (l *LogTestHelper) GetConflictIndex(ents []pb.Entry) uint64 {
	return l.el.getConflictIndex(ents)
}

func (l *LogTestHelper) Term(index uint64) (uint64, error) {
	return l.el.term(index)
}

func (l *LogTestHelper) MatchTerm(index uint64, term uint64) bool {
	return l.el.matchTerm(index, term)
}

func (l *LogTestHelper) FirstIndex() uint64 {
	return l.el.firstIndex()
}

func (l *LogTestHelper) LastIndex() uint64 {
	return l.el.lastIndex()
}

func (l *LogTestHelper) UpToDate(index uint64, term uint64) bool {
	return l.el.upToDate(index, term)
}

func (l *LogTestHelper) Append(ents []pb.Entry) error {
	l.el.append(ents)
	return nil
}

func (l *LogTestHelper) AllEntries() []pb.Entry {
	ents, err := l.el.entries(l.el.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		return l.AllEntries()
	}
	panic(err)
}

func (l *LogTestHelper) Entries(start uint64,
	maxsize uint64) ([]pb.Entry, error) {
	return l.el.entries(start, maxsize)
}

func (l *LogTestHelper) EntriesToSave() []pb.Entry {
	return l.el.entriesToSave()
}

func (l *LogTestHelper) UnstableOffset() uint64 {
	return l.el.inmem.markerIndex
}

func (l *LogTestHelper) SetCommitted(v uint64) {
	l.el.committed = v
}

func (l *LogTestHelper) GetCommitted() uint64 {
	return l.el.committed
}

func (l *LogTestHelper) TryAppend(index uint64, logTerm uint64,
	committed uint64, ents []pb.Entry) (uint64, bool) {
	if l.el.matchTerm(index, logTerm) {
		l.el.tryAppend(index, ents)
		lastIndex := index + uint64(len(ents))
		l.el.commitTo(min(lastIndex, committed))
		return lastIndex, true
	}
	return 0, false
}

func (l *LogTestHelper) GetEntries(low uint64, high uint64,
	maxsize uint64) ([]pb.Entry, error) {
	return l.el.getEntries(low, high, maxsize)
}

func (l *LogTestHelper) TryCommit(index uint64, term uint64) bool {
	return l.el.tryCommit(index, term)
}

func (l *LogTestHelper) AppliedTo(index uint64) {
	l.el.commitUpdate(pb.UpdateCommit{Processed: index})
}

func (l *LogTestHelper) HasEntriesToApply() bool {
	return l.el.hasEntriesToApply()
}

func (l *LogTestHelper) EntriesToApply() []pb.Entry {
	return l.el.entriesToApply()
}

func (l *LogTestHelper) CheckBound(low uint64, high uint64) error {
	return l.el.checkBound(low, high)
}
