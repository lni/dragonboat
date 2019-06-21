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

// +build dragonboat_logdbtesthelper

package raft

import (
	"github.com/lni/dragonboat/v3/internal/server"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

func NewLog(logdb ILogDB) *entryLog {
	return newEntryLog(logdb, server.NewRateLimiter(0))
}

func (l *entryLog) GetConflictIndex(ents []pb.Entry) uint64 {
	return l.getConflictIndex(ents)
}

func (l *entryLog) Term(index uint64) (uint64, error) {
	return l.term(index)
}

func (l *entryLog) MatchTerm(index uint64, term uint64) bool {
	return l.matchTerm(index, term)
}

func (l *entryLog) FirstIndex() uint64 {
	return l.firstIndex()
}

func (l *entryLog) LastIndex() uint64 {
	return l.lastIndex()
}

func (l *entryLog) UpToDate(index uint64, term uint64) bool {
	return l.upToDate(index, term)
}

func (l *entryLog) Append(ents []pb.Entry) error {
	l.append(ents)
	return nil
}

func (l *entryLog) AllEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		return l.AllEntries()
	}
	panic(err)
}

func (l *entryLog) Entries(start uint64, maxsize uint64) ([]pb.Entry, error) {
	return l.entries(start, maxsize)
}

func (l *entryLog) EntriesToSave() []pb.Entry {
	return l.entriesToSave()
}

func (l *entryLog) UnstableOffset() uint64 {
	return l.inmem.markerIndex
}

func (l *entryLog) SetCommitted(v uint64) {
	l.committed = v
}

func (l *entryLog) GetCommitted() uint64 {
	return l.committed
}

func (l *entryLog) TryAppend(index uint64, logTerm uint64, committed uint64,
	ents []pb.Entry) (uint64, bool) {
	if l.matchTerm(index, logTerm) {
		l.tryAppend(index, ents)
		lastIndex := index + uint64(len(ents))
		l.commitTo(min(lastIndex, committed))
		return lastIndex, true
	}
	return 0, false
}

func (l *entryLog) GetEntries(low uint64, high uint64,
	maxsize uint64) ([]pb.Entry, error) {
	return l.getEntries(low, high, maxsize)
}

func (l *entryLog) TryCommit(index uint64, term uint64) bool {
	return l.tryCommit(index, term)
}

func (l *entryLog) AppliedTo(index uint64) {
	l.commitUpdate(pb.UpdateCommit{Processed: index})
}

func (l *entryLog) HasEntriesToApply() bool {
	return l.hasEntriesToApply()
}

func (l *entryLog) EntriesToApply() []pb.Entry {
	return l.entriesToApply()
}

func (l *entryLog) CheckBound(low uint64, high uint64) error {
	return l.checkBound(low, high)
}
