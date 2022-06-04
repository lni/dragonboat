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
	"math"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

// TestLogDB is used in raft test only. It is basically a logdb.logreader
// backed by a []entry.
type TestLogDB struct {
	entries     []pb.Entry
	markerIndex uint64
	markerTerm  uint64
	snapshot    pb.Snapshot
	state       pb.State
}

func NewTestLogDB() ILogDB {
	return &TestLogDB{
		entries: make([]pb.Entry, 0),
	}
}

func (db *TestLogDB) SetState(s pb.State) {
	db.state = s
}

func (db *TestLogDB) NodeState() (pb.State, pb.Membership) {
	return db.state, db.snapshot.Membership
}

func (db *TestLogDB) Snapshot() pb.Snapshot {
	return db.snapshot
}

func (db *TestLogDB) ApplySnapshot(ss pb.Snapshot) error {
	if db.snapshot.Index >= ss.Index {
		return ErrSnapshotOutOfDate
	}
	db.snapshot = ss
	db.markerIndex = ss.Index
	db.markerTerm = ss.Term
	db.entries = make([]pb.Entry, 0)
	return nil
}

func (db *TestLogDB) CreateSnapshot(ss pb.Snapshot) error {
	if db.snapshot.Index >= ss.Index {
		return ErrSnapshotOutOfDate
	}
	db.snapshot = ss
	return nil
}

func (db *TestLogDB) getSnapshot(index uint64,
	cs *pb.Membership) (pb.Snapshot, error) {
	if index <= db.snapshot.Index {
		return pb.Snapshot{}, ErrSnapshotOutOfDate
	}

	offset := db.markerIndex
	if index > db.lastIndex() {
		plog.Panicf("snapshot %d is out of bound lastindex(%d)",
			index, db.lastIndex())
	}
	ss := pb.Snapshot{
		Index: index,
		Term:  db.entries[index-offset-1].Term,
	}
	if cs != nil {
		ss.Membership = *cs
	}
	return ss, nil
}

func (db *TestLogDB) GetRange() (uint64, uint64) {
	return db.firstIndex(), db.lastIndex()
}

func (db *TestLogDB) firstIndex() uint64 {
	return db.markerIndex + 1
}

func (db *TestLogDB) lastIndex() uint64 {
	return db.markerIndex + uint64(len(db.entries))
}

func (db *TestLogDB) SetRange(firstIndex uint64, length uint64) {
	panic("not implemented")
}

func (db *TestLogDB) Term(index uint64) (uint64, error) {
	if index == db.markerIndex {
		return db.markerTerm, nil
	}
	ents, err := db.Entries(index, index+1, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

func (db *TestLogDB) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := db.firstIndex()
	if db.markerIndex+uint64(len(entries)) < first {
		return nil
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - db.markerIndex
	if uint64(len(db.entries)+1) > offset {
		db.entries = db.entries[:offset-1]
	} else if uint64(len(db.entries)+1) < offset {
		plog.Panicf("found a hole last index %d, first incoming index %d",
			db.lastIndex(), entries[0].Index)
	}
	db.entries = append(db.entries, entries...)
	return nil
}

func (db *TestLogDB) Entries(low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, error) {
	if low <= db.markerIndex {
		return nil, ErrCompacted
	}
	if high > db.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if len(db.entries) == 0 {
		return nil, ErrUnavailable
	}
	ents := db.entries[low-db.markerIndex-1 : high-db.markerIndex-1]
	return limitSize(ents, maxSize), nil
}

func (db *TestLogDB) Compact(index uint64) error {
	if index <= db.markerIndex {
		return ErrCompacted
	}
	if index > db.lastIndex() {
		return ErrUnavailable
	}
	if len(db.entries) == 0 {
		return ErrUnavailable
	}
	term, err := db.Term(index)
	if err != nil {
		return err
	}
	cut := index - db.markerIndex
	db.entries = db.entries[cut:]
	db.markerIndex = index
	db.markerTerm = term
	return nil
}
