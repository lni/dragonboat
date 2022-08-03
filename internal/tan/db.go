// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package tan

import (
	"bytes"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
)

var (
	plog = logger.GetLogger("tan")
)

const (
	stateFlag      uint64 = math.MaxUint64
	snapshotFlag   uint64 = math.MaxUint64 - 1
	compactionFlag uint64 = math.MaxUint64 - 2
)

var (
	// ErrClosed is the error used to indicate that the db has already been closed
	ErrClosed = errors.New("db closed")
	// ErrNoBootstrap is the error used to indicate that there is no saved
	// bootstrap record
	ErrNoBootstrap = errors.New("no bootstrap info")
	// ErrNoState is the error indicating that there is no state record in the db
	ErrNoState = errors.New("no state record")
)

// db is basically an instance of the core tan storage, it holds required
// resources and manages log and index data.
type db struct {
	name     string
	closed   atomic.Value
	closedCh chan struct{}
	opts     *Options
	dataDir  vfs.File
	dirname  string

	// for asynchronously delete files in the background
	deleteObsoleteCh     chan struct{}
	deleteobsoleteWorker *syncutil.Stopper

	// help to determine what is the current visible view of the data
	readState struct {
		sync.RWMutex
		val *readState
	}

	// where/how log data is maintained
	mu struct {
		sync.Mutex
		offset     int64
		logNum     fileNum
		logFile    vfs.File
		logWriter  *writer
		versions   *versionSet
		nodeStates *nodeStates
	}
}

func stateSyncChange(a, b pb.State) bool {
	return a.Term != b.Term || a.Vote != b.Vote
}

// write writes the update instance to the log and returns a boolean flag
// indicating whether a fsync() operation is required. Each pb.Update instance
// contains any number of raft entries, it is also possible to have raft
// snapshot info and raft state in it. Each of such pb.Update written into the
// db is the unit of
func (d *db) write(u pb.Update, buf []byte) (bool, error) {
	sz := u.SizeUpperLimit()
	if sz > len(buf) {
		buf = make([]byte, sz)
	}
	data := pb.MustMarshalTo(&u, buf)
	if _, ok := isCompactionUpdate(u); ok {
		panic("trying to write a compaction update")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.mu.nodeStates.getState(u.ShardID, u.ReplicaID)
	if pb.IsStateEqual(u.State, st) &&
		pb.IsEmptySnapshot(u.Snapshot) && len(u.EntriesToSave) == 0 {
		return false, nil
	}
	sync := !pb.IsEmptySnapshot(u.Snapshot) ||
		len(u.EntriesToSave) > 0 || stateSyncChange(u.State, st)
	return sync, d.doWriteLocked(u, data)
}

func (d *db) doWriteLocked(u pb.Update, data []byte) error {
	if err := d.makeRoomForWrite(); err != nil {
		return err
	}
	offset, err := d.mu.logWriter.writeRecord(data)
	if err != nil {
		return err
	}
	d.updateIndex(u, d.mu.offset, d.mu.logNum)
	d.mu.offset = offset
	d.mu.nodeStates.setState(u.ShardID, u.ReplicaID, u.State)
	return nil
}

// sync issues a fsync() operation on the underlying log file.
func (d *db) sync() error {
	return d.mu.logFile.Sync()
}

// updateIndex records the fileNum and position of the written update into the
// index.
func (d *db) updateIndex(update pb.Update, pos int64, logNum fileNum) {
	index := d.mu.nodeStates.getIndex(update.ShardID, update.ReplicaID)
	compactedTo, compactionUpdate := isCompactionUpdate(update)
	ei := indexEntry{
		pos:     pos,
		fileNum: logNum,
	}
	if compactionUpdate {
		// entry compaction
		index.currEntries.setCompactedTo(compactedTo)
		index.entries.setCompactedTo(compactedTo)
	} else {
		// regular entries
		if len(update.EntriesToSave) > 0 {
			ei.start = update.EntriesToSave[0].Index
			ei.end = update.EntriesToSave[len(update.EntriesToSave)-1].Index
			index.entries.update(ei)
			index.currEntries.update(ei)
		}
		// regular snapshot
		if !pb.IsEmptySnapshot(update.Snapshot) {
			ei.start = update.Snapshot.Index
			ei.end = snapshotFlag
			if index.snapshot.start < ei.start {
				index.snapshot = ei
			}
		}
		// regular state
		if !pb.IsEmptyState(update.State) {
			ei.start = update.State.Commit
			ei.end = stateFlag
			index.state = ei
		}
	}
}

func (d *db) makeRoomForWrite() error {
	if d.mu.offset < d.opts.MaxLogFileSize {
		return nil
	}
	return d.switchToNewLog()
}

// switchToNewLog flushes the index of the current log file to disk, update the
// readState hold by the db and then switch to a new log file.
func (d *db) switchToNewLog() error {
	if err := d.saveIndex(); err != nil {
		return err
	}
	defer d.updateReadStateLocked(nil)
	return d.createNewLog()
}

// getSnapshot returns the latest snapshot in the db. we record all seen
// snapshots into the db, but will only query for the most recent snapshot
// inserted into the db.
func (d *db) getSnapshot(shardID uint64, replicaID uint64) (pb.Snapshot, error) {
	d.mu.Lock()
	readState := d.loadReadState()
	ies, ok := readState.nodeStates.querySnapshot(shardID, replicaID)
	d.mu.Unlock()
	defer readState.unref()
	if !ok {
		return pb.Snapshot{}, nil
	}
	var snapshot pb.Snapshot
	f := func(u pb.Update, _ int64) bool {
		if pb.IsEmptySnapshot(u.Snapshot) {
			panic("empty snapshot")
		}
		snapshot = u.Snapshot
		return false
	}
	if err := d.readLog(ies, f); err != nil {
		return pb.Snapshot{}, err
	}
	return snapshot, nil
}

// getRaftState returns the raft state. Such a raft state record contains a
// pb.State value and raft entry details expressed as FirstIndex of the entry
// and EntryCount. The pb.State value returned in the State field of
// raftio.RaftState is the latest raft state written into the db.
func (d *db) getRaftState(shardID uint64, replicaID uint64,
	lastIndex uint64) (raftio.RaftState, error) {
	d.mu.Lock()
	readState := d.loadReadState()
	ie, ok := readState.nodeStates.queryState(shardID, replicaID)
	ies, _ := readState.nodeStates.query(shardID, replicaID, lastIndex+1, math.MaxUint64)
	d.mu.Unlock()
	defer readState.unref()
	if !ok {
		return raftio.RaftState{}, raftio.ErrNoSavedLog
	}
	var st raftio.RaftState
	if err := d.readLog(ie, func(u pb.Update, _ int64) bool {
		if pb.IsEmptyState(u.State) {
			panic("empty state")
		}
		st.State = u.State
		return false
	}); err != nil {
		return raftio.RaftState{}, err
	}
	prevIndex := uint64(0)
	for _, e := range ies {
		if prevIndex != 0 && prevIndex+1 != e.start {
			panic("gap in indexes")
		}
		prevIndex = e.end
	}
	if len(ies) > 0 {
		st.FirstIndex = lastIndex + 1
		st.EntryCount = ies[len(ies)-1].end - st.FirstIndex + 1
	}
	return st, nil
}

// getEntries queries the db to return raft entries between [low, high), the
// max size of the returned entries is maxSize bytes. The results will be
// appended into the input entries slice which is already size bytes in size.
func (d *db) getEntries(shardID uint64, replicaID uint64,
	entries []pb.Entry, size uint64, low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	d.mu.Lock()
	readState := d.loadReadState()
	ies, ok := readState.nodeStates.query(shardID, replicaID, low, high)
	compactedTo := readState.nodeStates.compactedTo(shardID, replicaID)
	d.mu.Unlock()
	defer readState.unref()
	if !ok {
		return entries, size, nil
	}
	if low <= compactedTo {
		return entries, size, nil
	}
	if maxSize == 0 {
		maxSize = math.MaxUint64
	}
	expected := low
	done := false
	for _, ie := range ies {
		queryIndex := ie
		f := func(u pb.Update, _ int64) bool {
			// TODO: optimize this, not to append one by one
			for _, e := range u.EntriesToSave {
				nsz := uint64(e.SizeUpperLimit())
				if e.Index < expected {
					continue
				}
				if e.Index == expected && e.Index < high &&
					e.Index >= queryIndex.start && e.Index <= queryIndex.end {
					size += nsz
					expected++
					if len(entries) > 0 && entries[len(entries)-1].Index+1 != e.Index {
						panic("gap in entry index")
					}
					entries = append(entries, e)
					if size > maxSize {
						done = true
						return false
					}
				} else {
					return false
				}
			}
			return true
		}
		if err := d.readLog(ie, f); err != nil {
			return nil, 0, err
		}
		if done {
			return entries, size, nil
		}
	}
	return entries, size, nil
}

// readLog queries the db for the saved pb.Update record identified by the
// specified indexEntry parameter. For each encountered pb.Update record,
// h will be invoked with the encountered pb.Update value passed to it.
func (d *db) readLog(ie indexEntry,
	h func(u pb.Update, offset int64) bool) (err error) {
	fn := makeFilename(d.opts.FS, d.dirname, fileTypeLog, ie.fileNum)
	f, err := d.opts.FS.Open(fn)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.Close())
	}()
	rr := newReader(f, ie.fileNum)
	if ie.pos > 0 {
		if err := rr.seekRecord(ie.pos); err != nil {
			return errors.WithStack(err)
		}
	}
	var buf bytes.Buffer
	var r io.Reader
	for {
		offset := rr.offset()
		r, err = rr.next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.WithStack(err)
		}
		if _, err = io.Copy(&buf, r); err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "error when reading WAL")
		}
		var update pb.Update
		pb.MustUnmarshal(&update, buf.Bytes())
		if !h(update, offset) {
			break
		}
		buf.Reset()
	}
	return nil
}
