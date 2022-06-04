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
	"bufio"
	"bytes"

	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
)

type nodeStates struct {
	indexes map[raftio.NodeInfo]*nodeIndex
	states  map[raftio.NodeInfo]pb.State
}

func newNodeStates() *nodeStates {
	return &nodeStates{
		indexes: make(map[raftio.NodeInfo]*nodeIndex),
		states:  make(map[raftio.NodeInfo]pb.State),
	}
}

func (s *nodeStates) checkNodeInfo(shardID uint64, replicaID uint64) {
	if shardID == 0 && replicaID == 0 {
		panic("shardID/replicaID are both empty")
	}
}

func (s *nodeStates) getState(shardID uint64, replicaID uint64) pb.State {
	s.checkNodeInfo(shardID, replicaID)
	ni := raftio.NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	st, ok := s.states[ni]
	if !ok {
		st = pb.State{}
		s.states[ni] = st
	}
	return st
}

func (s *nodeStates) setState(shardID uint64, replicaID uint64, st pb.State) {
	s.checkNodeInfo(shardID, replicaID)
	ni := raftio.NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	s.states[ni] = st
}

func (s *nodeStates) getIndex(shardID uint64, replicaID uint64) *nodeIndex {
	s.checkNodeInfo(shardID, replicaID)
	ni := raftio.NodeInfo{ShardID: shardID, ReplicaID: replicaID}
	idx, ok := s.indexes[ni]
	if !ok {
		idx = &nodeIndex{shardID: shardID, replicaID: replicaID}
		s.indexes[ni] = idx
	}
	return idx
}

func (s *nodeStates) save(dirname string,
	dir vfs.File, fileNum fileNum, fs vfs.FS) (err error) {
	tmpFn := makeFilename(fs, dirname, fileTypeIndexTemp, fileNum)
	fn := makeFilename(fs, dirname, fileTypeIndex, fileNum)
	file, err := fs.Create(tmpFn)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, file.Sync())
		err = firstError(err, file.Close())
		if err == nil {
			err = fs.Rename(tmpFn, fn)
		}
		err = firstError(err, dir.Sync())
	}()
	w := newWriter(file)
	defer func() {
		err = firstError(err, w.close())
	}()
	rw, err := w.next()
	if err != nil {
		return err
	}
	e := indexEncoder{new(bytes.Buffer)}
	e.writeUvarint(uint64(len(s.indexes)))
	if _, err := rw.Write(e.Bytes()); err != nil {
		return err
	}
	for ni, n := range s.indexes {
		rw, err = w.next()
		if err != nil {
			return err
		}
		if ni.ShardID != n.shardID || ni.ReplicaID != n.replicaID {
			panic("inconsistent shardID/replicaID")
		}
		e := indexEncoder{new(bytes.Buffer)}
		e.writeUvarint(n.shardID)
		e.writeUvarint(n.replicaID)
		if _, err := rw.Write(e.Bytes()); err != nil {
			return err
		}
		rw, err = w.next()
		if err != nil {
			return err
		}
		if err := n.currEntries.encode(rw); err != nil {
			return err
		}
		n.currEntries = index{}
		rw, err = w.next()
		if err != nil {
			return err
		}
		snapshot := index{[]indexEntry{n.snapshot}, 0}
		if err := snapshot.encode(rw); err != nil {
			return err
		}
		rw, err = w.next()
		if err != nil {
			return err
		}
		state := index{[]indexEntry{n.state}, 0}
		if err := state.encode(rw); err != nil {
			return err
		}
	}
	return nil
}

func (s *nodeStates) load(dirname string, fn fileNum, fs vfs.FS) (err error) {
	file, err := fs.Open(makeFilename(fs, dirname, fileTypeIndex, fn))
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, file.Close())
	}()
	r := newReader(file, fileNum(0))
	rr, err := r.next()
	if err != nil {
		return err
	}
	d := &indexDecoder{bufio.NewReader(rr)}
	sz, err := d.readUvarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < sz; i++ {
		var entries index
		var snapshots index
		var state index
		rr, err = r.next()
		if err != nil {
			return err
		}
		d = &indexDecoder{bufio.NewReader(rr)}
		shardID, err := d.readUvarint()
		if err != nil {
			return err
		}
		replicaID, err := d.readUvarint()
		if err != nil {
			return err
		}
		n := s.getIndex(shardID, replicaID)
		if n.currEntries.size() > 0 {
			panic("current entries not empty")
		}
		rr, err = r.next()
		if err != nil {
			return err
		}
		d = &indexDecoder{bufio.NewReader(rr)}
		if err := entries.decode(d); err != nil {
			return err
		}
		for idx, e := range entries.entries {
			if e.isSnapshot() || e.isState() {
				plog.Panicf("unexpected type %v", e)
			}
			if idx == 0 {
				// just crossed index file boundary, we must merge here, consider
				// consider existing index entries [{1 26 2 0} {27 27 3 47}], when
				// {27 40 5 23} is loaded as the first index entry from a new index file,
				// it needs to overwrite the {27 27 3 47} entry.
				n.entries.update(e)
			} else {
				// must append only here, we can't merge indexEntries here, consider
				// [{101 101 2 0} {102 104 2 58}] in an index file
				// once merged via update(), the output is {101 104 2 0}, this would be
				// incorrect if there are entries with in index=2 between log file offset
				// 0 and 58
				n.entries.append(e)
			}
		}
		n.entries.setCompactedTo(entries.compactedTo)
		rr, err = r.next()
		if err != nil {
			return err
		}
		d = &indexDecoder{bufio.NewReader(rr)}
		if err := snapshots.decode(d); err != nil {
			return err
		}
		if len(snapshots.entries) > 1 {
			panic("unexpected snapshot entry count")
		}
		if len(snapshots.entries) > 0 {
			n.snapshot = snapshots.entries[0]
		}
		rr, err = r.next()
		if err != nil {
			return err
		}
		d = &indexDecoder{bufio.NewReader(rr)}
		if err := state.decode(d); err != nil {
			return err
		}
		if len(state.entries) > 1 {
			panic("unexpected state entry count")
		}
		if len(state.entries) > 0 && !state.entries[0].empty() {
			n.state = state.entries[0]
		}
	}
	return nil
}

func (s *nodeStates) querySnapshot(shardID uint64, replicaID uint64) (indexEntry, bool) {
	n := s.getIndex(shardID, replicaID)
	return n.querySnapshot()
}

func (s *nodeStates) queryState(shardID uint64, replicaID uint64) (indexEntry, bool) {
	n := s.getIndex(shardID, replicaID)
	return n.getState()
}

func (s *nodeStates) query(shardID uint64, replicaID uint64,
	low uint64, high uint64) ([]indexEntry, bool) {
	n := s.getIndex(shardID, replicaID)
	return n.query(low, high)
}

func (s *nodeStates) compactedTo(shardID uint64, replicaID uint64) uint64 {
	n := s.getIndex(shardID, replicaID)
	return n.entries.compactedTo
}

func (s *nodeStates) getObsolete(fns []fileNum) []fileNum {
	var result []fileNum
	for _, fn := range fns {
		inUse := false
		for _, index := range s.indexes {
			if index.fileInUse(fn) {
				inUse = true
				break
			}
		}
		if !inUse {
			result = append(result, fn)
		}
	}
	return result
}
