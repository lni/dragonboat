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
	"encoding/binary"
	"io"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"

	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var errCorruptIndexFile = errors.New("corrupt index file")

const (
	// when within the same []pb.Update, reads can be amplified up to
	// indexBlockSize bytes. in other words, within the same log file, each index
	// covers up to indexBlockSize bytes.
	indexBlockSize int64 = 1024 * 128
)

type indexEntry struct {
	start   uint64
	end     uint64
	fileNum fileNum
	pos     int64
	length  int64
}

func (e *indexEntry) empty() bool {
	return e.fileNum == 0
}

func (e *indexEntry) indexBlock() int64 {
	return e.pos / indexBlockSize
}

func (e *indexEntry) merge(n indexEntry) (indexEntry, indexEntry, bool) {
	if e.end+1 == n.start && e.pos+e.length == n.pos &&
		e.fileNum == n.fileNum && e.indexBlock() == n.indexBlock() {
		result := *e
		result.end = n.end
		result.length = e.length + n.length
		return result, indexEntry{}, true
	}
	return *e, n, false
}

// return a tuple of (indexEntry1, indexEntry2, merged, moreMergeRequired)
func (e *indexEntry) update(n indexEntry) (indexEntry, indexEntry, bool, bool) {
	m, _, merged := e.merge(n)
	if merged {
		return m, indexEntry{}, true, false
	}
	// overwrite
	if n.start == e.start {
		return n, indexEntry{}, true, false
	}
	// overwrite and more merge required
	if n.start < e.start {
		return n, indexEntry{}, true, true
	}
	keep := *e
	// partial overwrite
	if n.start > e.start && n.start <= e.end {
		keep.end = n.start - 1
		return keep, n, false, false
	}
	return keep, n, false, false
}

func (e *indexEntry) isState() bool {
	return e.end == stateFlag
}

func (e *indexEntry) isSnapshot() bool {
	return e.end == snapshotFlag
}

type indexDecoder struct {
	byteReader
}

func (d indexDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptIndexFile
		}
		return 0, err
	}
	return u, nil
}

type indexEncoder struct {
	*bytes.Buffer
}

func (e indexEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

type index struct {
	entries []indexEntry
	// entries between (0, compactedTo) are ready for compaction
	compactedTo uint64
}

func (i *index) setCompactedTo(index uint64) {
	if index > i.compactedTo {
		i.compactedTo = index
	}
}

func (i *index) size() uint64 {
	return uint64(len(i.entries))
}

func (i *index) last() indexEntry {
	return i.entries[i.size()-1]
}

func (i *index) cut() {
	i.entries = i.entries[:i.size()-1]
}

func (i *index) append(e indexEntry) {
	i.entries = append(i.entries, e)
}

func (i *index) update(e indexEntry) {
	if i.size() == 0 {
		i.append(e)
		return
	}
	last := i.last()
	e1, e2, merged, more := last.update(e)
	if more {
		if i.size() == 1 {
			i.entries = []indexEntry{e1}
			return
		}
		i.cut()
		i.update(e1)
		return
	}
	i.entries[i.size()-1] = e1
	if !merged {
		i.append(e2)
	}
}

func binarySearch(entries []indexEntry,
	start int, end int, raftIndex uint64) (int, bool) {
	if start == end {
		if entries[start].start <= raftIndex && entries[start].end >= raftIndex {
			return start, true
		}
		return 0, false
	}
	mid := start + (end-start)/2
	left := entries[start : mid+1]
	if left[0].start <= raftIndex && left[len(left)-1].end >= raftIndex {
		return binarySearch(entries, start, mid, raftIndex)
	}
	return binarySearch(entries, mid+1, end, raftIndex)
}

func (i *index) query(low uint64, high uint64) ([]indexEntry, bool) {
	if high < low {
		panic("high < low")
	}
	if len(i.entries) == 0 {
		return []indexEntry{}, false
	}
	startIdx, ok := binarySearch(i.entries, 0, len(i.entries)-1, low)
	if !ok {
		return []indexEntry{}, false
	}
	var result []indexEntry
	for idx := startIdx; idx < len(i.entries); idx++ {
		if high <= i.entries[idx].start {
			break
		}
		if len(result) > 0 {
			if result[len(result)-1].end+1 != i.entries[idx].start {
				break
			}
		}
		result = append(result, i.entries[idx])
	}
	return result, true
}

func (i *index) encode(w io.Writer) error {
	e := indexEncoder{new(bytes.Buffer)}
	e.writeUvarint(uint64(len(i.entries)))
	for _, entry := range i.entries {
		e.writeUvarint(entry.start)
		e.writeUvarint(entry.end)
		e.writeUvarint(uint64(entry.fileNum))
		e.writeUvarint(uint64(entry.pos))
		e.writeUvarint(uint64(entry.length))
	}
	e.writeUvarint(i.compactedTo)
	_, err := w.Write(e.Bytes())
	return err
}

func (i *index) decode(d *indexDecoder) error {
	sz, err := d.readUvarint()
	if err != nil {
		return err
	}
	for idx := uint64(0); idx < sz; idx++ {
		start, err := d.readUvarint()
		if err != nil {
			return err
		}
		end, err := d.readUvarint()
		if err != nil {
			return err
		}
		logNum, err := d.readUvarint()
		if err != nil {
			return err
		}
		pos, err := d.readUvarint()
		if err != nil {
			return err
		}
		length, err := d.readUvarint()
		if err != nil {
			return err
		}
		e := indexEntry{
			start:   start,
			end:     end,
			fileNum: fileNum(logNum),
			pos:     int64(pos),
			length:  int64(length),
		}
		i.entries = append(i.entries, e)
	}
	compactedTo, err := d.readUvarint()
	if err != nil {
		return err
	}
	i.compactedTo = compactedTo
	return nil
}

func (i *index) compaction() fileNum {
	return i.entryCompaction()
}

func (i *index) entryCompaction() fileNum {
	maxObsoleteFileNum := fileNum(math.MaxUint64)
	if i.size() == 0 {
		return maxObsoleteFileNum
	}
	fn := i.entries[0].fileNum
	for j := uint64(1); j < i.size(); j++ {
		ie := i.entries[j]
		if ie.fileNum != fn {
			prev := i.entries[j-1]
			if prev.isSnapshot() || prev.isState() {
				panic("not an entry index")
			}
			if prev.end <= i.compactedTo {
				maxObsoleteFileNum = prev.fileNum
			}
			fn = ie.fileNum
		}
	}
	if maxObsoleteFileNum == fileNum(math.MaxUint64) && i.size() > 0 {
		maxObsoleteFileNum = i.entries[0].fileNum - 1
	}
	return maxObsoleteFileNum
}

func (i *index) removeObsolete(maxObsoleteFileNum fileNum) []fileNum {
	var obsolete []fileNum
	index := 0
	fn := fileNum(0)
	for j := range i.entries {
		if i.entries[j].fileNum <= maxObsoleteFileNum {
			if fn != i.entries[j].fileNum {
				obsolete = append(obsolete, i.entries[j].fileNum)
				fn = i.entries[j].fileNum
			}
			index = j
		}
	}
	if len(obsolete) > 0 {
		i.entries = append(make([]indexEntry, 0), i.entries[index+1:]...)
		return obsolete
	}
	return nil
}

type nodeIndex struct {
	clusterID   uint64
	nodeID      uint64
	entries     index
	currEntries index
	snapshot    indexEntry
	state       indexEntry
}

func (n *nodeIndex) removeAll() {
	n.entries = index{}
	n.currEntries = index{}
	n.snapshot = indexEntry{}
	n.state = indexEntry{}
}

func (n *nodeIndex) update(entry indexEntry, ss indexEntry, state indexEntry) {
	n.entries.update(entry)
	n.currEntries.update(entry)
	if ss.start > n.snapshot.start {
		n.snapshot = ss
	}
	if !state.empty() {
		n.state = state
	}
}

func (n *nodeIndex) fileInUse(fn fileNum) bool {
	if n.snapshot.fileNum == fn || n.state.fileNum == fn {
		return true
	}
	for _, ie := range n.entries.entries {
		if ie.fileNum > fn {
			break
		}
		if ie.fileNum == fn {
			return true
		}
	}
	return false
}

func (n *nodeIndex) query(low uint64, high uint64) ([]indexEntry, bool) {
	return n.entries.query(low, high)
}

func (n *nodeIndex) getState() (indexEntry, bool) {
	return n.state, !n.state.empty()
}

func (n *nodeIndex) querySnapshot() (indexEntry, bool) {
	return n.snapshot, !n.snapshot.empty()
}

func (n *nodeIndex) stateCompaction() fileNum {
	if n.state.empty() {
		return fileNum(math.MaxUint64)
	}
	return n.state.fileNum - 1
}

func (n *nodeIndex) snapshotCompaction() fileNum {
	if n.snapshot.empty() {
		return fileNum(math.MaxUint64)
	}
	return n.snapshot.fileNum - 1
}

func (n *nodeIndex) compaction() []fileNum {
	efn := n.entries.compaction()
	sfn := n.snapshotCompaction()
	stateFn := n.stateCompaction()
	maxObsoleteFileNum := fileNum(0)
	if efn < sfn {
		maxObsoleteFileNum = efn
	} else {
		maxObsoleteFileNum = sfn
	}
	if stateFn < maxObsoleteFileNum {
		maxObsoleteFileNum = stateFn
	}
	if maxObsoleteFileNum == fileNum(math.MaxUint64) ||
		maxObsoleteFileNum == fileNum(0) {
		return nil
	}
	obsoleteEntries := n.entries.removeObsolete(maxObsoleteFileNum)
	return obsoleteEntries
}

type state struct {
	indexes map[raftio.NodeInfo]*nodeIndex
	states  map[raftio.NodeInfo]pb.State
}

func newState() *state {
	return &state{
		indexes: make(map[raftio.NodeInfo]*nodeIndex),
		states:  make(map[raftio.NodeInfo]pb.State),
	}
}

func (s *state) checkNodeInfo(clusterID uint64, nodeID uint64) {
	if clusterID == 0 && nodeID == 0 {
		panic("clusterID/nodeID are both empty")
	}
}

func (s *state) getState(clusterID uint64, nodeID uint64) pb.State {
	s.checkNodeInfo(clusterID, nodeID)
	ni := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	st, ok := s.states[ni]
	if !ok {
		st = pb.State{}
		s.states[ni] = st
	}
	return st
}

func (s *state) setState(clusterID uint64, nodeID uint64, st pb.State) {
	s.checkNodeInfo(clusterID, nodeID)
	ni := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	s.states[ni] = st
}

func (s *state) getIndex(clusterID uint64, nodeID uint64) *nodeIndex {
	s.checkNodeInfo(clusterID, nodeID)
	ni := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	idx, ok := s.indexes[ni]
	if !ok {
		idx = &nodeIndex{clusterID: clusterID, nodeID: nodeID}
		s.indexes[ni] = idx
	}
	return idx
}

func (s *state) save(dirname string,
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
		if ni.ClusterID != n.clusterID || ni.NodeID != n.nodeID {
			panic("inconsistent clusterID/nodeID")
		}
		e := indexEncoder{new(bytes.Buffer)}
		e.writeUvarint(n.clusterID)
		e.writeUvarint(n.nodeID)
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

func (s *state) load(dirname string, fn fileNum, fs vfs.FS) (err error) {
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
		clusterID, err := d.readUvarint()
		if err != nil {
			return err
		}
		nodeID, err := d.readUvarint()
		if err != nil {
			return err
		}
		n := s.getIndex(clusterID, nodeID)
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

func (s *state) querySnapshot(clusterID uint64, nodeID uint64) (indexEntry, bool) {
	n := s.getIndex(clusterID, nodeID)
	return n.querySnapshot()
}

func (s *state) queryState(clusterID uint64, nodeID uint64) (indexEntry, bool) {
	n := s.getIndex(clusterID, nodeID)
	return n.getState()
}

func (s *state) query(clusterID uint64, nodeID uint64,
	low uint64, high uint64) ([]indexEntry, bool) {
	n := s.getIndex(clusterID, nodeID)
	return n.query(low, high)
}

func (s *state) compactedTo(clusterID uint64, nodeID uint64) uint64 {
	n := s.getIndex(clusterID, nodeID)
	return n.entries.compactedTo
}

func (s *state) getObsolete(fns []fileNum) []fileNum {
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
