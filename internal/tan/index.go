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
	"encoding/binary"
	"io"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
)

var errCorruptIndexFile = errors.New("corrupt index file")

const (
	// each indexEntry covers up to indexBlockSize bytes within the same log file.
	indexBlockSize int64 = 1024 * 128
)

// indexEntry represents an entry in the index. Each indexEntry points to a
// kind of record in the db, it might be a slice of continuous raft entries,
// a raft state record or a raft snapshot record.
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

// merge merges two indexEntry records. This is for raft entry indexes as we
// focus on scan performance, we don't need to index individual raft entries in
// the db, they are stored continuously by their raft entry index value anyway.
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
	// for raft entries, new writes always overwrite old entries
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

// whether the indexEntry points to a state record
func (e *indexEntry) isState() bool {
	return e.end == stateFlag
}

// whether the indexEntry points to a snapshot record
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

// index is the index to raft entry records in the db
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

func (i *index) query(low uint64, high uint64) ([]indexEntry, bool) {
	if high < low {
		panic("high < low")
	}
	if len(i.entries) == 0 {
		return []indexEntry{}, false
	}

	startIdx := sort.Search(len(i.entries), func(pos int) bool {
		return low <= i.entries[pos].end
	})
	if !(startIdx < len(i.entries)) {
		return []indexEntry{}, false
	}
	if !(low >= i.entries[startIdx].start) {
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

// entryCompaction calculates and returns the max log file fileNum that is
// considered as obsolete and can be safely deleted.
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

// removeObsolete removes obsolete indexEntry records from the index
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

// nodeIndex is the index for all records that belong to a single raft node
type nodeIndex struct {
	shardID   uint64
	replicaID uint64
	// entries contains all indexEntry records
	entries index
	// currEntries contains only indexEntry records that belong to the current log file
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
