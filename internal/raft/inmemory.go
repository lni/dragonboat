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
	"github.com/lni/dragonboat/v3/internal/settings"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	entrySliceSize    = settings.Soft.InMemEntrySliceSize
	minEntrySliceSize = settings.Soft.MinEntrySliceFreeSize
)

// inMemory is a two stage in memory log storage struct to keep log entries
// that will be used by the raft protocol in immediate future.
type inMemory struct {
	shrunk      bool
	snapshot    *pb.Snapshot
	entries     []pb.Entry
	markerIndex uint64
	savedTo     uint64
	rl          *server.RateLimiter
}

func newInMemory(lastIndex uint64, rl *server.RateLimiter) inMemory {
	if minEntrySliceSize >= entrySliceSize {
		panic("minEntrySliceSize >= entrySliceSize")
	}
	return inMemory{
		markerIndex: lastIndex + 1,
		savedTo:     lastIndex,
		rl:          rl,
	}
}

func (im *inMemory) checkMarkerIndex() {
	if len(im.entries) > 0 {
		if im.entries[0].Index != im.markerIndex {
			plog.Panicf("marker index %d, first index %d",
				im.markerIndex, im.entries[0].Index)
		}
	}
}

func (im *inMemory) getEntries(low uint64, high uint64) []pb.Entry {
	upperBound := im.markerIndex + uint64(len(im.entries))
	if low > high || low < im.markerIndex {
		plog.Panicf("invalid low value %d, high %d, marker index %d",
			low, high, im.markerIndex)
	}
	if high > upperBound {
		plog.Panicf("invalid high value %d, upperBound %d", high, upperBound)
	}
	return im.entries[low-im.markerIndex : high-im.markerIndex]
}

func (im *inMemory) getSnapshotIndex() (uint64, bool) {
	if im.snapshot != nil {
		return im.snapshot.Index, true
	}
	return 0, false
}

func (im *inMemory) getLastIndex() (uint64, bool) {
	if len(im.entries) > 0 {
		return im.entries[len(im.entries)-1].Index, true
	}
	return im.getSnapshotIndex()
}

func (im *inMemory) getTerm(index uint64) (uint64, bool) {
	if index < im.markerIndex {
		if idx, ok := im.getSnapshotIndex(); ok && idx == index {
			return im.snapshot.Term, true
		}
		return 0, false
	}
	lastIndex, ok := im.getLastIndex()
	if ok && index <= lastIndex {
		return im.entries[index-im.markerIndex].Term, true
	}
	return 0, false
}

func (im *inMemory) commitUpdate(cu pb.UpdateCommit) {
	if cu.StableLogTo > 0 {
		im.savedLogTo(cu.StableLogTo, cu.StableLogTerm)
	}
	if cu.StableSnapshotTo > 0 {
		im.savedSnapshotTo(cu.StableSnapshotTo)
	}
}

func (im *inMemory) entriesToSave() []pb.Entry {
	idx := im.savedTo + 1
	if idx-im.markerIndex > uint64(len(im.entries)) {
		return []pb.Entry{}
	}
	return im.entries[idx-im.markerIndex:]
}

func (im *inMemory) savedLogTo(index uint64, term uint64) {
	if index < im.markerIndex {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	if index > im.entries[len(im.entries)-1].Index ||
		term != im.entries[index-im.markerIndex].Term {
		return
	}
	im.savedTo = index
}

func (im *inMemory) appliedLogTo(index uint64) {
	if index < im.markerIndex {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	if index > im.entries[len(im.entries)-1].Index {
		return
	}
	newMarkerIndex := index
	applied := im.entries[:newMarkerIndex-im.markerIndex]
	im.shrunk = true
	im.entries = im.entries[newMarkerIndex-im.markerIndex:]
	im.markerIndex = newMarkerIndex
	im.resizeEntrySlice()
	im.checkMarkerIndex()
	if im.rateLimited() {
		im.rl.Decrease(getEntrySliceSize(applied))
	}
}

func (im *inMemory) savedSnapshotTo(index uint64) {
	if idx, ok := im.getSnapshotIndex(); ok && idx == index {
		im.snapshot = nil
	} else if ok && idx != index {
		plog.Warningf("snapshot index does not match")
	}
}

func (im *inMemory) resize() {
	old := im.entries
	im.shrunk = false
	sz := max(entrySliceSize, uint64(len(old)*2))
	im.entries = make([]pb.Entry, 0, sz)
	im.entries = append(im.entries, old...)
}

func (im *inMemory) tryResize() {
	if im.shrunk {
		im.resize()
	}
}

func (im *inMemory) resizeEntrySlice() {
	if im.shrunk && cap(im.entries)-len(im.entries) < int(minEntrySliceSize) {
		im.resize()
	}
}

func (im *inMemory) merge(ents []pb.Entry) {
	firstNewIndex := ents[0].Index
	im.resizeEntrySlice()
	if firstNewIndex == im.markerIndex+uint64(len(im.entries)) {
		checkEntriesToAppend(im.entries, ents)
		im.entries = append(im.entries, ents...)
		if im.rateLimited() {
			im.rl.Increase(getEntrySliceSize(ents))
		}
	} else if firstNewIndex <= im.markerIndex {
		im.markerIndex = firstNewIndex
		// ents might come from entryQueue, copy it to its own storage
		im.shrunk = false
		im.entries = newEntrySlice(ents)
		im.savedTo = firstNewIndex - 1
		if im.rateLimited() {
			im.rl.Set(getEntrySliceSize(ents))
		}
	} else {
		existing := im.getEntries(im.markerIndex, firstNewIndex)
		checkEntriesToAppend(existing, ents)
		im.shrunk = false
		im.entries = make([]pb.Entry, 0, len(existing)+len(ents))
		im.entries = append(im.entries, existing...)
		im.entries = append(im.entries, ents...)
		im.savedTo = min(im.savedTo, firstNewIndex-1)
		if im.rateLimited() {
			sz := getEntrySliceSize(ents) + getEntrySliceSize(existing)
			im.rl.Set(sz)
		}
	}
	im.checkMarkerIndex()
}

func (im *inMemory) restore(ss pb.Snapshot) {
	im.snapshot = &ss
	im.markerIndex = ss.Index + 1
	im.shrunk = false
	im.entries = nil
	im.savedTo = ss.Index
	if im.rateLimited() {
		im.rl.Set(0)
	}
}

func (im *inMemory) rateLimited() bool {
	return im.rl != nil && im.rl.Enabled()
}
