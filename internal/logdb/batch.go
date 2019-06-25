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

package logdb

import (
	"errors"
	"math"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

//
// Rather than storing each raft entry as an individual record, dragonboat
// batches entries first, each persisted record can thus contain many raft
// entries. This idea is based on the observations that:
//  * entries are usually saved together
//  * entries are usually read and consumed together
// This idea helped to achieve better latency and throughput performance.
//
// We also compact redundant index/term values whenever possible. In such
// approach, rather than repeatedly storing the continuously increamental
// index values and the identifical term values, we represent them in the
// way implemented in the compactBatchFields function below.
//
// To the maximum of our knowledge, dragonboat is the original inventor
// of the ideas above, they were publicly disclosed on github.com when
// dragnoboat made its first public release.
//
// To use/implement the above ideas in your software, please include the
// above copyright notice in your source file, dragonboat's Apache2 license
// also requires to include dragonboat's NOTICE file.
//

func getBatchID(index uint64) uint64 {
	return index / batchSize
}

func entriesSize(ents []pb.Entry) uint64 {
	sz := uint64(0)
	for _, e := range ents {
		sz += uint64(e.SizeUpperLimit())
	}
	return sz
}

func getBatchIDRange(low uint64, high uint64) (uint64, uint64) {
	lowBatchID := getBatchID(low)
	highBatchID := getBatchID(high)
	if high%batchSize == 0 {
		return lowBatchID, highBatchID
	}
	return lowBatchID, highBatchID + 1
}

func restoreBatchFields(eb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) <= 1 {
		panic("restore called on small batch")
	}
	if eb.Entries[len(eb.Entries)-1].Term == 0 {
		term := eb.Entries[0].Term
		idx := eb.Entries[0].Index
		for i := 1; i < len(eb.Entries); i++ {
			eb.Entries[i].Term = term
			eb.Entries[i].Index = idx + uint64(i)
		}
	}
	return eb
}

func compactBatchFields(eb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) <= 1 {
		panic("compact called on small batch")
	}
	expLastIdx := eb.Entries[0].Index + uint64(len(eb.Entries)-1)
	if eb.Entries[0].Term == eb.Entries[len(eb.Entries)-1].Term &&
		expLastIdx == eb.Entries[len(eb.Entries)-1].Index {
		for i := 1; i < len(eb.Entries); i++ {
			eb.Entries[i].Term = 0
			eb.Entries[i].Index = 0
		}
	}
	return eb
}

func getMergedFirstBatch(eb pb.EntryBatch, lb pb.EntryBatch) pb.EntryBatch {
	if len(eb.Entries) == 0 || len(lb.Entries) == 0 {
		panic("getMergedFirstBatch called on empty batch")
	}
	batchID := getBatchID(eb.Entries[0].Index)
	if batchID < getBatchID(lb.Entries[0].Index) {
		panic("eb batch < lb batch")
	}
	if batchID > getBatchID(lb.Entries[0].Index) {
		return eb
	}
	if eb.Entries[0].Index > lb.Entries[0].Index {
		if eb.Entries[0].Index <= lb.Entries[len(lb.Entries)-1].Index {
			firstIndex := eb.Entries[0].Index
			i := 0
			for ; i < len(lb.Entries); i++ {
				if lb.Entries[i].Index >= firstIndex {
					break
				}
			}
			lb.Entries = append(lb.Entries[:i], eb.Entries...)
		} else {
			lb.Entries = append(lb.Entries, eb.Entries...)
		}
		return lb
	}
	return eb
}

type batchedEntries struct {
	cs   *rdbcache
	keys *logdbKeyPool
	kvs  kv.IKVStore
}

func newBatchedEntries(cs *rdbcache,
	keys *logdbKeyPool, kvs kv.IKVStore) entryManager {
	return &batchedEntries{
		cs:   cs,
		keys: keys,
		kvs:  kvs,
	}
}

func (be *batchedEntries) iterate(ents []pb.Entry, maxIndex uint64,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	if high > maxIndex+1 {
		high = maxIndex + 1
	}
	lowID, highID := getBatchIDRange(low, high)
	ebs, err := be.iterateBatches(clusterID, nodeID, lowID, highID)
	if err != nil {
		return nil, 0, err
	}
	if len(ebs) == 0 {
		return ents, size, nil
	}
	exp := low
	for _, eb := range ebs {
		if len(eb.Entries) > 1 {
			eb = restoreBatchFields(eb)
		}
		for _, e := range eb.Entries {
			if e.Index >= low && e.Index < high {
				if e.Index != exp {
					return ents, size, nil
				}
				exp = e.Index + 1
				size += uint64(e.SizeUpperLimit())
				ents = append(ents, e)
				if size > maxSize {
					return ents, size, nil
				}
			}
		}
	}
	return ents, entriesSize(ents), nil
}

func (be *batchedEntries) iterateBatches(clusterID uint64,
	nodeID uint64, low uint64, high uint64) ([]pb.EntryBatch, error) {
	ents := make([]pb.EntryBatch, 0)
	if low+1 == high {
		e, ok := be.getBatchFromDB(clusterID, nodeID, low)
		if !ok {
			return []pb.EntryBatch{}, nil
		}
		ents = append(ents, e)
		return ents, nil
	}
	fk := be.keys.get()
	lk := be.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.SetEntryBatchKey(clusterID, nodeID, low)
	lk.SetEntryBatchKey(clusterID, nodeID, high)
	expectedID := low
	op := func(key []byte, data []byte) (bool, error) {
		var eb pb.EntryBatch
		if err := eb.Unmarshal(data); err != nil {
			panic(err)
		}
		if getBatchID(eb.Entries[0].Index) != expectedID {
			return false, nil
		}
		ents = append(ents, eb)
		expectedID++
		return true, nil
	}
	if err := be.kvs.IterateValue(fk.Key(), lk.Key(), false, op); err != nil {
		return nil, err
	}
	return ents, nil
}

func (be *batchedEntries) getRange(clusterID uint64,
	nodeID uint64, snapshotIndex uint64, maxIndex uint64) (uint64, uint64, error) {
	fk := be.keys.get()
	lk := be.keys.get()
	defer fk.Release()
	defer lk.Release()
	low, high := getBatchIDRange(snapshotIndex, maxIndex+1)
	fk.SetEntryBatchKey(clusterID, nodeID, low)
	lk.SetEntryBatchKey(clusterID, nodeID, high)
	firstIndex := uint64(0)
	length := uint64(0)
	op := func(key []byte, data []byte) (bool, error) {
		var eb pb.EntryBatch
		if err := eb.Unmarshal(data); err != nil {
			panic(err)
		}
		if len(eb.Entries) == 0 {
			panic("empty batch found")
		}
		if len(eb.Entries) > 1 {
			eb = restoreBatchFields(eb)
		}
		for _, e := range eb.Entries {
			if e.Index >= snapshotIndex && e.Index <= maxIndex {
				if firstIndex == uint64(0) {
					firstIndex = e.Index
					return false, nil
				}
			}
		}
		return true, nil
	}
	if err := be.kvs.IterateValue(fk.Key(), lk.Key(), false, op); err != nil {
		return 0, 0, err
	}
	if firstIndex == 0 && maxIndex != 0 {
		panic("firstIndex == 0 && maxIndex != 0")
	}
	if firstIndex > 0 {
		length = maxIndex - firstIndex + 1
	}
	return firstIndex, length, nil
}

func (be *batchedEntries) rangedOp(clusterID uint64,
	nodeID uint64, index uint64,
	op func(fk *PooledKey, lk *PooledKey) error) error {
	fk := be.keys.get()
	lk := be.keys.get()
	defer fk.Release()
	defer lk.Release()
	batchID := getBatchID(index)
	if batchID == 0 || batchID == 1 {
		return nil
	}
	fk.SetEntryBatchKey(clusterID, nodeID, 0)
	lk.SetEntryBatchKey(clusterID, nodeID, batchID-1)
	return op(fk, lk)
}

func (be *batchedEntries) recordBatch(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64, eb pb.EntryBatch,
	firstBatchID uint64, lastBatchID uint64, ctx raftio.IContext) {
	if len(eb.Entries) == 0 {
		return
	}
	batchID := getBatchID(eb.Entries[0].Index)
	var meb pb.EntryBatch
	if firstBatchID == batchID {
		lb := ctx.GetLastEntryBatch()
		meb = be.getMergedFirstBatch(clusterID, nodeID, eb, lb)
	} else {
		meb = eb
	}
	if lastBatchID == batchID {
		be.cs.setLastEntryBatch(clusterID, nodeID, meb)
	}
	if len(meb.Entries) > 1 {
		meb = compactBatchFields(meb)
	}
	szul := meb.SizeUpperLimit()
	data := ctx.GetValueBuffer(uint64(szul))
	sz, err := meb.MarshalTo(data)
	if err != nil {
		panic(err)
	}
	data = data[:sz]
	k := ctx.GetKey()
	k.SetEntryBatchKey(clusterID, nodeID, batchID)
	wb.Put(k.Key(), data)
}

func (be *batchedEntries) record(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64,
	ctx raftio.IContext, entries []pb.Entry) uint64 {
	if len(entries) == 0 {
		panic("empty entries")
	}
	eb := ctx.GetEntryBatch()
	eb.Entries = eb.Entries[:0]
	currentBatchIdx := uint64(math.MaxUint64)
	idx := 0
	maxIndex := uint64(0)
	firstBatchID := getBatchID(entries[0].Index)
	lastBatchID := getBatchID(entries[len(entries)-1].Index)
	for idx < len(entries) {
		ent := entries[idx]
		if ent.Index > maxIndex {
			maxIndex = ent.Index
		}
		batchID := getBatchID(ent.Index)
		if batchID != currentBatchIdx {
			be.recordBatch(wb, clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
			eb.Entries = eb.Entries[:0]
			currentBatchIdx = batchID
		}
		eb.Entries = append(eb.Entries, ent)
		idx++
	}
	if len(eb.Entries) > 0 {
		be.recordBatch(wb, clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
	}
	return maxIndex
}

func (be *batchedEntries) getBatchFromDB(clusterID uint64,
	nodeID uint64, batchID uint64) (pb.EntryBatch, bool) {
	var e pb.EntryBatch
	k := be.keys.get()
	defer k.Release()
	k.SetEntryBatchKey(clusterID, nodeID, batchID)
	if err := be.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return errors.New("no such entry")
		}
		if err := e.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return e, false
	}
	if len(e.Entries) > 1 {
		return restoreBatchFields(e), true
	}
	return e, true
}

func (be *batchedEntries) getLastBatch(clusterID uint64,
	nodeID uint64, firstIndex uint64, lb pb.EntryBatch) (pb.EntryBatch, bool) {
	batchID := getBatchID(firstIndex)
	lb, ok := be.cs.getLastEntryBatch(clusterID, nodeID, lb)
	if !ok || batchID < getBatchID(lb.Entries[0].Index) {
		lb, ok = be.getBatchFromDB(clusterID, nodeID, batchID)
		if !ok {
			return pb.EntryBatch{}, false
		}
	}
	return lb, true
}

func (be *batchedEntries) getMergedFirstBatch(clusterID uint64,
	nodeID uint64, eb pb.EntryBatch, lb pb.EntryBatch) pb.EntryBatch {
	// batch aligned
	if eb.Entries[0].Index%batchSize == 0 {
		return eb
	}
	lb, ok := be.getLastBatch(clusterID, nodeID, eb.Entries[0].Index, lb)
	if !ok {
		return eb
	}
	return getMergedFirstBatch(eb, lb)
}

func (be *batchedEntries) binaryFormat() uint32 {
	return raftio.LogDBBinVersion
}
