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

package logdb

import (
	"math"

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

//
// Rather than storing each raft entry as an individual record, dragonboat can
// batches entries first and store multiple consecutive entries as a batch
// record. This idea is based on the observations that:
//  * consecutive entries are usually saved together
//  * consecutive entries are usually read and consumed together
//  * it is quite CPU expensive to individually insert entries to the
//    underlying memtables used by the Key-Value stores.
//
// Maintaining and reusing some kind of insert hint to avoid repeatedly locating
// the memtable positions for new entries is not enough as profiling shows that
// only saves a relatively small percentage of CPU cycles.
//
// We also compact redundant index/term values whenever possible. In such
// approach, rather than repeatedly storing the consecutive increamental
// index values and the identifical term values, we represent them in the
// way implemented in the compactBatchFields function below.
//
// These optimizations helped to achieve better latency and throughput
// performance.
//
// The obvious disadvantages of doing these are -
//  * slightly increased complexity
//  * last few entries may need to be stored in RAM waiting to be used to
//    form the next batch, this increases the memory footprint and
//   	potentially be a problem when there are large number of raft groups
//    or some raft groups have huge batches
//
// To the maximum of our knowledge, dragonboat is the original inventor
// of the optimizations above, they were publicly disclosed on github.com
// when dragnoboat made its first public release.
//
// To use/implement the above optimizations in your software, please include
// the above copyright notice in your source file, dragonboat's Apache2 license
// also explicitly requires to include dragonboat's NOTICE file.
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
	cs   *cache
	keys *keyPool
	kvs  kv.IKVStore
}

var _ entryManager = (*batchedEntries)(nil)

func newBatchedEntries(cs *cache,
	keys *keyPool, kvs kv.IKVStore) entryManager {
	return &batchedEntries{
		cs:   cs,
		keys: keys,
		kvs:  kvs,
	}
}

func (be *batchedEntries) iterate(ents []pb.Entry, maxIndex uint64,
	size uint64, shardID uint64, replicaID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	if high > maxIndex+1 {
		high = maxIndex + 1
	}
	lowID, highID := getBatchIDRange(low, high)
	ebs, err := be.iterateBatches(shardID, replicaID, lowID, highID)
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

func (be *batchedEntries) iterateBatches(shardID uint64,
	replicaID uint64, low uint64, high uint64) ([]pb.EntryBatch, error) {
	ents := make([]pb.EntryBatch, 0)
	if low+1 == high {
		e, ok := be.getBatchFromDB(shardID, replicaID, low)
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
	fk.SetEntryBatchKey(shardID, replicaID, low)
	lk.SetEntryBatchKey(shardID, replicaID, high)
	expectedID := low
	op := func(key []byte, data []byte) (bool, error) {
		var eb pb.EntryBatch
		pb.MustUnmarshal(&eb, data)
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

func (be *batchedEntries) getRange(shardID uint64,
	replicaID uint64, snapshotIndex uint64, maxIndex uint64) (uint64, uint64, error) {
	fk := be.keys.get()
	lk := be.keys.get()
	defer fk.Release()
	defer lk.Release()
	low, high := getBatchIDRange(snapshotIndex, maxIndex+1)
	fk.SetEntryBatchKey(shardID, replicaID, low)
	lk.SetEntryBatchKey(shardID, replicaID, high)
	firstIndex := uint64(0)
	length := uint64(0)
	op := func(key []byte, data []byte) (bool, error) {
		var eb pb.EntryBatch
		pb.MustUnmarshal(&eb, data)
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

func (be *batchedEntries) rangedOp(shardID uint64,
	replicaID uint64, index uint64, op func(fk *Key, lk *Key) error) error {
	fk := be.keys.get()
	lk := be.keys.get()
	defer fk.Release()
	defer lk.Release()
	batchID := getBatchID(index)
	if batchID == 0 || batchID == 1 {
		return nil
	}
	fk.SetEntryBatchKey(shardID, replicaID, 0)
	lk.SetEntryBatchKey(shardID, replicaID, batchID-1)
	return op(fk, lk)
}

func (be *batchedEntries) recordBatch(wb kv.IWriteBatch,
	shardID uint64, replicaID uint64, eb pb.EntryBatch,
	firstBatchID uint64, lastBatchID uint64, ctx IContext) {
	if len(eb.Entries) == 0 {
		return
	}
	batchID := getBatchID(eb.Entries[0].Index)
	var meb pb.EntryBatch
	if firstBatchID == batchID {
		lb := ctx.GetLastEntryBatch()
		meb = be.getMergedFirstBatch(shardID, replicaID, eb, lb)
	} else {
		meb = eb
	}
	if lastBatchID == batchID {
		be.cs.setLastBatch(shardID, replicaID, meb)
	}
	if len(meb.Entries) > 1 {
		meb = compactBatchFields(meb)
	}
	szul := meb.SizeUpperLimit()
	data := ctx.GetValueBuffer(uint64(szul))
	data = pb.MustMarshalTo(&meb, data)
	k := ctx.GetKey()
	k.SetEntryBatchKey(shardID, replicaID, batchID)
	wb.Put(k.Key(), data)
}

func (be *batchedEntries) record(wb kv.IWriteBatch,
	shardID uint64, replicaID uint64, ctx IContext, entries []pb.Entry) uint64 {
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
			be.recordBatch(wb, shardID, replicaID, eb, firstBatchID, lastBatchID, ctx)
			eb.Entries = eb.Entries[:0]
			currentBatchIdx = batchID
		}
		eb.Entries = append(eb.Entries, ent)
		idx++
	}
	if len(eb.Entries) > 0 {
		be.recordBatch(wb, shardID, replicaID, eb, firstBatchID, lastBatchID, ctx)
	}
	return maxIndex
}

func (be *batchedEntries) getBatchFromDB(shardID uint64,
	replicaID uint64, batchID uint64) (pb.EntryBatch, bool) {
	var e pb.EntryBatch
	k := be.keys.get()
	defer k.Release()
	k.SetEntryBatchKey(shardID, replicaID, batchID)
	if err := be.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return errors.New("no such entry")
		}
		pb.MustUnmarshal(&e, data)
		return nil
	}); err != nil {
		return e, false
	}
	if len(e.Entries) > 1 {
		return restoreBatchFields(e), true
	}
	return e, true
}

func (be *batchedEntries) getLastBatch(shardID uint64,
	replicaID uint64, firstIndex uint64, lb pb.EntryBatch) (pb.EntryBatch, bool) {
	batchID := getBatchID(firstIndex)
	lb, ok := be.cs.getLastBatch(shardID, replicaID, lb)
	if !ok || batchID < getBatchID(lb.Entries[0].Index) {
		lb, ok = be.getBatchFromDB(shardID, replicaID, batchID)
		if !ok {
			return pb.EntryBatch{}, false
		}
	}
	return lb, true
}

func (be *batchedEntries) getMergedFirstBatch(shardID uint64,
	replicaID uint64, eb pb.EntryBatch, lb pb.EntryBatch) pb.EntryBatch {
	// batch aligned
	if eb.Entries[0].Index%batchSize == 0 {
		return eb
	}
	lb, ok := be.getLastBatch(shardID, replicaID, eb.Entries[0].Index, lb)
	if !ok {
		return eb
	}
	return getMergedFirstBatch(eb, lb)
}

func (be *batchedEntries) binaryFormat() uint32 {
	return raftio.LogDBBinVersion
}
