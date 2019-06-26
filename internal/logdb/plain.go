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
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type plainEntries struct {
	cs   *rdbcache
	keys *logdbKeyPool
	kvs  kv.IKVStore
}

func newPlainEntries(cs *rdbcache,
	keys *logdbKeyPool, kvs kv.IKVStore) entryManager {
	return &plainEntries{
		cs:   cs,
		keys: keys,
		kvs:  kvs,
	}
}

func (pe *plainEntries) record(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64,
	ctx raftio.IContext, entries []pb.Entry) uint64 {
	idx := 0
	maxIndex := uint64(0)
	for idx < len(entries) {
		ent := entries[idx]
		esz := uint64(ent.SizeUpperLimit())
		data := ctx.GetValueBuffer(esz)
		if uint64(len(data)) < esz {
			panic("got a small buffer")
		}
		ms, err := ent.MarshalTo(data)
		if err != nil {
			panic(err)
		}
		data = data[:ms]
		k := ctx.GetKey()
		k.SetEntryKey(clusterID, nodeID, ent.Index)
		wb.Put(k.Key(), data)
		if ent.Index > maxIndex {
			maxIndex = ent.Index
		}
		idx++
	}
	return maxIndex
}

func (pe *plainEntries) iterate(ents []pb.Entry, maxIndex uint64,
	size uint64, clusterID uint64, nodeID uint64,
	low uint64, high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	if low+1 == high && low <= maxIndex {
		e, err := pe.getEntry(clusterID, nodeID, low)
		if err != nil {
			return nil, 0, err
		}
		ents = append(ents, e)
		size += uint64(e.SizeUpperLimit())
		return ents, size, nil
	}
	if high > maxIndex+1 {
		high = maxIndex + 1
	}
	fk := pe.keys.get()
	lk := pe.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.SetEntryKey(clusterID, nodeID, low)
	lk.SetEntryKey(clusterID, nodeID, high)
	expectedIndex := low
	op := func(key []byte, data []byte) (bool, error) {
		var e pb.Entry
		if err := e.Unmarshal(data); err != nil {
			panic(e)
		}
		if e.Index != expectedIndex {
			return false, nil
		}
		size += uint64(e.SizeUpperLimit())
		ents = append(ents, e)
		expectedIndex++
		if size > maxSize {
			return false, nil
		}
		return true, nil
	}
	if err := pe.kvs.IterateValue(fk.Key(), lk.Key(), false, op); err != nil {
		return nil, 0, err
	}
	return ents, size, nil
}

func (pe *plainEntries) getEntry(clusterID uint64,
	nodeID uint64, index uint64) (pb.Entry, error) {
	k := pe.keys.get()
	defer k.Release()
	k.SetEntryKey(clusterID, nodeID, index)
	var e pb.Entry
	op := func(v []byte) error {
		if err := e.Unmarshal(v); err != nil {
			panic(err)
		}
		return nil
	}
	if err := pe.kvs.GetValue(k.Key(), op); err != nil {
		return pb.Entry{}, err
	}
	return e, nil
}

func (pe *plainEntries) getRange(clusterID uint64,
	nodeID uint64, snapshotIndex uint64, maxIndex uint64) (uint64, uint64, error) {
	fk := pe.keys.get()
	lk := pe.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.SetEntryKey(clusterID, nodeID, snapshotIndex)
	lk.SetEntryKey(clusterID, nodeID, maxIndex)
	firstIndex := uint64(0)
	length := uint64(0)
	op := func(key []byte, data []byte) (bool, error) {
		if firstIndex == 0 {
			var e pb.Entry
			if err := e.Unmarshal(data); err != nil {
				return false, err
			}
			firstIndex = e.Index
			return false, nil
		}
		return true, nil
	}
	if err := pe.kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
		return 0, 0, err
	}
	if firstIndex == 0 && maxIndex != 0 {
		plog.Panicf("first index %d, max index %d", firstIndex, maxIndex)
	}
	if firstIndex > 0 {
		length = maxIndex - firstIndex + 1
	}
	return firstIndex, length, nil
}

func (pe *plainEntries) rangedOp(clusterID uint64,
	nodeID uint64, index uint64,
	op func(fk *PooledKey, lk *PooledKey) error) error {
	fk := pe.keys.get()
	lk := pe.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.SetEntryKey(clusterID, nodeID, 0)
	lk.SetEntryKey(clusterID, nodeID, index)
	return op(fk, lk)
}

func (pe *plainEntries) binaryFormat() uint32 {
	return raftio.PlainLogDBBinVersion
}
