// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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
	"encoding/binary"
	"errors"
	"math"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

const (
	// RocksDBLogDBName is the type name of the rocksdb LogDB.
	RocksDBLogDBName = "rocksdb-logdb"
	// LogDBType is the logdb type name.
	LogDBType = "fully-batched rocksdb"
)

var (
	batchSize = settings.Hard.LogDBEntryBatchSize
)

// RDB is the struct used to manage rocksdb backed persistent Log stores.
type RDB struct {
	cs   *rdbcache
	keys *logdbKeyPool
	kvs  IKvStore
}

func openRDB(dir string, wal string) (*RDB, error) {
	rocksdb, err := openLMDB(dir, wal)
	if err != nil {
		return nil, err
	}
	return &RDB{
		cs:   newRDBCache(),
		keys: newLogdbKeyPool(),
		kvs:  rocksdb,
	}, nil
}

func (r *RDB) close() {
	if err := r.kvs.Close(); err != nil {
		panic(err)
	}
}

func (r *RDB) getWriteBatch() IWriteBatch {
	return r.kvs.GetWriteBatch(nil)
}

func (r *RDB) listNodeInfo() ([]raftio.NodeInfo, error) {
	firstKey := newKey(bootstrapKeySize, nil)
	lastKey := newKey(bootstrapKeySize, nil)
	firstKey.setBootstrapKey(0, 0)
	lastKey.setBootstrapKey(math.MaxUint64, math.MaxUint64)
	ni := make([]raftio.NodeInfo, 0)
	op := func(key []byte, data []byte) (bool, error) {
		cid, nid := parseNodeInfoKey(key)
		ni = append(ni, raftio.GetNodeInfo(cid, nid))
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), true, op)
	return ni, nil
}

func (r *RDB) readRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (*raftio.RaftState, error) {
	firstIndex, length, err := r.getEntryRange(clusterID, nodeID, lastIndex)
	if err != nil {
		return nil, err
	}
	state, err := r.readState(clusterID, nodeID)
	if err != nil {
		return nil, err
	}
	rs := &raftio.RaftState{
		State:      state,
		FirstIndex: firstIndex,
		EntryCount: length,
	}
	return rs, nil
}

func (r *RDB) saveRaftState(updates []pb.Update,
	ctx raftio.IContext) error {
	wb := r.kvs.GetWriteBatch(ctx)
	for _, ud := range updates {
		r.recordState(ud.ClusterID, ud.NodeID, ud.State, wb, ctx)
		if !pb.IsEmptySnapshot(ud.Snapshot) {
			if len(ud.EntriesToSave) > 0 {
				if ud.Snapshot.Index > ud.EntriesToSave[len(ud.EntriesToSave)-1].Index {
					plog.Panicf("max index not handled, %d, %d",
						ud.Snapshot.Index, ud.EntriesToSave[len(ud.EntriesToSave)-1].Index)
				}
			}
			r.recordSnapshot(wb, ud)
			r.setMaxIndex(wb, ud, ud.Snapshot.Index, ctx)
		}
	}
	r.saveEntries(updates, wb, ctx)
	if wb.Count() > 0 {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *RDB) setMaxIndex(wb IWriteBatch,
	ud pb.Update, maxIndex uint64, ctx raftio.IContext) {
	r.cs.setMaxIndex(ud.ClusterID, ud.NodeID, maxIndex)
	r.recordMaxIndex(wb, ud.ClusterID, ud.NodeID, maxIndex, ctx)
}

func (r *RDB) recordSnapshot(wb IWriteBatch, ud pb.Update) {
	if pb.IsEmptySnapshot(ud.Snapshot) {
		return
	}
	ko := newKey(snapshotKeySize, nil)
	ko.setSnapshotKey(ud.ClusterID, ud.NodeID, ud.Snapshot.Index)
	data, err := ud.Snapshot.Marshal()
	if err != nil {
		panic(err)
	}
	wb.Put(ko.Key(), data)
}

func (r *RDB) recordMaxIndex(wb IWriteBatch,
	clusterID uint64, nodeID uint64, index uint64, ctx raftio.IContext) {
	data := ctx.GetValueBuffer(8)
	binary.BigEndian.PutUint64(data, index)
	data = data[:8]
	ko := ctx.GetKey()
	ko.SetMaxIndexKey(clusterID, nodeID)
	wb.Put(ko.Key(), data)
}

func (r *RDB) recordState(clusterID uint64,
	nodeID uint64, st pb.State,
	wb IWriteBatch, ctx raftio.IContext) {
	if pb.IsEmptyState(st) {
		return
	}
	if !r.cs.setState(clusterID, nodeID, st) {
		return
	}
	data := ctx.GetValueBuffer(uint64(st.Size()))
	ms, err := st.MarshalTo(data)
	if err != nil {
		panic(err)
	}
	data = data[:ms]
	ko := ctx.GetKey()
	ko.SetStateKey(clusterID, nodeID)
	wb.Put(ko.Key(), data)
}

func (r *RDB) saveBootstrapInfo(clusterID uint64,
	nodeID uint64, bootstrap pb.Bootstrap) error {
	data, err := bootstrap.Marshal()
	if err != nil {
		panic(err)
	}
	ko := newKey(maxKeySize, nil)
	ko.setBootstrapKey(clusterID, nodeID)
	return r.kvs.SaveValue(ko.Key(), data)
}

func (r *RDB) getBootstrapInfo(clusterID uint64,
	nodeID uint64) (*pb.Bootstrap, error) {
	ko := newKey(maxKeySize, nil)
	ko.setBootstrapKey(clusterID, nodeID)
	bootstrap := &pb.Bootstrap{}
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoBootstrapInfo
		}
		if err := bootstrap.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return bootstrap, nil
}

func (r *RDB) saveEntries(updates []pb.Update,
	wb IWriteBatch, ctx raftio.IContext) {
	if len(updates) == 0 {
		return
	}
	for _, ud := range updates {
		clusterID := ud.ClusterID
		nodeID := ud.NodeID
		if len(ud.EntriesToSave) > 0 {
			mi := r.recordEntries(wb, clusterID, nodeID, ctx, ud.EntriesToSave)
			if mi > 0 {
				r.setMaxIndex(wb, ud, mi, ctx)
			}
		}
	}
}

func (r *RDB) iterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	maxIndex, err := r.readMaxIndex(clusterID, nodeID)
	if err == raftio.ErrNoSavedLog {
		return ents, size, nil
	}
	if err != nil {
		panic(err)
	}
	if high > maxIndex+1 {
		high = maxIndex + 1
	}
	lowID, highID := getBatchIDRange(low, high)
	ebs, err := r.iterateEntryBatches(clusterID, nodeID, lowID, highID)
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

func (r *RDB) iterateEntryBatches(clusterID uint64,
	nodeID uint64, low uint64, high uint64) ([]pb.EntryBatch, error) {
	ents := make([]pb.EntryBatch, 0)
	if low+1 == high {
		e, ok := r.getEntryBatchFromDB(clusterID, nodeID, low)
		if !ok {
			return []pb.EntryBatch{}, nil
		}
		ents = append(ents, e)
		return ents, nil
	}
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	firstKey.SetEntryBatchKey(clusterID, nodeID, low)
	lastKey.SetEntryBatchKey(clusterID, nodeID, high)
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
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), false, op)
	return ents, nil
}

func (r *RDB) saveSnapshots(updates []pb.Update) error {
	wb := r.kvs.GetWriteBatch(nil)
	defer wb.Destroy()
	toSave := false
	for _, ud := range updates {
		if ud.Snapshot.Index > 0 {
			r.recordSnapshot(wb, ud)
			toSave = true
		}
	}
	if toSave {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *RDB) deleteSnapshot(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) error {
	ko := r.keys.get()
	defer ko.Release()
	ko.setSnapshotKey(clusterID, nodeID, snapshotIndex)
	return r.kvs.DeleteValue(ko.Key())
}

func (r *RDB) listSnapshots(clusterID uint64,
	nodeID uint64) ([]pb.Snapshot, error) {
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	firstKey.setSnapshotKey(clusterID, nodeID, 0)
	lastKey.setSnapshotKey(clusterID, nodeID, math.MaxUint64)
	snapshots := make([]pb.Snapshot, 0)
	op := func(key []byte, data []byte) (bool, error) {
		var ss pb.Snapshot
		if err := ss.Unmarshal(data); err != nil {
			panic(err)
		}
		snapshots = append(snapshots, ss)
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), true, op)
	return snapshots, nil
}

func (r *RDB) getEntryRange(clusterID uint64,
	nodeID uint64, lastIndex uint64) (uint64, uint64, error) {
	maxIndex, err := r.readMaxIndex(clusterID, nodeID)
	if err == raftio.ErrNoSavedLog {
		return lastIndex, 0, nil
	}
	if err != nil {
		panic(err)
	}
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	low, high := getBatchIDRange(lastIndex, maxIndex+1)
	firstKey.SetEntryBatchKey(clusterID, nodeID, low)
	lastKey.SetEntryBatchKey(clusterID, nodeID, high)
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
			if e.Index >= lastIndex && e.Index <= maxIndex {
				length++
				if firstIndex == uint64(0) {
					firstIndex = e.Index
				}
			}
		}
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), false, op)
	return firstIndex, length, nil
}

func (r *RDB) readMaxIndex(clusterID uint64, nodeID uint64) (uint64, error) {
	if v, ok := r.cs.getMaxIndex(clusterID, nodeID); ok {
		return v, nil
	}
	ko := r.keys.get()
	defer ko.Release()
	ko.SetMaxIndexKey(clusterID, nodeID)
	maxIndex := uint64(0)
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoSavedLog
		}
		maxIndex = binary.BigEndian.Uint64(data)
		return nil
	}); err != nil {
		return 0, err
	}
	return maxIndex, nil
}

func (r *RDB) readState(clusterID uint64,
	nodeID uint64) (*pb.State, error) {
	ko := r.keys.get()
	defer ko.Release()
	ko.SetStateKey(clusterID, nodeID)
	hs := &pb.State{}
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoSavedLog
		}
		if err := hs.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return hs, nil
}

func (r *RDB) removeEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	op := func(firstKey *PooledKey, lastKey *PooledKey) error {
		return r.kvs.RemoveEntries(firstKey.Key(), lastKey.Key())
	}
	return r.rangedEntryOp(clusterID, nodeID, index, op)
}

func (r *RDB) compaction(clusterID uint64, nodeID uint64, index uint64) error {
	op := func(firstKey *PooledKey, lastKey *PooledKey) error {
		return r.kvs.Compaction(firstKey.Key(), lastKey.Key())
	}
	return r.rangedEntryOp(clusterID, nodeID, index, op)
}

func (r *RDB) rangedEntryOp(clusterID uint64,
	nodeID uint64, index uint64,
	op func(firstKey *PooledKey, lastKey *PooledKey) error) error {
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	batchID := getBatchID(index)
	if batchID == 0 || batchID == 1 {
		return nil
	}
	firstKey.SetEntryBatchKey(clusterID, nodeID, 0)
	lastKey.SetEntryBatchKey(clusterID, nodeID, batchID-1)
	return op(firstKey, lastKey)
}

func (r *RDB) recordEntryBatch(wb IWriteBatch,
	clusterID uint64, nodeID uint64, eb pb.EntryBatch,
	firstBatchID uint64, lastBatchID uint64, ctx raftio.IContext) {
	if len(eb.Entries) == 0 {
		return
	}
	batchID := getBatchID(eb.Entries[0].Index)
	var meb pb.EntryBatch
	if firstBatchID == batchID {
		lb := ctx.GetLastEntryBatch()
		meb = r.getMergedFirstBatch(clusterID, nodeID, eb, lb)
	} else {
		meb = eb
	}
	if lastBatchID == batchID {
		r.cs.setLastEntryBatch(clusterID, nodeID, meb)
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
	key := ctx.GetKey()
	key.SetEntryBatchKey(clusterID, nodeID, batchID)
	wb.Put(key.Key(), data)
}

func (r *RDB) recordEntries(wb IWriteBatch,
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
			r.recordEntryBatch(wb,
				clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
			eb.Entries = eb.Entries[:0]
			currentBatchIdx = batchID
		}
		eb.Entries = append(eb.Entries, ent)
		idx++
	}
	if len(eb.Entries) > 0 {
		r.recordEntryBatch(wb,
			clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
	}
	return maxIndex
}

func (r *RDB) getEntryBatchFromDB(clusterID uint64,
	nodeID uint64, batchID uint64) (pb.EntryBatch, bool) {
	var e pb.EntryBatch
	key := r.keys.get()
	defer key.Release()
	key.SetEntryBatchKey(clusterID, nodeID, batchID)
	if err := r.kvs.GetValue(key.Key(), func(data []byte) error {
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

func (r *RDB) getLastBatch(clusterID uint64,
	nodeID uint64, firstIndex uint64, lb pb.EntryBatch) (pb.EntryBatch, bool) {
	batchID := getBatchID(firstIndex)
	lb, ok := r.cs.getLastEntryBatch(clusterID, nodeID, lb)
	if !ok || batchID < getBatchID(lb.Entries[0].Index) {
		lb, ok = r.getEntryBatchFromDB(clusterID, nodeID, batchID)
		if !ok {
			return pb.EntryBatch{}, false
		}
	}
	return lb, true
}

func (r *RDB) getMergedFirstBatch(clusterID uint64,
	nodeID uint64, eb pb.EntryBatch, lb pb.EntryBatch) pb.EntryBatch {
	// batch aligned
	if eb.Entries[0].Index%batchSize == 0 {
		return eb
	}
	lb, ok := r.getLastBatch(clusterID, nodeID, eb.Entries[0].Index, lb)
	if !ok {
		return eb
	}
	return getMergedFirstBatch(eb, lb)
}
