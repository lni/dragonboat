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

var (
	batchSize = settings.Hard.LogDBEntryBatchSize
)

type entryManager interface {
	record(wb IWriteBatch,
		clusterID uint64, nodeID uint64,
		ctx raftio.IContext, entries []pb.Entry) uint64
	iterate(ents []pb.Entry, maxIndex uint64,
		size uint64, clusterID uint64, nodeID uint64,
		low uint64, high uint64, maxSize uint64) ([]pb.Entry, uint64, error)
	getRange(clusterID uint64,
		nodeID uint64, lastIndex uint64, maxIndex uint64) (uint64, uint64, error)
	rangedOp(clusterID uint64,
		nodeID uint64, index uint64, op func(firstKey *PooledKey, lastKey *PooledKey) error) error
}

// RDB is the struct used to manage rocksdb backed persistent Log stores.
type RDB struct {
	cs      *rdbcache
	keys    *logdbKeyPool
	kvs     IKvStore
	entries entryManager
}

func openRDB(dir string, wal string) (*RDB, error) {
	kvs, err := newKVStore(dir, wal)
	if err != nil {
		return nil, err
	}
	cs := newRDBCache()
	pool := newLogdbKeyPool()
	return &RDB{
		cs:      cs,
		keys:    pool,
		kvs:     kvs,
		entries: newBatchedEntries(cs, pool, kvs),
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
	firstIndex, length, err := r.getRange(clusterID, nodeID, lastIndex)
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

func (r *RDB) getRange(clusterID uint64,
	nodeID uint64, lastIndex uint64) (uint64, uint64, error) {
	maxIndex, err := r.readMaxIndex(clusterID, nodeID)
	if err == raftio.ErrNoSavedLog {
		return lastIndex, 0, nil
	}
	if err != nil {
		panic(err)
	}
	return r.entries.getRange(clusterID, nodeID, lastIndex, maxIndex)
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

func (r *RDB) importSnapshot(ss pb.Snapshot, nodeID uint64) error {
	if ss.Type == pb.UnknownStateMachine {
		panic("Unknown state machine type")
	}
	snapshots, err := r.listSnapshots(ss.ClusterId, nodeID)
	if err != nil {
		return err
	}
	selectedss := make([]pb.Snapshot, 0)
	for _, curss := range snapshots {
		if curss.Index >= ss.Index {
			selectedss = append(selectedss, curss)
		}
	}
	wb := r.getWriteBatch()
	bsrec := pb.Bootstrap{Join: true, Type: ss.Type}
	state := pb.State{Term: ss.Term, Commit: ss.Index}
	r.recordRemoveNodeData(wb, selectedss, ss.ClusterId, nodeID)
	r.recordBootstrap(wb, ss.ClusterId, nodeID, bsrec)
	r.recordStateAllocs(wb, ss.ClusterId, nodeID, state)
	r.recordSnapshot(wb, pb.Update{
		ClusterID: ss.ClusterId, NodeID: nodeID, Snapshot: ss,
	})
	return r.kvs.CommitWriteBatch(wb)
}

func (r *RDB) setMaxIndex(wb IWriteBatch,
	ud pb.Update, maxIndex uint64, ctx raftio.IContext) {
	r.cs.setMaxIndex(ud.ClusterID, ud.NodeID, maxIndex)
	r.recordMaxIndex(wb, ud.ClusterID, ud.NodeID, maxIndex, ctx)
}

func (r *RDB) recordBootstrap(wb IWriteBatch,
	clusterID uint64, nodeID uint64, bsrec pb.Bootstrap) {
	bskey := newKey(maxKeySize, nil)
	bskey.setBootstrapKey(clusterID, nodeID)
	bsdata, err := bsrec.Marshal()
	if err != nil {
		panic(err)
	}
	wb.Put(bskey.Key(), bsdata)
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

func (r *RDB) recordStateAllocs(wb IWriteBatch,
	clusterID uint64, nodeID uint64, st pb.State) {
	data, err := st.Marshal()
	if err != nil {
		panic(err)
	}
	key := newKey(snapshotKeySize, nil)
	key.SetStateKey(clusterID, nodeID)
	wb.Put(key.Key(), data)
}

func (r *RDB) recordState(clusterID uint64,
	nodeID uint64, st pb.State, wb IWriteBatch, ctx raftio.IContext) {
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
	wb := r.getWriteBatch()
	r.recordBootstrap(wb, clusterID, nodeID, bootstrap)
	return r.kvs.CommitWriteBatch(wb)
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
	return r.entries.rangedOp(clusterID, nodeID, index, op)
}

func (r *RDB) removeNodeData(clusterID uint64, nodeID uint64) error {
	wb := r.getWriteBatch()
	defer wb.Clear()
	snapshots, err := r.listSnapshots(clusterID, nodeID)
	if err != nil {
		return err
	}
	r.recordRemoveNodeData(wb, snapshots, clusterID, nodeID)
	if err := r.kvs.CommitDeleteBatch(wb); err != nil {
		return err
	}
	if err := r.removeEntriesTo(clusterID, nodeID, math.MaxUint64); err != nil {
		return err
	}
	return r.compaction(clusterID, nodeID, math.MaxUint64)
}

func (r *RDB) recordRemoveNodeData(wb IWriteBatch,
	snapshots []pb.Snapshot, clusterID uint64, nodeID uint64) {
	stateKey := newKey(maxKeySize, nil)
	stateKey.SetStateKey(clusterID, nodeID)
	wb.Delete(stateKey.Key())
	bootstrapKey := newKey(maxKeySize, nil)
	bootstrapKey.setBootstrapKey(clusterID, nodeID)
	wb.Delete(bootstrapKey.Key())
	maxIndexKey := newKey(maxKeySize, nil)
	maxIndexKey.SetMaxIndexKey(clusterID, nodeID)
	wb.Delete(maxIndexKey.Key())
	for _, ss := range snapshots {
		key := newKey(maxKeySize, nil)
		key.setSnapshotKey(clusterID, nodeID, ss.Index)
		wb.Delete(key.Key())
	}
}

func (r *RDB) compaction(clusterID uint64, nodeID uint64, index uint64) error {
	op := func(firstKey *PooledKey, lastKey *PooledKey) error {
		return r.kvs.Compaction(firstKey.Key(), lastKey.Key())
	}
	return r.entries.rangedOp(clusterID, nodeID, index, op)
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
			mi := r.entries.record(wb, clusterID, nodeID, ctx, ud.EntriesToSave)
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
	return r.entries.iterate(ents,
		maxIndex, size, clusterID, nodeID, low, high, maxSize)
}

type batchedEntries struct {
	cs   *rdbcache
	keys *logdbKeyPool
	kvs  IKvStore
}

func newBatchedEntries(cs *rdbcache,
	keys *logdbKeyPool, kvs IKvStore) entryManager {
	return &batchedEntries{
		cs:   cs,
		keys: keys,
		kvs:  kvs,
	}
}

func (r *batchedEntries) iterate(ents []pb.Entry, maxIndex uint64,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	if high > maxIndex+1 {
		high = maxIndex + 1
	}
	lowID, highID := getBatchIDRange(low, high)
	ebs, err := r.iterateBatches(clusterID, nodeID, lowID, highID)
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

func (r *batchedEntries) iterateBatches(clusterID uint64,
	nodeID uint64, low uint64, high uint64) ([]pb.EntryBatch, error) {
	ents := make([]pb.EntryBatch, 0)
	if low+1 == high {
		e, ok := r.getBatchFromDB(clusterID, nodeID, low)
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

func (r *batchedEntries) getRange(clusterID uint64,
	nodeID uint64, lastIndex uint64, maxIndex uint64) (uint64, uint64, error) {
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

func (r *batchedEntries) rangedOp(clusterID uint64,
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

func (r *batchedEntries) recordBatch(wb IWriteBatch,
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

func (r *batchedEntries) record(wb IWriteBatch,
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
			r.recordBatch(wb, clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
			eb.Entries = eb.Entries[:0]
			currentBatchIdx = batchID
		}
		eb.Entries = append(eb.Entries, ent)
		idx++
	}
	if len(eb.Entries) > 0 {
		r.recordBatch(wb, clusterID, nodeID, eb, firstBatchID, lastBatchID, ctx)
	}
	return maxIndex
}

func (r *batchedEntries) getBatchFromDB(clusterID uint64,
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

func (r *batchedEntries) getLastBatch(clusterID uint64,
	nodeID uint64, firstIndex uint64, lb pb.EntryBatch) (pb.EntryBatch, bool) {
	batchID := getBatchID(firstIndex)
	lb, ok := r.cs.getLastEntryBatch(clusterID, nodeID, lb)
	if !ok || batchID < getBatchID(lb.Entries[0].Index) {
		lb, ok = r.getBatchFromDB(clusterID, nodeID, batchID)
		if !ok {
			return pb.EntryBatch{}, false
		}
	}
	return lb, true
}

func (r *batchedEntries) getMergedFirstBatch(clusterID uint64,
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
