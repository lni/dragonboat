// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"math"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	batchSize = settings.Hard.LogDBEntryBatchSize
)

type entryManager interface {
	binaryFormat() uint32
	record(wb kv.IWriteBatch,
		clusterID uint64, nodeID uint64, ctx IContext, entries []pb.Entry) uint64
	iterate(ents []pb.Entry, maxIndex uint64,
		size uint64, clusterID uint64, nodeID uint64,
		low uint64, high uint64, maxSize uint64) ([]pb.Entry, uint64, error)
	getRange(clusterID uint64,
		nodeID uint64, snapshotIndex uint64,
		maxIndex uint64) (uint64, uint64, error)
	rangedOp(clusterID uint64,
		nodeID uint64, index uint64, op func(*Key, *Key) error) error
}

// db is the struct used to manage log DB.
type db struct {
	cs      *cache
	keys    *keyPool
	kvs     kv.IKVStore
	entries entryManager
}

func hasEntryRecord(kvs kv.IKVStore, batched bool) (bool, error) {
	fk := newKey(entryKeySize, nil)
	lk := newKey(entryKeySize, nil)
	if !batched {
		fk.SetEntryKey(0, 0, 0)
		lk.SetEntryKey(math.MaxUint64, math.MaxUint64, math.MaxUint64)
	} else {
		fk.SetEntryBatchKey(0, 0, 0)
		lk.SetEntryBatchKey(math.MaxUint64, math.MaxUint64, math.MaxUint64)
	}
	located := false
	op := func(key []byte, data []byte) (bool, error) {
		located = true
		return false, nil
	}
	if err := kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
		return false, err
	}
	return located, nil
}

func openRDB(config config.LogDBConfig,
	callback kv.LogDBCallback, dir string, wal string, batched bool,
	fs vfs.IFS, kvf kvFactory) (*db, error) {
	kvs, err := kvf(config, callback, dir, wal, fs)
	if err != nil {
		return nil, err
	}
	cs := newCache()
	pool := newLogDBKeyPool()
	var em entryManager
	if batched {
		em = newBatchedEntries(cs, pool, kvs)
	} else {
		em = newPlainEntries(cs, pool, kvs)
	}
	return &db{
		cs:      cs,
		keys:    pool,
		kvs:     kvs,
		entries: em,
	}, nil
}

func (r *db) name() string {
	return r.kvs.Name()
}

func (r *db) selfCheckFailed() (bool, error) {
	_, batched := r.entries.(*batchedEntries)
	return hasEntryRecord(r.kvs, !batched)
}

func (r *db) binaryFormat() uint32 {
	return r.entries.binaryFormat()
}

func (r *db) close() {
	if err := r.kvs.Close(); err != nil {
		panic(err)
	}
}

func (r *db) getWriteBatch(ctx IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb == nil {
			wb = r.kvs.GetWriteBatch()
			ctx.SetWriteBatch(wb)
		}
		return wb.(kv.IWriteBatch)
	}
	return r.kvs.GetWriteBatch()
}

func (r *db) listNodeInfo() ([]raftio.NodeInfo, error) {
	fk := newKey(bootstrapKeySize, nil)
	lk := newKey(bootstrapKeySize, nil)
	fk.setBootstrapKey(0, 0)
	lk.setBootstrapKey(math.MaxUint64, math.MaxUint64)
	ni := make([]raftio.NodeInfo, 0)
	op := func(key []byte, data []byte) (bool, error) {
		cid, nid := parseNodeInfoKey(key)
		ni = append(ni, raftio.GetNodeInfo(cid, nid))
		return true, nil
	}
	if err := r.kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
		return []raftio.NodeInfo{}, err
	}
	return ni, nil
}

func (r *db) readRaftState(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) (raftio.RaftState, error) {
	firstIndex, length, err := r.getRange(clusterID, nodeID, snapshotIndex)
	if err != nil {
		return raftio.RaftState{}, err
	}
	state, err := r.getState(clusterID, nodeID)
	if err != nil {
		return raftio.RaftState{}, err
	}
	return raftio.RaftState{
		State:      state,
		FirstIndex: firstIndex,
		EntryCount: length,
	}, nil
}

func (r *db) getRange(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) (uint64, uint64, error) {
	maxIndex, err := r.getMaxIndex(clusterID, nodeID)
	if err == raftio.ErrNoSavedLog {
		return snapshotIndex, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if snapshotIndex == maxIndex {
		return snapshotIndex, 0, nil
	}
	return r.entries.getRange(clusterID, nodeID, snapshotIndex, maxIndex)
}

func (r *db) saveRaftState(updates []pb.Update, ctx IContext) error {
	wb := r.getWriteBatch(ctx)
	for _, ud := range updates {
		r.saveState(ud.ClusterID, ud.NodeID, ud.State, wb, ctx)
		if !pb.IsEmptySnapshot(ud.Snapshot) {
			if len(ud.EntriesToSave) > 0 {
				// raft/inMemory makes sure such entries no longer need to be saved
				lastIndex := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index
				if ud.Snapshot.Index > lastIndex {
					plog.Panicf("max index not handled, %d, %d",
						ud.Snapshot.Index, lastIndex)
				}
			}
			r.saveSnapshot(wb, ud)
			r.setMaxIndex(wb, ud, ud.Snapshot.Index, ctx)
		}
	}
	r.saveEntries(updates, wb, ctx)
	if wb.Count() > 0 {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *db) importSnapshot(ss pb.Snapshot, nodeID uint64) error {
	if ss.Type == pb.UnknownStateMachine {
		panic("Unknown state machine type")
	}
	snapshots, err := r.listSnapshots(ss.ClusterId, nodeID, math.MaxUint64)
	if err != nil {
		return err
	}
	selectedss := make([]pb.Snapshot, 0)
	for _, curss := range snapshots {
		if curss.Index >= ss.Index {
			selectedss = append(selectedss, curss)
		}
	}
	wb := r.getWriteBatch(nil)
	bsrec := pb.Bootstrap{
		Join: true,
		Type: ss.Type,
	}
	state := pb.State{
		Term:   ss.Term,
		Commit: ss.Index,
	}
	r.saveRemoveNodeData(wb, selectedss, ss.ClusterId, nodeID)
	r.saveBootstrap(wb, ss.ClusterId, nodeID, bsrec)
	r.saveStateAllocs(wb, ss.ClusterId, nodeID, state)
	r.saveSnapshot(wb, pb.Update{
		ClusterID: ss.ClusterId,
		NodeID:    nodeID,
		Snapshot:  ss,
	})
	r.saveMaxIndex(wb, ss.ClusterId, nodeID, ss.Index, nil)
	return r.kvs.CommitWriteBatch(wb)
}

func (r *db) setMaxIndex(wb kv.IWriteBatch,
	ud pb.Update, maxIndex uint64, ctx IContext) {
	r.cs.setMaxIndex(ud.ClusterID, ud.NodeID, maxIndex)
	r.saveMaxIndex(wb, ud.ClusterID, ud.NodeID, maxIndex, ctx)
}

func (r *db) saveBootstrap(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64, bs pb.Bootstrap) {
	k := newKey(maxKeySize, nil)
	k.setBootstrapKey(clusterID, nodeID)
	data, err := bs.Marshal()
	if err != nil {
		panic(err)
	}
	wb.Put(k.Key(), data)
}

func (r *db) saveSnapshot(wb kv.IWriteBatch, ud pb.Update) {
	if pb.IsEmptySnapshot(ud.Snapshot) {
		return
	}
	k := newKey(snapshotKeySize, nil)
	k.setSnapshotKey(ud.ClusterID, ud.NodeID, ud.Snapshot.Index)
	data, err := ud.Snapshot.Marshal()
	if err != nil {
		panic(err)
	}
	wb.Put(k.Key(), data)
}

func (r *db) saveMaxIndex(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64, index uint64, ctx IContext) {
	var data []byte
	var k IReusableKey
	if ctx != nil {
		data = ctx.GetValueBuffer(8)
	} else {
		data = make([]byte, 8)
	}
	binary.BigEndian.PutUint64(data, index)
	data = data[:8]
	if ctx != nil {
		k = ctx.GetKey()
	} else {
		k = newKey(maxKeySize, nil)
	}
	k.SetMaxIndexKey(clusterID, nodeID)
	wb.Put(k.Key(), data)
}

func (r *db) saveStateAllocs(wb kv.IWriteBatch,
	clusterID uint64, nodeID uint64, st pb.State) {
	data, err := st.Marshal()
	if err != nil {
		panic(err)
	}
	k := newKey(snapshotKeySize, nil)
	k.SetStateKey(clusterID, nodeID)
	wb.Put(k.Key(), data)
}

func (r *db) saveState(clusterID uint64,
	nodeID uint64, st pb.State, wb kv.IWriteBatch, ctx IContext) {
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
	k := ctx.GetKey()
	k.SetStateKey(clusterID, nodeID)
	wb.Put(k.Key(), data[:ms])
}

func (r *db) saveBootstrapInfo(clusterID uint64,
	nodeID uint64, bs pb.Bootstrap) error {
	wb := r.getWriteBatch(nil)
	r.saveBootstrap(wb, clusterID, nodeID, bs)
	return r.kvs.CommitWriteBatch(wb)
}

func (r *db) getBootstrapInfo(clusterID uint64,
	nodeID uint64) (pb.Bootstrap, error) {
	k := newKey(maxKeySize, nil)
	k.setBootstrapKey(clusterID, nodeID)
	bootstrap := pb.Bootstrap{}
	if err := r.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoBootstrapInfo
		}
		if err := bootstrap.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return pb.Bootstrap{}, err
	}
	return bootstrap, nil
}

func (r *db) saveSnapshots(updates []pb.Update) error {
	wb := r.getWriteBatch(nil)
	defer wb.Destroy()
	toSave := false
	for _, ud := range updates {
		if ud.Snapshot.Index > 0 {
			r.saveSnapshot(wb, ud)
			toSave = true
		}
	}
	if toSave {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *db) deleteSnapshot(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) error {
	k := r.keys.get()
	defer k.Release()
	k.setSnapshotKey(clusterID, nodeID, snapshotIndex)
	return r.kvs.DeleteValue(k.Key())
}

func (r *db) listSnapshots(clusterID uint64,
	nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	fk := r.keys.get()
	lk := r.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.setSnapshotKey(clusterID, nodeID, 0)
	lk.setSnapshotKey(clusterID, nodeID, index)
	snapshots := make([]pb.Snapshot, 0)
	op := func(key []byte, data []byte) (bool, error) {
		var ss pb.Snapshot
		if err := ss.Unmarshal(data); err != nil {
			panic(err)
		}
		snapshots = append(snapshots, ss)
		return true, nil
	}
	if err := r.kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
		return []pb.Snapshot{}, err
	}
	return snapshots, nil
}

func (r *db) getMaxIndex(clusterID uint64, nodeID uint64) (uint64, error) {
	if v, ok := r.cs.getMaxIndex(clusterID, nodeID); ok {
		return v, nil
	}
	k := r.keys.get()
	defer k.Release()
	k.SetMaxIndexKey(clusterID, nodeID)
	maxIndex := uint64(0)
	if err := r.kvs.GetValue(k.Key(), func(data []byte) error {
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

func (r *db) getState(clusterID uint64, nodeID uint64) (pb.State, error) {
	k := r.keys.get()
	defer k.Release()
	k.SetStateKey(clusterID, nodeID)
	hs := pb.State{}
	if err := r.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoSavedLog
		}
		if err := hs.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return pb.State{}, err
	}
	return hs, nil
}

func (r *db) removeEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	op := func(fk *Key, lk *Key) error {
		return r.kvs.BulkRemoveEntries(fk.Key(), lk.Key())
	}
	return r.entries.rangedOp(clusterID, nodeID, index, op)
}

func (r *db) removeNodeData(clusterID uint64, nodeID uint64) error {
	wb := r.getWriteBatch(nil)
	defer wb.Clear()
	snapshots, err := r.listSnapshots(clusterID, nodeID, math.MaxUint64)
	if err != nil {
		return err
	}
	r.saveRemoveNodeData(wb, snapshots, clusterID, nodeID)
	if err := r.kvs.CommitWriteBatch(wb); err != nil {
		return err
	}
	r.cs.setMaxIndex(clusterID, nodeID, 0)
	return r.removeEntriesTo(clusterID, nodeID, math.MaxUint64)
}

func (r *db) saveRemoveNodeData(wb kv.IWriteBatch,
	snapshots []pb.Snapshot, clusterID uint64, nodeID uint64) {
	stateKey := newKey(maxKeySize, nil)
	stateKey.SetStateKey(clusterID, nodeID)
	wb.Delete(stateKey.Key())
	bsKey := newKey(maxKeySize, nil)
	bsKey.setBootstrapKey(clusterID, nodeID)
	wb.Delete(bsKey.Key())
	miKey := newKey(maxKeySize, nil)
	miKey.SetMaxIndexKey(clusterID, nodeID)
	wb.Delete(miKey.Key())
	for _, ss := range snapshots {
		k := newKey(maxKeySize, nil)
		k.setSnapshotKey(clusterID, nodeID, ss.Index)
		wb.Delete(k.Key())
	}
}

func (r *db) compact(clusterID uint64, nodeID uint64, index uint64) error {
	op := func(fk *Key, lk *Key) error {
		return r.kvs.CompactEntries(fk.Key(), lk.Key())
	}
	return r.entries.rangedOp(clusterID, nodeID, index, op)
}

func (r *db) saveEntries(updates []pb.Update, wb kv.IWriteBatch, ctx IContext) {
	for _, ud := range updates {
		if len(ud.EntriesToSave) > 0 {
			mi := r.entries.record(wb, ud.ClusterID, ud.NodeID, ctx, ud.EntriesToSave)
			if mi > 0 {
				r.setMaxIndex(wb, ud, mi, ctx)
			}
		}
	}
}

func (r *db) iterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	maxIndex, err := r.getMaxIndex(clusterID, nodeID)
	if err == raftio.ErrNoSavedLog {
		return ents, size, nil
	}
	if err != nil {
		panic(err)
	}
	return r.entries.iterate(ents, maxIndex, size,
		clusterID, nodeID, low, high, maxSize)
}
