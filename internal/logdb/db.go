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

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	batchSize = settings.Hard.LogDBEntryBatchSize
)

type entryManager interface {
	binaryFormat() uint32
	record(wb kv.IWriteBatch,
		shardID uint64, replicaID uint64, ctx IContext, entries []pb.Entry) uint64
	iterate(ents []pb.Entry, maxIndex uint64,
		size uint64, shardID uint64, replicaID uint64,
		low uint64, high uint64, maxSize uint64) ([]pb.Entry, uint64, error)
	getRange(shardID uint64,
		replicaID uint64, snapshotIndex uint64,
		maxIndex uint64) (uint64, uint64, error)
	rangedOp(shardID uint64,
		replicaID uint64, index uint64, op func(*Key, *Key) error) error
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
	fs vfs.IFS, kvf kv.Factory) (*db, error) {
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

func (r *db) close() error {
	return r.kvs.Close()
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

func (r *db) readRaftState(shardID uint64,
	replicaID uint64, snapshotIndex uint64) (raftio.RaftState, error) {
	firstIndex, length, err := r.getRange(shardID, replicaID, snapshotIndex)
	if err != nil {
		return raftio.RaftState{}, err
	}
	state, err := r.getState(shardID, replicaID)
	if err != nil {
		return raftio.RaftState{}, err
	}
	return raftio.RaftState{
		State:      state,
		FirstIndex: firstIndex,
		EntryCount: length,
	}, nil
}

func (r *db) getRange(shardID uint64,
	replicaID uint64, snapshotIndex uint64) (uint64, uint64, error) {
	maxIndex, err := r.getMaxIndex(shardID, replicaID)
	if err == raftio.ErrNoSavedLog {
		return snapshotIndex, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if snapshotIndex == maxIndex {
		return snapshotIndex, 0, nil
	}
	return r.entries.getRange(shardID, replicaID, snapshotIndex, maxIndex)
}

func (r *db) saveRaftState(updates []pb.Update, ctx IContext) error {
	wb := r.getWriteBatch(ctx)
	for _, ud := range updates {
		r.saveState(ud.ShardID, ud.ReplicaID, ud.State, wb, ctx)
		if !pb.IsEmptySnapshot(ud.Snapshot) &&
			r.cs.trySaveSnapshot(ud.ShardID, ud.ReplicaID, ud.Snapshot.Index) {
			if len(ud.EntriesToSave) > 0 {
				// raft/inMemory makes sure such entries no longer need to be saved
				lastIndex := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index
				if ud.Snapshot.Index > lastIndex {
					plog.Panicf("max index not handled, %d, %d",
						ud.Snapshot.Index, lastIndex)
				}
			}
			if err := r.saveSnapshot(wb, ud); err != nil {
				return nil
			}
			r.setMaxIndex(wb, ud, ud.Snapshot.Index, ctx)
		}
	}
	r.saveEntries(updates, wb, ctx)
	if wb.Count() > 0 {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *db) importSnapshot(ss pb.Snapshot, replicaID uint64) error {
	if ss.Type == pb.UnknownStateMachine {
		panic("Unknown state machine type")
	}
	snapshots, err := r.listSnapshots(ss.ShardID, replicaID, math.MaxUint64)
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
	r.saveRemoveNodeData(wb, selectedss, ss.ShardID, replicaID)
	r.saveBootstrap(wb, ss.ShardID, replicaID, bsrec)
	r.saveStateAllocs(wb, ss.ShardID, replicaID, state)
	if err := r.saveSnapshot(wb, pb.Update{
		ShardID:   ss.ShardID,
		ReplicaID: replicaID,
		Snapshot:  ss,
	}); err != nil {
		return err
	}
	r.saveMaxIndex(wb, ss.ShardID, replicaID, ss.Index, nil)
	return r.kvs.CommitWriteBatch(wb)
}

func (r *db) setMaxIndex(wb kv.IWriteBatch,
	ud pb.Update, maxIndex uint64, ctx IContext) {
	r.cs.setMaxIndex(ud.ShardID, ud.ReplicaID, maxIndex)
	r.saveMaxIndex(wb, ud.ShardID, ud.ReplicaID, maxIndex, ctx)
}

func (r *db) saveBootstrap(wb kv.IWriteBatch,
	shardID uint64, replicaID uint64, bs pb.Bootstrap) {
	k := newKey(maxKeySize, nil)
	k.setBootstrapKey(shardID, replicaID)
	data := pb.MustMarshal(&bs)
	wb.Put(k.Key(), data)
}

func (r *db) saveSnapshot(wb kv.IWriteBatch, ud pb.Update) error {
	if pb.IsEmptySnapshot(ud.Snapshot) {
		return nil
	}
	snapshots, err := r.listSnapshots(ud.ShardID, ud.ReplicaID, math.MaxUint64)
	if err != nil {
		return err
	}
	for _, ss := range snapshots {
		if ud.Snapshot.Index > ss.Index {
			k := newKey(maxKeySize, nil)
			k.setSnapshotKey(ud.ShardID, ud.ReplicaID, ss.Index)
			wb.Delete(k.Key())
		}
	}
	k := newKey(snapshotKeySize, nil)
	k.setSnapshotKey(ud.ShardID, ud.ReplicaID, ud.Snapshot.Index)
	data := pb.MustMarshal(&ud.Snapshot)
	wb.Put(k.Key(), data)
	return nil
}

func (r *db) saveMaxIndex(wb kv.IWriteBatch,
	shardID uint64, replicaID uint64, index uint64, ctx IContext) {
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
	k.SetMaxIndexKey(shardID, replicaID)
	wb.Put(k.Key(), data)
}

func (r *db) saveStateAllocs(wb kv.IWriteBatch,
	shardID uint64, replicaID uint64, st pb.State) {
	data := pb.MustMarshal(&st)
	k := newKey(snapshotKeySize, nil)
	k.SetStateKey(shardID, replicaID)
	wb.Put(k.Key(), data)
}

func (r *db) saveState(shardID uint64,
	replicaID uint64, st pb.State, wb kv.IWriteBatch, ctx IContext) {
	if pb.IsEmptyState(st) {
		return
	}
	if !r.cs.setState(shardID, replicaID, st) {
		return
	}
	data := ctx.GetValueBuffer(uint64(st.Size()))
	result := pb.MustMarshalTo(&st, data)
	k := ctx.GetKey()
	k.SetStateKey(shardID, replicaID)
	wb.Put(k.Key(), result)
}

func (r *db) saveBootstrapInfo(shardID uint64,
	replicaID uint64, bs pb.Bootstrap) error {
	wb := r.getWriteBatch(nil)
	r.saveBootstrap(wb, shardID, replicaID, bs)
	return r.kvs.CommitWriteBatch(wb)
}

func (r *db) getBootstrapInfo(shardID uint64,
	replicaID uint64) (pb.Bootstrap, error) {
	k := newKey(maxKeySize, nil)
	k.setBootstrapKey(shardID, replicaID)
	bootstrap := pb.Bootstrap{}
	if err := r.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoBootstrapInfo
		}
		pb.MustUnmarshal(&bootstrap, data)
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
		if !pb.IsEmptySnapshot(ud.Snapshot) &&
			r.cs.trySaveSnapshot(ud.ShardID, ud.ReplicaID, ud.Snapshot.Index) {
			if err := r.saveSnapshot(wb, ud); err != nil {
				return nil
			}
			toSave = true
		}
	}
	if toSave {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *db) getSnapshot(shardID uint64, replicaID uint64) (pb.Snapshot, error) {
	snapshots, err := r.listSnapshots(shardID, replicaID, math.MaxUint64)
	if err != nil {
		return pb.Snapshot{}, err
	}
	if len(snapshots) > 0 {
		ss := snapshots[len(snapshots)-1]
		r.cs.setSnapshotIndex(shardID, replicaID, ss.Index)
		return ss, nil
	}
	return pb.Snapshot{}, nil
}

// previously, snapshots are stored with its index value as the least
// significant part of the key. from v3.4, we only store the latest snapshot in
// LogDB and the least significant part of the key is set to math.MaxUint64.
func (r *db) listSnapshots(shardID uint64,
	replicaID uint64, index uint64) ([]pb.Snapshot, error) {
	fk := r.keys.get()
	lk := r.keys.get()
	defer fk.Release()
	defer lk.Release()
	fk.setSnapshotKey(shardID, replicaID, 0)
	lk.setSnapshotKey(shardID, replicaID, index)
	snapshots := make([]pb.Snapshot, 0)
	op := func(key []byte, data []byte) (bool, error) {
		var ss pb.Snapshot
		pb.MustUnmarshal(&ss, data)
		snapshots = append(snapshots, ss)
		return true, nil
	}
	if err := r.kvs.IterateValue(fk.Key(), lk.Key(), true, op); err != nil {
		return []pb.Snapshot{}, err
	}
	return snapshots, nil
}

func (r *db) getMaxIndex(shardID uint64, replicaID uint64) (uint64, error) {
	if v, ok := r.cs.getMaxIndex(shardID, replicaID); ok {
		return v, nil
	}
	k := r.keys.get()
	defer k.Release()
	k.SetMaxIndexKey(shardID, replicaID)
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

func (r *db) getState(shardID uint64, replicaID uint64) (pb.State, error) {
	k := r.keys.get()
	defer k.Release()
	k.SetStateKey(shardID, replicaID)
	hs := pb.State{}
	if err := r.kvs.GetValue(k.Key(), func(data []byte) error {
		if len(data) == 0 {
			return raftio.ErrNoSavedLog
		}
		pb.MustUnmarshal(&hs, data)
		return nil
	}); err != nil {
		return pb.State{}, err
	}
	return hs, nil
}

func (r *db) removeEntriesTo(shardID uint64,
	replicaID uint64, index uint64) error {
	op := func(fk *Key, lk *Key) error {
		return r.kvs.BulkRemoveEntries(fk.Key(), lk.Key())
	}
	return r.entries.rangedOp(shardID, replicaID, index, op)
}

func (r *db) removeNodeData(shardID uint64, replicaID uint64) error {
	wb := r.getWriteBatch(nil)
	defer wb.Clear()
	snapshots, err := r.listSnapshots(shardID, replicaID, math.MaxUint64)
	if err != nil {
		return err
	}
	r.saveRemoveNodeData(wb, snapshots, shardID, replicaID)
	if err := r.kvs.CommitWriteBatch(wb); err != nil {
		return err
	}
	r.cs.setMaxIndex(shardID, replicaID, 0)
	return r.removeEntriesTo(shardID, replicaID, math.MaxUint64)
}

func (r *db) saveRemoveNodeData(wb kv.IWriteBatch,
	snapshots []pb.Snapshot, shardID uint64, replicaID uint64) {
	stateKey := newKey(maxKeySize, nil)
	stateKey.SetStateKey(shardID, replicaID)
	wb.Delete(stateKey.Key())
	bsKey := newKey(maxKeySize, nil)
	bsKey.setBootstrapKey(shardID, replicaID)
	wb.Delete(bsKey.Key())
	miKey := newKey(maxKeySize, nil)
	miKey.SetMaxIndexKey(shardID, replicaID)
	wb.Delete(miKey.Key())
	for _, ss := range snapshots {
		k := newKey(maxKeySize, nil)
		k.setSnapshotKey(shardID, replicaID, ss.Index)
		wb.Delete(k.Key())
	}
}

func (r *db) compact(shardID uint64, replicaID uint64, index uint64) error {
	op := func(fk *Key, lk *Key) error {
		return r.kvs.CompactEntries(fk.Key(), lk.Key())
	}
	return r.entries.rangedOp(shardID, replicaID, index, op)
}

func (r *db) saveEntries(updates []pb.Update, wb kv.IWriteBatch, ctx IContext) {
	for _, ud := range updates {
		if len(ud.EntriesToSave) > 0 {
			mi := r.entries.record(wb, ud.ShardID, ud.ReplicaID, ctx, ud.EntriesToSave)
			if mi > 0 {
				r.setMaxIndex(wb, ud, mi, ctx)
			}
		}
	}
}

func (r *db) iterateEntries(ents []pb.Entry,
	size uint64, shardID uint64, replicaID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	maxIndex, err := r.getMaxIndex(shardID, replicaID)
	if err == raftio.ErrNoSavedLog {
		return ents, size, nil
	}
	if err != nil {
		err = errors.Wrapf(err, "%s failed to get max index", dn(shardID, replicaID))
		return nil, 0, err
	}
	entries, sz, err := r.entries.iterate(ents, maxIndex, size,
		shardID, replicaID, low, high, maxSize)
	err = errors.Wrapf(err, "%s failed to iterate entries, %d, %d, %d, %d",
		dn(shardID, replicaID), low, high, maxSize, maxIndex)
	return entries, sz, err
}
