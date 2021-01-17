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
	"fmt"
	"math"
	"sync/atomic"

	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

// ShardedDB is a LogDB implementation using sharded rocksdb instances.
type ShardedDB struct {
	completedCompactions uint64
	config               config.LogDBConfig
	ctxs                 []IContext
	shards               []*db
	partitioner          server.IPartitioner
	compactionCh         chan struct{}
	compactions          *compactions
	stopper              *syncutil.Stopper
}

var _ raftio.ILogDB = (*ShardedDB)(nil)

type shardCallback struct {
	shard uint64
	f     config.LogDBCallback
}

func (sc *shardCallback) callback(busy bool) {
	if sc.f != nil {
		sc.f(config.LogDBInfo{Shard: sc.shard, Busy: busy})
	}
}

// OpenShardedDB creates a ShardedDB instance.
func OpenShardedDB(config config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, lldirs []string, batched bool, check bool,
	fs vfs.IFS, kvf kvFactory) (*ShardedDB, error) {
	if config.Expert.LogDB.IsEmpty() {
		panic("config.Expert.LogDB.IsEmpty()")
	}
	if check && batched {
		plog.Panicf("check and batched both set")
	}
	shards := make([]*db, 0)
	closeAll := func(all []*db) {
		for _, s := range all {
			s.close()
		}
	}
	for i := uint64(0); i < config.Expert.LogDB.Shards; i++ {
		dir := fs.PathJoin(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lldirs) > 0 {
			lldir = fs.PathJoin(lldirs[i], fmt.Sprintf("logdb-%d", i))
		}
		sc := shardCallback{shard: i, f: cb}
		db, err := openRDB(config.Expert.LogDB,
			sc.callback, dir, lldir, batched, fs, kvf)
		if err != nil {
			closeAll(shards)
			return nil, err
		}
		shards = append(shards, db)
	}
	if check && !batched {
		for _, s := range shards {
			located, err := hasEntryRecord(s.kvs, true)
			if err != nil {
				closeAll(shards)
				return nil, err
			}
			if located {
				closeAll(shards)
				return OpenShardedDB(config, cb, dirs, lldirs, true, false, fs, kvf)
			}
		}
	}
	if batched {
		plog.Infof("using batched logdb")
	} else {
		plog.Infof("using plain logdb")
	}
	partitioner := server.NewDoubleFixedPartitioner(config.Expert.Engine.ExecShards,
		config.Expert.LogDB.Shards)
	mw := &ShardedDB{
		config:       config.Expert.LogDB,
		shards:       shards,
		ctxs:         make([]IContext, config.Expert.Engine.ExecShards),
		partitioner:  partitioner,
		compactions:  newCompactions(),
		compactionCh: make(chan struct{}, 1),
		stopper:      syncutil.NewStopper(),
	}
	for i := uint64(0); i < config.Expert.Engine.ExecShards; i++ {
		mw.ctxs[i] = newContext(mw.config.SaveBufferSize, mw.config.MaxSaveBufferSize)
	}
	mw.stopper.RunWorker(func() {
		mw.compactionWorkerMain()
	})
	return mw, nil
}

// Name returns the type name of the instance.
func (s *ShardedDB) Name() string {
	return fmt.Sprintf("sharded-%s", s.shards[0].name())
}

// BinaryFormat is the binary format supported by the sharded DB.
func (s *ShardedDB) BinaryFormat() uint32 {
	return s.shards[0].binaryFormat()
}

// SelfCheckFailed runs a self check on all db shards and report whether any
// failure is observed.
func (s *ShardedDB) SelfCheckFailed() (bool, error) {
	for _, shard := range s.shards {
		failed, err := shard.selfCheckFailed()
		if err != nil {
			return false, err
		}
		if failed {
			return true, nil
		}
	}
	return false, nil
}

// SaveRaftState saves the raft state and logs found in the raft.Update list
// to the log db.
func (s *ShardedDB) SaveRaftState(updates []pb.Update, shardID uint64) error {
	if shardID-1 >= uint64(len(s.ctxs)) {
		plog.Panicf("invalid shardID %d, len(s.ctxs): %d", shardID, len(s.ctxs))
	}
	ctx := s.ctxs[shardID-1]
	ctx.Reset()
	return s.SaveRaftStateCtx(updates, ctx)
}

// GetLogDBThreadContext return an IContext instance. This method is expected
// to be used in benchmarks and tests only.
func (s *ShardedDB) GetLogDBThreadContext() IContext {
	return newContext(s.config.SaveBufferSize, s.config.MaxSaveBufferSize)
}

// SaveRaftStateCtx saves the raft state and logs found in the raft.Update list
// to the log db.
func (s *ShardedDB) SaveRaftStateCtx(updates []pb.Update, ctx IContext) error {
	if len(updates) == 0 {
		return nil
	}
	pid := s.getParititionID(updates)
	return s.shards[pid].saveRaftState(updates, ctx)
}

// ReadRaftState returns the persistent state of the specified raft node.
func (s *ShardedDB) ReadRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (raftio.RaftState, error) {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].readRaftState(clusterID, nodeID, lastIndex)
}

// ListNodeInfo lists all available NodeInfo found in the log db.
func (s *ShardedDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	r := make([]raftio.NodeInfo, 0)
	for _, v := range s.shards {
		n, err := v.listNodeInfo()
		if err != nil {
			return nil, err
		}
		r = append(r, n...)
	}
	return r, nil
}

// SaveSnapshots saves all snapshot metadata found in the raft.Update list.
func (s *ShardedDB) SaveSnapshots(updates []pb.Update) error {
	if len(updates) == 0 {
		return nil
	}
	pid := s.getParititionID(updates)
	return s.shards[pid].saveSnapshots(updates)
}

// DeleteSnapshot removes the specified snapshot metadata from the log db.
func (s *ShardedDB) DeleteSnapshot(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) error {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].deleteSnapshot(clusterID, nodeID, snapshotIndex)
}

// ListSnapshots lists all available snapshots associated with the specified
// raft node.
func (s *ShardedDB) ListSnapshots(clusterID uint64,
	nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].listSnapshots(clusterID, nodeID, index)
}

// SaveBootstrapInfo saves the specified bootstrap info for the given node.
func (s *ShardedDB) SaveBootstrapInfo(clusterID uint64,
	nodeID uint64, bootstrap pb.Bootstrap) error {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].saveBootstrapInfo(clusterID, nodeID, bootstrap)
}

// GetBootstrapInfo returns the saved bootstrap info for the given node.
func (s *ShardedDB) GetBootstrapInfo(clusterID uint64,
	nodeID uint64) (pb.Bootstrap, error) {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].getBootstrapInfo(clusterID, nodeID)
}

// IterateEntries returns a list of saved entries starting with index low up to
// index high with a max size of maxSize.
func (s *ShardedDB) IterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].iterateEntries(ents,
		size, clusterID, nodeID, low, high, maxSize)
}

// RemoveEntriesTo removes entries associated with the specified raft node up
// to the specified index.
func (s *ShardedDB) RemoveEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	idx := s.partitioner.GetPartitionID(clusterID)
	if err := s.shards[idx].removeEntriesTo(clusterID,
		nodeID, index); err != nil {
		return err
	}
	return nil
}

// CompactEntriesTo reclaims underlying storage space used for storing
// entries up to the specified index.
func (s *ShardedDB) CompactEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) (<-chan struct{}, error) {
	done := s.addCompaction(clusterID, nodeID, index)
	return done, nil
}

// RemoveNodeData deletes all node data that belongs to the specified node.
func (s *ShardedDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	idx := s.partitioner.GetPartitionID(clusterID)
	return s.shards[idx].removeNodeData(clusterID, nodeID)
}

// ImportSnapshot imports the snapshot record and other metadata records to the
// system.
func (s *ShardedDB) ImportSnapshot(ss pb.Snapshot, nodeID uint64) error {
	idx := s.partitioner.GetPartitionID(ss.ClusterId)
	return s.shards[idx].importSnapshot(ss, nodeID)
}

// Close closes the ShardedDB instance.
func (s *ShardedDB) Close() {
	s.stopper.Stop()
	for _, v := range s.shards {
		v.close()
	}
	for _, v := range s.ctxs {
		v.Destroy()
	}
}

func (s *ShardedDB) getParititionID(updates []pb.Update) uint64 {
	pid := uint64(math.MaxUint64)
	for _, ud := range updates {
		id := s.partitioner.GetPartitionID(ud.ClusterID)
		if pid == math.MaxUint64 {
			pid = id
		} else if pid != id {
			plog.Panicf("multiple pid value found")
		}
	}
	if pid == uint64(math.MaxUint64) {
		plog.Panicf("invalid partition id")
	}
	return pid
}

func (s *ShardedDB) compactionWorkerMain() {
	for {
		select {
		case <-s.stopper.ShouldStop():
			return
		case <-s.compactionCh:
			s.compact()
		}
		select {
		case <-s.stopper.ShouldStop():
			return
		default:
		}
	}
}

func (s *ShardedDB) addCompaction(clusterID uint64,
	nodeID uint64, index uint64) chan struct{} {
	task := task{
		clusterID: clusterID,
		nodeID:    nodeID,
		index:     index,
	}
	done := s.compactions.addTask(task)
	select {
	case s.compactionCh <- struct{}{}:
	default:
	}
	return done
}

func (s *ShardedDB) compact() {
	for {
		if t, hasTask := s.compactions.getTask(); hasTask {
			idx := s.partitioner.GetPartitionID(t.clusterID)
			shard := s.shards[idx]
			if err := shard.compact(t.clusterID, t.nodeID, t.index); err != nil {
				panic(err)
			}
			atomic.AddUint64(&s.completedCompactions, 1)
			close(t.done)
			plog.Infof("%s completed LogDB compaction up to index %d",
				dn(t.clusterID, t.nodeID), t.index)
			select {
			case <-s.stopper.ShouldStop():
				return
			default:
			}
		} else {
			return
		}
	}
}
