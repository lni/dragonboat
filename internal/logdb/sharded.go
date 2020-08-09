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

var (
	// InitRDBContextValueSize defines the initial size of RDB buffer.
	InitRDBContextValueSize uint64 = 32 * 1024
	// RDBContextValueSize defines the max size of RDB buffer to be retained.
	RDBContextValueSize uint64 = 64 * 1024 * 1024
)

// ShardedDB is a LogDB implementation using sharded rocksdb instances.
type ShardedDB struct {
	completedCompactions uint64
	shards               []*db
	partitioner          server.IPartitioner
	compactionCh         chan struct{}
	compactions          *compactions
	stopper              *syncutil.Stopper
}

var _ raftio.ILogDB = (*ShardedDB)(nil)

func checkAllShards(config config.NodeHostConfig,
	dirs []string, lls []string, fs vfs.IFS, kvf kvFactory) (bool, error) {
	for i := uint64(0); i < config.Expert.LogDBShards; i++ {
		dir := fs.PathJoin(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lls) > 0 {
			lldir = fs.PathJoin(lls[i], fmt.Sprintf("logdb-%d", i))
		}
		batched, err := hasBatchedRecord(config.LogDB, dir, lldir, fs, kvf)
		if err != nil {
			return false, err
		}
		if batched {
			return true, nil
		}
	}
	return false, nil
}

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
func OpenShardedDB(config config.NodeHostConfig, callback config.LogDBCallback,
	dirs []string, lldirs []string, batched bool, check bool,
	fs vfs.IFS, kvf kvFactory) (*ShardedDB, error) {
	if config.LogDB.IsEmpty() || config.Expert.IsEmpty() {
		panic("config.LogDB.IsEmpty() || config.Expert.IsEmpty()")
	}
	shards := make([]*db, 0)
	if batched {
		plog.Infof("using batched ShardedDB")
	} else {
		plog.Infof("using plain ShardedDB")
	}
	if check && batched {
		panic("check && batched both set to true")
	}
	var err error
	if check {
		plog.Infof("checking all LogDB shards...")
		batched, err = checkAllShards(config, dirs, lldirs, fs, kvf)
		if err != nil {
			return nil, err
		}
		plog.Infof("all shards checked, batched: %t", batched)
	}
	for i := uint64(0); i < config.Expert.LogDBShards; i++ {
		dir := fs.PathJoin(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lldirs) > 0 {
			lldir = fs.PathJoin(lldirs[i], fmt.Sprintf("logdb-%d", i))
		}
		sc := shardCallback{shard: i, f: callback}
		db, err := openRDB(config.LogDB, sc.callback, dir, lldir, batched, fs, kvf)
		if err != nil {
			for _, s := range shards {
				s.close()
			}
			return nil, err
		}
		shards = append(shards, db)
	}
	partitioner := server.NewDoubleFixedPartitioner(config.Expert.ExecShards,
		config.Expert.LogDBShards)
	mw := &ShardedDB{
		shards:       shards,
		partitioner:  partitioner,
		compactions:  newCompactions(),
		compactionCh: make(chan struct{}, 1),
		stopper:      syncutil.NewStopper(),
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

// GetLogDBThreadContext return a IContext instance.
func (s *ShardedDB) GetLogDBThreadContext() raftio.IContext {
	wb := s.shards[0].getWriteBatch()
	return newContext(InitRDBContextValueSize, wb)
}

// SaveRaftState saves the raft state and logs found in the raft.Update list
// to the log db.
func (s *ShardedDB) SaveRaftState(updates []pb.Update,
	ctx raftio.IContext) error {
	if len(updates) == 0 {
		return nil
	}
	pid := s.getParititionID(updates)
	return s.shards[pid].saveRaftState(updates, ctx)
}

// ReadRaftState returns the persistent state of the specified raft node.
func (s *ShardedDB) ReadRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (*raftio.RaftState, error) {
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
	nodeID uint64) (*pb.Bootstrap, error) {
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
}

func (s *ShardedDB) getParititionID(updates []pb.Update) uint64 {
	pid := uint64(math.MaxUint64)
	for _, ud := range updates {
		id := s.partitioner.GetPartitionID(ud.ClusterID)
		if pid == math.MaxUint64 {
			pid = id
		} else {
			if pid != id {
				plog.Panicf("multiple pid value found")
			}
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
