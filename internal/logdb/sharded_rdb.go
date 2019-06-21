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
	"path/filepath"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	numOfStepEngineWorker = settings.Hard.StepEngineWorkerCount
	numOfRocksDBInstance  = settings.Hard.LogDBPoolSize
	// RDBContextValueSize defines the size of byte array managed in RDB context.
	RDBContextValueSize uint64 = 1024 * 1024 * 64
)

// ShardedRDB is a LogDB implementation using sharded rocksdb instances.
type ShardedRDB struct {
	completedCompactions uint64
	shards               []*rdb
	partitioner          server.IPartitioner
	compactionCh         chan struct{}
	compactions          *compactions
	stopper              *syncutil.Stopper
}

func checkAllShards(dirs []string, lls []string, kvf kvFactory) (bool, error) {
	for i := uint64(0); i < numOfRocksDBInstance; i++ {
		dir := filepath.Join(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lls) > 0 {
			lldir = filepath.Join(lls[i], fmt.Sprintf("logdb-%d", i))
		}
		batched, err := hasBatchedRecord(dir, lldir, kvf)
		if err != nil {
			return false, err
		}
		if batched {
			return true, nil
		}
	}
	return false, nil
}

// OpenShardedRDB creates a ShardedRDB instance.
func OpenShardedRDB(dirs []string, lldirs []string,
	batched bool, check bool, kvf kvFactory) (*ShardedRDB, error) {
	shards := make([]*rdb, 0)
	if batched {
		plog.Infof("using batched ShardedRDB")
	} else {
		plog.Infof("using plain ShardedRDB")
	}
	if check && batched {
		panic("check && batched both set to true")
	}
	var err error
	if check {
		plog.Infof("checking all LogDB shards...")
		batched, err = checkAllShards(dirs, lldirs, kvf)
		if err != nil {
			return nil, err
		}
		plog.Infof("all shards checked, batched: %t", batched)
	}
	for i := uint64(0); i < numOfRocksDBInstance; i++ {
		dir := filepath.Join(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lldirs) > 0 {
			lldir = filepath.Join(lldirs[i], fmt.Sprintf("logdb-%d", i))
		}
		db, err := openRDB(dir, lldir, batched, kvf)
		if err != nil {
			for _, s := range shards {
				s.close()
			}
			return nil, err
		}
		shards = append(shards, db)
	}
	partitioner := server.NewDoubleFixedPartitioner(numOfRocksDBInstance,
		numOfStepEngineWorker)
	mw := &ShardedRDB{
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
func (mw *ShardedRDB) Name() string {
	return fmt.Sprintf("sharded-%s", mw.shards[0].name())
}

// BinaryFormat is the binary format supported by the sharded DB.
func (mw *ShardedRDB) BinaryFormat() uint32 {
	return mw.shards[0].binaryFormat()
}

// SelfCheckFailed runs a self check on all db shards and report whether any
// failure is observed.
func (mw *ShardedRDB) SelfCheckFailed() (bool, error) {
	for _, shard := range mw.shards {
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
func (mw *ShardedRDB) GetLogDBThreadContext() raftio.IContext {
	wb := mw.shards[0].getWriteBatch()
	return newRDBContext(RDBContextValueSize, wb)
}

// SaveRaftState saves the raft state and logs found in the raft.Update list
// to the log db.
func (mw *ShardedRDB) SaveRaftState(updates []pb.Update,
	ctx raftio.IContext) error {
	if len(updates) == 0 {
		return nil
	}
	pid := mw.getParititionID(updates)
	return mw.shards[pid].saveRaftState(updates, ctx)
}

// ReadRaftState returns the persistent state of the specified raft node.
func (mw *ShardedRDB) ReadRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (*raftio.RaftState, error) {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].readRaftState(clusterID, nodeID, lastIndex)
}

// ListNodeInfo lists all available NodeInfo found in the log db.
func (mw *ShardedRDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	r := make([]raftio.NodeInfo, 0)
	for _, v := range mw.shards {
		n, err := v.listNodeInfo()
		if err != nil {
			return nil, err
		}
		r = append(r, n...)
	}
	return r, nil
}

// SaveSnapshots saves all snapshot metadata found in the raft.Update list.
func (mw *ShardedRDB) SaveSnapshots(updates []pb.Update) error {
	if len(updates) == 0 {
		return nil
	}
	pid := mw.getParititionID(updates)
	return mw.shards[pid].saveSnapshots(updates)
}

// DeleteSnapshot removes the specified snapshot metadata from the log db.
func (mw *ShardedRDB) DeleteSnapshot(clusterID uint64,
	nodeID uint64, snapshotIndex uint64) error {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].deleteSnapshot(clusterID, nodeID, snapshotIndex)
}

// ListSnapshots lists all available snapshots associated with the specified
// raft node.
func (mw *ShardedRDB) ListSnapshots(clusterID uint64,
	nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].listSnapshots(clusterID, nodeID, index)
}

// SaveBootstrapInfo saves the specified bootstrap info for the given node.
func (mw *ShardedRDB) SaveBootstrapInfo(clusterID uint64,
	nodeID uint64, bootstrap pb.Bootstrap) error {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].saveBootstrapInfo(clusterID, nodeID, bootstrap)
}

// GetBootstrapInfo returns the saved bootstrap info for the given node.
func (mw *ShardedRDB) GetBootstrapInfo(clusterID uint64,
	nodeID uint64) (*pb.Bootstrap, error) {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].getBootstrapInfo(clusterID, nodeID)
}

// IterateEntries returns a list of saved entries starting with index low up to
// index high with a max size of maxSize.
func (mw *ShardedRDB) IterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64,
	maxSize uint64) ([]pb.Entry, uint64, error) {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].iterateEntries(ents,
		size, clusterID, nodeID, low, high, maxSize)
}

// RemoveEntriesTo removes entries associated with the specified raft node up
// to the specified index.
func (mw *ShardedRDB) RemoveEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	idx := mw.partitioner.GetPartitionID(clusterID)
	if err := mw.shards[idx].removeEntriesTo(clusterID,
		nodeID, index); err != nil {
		return err
	}
	mw.addCompaction(clusterID, nodeID, index)
	return nil
}

// RemoveNodeData deletes all node data that belongs to the specified node.
func (mw *ShardedRDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	idx := mw.partitioner.GetPartitionID(clusterID)
	return mw.shards[idx].removeNodeData(clusterID, nodeID)
}

// ImportSnapshot imports the snapshot record and other metadata records to the
// system.
func (mw *ShardedRDB) ImportSnapshot(ss pb.Snapshot, nodeID uint64) error {
	idx := mw.partitioner.GetPartitionID(ss.ClusterId)
	return mw.shards[idx].importSnapshot(ss, nodeID)
}

// Close closes the ShardedRDB instance.
func (mw *ShardedRDB) Close() {
	mw.stopper.Stop()
	for _, v := range mw.shards {
		v.close()
	}
}

func (mw *ShardedRDB) getParititionID(updates []pb.Update) uint64 {
	pid := uint64(math.MaxUint64)
	for _, ud := range updates {
		id := mw.partitioner.GetPartitionID(ud.ClusterID)
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

func (mw *ShardedRDB) compactionWorkerMain() {
	for {
		select {
		case <-mw.stopper.ShouldStop():
			return
		case <-mw.compactionCh:
			mw.compaction()
		}
		select {
		case <-mw.stopper.ShouldStop():
			return
		default:
		}
	}
}

func (mw *ShardedRDB) addCompaction(clusterID uint64,
	nodeID uint64, index uint64) {
	task := task{
		clusterID: clusterID,
		nodeID:    nodeID,
		index:     index,
	}
	mw.compactions.addTask(task)
	select {
	case mw.compactionCh <- struct{}{}:
	default:
	}
}

func (mw *ShardedRDB) compaction() {
	for {
		t, hasTask := mw.compactions.getTask()
		if !hasTask {
			return
		}
		idx := mw.partitioner.GetPartitionID(t.clusterID)
		shard := mw.shards[idx]
		if err := shard.compaction(t.clusterID, t.nodeID, t.index); err != nil {
			panic(err)
		}
		atomic.AddUint64(&mw.completedCompactions, 1)
		select {
		case <-mw.stopper.ShouldStop():
			return
		default:
		}
	}
}
