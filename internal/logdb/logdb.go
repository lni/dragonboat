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

/*
Package logdb implements the persistent log storage used by Dragonboat.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package logdb

import (
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("logdb")
)

// IReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
type IReusableKey interface {
	SetEntryBatchKey(shardID uint64, replicaID uint64, index uint64)
	// SetEntryKey sets the key to be an entry key for the specified Raft node
	// with the specified entry index.
	SetEntryKey(shardID uint64, replicaID uint64, index uint64)
	// SetStateKey sets the key to be an persistent state key suitable
	// for the specified Raft shard node.
	SetStateKey(shardID uint64, replicaID uint64)
	// SetMaxIndexKey sets the key to be the max possible index key for the
	// specified Raft shard node.
	SetMaxIndexKey(shardID uint64, replicaID uint64)
	// Key returns the underlying byte slice of the key.
	Key() []byte
	// Release releases the key instance so it can be reused in the future.
	Release()
}

// IContext is the per thread context used in the logdb module.
// IContext is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing ILogDB.
type IContext interface {
	// Destroy destroys the IContext instance.
	Destroy()
	// Reset resets the IContext instance, all previous returned keys and
	// buffers will be put back to the IContext instance and be ready to
	// be used for the next iteration.
	Reset()
	// GetKey returns a reusable key.
	GetKey() IReusableKey
	// GetValueBuffer returns a byte buffer with at least sz bytes in length.
	GetValueBuffer(sz uint64) []byte
	// GetWriteBatch returns a write batch or transaction instance.
	GetWriteBatch() interface{}
	// SetWriteBatch adds the write batch to the IContext instance.
	SetWriteBatch(wb interface{})
	// GetEntryBatch returns an entry batch instance.
	GetEntryBatch() pb.EntryBatch
	// GetLastEntryBatch returns an entry batch instance.
	GetLastEntryBatch() pb.EntryBatch
}

// DefaultFactory is the default factory for creating LogDB instance.
type DefaultFactory struct {
}

// NewDefaultFactory creates a new DefaultFactory instance.
func NewDefaultFactory() *DefaultFactory {
	return &DefaultFactory{}
}

// Create creates the LogDB instance.
func (f *DefaultFactory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewDefaultLogDB(cfg, cb, dirs, lldirs)
}

// Name returns the name of the default LogDB instance.
func (f *DefaultFactory) Name() string {
	return "sharded-pebble"
}

// NewDefaultLogDB creates a Log DB instance using the default KV store
// implementation. The created Log DB tries to store entry records in
// plain format but it switches to the batched mode if there is already
// batched entries saved in the existing DB.
func NewDefaultLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, false, true, newDefaultKVStore)
}

// NewDefaultBatchedLogDB creates a Log DB instance using the default KV store
// implementation with batched entry support.
func NewDefaultBatchedLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, true, false, newDefaultKVStore)
}

// NewLogDB creates a Log DB instance based on provided configuration
// parameters. The underlying KV store used by the Log DB instance is created
// by the provided factory function.
func NewLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback, dirs []string, lldirs []string,
	batched bool, check bool, f kv.Factory) (raftio.ILogDB, error) {
	checkDirs(config.Expert.LogDB.Shards, dirs, lldirs)
	llDirRequired := len(lldirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < config.Expert.LogDB.Shards; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lldirs = append(lldirs, lldirs[0])
			}
		}
	}
	return OpenShardedDB(config, callback, dirs, lldirs, batched, check, f)
}

func checkDirs(numOfShards uint64, dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfShards {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfShards)
		}
		if len(lldirs) > 0 {
			if len(dirs) != len(lldirs) {
				plog.Panicf("%v regular dirs, but %v low latency dirs", dirs, lldirs)
			}
		}
	} else {
		panic("no regular dir")
	}
}
