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

package pebble

import (
	"github.com/cockroachdb/pebble"
	pvfs "github.com/cockroachdb/pebble/vfs"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/vfs"
)

// readOnlyKV is a read-only pebble based IKVStore type.
type readOnlyKV struct {
	*KV
}

var _ kv.IKVStore = (*readOnlyKV)(nil)

// NewReadOnlyKVStore opens an existing Pebble DB in read-only mode.
// fs is a PebbleDB virtual file system. It can be obtained by calling
// vfs.NewPebbleFS() with a Dragonboat virtual file system or NewNoLockFS().
func NewReadOnlyKVStore(config config.LogDBConfig,
	dir string, walDir string, fs pvfs.FS) (kv.IKVStore, error) {

	if config.IsEmpty() {
		panic("invalid LogDBConfig")
	}
	blockSize := int(config.KVBlockSize)
	writeBufferSize := int(config.KVWriteBufferSize)
	maxWriteBufferNumber := int(config.KVMaxWriteBufferNumber)
	l0FileNumCompactionTrigger := int(config.KVLevel0FileNumCompactionTrigger)
	l0StopWritesTrigger := int(config.KVLevel0StopWritesTrigger)
	maxBytesForLevelBase := int64(config.KVMaxBytesForLevelBase)
	targetFileSizeBase := int64(config.KVTargetFileSizeBase)
	cacheSize := int64(config.KVLRUCacheSize)
	levelSizeMultiplier := int64(config.KVTargetFileSizeMultiplier)
	numOfLevels := int64(config.KVNumOfLevels)
	lopts := make([]pebble.LevelOptions, 0)
	sz := targetFileSizeBase
	for l := int64(0); l < numOfLevels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: sz,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}
	if inMonkeyTesting {
		writeBufferSize = 1024 * 1024 * 4
	}
	cache := pebble.NewCache(cacheSize)
	ro := &pebble.IterOptions{}
	opts := &pebble.Options{
		Levels:                      lopts,
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		LBaseMaxBytes:               maxBytesForLevelBase,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		Cache:                       cache,
		FS:                          fs,
		ReadOnly:                    true,
		Logger:                      PebbleLogger,
	}
	kv := &KV{
		ro:     ro,
		opts:   opts,
		config: config,
		dbSet:  make(chan struct{}),
	}
	if len(walDir) > 0 {
		opts.WALDir = walDir
	}

	pdb, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	cache.Unref()
	kv.db = pdb

	return &readOnlyKV{kv}, nil
}

// Name returns the IKVStore type name.
func (r *readOnlyKV) Name() string {
	return "pebble-readonly"
}

// Close closes the read-only KV store.
func (r *readOnlyKV) Close() error {
	return r.db.Close()
}

// SaveValue is not supported in read-only mode.
func (r *readOnlyKV) SaveValue(key []byte, value []byte) error {
	panic("read-only store: SaveValue not supported")
}

// DeleteValue is not supported in read-only mode.
func (r *readOnlyKV) DeleteValue(key []byte) error {
	panic("read-only store: DeleteValue not supported")
}

// GetWriteBatch is not supported in read-only mode.
func (r *readOnlyKV) GetWriteBatch() kv.IWriteBatch {
	panic("read-only store: GetWriteBatch not supported")
}

// CommitWriteBatch is not supported in read-only mode.
func (r *readOnlyKV) CommitWriteBatch(wb kv.IWriteBatch) error {
	panic("read-only store: CommitWriteBatch not supported")
}

// BulkRemoveEntries is not supported in read-only mode.
func (r *readOnlyKV) BulkRemoveEntries(fk []byte, lk []byte) error {
	panic("read-only store: BulkRemoveEntries not supported")
}

// CompactEntries is a no-op in read-only mode.
func (r *readOnlyKV) CompactEntries(fk []byte, lk []byte) error {
	return nil
}

// FullCompaction is a no-op in read-only mode.
func (r *readOnlyKV) FullCompaction() error {
	return nil
}

// PebbleReadOnlyKVStore creates a read-only pebble-based IKVStore instance.
// Implementation of logdb.kvFactory.
func PebbleReadOnlyKVStore(config config.LogDBConfig,
	callback kv.LogDBCallback,
	dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	if fs != vfs.DefaultFS {
		_, isErrorFS := fs.(*vfs.ErrorFS)
		_, isMemFS := fs.(*vfs.MemFS)
		if !isErrorFS && !isMemFS {
			panic("invalid fs")
		}
	}
	return NewReadOnlyKVStore(config, dir, wal, vfs.NewPebbleFS(fs))
}
