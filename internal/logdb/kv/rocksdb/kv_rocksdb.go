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

package rocksdb

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/rocksdb/gorocksdb"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
)

var (
	plog = logger.GetLogger("rocksdb")
)

const (
	maxLogFileSize = 1024 * 1024 * 128
)

var versionWarning sync.Once

// NewKVStore returns a RocksDB based IKVStore instance.
func NewKVStore(config config.LogDBConfig, callback kv.LogDBCallback,
	dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	if fs != vfs.DefaultFS {
		panic("only vfs.DefaultFS is supported")
	}
	checkRocksDBVersion()
	return openRocksDB(config, callback, dir, wal, fs)
}

func checkRocksDBVersion() {
	major := gorocksdb.GetRocksDBVersionMajor()
	minor := gorocksdb.GetRocksDBVersionMinor()
	if major > 6 || (major == 6 && minor >= 3) {
		return
	}
	versionWarning.Do(func() {
		plog.Warningf("RocksDB v6.3.x is required, v6.4.x is recommended")
	})
}

// KV is a RocksDB based IKVStore type.
type KV struct {
	directory string
	bbto      *gorocksdb.BlockBasedTableOptions
	cache     *gorocksdb.Cache
	db        *gorocksdb.DB
	ro        *gorocksdb.ReadOptions
	wo        *gorocksdb.WriteOptions
	opts      *gorocksdb.Options
}

var _ kv.IKVStore = (*KV)(nil)

var tolerateTailCorruptionWarning uint32
var useUniversalCompactionWarning uint32

func getRocksDBOptions(config config.LogDBConfig,
	walDirectory string) (*gorocksdb.Options,
	*gorocksdb.BlockBasedTableOptions, *gorocksdb.Cache) {
	// TODO:
	// log the settings
	blockSize := int(config.KVBlockSize)
	logDBLRUCacheSize := int(config.KVLRUCacheSize)
	maxBackgroundCompactions := int(config.KVMaxBackgroundCompactions)
	maxBackgroundFlushes := int(config.KVMaxBackgroundFlushes)
	keepLogFileNum := int(config.KVKeepLogFileNum)
	writeBufferSize := int(config.KVWriteBufferSize)
	maxWriteBufferNumber := int(config.KVMaxWriteBufferNumber)
	l0FileNumCompactionTrigger := int(config.KVLevel0FileNumCompactionTrigger)
	l0SlowdownWritesTrigger := int(config.KVLevel0SlowdownWritesTrigger)
	l0StopWritesTrigger := int(config.KVLevel0StopWritesTrigger)
	numOfLevels := int(config.KVNumOfLevels)
	maxBytesForLevelBase := config.KVMaxBytesForLevelBase
	maxBytesForLevelMultiplier := float64(config.KVMaxBytesForLevelMultiplier)
	targetFileSizeBase := config.KVTargetFileSizeBase
	targetFileSizeMultiplier := int(config.KVTargetFileSizeMultiplier)
	dynamicLevelBytes := config.KVLevelCompactionDynamicLevelBytes
	recycleLogFileNum := int(config.KVRecycleLogFileNum)
	tolerateTailCorruption := settings.Soft.KVTolerateCorruptedTailRecords
	useUniversalCompaction := settings.Soft.KVUseUniversalCompaction
	// generate the options
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetWholeKeyFiltering(true)
	bbto.SetBlockSize(blockSize)
	var cache *gorocksdb.Cache
	if inMonkeyTesting {
		bbto.SetNoBlockCache(true)
	} else {
		if logDBLRUCacheSize > 0 {
			cache = gorocksdb.NewLRUCache(logDBLRUCacheSize)
			bbto.SetBlockCache(cache)
		} else {
			bbto.SetNoBlockCache(true)
		}
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetMaxManifestFileSize(maxLogFileSize)
	opts.SetMaxLogFileSize(maxLogFileSize)
	opts.SetKeepLogFileNum(keepLogFileNum)
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	if len(walDirectory) > 0 {
		opts.SetWalDir(walDirectory)
	}
	opts.SetCompression(gorocksdb.NoCompression)
	if inMonkeyTesting {
		// rocksdb perallocates size for its log file and the size is calculated
		// based on the write buffer size.
		opts.SetWriteBufferSize(512 * 1024)
	} else {
		opts.SetWriteBufferSize(writeBufferSize)
	}
	// by default, in our deployments, we try to minimize write amplifcation -
	// L0 size = 256MBytes * 2 (min_write_buffer_number_to_merge) * \
	//              8 (level0_file_num_compaction_trigger)
	//         = 4GBytes
	// L1 size close to L0, 4GBytes, max_bytes_for_level_base = 4GBytes,
	//   max_bytes_for_level_multiplier = 2
	// L2 size is 8G, L3 is 16G, L4 is 32G, L5 64G...
	//
	// note this is the size of a shard, and the content of the rdb is expected
	// to be regularly compacted by raft. users are also free to change these
	// settings when they feel necessary.
	opts.SetLevel0FileNumCompactionTrigger(l0FileNumCompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(l0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(l0StopWritesTrigger)
	opts.SetMaxWriteBufferNumber(maxWriteBufferNumber)
	opts.SetNumLevels(numOfLevels)
	opts.SetMaxBytesForLevelBase(maxBytesForLevelBase)
	opts.SetMaxBytesForLevelMultiplier(maxBytesForLevelMultiplier)
	opts.SetTargetFileSizeBase(targetFileSizeBase)
	opts.SetTargetFileSizeMultiplier(targetFileSizeMultiplier)
	opts.SetMaxBackgroundCompactions(maxBackgroundCompactions)
	opts.SetMaxBackgroundFlushes(maxBackgroundFlushes)
	opts.SetRecycleLogFileNum(recycleLogFileNum)
	if tolerateTailCorruption {
		if atomic.CompareAndSwapUint32(&tolerateTailCorruptionWarning, 0, 1) {
			plog.Infof("RocksDB's recovery mode set to kTolerateCorruptedTailRecords")
		}
		opts.SetWALRecoveryMode(gorocksdb.TolerateCorruptedTailRecords)
	}
	if useUniversalCompaction {
		if atomic.CompareAndSwapUint32(&useUniversalCompactionWarning, 0, 1) {
			plog.Infof("use Universal Compaction in RocksDB")
		}
		opts.SetCompactionStyle(gorocksdb.UniversalCompactionStyle)
	}
	if dynamicLevelBytes != 0 {
		opts.SetLevelCompactionDynamicLevelBytes(true)
	}
	return opts, bbto, cache
}

func openRocksDB(config config.LogDBConfig, _ kv.LogDBCallback,
	dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	if config.IsEmpty() {
		panic("invalid LogDBConfig")
	}
	// gorocksdb.OpenDb allows the main db directory to be created on open
	// but WAL directory must exist before calling Open.
	if err := fileutil.MkdirAll(wal, fs); err != nil {
		panic(err)
	}
	opts, bbto, cache := getRocksDBOptions(config, wal)
	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTotalOrderSeek(true)
	return &KV{
		directory: dir,
		bbto:      bbto,
		cache:     cache,
		db:        db,
		ro:        ro,
		wo:        wo,
		opts:      opts,
	}, nil
}

// Name returns the IKVStore type name.
func (r *KV) Name() string {
	return "rocksdb"
}

// Close closes the RDB object.
func (r *KV) Close() error {
	if r.db != nil {
		r.db.Close()
	}
	if r.cache != nil {
		r.cache.Destroy()
	}
	if r.bbto != nil {
		r.bbto.Destroy()
	}
	if r.opts != nil {
		r.opts.Destroy()
	}
	if r.wo != nil {
		r.wo.Destroy()
	}
	if r.ro != nil {
		r.ro.Destroy()
	}
	r.db = nil
	return nil
}

// IterateValue iterates the key value pairs.
func (r *KV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) error {
	iter := r.db.NewIterator(r.ro)
	defer func() {
		iter.Close()
	}()
	for iter.Seek(fk); ; iter.Next() {
		v, err := iter.IsValid()
		if err != nil {
			return err
		}
		if !v {
			break
		}
		key, ok := iter.OKey()
		if !ok {
			panic("failed to get key")
		}
		keyData := key.Data()
		if inc {
			if bytes.Compare(keyData, lk) > 0 {
				key.Free()
				return nil
			}
		} else {
			if bytes.Compare(keyData, lk) >= 0 {
				key.Free()
				return nil
			}
		}
		key.Free()
		val, ok := iter.OValue()
		if !ok {
			panic("failed to get value")
		}
		valData := val.Data()
		cont, err := op(keyData, valData)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// GetValue returns the value associated with the specified key.
func (r *KV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(r.ro, key)
	if err != nil {
		return err
	}
	defer val.Free()
	return op(val.Data())
}

// SaveValue saves the specified key value pair.
func (r *KV) SaveValue(key []byte, value []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Put(key, value)
	return r.db.Write(r.wo, wb)
}

// DeleteValue deletes the specified key value pair.
func (r *KV) DeleteValue(key []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Delete(key)
	return r.db.Write(r.wo, wb)
}

// GetWriteBatch returns a write batch instance.
func (r *KV) GetWriteBatch() kv.IWriteBatch {
	return gorocksdb.NewWriteBatch()
}

// CommitWriteBatch commits the write batch.
func (r *KV) CommitWriteBatch(wb kv.IWriteBatch) error {
	rocksdbwb, ok := wb.(*gorocksdb.WriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Write(r.wo, rocksdbwb)
}

// BulkRemoveEntries returns the keys specified by the input range.
func (r *KV) BulkRemoveEntries(fk []byte, lk []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.DeleteRange(fk, lk)
	return r.db.Write(r.wo, wb)
}

// CompactEntries compacts the specified key range.
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetChangeLevel(true)
	opts.SetTargetLevel(-1)
	defer opts.Destroy()
	rng := gorocksdb.Range{
		Start: fk,
		Limit: lk,
	}
	r.db.CompactRangeWithOptions(opts, rng)
	return nil
}

// FullCompaction compacts the whole key space.
func (r *KV) FullCompaction() error {
	fk := make([]byte, kv.MaxKeyLength)
	lk := make([]byte, kv.MaxKeyLength)
	for i := uint64(0); i < kv.MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetChangeLevel(true)
	opts.SetTargetLevel(-1)
	defer opts.Destroy()
	rng := gorocksdb.Range{
		Start: fk,
		Limit: lk,
	}
	r.db.CompactRangeWithOptions(opts, rng)
	return nil
}
