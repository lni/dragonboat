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

package rocksdb

import (
	"bytes"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/rocksdb/gorocksdb"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	plog = logger.GetLogger("rocksdb")
)

var (
	logDBLRUCacheSize        = int(settings.Soft.RDBLRUCacheSize)
	maxBackgroundCompactions = int(settings.Soft.RDBMaxBackgroundCompactions)
	maxBackgroundFlushes     = int(settings.Soft.RDBMaxBackgroundFlushes)
	keepLogFileNum           = int(settings.Soft.RDBKeepLogFileNum)
)

// NewKVStore returns a RocksDB based IKVStore instance.
func NewKVStore(dir string, wal string) (kv.IKVStore, error) {
	return openRocksDB(dir, wal)
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

// FIXME:
// move these option parameters to the settings package to make it configurable
func getRocksDBOptions(directory string,
	walDirectory string) (*gorocksdb.Options,
	*gorocksdb.BlockBasedTableOptions, *gorocksdb.Cache) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetWholeKeyFiltering(true)
	bbto.SetBlockSize(32 * 1024)
	var cache *gorocksdb.Cache
	if inMonkeyTesting {
		cache = gorocksdb.NewLRUCache(1024 * 512)
		bbto.SetBlockCache(cache)
	} else {
		if logDBLRUCacheSize > 0 {
			cache = gorocksdb.NewLRUCache(logDBLRUCacheSize)
			bbto.SetBlockCache(cache)
		} else {
			bbto.SetNoBlockCache(true)
		}
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetMaxManifestFileSize(1024 * 1024 * 128)
	opts.SetMaxLogFileSize(1024 * 1024 * 128)
	opts.SetKeepLogFileNum(keepLogFileNum)
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	if len(walDirectory) > 0 {
		opts.SetWalDir(walDirectory)
	}
	ddio := directIOSupported(directory)
	wdio := directIOSupported(walDirectory)
	if ddio && wdio {
		opts.SetUseDirectIOForFlushAndCompaction(true)
	}
	opts.SetCompression(gorocksdb.NoCompression)
	if inMonkeyTesting {
		// rocksdb perallocates size for its log file and the size is calculated
		// based on the write buffer size.
		opts.SetWriteBufferSize(512 * 1024)
	} else {
		opts.SetWriteBufferSize(256 * 1024 * 1024)
	}
	// in normal mode, by default, we try to minimize write amplifcation, we have
	// L0 size = 256MBytes * 2 (min_write_buffer_number_to_merge) * \
	//              8 (level0_file_num_compaction_trigger)
	//         = 4GBytes
	// L1 size close to L0, 4GBytes, max_bytes_for_level_base = 4GBytes,
	//   max_bytes_for_level_multiplier = 2
	// L2 size is 8G, L3 is 16G, L4 is 32G, L5 64G...
	//
	// note this is the size of a shard, and the content of the rdb is expected
	// to be compacted by raft.
	//
	opts.SetLevel0FileNumCompactionTrigger(8)
	opts.SetLevel0SlowdownWritesTrigger(17)
	opts.SetLevel0StopWritesTrigger(24)
	opts.SetMaxWriteBufferNumber(25)
	opts.SetNumLevels(7)
	// MaxBytesForLevelBase is the total size of L1, should be close to the size
	// of L0
	opts.SetMaxBytesForLevelBase(4 * 1024 * 1024 * 1024)
	opts.SetMaxBytesForLevelMultiplier(2)
	// files in L1 will have TargetFileSizeBase bytes
	opts.SetTargetFileSizeBase(256 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(1)
	// IO parallism
	opts.SetMaxBackgroundCompactions(maxBackgroundCompactions)
	opts.SetMaxBackgroundFlushes(maxBackgroundFlushes)
	return opts, bbto, cache
}

func openRocksDB(dir string, wal string) (*KV, error) {
	// gorocksdb.OpenDb allows the main db directory to be created on open
	// but WAL directory must exist before calling Open.
	walExist, err := fileutil.Exist(wal)
	if err != nil {
		return nil, err
	}
	if len(wal) > 0 && !walExist {
		if err := fileutil.Mkdir(wal); err != nil {
			plog.Panicf("cannot create dir for RDB WAL (%v)", err)
		}
	}
	dirExist, err := fileutil.Exist(dir)
	if err != nil {
		return nil, err
	}
	if !dirExist {
		if err := fileutil.Mkdir(dir); err != nil {
			plog.Panicf("cannot create dir (%v)", err)
		}
	}
	opts, bbto, cache := getRocksDBOptions(dir, wal)
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
func (r *KV) GetWriteBatch(ctx raftio.IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*gorocksdb.WriteBatch)
		}
	}
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
	return nil
}

// CompactEntries compacts the specified key range.
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	if err := r.deleteRange(fk, lk); err != nil {
		return err
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

func (r *KV) deleteRange(fk []byte, lk []byte) error {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
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
		kd := key.Data()
		if bytes.Compare(kd, lk) < 0 {
			wb.Delete(kd)
			key.Free()
		} else {
			key.Free()
			break
		}
	}
	if wb.Count() > 0 {
		return r.db.Write(r.wo, wb)
	}
	return nil
}
