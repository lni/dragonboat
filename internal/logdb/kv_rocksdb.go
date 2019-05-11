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

// +build !dragonboat_leveldb
// +build !dragonboat_pebble
// +build !dragonboat_custom_logdb

package logdb

import (
	"bytes"

	"github.com/lni/dragonboat/internal/logdb/gorocksdb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/raftio"
)

const (
	// LogDBType is the logdb type name
	LogDBType = "sharded-rocksdb"
)

var (
	logDBLRUCacheSize        = int(settings.Soft.RDBLRUCacheSize)
	maxBackgroundCompactions = int(settings.Soft.RDBMaxBackgroundCompactions)
	maxBackgroundFlushes     = int(settings.Soft.RDBMaxBackgroundFlushes)
)

func newKVStore(dir string, wal string) (IKvStore, error) {
	return openRocksDB(dir, wal)
}

type rocksdbKV struct {
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

func openRocksDB(dir string, wal string) (*rocksdbKV, error) {
	// gorocksdb.OpenDb allows the main db directory to be created on open
	// but WAL directory must exist before calling Open.
	if len(wal) > 0 && !fileutil.Exist(wal) {
		if err := fileutil.MkdirAll(wal); err != nil {
			plog.Panicf("cannot create dir for RDB WAL (%v)", err)
		}
	}
	if !fileutil.Exist(dir) {
		if err := fileutil.MkdirAll(dir); err != nil {
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
	return &rocksdbKV{
		directory: dir,
		bbto:      bbto,
		cache:     cache,
		db:        db,
		ro:        ro,
		wo:        wo,
		opts:      opts,
	}, nil
}

func (r *rocksdbKV) Name() string {
	return "rocksdb"
}

// Close closes the RDB object.
func (r *rocksdbKV) Close() error {
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

func (r *rocksdbKV) IterateValue(fk []byte, lk []byte, inc bool,
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

func (r *rocksdbKV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(r.ro, key)
	if err != nil {
		return err
	}
	defer val.Free()
	return op(val.Data())
}

func (r *rocksdbKV) SaveValue(key []byte, value []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Put(key, value)
	return r.db.Write(r.wo, wb)
}

func (r *rocksdbKV) DeleteValue(key []byte) error {
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	wb.Delete(key)
	return r.db.Write(r.wo, wb)
}

func (r *rocksdbKV) GetWriteBatch(ctx raftio.IContext) IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*gorocksdb.WriteBatch)
		}
	}
	return gorocksdb.NewWriteBatch()
}

func (r *rocksdbKV) CommitWriteBatch(wb IWriteBatch) error {
	rocksdbwb, ok := wb.(*gorocksdb.WriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Write(r.wo, rocksdbwb)
}

func (r *rocksdbKV) CommitDeleteBatch(wb IWriteBatch) error {
	return r.CommitWriteBatch(wb)
}

func (r *rocksdbKV) RemoveEntries(firstKey []byte, lastKey []byte) error {
	if err := r.db.DeleteFileInRange(firstKey, lastKey); err != nil {
		return err
	}
	return r.deleteRange(firstKey, lastKey)
}

func (r *rocksdbKV) Compaction(firstKey []byte, lastKey []byte) error {
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetChangeLevel(true)
	opts.SetTargetLevel(-1)
	defer opts.Destroy()
	rng := gorocksdb.Range{
		Start: firstKey,
		Limit: lastKey,
	}
	r.db.CompactRangeWithOptions(opts, rng)
	return nil
}

func (r *rocksdbKV) deleteRange(firstKey []byte, lastKey []byte) error {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	toDelete := make([][]byte, 0)
	for iter.Seek(firstKey); ; iter.Next() {
		v, err := iter.IsValid()
		if err != nil {
			return err
		}
		if !v {
			break
		}
		if done := func() bool {
			key, ok := iter.OKey()
			if !ok {
				panic("failed to get key")
			}
			defer key.Free()
			kd := key.Data()
			if bytes.Compare(kd, lastKey) >= 0 {
				return true
			}
			v := make([]byte, len(kd))
			copy(v, kd)
			toDelete = append(toDelete, v)
			return false
		}(); done {
			break
		}
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	for _, key := range toDelete {
		wb.Delete(key)
	}
	if wb.Count() > 0 {
		return r.db.Write(r.wo, wb)
	}
	return nil
}
