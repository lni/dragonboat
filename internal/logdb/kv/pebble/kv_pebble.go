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

package pebble

// WARNING: pebble support is expermental, DO NOT USE IT IN PRODUCTION.

import (
	"bytes"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	plog = logger.GetLogger("pebblekv")
)

const (
	maxLogFileSize = 1024 * 1024 * 128
	blockSize      = 1024 * 32
)

var (
	writeBufferSize            = int(settings.Soft.KVWriteBufferSize)
	maxWriteBufferNumber       = int(settings.Soft.KVMaxWriteBufferNumber)
	l0FileNumCompactionTrigger = int(settings.Soft.KVLevel0FileNumCompactionTrigger)
	l0SlowdownWritesTrigger    = int(settings.Soft.KVLevel0SlowdownWritesTrigger)
	l0StopWritesTrigger        = int(settings.Soft.KVLevel0StopWritesTrigger)
	maxBytesForLevelBase       = int64(settings.Soft.KVMaxBytesForLevelBase)
	targetFileSizeBase         = int64(settings.Soft.KVTargetFileSizeBase)
)

type pebbleWriteBatch struct {
	wb    *pebble.Batch
	db    *pebble.DB
	wo    *pebble.WriteOptions
	count int
}

func (w *pebbleWriteBatch) Destroy() {
	w.wb.Close()
}

func (w *pebbleWriteBatch) Put(key []byte, val []byte) {
	if err := w.wb.Set(key, val, w.wo); err != nil {
		panic(err)
	}
	w.count++
}

func (w *pebbleWriteBatch) Delete(key []byte) {
	if err := w.wb.Delete(key, w.wo); err != nil {
		panic(err)
	}
	w.count++
}

func (w *pebbleWriteBatch) Clear() {
	// TODO: reuse the write batch
	w.wb = w.db.NewBatch()
	w.count = 0
}

func (w *pebbleWriteBatch) Count() int {
	return w.count
}

// NewKVStore returns a pebble based IKVStore instance.
func NewKVStore(dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	return openPebbleDB(dir, wal, fs)
}

var _ kv.IKVStore = &KV{}

// KV is a pebble based IKVStore type.
type KV struct {
	db   *pebble.DB
	opts *pebble.Options
	ro   *pebble.IterOptions
	wo   *pebble.WriteOptions
}

var pebbleWarning sync.Once

func openPebbleDB(dir string, walDir string, fs vfs.IFS) (kv.IKVStore, error) {
	pebbleWarning.Do(func() {
		plog.Warningf("pebble support is experimental, DO NOT USE IN PRODUCTION")
		if fs == vfs.MemStrictFS {
			plog.Warningf("running in pebble memfs test mode")
		}
	})
	lopts := make([]pebble.LevelOptions, 0)
	sz := targetFileSizeBase
	for l := 0; l < 7; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: sz,
		}
		sz = sz * 2
		lopts = append(lopts, opt)
	}
	if inMonkeyTesting {
		writeBufferSize = 1024 * 1024 * 4
	}
	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		Levels:                      lopts,
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		LBaseMaxBytes:               maxBytesForLevelBase,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		Cache:                       cache,
		FS:                          vfs.NewPebbleFS(fs),
	}
	if len(walDir) > 0 {
		if err := fileutil.MkdirAll(walDir, fs); err != nil {
			return nil, err
		}
		opts.WALDir = walDir
	}
	if err := fileutil.MkdirAll(dir, fs); err != nil {
		return nil, err
	}
	pdb, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	cache.Unref()
	ro := &pebble.IterOptions{}
	wo := &pebble.WriteOptions{Sync: true}
	return &KV{
		db:   pdb,
		ro:   ro,
		wo:   wo,
		opts: opts,
	}, nil
}

// Name returns the IKVStore type name.
func (r *KV) Name() string {
	return "pebble"
}

// Close closes the RDB object.
func (r *KV) Close() error {
	if r.db != nil {
		r.db.Close()
	}
	r.db = nil
	return nil
}

func iteratorIsValid(iter *pebble.Iterator) bool {
	v := iter.Valid()
	if err := iter.Error(); err != nil {
		panic(err)
	}
	return v
}

// IterateValue ...
func (r *KV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) error {
	iter := r.db.NewIter(r.ro)
	defer iter.Close()
	for iter.SeekGE(fk); iteratorIsValid(iter); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if inc {
			if bytes.Compare(key, lk) > 0 {
				return nil
			}
		} else {
			if bytes.Compare(key, lk) >= 0 {
				return nil
			}
		}
		cont, err := op(key, val)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// GetValue ...
func (r *KV) GetValue(key []byte, op func([]byte) error) (err error) {
	val, closer, err := r.db.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	defer func() {
		if closer != nil {
			if cerr := closer.Close(); err == nil {
				err = cerr
			}
		}
	}()
	return op(val)
}

// SaveValue ...
func (r *KV) SaveValue(key []byte, value []byte) error {
	return r.db.Set(key, value, r.wo)
}

// DeleteValue ...
func (r *KV) DeleteValue(key []byte) error {
	return r.db.Delete(key, r.wo)
}

// GetWriteBatch ...
func (r *KV) GetWriteBatch(ctx raftio.IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			pwb := wb.(*pebbleWriteBatch)
			if pwb.db == r.db {
				return pwb
			}
		}
	}
	return &pebbleWriteBatch{wb: r.db.NewBatch(), db: r.db, wo: r.wo}
}

// CommitWriteBatch ...
func (r *KV) CommitWriteBatch(wb kv.IWriteBatch) error {
	pwb, ok := wb.(*pebbleWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Apply(pwb.wb, r.wo)
}

// BulkRemoveEntries ...
func (r *KV) BulkRemoveEntries(fk []byte, lk []byte) error {
	wb := r.db.NewBatch()
	defer wb.Close()
	if err := wb.DeleteRange(fk, lk, r.wo); err != nil {
		return err
	}
	return r.db.Apply(wb, r.wo)
}

// CompactEntries ...
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	return r.db.Compact(fk, lk)
}

// FullCompaction ...
func (r *KV) FullCompaction() error {
	fk := make([]byte, kv.MaxKeyLength)
	lk := make([]byte, kv.MaxKeyLength)
	for i := uint64(0); i < kv.MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	return r.db.Compact(fk, lk)
}
