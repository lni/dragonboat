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

package leveldb

import (
	"bytes"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/leveldb/levigo"
	"github.com/lni/dragonboat/v3/raftio"
)

type levelDBWriteBatch struct {
	wb    *levigo.WriteBatch
	count int
}

func (w *levelDBWriteBatch) Destroy() {
	w.wb.Close()
}

func (w *levelDBWriteBatch) Put(key []byte, val []byte) {
	w.wb.Put(key, val)
	w.count++
}

func (w *levelDBWriteBatch) Delete(key []byte) {
	w.wb.Delete(key)
	w.count++
}

func (w *levelDBWriteBatch) Clear() {
	w.wb.Clear()
	w.count = 0
}

func (w *levelDBWriteBatch) Count() int {
	return w.count
}

// NewKVStore returns a new leveldb based IKVStore instance.
func NewKVStore(dir string, wal string) (kv.IKVStore, error) {
	return openLevelDB(dir, wal)
}

// KV is a leveldb based IKVStore type.
type KV struct {
	db   *levigo.DB
	opts *levigo.Options
	ro   *levigo.ReadOptions
	wo   *levigo.WriteOptions
	fp   *levigo.FilterPolicy
}

func openLevelDB(dir string, wal string) (*KV, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	filter := levigo.NewBloomFilter(10)
	opts.SetFilterPolicy(filter)
	db, err := levigo.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	ro.SetFillCache(false)
	ro.SetVerifyChecksums(true)
	wo.SetSync(true)
	return &KV{
		db:   db,
		ro:   ro,
		wo:   wo,
		opts: opts,
		fp:   filter,
	}, nil
}

// Name returns the IKVStore type name.
func (r *KV) Name() string {
	return "leveldb"
}

// Close closes the RDB object.
func (r *KV) Close() error {
	if r.db != nil {
		r.db.Close()
	}
	if r.wo != nil {
		r.wo.Close()
	}
	if r.ro != nil {
		r.ro.Close()
	}
	if r.opts != nil {
		r.opts.Close()
	}
	if r.fp != nil {
		r.fp.Close()
	}
	r.db = nil
	return nil
}

// IterateValue ...
func (r *KV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) error {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	for iter.Seek(fk); ; iter.Next() {
		if err := iter.GetError(); err != nil {
			return err
		}
		if !iter.Valid() {
			break
		}
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
func (r *KV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(r.ro, key)
	if err != nil {
		return err
	}
	return op(val)
}

// SaveValue ...
func (r *KV) SaveValue(key []byte, value []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Put(key, value)
	return r.db.Write(r.wo, wb)
}

// DeleteValue ...
func (r *KV) DeleteValue(key []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Delete(key)
	return r.db.Write(r.wo, wb)
}

// GetWriteBatch ...
func (r *KV) GetWriteBatch(ctx raftio.IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*levelDBWriteBatch)
		}
	}
	return &levelDBWriteBatch{wb: levigo.NewWriteBatch()}
}

// CommitWriteBatch ...
func (r *KV) CommitWriteBatch(wb kv.IWriteBatch) error {
	lwb, ok := wb.(*levelDBWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Write(r.wo, lwb.wb)
}

// BulkRemoveEntries ...
func (r *KV) BulkRemoveEntries(fk []byte, lk []byte) error {
	return nil
}

func (r *KV) deleteRange(fk []byte, lk []byte) error {
	wb := r.GetWriteBatch(nil)
	it := r.db.NewIterator(r.ro)
	defer it.Close()
	for it.Seek(fk); it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), lk) < 0 {
			wb.Delete(it.Key())
		}
	}
	if err := it.GetError(); err != nil {
		return err
	}
	return r.CommitWriteBatch(wb)
}

// CompactEntries ...
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	if err := r.deleteRange(fk, lk); err != nil {
		return err
	}
	r.db.CompactRange(levigo.Range{Start: fk, Limit: lk})
	return nil
}

// FullCompaction ...
func (r *KV) FullCompaction() error {
	fk := make([]byte, kv.MaxKeyLength)
	lk := make([]byte, kv.MaxKeyLength)
	for i := uint64(0); i < kv.MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	r.db.CompactRange(levigo.Range{Start: fk, Limit: lk})
	return nil
}
