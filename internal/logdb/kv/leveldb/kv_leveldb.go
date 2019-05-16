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

package leveldb

import (
	"bytes"

	"github.com/lni/dragonboat/internal/logdb/kv"
	"github.com/lni/dragonboat/internal/logdb/kv/leveldb/levigo"
	"github.com/lni/dragonboat/raftio"
)

type LevelDBWriteBatch struct {
	wb    *levigo.WriteBatch
	count int
}

func (w *LevelDBWriteBatch) Destroy() {
	w.wb.Close()
}

func (w *LevelDBWriteBatch) Put(key []byte, val []byte) {
	w.wb.Put(key, val)
	w.count++
}

func (w *LevelDBWriteBatch) Delete(key []byte) {
	w.wb.Delete(key)
	w.count++
}

func (w *LevelDBWriteBatch) Clear() {
	w.wb.Clear()
	w.count = 0
}

func (w *LevelDBWriteBatch) Count() int {
	return w.count
}

func NewKVStore(dir string, wal string) (kv.IKVStore, error) {
	return openLevelDB(dir, wal)
}

type LevelDBKV struct {
	db   *levigo.DB
	opts *levigo.Options
	ro   *levigo.ReadOptions
	wo   *levigo.WriteOptions
	fp   *levigo.FilterPolicy
}

func openLevelDB(dir string, wal string) (*LevelDBKV, error) {
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
	return &LevelDBKV{
		db:   db,
		ro:   ro,
		wo:   wo,
		opts: opts,
		fp:   filter,
	}, nil
}

func (r *LevelDBKV) Name() string {
	return "leveldb"
}

// Close closes the RDB object.
func (r *LevelDBKV) Close() error {
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

func iteratorIsValid(iter *levigo.Iterator) bool {
	v := iter.Valid()
	if err := iter.GetError(); err != nil {
		panic(err)
	}
	return v
}

func (r *LevelDBKV) IterateValue(fk []byte, lk []byte, inc bool,
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

func (r *LevelDBKV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(r.ro, key)
	if err != nil {
		return err
	}
	return op(val)
}

func (r *LevelDBKV) SaveValue(key []byte, value []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Put(key, value)
	return r.db.Write(r.wo, wb)
}

func (r *LevelDBKV) DeleteValue(key []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Delete(key)
	return r.db.Write(r.wo, wb)
}

func (r *LevelDBKV) GetWriteBatch(ctx raftio.IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*LevelDBWriteBatch)
		}
	}
	return &LevelDBWriteBatch{wb: levigo.NewWriteBatch()}
}

func (r *LevelDBKV) CommitWriteBatch(wb kv.IWriteBatch) error {
	lwb, ok := wb.(*LevelDBWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Write(r.wo, lwb.wb)
}

func (r *LevelDBKV) CommitDeleteBatch(wb kv.IWriteBatch) error {
	return r.CommitWriteBatch(wb)
}

func (r *LevelDBKV) RemoveEntries(fk []byte, lk []byte) error {
	wb := levigo.NewWriteBatch()
	it := r.db.NewIterator(r.ro)
	defer it.Close()
	for it.Seek(fk); it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), lk) < 0 {
			wb.Delete(it.Key())
		}
	}
	if err := it.GetError(); err != nil {
		panic(err)
	}
	return r.db.Write(r.wo, wb)
}

func (r *LevelDBKV) Compaction(fk []byte, lk []byte) error {
	r.db.CompactRange(levigo.Range{Start: fk, Limit: lk})
	return nil
}
