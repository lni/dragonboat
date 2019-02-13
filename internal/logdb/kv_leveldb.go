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

// +build dragonboat_leveldb

package logdb

import (
	"bytes"

	"github.com/lni/dragonboat/internal/logdb/levigo"
	"github.com/lni/dragonboat/raftio"
)

const (
	// LogDBType is the logdb type name
	LogDBType = "sharded-leveldb"
)

type leveldbWriteBatch struct {
	wb    *levigo.WriteBatch
	count int
}

func (w *leveldbWriteBatch) Destroy() {
	w.wb.Close()
}

func (w *leveldbWriteBatch) Put(key []byte, val []byte) {
	w.wb.Put(key, val)
	w.count++
}

func (w *leveldbWriteBatch) Clear() {
	w.count = 0
}

func (w *leveldbWriteBatch) Count() int {
	return w.count
}

func newKVStore(dir string, wal string) (IKvStore, error) {
	return openLevelDB(dir, wal)
}

type leveldbKV struct {
	db   *levigo.DB
	opts *levigo.Options
	ro   *levigo.ReadOptions
	wo   *levigo.WriteOptions
}

func openLevelDB(dir string, wal string) (*leveldbKV, error) {
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
	return &leveldbKV{
		db:   db,
		ro:   ro,
		wo:   wo,
		opts: opts,
	}, nil
}

func (r *leveldbKV) Name() string {
	return "leveldb"
}

// Close closes the RDB object.
func (r *leveldbKV) Close() error {
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

func (r *leveldbKV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) {
	iter := r.db.NewIterator(r.ro)
	defer iter.Close()
	for iter.Seek(fk); iteratorIsValid(iter); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if inc {
			if bytes.Compare(key, lk) > 0 {
				return
			}
		} else {
			if bytes.Compare(key, lk) >= 0 {
				return
			}
		}
		cont, err := op(key, val)
		if err != nil {
			panic(err)
		}
		if !cont {
			return
		}
	}
}

func (r *leveldbKV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(r.ro, key)
	if err != nil {
		return err
	}
	return op(val)
}

func (r *leveldbKV) SaveValue(key []byte, value []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Put(key, value)
	return r.db.Write(r.wo, wb)
}

func (r *leveldbKV) DeleteValue(key []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Delete(key)
	return r.db.Write(r.wo, wb)
}

func (r *leveldbKV) GetWriteBatch(ctx raftio.IContext) IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*leveldbWriteBatch)
		}
	}
	return &leveldbWriteBatch{wb: levigo.NewWriteBatch()}
}

func (r *leveldbKV) CommitWriteBatch(wb IWriteBatch) error {
	lwb, ok := wb.(*leveldbWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return r.db.Write(r.wo, lwb.wb)
}

func (r *leveldbKV) RemoveEntries(firstKey []byte, lastKey []byte) error {
	return nil
}

func (r *leveldbKV) Compaction(firstKey []byte, lastKey []byte) error {
	func() {
		wb := levigo.NewWriteBatch()
		it := r.db.NewIterator(r.ro)
		defer it.Close()
		for it.Seek(firstKey); it.Valid(); it.Next() {
			if bytes.Compare(it.Key(), lastKey) < 0 {
				wb.Delete(it.Key())
			}
		}
		if err := it.GetError(); err != nil {
			panic(err)
		}
		if err := r.db.Write(r.wo, wb); err != nil {
			panic(err)
		}
	}()
	r.db.CompactRange(levigo.Range{Start: firstKey, Limit: lastKey})
	return nil
}
