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
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/raftio"
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
	w.wb = w.db.NewBatch()
	w.count = 0
}

func (w *pebbleWriteBatch) Count() int {
	return w.count
}

// NewKVStore returns a pebble based IKVStore instance.
func NewKVStore(dir string, wal string) (kv.IKVStore, error) {
	return openPebbleDB(dir, wal)
}

// KV is a pebble based IKVStore type.
type KV struct {
	db   *pebble.DB
	opts *pebble.Options
	ro   *pebble.IterOptions
	wo   *pebble.WriteOptions
}

func openPebbleDB(dir string, walDir string) (*KV, error) {
	fmt.Printf("pebble support is experimental, DO NOT USE IN PRODUCTION\n")
	lopts := pebble.LevelOptions{Compression: pebble.NoCompression}
	opts := &pebble.Options{
		Levels: []pebble.LevelOptions{lopts},
	}
	if len(walDir) > 0 {
		opts.WALDir = walDir
	}
	pdb, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
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
func (r *KV) GetValue(key []byte,
	op func([]byte) error) error {
	val, err := r.db.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
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
			return ctx.GetWriteBatch().(*pebbleWriteBatch)
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
	return nil
}

func (r *KV) deleteRange(fk []byte, lk []byte) error {
	iter := r.db.NewIter(r.ro)
	defer iter.Close()
	wb := r.GetWriteBatch(nil)
	for iter.SeekGE(fk); iteratorIsValid(iter); iter.Next() {
		if bytes.Compare(iter.Key(), lk) >= 0 {
			break
		}
		wb.Delete(iter.Key())
	}
	if wb.Count() > 0 {
		return r.CommitWriteBatch(wb)
	}
	return nil
}

// CompactEntries ...
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	if err := r.deleteRange(fk, lk); err != nil {
		return err
	}
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
