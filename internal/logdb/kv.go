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

package logdb

import (
	"github.com/lni/dragonboat/raftio"
)

// IKvStore is the interface used by the RDB struct to access the underlying
// Key-Value store.
type IKvStore interface {
	// Name is the IKvStore name.
	Name() string
	// Close closes the underlying Key-Value store.
	Close() error
	// IterateValue iterates the key range specified by the first key fk and
	// last key lk. The inc boolean flag indicates whether it is inclusive for
	// the last key lk. For each iterated entry, the specified op will be invoked
	// on that key-value pair, the specified op func returns a boolean flag to
	// indicate whether IterateValue should continue to iterate entries.
	IterateValue(fk []byte,
		lk []byte, inc bool, op func(key []byte, data []byte) (bool, error))
	// GetValue queries the value specified the input key, the returned value
	// byte slice is passed to the specified op func.
	GetValue(key []byte, op func([]byte) error) error
	// Save value saves the specified key value pair to the underlying key-value
	// pair.
	SaveValue(key []byte, value []byte) error
	// DeleteValue deletes the key-value pair specified by the input key.
	DeleteValue(key []byte) error
	// GetWriteBatch returns an IWriteBatch object to be used by RDB.
	GetWriteBatch(ctx raftio.IContext) IWriteBatch
	// CommitWriteBatch atomically writes everything included in the write batch
	// to the underlying key-value store.
	CommitWriteBatch(wb IWriteBatch) error
	// RemoveEntries removes entries specified by the range [firstKey, lastKey).
	// RemoveEntries is called in the main execution thread of raft, it is
	// suppose to immediately return without significant delay.
	RemoveEntries(firstKey []byte, lastKey []byte) error
	// Compaction is called by the compaction goroutine to compact the key-value
	// store for the specified range [firstKey, lastKey). This method is expected
	// to complete in the order of seconds.
	Compaction(firstKey []byte, lastKey []byte) error
}

// IWriteBatch is the interface representing a write batch capable of
// atomically writing many key-value pairs to the key-value store.
type IWriteBatch interface {
	Destroy()
	Put([]byte, []byte)
	Clear()
	Count() int
}

type kvpair struct {
	key []byte
	val []byte
}

type simpleWriteBatch struct {
	vals []kvpair
}

func newSimpleWriteBatch() *simpleWriteBatch {
	return &simpleWriteBatch{vals: make([]kvpair, 0)}
}

func (wb *simpleWriteBatch) Destroy() {
	wb.vals = nil
}

func (wb *simpleWriteBatch) Put(key []byte, val []byte) {
	k := make([]byte, len(key))
	v := make([]byte, len(val))
	copy(k, key)
	copy(v, val)
	wb.vals = append(wb.vals, kvpair{key: k, val: v})
}

func (wb *simpleWriteBatch) Clear() {
	wb.vals = make([]kvpair, 0)
}

func (wb *simpleWriteBatch) Count() int {
	return len(wb.vals)
}
