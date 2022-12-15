// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package kv

import (
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/vfs"
)

const (
	// MaxKeyLength is the max length of keys allowed
	MaxKeyLength uint64 = 1024
)

// Factory is the factory function type used for creating IKVStore instances.
type Factory func(config.LogDBConfig,
	LogDBCallback, string, string, vfs.IFS) (IKVStore, error)

// LogDBCallback is a callback function called by the LogDB
type LogDBCallback func(busy bool)

// IWriteBatch is the interface representing a write batch capable of
// atomically writing many key-value pairs to the key-value store.
type IWriteBatch interface {
	Destroy()
	Put([]byte, []byte)
	Delete([]byte)
	Clear()
	Count() int
}

// IKVStore is the interface used by the RDB struct to access the underlying
// Key-Value store.
type IKVStore interface {
	// Name is the IKVStore name.
	Name() string
	// Close closes the underlying Key-Value store.
	Close() error
	// IterateValue iterates the key range specified by the first key fk and
	// last key lk. The inc boolean flag indicates whether it is inclusive for
	// the last key lk. For each iterated entry, the specified op will be invoked
	// on that key-value pair, the specified op func returns a boolean flag to
	// indicate whether IterateValue should continue to iterate entries.
	IterateValue(fk []byte,
		lk []byte, inc bool, op func(key []byte, data []byte) (bool, error)) error
	// GetValue queries the value specified the input key, the returned value
	// byte slice is passed to the specified op func.
	GetValue(key []byte, op func([]byte) error) error
	// Save value saves the specified key value pair to the underlying key-value
	// pair.
	SaveValue(key []byte, value []byte) error
	// DeleteValue deletes the key-value pair specified by the input key.
	DeleteValue(key []byte) error
	// GetWriteBatch returns an IWriteBatch object to be used by RDB.
	GetWriteBatch() IWriteBatch
	// CommitWriteBatch atomically writes everything included in the write batch
	// to the underlying key-value store.
	CommitWriteBatch(wb IWriteBatch) error
	// BulkRemoveEntries removes entries specified by the range [firstKey,
	// lastKey). BulkRemoveEntries is called in the main execution thread of raft,
	// it is supposed to immediately return without significant delay.
	// BulkRemoveEntries is usually implemented in KV store's range delete feature.
	BulkRemoveEntries(firstKey []byte, lastKey []byte) error
	// CompactEntries reclaims the underlying disk space used for storing entries
	// specified the input range.
	CompactEntries(firstKey []byte, lastKey []byte) error
	// FullCompaction compact the entire key space.
	FullCompaction() error
}
