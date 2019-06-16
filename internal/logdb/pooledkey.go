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

package logdb

import (
	"encoding/binary"
	"sync"
)

const (
	maxKeySize             uint64 = 28
	entryKeySize           uint64 = 28
	persistentStateKeySize uint64 = 20
	maxIndexKeySize        uint64 = 20
	nodeInfoKeySize        uint64 = 20
	bootstrapKeySize       uint64 = 20
	snapshotKeySize        uint64 = 28
	dataSize               uint64 = entryKeySize
)

var (
	entryKeyHeader           = [2]byte{0x1, 0x1}
	persistentStateKeyHeader = [2]byte{0x2, 0x2}
	maxIndexKeyHeader        = [2]byte{0x3, 0x3}
	nodeInfoKeyHeader        = [2]byte{0x4, 0x4}
	snapshotKeyHeader        = [2]byte{0x5, 0x5}
	bootstrapKeyHeader       = [2]byte{0x6, 0x6}
	entryBatchKeyHeader      = [2]byte{0x7, 0x7}
)

// PooledKey represents keys that are managed by a sync.Pool to be reused.
type PooledKey struct {
	data []byte
	key  []byte
	pool *sync.Pool
}

// NewKey creates and returns a new PooledKey instance.
func NewKey(sz uint64, pool *sync.Pool) *PooledKey {
	return newKey(sz, pool)
}

func newKey(sz uint64, pool *sync.Pool) *PooledKey {
	return &PooledKey{
		data: make([]byte, sz),
		pool: pool,
	}
}

// Release puts the key back to the pool.
func (k *PooledKey) Release() {
	k.key = nil
	if k.pool != nil {
		k.pool.Put(k)
	}
}

// Key returns the []byte of the key.
func (k *PooledKey) Key() []byte {
	return k.key
}

// SetMinimumKey sets the key to the minimum possible value.
func (k *PooledKey) SetMinimumKey() {
	k.key = k.data
	for i := 0; i < len(k.key); i++ {
		k.key[i] = byte(0)
	}
}

// SetMaximumKey sets the key to the maximum possible value.
func (k *PooledKey) SetMaximumKey() {
	k.key = k.data
	for i := 0; i < len(k.key); i++ {
		k.key[i] = byte(0xFF)
	}
}

// SetEntryBatchKey sets the key value opf the entry batch.
func (k *PooledKey) SetEntryBatchKey(clusterID uint64,
	nodeID uint64, batchID uint64) {
	k.useAsEntryKey()
	k.key[0] = entryBatchKeyHeader[0]
	k.key[1] = entryBatchKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
	binary.BigEndian.PutUint64(k.key[20:], batchID)
}

// SetEntryKey sets the key value to the specified entry key.
func (k *PooledKey) SetEntryKey(clusterID uint64, nodeID uint64, index uint64) {
	k.useAsEntryKey()
	k.key[0] = entryKeyHeader[0]
	k.key[1] = entryKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	// the 8 bytes node ID is actually not required in the key. it is stored as
	// an extra safenet - we don't know what we don't know, it is used as extra
	// protection between different node instances when things get ugly.
	// the wasted 8 bytes per entry is not a big deal - storing the index is
	// wasteful as well.
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
	binary.BigEndian.PutUint64(k.key[20:], index)
}

// SetStateKey sets the key value to the specified State.
func (k *PooledKey) SetStateKey(clusterID uint64, nodeID uint64) {
	k.useAsStateKey()
	k.key[0] = persistentStateKeyHeader[0]
	k.key[1] = persistentStateKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
}

// SetMaxIndexKey sets the key value to the max index record key.
func (k *PooledKey) SetMaxIndexKey(clusterID uint64, nodeID uint64) {
	k.useAsMaxIndexKey()
	k.key[0] = maxIndexKeyHeader[0]
	k.key[1] = maxIndexKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
}

func (k *PooledKey) useAsEntryKey() {
	k.key = k.data
}

func (k *PooledKey) useAsSnapshotKey() {
	k.key = k.data
}

func (k *PooledKey) useAsStateKey() {
	k.key = k.data[:persistentStateKeySize]
}

func (k *PooledKey) useAsMaxIndexKey() {
	k.key = k.data[:maxIndexKeySize]
}

func (k *PooledKey) useAsNodeInfoKey() {
	k.key = k.data[:nodeInfoKeySize]
}

func (k *PooledKey) useAsBootstrapKey() {
	k.key = k.data[:bootstrapKeySize]
}

func parseNodeInfoKey(data []byte) (uint64, uint64) {
	if len(data) != 20 {
		panic("invalid node info data")
	}
	cid := binary.BigEndian.Uint64(data[4:])
	nid := binary.BigEndian.Uint64(data[12:])
	return cid, nid
}

func (k *PooledKey) setNodeInfoKey(clusterID uint64, nodeID uint64) {
	k.useAsNodeInfoKey()
	k.key[0] = nodeInfoKeyHeader[0]
	k.key[1] = nodeInfoKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
}

func (k *PooledKey) setBootstrapKey(clusterID uint64, nodeID uint64) {
	k.useAsBootstrapKey()
	k.key[0] = bootstrapKeyHeader[0]
	k.key[1] = bootstrapKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
}

func (k *PooledKey) setSnapshotKey(clusterID uint64, nodeID uint64, index uint64) {
	k.useAsSnapshotKey()
	k.key[0] = snapshotKeyHeader[0]
	k.key[1] = snapshotKeyHeader[1]
	k.key[2] = 0
	k.key[3] = 0
	binary.BigEndian.PutUint64(k.key[4:], clusterID)
	binary.BigEndian.PutUint64(k.key[12:], nodeID)
	binary.BigEndian.PutUint64(k.key[20:], index)
}

type logdbKeyPool struct {
	pool *sync.Pool
}

func newLogdbKeyPool() *logdbKeyPool {
	p := &sync.Pool{}
	p.New = func() interface{} {
		return newKey(dataSize, p)
	}
	return &logdbKeyPool{
		pool: p,
	}
}

func (p *logdbKeyPool) get() *PooledKey {
	return p.pool.Get().(*PooledKey)
}
