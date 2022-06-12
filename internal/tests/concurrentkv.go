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

package tests

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/tests/kvpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type kvdata struct {
	kvs   sync.Map
	count uint64
	junk  []byte
}

// KVJson is an util struct for serializing and deserializing data.
type KVJson struct {
	KVStore map[string]string `json:"KVStore"`
	Count   uint64            `json:"Count"`
	Junk    []byte            `json:"Junk"`
}

// ConcurrentKVTest is a in memory key-value store struct used for testing
// purposes. Note that both key/value are suppose to be valid utf-8 strings.
type ConcurrentKVTest struct {
	ShardID          uint64
	ReplicaID        uint64
	kvdata           unsafe.Pointer
	externalFileTest bool
	closed           uint32
}

// NewConcurrentKVTest creates and return a new KVTest object.
func NewConcurrentKVTest(shardID uint64, replicaID uint64) sm.IConcurrentStateMachine {
	s := &ConcurrentKVTest{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	kvdata := &kvdata{junk: make([]byte, 3*1024)}
	// write some junk data consistent across the shard
	for i := 0; i < len(kvdata.junk); i++ {
		kvdata.junk[i] = 2
	}
	s.kvdata = unsafe.Pointer(kvdata)

	v := os.Getenv("EXTERNALFILETEST")
	s.externalFileTest = len(v) > 0
	fmt.Printf("junk data inserted, external file test %t\n", s.externalFileTest)
	return s
}

// Lookup performances local looks up for the sepcified data.
func (s *ConcurrentKVTest) Lookup(key interface{}) (interface{}, error) {
	kvdata := (*kvdata)(atomic.LoadPointer(&(s.kvdata)))
	query := string(key.([]byte))
	v, ok := kvdata.kvs.Load(query)
	if ok {
		return []byte(v.(string)), nil
	}
	return []byte(""), nil
}

// Update updates the object using the specified committed raft entry.
func (s *ConcurrentKVTest) Update(ents []sm.Entry) ([]sm.Entry, error) {
	for i := 0; i < len(ents); i++ {
		dataKv := &kvpb.PBKV{}
		err := dataKv.Unmarshal(ents[i].Cmd)
		if err != nil {
			panic(err)
		}
		key := dataKv.GetKey()
		val := dataKv.GetVal()
		kvdata := (*kvdata)(atomic.LoadPointer(&(s.kvdata)))
		kvdata.kvs.Store(key, val)
		ents[i].Result = sm.Result{Value: uint64(len(ents[i].Cmd))}
	}
	return ents, nil
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (s *ConcurrentKVTest) PrepareSnapshot() (interface{}, error) {
	p := (*kvdata)(atomic.LoadPointer(&(s.kvdata)))
	data := &kvdata{
		count: p.count,
	}
	data.junk = append(data.junk, p.junk...)
	p.kvs.Range(func(k, v interface{}) bool {
		key := k.(string)
		val := v.(string)
		data.kvs.Store(key, val)
		return true
	})
	return data, nil
}

// SaveSnapshot saves the current object state into a snapshot using the
// specified io.Writer object.
func (s *ConcurrentKVTest) SaveSnapshot(ctx interface{},
	w io.Writer,
	fileCollection sm.ISnapshotFileCollection,
	done <-chan struct{}) error {
	if s.isClosed() {
		panic("save snapshot called after Close()")
	}
	delay := getLargeRandomDelay(s.ShardID)
	fmt.Printf("random delay %d ms\n", delay)
	for delay > 0 {
		delay -= 10
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			return sm.ErrSnapshotStopped
		default:
		}
	}
	ctxdata := ctx.(*kvdata)
	jsondata := &KVJson{
		KVStore: make(map[string]string),
		Count:   ctxdata.count,
		Junk:    ctxdata.junk,
	}
	ctxdata.kvs.Range(func(k, v interface{}) bool {
		key := k.(string)
		val := v.(string)
		jsondata.KVStore[key] = val
		return true
	})
	data, err := json.Marshal(jsondata)
	if err != nil {
		panic(err)
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		panic("didn't write the whole data buf")
	}
	return nil
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *ConcurrentKVTest) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{}) error {
	if s.isClosed() {
		panic("recover from snapshot called after Close()")
	}
	delay := getLargeRandomDelay(s.ShardID)
	fmt.Printf("random delay %d ms\n", delay)
	for delay > 0 {
		delay -= 10
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			return sm.ErrSnapshotStopped
		default:
		}
	}
	kvdata := &kvdata{}
	jsondata := &KVJson{}
	data, err := fileutil.ReadAll(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, jsondata); err != nil {
		return err
	}
	for k, v := range jsondata.KVStore {
		kvdata.kvs.Store(k, v)
	}
	kvdata.count = jsondata.Count
	kvdata.junk = jsondata.Junk
	atomic.StorePointer(&(s.kvdata), unsafe.Pointer(kvdata))
	return nil
}

// Close closes the IStateMachine instance
func (s *ConcurrentKVTest) Close() error {
	atomic.StoreUint32(&s.closed, 1)
	return nil
}

// GetHash returns a uint64 representing the current object state.
func (s *ConcurrentKVTest) GetHash() (uint64, error) {
	p := (*kvdata)(atomic.LoadPointer(&(s.kvdata)))
	jsondata := &KVJson{
		KVStore: make(map[string]string),
		Count:   p.count,
	}
	p.kvs.Range(func(k, v interface{}) bool {
		key := k.(string)
		val := v.(string)
		jsondata.KVStore[key] = val
		return true
	})
	data, err := json.Marshal(jsondata)
	if err != nil {
		panic(err)
	}
	hash := md5.New()
	if _, err = hash.Write(data); err != nil {
		panic(err)
	}
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8]), nil
}

func (s *ConcurrentKVTest) isClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}
