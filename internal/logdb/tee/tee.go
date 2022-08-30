// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

package tee

import (
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/internal/logdb/kv/pebble"
	"github.com/lni/dragonboat/v4/internal/tan"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("LogDB")
)

var dn = logutil.DescribeNode

func assertSameError(shardID uint64, replicaID uint64, e1 error, e2 error) {
	if errors.Is(e1, e2) || errors.Is(e2, e1) {
		return
	}
	plog.Panicf("conflict errors, %s, e1 %v, e2 %v",
		dn(shardID, replicaID), e1, e2)
}

// LogDB is a special LogDB module used for testing purposes.
type LogDB struct {
	mu      sync.Mutex
	stopper *syncutil.Stopper
	odb     raftio.ILogDB
	ndb     raftio.ILogDB
}

// NewTanLogDB creates a new RocksDB based LogDB instance
func NewTanLogDB(nhConfig config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	return tan.Factory.Create(nhConfig, cb, dirs, wals)
}

// NewPebbleLogDB creates a new LogDB instance.
func NewPebbleLogDB(nhConfig config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	return newKVLogDB(nhConfig, cb, dirs, wals, "tee-pebble", pebble.NewKVStore)
}

func newKVLogDB(nhConfig config.NodeHostConfig,
	cb config.LogDBCallback, dirs []string, wals []string,
	subdir string, f kv.Factory) (raftio.ILogDB, error) {
	ndirs := make([]string, 0)
	nwals := make([]string, 0)
	for _, v := range dirs {
		ndirs = append(ndirs, filepath.Join(v, subdir))
	}
	for _, v := range wals {
		nwals = append(nwals, filepath.Join(v, subdir))
	}
	return logdb.NewLogDB(nhConfig, cb, ndirs, nwals, false, false, f)
}

// NewTeeLogDB creates a new LogDB instance backed by a pebble and a tan
// based ILogDB.
func NewTeeLogDB(nhConfig config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	odb, err := NewTanLogDB(nhConfig, cb, dirs, wals)
	if err != nil {
		return nil, err
	}
	ndb, err := NewPebbleLogDB(nhConfig, cb, dirs, wals)
	if err != nil {
		return nil, err
	}
	return MakeTeeLogDB(odb, ndb), nil
}

// MakeTeeLogDB returns a LogDB instance combined from the specified odb and
// ndb instances.
func MakeTeeLogDB(odb raftio.ILogDB, ndb raftio.ILogDB) raftio.ILogDB {
	return &LogDB{
		stopper: syncutil.NewStopper(),
		odb:     odb,
		ndb:     ndb,
	}
}

// Name ...
func (t *LogDB) Name() string {
	return "Tee"
}

// Close ...
func (t *LogDB) Close() error {
	t.stopper.Stop()
	if err := t.odb.Close(); err != nil {
		return nil
	}
	return t.ndb.Close()
}

// BinaryFormat ...
func (t *LogDB) BinaryFormat() uint32 {
	o := t.odb.BinaryFormat()
	n := t.ndb.BinaryFormat()
	if o != n {
		plog.Panicf("binary format changed, odb %d, ndb %d", o, n)
	}
	return o
}

// ListNodeInfo ...
func (t *LogDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	o, oe := t.odb.ListNodeInfo()
	n, ne := t.ndb.ListNodeInfo()
	assertSameError(0, 0, oe, ne)
	if oe != nil {
		return nil, oe
	}
	sort.Slice(o, func(i, j int) bool {
		if o[i].ShardID == o[j].ShardID {
			return o[i].ReplicaID < o[j].ReplicaID
		}
		return o[i].ShardID < o[j].ShardID
	})
	sort.Slice(n, func(i, j int) bool {
		if n[i].ShardID == n[j].ShardID {
			return n[i].ReplicaID < n[j].ReplicaID
		}
		return n[i].ShardID < n[j].ShardID
	})
	if !reflect.DeepEqual(o, n) {
		plog.Panicf("conflict NodeInfo list, %+v, %+v", o, n)
	}
	return o, nil
}

// SaveBootstrapInfo ...
func (t *LogDB) SaveBootstrapInfo(shardID uint64,
	replicaID uint64, bootstrap pb.Bootstrap) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveBootstrapInfo(shardID, replicaID, bootstrap)
	ne := t.ndb.SaveBootstrapInfo(shardID, replicaID, bootstrap)
	assertSameError(shardID, replicaID, oe, ne)
	return oe
}

// GetBootstrapInfo ...
func (t *LogDB) GetBootstrapInfo(shardID uint64,
	replicaID uint64) (pb.Bootstrap, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ob, oe := t.odb.GetBootstrapInfo(shardID, replicaID)
	nb, ne := t.ndb.GetBootstrapInfo(shardID, replicaID)
	assertSameError(shardID, replicaID, oe, ne)
	if oe != nil {
		return pb.Bootstrap{}, oe
	}
	if !reflect.DeepEqual(ob, nb) {
		plog.Panicf("%s conflict GetBootstrapInfo values, %+v, %+v",
			dn(shardID, replicaID), ob, nb)
	}
	return ob, nil
}

// SaveRaftState ...
func (t *LogDB) SaveRaftState(updates []pb.Update, shardID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveRaftState(updates, shardID)
	ne := t.ndb.SaveRaftState(updates, shardID)
	assertSameError(0, 0, oe, ne)
	return oe
}

// IterateEntries ...
func (t *LogDB) IterateEntries(ents []pb.Entry,
	size uint64, shardID uint64, replicaID uint64, low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ce := make([]pb.Entry, len(ents))
	copy(ce, ents)
	ov, os, oe := t.odb.IterateEntries(ents,
		size, shardID, replicaID, low, high, maxSize)
	nv, ns, ne := t.ndb.IterateEntries(ce,
		size, shardID, replicaID, low, high, maxSize)
	assertSameError(0, 0, oe, ne)
	if oe != nil {
		return nil, 0, oe
	}
	if os != ns {
		plog.Infof("")
		plog.Panicf("%s conflict sizes, %d, %d, %+v, %+v",
			dn(shardID, replicaID), os, ns, ov, nv)
	}
	if len(ov) != 0 || len(nv) != 0 {
		if !reflect.DeepEqual(ov, nv) {
			plog.Panicf("%s conflict entry lists, len: %d, %+v \n\n len: %d, %+v",
				dn(shardID, replicaID), len(ov), ov, len(nv), nv)
		}
	}
	return ov, os, nil
}

// ReadRaftState ...
func (t *LogDB) ReadRaftState(shardID uint64,
	replicaID uint64, lastIndex uint64) (raftio.RaftState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	os, oe := t.odb.ReadRaftState(shardID, replicaID, lastIndex)
	ns, ne := t.ndb.ReadRaftState(shardID, replicaID, lastIndex)
	assertSameError(shardID, replicaID, oe, ne)
	if oe != nil {
		return raftio.RaftState{}, oe
	}
	if !reflect.DeepEqual(os, ns) {
		plog.Panicf("%s conflict ReadRaftState values, %+v, %+v",
			dn(shardID, replicaID), os, ns)
	}
	return os, nil
}

// RemoveEntriesTo ...
func (t *LogDB) RemoveEntriesTo(shardID uint64,
	replicaID uint64, index uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.RemoveEntriesTo(shardID, replicaID, index)
	ne := t.ndb.RemoveEntriesTo(shardID, replicaID, index)
	assertSameError(shardID, replicaID, oe, ne)
	return oe
}

// SaveSnapshots ...
func (t *LogDB) SaveSnapshots(updates []pb.Update) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveSnapshots(updates)
	ne := t.ndb.SaveSnapshots(updates)
	assertSameError(0, 0, oe, ne)
	return oe
}

// GetSnapshot ...
func (t *LogDB) GetSnapshot(shardID uint64,
	replicaID uint64) (pb.Snapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ov, oe := t.odb.GetSnapshot(shardID, replicaID)
	nv, ne := t.ndb.GetSnapshot(shardID, replicaID)
	assertSameError(shardID, replicaID, oe, ne)
	if oe != nil {
		return pb.Snapshot{}, oe
	}
	if !reflect.DeepEqual(ov, nv) {
		plog.Panicf("%s conflict snapshot lists, \n%+v \n\n %+v",
			dn(shardID, replicaID), ov, nv)
	}
	return ov, nil
}

// RemoveNodeData ...
func (t *LogDB) RemoveNodeData(shardID uint64, replicaID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.RemoveNodeData(shardID, replicaID)
	ne := t.ndb.RemoveNodeData(shardID, replicaID)
	assertSameError(shardID, replicaID, oe, ne)
	return oe
}

// ImportSnapshot ...
func (t *LogDB) ImportSnapshot(ss pb.Snapshot, replicaID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.ImportSnapshot(ss, replicaID)
	ne := t.ndb.ImportSnapshot(ss, replicaID)
	assertSameError(ss.ShardID, replicaID, oe, ne)
	return oe
}

// CompactEntriesTo ...
func (t *LogDB) CompactEntriesTo(shardID uint64,
	replicaID uint64, index uint64) (<-chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	done := make(chan struct{}, 1)
	oc, oe := t.odb.CompactEntriesTo(shardID, replicaID, index)
	nc, ne := t.ndb.CompactEntriesTo(shardID, replicaID, index)
	assertSameError(shardID, replicaID, oe, ne)
	if oe != nil {
		return nil, oe
	}
	t.stopper.RunWorker(func() {
		count := 0
		for {
			select {
			case <-oc:
				count++
			case <-nc:
				count++
			case <-t.stopper.ShouldStop():
				done <- struct{}{}
				return
			}
			if count == 2 {
				done <- struct{}{}
				return
			}
		}
	})
	return done, nil
}
