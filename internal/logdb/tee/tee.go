// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
	"sync"

	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/pebble"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/rocksdb"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	plog = logger.GetLogger("LogDB")
)

func assertSameError(e1 error, e2 error) {
	if e1 == e2 {
		return
	}
	plog.Panicf("conflict errors, e1 %v, e2 %v", e1, e2)
}

// LogDB is a special LogDB module used for testing purposes.
type LogDB struct {
	mu      sync.Mutex
	stopper *syncutil.Stopper
	odb     raftio.ILogDB
	ndb     raftio.ILogDB
}

// NewTeeLogDB creates a new LogDB instance.
func NewTeeLogDB(nhConfig config.NodeHostConfig,
	cb config.LogDBCallback, dirs []string, wals []string,
	fs vfs.IFS) (raftio.ILogDB, error) {
	odirs := make([]string, 0)
	owals := make([]string, 0)
	for _, v := range dirs {
		odirs = append(odirs, filepath.Join(v, "odir"))
	}
	for _, v := range wals {
		owals = append(owals, filepath.Join(v, "odir"))
	}
	odb, err := logdb.NewLogDB(nhConfig,
		cb, odirs, owals, false, false, fs, rocksdb.NewKVStore)
	if err != nil {
		return nil, err
	}
	ndirs := make([]string, 0)
	nwals := make([]string, 0)
	for _, v := range dirs {
		ndirs = append(ndirs, filepath.Join(v, "ndir"))
	}
	for _, v := range wals {
		nwals = append(nwals, filepath.Join(v, "ndir"))
	}
	ndb, err := logdb.NewLogDB(nhConfig,
		cb, ndirs, nwals, false, false, fs, pebble.NewKVStore)
	if err != nil {
		return nil, err
	}
	return &LogDB{
		stopper: syncutil.NewStopper(),
		odb:     odb,
		ndb:     ndb,
	}, nil
}

// Name ...
func (t *LogDB) Name() string {
	return "Tee"
}

// Close ...
func (t *LogDB) Close() {
	t.stopper.Stop()
	t.odb.Close()
	t.ndb.Close()
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
	assertSameError(oe, ne)
	if oe != nil {
		return nil, oe
	}
	if !reflect.DeepEqual(o, n) {
		plog.Panicf("conflict NodeInfo list len, %+v, %+v", o, n)
	}
	return o, nil
}

// SaveBootstrapInfo ...
func (t *LogDB) SaveBootstrapInfo(clusterID uint64,
	nodeID uint64, bootstrap pb.Bootstrap) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveBootstrapInfo(clusterID, nodeID, bootstrap)
	ne := t.ndb.SaveBootstrapInfo(clusterID, nodeID, bootstrap)
	assertSameError(oe, ne)
	return oe
}

// GetBootstrapInfo ...
func (t *LogDB) GetBootstrapInfo(clusterID uint64,
	nodeID uint64) (*pb.Bootstrap, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ob, oe := t.odb.GetBootstrapInfo(clusterID, nodeID)
	nb, ne := t.ndb.GetBootstrapInfo(clusterID, nodeID)
	assertSameError(oe, ne)
	if oe != nil {
		return nil, oe
	}
	if !reflect.DeepEqual(ob, nb) {
		plog.Panicf("conflict GetBootstrapInfo values, %+v, %+v", ob, nb)
	}
	return ob, nil
}

// SaveRaftState ...
func (t *LogDB) SaveRaftState(updates []pb.Update, shardID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveRaftState(updates, shardID)
	ne := t.ndb.SaveRaftState(updates, shardID)
	assertSameError(oe, ne)
	return oe
}

// IterateEntries ...
func (t *LogDB) IterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ov, os, oe := t.odb.IterateEntries(ents,
		size, clusterID, nodeID, low, high, maxSize)
	nv, ns, ne := t.ndb.IterateEntries(ents,
		size, clusterID, nodeID, low, high, maxSize)
	assertSameError(oe, ne)
	if oe != nil {
		return nil, 0, oe
	}
	if os != ns {
		plog.Panicf("conflict sizes, %d, %d", os, ns)
	}
	if !reflect.DeepEqual(ov, nv) {
		plog.Panicf("conflict entry lists, %+v, %+v", ov, nv)
	}
	return ov, os, nil
}

// ReadRaftState ...
func (t *LogDB) ReadRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (*raftio.RaftState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	os, oe := t.odb.ReadRaftState(clusterID, nodeID, lastIndex)
	ns, ne := t.odb.ReadRaftState(clusterID, nodeID, lastIndex)
	assertSameError(oe, ne)
	if oe != nil {
		return nil, oe
	}
	if !reflect.DeepEqual(os, ns) {
		plog.Panicf("conflict ReadRaftState values, %+v, %+v", os, ns)
	}
	return os, nil
}

// RemoveEntriesTo ...
func (t *LogDB) RemoveEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.RemoveEntriesTo(clusterID, nodeID, index)
	ne := t.ndb.RemoveEntriesTo(clusterID, nodeID, index)
	assertSameError(oe, ne)
	return oe
}

// SaveSnapshots ...
func (t *LogDB) SaveSnapshots(updates []pb.Update) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.SaveSnapshots(updates)
	ne := t.ndb.SaveSnapshots(updates)
	assertSameError(oe, ne)
	return oe
}

// DeleteSnapshot ...
func (t *LogDB) DeleteSnapshot(clusterID uint64,
	nodeID uint64, index uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.DeleteSnapshot(clusterID, nodeID, index)
	ne := t.ndb.DeleteSnapshot(clusterID, nodeID, index)
	assertSameError(oe, ne)
	return oe
}

// ListSnapshots ...
func (t *LogDB) ListSnapshots(clusterID uint64,
	nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ov, oe := t.odb.ListSnapshots(clusterID, nodeID, index)
	nv, ne := t.ndb.ListSnapshots(clusterID, nodeID, index)
	assertSameError(oe, ne)
	if oe != nil {
		return nil, oe
	}
	if !reflect.DeepEqual(ov, nv) {
		plog.Panicf("conflict snapshot lists, %+v, %+v", ov, nv)
	}
	return ov, nil
}

// RemoveNodeData ...
func (t *LogDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.RemoveNodeData(clusterID, nodeID)
	ne := t.ndb.RemoveNodeData(clusterID, nodeID)
	assertSameError(oe, ne)
	return oe
}

// ImportSnapshot ...
func (t *LogDB) ImportSnapshot(ss pb.Snapshot, nodeID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	oe := t.odb.ImportSnapshot(ss, nodeID)
	ne := t.ndb.ImportSnapshot(ss, nodeID)
	assertSameError(oe, ne)
	return oe
}

// CompactEntriesTo ...
func (t *LogDB) CompactEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) (<-chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	done := make(chan struct{}, 1)
	oc, oe := t.odb.CompactEntriesTo(clusterID, nodeID, index)
	nc, ne := t.ndb.CompactEntriesTo(clusterID, nodeID, index)
	assertSameError(oe, ne)
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
