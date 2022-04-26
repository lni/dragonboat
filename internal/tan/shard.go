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

package tan

import (
	"fmt"

	"github.com/cockroachdb/errors/oserror"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/vfs"
)

type shard interface {
	multiplexedLog() bool
	name(clusterID uint64, nodeID uint64) string
	get(clusterID uint64, nodeID uint64) (*db, bool)
	set(clusterID uint64, nodeID uint64, db *db)
	iterate(f func(*db) error) error
}

type singleNodeLogShard struct {
	shards map[raftio.NodeInfo]*db
}

func newSingleNodeLogShard() *singleNodeLogShard {
	return &singleNodeLogShard{
		shards: make(map[raftio.NodeInfo]*db),
	}
}

func (s *singleNodeLogShard) multiplexedLog() bool {
	return false
}

func (s *singleNodeLogShard) name(clusterID uint64, nodeID uint64) string {
	return fmt.Sprintf("node-%d-%d", clusterID, nodeID)
}

func (s *singleNodeLogShard) get(clusterID uint64,
	nodeID uint64) (*db, bool) {
	ni := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	v, ok := s.shards[ni]
	return v, ok
}

func (s *singleNodeLogShard) set(clusterID uint64, nodeID uint64, db *db) {
	ni := raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID}
	s.shards[ni] = db
}

func (s *singleNodeLogShard) iterate(f func(*db) error) error {
	for _, db := range s.shards {
		if err := f(db); err != nil {
			return err
		}
	}
	return nil
}

type multiplexedLogShard struct {
	shards map[uint64]*db
}

func newMultiplexedLogShard() *multiplexedLogShard {
	return &multiplexedLogShard{shards: make(map[uint64]*db)}
}

func (s *multiplexedLogShard) multiplexedLog() bool {
	return true
}

func (s *multiplexedLogShard) name(clusterID uint64, nodeID uint64) string {
	return fmt.Sprintf("shard-%d", s.shardID(clusterID))
}

func (s *multiplexedLogShard) shardID(clusterID uint64) uint64 {
	return clusterID % 16
}

func (s *multiplexedLogShard) get(clusterID uint64,
	nodeID uint64) (*db, bool) {
	v, ok := s.shards[s.shardID(clusterID)]
	return v, ok
}

func (s *multiplexedLogShard) set(clusterID uint64, nodeID uint64, db *db) {
	s.shards[s.shardID(clusterID)] = db
}

func (s *multiplexedLogShard) iterate(f func(*db) error) error {
	for _, db := range s.shards {
		if err := f(db); err != nil {
			return err
		}
	}
	return nil
}

type shards struct {
	fs      vfs.FS
	dirname string
	shards  shard
}

func newShards(dirname string, fs vfs.FS, singleNodeLog bool) *shards {
	var s shard
	if singleNodeLog {
		s = newSingleNodeLogShard()
	} else {
		s = newMultiplexedLogShard()
	}
	return &shards{
		fs:      fs,
		dirname: dirname,
		shards:  s,
	}
}

func (s *shards) multiplexedLog() bool {
	return s.shards.multiplexedLog()
}

func (s *shards) getDB(clusterID uint64, nodeID uint64) (*db, error) {
	db, ok := s.shards.get(clusterID, nodeID)
	if ok {
		return db, nil
	}
	name := s.shards.name(clusterID, nodeID)
	dbdir := s.fs.PathJoin(s.dirname, name)
	if err := s.prepareDir(dbdir); err != nil {
		return nil, err
	}
	db, err := open(dbdir, dbdir, &Options{FS: s.fs})
	if err != nil {
		return nil, err
	}
	s.shards.set(clusterID, nodeID, db)
	return db, nil
}

func (s *shards) prepareDir(dbdir string) error {
	if _, err := s.fs.Stat(dbdir); oserror.IsNotExist(err) {
		if err := fileutil.MkdirAll(dbdir, s.fs); err != nil {
			return err
		}
	}
	return nil
}

func (s *shards) iterate(f func(*db) error) error {
	return s.shards.iterate(f)
}
