// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type ReadonlyDB struct {
	Wrapped raftio.ILogDB
}

func (r *ReadonlyDB) Name() string {
	return r.Wrapped.Name()
}

func (r *ReadonlyDB) BinaryFormat() uint32 {
	return r.Wrapped.BinaryFormat()
}

func (r *ReadonlyDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	return r.Wrapped.ListNodeInfo()
}

func (r *ReadonlyDB) GetBootstrapInfo(clusterID uint64, nodeID uint64) (pb.Bootstrap, error) {
	return r.Wrapped.GetBootstrapInfo(clusterID, nodeID)
}

func (r *ReadonlyDB) IterateEntries(ents []pb.Entry, size uint64, clusterID uint64, nodeID uint64, low uint64, high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	return r.Wrapped.IterateEntries(ents, size, clusterID, nodeID, low, high, maxSize)
}

func (r *ReadonlyDB) ReadRaftState(clusterID uint64, nodeID uint64, lastIndex uint64) (raftio.RaftState, error) {
	return r.Wrapped.ReadRaftState(clusterID, nodeID, lastIndex)
}

func (r *ReadonlyDB) GetSnapshot(clusterID uint64, nodeID uint64) (pb.Snapshot, error) {
	return r.Wrapped.GetSnapshot(clusterID, nodeID)
}

func (r *ReadonlyDB) Close() error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) SaveBootstrapInfo(clusterID uint64, nodeID uint64, bootstrap pb.Bootstrap) error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) SaveRaftState(updates []pb.Update, shardID uint64) error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) RemoveEntriesTo(clusterID uint64, nodeID uint64, index uint64) error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) CompactEntriesTo(clusterID uint64, nodeID uint64, index uint64) (<-chan struct{}, error) {
	plog.Panicf("not implemented")
	return nil, nil
}

func (r *ReadonlyDB) SaveSnapshots(updates []pb.Update) error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	plog.Panicf("not implemented")
	return nil
}

func (r *ReadonlyDB) ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error {
	plog.Panicf("not implemented")
	return nil
}
