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
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockDB struct {
	mock.Mock
}

func (m *MockDB) Name() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockDB) BinaryFormat() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	args := m.Called()
	return args.Get(0).([]raftio.NodeInfo), args.Error(1)
}

func (m *MockDB) GetBootstrapInfo(clusterID, nodeID uint64) (pb.Bootstrap, error) {
	args := m.Called(clusterID, nodeID)
	return args.Get(0).(pb.Bootstrap), args.Error(1)
}
func (m *MockDB) IterateEntries(ents []pb.Entry, size, clusterID, nodeID, low, high, maxSize uint64) ([]pb.Entry, uint64, error) {
	args := m.Called(ents, size, clusterID, nodeID, low, high, maxSize)
	return args.Get(0).([]pb.Entry), args.Get(1).(uint64), args.Error(2)
}

func (m *MockDB) ReadRaftState(clusterID uint64, nodeID uint64, lastIndex uint64) (raftio.RaftState, error) {
	args := m.Called(clusterID, nodeID, lastIndex)
	return args.Get(0).(raftio.RaftState), args.Error(1)
}
func (m *MockDB) GetSnapshot(clusterID uint64, nodeID uint64) (pb.Snapshot, error) {
	args := m.Called(clusterID, nodeID)
	return args.Get(0).(pb.Snapshot), args.Error(1)
}

func (m *MockDB) Close() error { return nil }
func (m *MockDB) RemoveEntriesTo(_, _, _ uint64) error { return nil }
func (m *MockDB) CompactEntriesTo(_, _, _ uint64) (<-chan struct{}, error) { return nil, nil }
func (m *MockDB) SaveSnapshots(_ []pb.Update) error { return nil}
func (m *MockDB) RemoveNodeData(_, _ uint64) error { return nil }
func (m *MockDB) ImportSnapshot(_ pb.Snapshot, _ uint64) error { return nil }
func (m *MockDB) SaveBootstrapInfo(_, _ uint64, _ pb.Bootstrap) error { return nil }
func (m *MockDB) SaveRaftState(_ []pb.Update, _ uint64) error { return nil }

func TestReadOnlyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mockDB := MockDB{}
	readonlyDB := ReadonlyDB{Wrapped: &mockDB}
	uOne := uint64(1)

	mockDB.On("Name").Return("name")
	name := readonlyDB.Name()
	assert.Equal(t, name, "name")

	mockDB.On("BinaryFormat").Return(uint32(1))
	received := readonlyDB.BinaryFormat()
	assert.Equal(t, received, uint32(1))

	mockDB.On("ListNodeInfo").Return([]raftio.NodeInfo{{1,1}}, nil)
	nodeInfo, err := readonlyDB.ListNodeInfo()
	assert.NoError(t, err)
	assert.Equal(t, nodeInfo, []raftio.NodeInfo{{ClusterID: 1, NodeID: 1}})

	mockDB.On("GetBootstrapInfo", uOne, uOne).Return(pb.Bootstrap{Join: true, Type: pb.RegularStateMachine}, nil)
	bootstrapInfo, err := readonlyDB.GetBootstrapInfo(1, 1)
	assert.NoError(t, err)
	assert.Equal(t, bootstrapInfo, pb.Bootstrap{Join: true, Type: pb.RegularStateMachine})

	mockDB.On("IterateEntries", []pb.Entry{{Term: 1}}, uOne, uOne, uOne, uOne, uOne, uOne).Return([]pb.Entry{{Term: 1}}, uOne, nil)
	entries, size, err := readonlyDB.IterateEntries([]pb.Entry{{Term: 1}}, 1, 1, 1, 1, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, entries, []pb.Entry{{Term: 1}})
	assert.Equal(t, size, uOne)

	mockDB.On("ReadRaftState", uOne, uOne, uOne).Return(raftio.RaftState{FirstIndex: 1}, nil)
	raftState, err := readonlyDB.ReadRaftState(1, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, raftState, raftio.RaftState{FirstIndex: 1})

	mockDB.On("GetSnapshot",  uOne, uOne).Return(pb.Snapshot{Dummy: true}, nil)
	snapshot, err := readonlyDB.GetSnapshot(1, 1)
	assert.NoError(t, err)
	assert.Equal(t, snapshot, pb.Snapshot{Dummy: true})

	mockDB.AssertExpectations(t)
}

func assertPanic(t *testing.T, f func()) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic")
		}
	}()
	f()
}

func TestUnimplemented(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer deleteTestDB(fs)

	dir := "db-dir"
	lldir := "wal-db-dir"
	d := fs.PathJoin(RDBTestDirectory, dir)
	lld := fs.PathJoin(RDBTestDirectory, lldir)
	if err := fs.RemoveAll(d); err != nil {
		t.Fatalf("%v", err)
	}
	if err := fs.RemoveAll(lld); err != nil {
		t.Fatalf("%v", err)
	}

	db := getNewTestDB(dir, lldir, false, fs)
	defer func() { _ = db.Close() }()
	readonlyDB := ReadonlyDB{Wrapped: db}

	assertPanic(t, func() { _ = readonlyDB.Close() })
	assertPanic(t, func() { _ = readonlyDB.SaveBootstrapInfo(0, 0, pb.Bootstrap{}) })
	assertPanic(t, func() { _ = readonlyDB.SaveRaftState(nil, 0) })
	assertPanic(t, func() { _ = readonlyDB.RemoveEntriesTo(0, 0, 0) })
	assertPanic(t, func() { _, _ = readonlyDB.CompactEntriesTo(0, 0, 0) })
	assertPanic(t, func() { _ = readonlyDB.SaveSnapshots(nil) })
	assertPanic(t, func() { _ = readonlyDB.RemoveNodeData(0, 0) })
	assertPanic(t, func() { _ = readonlyDB.ImportSnapshot(pb.Snapshot{}, 0) })
}
