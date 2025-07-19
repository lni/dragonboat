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
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionTaskCanBeCreated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	assert.Equal(t, 0, p.len(), "size is not 0")
}

func TestCompactionTaskCanBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	assert.Equal(t, 1, p.len(), "len unexpectedly reported %d", p.len())
	assert.Equal(t, 1, len(p.pendings), "p.pending len is not 1")
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	assert.True(t, ok, "not added")
	assert.Equal(t, uint64(3), v.index, "unexpected index %d", v)
}

func TestCompactionTaskCanBeUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	p.addTask(task{shardID: 1, replicaID: 2, index: 10})
	assert.Equal(t, 1, len(p.pendings), "p.pending len is not 1")
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	assert.True(t, ok, "not added")
	assert.Equal(t, uint64(10), v.index, "unexpected index %d", v)
}

func TestCompactionDoneChanIsRetained(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	done := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}].done
	p.addTask(task{shardID: 1, replicaID: 2, index: 10})
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	assert.True(t, ok, "not added")
	assert.Equal(t, done, v.done, "chan not retained")
}

func TestCompactionTaskGetReturnTheExpectedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	tt := task{shardID: 1, replicaID: 2, index: 3}
	p.addTask(tt)
	assert.Equal(t, 1, p.len(), "len unexpectedly reported %d", p.len())
	task, ok := p.getTask()
	assert.True(t, ok, "ok flag unexpected")
	require.NotNil(t, task.done, "task.done == nil")
	task.done = nil
	assert.True(t, reflect.DeepEqual(&tt, &task), "%v vs %v", tt, task)
}

func TestCompactionTaskGetReturnAllExpectedValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	p.addTask(task{shardID: 2, replicaID: 2, index: 10})
	assert.Equal(t, 2, p.len(), "len unexpectedly reported %d", p.len())
	task1, ok := p.getTask()
	assert.True(t, ok, "ok flag unexpected")
	task2, ok := p.getTask()
	assert.True(t, ok, "ok flag unexpected")
	assert.True(t, task1.index == 3 || task1.index == 10, "unexpected task obj")
	assert.True(t, task2.index == 3 || task2.index == 10, "unexpected task obj")
	_, ok = p.getTask()
	assert.False(t, ok, "unexpected ok flag value")
}

func TestMovingCompactionIndexBackWillCausePanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	require.Panics(t, func() {
		p.addTask(task{shardID: 1, replicaID: 2, index: 2})
	})
}
