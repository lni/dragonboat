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
)

func TestCompactionTaskCanBeCreated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	if p.len() != 0 {
		t.Errorf("size is not 0")
	}
}

func TestCompactionTaskCanBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	if p.len() != 1 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	if len(p.pendings) != 1 {
		t.Errorf("p.pending len is not 1")
	}
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	if !ok {
		t.Errorf("not added")
	}
	if v.index != 3 {
		t.Errorf("unexpected index %d", v)
	}
}

func TestCompactionTaskCanBeUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	p.addTask(task{shardID: 1, replicaID: 2, index: 10})
	if len(p.pendings) != 1 {
		t.Errorf("p.pending len is not 1")
	}
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	if !ok {
		t.Errorf("not added")
	}
	if v.index != 10 {
		t.Errorf("unexpected index %d", v)
	}
}

func TestCompactionDoneChanIsRetained(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	done := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}].done
	p.addTask(task{shardID: 1, replicaID: 2, index: 10})
	v, ok := p.pendings[raftio.NodeInfo{ShardID: 1, ReplicaID: 2}]
	if !ok {
		t.Errorf("not added")
	}
	if v.done != done {
		t.Errorf("chan not retained")
	}
}

func TestCompactionTaskGetReturnTheExpectedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	tt := task{shardID: 1, replicaID: 2, index: 3}
	p.addTask(tt)
	if p.len() != 1 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	task, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	if task.done == nil {
		t.Fatalf("task.done == nil")
	}
	task.done = nil
	if !reflect.DeepEqual(&tt, &task) {
		t.Errorf("%v vs %v", tt, task)
	}
}

func TestCompactionTaskGetReturnAllExpectedValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	p.addTask(task{shardID: 2, replicaID: 2, index: 10})
	if p.len() != 2 {
		t.Errorf("len unexpectedly reported %d", p.len())
	}
	task1, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	task2, ok := p.getTask()
	if !ok {
		t.Errorf("ok flag unexpected")
	}
	if task1.index != 3 && task1.index != 10 {
		t.Errorf("unexpected task obj")
	}
	if task2.index != 3 && task2.index != 10 {
		t.Errorf("unexpected task obj")
	}
	_, ok = p.getTask()
	if ok {
		t.Errorf("unexpected ok flag value")
	}
}

func TestMovingCompactionIndexBackWillCausePanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("not panic")
		}
	}()
	p := newCompactions()
	p.addTask(task{shardID: 1, replicaID: 2, index: 3})
	p.addTask(task{shardID: 1, replicaID: 2, index: 2})
}
