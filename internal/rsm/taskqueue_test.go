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

package rsm

import (
	"reflect"
	"testing"
)

func TestTaskQueueCanBeCreated(t *testing.T) {
	tq := NewTaskQueue()
	if sz := tq.Size(); sz != 0 {
		t.Errorf("unexpected len %d", sz)
	}
	if _, ok := tq.Get(); ok {
		t.Errorf("unexpectedly returned item from tq")
	}
}

func TestAddToTaskQueue(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	if sz := tq.Size(); sz != 3 {
		t.Errorf("unexpected len %d", sz)
	}
	if !reflect.DeepEqual(&t1, &(tq.tasks[0])) ||
		!reflect.DeepEqual(&t2, &(tq.tasks[1])) ||
		!reflect.DeepEqual(&t3, &(tq.tasks[2])) {
		t.Errorf("value changed")
	}
}

func TestGetCanReturnAddedTaskFromTaskQueue(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	if v1, ok := tq.Get(); !ok || !reflect.DeepEqual(&t1, &v1) {
		t.Errorf("unexpected result")
	}
	if v2, ok := tq.Get(); !ok || !reflect.DeepEqual(&t2, &v2) {
		t.Errorf("unexpected result")
	}
	if sz := tq.Size(); sz != 1 {
		t.Errorf("unexpected size %d", sz)
	}
	if v3, ok := tq.Get(); !ok || !reflect.DeepEqual(&t3, &v3) {
		t.Errorf("unexpected result")
	}
}

func TestTaskQueueResize(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < initialTaskQueueCap*3; i++ {
		task := Task{ShardID: i}
		tq.Add(task)
	}
	if initCap := uint64(cap(tq.tasks)); initCap < initialTaskQueueCap*3 {
		t.Errorf("unexpected init cap")
	}
	for {
		if _, ok := tq.Get(); !ok {
			break
		}
	}
	if tq.Size() != 0 {
		t.Errorf("unexpected size %d", tq.Size())
	}
	if curCap := uint64(cap(tq.tasks)); curCap > initialTaskQueueCap {
		t.Errorf("not resized")
	}
}

func TestMoreEntryToApply(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < taskQueueBusyCap-1; i++ {
		task := Task{ShardID: i}
		tq.Add(task)
		if !tq.MoreEntryToApply() {
			t.Errorf("unexpectedly reported no more entry to apply")
		}
	}
	task := Task{ShardID: 0}
	tq.Add(task)
	if tq.MoreEntryToApply() {
		t.Errorf("entry not limited")
	}
}

func TestTaskQueueGetAll(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	all := tq.GetAll()
	if len(all) != 0 {
		t.Errorf("unexpected results")
	}
	tq.Add(t1)
	all = tq.GetAll()
	if len(all) != 1 {
		t.Fatalf("unexpected results")
	}
	if !reflect.DeepEqual(&t1, &all[0]) {
		t.Fatalf("unexpected task")
	}
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	all = tq.GetAll()
	if len(all) != 3 {
		t.Fatalf("unexpected results")
	}
	if !reflect.DeepEqual(&t1, &all[0]) ||
		!reflect.DeepEqual(&t2, &all[1]) ||
		!reflect.DeepEqual(&t3, &all[2]) {
		t.Errorf("unexpected results")
	}
}
