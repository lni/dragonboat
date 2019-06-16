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

package rsm

import (
	"reflect"
	"testing"
)

func TestTaskQueueCanBeCreated(t *testing.T) {
	tq := NewTaskQueue()
	sz := tq.Size()
	if sz != 0 {
		t.Errorf("unexpected len %d", sz)
	}
	_, ok := tq.Get()
	if ok {
		t.Errorf("unexpectedly returned item from tq")
	}
}

func TestAddToTaskQueue(t *testing.T) {
	t1 := Task{ClusterID: 1}
	t2 := Task{ClusterID: 2}
	t3 := Task{ClusterID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	sz := tq.Size()
	if sz != 3 {
		t.Errorf("unexpected len %d", sz)
	}
	if !reflect.DeepEqual(&t1, &(tq.tasks[0])) ||
		!reflect.DeepEqual(&t2, &(tq.tasks[1])) ||
		!reflect.DeepEqual(&t3, &(tq.tasks[2])) {
		t.Errorf("value changed")
	}
}

func TestGetCanReturnAddedTaskFromTaskQueue(t *testing.T) {
	t1 := Task{ClusterID: 1}
	t2 := Task{ClusterID: 2}
	t3 := Task{ClusterID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	v1, ok := tq.Get()
	if !ok || !reflect.DeepEqual(&t1, &v1) {
		t.Errorf("unexpected result")
	}
	v2, ok := tq.Get()
	if !ok || !reflect.DeepEqual(&t2, &v2) {
		t.Errorf("unexpected result")
	}
	sz := tq.Size()
	if sz != 1 {
		t.Errorf("unexpected size %d", sz)
	}
	v3, ok := tq.Get()
	if !ok || !reflect.DeepEqual(&t3, &v3) {
		t.Errorf("unexpected result")
	}
}

func TestTaskQueueResize(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < initialTaskQueueCap*3; i++ {
		task := Task{ClusterID: i}
		tq.Add(task)
	}
	initCap := uint64(cap(tq.tasks))
	if initCap < initialTaskQueueCap*3 {
		t.Errorf("unexpected init cap")
	}
	for {
		_, ok := tq.Get()
		if !ok {
			break
		}
	}
	if tq.Size() != 0 {
		t.Errorf("unexpected size %d", tq.Size())
	}
	curCap := uint64(cap(tq.tasks))
	if curCap > initialTaskQueueCap {
		t.Errorf("not resized")
	}
}

func TestMoreEntryToApply(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < taskQueueBusyCap-snapshotTaskCSlots-1; i++ {
		task := Task{ClusterID: i}
		tq.Add(task)
		if !tq.MoreEntryToApply() {
			t.Errorf("unexpectedly reported no more entry to apply")
		}
	}
	task := Task{ClusterID: 0}
	tq.Add(task)
	if tq.MoreEntryToApply() {
		t.Errorf("entry not limited")
	}
}
