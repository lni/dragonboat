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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskQueueCanBeCreated(t *testing.T) {
	tq := NewTaskQueue()
	sz := tq.Size()
	assert.Equal(t, uint64(0), sz, "unexpected len")
	_, ok := tq.Get()
	assert.False(t, ok, "unexpectedly returned item from tq")
}

func TestAddToTaskQueue(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	sz := tq.Size()
	assert.Equal(t, uint64(3), sz, "unexpected len")
	assert.True(t, reflect.DeepEqual(&t1, &(tq.tasks[0])),
		"value changed")
	assert.True(t, reflect.DeepEqual(&t2, &(tq.tasks[1])),
		"value changed")
	assert.True(t, reflect.DeepEqual(&t3, &(tq.tasks[2])),
		"value changed")
}

func TestGetCanReturnAddedTaskFromTaskQueue(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	v1, ok := tq.Get()
	require.True(t, ok, "unexpected result")
	assert.True(t, reflect.DeepEqual(&t1, &v1), "unexpected result")
	v2, ok := tq.Get()
	require.True(t, ok, "unexpected result")
	assert.True(t, reflect.DeepEqual(&t2, &v2), "unexpected result")
	sz := tq.Size()
	assert.Equal(t, uint64(1), sz, "unexpected size")
	v3, ok := tq.Get()
	require.True(t, ok, "unexpected result")
	assert.True(t, reflect.DeepEqual(&t3, &v3), "unexpected result")
}

func TestTaskQueueResize(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < initialTaskQueueCap*3; i++ {
		task := Task{ShardID: i}
		tq.Add(task)
	}
	initCap := uint64(cap(tq.tasks))
	assert.GreaterOrEqual(t, initCap, initialTaskQueueCap*3,
		"unexpected init cap")
	for {
		if _, ok := tq.Get(); !ok {
			break
		}
	}
	sz := tq.Size()
	assert.Equal(t, uint64(0), sz, "unexpected size")
	curCap := uint64(cap(tq.tasks))
	assert.LessOrEqual(t, curCap, initialTaskQueueCap, "not resized")
}

func TestMoreEntryToApply(t *testing.T) {
	tq := NewTaskQueue()
	for i := uint64(0); i < taskQueueBusyCap-1; i++ {
		task := Task{ShardID: i}
		tq.Add(task)
		more := tq.MoreEntryToApply()
		assert.True(t, more, "unexpectedly reported no more entry to apply")
	}
	task := Task{ShardID: 0}
	tq.Add(task)
	more := tq.MoreEntryToApply()
	assert.False(t, more, "entry not limited")
}

func TestTaskQueueGetAll(t *testing.T) {
	t1 := Task{ShardID: 1}
	t2 := Task{ShardID: 2}
	t3 := Task{ShardID: 3}
	tq := NewTaskQueue()
	all := tq.GetAll()
	assert.Len(t, all, 0, "unexpected results")
	tq.Add(t1)
	all = tq.GetAll()
	require.Len(t, all, 1, "unexpected results")
	assert.True(t, reflect.DeepEqual(&t1, &all[0]), "unexpected task")
	tq.Add(t1)
	tq.Add(t2)
	tq.Add(t3)
	all = tq.GetAll()
	require.Len(t, all, 3, "unexpected results")
	assert.True(t, reflect.DeepEqual(&t1, &all[0]), "unexpected results")
	assert.True(t, reflect.DeepEqual(&t2, &all[1]), "unexpected results")
	assert.True(t, reflect.DeepEqual(&t3, &all[2]), "unexpected results")
}
