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
	"sync"

	"github.com/lni/dragonboat/v3/internal/settings"
)

var (
	initialTaskQueueCap uint64 = settings.Soft.TaskQueueInitialCap
	taskQueueBusyCap    uint64 = settings.Soft.TaskQueueTargetLength
	snapshotTaskCSlots  uint64 = 4
	emptyTask                  = Task{}
)

// TaskQueue is a queue of tasks to be processed by the state machine.
type TaskQueue struct {
	mu    sync.Mutex
	tasks []Task
	next  uint64
}

// NewTaskQueue creates and returns a new task queue.
func NewTaskQueue() *TaskQueue {
	tq := &TaskQueue{
		tasks: make([]Task, 0, initialTaskQueueCap),
	}
	return tq
}

// MoreEntryToApply returns a boolean value indicating whether it is ok to
// queue more entries to apply.
func (tq *TaskQueue) MoreEntryToApply() bool {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	sz := tq.size()
	return sz+snapshotTaskCSlots < taskQueueBusyCap
}

// Add adds a new task to the queue.
func (tq *TaskQueue) Add(task Task) {
	tq.mu.Lock()
	tq.tasks = append(tq.tasks, task)
	tq.mu.Unlock()
}

// Get returns a task from the queue if there is any.
func (tq *TaskQueue) Get() (Task, bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	if tq.next < uint64(len(tq.tasks)) {
		task := tq.tasks[tq.next]
		tq.tasks[tq.next] = emptyTask
		tq.next++
		tq.resize()
		return task, true
	}
	tq.resize()
	return emptyTask, false
}

// Size returns the number of queued tasks.
func (tq *TaskQueue) Size() uint64 {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	return tq.size()
}

func (tq *TaskQueue) size() uint64 {
	return uint64(len(tq.tasks)) - tq.next
}

func (tq *TaskQueue) resize() {
	if uint64(cap(tq.tasks)) > initialTaskQueueCap*2 {
		if tq.size() < initialTaskQueueCap {
			tasks := make([]Task, tq.size())
			copy(tasks, tq.tasks[tq.next:])
			tq.tasks = tasks
			tq.next = 0
		}
	}
}
