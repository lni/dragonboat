// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package dragonboat

import (
	"sync"
	"sync/atomic"

	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v3/internal/rsm"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type getSink func() pb.IChunkSink

type snapshotTask struct {
	mu        sync.Mutex
	getSinkFn getSink
	t         rsm.Task
	hasTask   bool
}

func (sr *snapshotTask) setStreamTask(t rsm.Task, getSinkFn getSink) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.hasTask {
		plog.Panicf("setting stream snapshot task again %+v\n%+v", sr.t, t)
	}
	sr.hasTask = true
	sr.t = t
	sr.getSinkFn = getSinkFn
}

func (sr *snapshotTask) setTask(t rsm.Task) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.hasTask {
		plog.Panicf("setting snapshot task again %+v\n%+v", sr.t, t)
	}
	sr.hasTask = true
	sr.t = t
}

func (sr *snapshotTask) getTask() (rsm.Task, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	hasTask := sr.hasTask
	sr.hasTask = false
	return sr.t, hasTask
}

type snapshotState struct {
	savingFlag              uint32
	recoveringFlag          uint32
	streamingFlag           uint32
	snapshotIndex           uint64
	reqSnapshotIndex        uint64
	compactLogTo            uint64
	compactedTo             uint64
	recoverReady            snapshotTask
	saveSnapshotReady       snapshotTask
	streamSnapshotReady     snapshotTask
	recoverCompleted        snapshotTask
	saveSnapshotCompleted   snapshotTask
	streamSnapshotCompleted snapshotTask
}

func (rs *snapshotState) recovering() bool {
	return atomic.LoadUint32(&rs.recoveringFlag) == 1
}

func (rs *snapshotState) setRecovering() {
	atomic.StoreUint32(&rs.recoveringFlag, 1)
}

func (rs *snapshotState) clearRecovering() {
	atomic.StoreUint32(&rs.recoveringFlag, 0)
}

func (rs *snapshotState) streaming() bool {
	return atomic.LoadUint32(&rs.streamingFlag) == 1
}

func (rs *snapshotState) setStreaming() {
	atomic.StoreUint32(&rs.streamingFlag, 1)
}

func (rs *snapshotState) clearStreaming() {
	atomic.StoreUint32(&rs.streamingFlag, 0)
}

func (rs *snapshotState) saving() bool {
	return atomic.LoadUint32(&rs.savingFlag) == 1
}

func (rs *snapshotState) setSaving() {
	atomic.StoreUint32(&rs.savingFlag, 1)
}

func (rs *snapshotState) clearSaving() {
	atomic.StoreUint32(&rs.savingFlag, 0)
}

func (rs *snapshotState) setIndex(index uint64) {
	atomic.StoreUint64(&rs.snapshotIndex, index)
}

func (rs *snapshotState) getIndex() uint64 {
	return atomic.LoadUint64(&rs.snapshotIndex)
}

func (rs *snapshotState) getReqIndex() uint64 {
	return atomic.LoadUint64(&rs.reqSnapshotIndex)
}

func (rs *snapshotState) setReqIndex(idx uint64) {
	atomic.StoreUint64(&rs.reqSnapshotIndex, idx)
}

func (rs *snapshotState) hasCompactLogTo() bool {
	return atomic.LoadUint64(&rs.compactLogTo) > 0
}

func (rs *snapshotState) getCompactLogTo() uint64 {
	return atomic.SwapUint64(&rs.compactLogTo, 0)
}

func (rs *snapshotState) setCompactLogTo(v uint64) {
	atomic.StoreUint64(&rs.compactLogTo, v)
}

func (rs *snapshotState) setCompactedTo(v uint64) {
	atomic.StoreUint64(&rs.compactedTo, v)
}

func (rs *snapshotState) getCompactedTo() uint64 {
	return atomic.SwapUint64(&rs.compactedTo, 0)
}

func (rs *snapshotState) hasCompactedTo() bool {
	return atomic.LoadUint64(&rs.compactedTo) > 0
}

func (rs *snapshotState) setStreamReq(t rsm.Task, getSinkFn getSink) {
	rs.streamSnapshotReady.setStreamTask(t, getSinkFn)
}

func (rs *snapshotState) setRecoverReq(t rsm.Task) {
	rs.recoverReady.setTask(t)
}

func (rs *snapshotState) getRecoverReq() (rsm.Task, bool) {
	return rs.recoverReady.getTask()
}

func (rs *snapshotState) setSaveReq(t rsm.Task) {
	rs.saveSnapshotReady.setTask(t)
}

func (rs *snapshotState) getSaveReq() (rsm.Task, bool) {
	return rs.saveSnapshotReady.getTask()
}

func (rs *snapshotState) getStreamReq() (rsm.Task, getSink, bool) {
	r, ok := rs.streamSnapshotReady.getTask()
	if !ok {
		return rsm.Task{}, nil, false
	}
	return r, rs.streamSnapshotReady.getSinkFn, true
}

func (rs *snapshotState) notifySnapshotStatus(saveSnapshot bool,
	recoverFromSnapshot bool, streamSnapshot bool,
	initialSnapshot bool, index uint64) {
	count := 0
	if saveSnapshot {
		count++
	}
	if recoverFromSnapshot {
		count++
	}
	if streamSnapshot {
		count++
	}
	if count != 1 {
		plog.Panicf("invalid request, save %t, recover %t, stream %t",
			saveSnapshot, recoverFromSnapshot, streamSnapshot)
	}
	t := rsm.Task{
		SnapshotRequested: saveSnapshot,
		SnapshotAvailable: recoverFromSnapshot,
		StreamSnapshot:    streamSnapshot,
		InitialSnapshot:   initialSnapshot,
		Index:             index,
	}
	if saveSnapshot {
		rs.saveSnapshotCompleted.setTask(t)
	} else if recoverFromSnapshot {
		rs.recoverCompleted.setTask(t)
	} else {
		rs.streamSnapshotCompleted.setTask(t)
	}
}

func (rs *snapshotState) getStreamCompleted() (rsm.Task, bool) {
	return rs.streamSnapshotCompleted.getTask()
}

func (rs *snapshotState) getRecoverCompleted() (rsm.Task, bool) {
	return rs.recoverCompleted.getTask()
}

func (rs *snapshotState) getSaveCompleted() (rsm.Task, bool) {
	return rs.saveSnapshotCompleted.getTask()
}

type task struct {
	intervalMs uint64
	lastRun    uint64
}

func newTask(interval uint64) *task {
	if interval == 0 {
		panic("invalid interval")
	}
	return &task{
		intervalMs: interval,
		lastRun:    random.LockGuardedRand.Uint64() % interval,
	}
}

func (t *task) timeToRun(tt uint64) bool {
	if tt-t.lastRun >= t.intervalMs {
		t.lastRun = tt
		return true
	}
	return false
}
