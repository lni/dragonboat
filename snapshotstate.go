// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/utils/random"
	pb "github.com/lni/dragonboat/raftpb"
)

type getSink func() pb.IChunkSink

type snapshotRecord struct {
	mu        sync.Mutex
	getSinkFn getSink
	record    rsm.Task
	hasRecord bool
}

func (sr *snapshotRecord) setStreamRecord(rec rsm.Task,
	getSinkFn getSink) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.hasRecord {
		return false
	}
	sr.hasRecord = true
	sr.record = rec
	sr.getSinkFn = getSinkFn
	return true
}

func (sr *snapshotRecord) getStreamRecord() (rsm.Task, getSink, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	hasRecord := sr.hasRecord
	sr.hasRecord = false
	return sr.record, sr.getSinkFn, hasRecord
}

func (sr *snapshotRecord) setRecord(rec rsm.Task) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.hasRecord {
		plog.Panicf("setting snapshot record again %+v\n%+v", sr.record, rec)
	}
	sr.hasRecord = true
	sr.record = rec
}

func (sr *snapshotRecord) getRecord() (rsm.Task, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	hasRecord := sr.hasRecord
	sr.hasRecord = false
	return sr.record, hasRecord
}

type snapshotState struct {
	takingSnapshotFlag         uint32
	recoveringFromSnapshotFlag uint32
	streamingSnapshotFlag      uint32
	snapshotIndex              uint64
	reqSnapshotIndex           uint64
	compactLogTo               uint64
	recoverReady               snapshotRecord
	saveSnapshotReady          snapshotRecord
	streamSnapshotReady        snapshotRecord
	recoverCompleted           snapshotRecord
	saveSnapshotCompleted      snapshotRecord
	streamSnapshotCompleted    snapshotRecord
}

func (rs *snapshotState) recoveringFromSnapshot() bool {
	return atomic.LoadUint32(&rs.recoveringFromSnapshotFlag) == 1
}

func (rs *snapshotState) setRecoveringFromSnapshot() {
	atomic.StoreUint32(&rs.recoveringFromSnapshotFlag, 1)
}

func (rs *snapshotState) clearRecoveringFromSnapshot() {
	atomic.StoreUint32(&rs.recoveringFromSnapshotFlag, 0)
}

func (rs *snapshotState) streamingSnapshot() bool {
	return atomic.LoadUint32(&rs.streamingSnapshotFlag) == 1
}

func (rs *snapshotState) setStreamingSnapshot() {
	atomic.StoreUint32(&rs.streamingSnapshotFlag, 1)
}

func (rs *snapshotState) clearStreamingSnapshot() {
	atomic.StoreUint32(&rs.streamingSnapshotFlag, 0)
}

func (rs *snapshotState) takingSnapshot() bool {
	return atomic.LoadUint32(&rs.takingSnapshotFlag) == 1
}

func (rs *snapshotState) setTakingSnapshot() {
	atomic.StoreUint32(&rs.takingSnapshotFlag, 1)
}

func (rs *snapshotState) clearTakingSnapshot() {
	atomic.StoreUint32(&rs.takingSnapshotFlag, 0)
}

func (rs *snapshotState) setSnapshotIndex(index uint64) {
	atomic.StoreUint64(&rs.snapshotIndex, index)
}

func (rs *snapshotState) getSnapshotIndex() uint64 {
	return atomic.LoadUint64(&rs.snapshotIndex)
}

func (rs *snapshotState) getReqSnapshotIndex() uint64 {
	return atomic.LoadUint64(&rs.reqSnapshotIndex)
}

func (rs *snapshotState) setReqSnapshotIndex(idx uint64) {
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

func (rs *snapshotState) setStreamSnapshotReq(rec rsm.Task,
	getSinkFn getSink) {
	rs.streamSnapshotReady.setStreamRecord(rec, getSinkFn)
}

func (rs *snapshotState) setRecoverFromSnapshotReq(rec rsm.Task) {
	rs.recoverReady.setRecord(rec)
}

func (rs *snapshotState) getRecoverFromSnapshotReq() (rsm.Task, bool) {
	return rs.recoverReady.getRecord()
}

func (rs *snapshotState) setSaveSnapshotReq(rec rsm.Task) {
	rs.saveSnapshotReady.setRecord(rec)
}

func (rs *snapshotState) getSaveSnapshotReq() (rsm.Task, bool) {
	return rs.saveSnapshotReady.getRecord()
}

func (rs *snapshotState) getStreamSnapshotReq() (rsm.Task, getSink, bool) {
	r, ok := rs.streamSnapshotReady.getRecord()
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
	rec := rsm.Task{
		SnapshotRequested: saveSnapshot,
		SnapshotAvailable: recoverFromSnapshot,
		StreamSnapshot:    streamSnapshot,
		InitialSnapshot:   initialSnapshot,
		Index:             index,
	}
	if saveSnapshot {
		rs.saveSnapshotCompleted.setRecord(rec)
	} else if recoverFromSnapshot {
		rs.recoverCompleted.setRecord(rec)
	} else {
		rs.streamSnapshotCompleted.setRecord(rec)
	}
}

func (rs *snapshotState) getStreamSnapshotCompleted() (rsm.Task, bool) {
	return rs.streamSnapshotCompleted.getRecord()
}

func (rs *snapshotState) getRecoverCompleted() (rsm.Task, bool) {
	return rs.recoverCompleted.getRecord()
}

func (rs *snapshotState) getSaveSnapshotCompleted() (rsm.Task, bool) {
	return rs.saveSnapshotCompleted.getRecord()
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
