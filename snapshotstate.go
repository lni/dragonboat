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
	pb "github.com/lni/dragonboat/raftpb"
)

type getSink func() pb.IChunkSink

type snapshotRecord struct {
	mu        sync.Mutex
	getSinkFn getSink
	record    rsm.Commit
	hasRecord bool
}

func (sr *snapshotRecord) setStreamRecord(rec rsm.Commit,
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

func (sr *snapshotRecord) getStreamRecord() (rsm.Commit, getSink, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	hasRecord := sr.hasRecord
	sr.hasRecord = false
	return sr.record, sr.getSinkFn, hasRecord
}

func (sr *snapshotRecord) setRecord(rec rsm.Commit) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.hasRecord {
		plog.Panicf("setting snapshot record again %+v\n%+v", sr.record, rec)
	}
	sr.hasRecord = true
	sr.record = rec
}

func (sr *snapshotRecord) getRecord() (rsm.Commit, bool) {
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

func (rs *snapshotState) getCompactLogTo() uint64 {
	return atomic.SwapUint64(&rs.compactLogTo, 0)
}

func (rs *snapshotState) setCompactLogTo(v uint64) {
	atomic.StoreUint64(&rs.compactLogTo, v)
}

func (rs *snapshotState) setStreamSnapshotReq(rec rsm.Commit,
	getSinkFn getSink) {
	rs.streamSnapshotReady.setStreamRecord(rec, getSinkFn)
}

func (rs *snapshotState) setRecoverFromSnapshotReq(rec rsm.Commit) {
	rs.recoverReady.setRecord(rec)
}

func (rs *snapshotState) getRecoverFromSnapshotReq() (rsm.Commit, bool) {
	return rs.recoverReady.getRecord()
}

func (rs *snapshotState) setSaveSnapshotReq(rec rsm.Commit) {
	rs.saveSnapshotReady.setRecord(rec)
}

func (rs *snapshotState) getSaveSnapshotReq() (rsm.Commit, bool) {
	return rs.saveSnapshotReady.getRecord()
}

func (rs *snapshotState) getStreamSnapshotReq() (rsm.Commit, getSink, bool) {
	r, ok := rs.streamSnapshotReady.getRecord()
	if !ok {
		return rsm.Commit{}, nil, false
	}
	return r, rs.streamSnapshotReady.getSinkFn, true
}

func (rs *snapshotState) notifySnapshotStatus(saveSnapshot bool,
	recoverFromSnapshot bool, streamSnapshot bool,
	initialSnapshot bool, index uint64) {
	count := 0
	if saveSnapshot {
		count += 1
	}
	if recoverFromSnapshot {
		count += 1
	}
	if streamSnapshot {
		count += 1
	}
	if count != 1 {
		plog.Panicf("invalid request, save %t, recover %t, stream %t",
			saveSnapshot, recoverFromSnapshot, streamSnapshot)
	}
	rec := rsm.Commit{
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

func (rs *snapshotState) getStreamSnapshotCompleted() (rsm.Commit, bool) {
	return rs.streamSnapshotCompleted.getRecord()
}

func (rs *snapshotState) getRecoverCompleted() (rsm.Commit, bool) {
	return rs.recoverCompleted.getRecord()
}

func (rs *snapshotState) getSaveSnapshotCompleted() (rsm.Commit, bool) {
	return rs.saveSnapshotCompleted.getRecord()
}
