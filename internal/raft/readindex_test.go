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

package raft

import (
	"testing"

	"github.com/lni/dragonboat/v4/raftpb"
)

func getTestSystemCtx(v uint64) raftpb.SystemCtx {
	return raftpb.SystemCtx{
		Low:  v,
		High: v + 1,
	}
}

func TestSameCtxCanNotBeAddedTwice(t *testing.T) {
	r := newReadIndex()
	r.addRequest(1, getTestSystemCtx(10001), 1)
	if len(r.pending) != 1 {
		t.Errorf("unexpected pending count %d", len(r.pending))
	}
	r.addRequest(2, getTestSystemCtx(10001), 2)
	if len(r.pending) != 1 {
		t.Errorf("added twice")
	}
}

func TestInconsistentPendingQueue(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r := newReadIndex()
	r.addRequest(1, getTestSystemCtx(10001), 1)
	r.queue = append(r.queue, getTestSystemCtx(10003))
	r.addRequest(2, getTestSystemCtx(10002), 2)
}

func TestReadIndexRequestCanBeAdded(t *testing.T) {
	r := newReadIndex()
	r.addRequest(1, getTestSystemCtx(10001), 1)
	r.addRequest(2, getTestSystemCtx(10002), 2)
	if !r.hasPendingRequest() {
		t.Errorf("hasPendingRequest %t, want true", r.hasPendingRequest())
	}
	if len(r.queue) != 2 || len(r.pending) != 2 {
		t.Errorf("request not recorded")
	}
	p, ok := r.pending[getTestSystemCtx(10002)]
	if !ok {
		t.Errorf("pending request 2 not recorded")
	}
	if p.index != 2 {
		t.Errorf("index %d, want 2", p.index)
	}
	if p.from != 2 {
		t.Errorf("from %d, want 2", p.from)
	}
	if p.ctx != getTestSystemCtx(10002) {
		t.Errorf("ctx %v, want 10002", p.ctx)
	}
	ctx := r.peepCtx()
	if ctx != getTestSystemCtx(10002) {
		t.Errorf("ctx %v, want 10002", ctx)
	}
}

func TestReadIndexChecksInputIndex(t *testing.T) {
	defer func() {
		checked := false
		if r := recover(); r != nil {
			checked = true
		}
		if !checked {
			t.Error("did not check input index")
		}
	}()

	r := newReadIndex()
	ctx := getTestSystemCtx(10001)
	ctx2 := getTestSystemCtx(10002)
	ctx3 := getTestSystemCtx(10003)
	r.addRequest(3, ctx, 1)
	r.addRequest(5, ctx2, 3)
	r.addRequest(4, ctx3, 2)
}

func TestAddConfirmationChecksInconsistentPendingQueue(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r := newReadIndex()
	ctx := getTestSystemCtx(10001)
	ctx2 := getTestSystemCtx(10002)
	ctx3 := getTestSystemCtx(10003)
	r.addRequest(3, ctx2, 1)
	r.addRequest(4, ctx, 3)
	r.addRequest(5, ctx3, 2)
	q := r.queue
	r.queue = append([]raftpb.SystemCtx{}, getTestSystemCtx(10004))
	r.queue = append(r.queue, q...)
	r.confirm(ctx, 1, 3)
	r.confirm(ctx, 3, 3)
}

func TestReadIndexLeaderCanBeConfirmed(t *testing.T) {
	r := newReadIndex()
	ctx := getTestSystemCtx(10001)
	ctx2 := getTestSystemCtx(10002)
	ctx3 := getTestSystemCtx(10003)
	r.addRequest(3, ctx2, 1)
	r.addRequest(4, ctx, 3)
	r.addRequest(5, ctx3, 2)
	ris := r.confirm(ctx, 1, 3)
	if ris != nil {
		t.Errorf("ris confirmed too early")
	}
	ris = r.confirm(ctx, 3, 3)
	if len(ris) != 2 {
		t.Fatalf("failed to confirm leader in ReadIndex")
	}
	if ris[1].index != 4 {
		t.Errorf("index %d, want 4", ris[1].index)
	}
	if ris[1].from != 3 {
		t.Errorf("from %d, want 3", ris[1].from)
	}
	if ris[1].ctx != ctx {
		t.Errorf("ctx %v, want %v", ris[1].ctx, ctx)
	}
	if ris[0].index != 4 {
		t.Errorf("index %d, want 4", ris[0].index)
	}
	if ris[0].from != 1 {
		t.Errorf("from %d, want 1", ris[0].from)
	}
	if ris[0].ctx != getTestSystemCtx(10002) {
		t.Errorf("ctx %d, want %v", ris[0].ctx, getTestSystemCtx(10002))
	}
	if len(r.pending) != 1 || len(r.queue) != 1 {
		t.Errorf("cofirmed requet not removed")
	}
}

func TestReadIndexIsResetAfterRaftStateChange(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewTestLogDB())
	r.readIndex.addRequest(3, getTestSystemCtx(10001), 1)
	if len(r.readIndex.queue) != 1 || len(r.readIndex.pending) != 1 {
		t.Errorf("add request failed")
	}
	r.reset(2, true)
	if len(r.readIndex.queue) != 0 || len(r.readIndex.pending) != 0 {
		t.Errorf("readIndex not reset after raft reset")
	}
}
