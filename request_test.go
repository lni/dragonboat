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

package dragonboat

import (
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/rsm"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/goutils/random"
)

func TestIsTempError(t *testing.T) {
	tests := []struct {
		err  error
		temp bool
	}{
		{ErrInvalidOperation, false},
		{ErrInvalidAddress, false},
		{ErrInvalidSession, false},
		{ErrTimeoutTooSmall, false},
		{ErrPayloadTooBig, false},
		{ErrSystemBusy, true},
		{ErrShardClosed, true},
		{ErrShardNotInitialized, true},
		{ErrTimeout, true},
		{ErrCanceled, false},
		{ErrRejected, false},
		{ErrAborted, true},
		{ErrShardNotReady, true},
		{ErrInvalidTarget, false},
		{ErrInvalidRange, false},
	}
	for idx, tt := range tests {
		if tmp := IsTempError(tt.err); tmp != tt.temp {
			t.Errorf("%d, IsTempError failed", idx)
		}
	}
}

func TestRequestCodeName(t *testing.T) {
	code := requestTimeout
	if code.String() != "RequestTimeout" {
		t.Errorf("unexpected request code name")
	}
}

func TestRequestStateCommitted(t *testing.T) {
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("no panic")
			}
		}()
		rs := &RequestState{}
		rs.committed()
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("no panic")
			}
		}()
		rs := &RequestState{notifyCommit: true}
		rs.committed()
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("no panic")
			}
		}()
		rs := &RequestState{
			notifyCommit: true,
			committedC:   make(chan RequestResult, 1),
		}
		rs.committed()
		rs.committed()
	}()
	rs := &RequestState{
		notifyCommit: true,
		committedC:   make(chan RequestResult, 1),
	}
	rs.committed()
	select {
	case cc := <-rs.committedC:
		if cc.code != requestCommitted {
			t.Errorf("not requestedCommitted")
		}
	default:
		t.Errorf("nothing in the committedC")
	}
}

func TestRequestStateReuse(t *testing.T) {
	rs := &RequestState{}
	rs.reuse(false)
	if rs.CompletedC == nil || cap(rs.CompletedC) != 1 {
		t.Errorf("completedC not ready")
	}
	if rs.committedC != nil {
		t.Errorf("committedC unexpectedly created")
	}
	rs = &RequestState{}
	rs.reuse(true)
	if rs.committedC == nil || cap(rs.committedC) != 1 {
		t.Errorf("committedC not ready")
	}
}

func TestResultCReturnsCompleteCWhenNotifyCommitNotSet(t *testing.T) {
	rs := &RequestState{
		CompletedC: make(chan RequestResult),
	}
	if rs.ResultC() != rs.CompletedC {
		t.Errorf("chan not equal")
	}
}

func TestResultCPanicWhenCommittedCIsNil(t *testing.T) {
	rs := &RequestState{
		CompletedC:   make(chan RequestResult),
		notifyCommit: true,
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("no panic")
		}
	}()
	rs.ResultC()
}

func TestResultCCanReceiveRequestResults(t *testing.T) {
	tests := []struct {
		notifyCommit        bool
		hasCommittedCResult bool
		completedCCode      RequestResultCode
		crash               bool
	}{
		{false, false, requestTimeout, false},
		{false, false, requestCompleted, false},
		{false, false, requestTerminated, false},
		{false, false, requestRejected, false},
		{false, false, requestDropped, false},
		{false, false, requestAborted, false},
		{false, false, requestCommitted, false},

		{true, false, requestTimeout, false},
		{true, false, requestCompleted, false},
		{true, false, requestTerminated, false},
		{true, false, requestRejected, false},
		{true, false, requestDropped, false},
		{true, false, requestAborted, true},
		{true, false, requestCommitted, true},

		{true, true, requestTimeout, false},
		{true, true, requestCompleted, false},
		{true, true, requestTerminated, false},
		{true, true, requestRejected, false},
		{true, true, requestDropped, true},
		{true, true, requestAborted, true},
		{true, true, requestCommitted, true},
	}
	for idx, tt := range tests {
		func() {
			rs := &RequestState{
				CompletedC:   make(chan RequestResult, 1),
				notifyCommit: tt.notifyCommit,
			}
			if tt.crash {
				rs.testErr = make(chan struct{})
			}
			if tt.notifyCommit {
				rs.committedC = make(chan RequestResult, 1)
			}
			if tt.hasCommittedCResult {
				rs.committed()
			}
			rs.notify(RequestResult{code: tt.completedCCode})
			ch := rs.ResultC()
			crashed := false
			if tt.hasCommittedCResult {
				select {
				case cc := <-ch:
					if cc.code != requestCommitted {
						t.Errorf("%d, not the expected committed result", idx)
					}
				case <-rs.testErr:
					crashed = true
					cc := <-ch
					if cc.code != requestCommitted {
						t.Errorf("%d, not the expected committed result", idx)
					}
				}
			}
			select {
			case cc := <-ch:
				if cc.code != tt.completedCCode {
					t.Errorf("%d, unexpected completedC value, got %s, want %s",
						idx, cc.code, tt.completedCCode)
				}
				if rs.testErr != nil {
					<-rs.testErr
					crashed = true
				}
			case <-rs.testErr:
				crashed = true
			}
			if tt.crash && !crashed {
				t.Errorf("%d, didn't crash", idx)
			}
			if ch != rs.ResultC() {
				t.Errorf("%d, ch changed", idx)
			}
		}()
	}
}

func TestPendingLeaderTransferCanBeCreated(t *testing.T) {
	p := newPendingLeaderTransfer()
	if len(p.leaderTransferC) != 0 || p.leaderTransferC == nil {
		t.Errorf("leaderTransferC not ready")
	}
}

func TestLeaderTransferCanBeRequested(t *testing.T) {
	p := newPendingLeaderTransfer()
	if err := p.request(1); err != nil {
		t.Errorf("failed to request leadership transfer %v", err)
	}
	if len(p.leaderTransferC) != 1 {
		t.Errorf("leader transfer not requested")
	}
}

func TestInvalidLeaderTransferIsNotAllowed(t *testing.T) {
	p := newPendingLeaderTransfer()
	if err := p.request(0); err != ErrInvalidTarget {
		t.Errorf("failed to reject invalid target node id")
	}
	if err := p.request(1); err != nil {
		t.Errorf("failed to request %v", err)
	}
	if err := p.request(2); err != ErrSystemBusy {
		t.Errorf("failed to reject")
	}
}

func TestCanGetExitingLeaderTransferRequest(t *testing.T) {
	p := newPendingLeaderTransfer()
	_, ok := p.get()
	if ok {
		t.Errorf("unexpectedly returned request")
	}
	if err := p.request(1); err != nil {
		t.Errorf("failed to request leadership transfer %v", err)
	}
	v, ok := p.get()
	if !ok || v != 1 {
		t.Errorf("failed to get request")
	}
	v, ok = p.get()
	if ok || v != 0 {
		t.Errorf("unexpectedly returned request")
	}
}

func TestRequestStatePanicWhenNotReadyForRead(t *testing.T) {
	fn := func(rs *RequestState) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("failed to trigger panic")
			}
		}()
		rs.mustBeReadyForLocalRead()
	}
	r1 := &RequestState{}
	r2 := &RequestState{node: &node{}}
	r3 := &RequestState{node: &node{initializedC: make(chan struct{})}}
	r3.node.setInitialized()
	fn(r1)
	fn(r2)
	fn(r3)
	r4 := &RequestState{node: &node{initializedC: make(chan struct{})}}
	r4.node.setInitialized()
	r4.readyToRead.set()
	r4.mustBeReadyForLocalRead()
}

func TestPendingSnapshotCanBeCreatedAndClosed(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	if len(ps.snapshotC) != 0 {
		t.Errorf("snapshotC not empty")
	}
	if ps.pending != nil {
		t.Errorf("pending not nil")
	}
	pending := &RequestState{
		CompletedC: make(chan RequestResult, 1),
	}
	ps.pending = pending
	ps.close()
	if ps.pending != nil {
		t.Errorf("pending not cleared")
	}
	select {
	case v := <-pending.ResultC():
		if !v.Terminated() {
			t.Errorf("unexpected code")
		}
	default:
		t.Errorf("close() didn't set pending to terminated")
	}
}

func TestPendingSnapshotCanBeRequested(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 10)
	if err != nil {
		t.Fatalf("failed to request snapshot")
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
		return
	}
	if ps.pending == nil {
		t.Errorf("pending not set")
	}
	if ss.deadline <= ps.getTick() {
		t.Errorf("deadline not set")
	}
	select {
	case <-snapshotC:
	default:
		t.Errorf("requested snapshot is not pushed")
	}
}

func TestPendingSnapshotCanReturnBusy(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	if _, err := ps.request(rsm.UserRequested, "", false, 0, 0, 10); err != nil {
		t.Errorf("failed to request snapshot")
	}
	if _, err := ps.request(rsm.UserRequested, "", false, 0, 0, 10); err != ErrSystemBusy {
		t.Errorf("failed to return ErrSystemBusy")
	}
}

func TestTooSmallSnapshotTimeoutIsRejected(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 0)
	if err != ErrTimeoutTooSmall {
		t.Errorf("request not rejected")
	}
	if ss != nil {
		t.Errorf("returned ss is not nil")
	}
}

func TestMultiplePendingSnapshotIsNotAllowed(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != nil {
		t.Fatalf("failed to request snapshot")
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
		return
	}
	ss, err = ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != ErrSystemBusy {
		t.Errorf("request not rejected")
	}
	if ss != nil {
		t.Errorf("returned ss is not nil")
	}
}

func TestPendingSnapshotCanBeGCed(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 20)
	if err != nil {
		t.Fatalf("failed to request snapshot")
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
	}
	if ps.pending == nil {
		t.Errorf("pending not set")
	}
	for i := uint64(1); i < 22; i++ {
		ps.tick(i)
		ps.gc()
		if ps.pending == nil {
			t.Errorf("pending cleared")
		}
	}
	ps.tick(uint64(22))
	ps.gc()
	if ps.pending != nil {
		t.Errorf("pending is not cleared")
	}
	select {
	case v := <-ss.ResultC():
		if !v.Timeout() {
			t.Errorf("not timeout")
		}
	default:
		t.Errorf("not notify as timed out")
	}
}

func TestPendingSnapshotCanBeApplied(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != nil {
		t.Fatalf("failed to request snapshot")
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
		return
	}
	ps.apply(ss.key, false, false, 123)
	select {
	case v := <-ss.ResultC():
		if v.SnapshotIndex() != 123 {
			t.Errorf("index value not returned")
		}
		if !v.Completed() {
			t.Errorf("not completed")
		}
	default:
		t.Errorf("ss is not applied")
	}
}

func TestPendingSnapshotCanBeIgnored(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != nil {
		t.Fatalf("failed to request snapshot")
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
		return
	}
	ps.apply(ss.key, true, false, 123)
	select {
	case v := <-ss.ResultC():
		if v.SnapshotIndex() != 0 {
			t.Errorf("index value incorrectly set")
		}
		if !v.Rejected() {
			t.Errorf("not rejected")
		}
	default:
		t.Errorf("ss is not applied")
	}
}

func TestPendingSnapshotIsIdentifiedByTheKey(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != nil {
		t.Fatalf("failed to request snapshot")
		return
	}
	if ss == nil {
		t.Fatalf("nil ss returned")
		return
	}
	if ps.pending == nil {
		t.Fatalf("pending not set")
	}
	ps.apply(ss.key+1, false, false, 123)
	if ps.pending == nil {
		t.Errorf("pending unexpectedly cleared")
	}
	select {
	case <-ss.ResultC():
		t.Fatalf("unexpectedly notified")
	default:
	}
}

func TestSnapshotCanNotBeRequestedAfterClose(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ps.close()
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	if err != ErrShardClosed {
		t.Errorf("not report as closed")
	}
	if ss != nil {
		t.Errorf("snapshot state returned")
	}
}

func TestCompactionOverheadDetailsIsRecorded(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	_, err := ps.request(rsm.UserRequested, "", true, 123, 0, 100)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	select {
	case req := <-snapshotC:
		if !req.OverrideCompaction || req.CompactionOverhead != 123 {
			t.Errorf("compaction details not recorded")
		}
	default:
		t.Errorf("snapshot request not available")
	}
}

func getPendingConfigChange(notifyCommit bool) (pendingConfigChange,
	chan configChangeRequest) {
	c := make(chan configChangeRequest, 1)
	return newPendingConfigChange(c, notifyCommit), c
}

func TestRequestStateRelease(t *testing.T) {
	rs := RequestState{
		key:         100,
		clientID:    200,
		seriesID:    300,
		respondedTo: 400,
		deadline:    500,
		node:        &node{},
		pool:        &sync.Pool{},
	}
	rs.readyToRead.set()
	rs.readyToRelease.set()
	exp := RequestState{pool: rs.pool}
	rs.Release()
	if !reflect.DeepEqual(&exp, &rs) {
		t.Errorf("unexpected state, got %+v, want %+v", rs, exp)
	}
}

func TestRequestStateSetToReadyToReleaseOnceNotified(t *testing.T) {
	rs := RequestState{
		CompletedC: make(chan RequestResult, 1),
	}
	if rs.readyToRelease.ready() {
		t.Errorf("already ready?")
	}
	rs.notify(RequestResult{})
	if !rs.readyToRelease.ready() {
		t.Errorf("failed to set ready to release to ready")
	}
}

func TestReleasingNotReadyRequestStateWillBeIgnored(t *testing.T) {
	rs := RequestState{
		key:         100,
		clientID:    200,
		seriesID:    300,
		respondedTo: 400,
		deadline:    500,
		node:        &node{},
		pool:        &sync.Pool{},
	}
	rs.Release()
	if rs.key != 100 || rs.deadline != 500 {
		t.Fatalf("unexpectedly released")
	}
}

func TestPendingConfigChangeCanBeCreatedAndClosed(t *testing.T) {
	pcc, c := getPendingConfigChange(false)
	select {
	case <-c:
		t.Errorf("unexpected content in confChangeC")
	default:
	}
	pcc.close()
	select {
	case _, ok := <-c:
		if ok {
			t.Errorf("suppose to be closed")
		}
	default:
		t.Errorf("missing closed signal")
	}
}

func TestCanNotMakeRequestOnClosedPendingConfigChange(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	pcc.close()
	if _, err := pcc.request(pb.ConfigChange{}, 100); err != ErrShardClosed {
		t.Errorf("failed to return ErrShardClosed, %v", err)
	}
}

func TestConfigChangeCanBeRequested(t *testing.T) {
	pcc, c := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	if rs == nil {
		t.Errorf("returned rs is nil")
	}
	if pcc.pending == nil {
		t.Errorf("request not internally recorded")
	}
	if len(c) != 1 {
		t.Errorf("len(c) = %d, want 1", len(c))
	}
	_, err = pcc.request(cc, 100)
	if err == nil {
		t.Errorf("not expect to be success")
	}
	if err != ErrSystemBusy {
		t.Errorf("expected ErrSystemBusy, %v", err)
	}
	pcc.close()
	select {
	case v := <-rs.ResultC():
		if !v.Terminated() {
			t.Errorf("returned %v, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("expect to return something")
	}
}

func TestPendingConfigChangeCanReturnBusy(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	if _, err := pcc.request(cc, 100); err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	if _, err := pcc.request(cc, 100); err != ErrSystemBusy {
		t.Errorf("failed to return busy: %v", err)
	}
}

func TestConfigChangeCanExpire(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	tickCount := uint64(100)
	rs, err := pcc.request(cc, tickCount)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	for i := uint64(0); i < tickCount; i++ {
		pcc.tick(i)
		pcc.gc()
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to has anything at this stage")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pcc.tick(i + tickCount)
		pcc.gc()
	}
	select {
	case v, ok := <-rs.ResultC():
		if ok {
			if !v.Timeout() {
				t.Errorf("v: %v, expect %d", v, requestTimeout)
			}
		}
	default:
		t.Errorf("expect to be expired")
	}
}

func TestCommittedConfigChangeRequestCanBeNotified(t *testing.T) {
	pcc, _ := getPendingConfigChange(true)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	pcc.committed(rs.key)
	select {
	case <-rs.committedC:
	default:
		t.Fatalf("committedC not signalled")
	}
}

func TestCompletedConfigChangeRequestCanBeNotified(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.apply(rs.key, false)
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("returned %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("suppose to return something")
	}
	if pcc.pending != nil {
		t.Errorf("pending rec not cleared")
	}
}

func TestConfigChangeRequestCanNotBeNotifiedWithDifferentKey(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.apply(rs.key+1, false)
	select {
	case <-rs.ResultC():
		t.Errorf("unexpectedly notified")
	default:
	}
	if pcc.pending == nil {
		t.Errorf("pending rec unexpectedly cleared")
	}
}

func TestConfigChangeCanBeDropped(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.dropped(rs.key)
	select {
	case v := <-rs.ResultC():
		if !v.Dropped() {
			t.Errorf("Dropped() is false")
		}
	default:
		t.Errorf("not dropped")
	}
	if pcc.pending != nil {
		t.Errorf("pending rec not cleared")
	}
}

func TestConfigChangeWithDifferentKeyWillNotBeDropped(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.dropped(rs.key + 1)
	select {
	case <-rs.ResultC():
		t.Errorf("CompletedC unexpectedly set")
	default:
	}
	if pcc.pending == nil {
		t.Errorf("pending rec unexpectedly cleared")
	}
}

//
// pending proposal
//

func getPendingProposal(notifyCommit bool) (pendingProposal, *entryQueue) {
	c := newEntryQueue(5, 0)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.pool = p
		obj.CompletedC = make(chan RequestResult, 1)
		if notifyCommit {
			obj.committedC = make(chan RequestResult, 1)
		}
		return obj
	}
	cfg := config.Config{ShardID: 100, ReplicaID: 120}
	return newPendingProposal(cfg, notifyCommit, p, c), c
}

func getBlankTestSession() *client.Session {
	return &client.Session{}
}

func TestPendingProposalCanBeCreatedAndClosed(t *testing.T) {
	pp, c := getPendingProposal(false)
	if len(c.get(false)) > 0 {
		t.Errorf("unexpected item in entry queue")
	}
	pp.close()
	if !c.stopped {
		t.Errorf("entry queue not closed")
	}
}

func countPendingProposal(p pendingProposal) int {
	total := 0
	for i := uint64(0); i < p.ps; i++ {
		total += len(p.shards[i].pending)
	}
	return total
}

// TODO:
// the test below uses at least 8GBs RAM, move it to a more suitable place
// and re-enable it

/*
func TestLargeProposalCanBeProposed(t *testing.T) {
	pp, _ := getPendingProposal()
	data := make([]byte, 8*1024*1024*1024)
	_, err := pp.propose(getBlankTestSession(), data, nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp, _ = getPendingProposal()
	for idx := range pp.shards {
		pp.shards[idx].cfg = config.Config{EntryCompressionType: config.Snappy}
	}
	data = make([]byte, 6*(0xffffffff-32)/7)
	_, err = pp.propose(getBlankTestSession(), data, nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	data = make([]byte, 6*(0xffffffff-32)/7+1)
	_, err = pp.propose(getBlankTestSession(), data, nil, time.Second)
	if err != ErrPayloadTooBig {
		t.Errorf("failed to return the expected error, %v", err)
	}
}*/

func TestProposalCanBeProposed(t *testing.T) {
	pp, c := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	if countPendingProposal(pp) != 1 {
		t.Errorf("len(pending)=%d, want 1", countPendingProposal(pp))
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to have anything completed")
	default:
	}
	q := c.get(false)
	if len(q) != 1 {
		t.Errorf("len(c)=%d, want 1", len(q))
	}
	pp.close()
	select {
	case v := <-rs.ResultC():
		if !v.Terminated() {
			t.Errorf("get %v, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("suppose to return terminated")
	}
}

func TestProposeOnClosedPendingProposalReturnError(t *testing.T) {
	pp, _ := getPendingProposal(false)
	pp.close()
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != ErrShardClosed {
		t.Errorf("unexpected err %v", err)
	}
}

func TestProposalCanBeCompleted(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key+1, sm.Result{}, false)
	select {
	case <-rs.ResultC():
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("get %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalCanBeDropped(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.dropped(rs.clientID, rs.seriesID, rs.key)
	select {
	case v := <-rs.ResultC():
		if !v.Dropped() {
			t.Errorf("not dropped")
		}
	default:
		t.Errorf("not notified")
	}
	for _, shard := range pp.shards {
		if len(shard.pending) > 0 {
			t.Errorf("pending request not cleared")
		}
	}
}

func TestProposalResultCanBeObtainedByCaller(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	result := sm.Result{
		Value: 1234,
		Data:  make([]byte, 128),
	}
	rand.Read(result.Data)
	pp.applied(rs.clientID, rs.seriesID, rs.key, result, false)
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("get %v, want %d", v, requestCompleted)
		}
		r := v.GetResult()
		if !reflect.DeepEqual(&r, &result) {
			t.Errorf("result changed")
		}
	default:
		t.Errorf("expect to get complete signal")
	}
}

func TestClientIDIsCheckedWhenApplyingProposal(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID+1, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case <-rs.ResultC():
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("get %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestSeriesIDIsCheckedWhenApplyingProposal(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID, rs.seriesID+1, rs.key, sm.Result{}, false)
	select {
	case <-rs.ResultC():
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("get %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalCanBeCommitted(t *testing.T) {
	pp, _ := getPendingProposal(true)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.committed(rs.clientID, rs.seriesID, rs.key)
	select {
	case <-rs.committedC:
	default:
		t.Errorf("not committed")
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.AppliedC():
		if !v.Completed() {
			t.Errorf("get %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalCanBeExpired(t *testing.T) {
	pp, _ := getPendingProposal(false)
	tickCount := uint64(100)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), tickCount)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	for i := uint64(0); i < tickCount; i++ {
		pp.tick(i)
		pp.gc()
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pp.tick(i + tickCount)
		pp.gc()
	}
	select {
	case v := <-rs.ResultC():
		if !v.Timeout() {
			t.Errorf("got %v, want %d", v, requestTimeout)
		}
	default:
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending/keys is not empty")
	}
}

func TestProposalErrorsAreReported(t *testing.T) {
	pp, c := getPendingProposal(false)
	for i := 0; i < 5; i++ {
		_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
		if err != nil {
			t.Errorf("propose failed")
		}
	}
	var cq []pb.Entry
	if c.leftInWrite {
		cq = c.left
	} else {
		cq = c.right
	}
	sz := len(cq)
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	if err != ErrSystemBusy {
		t.Errorf("suppose to return ErrSystemBusy")
	}
	if c.leftInWrite {
		cq = c.left
	} else {
		cq = c.right
	}
	if len(cq) != sz {
		t.Errorf("len(c)=%d, want %d", len(cq), sz)
	}
}

func TestClosePendingProposalIgnoresStepEngineActivities(t *testing.T) {
	pp, _ := getPendingProposal(false)
	session := &client.Session{
		ClientID:    100,
		SeriesID:    200,
		RespondedTo: 199,
	}
	rs, _ := pp.propose(session, nil, 100)
	select {
	case <-rs.ResultC():
		t.Fatalf("completedC is already signalled")
	default:
	}
	for i := uint64(0); i < pp.ps; i++ {
		pp.shards[i].stopped = true
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{Value: 1}, false)
	select {
	case <-rs.ResultC():
		t.Fatalf("completedC unexpectedly signaled")
	default:
	}
}

func getPendingReadIndex() (pendingReadIndex, *readIndexQueue) {
	q := newReadIndexQueue(5)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.pool = p
		obj.CompletedC = make(chan RequestResult, 1)
		return obj
	}
	return newPendingReadIndex(p, q), q
}

func TestPendingReadIndexCanBeCreatedAndClosed(t *testing.T) {
	pp, c := getPendingReadIndex()
	if len(c.get()) > 0 {
		t.Errorf("unexpected content")
	}
	pp.close()
	if !c.stopped {
		t.Errorf("not closed")
	}
}

func TestCanNotMakeRequestOnClosedPendingReadIndex(t *testing.T) {
	pp, _ := getPendingReadIndex()
	pp.close()
	if _, err := pp.read(100); err != ErrShardClosed {
		t.Errorf("failed to return ErrShardClosed %v", err)
	}
}

func TestPendingReadIndexCanRead(t *testing.T) {
	pp, c := getPendingReadIndex()
	rs, err := pp.read(100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything")
	default:
	}
	var q []*RequestState
	if c.leftInWrite {
		q = c.left[:c.idx]
	} else {
		q = c.right[:c.idx]
	}
	if len(q) != 1 {
		t.Errorf("read request not sent")
	}
	if pp.requests.pendingSize() != 1 {
		t.Errorf("req not recorded in temp")
	}
	if len(pp.batches) != 0 {
		t.Errorf("pending is expected to be empty")
	}
	pp.close()
	select {
	case v := <-rs.ResultC():
		if !v.Terminated() {
			t.Errorf("got %v, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("not expected to be signaled")
	}
}

func TestPendingReadIndexCanReturnBusy(t *testing.T) {
	pri, _ := getPendingReadIndex()
	for i := 0; i < 6; i++ {
		_, err := pri.read(100)
		if i != 5 && err != nil {
			t.Errorf("failed to do read")
		}
		if i == 5 && err != ErrSystemBusy {
			t.Errorf("failed to return ErrSystemBusy")
		}
	}
}

func TestPendingReadIndexCanComplete(t *testing.T) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReady([]pb.ReadyToRead{readState})
	pp.applied(499)
	select {
	case <-rs.ResultC():
		t.Errorf("not expected to be signaled")
	default:
	}
	if rs.readyToRead.ready() {
		t.Errorf("ready is already set")
	}
	pp.applied(500)
	if !rs.readyToRead.ready() {
		t.Errorf("ready not set")
	}
	select {
	case v := <-rs.ResultC():
		if !v.Completed() {
			t.Errorf("got %v, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to complete")
	}
	if len(pp.batches) != 0 {
		t.Errorf("leaking records")
	}
}

func TestPendingReadIndexCanBeDropped(t *testing.T) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	pp.dropped(s)
	select {
	case v := <-rs.ResultC():
		if !v.Dropped() {
			t.Errorf("got %v, want %d", v, requestDropped)
		}
	default:
		t.Errorf("expect to complete")
	}
	if len(pp.batches) > 0 {
		t.Errorf("not cleared")
	}
}

func testPendingReadIndexCanExpire(t *testing.T, addReady bool) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	if addReady {
		readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
		pp.addReady([]pb.ReadyToRead{readState})
	}
	tickToWait := 100 + defaultGCTick + 1
	for i := uint64(0); i < tickToWait; i++ {
		pp.tick(i)
		pp.applied(499)
	}
	select {
	case v := <-rs.ResultC():
		if !v.Timeout() {
			t.Errorf("got %v, want %d", v, requestTimeout)
		}
	default:
		t.Errorf("expect to complete")
	}
	if len(pp.batches) != 0 {
		t.Errorf("leaking records")
	}
}

func TestPendingReadIndexCanExpire(t *testing.T) {
	testPendingReadIndexCanExpire(t, true)
}

func TestPendingReadIndexCanExpireWithoutCallingAddReady(t *testing.T) {
	testPendingReadIndexCanExpire(t, false)
}

func TestNonEmptyReadBatchIsNeverExpired(t *testing.T) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(10000)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	if len(pp.batches) != 1 {
		t.Fatalf("unexpected batch count")
	}
	tickToWait := defaultGCTick * 10
	for i := uint64(0); i < tickToWait; i++ {
		pp.tick(i)
		pp.applied(499)
	}
	if len(pp.batches) == 0 {
		t.Fatalf("unexpectedly removed batch")
	}
}

func TestProposalAllocationCount(t *testing.T) {
	sz := 128
	data := make([]byte, sz)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint32(0)
	q := newEntryQueue(2048, 0)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	pp := newPendingProposal(cfg, false, p, q)
	session := client.NewNoOPSession(1, random.LockGuardedRand)
	ac := testing.AllocsPerRun(10000, func() {
		v := atomic.AddUint32(&total, 1)
		rs, err := pp.propose(session, data, 100)
		if err != nil {
			t.Errorf("%v", err)
		}
		if v%128 == 0 {
			atomic.StoreUint32(&total, 0)
			q.get(false)
		}
		pp.applied(rs.key, rs.clientID, rs.seriesID, sm.Result{Value: 1}, false)
		rs.readyToRelease.set()
		rs.Release()
	})
	if ac > 1 {
		t.Fatalf("ac %f, want <=1", ac)
	}
}

func TestReadIndexAllocationCount(t *testing.T) {
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	total := uint32(0)
	q := newReadIndexQueue(2048)
	pri := newPendingReadIndex(p, q)
	ac := testing.AllocsPerRun(10000, func() {
		v := atomic.AddUint32(&total, 1)
		rs, err := pri.read(100)
		if err != nil {
			t.Errorf("%v", err)
		}
		if v%128 == 0 {
			atomic.StoreUint32(&total, 0)
			q.get()
		}
		rs.readyToRelease.set()
		rs.Release()
	})
	if ac != 0 {
		t.Fatalf("ac %f, want 0", ac)
	}
}

func TestPendingRaftLogQueryCanBeCreated(t *testing.T) {
	p := newPendingRaftLogQuery()
	assert.Nil(t, p.mu.pending)
}

func TestPendingRaftLogQueryCanBeClosed(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	assert.NoError(t, err)
	assert.NotNil(t, p.mu.pending)
	p.close()
	assert.Nil(t, p.mu.pending)
	select {
	case v := <-rs.CompletedC:
		assert.True(t, v.Terminated())
	default:
		t.Fatalf("not terminated")
	}
}

func TestPendingRaftLogQueryCanAddRequest(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	assert.NotNil(t, rs)
	assert.NoError(t, err)
	rs, err = p.add(200, 200, 300)
	assert.Equal(t, ErrSystemBusy, err)
	assert.Nil(t, rs)
	assert.NotNil(t, p.mu.pending)
	assert.Equal(t, LogRange{FirstIndex: 100, LastIndex: 200}, p.mu.pending.logRange)
	assert.Equal(t, uint64(300), p.mu.pending.maxSize)
	assert.NotNil(t, p.mu.pending.CompletedC)
}

func TestPendingRaftLogQueryGet(t *testing.T) {
	p := newPendingRaftLogQuery()
	assert.Nil(t, p.get())
	rs, err := p.add(100, 200, 300)
	assert.NoError(t, err)
	assert.NotNil(t, p.mu.pending)
	result := p.get()
	assert.Equal(t, rs, result)
	assert.Equal(t, LogRange{FirstIndex: 100, LastIndex: 200}, result.logRange)
	assert.Equal(t, uint64(300), result.maxSize)
	assert.NotNil(t, result.CompletedC)
}

func TestPendingRaftLogQueryGetWhenReturnedIsCalledWithoutPendingRequest(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	p := newPendingRaftLogQuery()
	p.returned(false, LogRange{}, nil)
}

func TestPendingRaftLogQueryCanReturnOutOfRangeError(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	assert.NoError(t, err)
	lr := LogRange{FirstIndex: 150, LastIndex: 200}
	p.returned(true, lr, nil)
	select {
	case v := <-rs.CompletedC:
		assert.True(t, v.RequestOutOfRange())
		_, rrl := v.RaftLogs()
		assert.Equal(t, lr, rrl)
	default:
		t.Fatalf("no result available")
	}
}

func TestPendingRaftLogQueryCanReturnResults(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	assert.NoError(t, err)
	entries := []pb.Entry{{Index: 1}, {Index: 2}}
	lr := LogRange{FirstIndex: 100, LastIndex: 180}
	p.returned(false, lr, entries)
	select {
	case v := <-rs.CompletedC:
		assert.True(t, v.Completed())
		rentries, rrl := v.RaftLogs()
		assert.Equal(t, lr, rrl)
		assert.Equal(t, entries, rentries)
	default:
		t.Fatalf("no result available")
	}
}
