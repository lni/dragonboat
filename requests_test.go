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

package dragonboat

import (
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/rsm"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/random"
)

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
	if err := p.request(2); err != ErrPendingLeaderTransferExist {
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
	r3 := &RequestState{node: &node{}}
	r3.node.setInitialized()
	fn(r1)
	fn(r2)
	fn(r3)
	r4 := &RequestState{node: &node{}}
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 10)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
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

func TestTooSmallSnapshotTimeoutIsRejected(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 0)
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
	}
	ss, err = ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != ErrPendingSnapshotRequestExist {
		t.Errorf("request not rejected")
	}
	if ss != nil {
		t.Errorf("returned ss is not nil")
	}
}

func TestPendingSnapshotCanBeGCed(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 20)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
	}
	if ps.pending == nil {
		t.Errorf("pending not set")
	}
	for i := 0; i < 21; i++ {
		ps.tick()
		ps.gc()
		if ps.pending == nil {
			t.Errorf("pending cleared")
		}
	}
	ps.tick()
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != nil {
		t.Errorf("failed to request snapshot")
	}
	if ss == nil {
		t.Errorf("nil ss returned")
	}
	if ps.pending == nil {
		t.Errorf("pending not set")
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
	ss, err := ps.request(rsm.UserRequestedSnapshot, "", false, 0, 100)
	if err != ErrClusterClosed {
		t.Errorf("not report as closed")
	}
	if ss != nil {
		t.Errorf("snapshot state returned")
	}
}

func TestCompactionOverheadDetailsIsRecorded(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	_, err := ps.request(rsm.UserRequestedSnapshot, "", true, 123, 100)
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

func getPendingConfigChange(notifyCommit bool) (*pendingConfigChange,
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

func TestReleasingNotReadyRequestStateWillPanic(t *testing.T) {
	rs := RequestState{
		key:         100,
		clientID:    200,
		seriesID:    300,
		respondedTo: 400,
		deadline:    500,
		node:        &node{},
		pool:        &sync.Pool{},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("no panic")
		}
	}()
	rs.Release()
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
	if err != ErrPendingConfigChangeExist {
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

func TestConfigChangeCanExpire(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	tickCount := uint64(100)
	rs, err := pcc.request(cc, tickCount)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	for i := uint64(0); i < tickCount; i++ {
		pcc.tick()
		pcc.gc()
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to has anything at this stage")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pcc.tick()
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

func getPendingProposal(notifyCommit bool) (*pendingProposal, *entryQueue) {
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
	cfg := config.Config{ClusterID: 100, NodeID: 120}
	return newPendingProposal(cfg, notifyCommit, p, c, "nodehost:12345"), c
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

func countPendingProposal(p *pendingProposal) int {
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
	if err != ErrClusterClosed {
		t.Errorf("unexpected err %v", err)
	}
}

func TestProposalCanBeCompleted(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	// can't use ResultC() here, as it is basically testing the internal mechanism
	// of ResultC()
	case v := <-rs.CompletedC:
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
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, tickCount)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	for i := uint64(0); i < tickCount; i++ {
		pp.tick()
		pp.gc()
	}
	select {
	case <-rs.ResultC():
		t.Errorf("not suppose to return anything")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pp.tick()
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
		_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, 100)
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
	rs, _ := pp.propose(session, nil, nil, 100)
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

func getPendingSCRead() (*pendingReadIndex, *readIndexQueue) {
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

func TestPendingSCReadCanBeCreatedAndClosed(t *testing.T) {
	pp, c := getPendingSCRead()
	if len(c.get()) > 0 {
		t.Errorf("unexpected content")
	}
	pp.close()
	if !c.stopped {
		t.Errorf("not closed")
	}
}

func TestPendingSCReadCanRead(t *testing.T) {
	pp, c := getPendingSCRead()
	rs, err := pp.read(nil, 100)
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
	if len(pp.pending) != 0 {
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

func TestPendingSCReadCanComplete(t *testing.T) {
	pp, _ := getPendingSCRead()
	rs, err := pp.read(nil, 100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReadyToRead([]pb.ReadyToRead{readState})
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
	if len(pp.mapping) != 0 {
		t.Errorf("leaking records")
	}
	if len(pp.batches) == 0 {
		t.Errorf("batches is not suppose to be empty")
	}
}

func TestPendingReadIndexCanBeDropped(t *testing.T) {
	pp, _ := getPendingSCRead()
	rs, err := pp.read(nil, 100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	pp.dropped(s)
	select {
	case v := <-rs.ResultC():
		if !v.Dropped() {
			t.Errorf("got %v, want %d", v, requestDropped)
		}
	default:
		t.Errorf("expect to complete")
	}
	if len(pp.pending) > 0 || len(pp.batches) > 0 || len(pp.mapping) > 0 {
		t.Errorf("not cleared")
	}
}

func TestPendingSCReadCanExpire(t *testing.T) {
	pp, _ := getPendingSCRead()
	rs, err := pp.read(nil, 100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReadyToRead([]pb.ReadyToRead{readState})
	tickToWait := 100 + defaultGCTick + 1
	for i := uint64(0); i < tickToWait; i++ {
		pp.tick()
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
	if len(pp.pending) != 0 || len(pp.mapping) != 0 {
		t.Errorf("leaking records")
	}
}

func TestPendingSCReadCanExpireWithoutCallingAddReadyToRead(t *testing.T) {
	pp, _ := getPendingSCRead()
	rs, err := pp.read(nil, 100)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	tickToWait := 100 + defaultGCTick + 1
	for i := uint64(0); i < tickToWait; i++ {
		pp.tick()
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
	if len(pp.pending) != 0 || len(pp.mapping) != 0 {
		t.Errorf("leaking records")
	}
}

func TestExpiredSystemGcWillBeCollected(t *testing.T) {
	pp, _ := getPendingSCRead()
	if len(pp.systemGcTime) != 0 {
		t.Fatalf("systemGcTime is not empty")
	}
	expireTick := uint64(1000)
	for i := uint64(0); i < expireTick+1; i++ {
		pp.nextCtx()
		pp.tick()
	}
	if uint64(len(pp.systemGcTime)) != expireTick+1 {
		t.Errorf("unexpected system gc time length")
	}
	et := pp.systemGcTime[1].expireTime
	ctx := pp.systemGcTime[1].ctx
	now := pp.getTick()
	pp.gc(now)
	if uint64(len(pp.systemGcTime)) != expireTick {
		t.Errorf("unexpected system gc time length")
	}
	if pp.systemGcTime[0].expireTime != et ||
		pp.systemGcTime[0].ctx != ctx {
		t.Errorf("unexpected systemGcTime rec")
	}
}

func TestSystemGcTimeInSCReadCanBeCleanedUp(t *testing.T) {
	pp, _ := getPendingSCRead()
	for i := 0; i < 100000; i++ {
		pp.nextCtx()
	}
	if len(pp.systemGcTime) < 100000 {
		t.Errorf("len(pp.systemGcTime)=%d, want >100000", len(pp.systemGcTime))
	}
	for i := 0; i < 100000; i++ {
		pp.tick()
		pp.applied(499)
	}
	pp.applied(499)
	if len(pp.systemGcTime) != 0 {
		t.Errorf("not cleaning up systemGcTime")
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
	total := uint64(0)
	q := newEntryQueue(2048, 0)
	cfg := config.Config{ClusterID: 1, NodeID: 1}
	pp := newPendingProposal(cfg, false, p, q, "localhost:9090")
	session := client.NewNoOPSession(1, random.LockGuardedRand)
	ac := testing.AllocsPerRun(1000, func() {
		v := atomic.AddUint64(&total, 1)
		rs, err := pp.propose(session, data, nil, 100)
		if err != nil {
			t.Errorf("%v", err)
		}
		if v%128 == 0 {
			atomic.StoreUint64(&total, 0)
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
	total := uint64(0)
	q := newReadIndexQueue(2048)
	pri := newPendingReadIndex(p, q)
	ac := testing.AllocsPerRun(1000, func() {
		v := atomic.AddUint64(&total, 1)
		rs, err := pri.read(nil, 100)
		if err != nil {
			t.Errorf("%v", err)
		}
		if v%128 == 0 {
			atomic.StoreUint64(&total, 0)
			q.get()
		}
		rs.readyToRelease.set()
		rs.Release()
	})
	if ac != 0 {
		t.Fatalf("ac %f, want 0", ac)
	}
}
