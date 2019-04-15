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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/client"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	testTickInMillisecond uint64 = 50
)

func getPendingConfigChange() (*pendingConfigChange, chan *RequestState) {
	c := make(chan *RequestState, 1)
	return newPendingConfigChange(c, testTickInMillisecond), c
}

func TestRequestStateRelease(t *testing.T) {
	rs := RequestState{
		data:        make([]byte, 10),
		key:         100,
		clientID:    200,
		seriesID:    300,
		respondedTo: 400,
		deadline:    500,
		pool:        &sync.Pool{},
	}
	exp := RequestState{pool: rs.pool}
	rs.Release()
	if !reflect.DeepEqual(&exp, &rs) {
		t.Errorf("unexpected state, got %+v, want %+v", rs, exp)
	}
}

func TestPendingConfigChangeCanBeCreatedAndClosed(t *testing.T) {
	pcc, c := getPendingConfigChange()
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
	pcc, c := getPendingConfigChange()
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, time.Second)
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
	_, err = pcc.request(cc, time.Second)
	if err == nil {
		t.Errorf("not expect to be success")
	}
	if err != ErrPendingConfigChangeExist {
		t.Errorf("expected ErrSystemBusy, %v", err)
	}
	pcc.close()
	select {
	case v := <-rs.CompletedC:
		if !v.Terminated() {
			t.Errorf("returned %d, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("expect to return something")
	}
}

func TestConfigChangeCanExpire(t *testing.T) {
	pcc, _ := getPendingConfigChange()
	var cc pb.ConfigChange
	timeout := time.Duration(1000 * time.Millisecond)
	tickCount := uint64(1000 / testTickInMillisecond)
	rs, err := pcc.request(cc, timeout)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	for i := uint64(0); i < tickCount; i++ {
		pcc.increaseTick()
		pcc.gc()
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to has anything at this stage")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pcc.increaseTick()
		pcc.gc()
	}
	select {
	case v, ok := <-rs.CompletedC:
		if ok {
			if !v.Timeout() {
				t.Errorf("v: %d, expect %d", v, requestTimeout)
			}
		}
	default:
		t.Errorf("expect to be expired")
	}
}

func TestCompletedConfigChangeRequestCanBeNotified(t *testing.T) {
	pcc, _ := getPendingConfigChange()
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, time.Second)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.apply(rs.key, false)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("returned %d, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("suppose to return something")
	}
}

func TestConfigChangeRequestCanNotBeNotifiedWithDifferentKey(t *testing.T) {
	pcc, _ := getPendingConfigChange()
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, time.Second)
	if err != nil {
		t.Errorf("RequestConfigChange failed: %v", err)
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to return anything yet")
	default:
	}
	pcc.apply(rs.key+1, false)
	select {
	case <-rs.CompletedC:
		t.Errorf("unexpectedly notified")
	default:
	}
}

//
// pending proposal
//

func getPendingProposal() (*pendingProposal, *entryQueue) {
	c := newEntryQueue(5, 0)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.pool = p
		obj.CompletedC = make(chan RequestResult, 1)
		return obj
	}
	return newPendingProposal(p,
		c, 100, 120, "nodehost:12345", testTickInMillisecond), c
}

func getBlankTestSession() *client.Session {
	return &client.Session{}
}

func TestPendingProposalCanBeCreatedAndClosed(t *testing.T) {
	pp, c := getPendingProposal()
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

func TestProposalCanBeProposed(t *testing.T) {
	pp, c := getPendingProposal()
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	if countPendingProposal(pp) != 1 {
		t.Errorf("len(pending)=%d, want 1", countPendingProposal(pp))
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to have anything completed")
	default:
	}
	q := c.get(false)
	if len(q) != 1 {
		t.Errorf("len(c)=%d, want 1", len(q))
	}
	pp.close()
	select {
	case v := <-rs.CompletedC:
		if !v.Terminated() {
			t.Errorf("get %d, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("suppose to return terminated")
	}
}

func TestProposeOnClosedPendingProposalReturnError(t *testing.T) {
	pp, _ := getPendingProposal()
	pp.close()
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
	if err != ErrClusterClosed {
		t.Errorf("unexpected err %v", err)
	}
}

func TestProposalCanBeCompleted(t *testing.T) {
	pp, _ := getPendingProposal()
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key+1, sm.Result{}, false)
	select {
	case <-rs.CompletedC:
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("get %d, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalResultCanBeObtainedByCaller(t *testing.T) {
	pp, _ := getPendingProposal()
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
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
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("get %d, want %d", v, requestCompleted)
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
	pp, _ := getPendingProposal()
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID+1, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case <-rs.CompletedC:
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("get %d, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestSeriesIDIsCheckedWhenApplyingProposal(t *testing.T) {
	pp, _ := getPendingProposal()
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.clientID, rs.seriesID+1, rs.key, sm.Result{}, false)
	select {
	case <-rs.CompletedC:
		t.Errorf("unexpected applied proposal with invalid client ID")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("get %d, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalCanBeExpired(t *testing.T) {
	pp, _ := getPendingProposal()
	timeout := time.Duration(1000 * time.Millisecond)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, timeout)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	tickCount := uint64(1000 / testTickInMillisecond)
	for i := uint64(0); i < tickCount; i++ {
		pp.increaseTick()
		pp.gc()
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to return anything")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pp.increaseTick()
		pp.gc()
	}
	select {
	case v := <-rs.CompletedC:
		if !v.Timeout() {
			t.Errorf("got %d, want %d", v, requestTimeout)
		}
	default:
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending/keys is not empty")
	}
}

func TestProposalErrorsAreReported(t *testing.T) {
	pp, c := getPendingProposal()
	for i := 0; i < 5; i++ {
		_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
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
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), nil, time.Second)
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
	pp, c = getPendingProposal()
	var buffer bytes.Buffer
	for i := uint64(0); i < maxProposalPayloadSize; i++ {
		buffer.WriteString("a")
	}
	data := buffer.Bytes()
	_, err = pp.propose(getBlankTestSession(), data, nil, time.Second)
	if err != nil {
		t.Errorf("suppose to be successful")
	}
	buffer.WriteString("a")
	data = buffer.Bytes()
	if c.leftInWrite {
		cq = c.left
	} else {
		cq = c.right
	}
	sz = len(cq)
	_, err = pp.propose(getBlankTestSession(), data, nil, time.Second)
	if err != ErrPayloadTooBig {
		t.Errorf("suppose to return ErrPayloadTooBig")
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
	pp, _ := getPendingProposal()
	session := &client.Session{
		ClientID:    100,
		SeriesID:    200,
		RespondedTo: 199,
	}
	rs, _ := pp.propose(session, nil, nil, time.Duration(5*time.Second))
	select {
	case <-rs.CompletedC:
		t.Fatalf("completedC is already signalled")
	default:
	}
	for i := uint64(0); i < pp.ps; i++ {
		pp.shards[i].stopped = true
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{Value: 1}, false)
	select {
	case <-rs.CompletedC:
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
	return newPendingReadIndex(p, q, testTickInMillisecond), q
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
	rs, err := pp.read(nil, time.Second)
	if err != nil {
		t.Errorf("failed to do read")
	}
	select {
	case <-rs.CompletedC:
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
	case v := <-rs.CompletedC:
		if !v.Terminated() {
			t.Errorf("got %d, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("not expected to be signaled")
	}
}

func TestPendingSCReadCanComplete(t *testing.T) {
	pp, _ := getPendingSCRead()
	rs, err := pp.read(nil, time.Second)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReadyToRead([]pb.ReadyToRead{readState})
	pp.applied(499)
	select {
	case <-rs.CompletedC:
		t.Errorf("not expected to be signaled")
	default:
	}
	pp.applied(500)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("got %d, want %d", v, requestCompleted)
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

func TestPendingSCReadCanExpire(t *testing.T) {
	pp, _ := getPendingSCRead()
	timeout := time.Duration(1000 * time.Millisecond)
	rs, err := pp.read(nil, timeout)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReadyToRead([]pb.ReadyToRead{readState})
	tickToWait := 1000/testTickInMillisecond + defaultGCTick + 1
	for i := uint64(0); i < tickToWait; i++ {
		pp.increaseTick()
		pp.applied(499)
	}
	select {
	case v := <-rs.CompletedC:
		if !v.Timeout() {
			t.Errorf("got %d, want %d", v, requestTimeout)
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
	timeout := time.Duration(1000 * time.Millisecond)
	rs, err := pp.read(nil, timeout)
	if err != nil {
		t.Errorf("failed to do read")
	}
	s := pp.peepNextCtx()
	pp.addPendingRead(s, []*RequestState{rs})
	tickToWait := 1000/testTickInMillisecond + defaultGCTick + 1
	for i := uint64(0); i < tickToWait; i++ {
		pp.increaseTick()
		pp.applied(499)
	}
	select {
	case v := <-rs.CompletedC:
		if !v.Timeout() {
			t.Errorf("got %d, want %d", v, requestTimeout)
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
	expireTick := sysGcMillisecond / pp.tickInMillisecond
	for i := uint64(0); i < expireTick+1; i++ {
		pp.nextCtx()
		pp.increaseTick()
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
		pp.increaseTick()
		pp.applied(499)
	}
	pp.applied(499)
	if len(pp.systemGcTime) != 0 {
		t.Errorf("not cleaning up systemGcTime")
	}
}
