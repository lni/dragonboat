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
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		assert.Equal(t, tt.temp, IsTempError(tt.err), "test %d", idx)
	}
}

func TestRequestCodeName(t *testing.T) {
	code := requestTimeout
	assert.Equal(t, "RequestTimeout", code.String())
}

func TestRequestStateCommitted(t *testing.T) {
	require.Panics(t, func() {
		rs := &RequestState{}
		rs.committed()
	})

	require.Panics(t, func() {
		rs := &RequestState{notifyCommit: true}
		rs.committed()
	})

	require.Panics(t, func() {
		rs := &RequestState{
			notifyCommit: true,
			committedC:   make(chan RequestResult, 1),
		}
		rs.committed()
		rs.committed()
	})

	rs := &RequestState{
		notifyCommit: true,
		committedC:   make(chan RequestResult, 1),
	}
	rs.committed()
	select {
	case cc := <-rs.committedC:
		assert.Equal(t, requestCommitted, cc.code)
	default:
		assert.Fail(t, "nothing in the committedC")
	}
}

func TestRequestStateReuse(t *testing.T) {
	rs := &RequestState{}
	rs.reuse(false)
	require.NotNil(t, rs.CompletedC)
	assert.Equal(t, 1, cap(rs.CompletedC))
	assert.Nil(t, rs.committedC)

	rs = &RequestState{}
	rs.reuse(true)
	require.NotNil(t, rs.committedC)
	assert.Equal(t, 1, cap(rs.committedC))
}

func TestResultCReturnsCompleteCWhenNotifyCommitNotSet(t *testing.T) {
	rs := &RequestState{
		CompletedC: make(chan RequestResult),
	}
	assert.Equal(t, rs.CompletedC, rs.ResultC())
}

func TestResultCPanicWhenCommittedCIsNil(t *testing.T) {
	rs := &RequestState{
		CompletedC:   make(chan RequestResult),
		notifyCommit: true,
	}
	require.Panics(t, func() { rs.ResultC() })
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
					assert.Equal(t, requestCommitted, cc.code, "idx %d", idx)
				case <-rs.testErr:
					crashed = true
					cc := <-ch
					assert.Equal(t, requestCommitted, cc.code, "idx %d", idx)
				}
			}
			select {
			case cc := <-ch:
				assert.Equal(t, tt.completedCCode, cc.code, "idx %d", idx)
				if rs.testErr != nil {
					<-rs.testErr
					crashed = true
				}
			case <-rs.testErr:
				crashed = true
			}
			if tt.crash {
				assert.True(t, crashed, "idx %d", idx)
			}
			assert.Equal(t, ch, rs.ResultC(), "idx %d", idx)
		}()
	}
}

func TestPendingLeaderTransferCanBeCreated(t *testing.T) {
	p := newPendingLeaderTransfer()
	require.NotNil(t, p.leaderTransferC)
	assert.Empty(t, p.leaderTransferC)
}

func TestLeaderTransferCanBeRequested(t *testing.T) {
	p := newPendingLeaderTransfer()
	err := p.request(1)
	assert.NoError(t, err, "failed to request leadership transfer")
	assert.Len(t, p.leaderTransferC, 1)
}

func TestInvalidLeaderTransferIsNotAllowed(t *testing.T) {
	p := newPendingLeaderTransfer()
	err := p.request(0)
	assert.Equal(t, ErrInvalidTarget, err)

	err = p.request(1)
	assert.NoError(t, err)

	err = p.request(2)
	assert.Equal(t, ErrSystemBusy, err)
}

func TestCanGetExitingLeaderTransferRequest(t *testing.T) {
	p := newPendingLeaderTransfer()
	_, ok := p.get()
	assert.False(t, ok)

	err := p.request(1)
	assert.NoError(t, err, "failed to request leadership transfer")

	v, ok := p.get()
	assert.True(t, ok)
	assert.Equal(t, uint64(1), v)

	v, ok = p.get()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)
}

func TestRequestStatePanicWhenNotReadyForRead(t *testing.T) {
	fn := func(rs *RequestState) {
		require.Panics(t, func() {
			rs.mustBeReadyForLocalRead()
		})
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
	require.NotPanics(t, func() {
		r4.mustBeReadyForLocalRead()
	})
}

func TestPendingSnapshotCanBeCreatedAndClosed(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	assert.Empty(t, ps.snapshotC)
	assert.Nil(t, ps.pending)

	pending := &RequestState{
		CompletedC: make(chan RequestResult, 1),
	}
	ps.pending = pending
	ps.close()
	assert.Nil(t, ps.pending)
	select {
	case v := <-pending.ResultC():
		assert.True(t, v.Terminated())
	default:
		assert.Fail(t, "close() didn't set pending to terminated")
	}
}

func TestPendingSnapshotCanBeRequested(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 10)
	require.NoError(t, err)
	require.NotNil(t, ss)

	assert.NotNil(t, ps.pending)
	assert.True(t, ss.deadline > ps.getTick())
	select {
	case <-snapshotC:
	default:
		assert.Fail(t, "requested snapshot is not pushed")
	}
}

func TestPendingSnapshotCanReturnBusy(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	_, err := ps.request(rsm.UserRequested, "", false, 0, 0, 10)
	assert.NoError(t, err)
	_, err = ps.request(rsm.UserRequested, "", false, 0, 0, 10)
	assert.Equal(t, ErrSystemBusy, err)
}

func TestTooSmallSnapshotTimeoutIsRejected(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 0)
	assert.Equal(t, ErrTimeoutTooSmall, err)
	assert.Nil(t, ss)
}

func TestMultiplePendingSnapshotIsNotAllowed(t *testing.T) {
	snapshotC := make(chan<- rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	require.NoError(t, err)
	require.NotNil(t, ss)

	ss, err = ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	assert.Equal(t, ErrSystemBusy, err)
	assert.Nil(t, ss)
}

func TestPendingSnapshotCanBeGCed(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 20)
	require.NoError(t, err)
	require.NotNil(t, ss)
	assert.NotNil(t, ps.pending)

	for i := uint64(1); i < 22; i++ {
		ps.tick(i)
		ps.gc()
		assert.NotNil(t, ps.pending)
	}

	ps.tick(uint64(22))
	ps.gc()
	assert.Nil(t, ps.pending)
	select {
	case v := <-ss.ResultC():
		assert.True(t, v.Timeout())
	default:
		assert.Fail(t, "not notify as timed out")
	}
}

func TestPendingSnapshotCanBeApplied(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	require.NoError(t, err)
	require.NotNil(t, ss)

	ps.apply(ss.key, false, false, 123)
	select {
	case v := <-ss.ResultC():
		assert.Equal(t, uint64(123), v.SnapshotIndex())
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "ss is not applied")
	}
}

func TestPendingSnapshotCanBeIgnored(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	require.NoError(t, err)
	require.NotNil(t, ss)

	ps.apply(ss.key, true, false, 123)
	select {
	case v := <-ss.ResultC():
		assert.Equal(t, uint64(0), v.SnapshotIndex())
		assert.True(t, v.Rejected())
	default:
		assert.Fail(t, "ss is not applied")
	}
}

func TestPendingSnapshotIsIdentifiedByTheKey(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	require.NoError(t, err)
	require.NotNil(t, ss)
	require.NotNil(t, ps.pending)

	ps.apply(ss.key+1, false, false, 123)
	assert.NotNil(t, ps.pending)
	select {
	case <-ss.ResultC():
		assert.Fail(t, "unexpectedly notified")
	default:
	}
}

func TestSnapshotCanNotBeRequestedAfterClose(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	ps.close()
	ss, err := ps.request(rsm.UserRequested, "", false, 0, 0, 100)
	assert.Equal(t, ErrShardClosed, err)
	assert.Nil(t, ss)
}

func TestCompactionOverheadDetailsIsRecorded(t *testing.T) {
	snapshotC := make(chan rsm.SSRequest, 1)
	ps := newPendingSnapshot(snapshotC)
	_, err := ps.request(rsm.UserRequested, "", true, 123, 0, 100)
	assert.NoError(t, err)

	select {
	case req := <-snapshotC:
		assert.True(t, req.OverrideCompaction)
		assert.Equal(t, uint64(123), req.CompactionOverhead)
	default:
		assert.Fail(t, "snapshot request not available")
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
	assert.Equal(t, exp, rs)
}

func TestRequestStateSetToReadyToReleaseOnceNotified(t *testing.T) {
	rs := RequestState{
		CompletedC: make(chan RequestResult, 1),
	}
	assert.False(t, rs.readyToRelease.ready())
	rs.notify(RequestResult{})
	assert.True(t, rs.readyToRelease.ready())
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
	rsBefore := rs
	rs.Release()
	assert.Equal(t, rsBefore, rs)
}

func TestPendingConfigChangeCanBeCreatedAndClosed(t *testing.T) {
	pcc, c := getPendingConfigChange(false)
	select {
	case <-c:
		assert.Fail(t, "unexpected content in confChangeC")
	default:
	}

	pcc.close()
	select {
	case _, ok := <-c:
		assert.False(t, ok, "channel should be closed")
	default:
		assert.Fail(t, "missing closed signal")
	}
}

func TestCanNotMakeRequestOnClosedPendingConfigChange(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	pcc.close()
	_, err := pcc.request(pb.ConfigChange{}, 100)
	assert.Equal(t, ErrShardClosed, err)
}

func TestConfigChangeCanBeRequested(t *testing.T) {
	pcc, c := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)
	assert.NotNil(t, rs)
	assert.NotNil(t, pcc.pending)
	assert.Len(t, c, 1)

	_, err = pcc.request(cc, 100)
	assert.Error(t, err)
	assert.Equal(t, ErrSystemBusy, err)

	pcc.close()
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Terminated())
	default:
		assert.Fail(t, "expect to return something")
	}
}

func TestPendingConfigChangeCanReturnBusy(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	_, err := pcc.request(cc, 100)
	assert.NoError(t, err)

	_, err = pcc.request(cc, 100)
	assert.Equal(t, ErrSystemBusy, err)
}

func TestConfigChangeCanExpire(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	tickCount := uint64(100)
	rs, err := pcc.request(cc, tickCount)
	assert.NoError(t, err)

	for i := uint64(0); i < tickCount; i++ {
		pcc.tick(i)
		pcc.gc()
	}
	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to has anything at this stage")
	default:
	}

	for i := uint64(0); i < defaultGCTick+1; i++ {
		pcc.tick(i + tickCount)
		pcc.gc()
	}
	select {
	case v, ok := <-rs.ResultC():
		assert.True(t, ok)
		assert.True(t, v.Timeout())
	default:
		assert.Fail(t, "expect to be expired")
	}
}

func TestCommittedConfigChangeRequestCanBeNotified(t *testing.T) {
	pcc, _ := getPendingConfigChange(true)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)
	pcc.committed(rs.key)
	select {
	case <-rs.committedC:
	default:
		assert.Fail(t, "committedC not signalled")
	}
}

func TestCompletedConfigChangeRequestCanBeNotified(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything yet")
	default:
	}

	pcc.apply(rs.key, false)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "suppose to return something")
	}
	assert.Nil(t, pcc.pending)
}

func TestConfigChangeRequestCanNotBeNotifiedWithDifferentKey(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything yet")
	default:
	}

	pcc.apply(rs.key+1, false)
	select {
	case <-rs.ResultC():
		assert.Fail(t, "unexpectedly notified")
	default:
	}
	assert.NotNil(t, pcc.pending)
}

func TestConfigChangeCanBeDropped(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything yet")
	default:
	}

	pcc.dropped(rs.key)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Dropped())
	default:
		assert.Fail(t, "not dropped")
	}
	assert.Nil(t, pcc.pending)
}

func TestConfigChangeWithDifferentKeyWillNotBeDropped(t *testing.T) {
	pcc, _ := getPendingConfigChange(false)
	var cc pb.ConfigChange
	rs, err := pcc.request(cc, 100)
	assert.NoError(t, err)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything yet")
	default:
	}

	pcc.dropped(rs.key + 1)
	select {
	case <-rs.ResultC():
		assert.Fail(t, "CompletedC unexpectedly set")
	default:
	}
	assert.NotNil(t, pcc.pending)
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
	assert.Empty(t, c.get(false))
	pp.close()
	assert.True(t, c.stopped)
}

func countPendingProposal(p pendingProposal) int {
	total := 0
	for i := uint64(0); i < p.ps; i++ {
		total += len(p.shards[i].pending)
	}
	return total
}

func TestProposalCanBeProposed(t *testing.T) {
	pp, c := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)
	assert.Equal(t, 1, countPendingProposal(pp))

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to have anything completed")
	default:
	}

	q := c.get(false)
	assert.Len(t, q, 1)
	pp.close()

	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Terminated())
	default:
		assert.Fail(t, "suppose to return terminated")
	}
}

func TestProposeOnClosedPendingProposalReturnError(t *testing.T) {
	pp, _ := getPendingProposal(false)
	pp.close()
	_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	assert.Equal(t, ErrShardClosed, err)
}

func TestProposalCanBeCompleted(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)
	pp.applied(rs.clientID, rs.seriesID, rs.key+1, sm.Result{}, false)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "unexpected applied proposal with invalid client ID")
	default:
	}
	assert.NotZero(t, countPendingProposal(pp))

	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "expect to get complete signal")
	}
	assert.Zero(t, countPendingProposal(pp))
}

func TestProposalCanBeDropped(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)

	pp.dropped(rs.clientID, rs.seriesID, rs.key)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Dropped())
	default:
		assert.Fail(t, "not notified")
	}

	for _, shard := range pp.shards {
		assert.Empty(t, shard.pending)
	}
}

func TestProposalResultCanBeObtainedByCaller(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)

	result := sm.Result{
		Value: 1234,
		Data:  make([]byte, 128),
	}
	_, err = rand.Read(result.Data)
	require.NoError(t, err)
	pp.applied(rs.clientID, rs.seriesID, rs.key, result, false)

	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
		r := v.GetResult()
		assert.Equal(t, result, r)
	default:
		assert.Fail(t, "expect to get complete signal")
	}
}

func TestClientIDIsCheckedWhenApplyingProposal(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)

	pp.applied(rs.clientID+1, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case <-rs.ResultC():
		assert.Fail(t, "unexpected applied proposal with invalid client ID")
	default:
	}
	assert.NotZero(t, countPendingProposal(pp))

	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "expect to get complete signal")
	}
	assert.Zero(t, countPendingProposal(pp))
}

func TestSeriesIDIsCheckedWhenApplyingProposal(t *testing.T) {
	pp, _ := getPendingProposal(false)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)

	pp.applied(rs.clientID, rs.seriesID+1, rs.key, sm.Result{}, false)
	select {
	case <-rs.ResultC():
		assert.Fail(t, "unexpected applied proposal with invalid client ID")
	default:
	}
	assert.NotZero(t, countPendingProposal(pp))

	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "expect to get complete signal")
	}
	assert.Zero(t, countPendingProposal(pp))
}

func TestProposalCanBeCommitted(t *testing.T) {
	pp, _ := getPendingProposal(true)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	require.NoError(t, err)

	pp.committed(rs.clientID, rs.seriesID, rs.key)
	select {
	case <-rs.committedC:
	default:
		assert.Fail(t, "not committed")
	}
	assert.NotZero(t, countPendingProposal(pp))

	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{}, false)
	select {
	case v := <-rs.AppliedC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "expect to get complete signal")
	}
	assert.Zero(t, countPendingProposal(pp))
}

func TestProposalCanBeExpired(t *testing.T) {
	pp, _ := getPendingProposal(false)
	tickCount := uint64(100)
	rs, err := pp.propose(getBlankTestSession(), []byte("test data"), tickCount)
	require.NoError(t, err)

	for i := uint64(0); i < tickCount; i++ {
		pp.tick(i)
		pp.gc()
	}
	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything")
	default:
	}

	for i := uint64(0); i < defaultGCTick+1; i++ {
		pp.tick(i + tickCount)
		pp.gc()
	}
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Timeout())
	default:
		assert.Fail(t, "result channel is empty")
	}
	assert.Zero(t, countPendingProposal(pp))
}

func TestProposalErrorsAreReported(t *testing.T) {
	pp, c := getPendingProposal(false)
	for i := 0; i < 5; i++ {
		_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
		assert.NoError(t, err)
	}

	var cq []pb.Entry
	if c.leftInWrite {
		cq = c.left
	} else {
		cq = c.right
	}
	sz := len(cq)

	_, err := pp.propose(getBlankTestSession(), []byte("test data"), 100)
	assert.Equal(t, ErrSystemBusy, err)

	if c.leftInWrite {
		cq = c.left
	} else {
		cq = c.right
	}
	assert.Len(t, cq, sz)
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
		require.FailNow(t, "completedC is already signalled")
	default:
	}

	for i := uint64(0); i < pp.ps; i++ {
		pp.shards[i].stopped = true
	}
	pp.applied(rs.clientID, rs.seriesID, rs.key, sm.Result{Value: 1}, false)

	select {
	case <-rs.ResultC():
		require.FailNow(t, "completedC unexpectedly signaled")
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
	assert.Empty(t, c.get())
	pp.close()
	assert.True(t, c.stopped)
}

func TestCanNotMakeRequestOnClosedPendingReadIndex(t *testing.T) {
	pp, _ := getPendingReadIndex()
	pp.close()
	_, err := pp.read(100)
	assert.Equal(t, ErrShardClosed, err)
}

func TestPendingReadIndexCanRead(t *testing.T) {
	pp, c := getPendingReadIndex()
	rs, err := pp.read(100)
	require.NoError(t, err)

	select {
	case <-rs.ResultC():
		assert.Fail(t, "not suppose to return anything")
	default:
	}

	var q []*RequestState
	if c.leftInWrite {
		q = c.left[:c.idx]
	} else {
		q = c.right[:c.idx]
	}
	assert.Len(t, q, 1)
	assert.Equal(t, uint64(1), pp.requests.pendingSize())
	assert.Empty(t, pp.batches)

	pp.close()
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Terminated())
	default:
		assert.Fail(t, "not expected to be signaled")
	}
}

func TestPendingReadIndexCanReturnBusy(t *testing.T) {
	pri, _ := getPendingReadIndex()
	for i := 0; i < 6; i++ {
		_, err := pri.read(100)
		if i < 5 {
			assert.NoError(t, err)
		} else {
			assert.Equal(t, ErrSystemBusy, err)
		}
	}
}

func TestPendingReadIndexCanComplete(t *testing.T) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	require.NoError(t, err)

	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	readState := pb.ReadyToRead{Index: 500, SystemCtx: s}
	pp.addReady([]pb.ReadyToRead{readState})
	pp.applied(499)
	select {
	case <-rs.ResultC():
		assert.Fail(t, "not expected to be signaled")
	default:
	}
	assert.False(t, rs.readyToRead.ready())

	pp.applied(500)
	assert.True(t, rs.readyToRead.ready())
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Completed())
	default:
		assert.Fail(t, "expect to complete")
	}
	assert.Empty(t, pp.batches)
}

func TestPendingReadIndexCanBeDropped(t *testing.T) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	require.NoError(t, err)

	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	pp.dropped(s)
	select {
	case v := <-rs.ResultC():
		assert.True(t, v.Dropped())
	default:
		assert.Fail(t, "expect to complete")
	}
	assert.Empty(t, pp.batches)
}

func testPendingReadIndexCanExpire(t *testing.T, addReady bool) {
	pp, _ := getPendingReadIndex()
	rs, err := pp.read(100)
	require.NoError(t, err)

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
		assert.True(t, v.Timeout())
	default:
		assert.Fail(t, "expect to complete")
	}
	assert.Empty(t, pp.batches)
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
	require.NoError(t, err)

	s := pp.nextCtx()
	pp.add(s, []*RequestState{rs})
	require.Len(t, pp.batches, 1)

	tickToWait := defaultGCTick * 10
	for i := uint64(0); i < tickToWait; i++ {
		pp.tick(i)
		pp.applied(499)
	}
	assert.Len(t, pp.batches, 1)
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
		require.NoError(t, err)
		if v%128 == 0 {
			atomic.StoreUint32(&total, 0)
			q.get(false)
		}
		pp.applied(rs.key, rs.clientID, rs.seriesID, sm.Result{Value: 1}, false)
		rs.readyToRelease.set()
		rs.Release()
	})
	assert.LessOrEqual(t, ac, float64(1))
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
		require.NoError(t, err)
		if v%128 == 0 {
			atomic.StoreUint32(&total, 0)
			q.get()
		}
		rs.readyToRelease.set()
		rs.Release()
	})
	assert.Equal(t, float64(0), ac)
}

func TestPendingRaftLogQueryCanBeCreated(t *testing.T) {
	p := newPendingRaftLogQuery()
	assert.Nil(t, p.mu.pending)
}

func TestPendingRaftLogQueryCanBeClosed(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	require.NoError(t, err)
	require.NotNil(t, p.mu.pending)
	p.close()
	assert.Nil(t, p.mu.pending)
	select {
	case v := <-rs.CompletedC:
		assert.True(t, v.Terminated())
	default:
		assert.Fail(t, "not terminated")
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
	expectedRange := LogRange{FirstIndex: 100, LastIndex: 200}
	assert.Equal(t, expectedRange, p.mu.pending.logRange)
	assert.Equal(t, uint64(300), p.mu.pending.maxSize)
	assert.NotNil(t, p.mu.pending.CompletedC)
}

func TestPendingRaftLogQueryGet(t *testing.T) {
	p := newPendingRaftLogQuery()
	assert.Nil(t, p.get())

	rs, err := p.add(100, 200, 300)
	require.NoError(t, err)
	require.NotNil(t, p.mu.pending)

	result := p.get()
	assert.Equal(t, rs, result)
	expectedRange := LogRange{FirstIndex: 100, LastIndex: 200}
	assert.Equal(t, expectedRange, result.logRange)
	assert.Equal(t, uint64(300), result.maxSize)
	assert.NotNil(t, result.CompletedC)
}

func TestPendingRaftLogQueryGetWhenReturnedIsCalledWithoutPendingRequest(
	t *testing.T,
) {
	require.Panics(t, func() {
		p := newPendingRaftLogQuery()
		p.returned(false, LogRange{}, nil)
	})
}

func TestPendingRaftLogQueryCanReturnOutOfRangeError(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	require.NoError(t, err)
	lr := LogRange{FirstIndex: 150, LastIndex: 200}
	p.returned(true, lr, nil)
	select {
	case v := <-rs.CompletedC:
		assert.True(t, v.RequestOutOfRange())
		_, rrl := v.RaftLogs()
		assert.Equal(t, lr, rrl)
	default:
		assert.Fail(t, "no result available")
	}
}

func TestPendingRaftLogQueryCanReturnResults(t *testing.T) {
	p := newPendingRaftLogQuery()
	rs, err := p.add(100, 200, 300)
	require.NoError(t, err)
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
		assert.Fail(t, "no result available")
	}
}
