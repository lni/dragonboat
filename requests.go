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
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/logger"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/random"
)

const (
	badKeyCheck bool = false
)

var (
	defaultGCTick         uint64 = 2
	pendingProposalShards        = settings.Soft.PendingProposalShards
)

var (
	plog = logger.GetLogger("dragonboat")
)

var (
	// ErrInvalidOperation indicates that the requested operation is not allowed.
	// e.g. making read or write requests on witness node are not allowed.
	ErrInvalidOperation = errors.New("invalid operation")
	// ErrInvalidAddress indicates that the specified address is invalid.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrInvalidSession indicates that the specified client session is invalid.
	ErrInvalidSession = errors.New("invalid session")
	// ErrTimeoutTooSmall indicates that the specified timeout value is too small.
	ErrTimeoutTooSmall = errors.New("specified timeout value is too small")
	// ErrPayloadTooBig indicates that the payload is too big.
	ErrPayloadTooBig = errors.New("payload is too big")
	// ErrSystemBusy indicates that the system is too busy to handle the request.
	// This might be caused when the Raft node reached its MaxInMemLogSize limit
	// or other system limits.
	ErrSystemBusy = errors.New("system is too busy try again later")
	// ErrClusterClosed indicates that the requested cluster is being shut down.
	ErrClusterClosed = errors.New("raft cluster already closed")
	// ErrClusterNotInitialized indicates that the requested operation can not be
	// completed as the involved raft cluster has not been initialized yet.
	ErrClusterNotInitialized = errors.New("raft cluster not initialized yet")
	// ErrBadKey indicates that the key is bad, retry the request is recommended.
	ErrBadKey = errors.New("bad key try again later")
	// ErrPendingConfigChangeExist indicates that there is already a pending
	// membership change exist in the system.
	ErrPendingConfigChangeExist = errors.New("pending config change request exist")
	// ErrPendingSnapshotRequestExist indicates that there is already a pending
	// snapshot request exist in the system.
	ErrPendingSnapshotRequestExist = errors.New("pending snapshot request exist")
	// ErrTimeout indicates that the operation timed out.
	ErrTimeout = errors.New("timeout")
	// ErrSystemStopped indicates that the system is being shut down.
	ErrSystemStopped = errors.New("system stopped")
	// ErrCanceled indicates that the request has been canceled.
	ErrCanceled = errors.New("request canceled")
	// ErrRejected indicates that the request has been rejected.
	ErrRejected = errors.New("request rejected")
	// ErrClusterNotReady indicates that the request has been dropped as the
	// specified raft cluster is not ready to handle the request. Unknown leader
	// is the most common cause of this error.
	ErrClusterNotReady = errors.New("request dropped as the cluster is not ready")
	// ErrInvalidTarget indicates that the specified node id invalid.
	ErrInvalidTarget = errors.New("invalid target node ID")
	// ErrPendingLeaderTransferExist indicates that leader transfer request exist.
	ErrPendingLeaderTransferExist = errors.New("pending leader transfer exist")
)

// IsTempError returns a boolean value indicating whether the specified error
// is a temporary error that worth to be retried later with the exact same
// input, potentially on a more suitable NodeHost instance.
func IsTempError(err error) bool {
	return err == ErrSystemBusy ||
		err == ErrBadKey ||
		err == ErrPendingConfigChangeExist ||
		err == ErrClusterClosed ||
		err == ErrSystemStopped
}

// RequestResultCode is the result code returned to the client to indicate the
// outcome of the request.
type RequestResultCode int

// RequestResult is the result struct returned for the request.
type RequestResult struct {
	// code is the result state of the request.
	code RequestResultCode
	// Result is the returned result from the Update method of the state machine
	// instance. Result is only available when making a proposal and the Code
	// value is RequestCompleted.
	result         sm.Result
	snapshotResult bool
}

// Timeout returns a boolean value indicating whether the request timed out.
func (rr *RequestResult) Timeout() bool {
	return rr.code == requestTimeout
}

// Committed returns a boolean value indicating whether the request has been
// committed by Raft.
func (rr *RequestResult) Committed() bool {
	return rr.code == requestCompleted || rr.code == requestCommitted
}

// Completed returns a boolean value indicating whether the request completed
// successfully. For proposals, it means the proposal has been committed by the
// Raft cluster and applied on the local node. For ReadIndex operation, it means
// the cluster is now ready for a local read.
func (rr *RequestResult) Completed() bool {
	return rr.code == requestCompleted
}

// Terminated returns a boolean value indicating the request terminated due to
// the requested Raft cluster is being shut down.
func (rr *RequestResult) Terminated() bool {
	return rr.code == requestTerminated
}

// Aborted returns a boolean value indicating the request is aborted.
func (rr *RequestResult) Aborted() bool {
	return rr.code == requestAborted
}

// Rejected returns a boolean value indicating the request is rejected. For a
// proposal, it means that the used client session instance is not registered
// or it has been evicted on the server side. When requesting a client session
// to be registered, Rejected means the another client session with the same
// client ID has already been registered. When requesting a client session to
// be unregistered, Rejected means the specified client session is not found
// on the server side. For a membership change request, it means the request
// is out of order and thus not applied.
func (rr *RequestResult) Rejected() bool {
	return rr.code == requestRejected
}

// Dropped returns a boolean flag indicating whether the request has been
// dropped as the leader is unavailable or not ready yet. Such dropped requests
// can usually be retried once the leader is ready.
func (rr *RequestResult) Dropped() bool {
	return rr.code == requestDropped
}

// SnapshotIndex returns the index of the generated snapshot when the
// RequestResult is from a snapshot related request. Invoking this method on
// RequestResult instances not related to snapshots will cause panic.
func (rr *RequestResult) SnapshotIndex() uint64 {
	if !rr.snapshotResult {
		plog.Panicf("not a snapshot request result")
	}
	return rr.result.Value
}

// GetResult returns the result value of the request. When making a proposal,
// the returned result is the value returned by the Update method of the
// IStateMachine instance.
func (rr *RequestResult) GetResult() sm.Result {
	return rr.result
}

const (
	requestTimeout RequestResultCode = iota
	requestCompleted
	requestTerminated
	requestRejected
	requestDropped
	requestAborted
	requestCommitted
)

var requestResultCodeName = [...]string{
	"RequestTimeout",
	"RequestCompleted",
	"RequestTerminated",
	"RequestRejected",
	"RequestDropped",
	"RequestAborted",
	"RequestCommitted",
}

func (c RequestResultCode) String() string {
	return requestResultCodeName[uint64(c)]
}

type logicalClock struct {
	ltick      uint64
	lastGcTime uint64
	gcTick     uint64
}

func (p *logicalClock) tick() {
	atomic.AddUint64(&p.ltick, 1)
}

func (p *logicalClock) getTick() uint64 {
	return atomic.LoadUint64(&p.ltick)
}

type ready struct {
	val uint32
}

func (r *ready) ready() bool {
	return atomic.LoadUint32(&r.val) == 1
}

func (r *ready) clear() {
	atomic.StoreUint32(&r.val, 0)
}

func (r *ready) set() {
	atomic.StoreUint32(&r.val, 1)
}

// SysOpState is the object used to provide system maintenance operation result
// to users.
type SysOpState struct {
	completedC <-chan struct{}
}

// CompletedC returns a struct{} chan that is closed when the requested
// operation is completed.
//
// Deprecated: CompletedC() has been deprecated. Use ResultC() instead.
func (o *SysOpState) CompletedC() <-chan struct{} {
	return o.completedC
}

// ResultC returns a struct{} chan that is closed when the requested
// operation is completed.
func (o *SysOpState) ResultC() <-chan struct{} {
	return o.completedC
}

// RequestState is the object used to provide request result to users.
type RequestState struct {
	key            uint64
	clientID       uint64
	seriesID       uint64
	respondedTo    uint64
	deadline       uint64
	readyToRead    ready
	readyToRelease ready
	aggrC          chan RequestResult
	committedC     chan RequestResult
	// CompletedC is a channel for delivering request result to users.
	//
	// Deprecated: CompletedC has been deprecated. Use ResultC() or AppliedC()
	// instead.
	CompletedC   chan RequestResult
	node         *node
	pool         *sync.Pool
	notifyCommit bool
}

// AppliedC returns a channel of RequestResult for delivering request result.
// The returned channel reports the final outcomes of proposals and config
// changes, the return value can be of one of the Completed(), Dropped(),
// Timeout(), Rejected(), Terminated() or Aborted() values.
//
// Use ResultC() when the client wants to be notified when proposals or config
// changes are committed.
func (r *RequestState) AppliedC() chan RequestResult {
	return r.CompletedC
}

// ResultC returns a channel of RequestResult for delivering request results to
// users. When NotifyCommit is not enabled, the behaviour of the returned
// channel is the same as the one returned by the AppliedC() method. When
// NotifyCommit is enabled, up to two RequestResult values can be received from
// the returned channel. For example, for a successfully proposal that is
// eventually committed and applied, the returned chan RequestResult will return
// a RequestResult value to indicate the proposal is committed first, it will be
// followed by another RequestResult value indicating the proposal has been
// applied into the state machine.
//
// Use AppliedC() when your client don't need extra notification when proposals
// and config changes are committed.
func (r *RequestState) ResultC() chan RequestResult {
	if !r.notifyCommit {
		return r.CompletedC
	}
	if r.committedC == nil {
		plog.Panicf("committedC is nil")
	}
	if r.aggrC != nil {
		return r.aggrC
	}
	r.aggrC = make(chan RequestResult, 2)
	go func() {
		select {
		case cn := <-r.committedC:
			if cn.code != requestCommitted {
				plog.Panicf("unknown code, %s", cn.code)
			}
			r.aggrC <- cn
			cc := <-r.CompletedC
			r.aggrC <- cc
		case cc := <-r.CompletedC:
			if cc.Completed() || cc.Terminated() || cc.Timeout() {
				select {
				case ccn := <-r.committedC:
					r.aggrC <- ccn
				default:
				}
				r.aggrC <- cc
			}
		}
	}()
	return r.aggrC
}

func (r *RequestState) committed() {
	if !r.notifyCommit {
		plog.Panicf("notify commit not enabled")
	}
	if r.committedC == nil {
		plog.Panicf("committedC is nil")
	}
	select {
	case r.committedC <- RequestResult{code: requestCommitted}:
	default:
		plog.Panicf("RequestState.committedC is full")
	}
}

func (r *RequestState) timeout() {
	rr := RequestResult{
		code: requestTimeout,
	}
	r.notify(rr)
}

func (r *RequestState) terminated() {
	rr := RequestResult{
		code: requestTerminated,
	}
	r.notify(rr)
}

func (r *RequestState) dropped() {
	rr := RequestResult{
		code: requestDropped,
	}
	r.notify(rr)
}

func (r *RequestState) notify(result RequestResult) {
	select {
	case r.CompletedC <- result:
		r.readyToRelease.set()
	default:
		plog.Panicf("RequestState.CompletedC is full")
	}
}

// Release puts the RequestState instance back to an internal pool so it can be
// reused. Release is normally called after all RequestResult values have been
// received from the ResultC() channel.
func (r *RequestState) Release() {
	if r.pool != nil {
		if !r.readyToRelease.ready() {
			return
		}
		r.notifyCommit = false
		r.deadline = 0
		r.key = 0
		r.seriesID = 0
		r.clientID = 0
		r.respondedTo = 0
		r.node = nil
		r.readyToRead.clear()
		r.readyToRelease.clear()
		r.aggrC = nil
		r.pool.Put(r)
	}
}

func (r *RequestState) mustBeReadyForLocalRead() {
	if r.node == nil {
		plog.Panicf("invalid rs")
	}
	if !r.node.initialized() {
		plog.Panicf("%s not initialized", r.node.id())
	}
	if !r.readyToRead.ready() {
		plog.Panicf("not ready for local read")
	}
}

type proposalShard struct {
	mu             sync.Mutex
	proposals      *entryQueue
	pending        map[uint64]*RequestState
	pool           *sync.Pool
	cfg            config.Config
	stopped        bool
	notifyCommit   bool
	expireNotified uint64
	logicalClock
}

type keyGenerator struct {
	randMu sync.Mutex
	rand   *rand.Rand
}

func (k *keyGenerator) nextKey() uint64 {
	k.randMu.Lock()
	v := k.rand.Uint64()
	k.randMu.Unlock()
	return v
}

type pendingProposal struct {
	shards []*proposalShard
	keyg   []*keyGenerator
	ps     uint64
}

type readBatch struct {
	index    uint64
	requests []*RequestState
}

type pendingReadIndex struct {
	mu       sync.Mutex
	batches  map[pb.SystemCtx]readBatch
	requests *readIndexQueue
	stopped  bool
	pool     *sync.Pool
	logicalClock
}

type configChangeRequest struct {
	data []byte
	key  uint64
}

type pendingConfigChange struct {
	mu           sync.Mutex
	pending      *RequestState
	confChangeC  chan<- configChangeRequest
	notifyCommit bool
	logicalClock
}

type pendingSnapshot struct {
	mu        sync.Mutex
	pending   *RequestState
	snapshotC chan<- rsm.SSRequest
	logicalClock
}

type pendingLeaderTransfer struct {
	leaderTransferC chan uint64
}

func newPendingLeaderTransfer() *pendingLeaderTransfer {
	return &pendingLeaderTransfer{
		leaderTransferC: make(chan uint64, 1),
	}
}

func (l *pendingLeaderTransfer) request(target uint64) error {
	if target == pb.NoNode {
		return ErrInvalidTarget
	}
	select {
	case l.leaderTransferC <- target:
	default:
		return ErrPendingLeaderTransferExist
	}
	return nil
}

func (l *pendingLeaderTransfer) get() (uint64, bool) {
	select {
	case v := <-l.leaderTransferC:
		return v, true
	default:
	}
	return 0, false
}

func newPendingSnapshot(snapshotC chan<- rsm.SSRequest) *pendingSnapshot {
	gcTick := defaultGCTick
	if gcTick == 0 {
		plog.Panicf("invalid gcTick")
	}
	lcu := logicalClock{gcTick: gcTick}
	return &pendingSnapshot{
		logicalClock: lcu,
		snapshotC:    snapshotC,
	}
}

func (p *pendingSnapshot) notify(r RequestResult) {
	r.snapshotResult = true
	p.pending.notify(r)
}

func (p *pendingSnapshot) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshotC = nil
	if p.pending != nil {
		p.pending.terminated()
		p.pending = nil
	}
}

func (p *pendingSnapshot) request(st rsm.SSReqType,
	path string, override bool, overhead uint64,
	timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending != nil {
		return nil, ErrPendingSnapshotRequestExist
	}
	if p.snapshotC == nil {
		return nil, ErrClusterClosed
	}
	ssreq := rsm.SSRequest{
		Type:               st,
		Path:               path,
		Key:                random.LockGuardedRand.Uint64(),
		OverrideCompaction: override,
		CompactionOverhead: overhead,
	}
	req := &RequestState{
		key:          ssreq.Key,
		deadline:     p.getTick() + timeoutTick,
		CompletedC:   make(chan RequestResult, 1),
		notifyCommit: false,
	}
	select {
	case p.snapshotC <- ssreq:
		p.pending = req
		return req, nil
	default:
	}
	return nil, ErrSystemBusy
}

func (p *pendingSnapshot) gc() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	now := p.getTick()
	if now-p.logicalClock.lastGcTime < p.gcTick {
		return
	}
	p.logicalClock.lastGcTime = now
	if p.pending.deadline < now {
		p.pending.timeout()
		p.pending = nil
	}
}

func (p *pendingSnapshot) apply(key uint64,
	ignored bool, aborted bool, index uint64) {
	if ignored && aborted {
		plog.Panicf("ignored && aborted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		r := RequestResult{}
		if ignored {
			r.code = requestRejected
		} else if aborted {
			r.code = requestAborted
		} else {
			r.code = requestCompleted
			r.result.Value = index
		}
		p.notify(r)
		p.pending = nil
	}
}

func newPendingConfigChange(confChangeC chan<- configChangeRequest,
	notifyCommit bool) *pendingConfigChange {
	gcTick := defaultGCTick
	if gcTick == 0 {
		plog.Panicf("invalid gcTick")
	}
	lcu := logicalClock{gcTick: gcTick}
	return &pendingConfigChange{
		confChangeC:  confChangeC,
		logicalClock: lcu,
		notifyCommit: notifyCommit,
	}
}

func (p *pendingConfigChange) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.confChangeC != nil {
		if p.pending != nil {
			p.pending.terminated()
			p.pending = nil
		}
		close(p.confChangeC)
		p.confChangeC = nil
	}
}

func (p *pendingConfigChange) request(cc pb.ConfigChange,
	timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending != nil {
		return nil, ErrPendingConfigChangeExist
	}
	if p.confChangeC == nil {
		return nil, ErrClusterClosed
	}
	data, err := cc.Marshal()
	if err != nil {
		plog.Panicf("%v", err)
	}
	ccreq := configChangeRequest{
		key:  random.LockGuardedRand.Uint64(),
		data: data,
	}
	req := &RequestState{
		key:          ccreq.key,
		deadline:     p.getTick() + timeoutTick,
		CompletedC:   make(chan RequestResult, 1),
		notifyCommit: p.notifyCommit,
	}
	if p.notifyCommit {
		req.committedC = make(chan RequestResult, 1)
	}
	select {
	case p.confChangeC <- ccreq:
		p.pending = req
		return req, nil
	default:
	}
	return nil, ErrSystemBusy
}

func (p *pendingConfigChange) gc() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	now := p.getTick()
	if now-p.logicalClock.lastGcTime < p.gcTick {
		return
	}
	p.logicalClock.lastGcTime = now
	if p.pending.deadline < now {
		p.pending.timeout()
		p.pending = nil
	}
}

func (p *pendingConfigChange) committed(key uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		p.pending.committed()
	}
}

func (p *pendingConfigChange) dropped(key uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		p.pending.dropped()
		p.pending = nil
	}
}

func (p *pendingConfigChange) apply(key uint64, rejected bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	var v RequestResult
	if rejected {
		v.code = requestRejected
	} else {
		v.code = requestCompleted
	}
	if p.pending.key == key {
		p.pending.notify(v)
		p.pending = nil
	}
}

func newPendingReadIndex(pool *sync.Pool,
	requests *readIndexQueue) *pendingReadIndex {
	gcTick := defaultGCTick
	if gcTick == 0 {
		plog.Panicf("invalid gcTick")
	}
	lcu := logicalClock{gcTick: gcTick}
	p := &pendingReadIndex{
		batches:      make(map[pb.SystemCtx]readBatch),
		requests:     requests,
		logicalClock: lcu,
		pool:         pool,
	}
	return p
}

func (p *pendingReadIndex) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.requests != nil {
		p.requests.close()
		reqs := p.requests.get()
		for _, rec := range reqs {
			rec.terminated()
		}
	}
	for _, rb := range p.batches {
		for _, req := range rb.requests {
			if req != nil {
				req.terminated()
			}
		}
	}
}

func (p *pendingReadIndex) read(timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	req := p.pool.Get().(*RequestState)
	req.notifyCommit = false
	req.deadline = p.getTick() + timeoutTick
	if len(req.CompletedC) > 0 {
		req.CompletedC = make(chan RequestResult, 1)
	}
	ok, closed := p.requests.add(req)
	if closed {
		return nil, ErrClusterClosed
	}
	if !ok {
		return nil, ErrSystemBusy
	}
	return req, nil
}

func (p *pendingReadIndex) genCtx() pb.SystemCtx {
	et := p.getTick() + 30
	for {
		v := pb.SystemCtx{
			Low:  random.LockGuardedRand.Uint64(),
			High: et,
		}
		if v.Low != 0 {
			return v
		}
	}
}

func (p *pendingReadIndex) nextCtx() pb.SystemCtx {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.genCtx()
}

func (p *pendingReadIndex) addReady(reads []pb.ReadyToRead) {
	if len(reads) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range reads {
		if rb, ok := p.batches[v.SystemCtx]; ok {
			rb.index = v.Index
			p.batches[v.SystemCtx] = rb
		}
	}
}

func (p *pendingReadIndex) add(sys pb.SystemCtx, reqs []*RequestState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if _, ok := p.batches[sys]; ok {
		plog.Panicf("same system ctx added again %v", sys)
	} else {
		rs := make([]*RequestState, len(reqs))
		copy(rs, reqs)
		p.batches[sys] = readBatch{
			requests: rs,
		}
	}
}

func (p *pendingReadIndex) dropped(system pb.SystemCtx) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if rb, ok := p.batches[system]; ok {
		for _, req := range rb.requests {
			if req != nil {
				req.dropped()
			}
		}
		delete(p.batches, system)
	}
}

func (p *pendingReadIndex) applied(applied uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || len(p.batches) == 0 {
		return
	}
	now := p.getTick()
	for sys, rb := range p.batches {
		if rb.index > 0 && rb.index <= applied {
			for _, req := range rb.requests {
				if req != nil {
					var v RequestResult
					if req.deadline > now {
						req.readyToRead.set()
						v.code = requestCompleted
					} else {
						v.code = requestTimeout
					}
					req.notify(v)
				}
			}
			delete(p.batches, sys)
		}
	}
	if now-p.logicalClock.lastGcTime < p.gcTick {
		return
	}
	p.logicalClock.lastGcTime = now
	p.gc(now)
}

func (p *pendingReadIndex) gc(now uint64) {
	if len(p.batches) == 0 {
		return
	}
	for sys, rb := range p.batches {
		for idx, req := range rb.requests {
			if req != nil && req.deadline < now {
				req.timeout()
				rb.requests[idx] = nil
				p.batches[sys] = rb
			}
		}
	}
	for sys, rb := range p.batches {
		if sys.High < now {
			empty := true
			for _, req := range rb.requests {
				if req != nil {
					empty = false
					break
				}
			}
			if empty {
				delete(p.batches, sys)
			}
		}
	}
}

func getRandomGenerator(clusterID uint64,
	nodeID uint64, addr string, partition uint64) *keyGenerator {
	pid := os.Getpid()
	nano := time.Now().UnixNano()
	seedStr := fmt.Sprintf("%d-%d-%d-%d-%s-%d",
		pid, nano, clusterID, nodeID, addr, partition)
	m := sha512.New()
	if _, err := io.WriteString(m, seedStr); err != nil {
		plog.Panicf("%v", err)
	}
	sum := m.Sum(nil)
	seed := binary.LittleEndian.Uint64(sum)
	return &keyGenerator{rand: rand.New(rand.NewSource(int64(seed)))}
}

func newPendingProposal(cfg config.Config, notifyCommit bool,
	pool *sync.Pool, proposals *entryQueue, raftAddress string) *pendingProposal {
	ps := pendingProposalShards
	p := &pendingProposal{
		shards: make([]*proposalShard, ps),
		keyg:   make([]*keyGenerator, ps),
		ps:     ps,
	}
	for i := uint64(0); i < ps; i++ {
		p.shards[i] = newPendingProposalShard(cfg, notifyCommit, pool, proposals)
		p.keyg[i] = getRandomGenerator(cfg.ClusterID, cfg.NodeID, raftAddress, i)
	}
	return p
}

func (p *pendingProposal) propose(session *client.Session,
	cmd []byte, timeoutTick uint64) (*RequestState, error) {
	key := p.nextKey(session.ClientID)
	pp := p.shards[key%p.ps]
	return pp.propose(session, cmd, key, timeoutTick)
}

func (p *pendingProposal) close() {
	for _, pp := range p.shards {
		pp.close()
	}
}

func (p *pendingProposal) committed(clientID uint64,
	seriesID uint64, key uint64) {
	pp := p.shards[key%p.ps]
	pp.committed(clientID, seriesID, key)
}

func (p *pendingProposal) dropped(clientID uint64,
	seriesID uint64, key uint64) {
	pp := p.shards[key%p.ps]
	pp.dropped(clientID, seriesID, key)
}

func (p *pendingProposal) applied(clientID uint64,
	seriesID uint64, key uint64, result sm.Result, rejected bool) {
	pp := p.shards[key%p.ps]
	pp.applied(clientID, seriesID, key, result, rejected)
}

func (p *pendingProposal) nextKey(clientID uint64) uint64 {
	return p.keyg[clientID%p.ps].nextKey()
}

func (p *pendingProposal) tick() {
	for i := uint64(0); i < p.ps; i++ {
		p.shards[i].tick()
	}
}

func (p *pendingProposal) gc() {
	for i := uint64(0); i < p.ps; i++ {
		pp := p.shards[i]
		pp.gc()
	}
}

func newPendingProposalShard(cfg config.Config,
	notifyCommit bool, pool *sync.Pool, proposals *entryQueue) *proposalShard {
	gcTick := defaultGCTick
	if gcTick == 0 {
		plog.Panicf("invalid gcTick")
	}
	lcu := logicalClock{gcTick: gcTick}
	p := &proposalShard{
		proposals:    proposals,
		pending:      make(map[uint64]*RequestState),
		logicalClock: lcu,
		pool:         pool,
		cfg:          cfg,
		notifyCommit: notifyCommit,
	}
	return p
}

func (p *proposalShard) propose(session *client.Session,
	cmd []byte, key uint64, timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	if rsm.GetMaxBlockSize(p.cfg.EntryCompressionType) < uint64(len(cmd)) {
		return nil, ErrPayloadTooBig
	}
	entry := pb.Entry{
		Key:         key,
		ClientID:    session.ClientID,
		SeriesID:    session.SeriesID,
		RespondedTo: session.RespondedTo,
	}
	if len(cmd) == 0 {
		entry.Type = pb.ApplicationEntry
	} else {
		entry.Type = pb.EncodedEntry
		entry.Cmd = prepareProposalPayload(p.cfg.EntryCompressionType, cmd)
	}
	req := p.pool.Get().(*RequestState)
	req.clientID = session.ClientID
	req.seriesID = session.SeriesID
	req.key = entry.Key
	req.deadline = p.getTick() + timeoutTick
	req.notifyCommit = p.notifyCommit
	if req.aggrC != nil {
		plog.Panicf("aggrC not nil")
	}
	if len(req.CompletedC) > 0 {
		req.CompletedC = make(chan RequestResult, 1)
	}
	if p.notifyCommit {
		if len(req.committedC) > 0 || req.committedC == nil {
			req.committedC = make(chan RequestResult, 1)
		}
	} else {
		req.committedC = nil
	}

	p.mu.Lock()
	if badKeyCheck {
		_, ok := p.pending[entry.Key]
		if ok {
			plog.Warningf("bad key")
			p.mu.Unlock()
			return nil, ErrBadKey
		}
	}
	p.pending[entry.Key] = req
	p.mu.Unlock()

	added, stopped := p.proposals.add(entry)
	if stopped {
		plog.Warningf("%s dropped proposal, cluster stopped",
			dn(p.cfg.ClusterID, p.cfg.NodeID))
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		return nil, ErrClusterClosed
	}
	if !added {
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		plog.Debugf("%s dropped proposal, overloaded",
			dn(p.cfg.ClusterID, p.cfg.NodeID))
		return nil, ErrSystemBusy
	}
	return req, nil
}

func (p *proposalShard) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.proposals != nil {
		p.proposals.close()
	}
	for _, rec := range p.pending {
		rec.terminated()
	}
}

func (p *proposalShard) getProposal(clientID uint64,
	seriesID uint64, key uint64, now uint64) *RequestState {
	return p.takeProposal(clientID, seriesID, key, now, true)
}

func (p *proposalShard) borrowProposal(clientID uint64,
	seriesID uint64, key uint64, now uint64) *RequestState {
	return p.takeProposal(clientID, seriesID, key, now, false)
}

func (p *proposalShard) takeProposal(clientID uint64,
	seriesID uint64, key uint64, now uint64, remove bool) *RequestState {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	ps, ok := p.pending[key]
	if ok && ps.deadline >= now {
		if ps.clientID == clientID && ps.seriesID == seriesID {
			if remove {
				delete(p.pending, key)
			}
			p.mu.Unlock()
			return ps
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *proposalShard) committed(clientID uint64, seriesID uint64, key uint64) {
	tick := p.getTick()
	ps := p.borrowProposal(clientID, seriesID, key, tick)
	if ps != nil {
		ps.committed()
	}
}

func (p *proposalShard) dropped(clientID uint64, seriesID uint64, key uint64) {
	tick := p.getTick()
	ps := p.getProposal(clientID, seriesID, key, tick)
	if ps != nil {
		ps.dropped()
	}
}

func (p *proposalShard) applied(clientID uint64,
	seriesID uint64, key uint64, result sm.Result, rejected bool) {
	now := p.getTick()
	var code RequestResultCode
	if rejected {
		code = requestRejected
	} else {
		code = requestCompleted
	}
	ps := p.getProposal(clientID, seriesID, key, now)
	if ps != nil {
		ps.notify(RequestResult{code: code, result: result})
	}
	if now != p.expireNotified {
		p.gcAt(now)
		p.expireNotified = now
	}
}

func (p *proposalShard) gc() {
	now := p.getTick()
	p.gcAt(now)
}

func (p *proposalShard) gcAt(now uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if now-p.lastGcTime < p.gcTick {
		return
	}
	p.lastGcTime = now
	deletedKeys := make(map[uint64]bool)
	for key, rec := range p.pending {
		if rec.deadline < now {
			rec.timeout()
			deletedKeys[key] = true
		}
	}
	if len(deletedKeys) == 0 {
		return
	}
	for key := range deletedKeys {
		delete(p.pending, key)
	}
}

func prepareProposalPayload(ct config.CompressionType, cmd []byte) []byte {
	dct := rsm.ToDioCompressionType(ct)
	return rsm.GetEncodedPayload(dct, cmd, nil)
}
