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
	"crypto/md5"
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
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	"github.com/lni/dragonboat/v3/logger"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	badKeyCheck      bool   = false
	sysGcMillisecond uint64 = 15000
)

var (
	defaultGCTick uint64 = 2
	plog                 = logger.GetLogger("dragonboat")
)

var (
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
	// ErrClusterNotInitialized indicates that the requested cluster has not been
	// initialized yet.
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
	// raft cluster is not ready.
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

// Completed returns a boolean value indicating the request request completed
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
		panic("not a snapshot request result")
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
)

var requestResultCodeName = [...]string{
	"RequestTimeout",
	"RequestCompleted",
	"RequestTerminated",
	"RequestRejected",
	"RequestDropped",
}

func (c RequestResultCode) String() string {
	return requestResultCodeName[uint64(c)]
}

func getTerminatedResult() RequestResult {
	return RequestResult{
		code: requestTerminated,
	}
}

func getTimeoutResult() RequestResult {
	return RequestResult{
		code: requestTimeout,
	}
}

func getDroppedResult() RequestResult {
	return RequestResult{
		code: requestDropped,
	}
}

const (
	maxProposalPayloadSize = settings.MaxProposalPayloadSize
)

type logicalClock struct {
	ltick             uint64
	lastGcTime        uint64
	gcTick            uint64
	tickInMillisecond uint64
}

func (p *logicalClock) tick() {
	atomic.AddUint64(&p.ltick, 1)
}

func (p *logicalClock) getTick() uint64 {
	return atomic.LoadUint64(&p.ltick)
}

func (p *logicalClock) getTimeoutTick(timeout time.Duration) uint64 {
	timeoutMs := uint64(timeout.Nanoseconds() / 1000000)
	return timeoutMs / p.tickInMillisecond
}

// ICompleteHandler is a handler interface that will be invoked when the request
// in completed. This interface is used by the language bindings, applications
// are not expected to directly use this interface.
type ICompleteHandler interface {
	Notify(RequestResult)
	Release()
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

// RequestState is the object used to provide request result to users.
type RequestState struct {
	key             uint64
	clientID        uint64
	seriesID        uint64
	respondedTo     uint64
	deadline        uint64
	readyToRead     ready
	readyToRelease  ready
	completeHandler ICompleteHandler
	// CompletedC is a channel for delivering request result to users.
	CompletedC chan RequestResult
	node       *node
	pool       *sync.Pool
}

func (r *RequestState) notify(result RequestResult) {
	r.readyToRelease.set()
	if r.completeHandler == nil {
		select {
		case r.CompletedC <- result:
		default:
			plog.Panicf("RequestState.CompletedC is full")
		}
	} else {
		r.completeHandler.Notify(result)
		r.completeHandler.Release()
		r.Release()
	}
}

// Release puts the RequestState instance back to an internal pool so it can be
// reused. Release should only be called after RequestResult has been received
// from the CompletedC channel.
func (r *RequestState) Release() {
	if r.pool != nil {
		if !r.readyToRelease.ready() {
			panic("RequestState released when never notified")
		}
		r.deadline = 0
		r.key = 0
		r.seriesID = 0
		r.clientID = 0
		r.respondedTo = 0
		r.completeHandler = nil
		r.node = nil
		r.readyToRead.clear()
		r.readyToRelease.clear()
		r.pool.Put(r)
	}
}

func (r *RequestState) mustBeReadyForLocalRead() {
	if r.node == nil {
		panic("invalid rs")
	}
	if !r.node.initialized() {
		plog.Panicf("%s not initialized", r.node.id())
	}
	if !r.readyToRead.ready() {
		panic("not ready for local read")
	}
}

type proposalShard struct {
	mu             sync.Mutex
	proposals      *entryQueue
	pending        map[uint64]*RequestState
	pool           *sync.Pool
	stopped        bool
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

type systemCtxGcTime struct {
	ctx        pb.SystemCtx
	expireTime uint64
}

type pendingReadIndex struct {
	seqKey uint64
	mu     sync.Mutex
	// user generated ctx->requestState
	pending map[uint64]*RequestState
	// system ctx->appliedIndex
	batches map[pb.SystemCtx]uint64
	// user generated ctx->batched system ctx
	mapping map[uint64]pb.SystemCtx
	// cached system ctx used to call node.ReadIndex
	system       pb.SystemCtx
	systemGcTime []systemCtxGcTime
	requests     *readIndexQueue
	stopped      bool
	pool         *sync.Pool
	logicalClock
}

type configChangeRequest struct {
	data []byte
	key  uint64
}

type pendingConfigChange struct {
	mu          sync.Mutex
	pending     *RequestState
	confChangeC chan<- configChangeRequest
	logicalClock
}

type pendingSnapshot struct {
	mu        sync.Mutex
	pending   *RequestState
	snapshotC chan<- rsm.SnapshotRequest
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

func newPendingSnapshot(snapshotC chan<- rsm.SnapshotRequest,
	tickInMillisecond uint64) *pendingSnapshot {
	gcTick := defaultGCTick
	if gcTick == 0 {
		panic("invalid gcTick")
	}
	lcu := logicalClock{
		tickInMillisecond: tickInMillisecond,
		gcTick:            gcTick,
	}
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
		p.notify(getTerminatedResult())
		p.pending = nil
	}
}

func (p *pendingSnapshot) request(st rsm.SnapshotRequestType,
	path string, timeout time.Duration) (*RequestState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	timeoutTick := p.getTimeoutTick(timeout)
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	if p.pending != nil {
		return nil, ErrPendingSnapshotRequestExist
	}
	if p.snapshotC == nil {
		return nil, ErrClusterClosed
	}
	ssreq := rsm.SnapshotRequest{
		Type: st,
		Path: path,
		Key:  random.LockGuardedRand.Uint64(),
	}
	req := &RequestState{
		key:        ssreq.Key,
		deadline:   p.getTick() + timeoutTick,
		CompletedC: make(chan RequestResult, 1),
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
		p.notify(getTimeoutResult())
		p.pending = nil
	}
}

func (p *pendingSnapshot) apply(key uint64, ignored bool, index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		r := RequestResult{}
		if ignored {
			r.code = requestRejected
		} else {
			r.code = requestCompleted
			r.result.Value = index
		}
		p.notify(r)
		p.pending = nil
	}
}

func newPendingConfigChange(confChangeC chan<- configChangeRequest,
	tickInMillisecond uint64) *pendingConfigChange {
	gcTick := defaultGCTick
	if gcTick == 0 {
		panic("invalid gcTick")
	}
	lcu := logicalClock{
		tickInMillisecond: tickInMillisecond,
		gcTick:            gcTick,
	}
	return &pendingConfigChange{
		confChangeC:  confChangeC,
		logicalClock: lcu,
	}
}

func (p *pendingConfigChange) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.confChangeC != nil {
		if p.pending != nil {
			p.pending.notify(getTerminatedResult())
			p.pending = nil
		}
		close(p.confChangeC)
		p.confChangeC = nil
	}
}

func (p *pendingConfigChange) request(cc pb.ConfigChange,
	timeout time.Duration) (*RequestState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	timeoutTick := p.getTimeoutTick(timeout)
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	if p.pending != nil {
		return nil, ErrPendingConfigChangeExist
	}
	if p.confChangeC == nil {
		return nil, ErrClusterClosed
	}
	data, err := cc.Marshal()
	if err != nil {
		panic(err)
	}
	ccreq := configChangeRequest{
		key:  random.LockGuardedRand.Uint64(),
		data: data,
	}
	req := &RequestState{
		key:        ccreq.key,
		deadline:   p.getTick() + timeoutTick,
		CompletedC: make(chan RequestResult, 1),
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
		p.pending.notify(getTimeoutResult())
		p.pending = nil
	}
}

func (p *pendingConfigChange) dropped(key uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		p.pending.notify(getDroppedResult())
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

func newPendingReadIndex(pool *sync.Pool, requests *readIndexQueue,
	tickInMillisecond uint64) *pendingReadIndex {
	gcTick := defaultGCTick
	if gcTick == 0 {
		panic("invalid gcTick")
	}
	lcu := logicalClock{
		tickInMillisecond: tickInMillisecond,
		gcTick:            gcTick,
	}
	p := &pendingReadIndex{
		pending:      make(map[uint64]*RequestState),
		batches:      make(map[pb.SystemCtx]uint64),
		mapping:      make(map[uint64]pb.SystemCtx),
		systemGcTime: make([]systemCtxGcTime, 0),
		requests:     requests,
		logicalClock: lcu,
		pool:         pool,
	}
	p.system = p.nextSystemCtx()
	return p
}

func (p *pendingReadIndex) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.requests != nil {
		p.requests.close()
		tmp := p.requests.get()
		for _, v := range tmp {
			v.notify(getTerminatedResult())
		}
	}
	for _, v := range p.pending {
		v.notify(getTerminatedResult())
	}
}

func (p *pendingReadIndex) read(handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	timeoutTick := p.getTimeoutTick(timeout)
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	req := p.pool.Get().(*RequestState)
	req.completeHandler = handler
	req.key = p.nextUserCtx()
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

func (p *pendingReadIndex) nextUserCtx() uint64 {
	return atomic.AddUint64(&p.seqKey, 1)
}

func (p *pendingReadIndex) nextSystemCtx() pb.SystemCtx {
	for {
		v := pb.SystemCtx{
			Low:  random.LockGuardedRand.Uint64(),
			High: random.LockGuardedRand.Uint64(),
		}
		if v.Low != 0 && v.High != 0 {
			return v
		}
	}
}

func (p *pendingReadIndex) nextCtx() pb.SystemCtx {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.system
	p.system = p.nextSystemCtx()
	expireTick := sysGcMillisecond / p.tickInMillisecond
	p.systemGcTime = append(p.systemGcTime,
		systemCtxGcTime{
			ctx:        v,
			expireTime: p.getTick() + expireTick,
		})
	return v
}

func (p *pendingReadIndex) peepNextCtx() pb.SystemCtx {
	p.mu.Lock()
	v := p.system
	p.mu.Unlock()
	return v
}

func (p *pendingReadIndex) addReadyToRead(readStates []pb.ReadyToRead) {
	if len(readStates) == 0 {
		return
	}
	p.mu.Lock()
	for _, v := range readStates {
		p.batches[v.SystemCtx] = v.Index
	}
	p.mu.Unlock()
}

func (p *pendingReadIndex) addPendingRead(system pb.SystemCtx,
	reqs []*RequestState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	for _, req := range reqs {
		if _, ok := p.pending[req.key]; ok {
			panic("key already in the pending map")
		}
		p.pending[req.key] = req
		p.mapping[req.key] = system
	}
}

func (p *pendingReadIndex) dropped(system pb.SystemCtx) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	var toDelete []uint64
	for key, val := range p.mapping {
		if val == system {
			req, ok := p.pending[key]
			if !ok {
				panic("inconsistent data")
			}
			req.notify(getDroppedResult())
			delete(p.pending, key)
			toDelete = append(toDelete, key)
		}
	}
	for _, key := range toDelete {
		delete(p.mapping, key)
	}
	delete(p.batches, system)
}

func (p *pendingReadIndex) applied(applied uint64) {
	// FIXME:
	// when there is no pending request, we still want to get a chance to cleanup
	// systemGcTime. the parameter sysGcMillisecond is how many
	// milliseconds are allowed before the readIndex message is considered as
	// timeout. as we send one msgIndex per 1 millisecond by default, we expect
	// the max length of systemGcTime to be less than
	// sysGcMillisecond.
	// here as you can see the one msgIndex per 1 millisecond by default
	// is not taken into consideration when checking the systemGcTime,
	// this need to be fixed.
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	if len(p.pending) == 0 &&
		uint64(len(p.systemGcTime)) < sysGcMillisecond {
		p.mu.Unlock()
		return
	}
	now := p.getTick()
	toDelete := make([]uint64, 0)
	for userKey, req := range p.pending {
		systemCtx, ok := p.mapping[userKey]
		if !ok {
			panic("mapping is missing")
		}
		bindex, bok := p.batches[systemCtx]
		if !bok || bindex > applied {
			continue
		} else {
			toDelete = append(toDelete, userKey)
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
	for _, v := range toDelete {
		delete(p.pending, v)
		delete(p.mapping, v)
	}
	if now-p.logicalClock.lastGcTime < p.gcTick {
		p.mu.Unlock()
		return
	}
	p.logicalClock.lastGcTime = now
	p.gc(now)
	p.mu.Unlock()
}

func (p *pendingReadIndex) gc(now uint64) {
	toDeleteCount := 0
	for _, v := range p.systemGcTime {
		if v.expireTime < now {
			delete(p.batches, v.ctx)
			toDeleteCount++
		} else {
			break
		}
	}
	if toDeleteCount > 0 {
		p.systemGcTime = p.systemGcTime[toDeleteCount:]
	}
	if len(p.pending) > 0 {
		toDelete := make([]uint64, 0)
		for userKey, req := range p.pending {
			if req.deadline < now {
				req.notify(getTimeoutResult())
				toDelete = append(toDelete, userKey)
			}
		}
		for _, v := range toDelete {
			delete(p.pending, v)
			delete(p.mapping, v)
		}
	}
}

func getRandomGenerator(clusterID uint64,
	nodeID uint64, addr string, partition uint64) *keyGenerator {
	pid := os.Getpid()
	nano := time.Now().UnixNano()
	seedStr := fmt.Sprintf("%d-%d-%d-%d-%s-%d",
		pid, nano, clusterID, nodeID, addr, partition)
	m := md5.New()
	if _, err := io.WriteString(m, seedStr); err != nil {
		panic(err)
	}
	md5sum := m.Sum(nil)
	seed := binary.LittleEndian.Uint64(md5sum)
	return &keyGenerator{rand: rand.New(rand.NewSource(int64(seed)))}
}

func newPendingProposal(pool *sync.Pool,
	proposals *entryQueue, clusterID uint64, nodeID uint64, raftAddress string,
	tickInMillisecond uint64) *pendingProposal {
	ps := uint64(16)
	p := &pendingProposal{
		shards: make([]*proposalShard, ps),
		keyg:   make([]*keyGenerator, ps),
		ps:     ps,
	}
	for i := uint64(0); i < ps; i++ {
		p.shards[i] = newPendingProposalShard(pool,
			proposals, tickInMillisecond)
		p.keyg[i] = getRandomGenerator(clusterID, nodeID, raftAddress, i)
	}
	return p
}

func (p *pendingProposal) propose(session *client.Session,
	cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	key := p.nextKey(session.ClientID)
	pp := p.shards[key%p.ps]
	return pp.propose(session, cmd, key, handler, timeout)
}

func (p *pendingProposal) close() {
	for _, pp := range p.shards {
		pp.close()
	}
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

func newPendingProposalShard(pool *sync.Pool,
	proposals *entryQueue, tickInMillisecond uint64) *proposalShard {
	gcTick := defaultGCTick
	if gcTick == 0 {
		panic("invalid gcTick")
	}
	lcu := logicalClock{
		tickInMillisecond: tickInMillisecond,
		gcTick:            gcTick,
	}
	p := &proposalShard{
		proposals:    proposals,
		pending:      make(map[uint64]*RequestState),
		logicalClock: lcu,
		pool:         pool,
	}
	return p
}

func (p *proposalShard) propose(session *client.Session,
	cmd []byte, key uint64, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	timeoutTick := p.getTimeoutTick(timeout)
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	if uint64(len(cmd)) > maxProposalPayloadSize {
		return nil, ErrPayloadTooBig
	}
	entry := pb.Entry{
		Key:         key,
		ClientID:    session.ClientID,
		SeriesID:    session.SeriesID,
		RespondedTo: session.RespondedTo,
		Cmd:         prepareProposalPayload(cmd),
	}
	req := p.pool.Get().(*RequestState)
	req.clientID = session.ClientID
	req.seriesID = session.SeriesID
	req.completeHandler = handler
	req.key = entry.Key
	req.deadline = p.getTick() + timeoutTick
	if len(req.CompletedC) > 0 {
		req.CompletedC = make(chan RequestResult, 1)
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
		plog.Warningf("dropping proposals, cluster stopped")
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		return nil, ErrClusterClosed
	}
	if !added {
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		plog.Warningf("dropping proposals, overloaded")
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
	for _, c := range p.pending {
		c.notify(getTerminatedResult())
	}
}

func (p *proposalShard) getProposal(clientID uint64,
	seriesID uint64, key uint64, now uint64) *RequestState {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	ps, ok := p.pending[key]
	if ok && ps.deadline >= now {
		if ps.clientID == clientID && ps.seriesID == seriesID {
			delete(p.pending, key)
			p.mu.Unlock()
			return ps
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *proposalShard) dropped(clientID uint64, seriesID uint64, key uint64) {
	tick := p.getTick()
	ps := p.getProposal(clientID, seriesID, key, tick)
	if ps != nil {
		ps.notify(getDroppedResult())
	}
	if tick != p.expireNotified {
		p.gcAt(tick)
		p.expireNotified = tick
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
	for key, pRec := range p.pending {
		if pRec.deadline < now {
			pRec.notify(getTimeoutResult())
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

func prepareProposalPayload(cmd []byte) []byte {
	dst := make([]byte, len(cmd))
	copy(dst, cmd)
	return dst
}
