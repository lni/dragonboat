// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//
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
//
//
// This file contains code derived from CockroachDB. The async send message
// pattern used in ASyncSend/connectAndProcess/connectAndProcess is similar
// to the one used in CockroachDB.
//

/*
Package transport implements the transport component used for exchanging
Raft messages between NodeHosts.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	ct "github.com/lni/dragonboat/v3/plugin/chan"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/netutil/rubyist/circuitbreaker"
	"github.com/lni/goutils/syncutil"
)

const (
	maxMsgBatchSize = settings.MaxMessageBatchSize
	// UnmanagedDeploymentID is the special DeploymentID used when the system is
	// not managed by master servers.
	UnmanagedDeploymentID = uint64(1)
)

var (
	lazyFreeCycle = settings.Soft.LazyFreeCycle
)

var (
	plog                = logger.GetLogger("transport")
	streamConnections   = settings.Soft.StreamConnections
	sendQueueLen        = settings.Soft.SendQueueLength
	errChunkSendSkipped = errors.New("chunk is skipped")
	errBatchSendSkipped = errors.New("raft request batch is skipped")
	dialTimeoutSecond   = settings.Soft.GetConnectedTimeoutSecond
	idleTimeout         = time.Minute
	dn                  = logutil.DescribeNode
)

// INodeAddressResolver converts the (cluster id, node id( tuple to network
// address
type INodeAddressResolver interface {
	Resolve(uint64, uint64) (string, string, error)
	ReverseResolve(string) []raftio.NodeInfo
	AddRemoteAddress(uint64, uint64, string)
}

// IRaftMessageHandler is the interface required to handle incoming raft
// requests.
type IRaftMessageHandler interface {
	HandleMessageBatch(batch pb.MessageBatch) (uint64, uint64)
	HandleUnreachable(clusterID uint64, nodeID uint64)
	HandleSnapshotStatus(clusterID uint64, nodeID uint64, rejected bool)
	HandleSnapshot(clusterID uint64, nodeID uint64, from uint64)
}

// ITransport is the interface of the transport layer used for exchanging
// Raft messages.
type ITransport interface {
	Name() string
	SetUnmanagedDeploymentID()
	SetDeploymentID(uint64)
	SetMessageHandler(IRaftMessageHandler)
	ASyncSend(pb.Message) bool
	ASyncSendSnapshot(pb.Message) bool
	GetStreamConnection(clusterID uint64, nodeID uint64) *Sink
	Stop()
}

//
// funcs used mainly in testing
//

// StreamChunkSendFunc is a func type that is used to determine whether a
// snapshot chunk should indeed be sent. This func is used in test only.
type StreamChunkSendFunc func(pb.Chunk) (pb.Chunk, bool)

// SendMessageBatchFunc is a func type that is used to determine whether the
// specified message batch should be sent. This func is used in test only.
type SendMessageBatchFunc func(pb.MessageBatch) (pb.MessageBatch, bool)

// DeploymentID struct is the manager type used to manage the deployment id
// value.
type DeploymentID struct {
	deploymentID uint64
}

func (d *DeploymentID) deploymentIDSet() bool {
	v := atomic.LoadUint64(&d.deploymentID)
	return v != 0
}

// SetUnmanagedDeploymentID sets the deployment id to indicate that the user
// is not managed.
func (d *DeploymentID) SetUnmanagedDeploymentID() {
	d.SetDeploymentID(UnmanagedDeploymentID)
}

// SetDeploymentID sets the deployment id to the specified value.
func (d *DeploymentID) SetDeploymentID(x uint64) {
	v := atomic.LoadUint64(&d.deploymentID)
	if v != 0 {
		panic("trying to set deployment id again")
	} else {
		atomic.StoreUint64(&d.deploymentID, x)
	}
}

func (d *DeploymentID) getDeploymentID() uint64 {
	return atomic.LoadUint64(&d.deploymentID)
}

type sendQueue struct {
	ch chan pb.Message
	rl *server.RateLimiter
}

func (sq *sendQueue) rateLimited() bool {
	return sq.rl.RateLimited()
}

func (sq *sendQueue) increase(msg pb.Message) {
	if msg.Type != pb.Replicate {
		return
	}
	sq.rl.Increase(pb.GetEntrySliceInMemSize(msg.Entries))
}

func (sq *sendQueue) decrease(msg pb.Message) {
	if msg.Type != pb.Replicate {
		return
	}
	sq.rl.Decrease(pb.GetEntrySliceInMemSize(msg.Entries))
}

// ITransportEvent is the interface for notifying connection status changes.
type ITransportEvent interface {
	ConnectionEstablished(string, bool)
	ConnectionFailed(string, bool)
}

var _ ITransport = &Transport{}

// Transport is the transport layer for delivering raft messages and snapshots.
type Transport struct {
	DeploymentID
	mu struct {
		sync.Mutex
		// each (cluster id, node id) pair has its own queue and breaker
		queues   map[string]sendQueue
		breakers map[string]*circuit.Breaker
	}
	jobs                uint32
	metrics             *transportMetrics
	serverCtx           *server.Context
	nhConfig            config.NodeHostConfig
	sourceAddress       string
	resolver            INodeAddressResolver
	stopper             *syncutil.Stopper
	folder              server.GetSnapshotDirFunc
	raftRPC             raftio.IRaftRPC
	handler             atomic.Value
	streamChunkSent     atomic.Value
	preStreamChunkSend  atomic.Value // StreamChunkSendFunc
	preSendMessageBatch atomic.Value // SendMessageBatchFunc
	ctx                 context.Context
	cancel              context.CancelFunc
	streamConnections   uint64
	sysEvents           ITransportEvent
	fs                  vfs.IFS
}

// NewTransport creates a new Transport object.
func NewTransport(nhConfig config.NodeHostConfig,
	ctx *server.Context, resolver INodeAddressResolver,
	folder server.GetSnapshotDirFunc, sysEvents ITransportEvent,
	fs vfs.IFS) (*Transport, error) {
	address := nhConfig.RaftAddress
	t := &Transport{
		nhConfig:          nhConfig,
		serverCtx:         ctx,
		sourceAddress:     address,
		resolver:          resolver,
		stopper:           syncutil.NewStopper(),
		folder:            folder,
		streamConnections: streamConnections,
		sysEvents:         sysEvents,
		fs:                fs,
	}
	chunks := NewChunks(t.handleRequest,
		t.snapshotReceived, t.getDeploymentID, t.folder, fs)
	raftRPC := createTransportRPC(nhConfig, t.handleRequest, chunks)
	plog.Infof("transport type: %s", raftRPC.Name())
	t.raftRPC = raftRPC
	if err := t.raftRPC.Start(); err != nil {
		plog.Errorf("transport rpc failed to start %v", err)
		t.raftRPC.Stop()
		return nil, err
	}
	t.stopper.RunWorker(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				chunks.Tick()
			case <-t.stopper.ShouldStop():
				chunks.Close()
				return
			}
		}
	})
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.mu.queues = make(map[string]sendQueue)
	t.mu.breakers = make(map[string]*circuit.Breaker)
	msgConn := func() float64 {
		t.mu.Lock()
		defer t.mu.Unlock()
		return float64(len(t.mu.queues))
	}
	ssCount := func() float64 {
		return float64(atomic.LoadUint32(&t.jobs))
	}
	t.metrics = newTransportMetrics(true, msgConn, ssCount)
	return t, nil
}

// Name returns the type name of the transport module
func (t *Transport) Name() string {
	return t.raftRPC.Name()
}

// GetRaftRPC returns the raft RPC instance.
func (t *Transport) GetRaftRPC() raftio.IRaftRPC {
	return t.raftRPC
}

// SetPreSendMessageBatchHook set the SendMessageBatch hook.
// This function is only expected to be used in monkey testing.
func (t *Transport) SetPreSendMessageBatchHook(h SendMessageBatchFunc) {
	t.preSendMessageBatch.Store(h)
}

// SetPreStreamChunkSendHook sets the StreamChunkSend hook function that will
// be called before each snapshot chunk is sent.
func (t *Transport) SetPreStreamChunkSendHook(h StreamChunkSendFunc) {
	t.preStreamChunkSend.Store(h)
}

// Stop stops the Transport object.
func (t *Transport) Stop() {
	t.cancel()
	t.stopper.Stop()
	t.raftRPC.Stop()
}

// GetCircuitBreaker returns the circuit breaker used for the specified
// target node.
func (t *Transport) GetCircuitBreaker(key string) *circuit.Breaker {
	t.mu.Lock()
	breaker, ok := t.mu.breakers[key]
	if !ok {
		breaker = netutil.NewBreaker()
		t.mu.breakers[key] = breaker
	}
	t.mu.Unlock()

	return breaker
}

// SetMessageHandler sets the raft message handler.
func (t *Transport) SetMessageHandler(handler IRaftMessageHandler) {
	v := t.handler.Load()
	if v != nil {
		panic("trying to set the grpctransport handler again")
	}
	t.handler.Store(handler)
}

func (t *Transport) handleRequest(req pb.MessageBatch) {
	did := t.getDeploymentID()
	if req.DeploymentId != did {
		plog.Warningf("deployment id does not match %d vs %d, message dropped",
			req.DeploymentId, did)
		return
	}
	if req.BinVer != raftio.RPCBinVersion {
		plog.Warningf("binary compatibility version not match %d vs %d",
			req.BinVer, raftio.RPCBinVersion)
		return
	}
	handler := t.handler.Load()
	if handler == nil {
		return
	}
	addr := req.SourceAddress
	if len(addr) > 0 {
		for _, r := range req.Requests {
			if r.From != 0 {
				t.resolver.AddRemoteAddress(r.ClusterId, r.From, addr)
			}
		}
	}
	ssCount, msgCount := handler.(IRaftMessageHandler).HandleMessageBatch(req)
	dropedMsgCount := uint64(len(req.Requests)) - ssCount - msgCount
	t.metrics.receivedMessages(ssCount, msgCount, dropedMsgCount)
}

func (t *Transport) snapshotReceived(clusterID uint64,
	nodeID uint64, from uint64) {
	handler := t.handler.Load()
	if handler == nil {
		return
	}
	handler.(IRaftMessageHandler).HandleSnapshot(clusterID, nodeID, from)
}

func (t *Transport) sendUnreachableNotification(addr string) {
	handler := t.handler.Load()
	if handler == nil {
		return
	}
	h := handler.(IRaftMessageHandler)
	edp := t.resolver.ReverseResolve(addr)
	plog.Infof("%s became unreachable, affecting %d raft nodes", addr, len(edp))
	for _, rec := range edp {
		h.HandleUnreachable(rec.ClusterID, rec.NodeID)
	}
}

// ASyncSend sends raft messages using RPC
//
// The generic async send Go pattern used in ASyncSend is found in CockroachDB's
// codebase.
func (t *Transport) ASyncSend(req pb.Message) bool {
	v := t.asyncSend(req)
	if !v {
		t.metrics.messageSendFailure(1)
	}
	return v
}

func (t *Transport) asyncSend(req pb.Message) bool {
	if req.Type == pb.InstallSnapshot {
		panic("snapshot message must be sent via its own channel.")
	}
	toNodeID := req.To
	clusterID := req.ClusterId
	from := req.From
	addr, key, err := t.resolver.Resolve(clusterID, toNodeID)
	if err != nil {
		plog.Warningf("%s do not have the address for %s, dropping a message",
			t.sourceAddress, dn(clusterID, toNodeID))
		return false
	}
	// fail fast
	if !t.GetCircuitBreaker(addr).Ready() {
		t.metrics.messageConnectionFailure()
		return false
	}
	// get the channel, create it in case it is not in the queue map
	t.mu.Lock()
	sq, ok := t.mu.queues[key]
	if !ok {
		sq = sendQueue{
			ch: make(chan pb.Message, sendQueueLen),
			rl: server.NewRateLimiter(t.nhConfig.MaxSendQueueSize),
		}
		t.mu.queues[key] = sq
	}
	t.mu.Unlock()
	if !ok {
		shutdownQueue := func() {
			t.mu.Lock()
			delete(t.mu.queues, key)
			t.mu.Unlock()
		}
		t.stopper.RunWorker(func() {
			t.connectAndProcess(clusterID, toNodeID, addr, sq, from)
			shutdownQueue()
			t.sendUnreachableNotification(addr)
		})
	}
	if sq.rateLimited() {
		return false
	}
	select {
	case sq.ch <- req:
		sq.increase(req)
		return true
	default:
		return false
	}
}

func (t *Transport) connectAndProcess(clusterID uint64, toNodeID uint64,
	remoteHost string, sq sendQueue, from uint64) {
	breaker := t.GetCircuitBreaker(remoteHost)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	if err := func() error {
		plog.Infof("%s is trying to connect to %s",
			t.sourceAddress, remoteHost)
		conn, err := t.raftRPC.GetConnection(t.ctx, remoteHost)
		if err != nil {
			plog.Errorf("Nodehost %s failed to get a connection to %s, %v",
				t.sourceAddress, remoteHost, err)
			return err
		}
		defer conn.Close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			plog.Infof("%s, raft RPC stream to %s (%s) established",
				dn(clusterID, from), dn(clusterID, toNodeID), remoteHost)
			t.sysEvents.ConnectionEstablished(remoteHost, false)
		}
		return t.processQueue(clusterID, toNodeID, sq, conn)
	}(); err != nil {
		plog.Warningf("breaker %s to %s failed, connect and process failed: %s",
			t.sourceAddress, remoteHost, err.Error())
		breaker.Fail()
		t.metrics.messageConnectionFailure()
		t.sysEvents.ConnectionFailed(remoteHost, false)
	}
}

func (t *Transport) processQueue(clusterID uint64, toNodeID uint64,
	sq sendQueue, conn raftio.IConnection) error {
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	sz := uint64(0)
	batch := pb.MessageBatch{
		SourceAddress: t.sourceAddress,
		BinVer:        raftio.RPCBinVersion,
	}
	requests := make([]pb.Message, 0)
	var deploymentIDSet bool
	var deploymentID uint64
	for {
		idleTimer.Reset(idleTimeout)
		// drop the message if deployment id is not set.
		if !deploymentIDSet {
			if t.deploymentIDSet() {
				deploymentIDSet = true
				deploymentID = t.getDeploymentID()
			}
		}
		select {
		case <-t.stopper.ShouldStop():
			return nil
		case <-idleTimer.C:
			return nil
		case req := <-sq.ch:
			sq.decrease(req)
			sz += uint64(req.SizeUpperLimit())
			requests = append(requests, req)
			// batch below allows multiple requests to be sent in a single message,
			// then each request can have multiple log entries.
			for done := false; !done && sz < maxMsgBatchSize; {
				select {
				case req = <-sq.ch:
					sq.decrease(req)
					sz += uint64(req.SizeUpperLimit())
					requests = append(requests, req)
				default:
					done = true
				}
			}
			// loaded enough requests, check whether we have the deployment id
			if deploymentIDSet {
				batch.DeploymentId = deploymentID
			} else {
				plog.Warningf("Messages dropped as no valid deployment id set")
				requests = requests[:0]
				continue
			}
			twoBatch := false
			if sz < maxMsgBatchSize || len(requests) == 1 {
				batch.Requests = requests
			} else {
				twoBatch = true
				batch.Requests = requests[:len(requests)-1]
			}
			if err := t.sendMessageBatch(conn, batch); err != nil {
				plog.Errorf("send batch failed, target %s (%v), %d",
					dn(clusterID, toNodeID), err, len(batch.Requests))
				return err
			}
			if twoBatch {
				batch.Requests = []pb.Message{requests[len(requests)-1]}
				if err := t.sendMessageBatch(conn, batch); err != nil {
					plog.Errorf("send batch failed, taret node %s (%v), %d",
						dn(clusterID, toNodeID), err, len(batch.Requests))
					return err
				}
			}
			sz = 0
			requests, batch = lazyFree(requests, batch)
			requests = requests[:0]
		}
	}
}

func lazyFree(reqs []pb.Message,
	mb pb.MessageBatch) ([]pb.Message, pb.MessageBatch) {
	if lazyFreeCycle > 0 {
		for i := 0; i < len(reqs); i++ {
			reqs[i].Entries = nil
		}
		mb.Requests = []pb.Message{}
	}
	return reqs, mb
}

func (t *Transport) sendMessageBatch(conn raftio.IConnection,
	batch pb.MessageBatch) error {
	v := t.preSendMessageBatch.Load()
	if v != nil {
		updated, shouldSend := v.(SendMessageBatchFunc)(batch)
		if !shouldSend {
			return errBatchSendSkipped
		}
		return conn.SendMessageBatch(updated)
	}
	if err := conn.SendMessageBatch(batch); err != nil {
		t.metrics.messageSendFailure(uint64(len(batch.Requests)))
		return err
	}
	t.metrics.messageSendSuccess(uint64(len(batch.Requests)))
	return nil
}

func getDialTimeoutSecond() uint64 {
	return atomic.LoadUint64(&dialTimeoutSecond)
}

func setDialTimeoutSecond(v uint64) {
	atomic.StoreUint64(&dialTimeoutSecond, v)
}

func createTransportRPC(nhConfig config.NodeHostConfig,
	requestHandler raftio.RequestHandler,
	chunkHandler raftio.IChunkHandler) raftio.IRaftRPC {
	var factory config.RaftRPCFactoryFunc
	if nhConfig.RaftRPCFactory != nil {
		factory = nhConfig.RaftRPCFactory
	} else if memfsTest {
		factory = ct.NewChanTransport
	} else {
		factory = NewTCPTransport
	}
	return factory(nhConfig, requestHandler, chunkHandler)
}
