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

package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
)

var (
	// NOOPRaftName is the module name for the NOOP transport module.
	NOOPRaftName = "noop-test-transport"
	// ErrRequestedToFail is the error used to indicate that the error is
	// requested.
	ErrRequestedToFail = errors.New("requested to returned error")
)

type noopRequest struct {
	mu      sync.Mutex
	fail    bool
	blocked bool
}

func (r *noopRequest) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *noopRequest) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

func (r *noopRequest) SetBlocked(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked = v
}

func (r *noopRequest) Blocked() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.blocked
}

type noopConnectRequest struct {
	mu   sync.Mutex
	fail bool
}

func (r *noopConnectRequest) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *noopConnectRequest) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

// NOOPConnection is the connection used to exchange messages between node hosts.
type NOOPConnection struct {
	req *noopRequest
}

// Close closes the NOOPConnection instance.
func (c *NOOPConnection) Close() {
}

// SendMessageBatch return ErrRequestedToFail when requested.
func (c *NOOPConnection) SendMessageBatch(batch raftpb.MessageBatch) error {
	if c.req.Fail() {
		return ErrRequestedToFail
	}
	for c.req.Blocked() {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// NOOPSnapshotConnection is the connection used to send snapshots.
type NOOPSnapshotConnection struct {
	req             *noopRequest
	sendChunksCount uint64
}

// Close closes the NOOPSnapshotConnection.
func (c *NOOPSnapshotConnection) Close() {
}

// SendSnapshotChunk returns ErrRequestedToFail when requested.
func (c *NOOPSnapshotConnection) SendSnapshotChunk(chunk raftpb.SnapshotChunk) error {
	if c.req.Fail() {
		return ErrRequestedToFail
	}
	c.sendChunksCount++
	return nil
}

// NOOPTransport is a transport module for testing purposes. It does not
// actually has the ability to exchange messages or snapshots between
// nodehosts.
type NOOPTransport struct {
	connected  uint64
	tryConnect uint64
	req        *noopRequest
	connReq    *noopConnectRequest
}

// NewNOOPTransport creates a new NOOPTransport instance.
func NewNOOPTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.RequestHandler,
	sinkFactory raftio.ChunkSinkFactory) raftio.IRaftRPC {
	return &NOOPTransport{
		req:     &noopRequest{},
		connReq: &noopConnectRequest{},
	}
}

// Start starts the NOOPTransport instance.
func (g *NOOPTransport) Start() error {
	return nil
}

// Stop stops the NOOPTransport instance.
func (g *NOOPTransport) Stop() {
}

// GetConnection returns a connection.
func (g *NOOPTransport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	atomic.AddUint64(&g.tryConnect, 1)
	if g.connReq.Fail() {
		return nil, ErrRequestedToFail
	}
	atomic.AddUint64(&g.connected, 1)
	return &NOOPConnection{req: g.req}, nil
}

// GetSnapshotConnection returns a snapshot connection.
func (g *NOOPTransport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	atomic.AddUint64(&g.tryConnect, 1)
	if g.connReq.Fail() {
		return nil, ErrRequestedToFail
	}
	atomic.AddUint64(&g.connected, 1)
	return &NOOPSnapshotConnection{req: g.req}, nil
}

// Name returns the module name.
func (g *NOOPTransport) Name() string {
	return NOOPRaftName
}
