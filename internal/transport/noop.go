// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/stringutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
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

// SendChunk returns ErrRequestedToFail when requested.
func (c *NOOPSnapshotConnection) SendChunk(chunk raftpb.Chunk) error {
	if c.req.Fail() {
		return ErrRequestedToFail
	}
	c.sendChunksCount++
	return nil
}

// NOOPTransportFactory is a NOOP transport module used in testing
type NOOPTransportFactory struct{}

// Create creates a noop transport instance.
func (n *NOOPTransportFactory) Create(nhConfig config.NodeHostConfig,
	handler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return NewNOOPTransport(nhConfig, handler, chunkHandler)
}

// Validate returns a boolean value indicating whether the input address is
// valid.
func (n *NOOPTransportFactory) Validate(addr string) bool {
	return stringutil.IsValidAddress(addr)
}

// NOOPTransport is a transport module for testing purposes. It does not
// actually has the ability to exchange messages or snapshots between
// nodehosts.
type NOOPTransport struct {
	req        *noopRequest
	connReq    *noopConnectRequest
	connected  uint64
	tryConnect uint64
}

// NewNOOPTransport creates a new NOOPTransport instance.
func NewNOOPTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return &NOOPTransport{
		req:     &noopRequest{},
		connReq: &noopConnectRequest{},
	}
}

// Start starts the NOOPTransport instance.
func (g *NOOPTransport) Start() error {
	return nil
}

// Close closes the NOOPTransport instance.
func (g *NOOPTransport) Close() error {
	return nil
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
