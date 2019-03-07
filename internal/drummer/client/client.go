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

/*
Package client contains RPC client functions and structs.
*/
package client

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/compression"
	"github.com/lni/dragonboat/internal/utils/netutil"
	"github.com/lni/dragonboat/logger"
)

var (
	plog = logger.GetLogger("drummer/client")
)

// Connection is a gRPC client connection.
type Connection struct {
	connection *grpc.ClientConn
	closed     uint32
}

// ClientConn returns the gRPC client connection.
func (c *Connection) ClientConn() *grpc.ClientConn {
	return c.connection
}

// Close closes the connection.
func (c *Connection) Close() {
	atomic.StoreUint32(&c.closed, 1)
	if err := c.connection.Close(); err != nil {
		plog.Errorf("failed to close the connection")
	}
}

// IsClosed returns whether the connection has been closed or not.
func (c *Connection) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

type addDialOptionsFunc func([]grpc.DialOption) []grpc.DialOption

// Pool manages a pool of gRPC client connections keyed by their
// destination addresses.
type Pool struct {
	mu          sync.Mutex
	addOptions  addDialOptionsFunc
	connections map[string]*Connection
}

// NewConnectionPool creates a new object.
func NewConnectionPool() *Pool {
	p := &Pool{
		connections: make(map[string]*Connection),
	}
	return p
}

// NewDrummerConnectionPool returns a gRPC connection pool with all
// connections configured for use with Drummer servers.
func NewDrummerConnectionPool() *Pool {
	p := NewConnectionPool()
	p.SetAddDialOptionsFunc(drummerExtraDialOptions)
	return p
}

func drummerExtraDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	co1 := grpc.MaxCallRecvMsgSize(int(settings.Soft.MaxDrummerClientMsgSize))
	co2 := grpc.MaxCallSendMsgSize(int(settings.Soft.MaxDrummerClientMsgSize))
	co3 := grpc.WithCompressor(compression.NewCompressor())
	co4 := grpc.WithDecompressor(compression.NewDecompressor())
	opts = append(opts, grpc.WithDefaultCallOptions(co1, co2))
	opts = append(opts, co3)
	opts = append(opts, co4)
	return opts
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.connections {
		c.Close()
	}
	p.connections = make(map[string]*Connection)
}

// GetConnection tries to get a connection based on the specified TLS config.
// The specified tlsConfig should be configured to use TLS mutual
// authentication or it should be nil for using insecure connection.
func (p *Pool) GetConnection(ctx context.Context, addr string,
	tlsConfig *tls.Config) (*Connection, error) {
	return p.getConnection(ctx, addr, tlsConfig)
}

// GetInsecureConnection returns an insecure Connection instance connected to
// the specified grpc server. It is recommended to use TLS connection for
// secured communication.
func (p *Pool) GetInsecureConnection(ctx context.Context,
	addr string) (*Connection, error) {
	return p.getConnection(ctx, addr, nil)
}

// GetTLSConnection returns an TLS connection connected to the specified grpc
// server.
func (p *Pool) GetTLSConnection(ctx context.Context, addr string,
	caFile string, certFile string, keyFile string) (*Connection, error) {
	hostname, err := netutil.GetHost(addr)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := netutil.GetClientTLSConfig(hostname,
		caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		plog.Panicf("tlsConfig not expected to be nil")
	}
	return p.getConnection(ctx, addr, tlsConfig)
}

// SetAddDialOptionsFunc sets the func used for adding extra connection options.
func (p *Pool) SetAddDialOptionsFunc(f addDialOptionsFunc) {
	p.addOptions = f
}

func (p *Pool) getConnection(ctx context.Context, addr string,
	tlsConfig *tls.Config) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.connections[addr]
	if ok && !c.IsClosed() {
		return c, nil
	}
	grpcConn, err := p.newConnection(ctx, addr, tlsConfig)
	if err != nil {
		return nil, err
	}
	c = &Connection{
		connection: grpcConn,
	}
	p.connections[addr] = c
	tt := "insecure"
	if tlsConfig != nil {
		tt = "TLS"
	}
	plog.Infof("connected to %s using %s transport", addr, tt)
	return c, nil
}

func (p *Pool) newConnection(ctx context.Context,
	addr string, tlsConfig *tls.Config) (*grpc.ClientConn, error) {
	var C *grpc.ClientConn
	if err := func() error {
		var opts []grpc.DialOption
		if tlsConfig == nil {
			opts = append(opts, grpc.WithInsecure())
		} else {
			tc := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
			opts = append(opts, tc)
		}
		opts = append(opts, grpc.WithBlock())
		if p.addOptions != nil {
			opts = p.addOptions(opts)
		}
		conn, err := grpc.DialContext(ctx, addr, opts...)
		if err != nil {
			return err
		}
		C = conn
		plog.Infof("connection to RPC server %s established", addr)
		return nil
	}(); err != nil {
		plog.Warningf("failed to connect to RPC server %s, %v", addr, err)
		return nil, err
	}
	return C, nil
}
