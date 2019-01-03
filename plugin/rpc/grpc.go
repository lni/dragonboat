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

package rpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/netutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/logger"
	pb "github.com/lni/dragonboat/plugin/rpc/transportpb"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
)

func init() {
	grpclog.SetLoggerV2(&grpcLogger{logger: logger.GetLogger("grpc")})
}

var (
	plog = logger.GetLogger("grpc-rpc")
	// GRPCRaftRPCName is the type name of the grpc raft RPC module.
	GRPCRaftRPCName = "grpc-transport"
	// FIXME
	// not all following consts make sense, e.g. 128G max snapshot size on WAN
	// when there can be many snapshots to send?
	initialConnWindowSize = int32(settings.Soft.InitialConnWindowSize)
	initialWindowSize     = int32(settings.Soft.InitialWindowSize)
	maxStreamCount        = uint32(settings.Soft.MaxTransportStreamCount)
	dialTimeoutSecond     = time.Duration(settings.Soft.GetConnectedTimeoutSecond) * time.Second
)

// GRPCConnection is the GRPC based client for sending raft messages.
type GRPCConnection struct {
	conn   *grpc.ClientConn
	cancel context.CancelFunc
	stream pb.Transport_RaftMessageClient
}

// NewGRPCConnection create a GRPCConnection instance.
func NewGRPCConnection(ctx context.Context,
	conn *grpc.ClientConn) (*GRPCConnection, error) {
	c := &GRPCConnection{conn: conn}
	var sctx context.Context
	client := pb.NewTransportClient(conn)
	sctx, c.cancel = context.WithCancel(ctx)
	stream, err := client.RaftMessage(sctx)
	if err != nil {
		return nil, err
	}
	c.stream = stream
	return c, nil
}

// Close closes the GRPCConnection instance.
func (c *GRPCConnection) Close() {
	c.cancel()
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the conn %v", err)
	}
}

// SendMessageBatch sends the specified message batch.
func (c *GRPCConnection) SendMessageBatch(batch raftpb.MessageBatch) error {
	return c.stream.Send(&batch)
}

// GRPCSnapshotConnection is the GRPC based ISnapshotConnection instance for
// sending snapshot data.
type GRPCSnapshotConnection struct {
	conn   *grpc.ClientConn
	cancel context.CancelFunc
	stream pb.Transport_RaftSnapshotClient
}

// NewGRPCSnapshotConnection creates a GRPCSnapshotConnection intance.
func NewGRPCSnapshotConnection(ctx context.Context,
	conn *grpc.ClientConn) (*GRPCSnapshotConnection, error) {
	c := &GRPCSnapshotConnection{conn: conn}
	client := pb.NewTransportClient(conn)
	var sctx context.Context
	sctx, c.cancel = context.WithCancel(ctx)
	stream, err := client.RaftSnapshot(sctx)
	if err != nil {
		return nil, err
	}
	c.stream = stream
	return c, nil
}

// Close closes the GRPCSnapshotConnection instance.
func (c *GRPCSnapshotConnection) Close() {
	c.cancel()
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the conn %v", err)
	}
}

// SendSnapshotChunk sends the specified snapshot chunk.
func (c *GRPCSnapshotConnection) SendSnapshotChunk(chunk raftpb.SnapshotChunk) error {
	return c.stream.Send(&chunk)
}

// RaftGRPC is the GRPC based RPC moldule.
type RaftGRPC struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	grpcServer     *grpc.Server
	requestHandler raftio.RequestHandler
	sinkFactory    raftio.ChunkSinkFactory
}

// NewRaftGRPC creates the GRPC based raft RPC instance.
func NewRaftGRPC(nhConfig config.NodeHostConfig,
	requestHandler raftio.RequestHandler,
	sinkFactory raftio.ChunkSinkFactory) raftio.IRaftRPC {
	plog.Infof("creating %s, grpc-go version: %s", GRPCRaftRPCName, grpc.Version)
	return &RaftGRPC{
		nhConfig:       nhConfig,
		stopper:        syncutil.NewStopper(),
		requestHandler: requestHandler,
		sinkFactory:    sinkFactory,
	}
}

// Start starts the server component of the GRPC based RPC instance.
func (g *RaftGRPC) Start() error {
	address := g.nhConfig.RaftAddress
	listener, err := netutil.NewStoppableListener(address, nil, g.stopper.ShouldStop())
	if err != nil {
		plog.Panicf("failed to new a stoppable listener, %v", err)
	}
	// double check the config parameters to make sure they all make sense
	if settings.MaxProposalPayloadSize >= settings.MaxMessageSize {
		plog.Panicf("settings.MaxProposalPayloadSize >= settings.GRPCMaxMessageSize")
	}
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxConcurrentStreams(maxStreamCount))
	if !monkeyTest {
		plog.Infof("using large window size")
		opts = append(opts, grpc.InitialWindowSize(initialWindowSize))
		opts = append(opts, grpc.InitialConnWindowSize(initialConnWindowSize))
	}
	opts = append(opts, grpc.MaxRecvMsgSize(math.MaxInt32))
	opts = append(opts, grpc.MaxSendMsgSize(math.MaxInt32))
	tt := "insecure"
	tlsConfig, err := g.nhConfig.GetServerTLSConfig()
	if err != nil {
		panic(err)
	}
	if tlsConfig != nil {
		tt = "TLS"
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	plog.Infof("gRPC based raft %s transport is going to listen on %s",
		tt, address)
	g.grpcServer = grpc.NewServer(opts...)
	pb.RegisterTransportServer(g.grpcServer, g)
	g.stopper.RunWorker(func() {
		err := g.grpcServer.Serve(listener)
		if err != grpc.ErrServerStopped && err != netutil.ErrListenerStopped {
			panic(err)
		}
	})
	return nil
}

// Stop stops the RaftGRPC instance.
func (g *RaftGRPC) Stop() {
	g.stopper.Stop()
	g.grpcServer.Stop()
}

func (g *RaftGRPC) getDialOptions(target string) []grpc.DialOption {
	var opts []grpc.DialOption
	tlsConfig, err := g.nhConfig.GetClientTLSConfig(target)
	if err != nil {
		plog.Panicf("failed to get client TLS config %v", err)
	}
	if tlsConfig != nil {
		plog.Infof("using tls to connect to server")
		tc := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
		opts = append(opts, tc)
	} else {
		plog.Infof("using insecure connection to connect to server")
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithBlock())
	co1 := grpc.MaxCallRecvMsgSize(math.MaxInt32)
	co2 := grpc.MaxCallSendMsgSize(math.MaxInt32)
	opts = append(opts, grpc.WithDefaultCallOptions(co1, co2))
	if !monkeyTest {
		opts = append(opts, grpc.WithInitialConnWindowSize(initialConnWindowSize))
		opts = append(opts, grpc.WithInitialWindowSize(initialWindowSize))
	}
	return opts
}

// GetConnection gets the IConnection instance used for sending raft messages.
func (g *RaftGRPC) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	opts := g.getDialOptions(target)
	dctx, cancel := context.WithTimeout(ctx, dialTimeoutSecond)
	conn, err := grpc.DialContext(dctx, target, opts...)
	cancel()
	if err != nil {
		return nil, err
	}
	c, err := NewGRPCConnection(ctx, conn)
	if err != nil {
		if err = conn.Close(); err != nil {
			plog.Errorf("failed to close the conn %v", err)
			return nil, err
		}
	}
	return c, nil
}

// GetSnapshotConnection gets the ISnapshotConnection instance used for
// sending raft snapshot chunks.
func (g *RaftGRPC) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	opts := g.getDialOptions(target)
	dctx, cancel := context.WithTimeout(ctx, dialTimeoutSecond)
	conn, err := grpc.DialContext(dctx, target, opts...)
	cancel()
	if err != nil {
		return nil, err
	}
	c, err := NewGRPCSnapshotConnection(ctx, conn)
	if err != nil {
		if err = conn.Close(); err != nil {
			plog.Errorf("failed to close the conn %v", err)
			return nil, err
		}
	}
	return c, nil
}

// Name returns the type name of the RaftGRPC instance.
func (g *RaftGRPC) Name() string {
	return GRPCRaftRPCName
}

// RaftMessage is the gRPC server side service method.
func (g *RaftGRPC) RaftMessage(stream pb.Transport_RaftMessageServer) error {
	errorC := make(chan error, 1)
	g.stopper.RunWorker(func() {
		errorC <- func() error {
			for {
				batch, err := stream.Recv()
				if err == io.EOF {
					plog.Warningf("grpc receive side stream.recv returned EOF")
					return nil
				}
				if err != nil {
					plog.Warningf("grpc receive side stream.recv returned err %v", err)
					return err
				}
				if len(batch.Requests) == 0 {
					continue
				}
				g.requestHandler(*batch)
			}
		}()
	})
	select {
	case err := <-errorC:
		return err
	case <-g.stopper.ShouldStop():
		return nil
	}
}

// RaftSnapshot is the gRPC server side service method.
func (g *RaftGRPC) RaftSnapshot(stream pb.Transport_RaftSnapshotServer) error {
	errorC := make(chan error, 1)
	chunks := g.sinkFactory()
	defer chunks.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	g.stopper.RunWorker(func() {
		errorC <- func() error {
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					plog.Errorf("grpcsnapshot receive side stream.recv returned EOF")
					return nil
				}
				if err != nil {
					plog.Errorf("grpcsnapshot receive side stream.recv returned %v", err)
					return err
				}
				chunks.AddChunk(*chunk)
			}
		}()
	})
	for {
		select {
		case err := <-errorC:
			return err
		case <-g.stopper.ShouldStop():
			return nil
		case <-ticker.C:
			chunks.Tick()
		}
	}
}

type grpcLogger struct {
	logger logger.ILogger
}

func (l *grpcLogger) Info(args ...interface{}) {
	l.logger.Infof("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Infoln(args ...interface{}) {
	l.logger.Infof("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *grpcLogger) Warning(args ...interface{}) {
	l.logger.Warningf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Warningln(args ...interface{}) {
	l.logger.Warningf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Warningf(format string, args ...interface{}) {
	l.logger.Warningf(format, args...)
}

func (l *grpcLogger) Error(args ...interface{}) {
	l.logger.Errorf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Errorln(args ...interface{}) {
	l.logger.Errorf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *grpcLogger) Fatal(args ...interface{}) {
	l.logger.Panicf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Fatalln(args ...interface{}) {
	l.logger.Panicf("%s", fmt.Sprint(args...))
}

func (l *grpcLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Panicf(format, args...)
}

func (l *grpcLogger) V(v int) bool {
	return true
}
