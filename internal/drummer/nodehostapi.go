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

package drummer

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/client"
	pb "github.com/lni/dragonboat/internal/drummer/multiraftpb"
	"github.com/lni/dragonboat/internal/utils/netutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	sm "github.com/lni/dragonboat/statemachine"
)

// NodehostAPI implements the grpc server used for making raft IO requests.
type NodehostAPI struct {
	nh        *dragonboat.NodeHost
	stopper   *syncutil.Stopper
	server    *grpc.Server
	mu        sync.Mutex
	supportCS map[uint64]bool
}

// NewNodehostAPI creates a new NodehostAPI server instance.
func NewNodehostAPI(address string, nh *dragonboat.NodeHost) *NodehostAPI {
	stopper := syncutil.NewStopper()
	stoppableListener, err := netutil.NewStoppableListener(address, nil,
		stopper.ShouldStop())
	if err != nil {
		plog.Panicf("addr %s, %v", address, err)
	}
	var opts []grpc.ServerOption
	tt := "insecure"
	nhCfg := nh.NodeHostConfig()
	tlsConfig, err := nhCfg.GetServerTLSConfig()
	if err != nil {
		panic(err)
	}
	if tlsConfig != nil {
		tt = "TLS"
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	server := grpc.NewServer(opts...)
	m := &NodehostAPI{
		nh:        nh,
		stopper:   stopper,
		server:    server,
		supportCS: make(map[uint64]bool),
	}
	pb.RegisterNodehostAPIServer(server, m)
	stopper.RunWorker(func() {
		if err = server.Serve(stoppableListener); err != nil {
			plog.Errorf("serve failed %v", err)
		}
	})
	plog.Infof("Nodehost API server using %s transport is available at %s",
		tt, address)
	return m
}

// Stop stops the NodehostAPI instance.
func (api *NodehostAPI) Stop() {
	api.stopper.Stop()
	api.server.Stop()
}

func (api *NodehostAPI) supportRegularSession(clusterID uint64) (bool, error) {
	api.mu.Lock()
	defer api.mu.Unlock()
	v, ok := api.supportCS[clusterID]
	if ok {
		return v, nil
	}
	nhi := api.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
	for _, ci := range nhi.ClusterInfoList {
		api.supportCS[clusterID] = ci.StateMachineType != sm.OnDiskStateMachine
	}
	v, ok = api.supportCS[clusterID]
	if ok {
		return v, nil
	}
	return false, errors.New("unknown state machine type")
}

// GetSession gets a new client session instance.
func (api *NodehostAPI) GetSession(ctx context.Context,
	req *pb.SessionRequest) (*client.Session, error) {
	s, err := api.supportRegularSession(req.ClusterId)
	if err != nil {
		return nil, err
	}
	if s {
		cs, err := api.nh.SyncGetSession(ctx, req.ClusterId)
		return cs, grpcError(err)
	}
	return api.nh.GetNoOPSession(req.ClusterId), nil
}

// CloseSession closes the specified client session instance.
func (api *NodehostAPI) CloseSession(ctx context.Context,
	cs *client.Session) (*pb.SessionResponse, error) {
	if cs.IsNoOPSession() {
		return &pb.SessionResponse{Completed: true}, nil
	}
	if err := api.nh.SyncCloseSession(ctx, cs); err != nil {
		e := grpcError(err)
		return &pb.SessionResponse{Completed: false}, e
	}
	return &pb.SessionResponse{Completed: true}, nil
}

// Propose makes a propose.
func (api *NodehostAPI) Propose(ctx context.Context,
	req *pb.RaftProposal) (*pb.RaftResponse, error) {
	v, err := api.nh.SyncPropose(ctx, &req.Session, req.Data)
	if err != nil {
		return nil, grpcError(err)
	}
	return &pb.RaftResponse{Result: v.Value}, nil
}

// Read makes a linearizable read operation.
func (api *NodehostAPI) Read(ctx context.Context,
	req *pb.RaftReadIndex) (*pb.RaftResponse, error) {
	data, err := api.nh.SyncRead(ctx, req.ClusterId, req.Data)
	if err != nil {
		return nil, grpcError(err)
	}
	return &pb.RaftResponse{Data: data.([]byte)}, nil
}

// GRPCError converts errors defined in package multiraft to gRPC errors
func GRPCError(err error) error {
	return grpcError(err)
}

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	var code codes.Code
	if err == dragonboat.ErrInvalidSession {
		code = codes.InvalidArgument
	} else if err == dragonboat.ErrPayloadTooBig || err == dragonboat.ErrTimeoutTooSmall {
		code = codes.InvalidArgument
	} else if err == dragonboat.ErrSystemBusy || err == dragonboat.ErrBadKey ||
		err == dragonboat.ErrSystemStopped || err == dragonboat.ErrClusterClosed {
		code = codes.Unavailable
	} else if err == dragonboat.ErrClusterNotFound {
		code = codes.NotFound
	} else if err == context.Canceled || err == dragonboat.ErrCanceled {
		code = codes.Canceled
	} else if err == context.DeadlineExceeded || err == dragonboat.ErrTimeout {
		code = codes.DeadlineExceeded
	} else {
		code = codes.Unknown
	}
	return status.Errorf(code, err.Error())
}
