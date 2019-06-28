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
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	pb "github.com/lni/dragonboat/v3/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	// ErrDBKVUpdateRejected indicates that the update for KV has been
	// rejected.
	ErrDBKVUpdateRejected    = errors.New("DB KV update rejected")
	raftOpTimeoutMillisecond = settings.Soft.LocalRaftRequestTimeoutMs
)

type server struct {
	nh      *dragonboat.NodeHost
	randSrc random.Source
}

func newDrummerServer(nh *dragonboat.NodeHost,
	randSrc random.Source) *server {
	return &server{nh: nh, randSrc: randSrc}
}

//
// functions below implements the DrummerAPI RPC interface
// they allow drummer-cmd tool or nodehost-server to access the functionality
// of the Drummer Server backed by the DB.
//

func (s *server) AddDrummerServer(ctx context.Context,
	req *pb.DrummerConfigRequest) (*pb.Empty, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := s.nh.RequestAddNode(defaultClusterID,
		req.NodeId, req.Address, 0, timeout)
	if err != nil {
		return nil, err
	}
	return waitDrummerRequestResult(ctx, rs)
}

func (s *server) RemoveDrummerServer(ctx context.Context,
	req *pb.DrummerConfigRequest) (*pb.Empty, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := s.nh.RequestDeleteNode(defaultClusterID, req.NodeId, 0, timeout)
	if err != nil {
		return nil, err
	}
	return waitDrummerRequestResult(ctx, rs)
}

func (s *server) GetDeploymentInfo(ctx context.Context,
	e *pb.Empty) (*pb.DeploymentInfo, error) {
	did, err := s.getDeploymentID(ctx)
	if err != nil {
		return nil, GRPCError(err)
	}
	di := pb.DeploymentInfo{
		DeploymentId: did,
	}
	return &di, nil
}

func (s *server) GetClusterConfigChangeIndexList(ctx context.Context,
	e *pb.Empty) (*pb.ConfigChangeIndexList, error) {
	sc, err := s.getSchedulerContext(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]uint64)
	for clusterID, c := range sc.ClusterImage.Clusters {
		result[clusterID] = c.ConfigChangeIndex
	}
	return &pb.ConfigChangeIndexList{Indexes: result}, nil
}

func (s *server) ReportAvailableNodeHost(ctx context.Context,
	nhi *pb.NodeHostInfo) (*pb.NodeHostRequestCollection, error) {
	if err := s.updateNodeHostInfo(ctx, nhi); err != nil {
		return nil, err
	}
	reqs, err := s.getRequests(ctx, nhi.RaftAddress)
	if err != nil {
		return nil, err
	}
	return &pb.NodeHostRequestCollection{Requests: reqs}, nil
}

func (s *server) GetNodeHostCollection(ctx context.Context,
	e *pb.Empty) (*pb.NodeHostCollection, error) {
	sc, err := s.getSchedulerContext(ctx)
	if err != nil {
		return nil, err
	}
	r := &pb.NodeHostCollection{
		Collection: make([]pb.NodeHostInfo, 0),
		Tick:       sc.Tick,
	}
	for _, v := range sc.NodeHostInfo {
		r.Collection = append(r.Collection, v)
	}
	return r, nil
}

func (s *server) GetClusters(ctx context.Context,
	e *pb.Empty) (*pb.ClusterCollection, error) {
	req := pb.LookupRequest{
		Type: pb.LookupRequest_CLUSTER,
	}
	resp, err := s.lookupDB(ctx, req)
	if err != nil {
		return nil, GRPCError(err)
	}
	cc := pb.ClusterCollection{
		Clusters: resp.Clusters,
	}
	return &cc, nil
}

func (s *server) SetBootstrapped(ctx context.Context,
	e *pb.Empty) (*pb.ChangeResponse, error) {
	return s.setFinalizedKV(ctx, bootstrappedKey, "true")
}

func (s *server) SetRegions(ctx context.Context,
	r *pb.Regions) (*pb.ChangeResponse, error) {
	data, err := r.Marshal()
	if err != nil {
		panic(err)
	}
	return s.setFinalizedKV(ctx, regionsKey, string(data))
}

func (s *server) SubmitChange(ctx context.Context,
	c *pb.Change) (*pb.ChangeResponse, error) {
	session, err := s.getSession(ctx, defaultClusterID)
	if err != nil {
		return nil, err
	}
	defer func() {
		cc, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err = s.nh.SyncCloseSession(cc, session); err != nil {
			plog.Errorf("close session failed %v", err)
		}
	}()
	du := pb.Update{
		Type:   pb.Update_CLUSTER,
		Change: *c,
	}
	code, err := s.proposeDrummerUpdate(ctx, session, du)
	if err != nil {
		return nil, GRPCError(err)
	}
	if code.Value == DBUpdated {
		return &pb.ChangeResponse{
			Code: pb.ChangeResponse_OK,
		}, nil
	} else if code.Value == ClusterExists {
		return &pb.ChangeResponse{
			Code: pb.ChangeResponse_CLUSTER_EXIST,
		}, nil
	} else if code.Value == DBBootstrapped {
		return &pb.ChangeResponse{
			Code: pb.ChangeResponse_BOOTSTRAPPED,
		}, nil
	}
	panic("unknown update response")
}

func (s *server) GetClusterStates(ctx context.Context,
	req *pb.ClusterStateRequest) (*pb.ClusterStates, error) {
	r := pb.LookupRequest{
		Type:  pb.LookupRequest_CLUSTER_STATES,
		Stats: *req,
	}
	data, err := r.Marshal()
	if err != nil {
		panic(err)
	}
	respData, err := s.nh.SyncRead(ctx, defaultClusterID, data)
	if err != nil {
		return nil, err
	}
	if len(respData.([]byte)) == 0 {
		return nil, dragonboat.ErrClusterNotFound
	}
	c := &pb.ClusterStates{}
	if err := c.Unmarshal(respData.([]byte)); err != nil {
		panic(err)
	}
	return c, nil
}

func waitDrummerRequestResult(ctx context.Context,
	rs *dragonboat.RequestState) (*pb.Empty, error) {
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			return nil, nil
		} else if r.Timeout() {
			return nil, dragonboat.ErrTimeout
		} else if r.Terminated() {
			return nil, dragonboat.ErrClusterClosed
		} else if r.Dropped() {
			return nil, dragonboat.ErrClusterNotReady
		}
		plog.Panicf("unknown v code")
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return nil, dragonboat.ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return nil, dragonboat.ErrTimeout
		}
	}
	panic("should never reach here")
}

func toClusterState(mc *multiCluster, mnh *multiNodeHost,
	tick uint64, clusterID uint64) (*pb.ClusterState, error) {
	c := mc.getClusterInfo(clusterID)
	if c == nil {
		return nil, dragonboat.ErrClusterNotFound
	}
	nodes := make(map[uint64]string)
	rpcAddresses := make(map[uint64]string)
	leaderNodeID := uint64(0)
	for _, n := range c.Nodes {
		nodes[n.NodeID] = n.Address
		if n.IsLeader {
			leaderNodeID = n.NodeID
		}
	}
	for nid, addr := range nodes {
		cs := mnh.get(addr)
		if cs != nil {
			rpcAddresses[nid] = cs.RPCAddress
		} else {
			rpcAddresses[nid] = ""
		}
	}
	r := &pb.ClusterState{
		ClusterId:         c.ClusterID,
		ConfigChangeIndex: c.ConfigChangeIndex,
		Nodes:             nodes,
		RPCAddresses:      rpcAddresses,
	}
	if !c.available(tick) {
		r.State = pb.ClusterState_UNAVAILABLE
	} else {
		r.State = pb.ClusterState_OK
	}
	if leaderNodeID != uint64(0) {
		r.LeaderNodeId = leaderNodeID
	}
	return r, nil
}

func (s *server) getSession(ctx context.Context,
	clusterID uint64) (*client.Session, error) {
	var lastError error
	for i := 0; i < 3; i++ {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cs, err := s.nh.SyncGetSession(c, clusterID)
		if err == nil {
			return cs, nil
		}
		lastError = err
	}
	return nil, lastError
}

func (s *server) setDeploymentID(ctx context.Context,
	session *client.Session) (uint64, error) {
	uintDid := s.randSrc.Uint64()
	did := strconv.FormatUint(uintDid, 10)
	code, err := s.proposeFinalizedKV(ctx, session, deploymentIDKey, did, 0)
	if err != nil {
		return 0, err
	}
	if code == DBKVUpdated {
		plog.Infof("DeploymentID set to %d", uintDid)
		return uintDid, nil
	}
	resp, err := s.lookupKV(ctx, deploymentIDKey)
	if err != nil {
		return 0, err
	}
	respDid, err := strconv.ParseUint(resp.KvResult.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	plog.Infof("DeploymentID returned %d", respDid)
	return respDid, nil
}

func (s *server) getRegions(ctx context.Context) (*pb.Regions, error) {
	resp, err := s.lookupKV(ctx, regionsKey)
	if err != nil {
		return nil, err
	}
	if len(resp.KvResult.Value) == 0 {
		return nil, errors.New("regions not set")
	}
	var r pb.Regions
	err = r.Unmarshal([]byte(resp.KvResult.Value))
	if err != nil {
		panic(err)
	}
	return &r, nil
}

func (s *server) getElectionInfo(ctx context.Context) (*pb.KV, error) {
	req := pb.LookupRequest{
		Type: pb.LookupRequest_KV,
		KvLookup: pb.KV{
			Key: electionKey,
		},
	}
	resp, err := s.lookupDB(ctx, req)
	if err != nil {
		return nil, err
	}
	return &resp.KvResult, nil
}

func (s *server) setFinalizedKV(ctx context.Context,
	key string, value string) (*pb.ChangeResponse, error) {
	session, err := s.getSession(ctx, defaultClusterID)
	if err != nil {
		return nil, err
	}
	defer func() {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err = s.nh.SyncCloseSession(c, session); err != nil {
			plog.Errorf("close session failed %v", err)
		}
	}()
	code, err := s.proposeFinalizedKV(ctx, session, key, value, 0)
	if err != nil {
		return nil, GRPCError(err)
	}
	if code == DBKVUpdated || code == DBKVFinalized {
		return &pb.ChangeResponse{
			Code: pb.ChangeResponse_OK,
		}, nil
	}
	panic("unknown code")
}

func (s *server) makeDrummerVote(ctx context.Context,
	session *client.Session, kv pb.KV) (uint64, error) {
	u := pb.Update{
		Type:     pb.Update_KV,
		KvUpdate: kv,
	}
	v, err := s.proposeDrummerUpdate(ctx, session, u)
	if err != nil {
		return 0, err
	}
	return v.Value, nil
}

func (s *server) getBootstrapped(ctx context.Context) (bool, error) {
	return s.getBooleanKV(ctx, bootstrappedKey)
}

func (s *server) getLaunched(ctx context.Context) (bool, error) {
	return s.getBooleanKV(ctx, launchedKey)
}

func (s *server) getSchedulerContext(ctx context.Context) (*schedulerContext, error) {
	req := pb.LookupRequest{
		Type: pb.LookupRequest_SCHEDULER_CONTEXT,
	}
	data, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	respData, err := s.nh.SyncRead(ctx, defaultClusterID, data)
	if err != nil {
		return nil, err
	}
	sc := &schedulerContext{}
	if err := json.Unmarshal(respData.([]byte), &sc); err != nil {
		panic(err)
	}
	return sc, nil
}

func (s *server) updateNodeHostInfo(ctx context.Context,
	nhi *pb.NodeHostInfo) error {
	update := pb.Update{
		Type:         pb.Update_NODEHOST_INFO,
		NodehostInfo: *nhi,
	}
	session, err := s.getSession(ctx, defaultClusterID)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err = s.nh.SyncCloseSession(c, session); err != nil {
			plog.Errorf("close session failed %v", err)
		}
	}()
	_, err = s.proposeDrummerUpdate(ctx, session, update)
	return err
}

func (s *server) getRequests(ctx context.Context,
	addr string) ([]pb.NodeHostRequest, error) {
	resp := pb.LookupRequest{
		Type:    pb.LookupRequest_REQUESTS,
		Address: addr,
	}
	data, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	result, err := s.nh.SyncRead(ctx, defaultClusterID, data)
	if err != nil {
		return nil, err
	}
	var v pb.LookupResponse
	if err = v.Unmarshal(result.([]byte)); err != nil {
		panic(err)
	}
	return v.Requests.Requests, nil
}

func (s *server) getBooleanKV(ctx context.Context,
	key string) (bool, error) {
	resp, err := s.lookupKV(ctx, key)
	if err != nil {
		return false, err
	}
	if resp.KvResult.Value == "false" || resp.KvResult.Value == "" {
		return false, nil
	} else if resp.KvResult.Value == "true" {
		return true, nil
	} else {
		panic("unknown value")
	}
}

func (s *server) lookupDB(ctx context.Context,
	req pb.LookupRequest) (*pb.LookupResponse, error) {
	timeout := time.Duration(raftOpTimeoutMillisecond) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	data, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	result, err := s.nh.SyncRead(ctx, defaultClusterID, data)
	if err != nil {
		return nil, err
	}
	var v pb.LookupResponse
	if err = v.Unmarshal(result.([]byte)); err != nil {
		panic(err)
	}
	return &v, nil
}

func (s *server) proposeDrummerUpdate(ctx context.Context,
	session *client.Session, u pb.Update) (sm.Result, error) {
	session.ClusterIDMustMatch(defaultClusterID)
	timeout := time.Duration(raftOpTimeoutMillisecond) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	data, err := u.Marshal()
	if err != nil {
		panic(err)
	}
	return s.nh.SyncPropose(ctx, session, data)
}

func (s *server) proposeFinalizedKV(ctx context.Context,
	session *client.Session, key string, value string,
	instanceID uint64) (uint64, error) {
	session.ClusterIDMustMatch(defaultClusterID)
	kv := pb.KV{
		Key:        key,
		Value:      value,
		Finalized:  true,
		InstanceId: instanceID,
	}
	u := pb.Update{
		Type:     pb.Update_KV,
		KvUpdate: kv,
	}
	result, err := s.proposeDrummerUpdate(ctx, session, u)
	if err != nil {
		return 0, err
	}
	return result.Value, err
}

func (s *server) lookupKV(ctx context.Context,
	key string) (*pb.LookupResponse, error) {
	timeout := time.Duration(raftOpTimeoutMillisecond) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req := pb.LookupRequest{
		Type:     pb.LookupRequest_KV,
		KvLookup: pb.KV{Key: key},
	}
	data, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	resp, err := s.nh.SyncRead(ctx, defaultClusterID, data)
	if err != nil {
		return nil, err
	}
	lookupResp := &pb.LookupResponse{}
	if err = lookupResp.Unmarshal(resp.([]byte)); err != nil {
		panic(err)
	}
	return lookupResp, nil
}

func (s *server) getDeploymentID(ctx context.Context) (uint64, error) {
	resp, err := s.lookupKV(ctx, deploymentIDKey)
	if err != nil {
		return 0, err
	}
	respDid, err := strconv.ParseUint(resp.KvResult.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	return respDid, nil
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, dragonboat.ErrDeadlineNotSet
	}
	now := time.Now()
	if now.After(d) {
		return 0, dragonboat.ErrInvalidDeadline
	}
	return d.Sub(now), nil
}
