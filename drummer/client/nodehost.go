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

package client

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	pb "github.com/lni/dragonboat/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
)

var (
	// DrummerClientName is the name of the default master client.
	DrummerClientName      = settings.Soft.DrummerClientName
	getConnectedTimeoutSec = settings.Soft.GetConnectedTimeoutSecond
	// localTimeoutMs is the timeout in millisecond value to use when ininating
	// Raft requests locally on nodehost.
	localTimeoutMs = time.Duration(settings.Soft.LocalRaftRequestTimeoutMs) * time.Millisecond
)

func isGRPCTempError(err error) bool {
	return grpc.Code(err) == codes.Unavailable
}

type drummerClient struct {
	nh  *dragonboat.NodeHost
	req struct {
		sync.Mutex
		requests []pb.NodeHostRequest
	}
	mu struct {
		sync.Mutex
		// app name -> func(uint64, uint64) IStateMachine
		smFactory map[string]pluginDetails
	}
	connections *Pool
}

// NewDrummerClient creates a new drummer client instance.
func NewDrummerClient(nh *dragonboat.NodeHost) *drummerClient {
	dc := &drummerClient{
		nh:          nh,
		connections: NewDrummerConnectionPool(),
	}
	dc.req.requests = make([]pb.NodeHostRequest, 0)
	// currently it is hard coded to scan the working dir for plugins.
	// might need to make this configurable.
	dc.mu.smFactory = getPluginMap(".")
	return dc
}

func (dc *drummerClient) Name() string {
	return DrummerClientName
}

func (dc *drummerClient) Stop() {
	dc.connections.Close()
}

func (dc *drummerClient) SendNodeHostInfo(ctx context.Context,
	drummerAPIAddress string, nhi dragonboat.NodeHostInfo) error {
	if IsNodeHostPartitioned(dc.nh) {
		plog.Infof("in partitioned test mode, dropping NodeHost Info report msg")
		return nil
	}
	conn, err := dc.getDrummerConnection(ctx, drummerAPIAddress)
	if err != nil {
		return err
	}
	client := pb.NewDrummerClient(conn.ClientConn())
	il, err := client.GetClusterConfigChangeIndexList(ctx, nil)
	if err != nil {
		conn.Close()
		return err
	}
	plog.Debugf("%s got cluster config change index from %s: %v",
		nhi.RaftAddress, drummerAPIAddress, il.Indexes)
	cil := make([]dragonboat.ClusterInfo, 0)
	for _, v := range nhi.ClusterInfoList {
		cci, ok := il.Indexes[v.ClusterID]
		if ok && cci >= v.ConfigChangeIndex && !v.Pending {
			v.Incomplete = true
			v.Nodes = nil
		}
		cil = append(cil, v)
	}
	for _, v := range cil {
		if !v.Incomplete && !v.Pending && len(v.Nodes) > 0 {
			plog.Debugf("%s updating nodehost info, %d, %v",
				nhi.RaftAddress, v.ConfigChangeIndex, v.Nodes)
		}
	}
	if !nhi.LogInfoIncluded {
		if len(nhi.LogInfo) != 0 {
			panic("!plogIncluded but len(logInfo) != 0")
		}
	}
	info := &pb.NodeHostInfo{
		RaftAddress:      nhi.RaftAddress,
		RPCAddress:       nhi.APIAddress,
		ClusterInfo:      toDrummerPBClusterInfo(cil),
		ClusterIdList:    nhi.ClusterIDList,
		PlogInfoIncluded: nhi.LogInfoIncluded,
		PlogInfo:         toDrummerPBLogInfo(nhi.LogInfo),
		Region:           nhi.Region,
	}
	requestCollection, err := client.ReportAvailableNodeHost(ctx, info)
	if err != nil {
		plog.Warningf("failed to report nodehost info to %s, %s",
			drummerAPIAddress, err)
		conn.Close()
		return err
	}
	plog.Infof("%s received %d nodehost requests from %s",
		dc.nh.RaftAddress(),
		len(requestCollection.Requests), drummerAPIAddress)
	if len(requestCollection.Requests) > 0 {
		dc.addRequests(requestCollection.Requests)
	}
	return nil
}

func toDrummerPBClusterInfo(cil []dragonboat.ClusterInfo) []pb.ClusterInfo {
	result := make([]pb.ClusterInfo, 0)
	for _, v := range cil {
		pbv := pb.ClusterInfo{
			ClusterId:         v.ClusterID,
			NodeId:            v.NodeID,
			IsLeader:          v.IsLeader,
			Nodes:             v.Nodes,
			ConfigChangeIndex: v.ConfigChangeIndex,
			Incomplete:        v.Incomplete,
			Pending:           v.Pending,
		}
		result = append(result, pbv)
	}
	return result
}

func toDrummerPBLogInfo(loginfo []raftio.NodeInfo) []pb.LogInfo {
	result := make([]pb.LogInfo, 0)
	for _, v := range loginfo {
		pbv := pb.LogInfo{
			ClusterId: v.ClusterID,
			NodeId:    v.NodeID,
		}
		result = append(result, pbv)
	}
	return result
}

func (dc *drummerClient) GetDeploymentID(ctx context.Context,
	drummerAPIAddress string) (uint64, error) {
	conn, err := dc.getDrummerConnection(ctx, drummerAPIAddress)
	if err != nil {
		return 0, err
	}
	client := pb.NewDrummerClient(conn.ClientConn())
	di, err := client.GetDeploymentInfo(ctx, &pb.Empty{})
	if err == nil {
		return di.DeploymentId, nil
	}
	conn.Close()
	return 0, err
}

func (dc *drummerClient) HandleMasterRequests(ctx context.Context) error {
	reqs := dc.getRequests()
	if len(reqs) == 0 {
		return nil
	}
	clusterIDMap := make(map[uint64]struct{})
	for _, req := range reqs {
		cid := req.Change.ClusterId
		clusterIDMap[cid] = struct{}{}
	}
	// each cluster is handled in its own worker goroutine
	stopper := syncutil.NewStopper()
	for k := range clusterIDMap {
		clusterID := k
		stopper.RunWorker(func() {
			for _, req := range reqs {
				if req.Change.ClusterId == clusterID {
					dc.handleRequest(ctx, req)
				}
			}
		})
	}
	stopper.Stop()
	return nil
}

func (dc *drummerClient) getDrummerConnection(ctx context.Context,
	drummerAPIAddress string) (*Connection, error) {
	getConnTimeout := time.Duration(getConnectedTimeoutSec) * time.Second
	getConnCtx, cancel := context.WithTimeout(ctx, getConnTimeout)
	defer cancel()
	nhCfg := dc.nh.NodeHostConfig()
	tlsConfig, err := nhCfg.GetClientTLSConfig(drummerAPIAddress)
	if err != nil {
		return nil, err
	}
	conn, err := dc.connections.GetConnection(getConnCtx,
		drummerAPIAddress, tlsConfig)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (dc *drummerClient) addRequests(reqs []pb.NodeHostRequest) {
	dc.req.Lock()
	defer dc.req.Unlock()
	dc.req.requests = append(dc.req.requests, reqs...)
}

func (dc *drummerClient) getRequests() []pb.NodeHostRequest {
	dc.req.Lock()
	defer dc.req.Unlock()
	results := dc.req.requests
	dc.req.requests = make([]pb.NodeHostRequest, 0)
	return results
}

func (dc *drummerClient) handleRequest(ctx context.Context,
	req pb.NodeHostRequest) {
	reqCtx, cancel := context.WithTimeout(ctx, localTimeoutMs)
	defer cancel()
	if req.Change.Type == pb.Request_CREATE {
		dc.handleInstantiateRequest(req)
	} else if req.Change.Type == pb.Request_DELETE ||
		req.Change.Type == pb.Request_ADD {
		dc.handleAddDeleteRequest(reqCtx, req)
	} else if req.Change.Type == pb.Request_KILL {
		dc.handleKillRequest(req)
	} else {
		panic("unknown request type")
	}
}

func (dc *drummerClient) handleKillRequest(req pb.NodeHostRequest) {
	nodeID := req.Change.Members[0]
	clusterID := req.Change.ClusterId
	plog.Debugf("kill request handled on %s for %s",
		dc.nh.RaftAddress(), logutil.DescribeNode(clusterID, nodeID))
	if err := dc.nh.StopNode(clusterID, nodeID); err != nil {
		plog.Errorf("removeClusterNode for %s failed, %v",
			logutil.DescribeNode(clusterID, nodeID), err)
	}
}

func (dc *drummerClient) handleAddDeleteRequest(ctx context.Context,
	req pb.NodeHostRequest) {
	nodeID := req.Change.Members[0]
	clusterID := req.Change.ClusterId

	var rs *dragonboat.RequestState
	var err error
	if req.Change.Type == pb.Request_DELETE {
		plog.Infof("delete request handled on %s for %s, conf change id %d",
			dc.nh.RaftAddress(), logutil.DescribeNode(clusterID, nodeID),
			req.Change.ConfChangeId)
		rs, err = dc.nh.RequestDeleteNode(clusterID, nodeID,
			req.Change.ConfChangeId, localTimeoutMs)
	} else if req.Change.Type == pb.Request_ADD {
		plog.Infof("add request handled on %s for %s, conf change id %d",
			dc.nh.RaftAddress(), logutil.DescribeNode(clusterID, nodeID),
			req.Change.ConfChangeId)
		url := req.AddressList[0]
		rs, err = dc.nh.RequestAddNode(clusterID, nodeID, url,
			req.Change.ConfChangeId, localTimeoutMs)
	} else {
		plog.Panicf("unknown request type %s", req.Change.Type)
	}
	if err == dragonboat.ErrClusterNotFound ||
		dragonboat.IsTempError(err) ||
		isGRPCTempError(err) {
		return
	} else if err != nil {
		panic(err)
	}
	select {
	case <-ctx.Done():
		return
	case v := <-rs.CompletedC:
		if !v.Completed() &&
			!v.Timeout() &&
			!v.Terminated() &&
			!v.Rejected() {
			plog.Panicf("unknown result code: %v", v)
		}
		return
	}
}

func getConfig(req pb.NodeHostRequest) config.Config {
	return config.Config{
		ElectionRTT:        req.Config.ElectionRTT,
		HeartbeatRTT:       req.Config.HeartbeatRTT,
		CheckQuorum:        req.Config.CheckQuorum,
		SnapshotEntries:    req.Config.SnapshotEntries,
		CompactionOverhead: req.Config.CompactionOverhead,
		MaxInMemLogSize:    req.Config.MaxInMemLogSize,
	}
}

func (dc *drummerClient) handleInstantiateRequest(req pb.NodeHostRequest) {
	requestType := ""
	nodeID := req.InstantiateNodeId
	clusterID := req.Change.ClusterId
	hasNodeInfo := dc.nh.HasNodeInfo(clusterID, nodeID)
	peers := make(map[uint64]string)
	// based on the request, check whether the local NodeInfo record is consistent
	// with what drummer wants us to do
	if req.Join && !req.Restore {
		// repair
		requestType = "join"
		if hasNodeInfo {
			plog.Warningf("node %s info found on %s, will ignore the request",
				logutil.DescribeNode(clusterID, nodeID), dc.nh.RaftAddress())
		}
	} else if !req.Join && req.Restore {
		// restore
		requestType = "restore"
		if !hasNodeInfo {
			plog.Warningf("node %s info not found on %s, disk has been replaced?",
				logutil.DescribeNode(clusterID, nodeID), dc.nh.RaftAddress())
			return
		}
	} else if !req.Join && !req.Restore {
		// launch
		requestType = "launch"
		if hasNodeInfo {
			plog.Panicf("node %s info found on %s, launch failed",
				logutil.DescribeNode(clusterID, nodeID), dc.nh.RaftAddress())
		}
		for k, v := range req.AddressList {
			plog.Debugf("remote node info - id:%s, address:%s",
				logutil.NodeID(req.NodeIdList[k]), v)
			peers[req.NodeIdList[k]] = v
		}
	} else {
		panic("unknown join && restore combination")
	}
	plog.Infof("%s request handled on %s for %s",
		requestType, dc.nh.RaftAddress(),
		logutil.DescribeNode(clusterID, nodeID))
	config := getConfig(req)
	config.NodeID = nodeID
	config.ClusterID = req.Change.ClusterId
	config.OrderedConfigChange = true
	pd, ok := dc.mu.smFactory[req.AppName]
	if !ok {
		// installation or configuration issue
		panic("failed to start the node as the plugin is not ready")
	}
	var err error
	if pd.isRegularStateMachine() {
		err = dc.nh.StartCluster(peers,
			req.Join, pd.createNativeStateMachine, config)
	} else if pd.isConcurrentStateMachine() {
		err = dc.nh.StartConcurrentCluster(peers,
			req.Join, pd.createConcurrentStateMachine, config)
	} else {
		err = dc.nh.StartClusterUsingPlugin(peers, req.Join, pd.filepath, config)
	}
	if err != nil {
		plog.Errorf("add cluster %s failed: %v",
			logutil.DescribeNode(clusterID, nodeID), err)
	}
}
