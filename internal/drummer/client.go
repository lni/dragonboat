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

package drummer

import (
	"context"
	"errors"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
)

var (
	// ErrInvalidRequest indicates the request can not be fulfilled as it is
	// regarded as invalid.
	ErrInvalidRequest = errors.New("invalid drummer request")
)

// AddDrummerServer adds a new drummer node with specified nodeID and address
// to the Drummer cluster.
func AddDrummerServer(ctx context.Context, client pb.DrummerClient,
	nodeID uint64, address string) (*pb.Empty, error) {
	req := &pb.DrummerConfigRequest{
		NodeId:  nodeID,
		Address: address,
	}
	return client.AddDrummerServer(ctx, req)
}

// RemoveDrummerServer removes the specified node from the Drummer cluster.
func RemoveDrummerServer(ctx context.Context, client pb.DrummerClient,
	nodeID uint64) (*pb.Empty, error) {
	req := &pb.DrummerConfigRequest{
		NodeId: nodeID,
	}
	return client.RemoveDrummerServer(ctx, req)
}

// SubmitCreateDrummerChange submits Drummer change used for defining clusters.
func SubmitCreateDrummerChange(ctx context.Context, client pb.DrummerClient,
	clusterID uint64, members []uint64, appName string) error {
	checkClusterIDValue(clusterID)
	if len(appName) == 0 {
		panic("empty app name")
	}
	if len(members) == 0 {
		panic("empty members")
	}
	change := pb.Change{
		Type:      pb.Change_CREATE,
		ClusterId: clusterID,
		Members:   members,
		AppName:   appName,
	}
	req, err := client.SubmitChange(ctx, &change)
	if err != nil {
		return err
	}
	if req.Code == pb.ChangeResponse_BOOTSTRAPPED {
		return ErrInvalidRequest
	}
	return nil
}

// GetClusterCollection returns known clusters from the Drummer server.
func GetClusterCollection(ctx context.Context,
	client pb.DrummerClient) (*pb.ClusterCollection, error) {
	return client.GetClusters(ctx, &pb.Empty{})
}

// GetClusterStates returns cluster states known to the Drummer server.
func GetClusterStates(ctx context.Context,
	client pb.DrummerClient, clusters []uint64) (*pb.ClusterStates, error) {
	req := &pb.ClusterStateRequest{
		ClusterIdList: clusters,
	}
	return client.GetClusterStates(ctx, req)
}

// SubmitRegions submits regions info to the Drummer server.
func SubmitRegions(ctx context.Context,
	client pb.DrummerClient, region pb.Regions) error {
	_, err := client.SetRegions(ctx, &region)
	return err
}

// SubmitBootstrappped sets the bootstrapped flag on Drummer server.
func SubmitBootstrappped(ctx context.Context,
	client pb.DrummerClient) error {
	_, err := client.SetBootstrapped(ctx, &pb.Empty{})
	return err
}

// GetNodeHostCollection returns nodehosts known to the Drummer.
func GetNodeHostCollection(ctx context.Context,
	client pb.DrummerClient) (*pb.NodeHostCollection, error) {
	return client.GetNodeHostCollection(ctx, &pb.Empty{})
}

func checkClusterIDValue(clusterID uint64) {
	if clusterID == 0 {
		panic("cluster id must be specified and it can not be 0")
	}
}
