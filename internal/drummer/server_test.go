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

// +build dragonboat_slowtest

package drummer

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	pb "github.com/lni/dragonboat/v3/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/v3/internal/utils/leaktest"
	"github.com/lni/dragonboat/v3/internal/utils/random"
)

func isTimeoutError(err error) bool {
	if err == dragonboat.ErrTimeout {
		return true
	}
	return grpc.Code(err) == codes.DeadlineExceeded
}

func TestDeploymentIDCanBeSetAndGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, _ := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	defer stopTestNodes(drummerNodes)
	var session *client.Session
	var err error
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		session, err = drummerNodes[0].nh.SyncGetSession(ctx, defaultClusterID)
		if err == nil {
			break
		}
	}
	if session == nil {
		t.Fatalf("failed to get a new client session")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		drummerNodes[0].nh.SyncCloseSession(ctx, session)
	}()
	var did uint64
	retry := 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		did, err = drummerNodes[0].drummer.server.setDeploymentID(ctx, session)
		if err == nil {
			break
		} else if err != nil && !isTimeoutError(err) {
			t.Fatalf("failed to set deployment id %v", err)
		}
		session.ProposalCompleted()
	}
	if did == 0 {
		t.Fatalf("failed to set did after %d retries", 5)
		return
	}
	plog.Infof("deployment id set")
	time.Sleep(10 * time.Second)
	retry = 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		plog.Infof("going to get deployment info")
		di, err := drummerNodes[1].drummer.server.GetDeploymentInfo(ctx, &pb.Empty{})
		if err == nil {
			plog.Infof("deployment id returned successfully")
			if di.DeploymentId != did {
				t.Errorf("did %d, want %d", di.DeploymentId, did)
			}
			return
		}
		plog.Infof("%v", err)
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("failed to get did")
}

func TestSetAndGetBootstrapped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, _ := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	defer stopTestNodes(drummerNodes)
	retry := 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, err := drummerNodes[0].drummer.server.getBootstrapped(ctx)
		if err == nil {
			if s {
				t.Errorf("bootstrapped flag set by default")
			}
			break
		} else if isTimeoutError(err) {
			time.Sleep(1 * time.Second)
			continue
		} else {
			t.Errorf("failed to set bootstrapped flag")
		}
	}
	retry = 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := drummerNodes[0].drummer.server.SetBootstrapped(ctx, &pb.Empty{})
		if err == nil {
			break
		} else if isTimeoutError(err) {
			time.Sleep(1 * time.Second)
			continue
		} else {
			t.Errorf("failed to set bootstrapped flag")
		}
	}
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, err := drummerNodes[0].drummer.server.getBootstrapped(ctx)
		if err == nil {
			if !s {
				t.Errorf("bootstrapped flag not set")
			}
			break
		} else if isTimeoutError(err) {
			time.Sleep(1 * time.Second)
			continue
		} else {
			t.Errorf("failed to set bootstrapped flag")
		}
	}
}

func TestRegionCanBeSetAndGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := pb.Regions{
		Region: []string{"north", "east", "south"},
		Count:  []uint64{2, 5, 8},
	}
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, _ := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	defer stopTestNodes(drummerNodes)
	retry := 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := drummerNodes[0].drummer.server.SetRegions(ctx, &r)
		if err == nil {
			break
		} else if isTimeoutError(err) {
			time.Sleep(1 * time.Second)
			continue
		} else {
			t.Errorf("failed to set regions: %v", err)
		}
	}
	retry = 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rr, err := drummerNodes[2].drummer.server.getRegions(ctx)
		if err == nil {
			if !reflect.DeepEqual(rr, &r) {
				t.Errorf("returned region is different, got %v, want %v", *rr, r)
			}
			break
		} else if isTimeoutError(err) {
			time.Sleep(1 * time.Second)
			continue
		} else {
			t.Errorf("failed to set regions")
		}
	}
}

func TestReportAvailableNodeHost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// test data copied from clusterimage_test.go
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
	}
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, _ := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	defer stopTestNodes(drummerNodes)

	s := drummerNodes[0].drummer.server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.ReportAvailableNodeHost(ctx, &nhi)
	if err != nil {
		t.Errorf("err %v, want nil", err)
	}
	c, err := s.GetNodeHostCollection(ctx, &pb.Empty{})
	if err != nil {
		t.Errorf("err %v, want nil", err)
	}
	if len(c.Collection) != 1 {
		t.Errorf("collection sz %d, want 1", len(c.Collection))
	}
	nhi.LastTick = 0
	if !reflect.DeepEqual(&c.Collection[0], &nhi) {
		t.Errorf("nhi %v, want %v", c.Collection[0], nhi)
	}
	mc, err := drummerNodes[0].GetMultiCluster()
	if err != nil {
		t.Fatalf("failed to get multi cluster")
	}
	if len(mc.Clusters) != 1 {
		t.Errorf("cluster sz %d, want 1", len(mc.Clusters))
	}
	if len(mc.Clusters[1].Nodes) != 3 {
		t.Errorf("nodes sz %d, want 3", len(mc.Clusters[0].Nodes))
	}
	mnh, err := drummerNodes[0].GetNodeHostInfo()
	if err != nil {
		t.Fatalf("failed to get milti nodehost")
	}
	if len(mnh.Nodehosts) != 1 {
		t.Errorf("nodehost sz %d, want 1", len(mnh.Nodehosts))
	}
}

func TestReportAvailableNodeHostWithManyLargeInput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	plogInfoList := make([]pb.LogInfo, 0)
	for i := 0; i < 20000; i++ {
		pi := pb.LogInfo{
			ClusterId: uint64(i),
			NodeId:    uint64(i),
		}
		plogInfoList = append(plogInfoList, pi)
	}
	ciList := make([]pb.ClusterInfo, 0)
	for i := 0; i < 10000; i++ {
		ci := pb.ClusterInfo{
			ClusterId:         uint64(i),
			NodeId:            uint64(i),
			ConfigChangeIndex: 100,
			Nodes: map[uint64]string{
				uint64(i):     random.String(32),
				rand.Uint64(): random.String(32),
				rand.Uint64(): random.String(32),
				rand.Uint64(): random.String(32),
				rand.Uint64(): random.String(32),
			},
		}
		ciList = append(ciList, ci)
	}
	nhi := pb.NodeHostInfo{
		RaftAddress:      random.String(32),
		RPCAddress:       random.String(32),
		LastTick:         100,
		Region:           "region-1",
		PlogInfoIncluded: true,
		PlogInfo:         plogInfoList,
		ClusterInfo:      ciList,
	}
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, _ := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	defer stopTestNodes(drummerNodes)
	s := drummerNodes[0].drummer.server
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := s.ReportAvailableNodeHost(ctx, &nhi)
		cancel()
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
	}
}
