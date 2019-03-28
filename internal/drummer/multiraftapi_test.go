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

// +build dragonboat_slowtest

package drummer

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/client"
	dc "github.com/lni/dragonboat/internal/drummer/client"
	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	mr "github.com/lni/dragonboat/internal/drummer/multiraftpb"
	"github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/random"
)

func getMultiraftAPIAddress(nodehostAddress string,
	dl *mtAddressList) (string, bool) {
	for idx, v := range dl.nodehostAddressList {
		if v == nodehostAddress {
			return dl.nodehostAPIAddressList[idx], true
		}
	}

	return "", false
}

func getTestNodeHostAddressOfFirstNode(drummerNodes []*testNode) (string, bool) {
	for _, node := range drummerNodes {
		mc, err := node.GetMultiCluster()
		if err != nil {
			return "", false
		}
		cluster := mc.GetCluster(mtClusterID)
		if cluster == nil {
			return "", false
		}
		n, ok := cluster.Nodes[mtNodeID1]
		if !ok {
			return "", false
		} else {
			return n.Address, true
		}
	}
	// should never reach here
	return "", false
}

type clientSessionTester func(*testing.T, *client.Session, mr.NodehostAPIClient)

func runMultiraftAPITest(t *testing.T, mutualTLS bool, tf clientSessionTester) {
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	if mutualTLS {
		for _, n := range drummerNodes {
			n.mutualTLS = true
		}
		for _, n := range nodehostNodes {
			n.mutualTLS = true
		}
	}
	startTestNodes(drummerNodes, dl)
	startTestNodes(nodehostNodes, dl)
	setRegionForNodehostNodes(nodehostNodes,
		[]string{"region-1", "region-2", "region-3", "region-4", "region-5"})
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	waitForStableNodes(drummerNodes, 25)
	waitForStableNodes(nodehostNodes, 25)
	time.Sleep(time.Duration(3*NodeHostInfoReportSecond) * time.Second)
	if !submitSimpleTestJob(dl, mutualTLS) {
		t.Errorf("failed to submit the test job")
	}
	waitTimeSec := (loopIntervalFactor + 5) * NodeHostInfoReportSecond
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	for _, node := range drummerNodes {
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get cluster info")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
	}
	nodeHostAddress, ok := getTestNodeHostAddressOfFirstNode(drummerNodes)
	if !ok {
		t.Errorf("failed to get node host address")
	}
	multiraftAPIAddress, ok := getMultiraftAPIAddress(nodeHostAddress, dl)
	if !ok {
		t.Errorf("failed to get api address")
	}
	p := dc.NewConnectionPool()
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var conn *dc.Connection
	var err error
	if mutualTLS {
		conn, err = p.GetTLSConnection(ctx,
			multiraftAPIAddress, caFile, certFile, keyFile)
	} else {
		conn, err = p.GetInsecureConnection(ctx, multiraftAPIAddress)
	}
	if err != nil {
		t.Fatalf("failed to get client")
	}
	client := mr.NewNodehostAPIClient(conn.ClientConn())
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &mr.SessionRequest{ClusterId: mtClusterID}
	cs, err := client.GetSession(ctx, req)
	if err != nil {
		t.Errorf("failed to get client session")
	}
	tf(t, cs, client)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := client.CloseSession(ctx, cs)
	if err != nil || !r.Completed {
		t.Errorf("failed to close client session, %v", err)
	}
}

func TestMultiraftAPICanQueryClusterInfoFromDrummer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	startTestNodes(nodehostNodes, dl)
	setRegionForNodehostNodes(nodehostNodes,
		[]string{"region-1", "region-2", "region-3", "region-4", "region-5"})
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	waitForStableNodes(drummerNodes, 25)
	waitForStableNodes(nodehostNodes, 25)
	time.Sleep(time.Duration(3*NodeHostInfoReportSecond) * time.Second)
	if !submitSimpleTestJob(dl, false) {
		t.Errorf("failed to submit the test job")
	}
	waitTimeSec := (loopIntervalFactor + 7) * NodeHostInfoReportSecond
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	for _, node := range drummerNodes {
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get cluster info")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
	}
	p := dc.NewDrummerConnectionPool()
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := p.GetInsecureConnection(ctx, dl.apiAddressList[0])
	if err != nil {
		t.Errorf("failed to get drummer client")
	}
	client := pb.NewDrummerClient(conn.ClientConn())
	req := &pb.ClusterStateRequest{ClusterIdList: []uint64{mtClusterID}}
	resp, err := client.GetClusterStates(ctx, req)
	if err != nil {
		t.Errorf("failed to get cluster info")
	}
	if len(resp.Collection) != 1 {
		t.Errorf("failed to return cluster info")
	}
	ci := resp.Collection[0]
	for _, addr := range ci.RPCAddresses {
		if len(addr) == 0 {
			t.Errorf("failed to get RPC address from Drummer")
		}
	}
	if ci.LeaderNodeId == 0 {
		t.Errorf("leader id not set")
	}
}

func TestMultiraftAPISessionCanBeCreated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tester := func(t *testing.T, cs *client.Session, c mr.NodehostAPIClient) {
		if cs.ClientID == 0 {
			t.Errorf("invalid client id")
		}
		if cs.SeriesID != client.SeriesIDFirstProposal {
			t.Errorf("series id %d, want %d", cs.SeriesID, client.SeriesIDFirstProposal)
		}
		if cs.RespondedTo != 0 {
			t.Errorf("invalid responded to")
		}
	}
	runMultiraftAPITest(t, false, tester)
	runMultiraftAPITest(t, true, tester)
}

func TestMultiraftAPICanProposalAndRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tester := func(t *testing.T, cs *client.Session, client mr.NodehostAPIClient) {
		// keep making some proposals, they should work
		for i := 0; i < 16; i++ {
			sz := i + 1
			k := "test-key"
			v := random.String(sz)
			kv := &kvpb.PBKV{
				Key: k,
				Val: v,
			}
			data, err := proto.Marshal(kv)
			if err != nil {
				panic(err)
			}
			raftProposal := &mr.RaftProposal{
				Session: *cs,
				Data:    data,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resp, err := client.Propose(ctx, raftProposal)
			if err != nil {
				t.Errorf("failed to make proposal %v", err)
			} else {
				if resp.Result != uint64(len(data)) {
					t.Errorf("result %d, want %d", resp.Result, uint64(len(data)))
				}
			}
			cs.ProposalCompleted()
			ri := &mr.RaftReadIndex{
				ClusterId: mtClusterID,
				Data:      []byte(kv.Key),
			}
			resp, err = client.Read(ctx, ri)
			if err != nil {
				t.Errorf("failed to read, %v", err)
			} else {
				if string(resp.Data) != kv.Val {
					t.Errorf("got %s, want %s", string(resp.Data), kv.Val)
				}
			}
			ri2 := &mr.RaftReadIndex{
				ClusterId: mtClusterID,
				Data:      []byte("no-such-key"),
			}
			resp, err = client.Read(ctx, ri2)
			if err != nil {
				t.Errorf("failed to read, %v", err)
			} else {
				if string(resp.Data) != "" {
					t.Errorf("got %s, want %s", string(resp.Data), "")
				}
			}
		}
	}

	runMultiraftAPITest(t, false, tester)
	runMultiraftAPITest(t, true, tester)
}

func getTestKVData() []byte {
	key := "test-key"
	val := "test-data"
	kv := &kvpb.PBKV{
		Key: key,
		Val: val,
	}
	data, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return data
}

func getTestKVData2() []byte {
	key := "test-key-2"
	val := "test-data-2"
	kv := &kvpb.PBKV{
		Key: key,
		Val: val,
	}
	data, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return data
}

func TestMultiraftAPIRejectUnregisteredClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tester := func(t *testing.T, cs *client.Session, client mr.NodehostAPIClient) {
		data := getTestKVData()
		raftProposal := &mr.RaftProposal{
			Session: *cs,
			Data:    data,
		}
		// change the client id to make sure it is not registered
		raftProposal.Session.ClientID = 12345
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := client.Propose(ctx, raftProposal)
		if err == nil {
			t.Errorf("unregistered client didn't return error")
		} else {
			if grpc.Code(err) != codes.InvalidArgument {
				t.Errorf("got %d, want %d", grpc.Code(err), codes.InvalidArgument)
			}
		}
	}

	runMultiraftAPITest(t, false, tester)
	runMultiraftAPITest(t, true, tester)
}

func TestMultiraftAPIReturnTheSameResultForSameSeriesID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tester := func(t *testing.T, cs *client.Session, client mr.NodehostAPIClient) {
		data := getTestKVData()
		raftProposal := &mr.RaftProposal{
			Session: *cs,
			Data:    data,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := client.Propose(ctx, raftProposal)
		if err != nil {
			t.Errorf("failed to make proposal")
		}
		if resp.Result != uint64(len(data)) {
			t.Errorf("unexpected result %d, want %d", resp.Result, uint64(len(data)))
		}
		// use the same client session, make a proposal with a different payload
		data2 := getTestKVData2()
		if len(data) == len(data2) {
			t.Errorf("payload length didn't change")
		}
		raftProposal2 := &mr.RaftProposal{
			Session: *cs,
			Data:    data2,
		}
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err = client.Propose(ctx, raftProposal2)
		if err != nil {
			t.Errorf("failed to make the second proposal")
		}
		// Result should be the same
		// the new data won't even be passed to the data store.
		if resp.Result != uint64(len(data)) {
			t.Errorf("unexpected result %d, want %d", resp.Result, uint64(len(data)))
		}
	}
	runMultiraftAPITest(t, false, tester)
	runMultiraftAPITest(t, true, tester)
}

func TestMultiraftAPITimeoutProposalWithRespondedSeriesID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tester := func(t *testing.T, cs *client.Session, client mr.NodehostAPIClient) {
		data := getTestKVData()
		raftProposal := &mr.RaftProposal{
			Session: *cs,
			Data:    data,
		}
		// set series id and responded to
		raftProposal.Session.SeriesID = 2
		raftProposal.Session.RespondedTo = 1
		// this should be fine
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := client.Propose(ctx, raftProposal)
		if err != nil {
			t.Errorf("failed to make proposal")
		}
		if resp.Result != uint64(len(data)) {
			t.Errorf("unexpected result %d, want %d", resp.Result, uint64(len(data)))
		}
		// use the same client session, make a proposal with a different payload
		data2 := getTestKVData2()
		if len(data) == len(data2) {
			t.Errorf("payload length didn't change")
		}
		raftProposal2 := &mr.RaftProposal{
			Session: *cs,
			Data:    data2,
		}
		// use a series id which has been previously responded to.
		raftProposal2.Session.SeriesID = 1
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err = client.Propose(ctx, raftProposal2)
		if err == nil {
			t.Errorf("propose didn't timeout")
		} else {
			if grpc.Code(err) != codes.DeadlineExceeded {
				t.Errorf("got %d, want %d",
					grpc.Code(err), codes.DeadlineExceeded)
			}
		}
	}

	runMultiraftAPITest(t, false, tester)
	runMultiraftAPITest(t, true, tester)
}
