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

// +build !dragonboat_slowtest
// +build !dragonboat_monkeytest

package drummer

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/statemachine"
)

func TestDBCanBeSnapshottedAndRestored(t *testing.T) {
	ci1 := pb.Cluster{
		ClusterId: 100,
		AppName:   "noop",
		Members:   []uint64{1, 2, 3},
	}
	ci2 := pb.Cluster{
		ClusterId: 200,
		AppName:   "noop",
		Members:   []uint64{1, 2, 3},
	}
	clusters := make(map[uint64]*pb.Cluster)
	clusters[100] = &ci1
	clusters[200] = &ci2
	testData := make([]byte, 32)
	rand.Read(testData)
	kvMap := make(map[string][]byte)
	kvMap["key1"] = []byte("value1")
	kvMap["key2"] = []byte("value2")
	kvMap["key3"] = testData
	d := &DB{
		ClusterID:     0,
		NodeID:        0,
		Clusters:      clusters,
		KVMap:         kvMap,
		ClusterImage:  newMultiCluster(),
		NodeHostImage: newMultiNodeHost(),
		NodeHostInfo:  make(map[string]pb.NodeHostInfo),
		Requests:      make(map[string][]pb.NodeHostRequest),
		Outgoing:      make(map[string][]pb.NodeHostRequest),
	}
	testRequestsCanBeUpdated(t, d)
	testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t, d)
	w := bytes.NewBufferString("")
	err := d.SaveSnapshot(w, nil, nil)
	if err != nil {
		t.Fatalf("failed to get snapshot")
	}
	hash1, _ := d.GetHash()
	r := bytes.NewBuffer(w.Bytes())
	newDB := &DB{}
	err = newDB.RecoverFromSnapshot(r, nil, nil)
	if err != nil {
		t.Fatalf("failed to recover from snapshot")
	}
	if !reflect.DeepEqual(newDB, d) {
		t.Errorf("recovered drummerdb not equal to the original, %v \n\n\n %v",
			newDB, d)
	}
	hash2, _ := newDB.GetHash()
	if hash1 != hash2 {
		t.Errorf("hash value mismatch")
	}
}

func testTickCanBeIncreased(t *testing.T, db statemachine.IStateMachine) {
	update := pb.Update{
		Type: pb.Update_TICK,
	}
	data, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	v1, err := db.Update(data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	v2, err := db.Update(data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	v3, err := db.Update(data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if v2.Value != v1.Value+tickIntervalSecond ||
		v3.Value != v2.Value+tickIntervalSecond ||
		v3.Value != 3*tickIntervalSecond {
		t.Errorf("not returning expected tick value")
	}
	if db.(*DB).Tick != 3*tickIntervalSecond {
		t.Errorf("unexpected tick value")
	}
	v4, _ := db.Update(data)
	if v4.Value != 4*tickIntervalSecond || db.(*DB).Tick != 4*tickIntervalSecond {
		t.Errorf("unexpected tick value")
	}
}

func TestTickCanBeIncreased(t *testing.T) {
	db := NewDB(0, 0)
	testTickCanBeIncreased(t, db)
}

func testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t *testing.T,
	db statemachine.IStateMachine) {
	update := pb.Update{
		Type: pb.Update_TICK,
	}
	tickData, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		RPCAddress:  "rpca1",
		Region:      "r1",
		ClusterInfo: []pb.ClusterInfo{
			{
				ClusterId: 1,
				NodeId:    2,
			},
			{
				ClusterId: 2,
				NodeId:    3,
				Nodes:     map[uint64]string{3: "a2", 4: "a1", 5: "a3"},
			},
		},
	}
	update = pb.Update{
		Type:         pb.Update_NODEHOST_INFO,
		NodehostInfo: nhi,
	}
	data, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	if _, err := db.Update(tickData); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := db.Update(tickData); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := db.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	clusterInfo := db.(*DB).ClusterImage
	nodehostInfo := db.(*DB).NodeHostImage
	if len(clusterInfo.Clusters) != 2 {
		t.Errorf("cluster info not updated")
	}
	if len(nodehostInfo.Nodehosts) != 1 {
		t.Errorf("nodehost info not updated")
	}
	nhs, ok := nodehostInfo.Nodehosts["a2"]
	if !ok {
		t.Errorf("nodehost not found")
	}
	if nhs.Tick != 2*tickIntervalSecond {
		t.Errorf("unexpected nodehost tick value")
	}
	ci, ok := clusterInfo.Clusters[2]
	if !ok {
		t.Errorf("cluster not found")
	}
	if ci.ClusterID != 2 {
		t.Errorf("unexpected cluster id value")
	}
	n, ok := ci.Nodes[3]
	if !ok {
		t.Errorf("node not found, %v", ci)
	}
	if n.Tick != 2*tickIntervalSecond {
		t.Errorf("unexpected node tick value")
	}
}

func TestNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t *testing.T) {
	db := NewDB(0, 0)
	testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t, db)
}

func testTickValue(t *testing.T, db statemachine.IStateMachine,
	addr string, clusterID uint64, nodeID uint64, tick uint64) {
	clusterInfo := db.(*DB).ClusterImage
	nodehostInfo := db.(*DB).NodeHostImage
	nhs, ok := nodehostInfo.Nodehosts[addr]
	if !ok {
		t.Fatalf("nodehost %s not found", addr)
	}
	if nhs.Tick != tick*tickIntervalSecond {
		t.Errorf("unexpected nodehost tick value, got %d, want %d", nhs.Tick, tick)
	}
	ci, ok := clusterInfo.Clusters[clusterID]
	if !ok {
		t.Fatalf("cluster %d not found", clusterID)
	}
	n, ok := ci.Nodes[nodeID]
	if !ok {
		t.Fatalf("node %d not found", nodeID)
	}
	if n.Tick != tick*tickIntervalSecond {
		t.Errorf("unexpected node tick value, got %d, want %d", n.Tick, tick)
	}
}

func TestTickUpdatedAsExpected(t *testing.T) {
	db := NewDB(0, 0)
	update := pb.Update{
		Type: pb.Update_TICK,
	}
	tickData, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		RPCAddress:  "rpca1",
		Region:      "r1",
		ClusterInfo: []pb.ClusterInfo{
			{
				ClusterId: 1,
				NodeId:    2,
				Nodes:     map[uint64]string{2: "a2", 3: "a1", 4: "a3"},
			},
		},
	}
	update = pb.Update{
		Type:         pb.Update_NODEHOST_INFO,
		NodehostInfo: nhi,
	}
	nhiData, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	for i := uint64(0); i < 10; i++ {
		if _, err := db.Update(tickData); err != nil {
			t.Fatalf("%v", err)
		}
		if _, err := db.Update(nhiData); err != nil {
			t.Fatalf("%v", err)
		}
		testTickValue(t, db, "a2", 1, 2, i+1)
	}
	for i := uint64(0); i < 10; i++ {
		if _, err := db.Update(nhiData); err != nil {
			t.Fatalf("%v", err)
		}
		testTickValue(t, db, "a2", 1, 2, 10)
	}
	for i := uint64(0); i < 10; i++ {
		if _, err := db.Update(tickData); err != nil {
			t.Fatalf("%v", err)
		}
		if _, err := db.Update(nhiData); err != nil {
			t.Fatalf("%v", err)
		}
		testTickValue(t, db, "a2", 1, 2, 11+i)
	}
}

func TestNodeHostInfoUpdateMovesRequests(t *testing.T) {
	db := NewDB(0, 0)
	testRequestsCanBeUpdated(t, db)
	testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t, db)
	reqs := db.(*DB).Requests
	if len(reqs) != 1 {
		t.Errorf("unexpected reqs size")
	}
	outgoings := db.(*DB).Outgoing
	if len(outgoings) != 1 {
		t.Errorf("unexpected outgoing size")
	}
	r, ok := outgoings["a2"]
	if !ok {
		t.Errorf("requests not in the outgoing map")
	}
	if len(r) != 2 {
		t.Errorf("requests size mismatch")
	}
	_, ok = reqs["a2"]
	if ok {
		t.Errorf("not expected to have reqs for a2 in requests")
	}
	q, ok := reqs["a1"]
	if !ok {
		t.Errorf("requests missing for a1")
	}
	if len(q) != 1 {
		t.Errorf("unexpected size")
	}
}

func testRequestsCanBeUpdated(t *testing.T, db statemachine.IStateMachine) {
	r1 := pb.NodeHostRequest{
		RaftAddress:       "a1",
		InstantiateNodeId: 1,
	}
	r2 := pb.NodeHostRequest{
		RaftAddress:       "a2",
		InstantiateNodeId: 2,
	}
	r3 := pb.NodeHostRequest{
		RaftAddress:       "a2",
		InstantiateNodeId: 3,
	}
	update := pb.Update{
		Type: pb.Update_REQUESTS,
		Requests: pb.NodeHostRequestCollection{
			Requests: []pb.NodeHostRequest{r1, r2, r3},
		},
	}
	data, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	v, _ := db.Update(data)
	if v.Value != 3 {
		t.Errorf("unexpected returned value")
	}
	reqs := db.(*DB).Requests
	if len(reqs) != 2 {
		t.Errorf("reqs size unexpected")
	}
	q1, ok := reqs["a1"]
	if !ok {
		t.Errorf("requests for a1 not found")
	}
	if len(q1) != 1 {
		t.Errorf("unexpected request size for a1")
	}
	q2, ok := reqs["a2"]
	if !ok {
		t.Errorf("requests for a2 not found")
	}
	if len(q2) != 2 {
		t.Errorf("unexpected request size for a2")
	}
}

func TestReqeustsCanBeUpdated(t *testing.T) {
	db := NewDB(0, 0)
	testRequestsCanBeUpdated(t, db)
}

func TestRequestLookup(t *testing.T) {
	db := NewDB(0, 0)
	testRequestsCanBeUpdated(t, db)
	lookup := pb.LookupRequest{
		Type:    pb.LookupRequest_REQUESTS,
		Address: "a2",
	}
	data, err := lookup.Marshal()
	if err != nil {
		panic(err)
	}
	result, _ := db.Lookup(data)
	var resp pb.LookupResponse
	if err := resp.Unmarshal(result.([]byte)); err != nil {
		panic(err)
	}
	if len(resp.Requests.Requests) != 0 {
		t.Errorf("unexpected requests count, want 0, got %d",
			len(resp.Requests.Requests))
	}
	testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t, db)
	result, _ = db.Lookup(data)
	if err := resp.Unmarshal(result.([]byte)); err != nil {
		panic(err)
	}
	reqs := resp.Requests.Requests
	if len(reqs) != 2 {
		t.Errorf("unexpected requests count, want 2, got %d",
			len(resp.Requests.Requests))
	}
	if reqs[0].InstantiateNodeId != 2 || reqs[1].InstantiateNodeId != 3 {
		t.Errorf("requests not ordered in the expected way")
	}
}

func TestSchedulerContextLookup(t *testing.T) {
	db := NewDB(0, 0)
	regions := pb.Regions{
		Region: []string{"v1", "v2"},
		Count:  []uint64{123, 345},
	}
	regionData, err := regions.Marshal()
	if err != nil {
		panic(err)
	}
	var kv pb.KV
	kv.Value = string(regionData)
	rd, err := kv.Marshal()
	if err != nil {
		panic(err)
	}
	db.(*DB).KVMap[regionsKey] = rd
	db.(*DB).Clusters[100] = &pb.Cluster{
		ClusterId: 100,
	}
	db.(*DB).Clusters[200] = &pb.Cluster{
		ClusterId: 200,
	}
	db.(*DB).Clusters[300] = &pb.Cluster{
		ClusterId: 300,
	}
	testRequestsCanBeUpdated(t, db)
	testNodeHostInfoUpdateUpdatesClusterAndNodeHostImage(t, db)
	lookup := pb.LookupRequest{
		Type: pb.LookupRequest_SCHEDULER_CONTEXT,
	}
	data, err := lookup.Marshal()
	if err != nil {
		panic(err)
	}
	result, _ := db.Lookup(data)
	var sc schedulerContext
	if err := json.Unmarshal(result.([]byte), &sc); err != nil {
		panic(err)
	}
	if sc.Tick != 2*tickIntervalSecond {
		t.Errorf("tick %d, want 2", sc.Tick)
	}
	clusterInfo := sc.ClusterImage
	nodehostInfo := sc.NodeHostImage
	if len(sc.ClusterImage.Clusters) != 2 {
		t.Fatalf("cluster info not updated, sz: %d", len(clusterInfo.Clusters))
	}
	if len(nodehostInfo.Nodehosts) != 1 {
		t.Fatalf("nodehost info not updated")
	}
	nhs, ok := nodehostInfo.Nodehosts["a2"]
	if !ok {
		t.Errorf("nodehost not found")
	}
	if nhs.Tick != 2*tickIntervalSecond {
		t.Errorf("unexpected nodehost tick value")
	}
	ci, ok := clusterInfo.Clusters[2]
	if !ok {
		t.Fatalf("cluster not found")
	}
	if ci.ClusterID != 2 {
		t.Errorf("unexpected cluster id value")
	}
	_, ok = ci.Nodes[3]
	if !ok {
		t.Fatalf("node not found, %v", ci)
	}
	if len(sc.Regions.Region) != 2 || len(sc.Regions.Count) != 2 {
		t.Errorf("regions not returned")
	}
	if len(sc.Clusters) != 3 {
		t.Errorf("clusters not returned")
	}
}

func TestClusterCanBeUpdatedAndLookedUp(t *testing.T) {
	change := pb.Change{
		Type:      pb.Change_CREATE,
		ClusterId: 123,
		Members:   []uint64{1, 2, 3},
		AppName:   "noop",
	}
	du := pb.Update{
		Type:   pb.Update_CLUSTER,
		Change: change,
	}
	data, err := du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	oldData := data
	d := NewDB(0, 0)
	code, _ := d.Update(data)
	if code.Value != DBUpdated {
		t.Errorf("code %d, want %d", code, DBUpdated)
	}
	// use the same input to update the drummer db again
	code, _ = d.Update(data)
	if code.Value != ClusterExists {
		t.Errorf("code %d, want %d", code, ClusterExists)
	}
	// set the bootstrapped flag
	bkv := pb.KV{
		Key:       bootstrappedKey,
		Value:     "bootstrapped",
		Finalized: true,
	}
	du = pb.Update{
		Type:     pb.Update_KV,
		KvUpdate: bkv,
	}
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	if !d.(*DB).bootstrapped() {
		t.Errorf("not bootstrapped")
	}
	code, _ = d.Update(oldData)
	if code.Value != DBBootstrapped {
		t.Errorf("code %d, want %d", code, DBBootstrapped)
	}
	if len(d.(*DB).Clusters) != 1 {
		t.Fatalf("failed to create cluster")
	}
	c := d.(*DB).Clusters[123]
	cluster := &pb.Cluster{
		ClusterId: 123,
		AppName:   "noop",
		Members:   []uint64{1, 2, 3},
	}
	if !reflect.DeepEqual(cluster, c) {
		t.Errorf("cluster recs not equal, \n%v\n%v", cluster, c)
	}
	req := pb.LookupRequest{
		Type: pb.LookupRequest_CLUSTER,
	}
	data, err = req.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal lookup request")
	}
	result, err := d.Lookup(data)
	if err != nil {
		t.Fatalf("lookup failed %v", err)
	}
	var resp pb.LookupResponse
	err = resp.Unmarshal(result.([]byte))
	if err != nil {
		t.Fatalf("failed to unmarshal")
	}
	if len(resp.Clusters) != 1 {
		t.Fatalf("not getting cluster back")
	}
	resqc := resp.Clusters[0]
	if resqc.ClusterId != 123 {
		t.Errorf("cluster id %d, want 123", resqc.ClusterId)
	}
	if len(resqc.Members) != 3 {
		t.Errorf("len(members)=%d, want 3", len(resqc.Members))
	}
	if resqc.AppName != "noop" {
		t.Errorf("app name %s, want noop", resqc.AppName)
	}
}

func TestKVMapCanBeUpdatedAndLookedUpForFinalizedValue(t *testing.T) {
	kv := pb.KV{
		Key:       "test-key",
		Value:     "test-data",
		Finalized: true,
	}
	du := pb.Update{
		Type:     pb.Update_KV,
		KvUpdate: kv,
	}
	data, err := du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	d := NewDB(0, 0)
	code, err := d.Update(data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if code.Value != DBKVUpdated {
		t.Errorf("code %d, want %d", code, DBKVUpdated)
	}
	// apply the same update again, suppose to be rejected
	code, err = d.Update(data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if code.Value != DBKVFinalized {
		t.Errorf("code %d, want %d", code, DBKVFinalized)
	}
	if len(d.(*DB).KVMap) != 1 {
		t.Errorf("kv map not updated")
	}
	v, ok := d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// v is the marshaled how KV rec
	var kvrec pb.KV
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
	// try to update the finalized value
	du.KvUpdate.Value = "test-data-2"
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	v, ok = d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// finalized value not changed
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
}

func TestKVMapCanBeUpdatedAndLookedUpForNotFinalizedValue(t *testing.T) {
	kv := pb.KV{
		Key:        "test-key",
		Value:      "test-data",
		InstanceId: 1000,
	}
	du := pb.Update{
		Type:     pb.Update_KV,
		KvUpdate: kv,
	}
	data, err := du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	d := NewDB(0, 0)
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	if len(d.(*DB).KVMap) != 1 {
		t.Errorf("kv map not updated")
	}
	_, ok := d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// with a different instance id, the value can not be updated
	du.KvUpdate.Value = "test-data-2"
	du.KvUpdate.InstanceId = 2000
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	v, ok := d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// finalized value not changed
	var kvrec pb.KV
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
	// with the same instance id, the value should be updated
	du.KvUpdate.InstanceId = 1000
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	v, ok = d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// finalized value not changed
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data-2" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
	// with old instance id value not matching the instance id, value
	// should not be updated
	du.KvUpdate.OldInstanceId = 3000
	du.KvUpdate.InstanceId = 0
	du.KvUpdate.Value = "test-data-3"
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	v, ok = d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// finalized value not changed
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data-2" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
	// with old instance id value matching the instance id, value should
	// be updated
	du.KvUpdate.OldInstanceId = 1000
	du.KvUpdate.InstanceId = 0
	du.KvUpdate.Value = "test-data-3"
	data, err = du.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal")
	}
	if _, err := d.Update(data); err != nil {
		t.Fatalf("%v", err)
	}
	v, ok = d.(*DB).KVMap["test-key"]
	if !ok {
		t.Errorf("kv map value not expected")
	}
	// finalized value not changed
	if err := kvrec.Unmarshal([]byte(v)); err != nil {
		panic(err)
	}
	if kvrec.Value != "test-data-3" {
		t.Errorf("value %s, want test-data", kvrec.Value)
	}
}

func expectPanic(t *testing.T) {
	if r := recover(); r == nil {
		t.Fatalf("expected to panic, but it didn't panic")
	}
}

func TestDBCanBeMarkedAsFailed(t *testing.T) {
	db := NewDB(0, 1)
	db.(*DB).Failed = true
	defer expectPanic(t)
	if _, err := db.Update(nil); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestLaunchDeadlineIsChecked(t *testing.T) {
	db := NewDB(0, 1)
	db.(*DB).LaunchDeadline = 100
	defer expectPanic(t)
	for i := 0; i <= 100; i++ {
		db.(*DB).applyTickUpdate()
	}
}

func TestLaunchRequestSetsTheLaunchFlag(t *testing.T) {
	db := NewDB(0, 1)
	r1 := pb.NodeHostRequest{
		RaftAddress:       "a1",
		InstantiateNodeId: 1,
		Join:              false,
		Restore:           false,
		Change: pb.Request{
			Type: pb.Request_CREATE,
		},
	}
	update := pb.Update{
		Type: pb.Update_REQUESTS,
		Requests: pb.NodeHostRequestCollection{
			Requests: []pb.NodeHostRequest{r1},
		},
	}
	data, err := update.Marshal()
	if err != nil {
		panic(err)
	}
	for i := 0; i <= 100; i++ {
		db.(*DB).applyTickUpdate()
	}
	if db.(*DB).launched() {
		t.Fatalf("launched flag already unexpectedly set")
	}
	v, _ := db.Update(data)
	if v.Value != 1 {
		t.Errorf("unexpected returned value")
	}
	if !db.(*DB).launched() {
		t.Fatalf("launched flag is not set")
	}
	if db.(*DB).LaunchDeadline != (101+launchDeadlineTick)*tickIntervalSecond {
		t.Errorf("deadline not set, deadline %d, tick %d",
			db.(*DB).LaunchDeadline, launchDeadlineTick)
	}
	if db.(*DB).Failed {
		t.Errorf("failed flag already set")
	}
}

func TestLaunchDeadlineIsClearedOnceAllNodesAreLaunched(t *testing.T) {
	db := NewDB(0, 1)
	db.(*DB).Clusters[1] = nil
	db.(*DB).LaunchDeadline = 100
	ci1 := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            1,
		Nodes:             map[uint64]string{1: "a1", 2: "a2"},
		ConfigChangeIndex: 3,
	}
	u1 := pb.Update{
		NodehostInfo: pb.NodeHostInfo{
			RaftAddress: "a1",
			ClusterInfo: []pb.ClusterInfo{ci1},
		},
		Type: pb.Update_NODEHOST_INFO,
	}
	data1, err := u1.Marshal()
	if err != nil {
		panic(err)
	}
	ci2 := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		Nodes:             map[uint64]string{1: "a1", 2: "a2"},
		ConfigChangeIndex: 3,
	}
	u2 := pb.Update{
		NodehostInfo: pb.NodeHostInfo{
			RaftAddress: "a2",
			ClusterInfo: []pb.ClusterInfo{ci2},
		},
		Type: pb.Update_NODEHOST_INFO,
	}
	data2, err := u2.Marshal()
	if err != nil {
		panic(err)
	}
	db.(*DB).applyTickUpdate()
	if _, err := db.Update(data1); err != nil {
		t.Fatalf("%v", err)
	}
	m := db.(*DB).getLaunchedClusters()
	if len(m) != 0 {
		t.Fatalf("launched cluster is not 0")
	}
	if db.(*DB).LaunchDeadline != 100 {
		t.Errorf("deadline is not cleared")
	}
	if _, err := db.Update(data2); err != nil {
		t.Fatalf("%v", err)
	}
	m = db.(*DB).getLaunchedClusters()
	if len(m) != 1 {
		t.Fatalf("launched cluster is not 1")
	}
	if db.(*DB).LaunchDeadline != 0 {
		t.Errorf("deadline is not cleared")
	}
}
