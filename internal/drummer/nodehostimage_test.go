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
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
)

// TODO:
// the following two tests were disabled. they need to be reviewed
func MultiNodeHostUpdate(t *testing.T) {
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	pl := pb.LogInfo{
		ClusterId: 100,
		NodeId:    200,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
		PlogInfo:    []pb.LogInfo{pl},
	}
	mnh := newMultiNodeHost()
	mnh.update(nhi)
	if len(mnh.Nodehosts) != 1 {
		t.Errorf("nodehosts sz=%d, want 1", len(mnh.Nodehosts))
	}
	nhs, ok := mnh.Nodehosts["a2"]
	if !ok {
		t.Fatalf("failed to get the node host spec rec")
	}
	if !nhs.hasCluster(1) {
		t.Errorf("has cluster failed")
	}
	if !nhs.hasLog(100, 200) {
		t.Errorf("has log failed")
	}
	if nhs.Address != "a2" {
		t.Errorf("unexpected node host")
	}
	if nhs.Tick != 100 {
		t.Errorf("tick=%d, want 100", nhs.Tick)
	}
	if nhs.Region != "region-1" {
		t.Errorf("region=%s, want region-1", nhs.Region)
	}
	if nhs.available(100+nodeHostTTL+1) ||
		!nhs.available(100+nodeHostTTL) {
		t.Errorf("available failed")
	}
	if len(mnh.toArray()) != 1 {
		t.Errorf("len(mnh.ToArray())=%d, want 1", len(mnh.toArray()))
	}
}

func MultiNodeHostUpdateCanSync(t *testing.T) {
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	ci2 := pb.ClusterInfo{
		ClusterId:         2,
		NodeId:            5,
		IsLeader:          true,
		Nodes:             map[uint64]string{5: "a5", 6: "a6", 7: "a7"},
		ConfigChangeIndex: 1,
	}
	ci3 := pb.ClusterInfo{
		ClusterId:         3,
		NodeId:            10,
		IsLeader:          true,
		Nodes:             map[uint64]string{10: "a10", 11: "a11", 12: "a12"},
		ConfigChangeIndex: 1,
	}
	pl := pb.LogInfo{
		ClusterId: 1,
		NodeId:    2,
	}
	pl2 := pb.LogInfo{
		ClusterId: 2,
		NodeId:    5,
	}
	pl3 := pb.LogInfo{
		ClusterId: 3,
		NodeId:    10,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci, ci2},
		PlogInfo:    []pb.LogInfo{pl, pl2},
	}
	mnh := newMultiNodeHost()
	mnh.update(nhi)
	nhs, ok := mnh.Nodehosts["a2"]
	if !ok {
		t.Fatalf("failed to get the node host spec rec")
	}
	if nhs.Tick != 100 {
		t.Errorf("tick=%d, want 100", nhs.Tick)
	}
	if len(nhs.Clusters) != 2 {
		t.Errorf("cluster sz=%d, want 2", len(nhs.Clusters))
	}
	if len(nhs.PersistentLog) != 2 {
		t.Errorf("persistent log sz=%d, want 2", len(nhs.PersistentLog))
	}
	nhi = pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    200,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci2, ci3},
		PlogInfo:    []pb.LogInfo{pl2, pl3},
	}
	mnh.update(nhi)
	nhs, ok = mnh.Nodehosts["a2"]
	if !ok {
		t.Fatalf("failed to get the node host spec rec")
	}
	if nhs.Tick != 200 {
		t.Errorf("tick=%d, want 200", nhs.Tick)
	}
	if len(nhs.Clusters) != 2 {
		t.Errorf("cluster sz=%d, want 2", len(nhs.Clusters))
	}
	if len(nhs.PersistentLog) != 2 {
		t.Errorf("persistent log sz=%d, want 2", len(nhs.PersistentLog))
	}
	_, c2a := nhs.Clusters[2]
	_, c3a := nhs.Clusters[3]
	if !c2a || !c3a {
		t.Errorf("clusters unexpected")
	}
	nhs.toPersistentLogMap()
	_, n5a := nhs.persistentLogMap[pl2]
	_, n10a := nhs.persistentLogMap[pl3]
	if !n5a || !n10a {
		t.Errorf("unexpected persistent log info")
	}
}

func TestMultiNodeHostDeepCopy(t *testing.T) {
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	pl := pb.LogInfo{
		ClusterId: 100,
		NodeId:    200,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress: "a2",
		LastTick:    100,
		Region:      "region-1",
		ClusterInfo: []pb.ClusterInfo{ci},
		PlogInfo:    []pb.LogInfo{pl},
	}
	mnh := newMultiNodeHost()
	mnh.update(nhi)
	dcmnh := mnh.deepCopy()
	if !reflect.DeepEqual(dcmnh, mnh) {
		t.Errorf("deep equal failed")
	}
	dcmnh.Nodehosts["a2"].Tick = 200
	if dcmnh.Nodehosts["a2"].Tick == mnh.Nodehosts["a2"].Tick {
		t.Errorf("deep copy is not actually deep copy")
	}
}

func TestMultiNodeHostCanForgetOldLogInfo(t *testing.T) {
	ci := pb.ClusterInfo{
		ClusterId:         1,
		NodeId:            2,
		IsLeader:          true,
		Nodes:             map[uint64]string{1: "a1", 2: "a2", 3: "a3"},
		ConfigChangeIndex: 1,
	}
	pl := pb.LogInfo{
		ClusterId: 100,
		NodeId:    200,
	}
	nhi := pb.NodeHostInfo{
		RaftAddress:      "a2",
		LastTick:         100,
		Region:           "region-1",
		PlogInfoIncluded: true,
		ClusterInfo:      []pb.ClusterInfo{ci},
		PlogInfo:         []pb.LogInfo{pl},
	}
	mnh := newMultiNodeHost()
	mnh.update(nhi)
	if !mnh.Nodehosts["a2"].hasLog(pl.ClusterId, pl.NodeId) {
		t.Errorf("plog info not found")
	}
	// when PlogInfoIncluded flag is false
	// update() won't update the persistentLog recorded
	nhi.PlogInfoIncluded = false
	nhi.PlogInfo = []pb.LogInfo{}
	mnh.update(nhi)
	if !mnh.Nodehosts["a2"].hasLog(pl.ClusterId, pl.NodeId) {
		t.Errorf("plog info not found")
	}
	pl2 := pb.LogInfo{
		ClusterId: 200,
		NodeId:    500,
	}
	nhi.PlogInfoIncluded = true
	nhi.PlogInfo = []pb.LogInfo{pl2}
	mnh.update(nhi)
	if mnh.Nodehosts["a2"].hasLog(pl.ClusterId, pl.NodeId) {
		t.Errorf("plog info unexpected")
	}
	if !mnh.Nodehosts["a2"].hasLog(pl2.ClusterId, pl2.NodeId) {
		t.Errorf("plog info not found")
	}
}
