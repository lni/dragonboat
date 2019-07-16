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
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
)

func testDrummerIsAwareOfNodeHosts(t *testing.T, tls bool) {
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	if tls {
		for _, n := range drummerNodes {
			n.mutualTLS = true
		}
		for _, n := range nodehostNodes {
			n.mutualTLS = true
		}
	}
	startTestNodes(drummerNodes, dl)
	startTestNodes(nodehostNodes, dl)
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	waitForStableNodes(drummerNodes, 25)
	waitForStableNodes(nodehostNodes, 25)

	time.Sleep(time.Duration(3*NodeHostInfoReportSecond) * time.Second)
	for _, node := range drummerNodes {
		mnh, err := node.GetNodeHostInfo()
		if err != nil {
			t.Fatalf("failed to get nodehost info")
		}
		if mnh.size() != len(nodehostNodes) {
			t.Errorf("node host count %d, want %d",
				mnh.size(), len(nodehostNodes))
		}
	}
}

func TestDrummerIsAwareOfNodeHosts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testDrummerIsAwareOfNodeHosts(t, false)
}

func TestDrummerServerSupportsTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testDrummerIsAwareOfNodeHosts(t, true)
}

func testClusterCanBeLaunched(t *testing.T, dl *mtAddressList,
	drummerNodes []*testNode, nodehostNodes []*testNode) {
	startTestNodes(drummerNodes, dl)
	startTestNodes(nodehostNodes, dl)
	waitForStableNodes(drummerNodes, 25)
	waitForStableNodes(nodehostNodes, 25)
	time.Sleep(time.Duration(3*NodeHostInfoReportSecond) * time.Second)
	if !submitSimpleTestJob(dl, false) {
		t.Errorf("failed to submit the test job")
	}
	waitTimeSec := (loopIntervalFactor + 5) * NodeHostInfoReportSecond
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	for _, node := range drummerNodes {
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get nodehost info")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
	}
}

func TestDrummerIsAwareOfClusterInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	testClusterCanBeLaunched(t, dl, drummerNodes, nodehostNodes)
}

func TestDrummerCanRestoreUnavailableClusters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	testClusterCanBeLaunched(t, dl, drummerNodes, nodehostNodes)

	// stop the second/third nodehost instances
	nodehostNodes[1].Stop()
	nodehostNodes[2].Stop()
	waitTimeSec := nodeHostTTL + NodeHostInfoReportSecond
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	// make sure the second/third nodehost instances are down
	// and the cluster is considered as unavailable
	for _, node := range drummerNodes {
		_, tick, err := node.GetMultiClusterAndTick()
		if err != nil {
			t.Fatalf("failed to get cluster info and tick")
		}
		mnh, err := node.GetNodeHostInfo()
		if err != nil {
			t.Fatalf("failed to get node host info")
		}
		n2 := mnh.get(dl.nodehostAddressList[1])
		n3 := mnh.get(dl.nodehostAddressList[2])
		if n2.available(tick) {
			t.Errorf("nodehost 2 is not unavailable, node tick %d, tick %d",
				n2.Tick, tick)
		}
		if n3.available(tick) {
			t.Errorf("nodehost 3 is not unavailable, node tick %d, tick %d",
				n3.Tick, tick)
		}
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get cluster info")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
		uc := mc.getUnavailableClusters(tick)
		if len(uc) != 1 {
			t.Errorf("suppose to have an unavailable cluster, got %d", len(uc))
		}
	}
	// restart the second/third nodehost instances
	nodehostNodes[1].Start(dl)
	nodehostNodes[2].Start(dl)
	waitTimeSec = (loopIntervalFactor + 4) * NodeHostInfoReportSecond
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	// make sure the second/third nodehost instances have recovered
	// and the cluster is reported as available
	for _, node := range drummerNodes {
		_, tick, err := node.GetMultiClusterAndTick()
		if err != nil {
			t.Fatalf("failed to get cluster info and tick")
		}
		mnh, err := node.GetNodeHostInfo()
		if err != nil {
			t.Fatalf("failed to get nodehost info")
		}
		n2 := mnh.get(dl.nodehostAddressList[1])
		n3 := mnh.get(dl.nodehostAddressList[2])
		if !n2.available(tick) {
			t.Errorf("nodehost 2 is unavailable, suppose to be available")
		}
		if !n3.available(tick) {
			t.Errorf("nodehost 3 is unavailable, suppose to be available")
		}
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get cluster info")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
		uc := mc.getUnavailableClusters(tick)
		if len(uc) != 0 {
			t.Errorf("unavailable cluster count %d, want 0", len(uc))
		}
	}
}

func TestDrummerReschedulesFailedRaftNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	defer stopTestNodes(drummerNodes)
	defer stopTestNodes(nodehostNodes)
	testClusterCanBeLaunched(t, dl, drummerNodes, nodehostNodes)

	// stop the third nodehost and wait for enough time
	nodehostNodes[2].Stop()
	waitTimeSec := nodeHostTTL + loopIntervalSecond*6
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	// make sure the cluster is stable with 3 raft nodes
	for _, node := range drummerNodes {
		mc, tick, err := node.GetMultiClusterAndTick()
		if err != nil {
			t.Fatalf("failed to get cluster info and tick")
		}
		if mc.size() != 1 {
			t.Errorf("cluster count %d, want %d", mc.size(), 1)
		}
		r := mc.getClusterForRepair(tick)
		if len(r) != 0 {
			t.Errorf("to be repaired cluster %d, want 0", len(r))
		}
		uc := mc.getUnavailableClusters(tick)
		if len(uc) != 0 {
			t.Errorf("unavailable cluster %d, want 0", len(uc))
		}
	}
	nodehostNodes[2].Start(dl)
	waitForStableNodes(nodehostNodes, 25)
}

func getFSyncAverageLatency() (uint64, error) {
	fn := "fsync_latency_test_safe_to_delete.data"
	defer os.RemoveAll(fn)
	return calculateFSyncAverageLatency(fn)
}

func calculateFSyncAverageLatency(fn string) (uint64, error) {
	f, err := os.Create(fn)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	data := make([]byte, 350)
	// no built-in monotonic clock in go1.8
	// have fun!
	start := time.Now().UnixNano()
	repeat := 200
	for i := 0; i < repeat; i++ {
		_, err := f.Write(data)
		if err != nil {
			return 0, err
		}
		f.Sync()
	}
	latency := (time.Now().UnixNano() - start) / int64(1000000*repeat)
	return uint64(latency), nil
}
