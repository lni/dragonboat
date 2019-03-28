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

package dragonboat

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/random"
)

func testNodeHostLinearizableReadWorks(t *testing.T, size int) {
	defer removeMTDirs()
	nhList := createMTNodeHostList()
	startAllClusters(nhList)
	defer stopMTNodeHosts(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	k := "test-key"
	d := random.String(size)
	kv := &kvpb.PBKV{
		Key: k,
		Val: d,
	}
	rec, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	if len(rec) > int(settings.MaxProposalPayloadSize) {
		t.Fatalf("input is too big")
	}
	testProposalCanBeMade(t, nhList[3], rec)
	testLinearizableReadReturnExpectedResult(t,
		nhList[2], []byte(kv.Key), []byte(kv.Val))
}

func TestNodeHostLinearizableReadWorksInMostBasicSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testNodeHostLinearizableReadWorks(t, 32)
}

func TestNodeHostLinearizableReadWorksWithLargePayload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if settings.MaxProposalPayloadSize < uint64(31*1024*1024) {
		t.Fatalf("MaxProposalPayloadSize not big enough")
	}
	testNodeHostLinearizableReadWorks(t,
		int(settings.MaxProposalPayloadSize-16))
}

func getTestKVData() []byte {
	k := "test-key"
	d := "test-data"
	// make a proposal
	kv := &kvpb.PBKV{
		Key: k,
		Val: d,
	}
	rec, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return rec
}

func TestNodeHostRemovedClusterCanBeAddedBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMTDirs()
	nhList := createMTNodeHostList()
	startAllClusters(nhList)
	defer stopMTNodeHosts(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	addresses := nhList[0].addresses
	// remove cluster 1 from 2 out of 5 nodehosts
	for i := 0; i < 2; i++ {
		done := false
		retry := 0
		for !done && retry < 5 {
			rs, err := nhList[3].nh.RequestDeleteNode(1,
				uint64(i+1), 0, time.Duration(10*time.Second))
			if err != nil {
				t.Fatalf("failed to remove node, i = %d, %v", i, err)
			}
			select {
			case v := <-rs.CompletedC:
				if v.Completed() {
					done = true
					continue
				} else {
					plog.Infof("delete node failed, v.code %d", v.code)
					time.Sleep(2 * time.Second)
				}
			}
			retry++
			time.Sleep(1 * time.Second)
		}
		if retry == 5 {
			t.Fatalf("failed to delete node %d", uint64(i+1))
		}
	}
	for i := 0; i < 2; i++ {
		err := nhList[i].nh.stopNode(1, 0, false)
		if err != ErrClusterNotFound {
			// see comments in TestNodeHostRemoveCluster for more details.
			continue
		}
	}
	waitForStableClusters(t, nhList, 30)
	// add those nodes back
	for i := 0; i < 2; i++ {
		done := false
		retry := 0
		for !done && retry < 5 {
			rs, err := nhList[3].nh.RequestAddNode(1,
				uint64(i+1+len(addresses)), addresses[i],
				0, time.Duration(10*time.Second))
			if err != nil {
				t.Fatalf("failed to add node, i = %d, %v", i, err)
			}
			select {
			case v := <-rs.CompletedC:
				if !v.Completed() {
					t.Errorf("i = %d, for RequestAddNode, got %d want %d",
						i, v, requestCompleted)
				} else {
					done = true
					continue
				}
			}
			retry++
			time.Sleep(1 * time.Second)
		}
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 2; i++ {
		nhList[i].RestartCluster(1)
	}
	time.Sleep(5 * time.Second)
	plog.Infof("all nodes restarted, wait for a cluster to be stable.")
	waitForStableClusters(t, nhList, 90)
	rec := getTestKVData()
	testProposalCanBeMade(t, nhList[3], rec)
	testLinearizableReadReturnExpectedResult(t,
		nhList[2], []byte("test-key"), []byte("test-data"))
}

func TestNodeHostRemoveCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMTDirs()
	nhList := createMTNodeHostList()
	defer stopMTNodeHosts(nhList)
	startAllClusters(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	// remove cluster 1 from 3 out of 5 nodehosts
	for i := 0; i < 3; i++ {
		retry := 0
		done := false
		for !done && retry < 5 {
			// proposal can be dropped due to failed checkQuorum operation.
			rs, err := nhList[3].nh.RequestDeleteNode(1,
				uint64(i+1), 0, time.Duration(10*time.Second))
			if err != nil {
				t.Fatalf("failed to remove node, i = %d, %v", i, err)
			}
			select {
			case v := <-rs.CompletedC:
				if v.Completed() {
					done = true
					continue
				}
			}
			time.Sleep(3 * time.Second)
			retry++
		}
		if retry == 5 {
			t.Fatalf("failed to remove node after 5 retries")
		}
	}
	time.Sleep(200 * time.Millisecond)
	// once removed, call to RemoveCluster should fail
	for i := 0; i < 3; i++ {
		err := nhList[i].nh.stopNode(1, 0, false)
		if err != ErrClusterNotFound {
			// this can happen when the node is being partitioned when the delete node
			// is requested. or simply because the leader didn't get the chance to
			// send out the heartbeat message to move the commit value on the node
			// being deleted (leader applies the delete before that heartbeat, then
			// the node being deleted won't get the heartbeat).
			continue
		}
	}
	waitForStableClusters(t, nhList, 30)
	// further proposal/linearizable read should still be ok as the new
	// cluster size for the cluster is expected to be 2
	rec := getTestKVData()
	testProposalCanBeMade(t, nhList[3], rec)
	testLinearizableReadReturnExpectedResult(t,
		nhList[3], []byte("test-key"), []byte("test-data"))
}

func TestMTClustersCanBeStartedAndStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMTDirs()
	nhList := createMTNodeHostList()
	startAllClusters(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	makeRandomProposal(nhList, 30)
	time.Sleep(3 * time.Second)
	checkStateMachine(t, nhList)
	stopMTNodeHosts(nhList)
}

func TestMTClustersCanBeStoppedAndRestarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMTDirs()
	nhList := createMTNodeHostList()
	startAllClusters(nhList)
	defer stopMTNodeHosts(nhList)
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	makeRandomProposal(nhList, 10)
	// stop node 0, 1, 2
	stopped := 0
	for _, node := range nhList {
		if node.listIndex == 0 || node.listIndex == 1 || node.listIndex == 2 {
			node.Stop()
			stopped++
			if stopped == 2 {
				makeRandomProposal(nhList, 10)
			}
		}
	}
	time.Sleep(5 * time.Second)
	plog.Infof("time to restart test nodes")
	// restart those 3 stopped node hosts
	nhList[0].Start()
	nhList[1].Start()
	nhList[2].Start()
	waitForStableClusters(t, nhList, 30)
	time.Sleep(200 * time.Millisecond)
	makeRandomProposal(nhList, 10)
}
