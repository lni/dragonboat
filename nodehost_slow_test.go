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

package dragonboat

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/tests"
	"github.com/lni/dragonboat/v3/internal/tests/kvpb"
	"github.com/lni/dragonboat/v3/internal/utils/lang"
	"github.com/lni/dragonboat/v3/internal/utils/leaktest"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

type multiraftMonkeyTestAddrList struct {
	addrList []string
}

func newMultiraftMonkeyTestAddrList() *multiraftMonkeyTestAddrList {
	return &multiraftMonkeyTestAddrList{addrList: make([]string, 0)}
}

func (m *multiraftMonkeyTestAddrList) fill(base uint64) {
	for i := uint64(1); i <= uint64(5); i++ {
		addr := fmt.Sprintf("localhost:%d", base+i)
		m.addrList = append(m.addrList, addr)
	}
}

func (m *multiraftMonkeyTestAddrList) Size() uint64 {
	return uint64(len(m.addrList))
}

func (m *multiraftMonkeyTestAddrList) Addresses() []string {
	return m.addrList
}

func getMultiraftMonkeyTestAddrList() *multiraftMonkeyTestAddrList {
	base := uint64(5400)
	v := os.Getenv("MULTIRAFTMTPORT")
	if len(v) > 0 {
		iv, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		base = uint64(iv)
		plog.Infof("using port base from env %d", base)
	} else {
		plog.Infof("using default port base %d", base)
	}
	dl := newMultiraftMonkeyTestAddrList()
	dl.fill(base)
	return dl
}

const (
	mtNumOfClusters    uint64 = 64
	mtWorkingDirectory        = "monkey_testing_nh_dir_safe_to_delete"
)

type mtNodeHost struct {
	listIndex uint64
	dir       string
	nh        *NodeHost
	stopped   bool
	addresses []string
}

func (n *mtNodeHost) Running() bool {
	return n.nh != nil && !n.stopped
}

func (n *mtNodeHost) Stop() {
	if n.stopped {
		panic("already stopped")
	}
	done := uint32(0)
	go func() {
		plog.Infof("going to stop the test node host, i %d", n.listIndex)
		n.nh.Stop()
		plog.Infof("test node host stopped, i %d", n.listIndex)
		atomic.StoreUint32(&done, 1)
	}()
	count := 0
	for {
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadUint32(&done) == 1 {
			break
		}
		count++
		if count == 200 {
			panic("failed to stop the node host")
		}
	}
	n.stopped = true
}

func (n *mtNodeHost) Start() {
	if !n.stopped {
		panic("already running")
	}
	rc, nhc := getMTConfig()
	config := config.NodeHostConfig{}
	config = nhc
	config.NodeHostDir = filepath.Join(n.dir, nhc.NodeHostDir)
	config.WALDir = filepath.Join(n.dir, nhc.WALDir)
	config.RaftAddress = n.addresses[n.listIndex]
	nh, err := NewNodeHost(config)
	if err != nil {
		panic(err)
	}
	n.nh = nh
	peers := make(map[uint64]string)
	for idx, v := range n.addresses {
		peers[uint64(idx+1)] = v
	}
	createStateMachine := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		ds := tests.NewKVTest(clusterID, nodeID)
		return rsm.NewNativeStateMachine(clusterID, nodeID, rsm.NewRegularStateMachine(ds), done)
	}
	for i := uint64(1); i <= mtNumOfClusters; i++ {
		rc.ClusterID = i
		rc.NodeID = n.listIndex + 1

		plog.Infof("starting cluster %d node %d", rc.ClusterID, rc.NodeID)
		if err := n.nh.startCluster(peers,
			false, createStateMachine, make(chan struct{}),
			rc, pb.RegularStateMachine); err != nil {
			panic(err)
		}
	}
	n.stopped = false
}

func (n *mtNodeHost) RestartCluster(clusterID uint64) {
	if n.stopped {
		panic("already stopped")
	}
	rc, _ := getMTConfig()
	createStateMachine := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		ds := tests.NewKVTest(clusterID, nodeID)
		return rsm.NewNativeStateMachine(clusterID,
			nodeID, rsm.NewRegularStateMachine(ds), done)
	}
	rc.ClusterID = clusterID
	// the extra 5 is to make it different from the initial
	rc.NodeID = n.listIndex + 1 + 5 // make it different from the initial
	plog.Infof("starting cluster %d node %d on node with list index %d",
		rc.ClusterID, rc.NodeID, n.listIndex)
	if err := n.nh.startCluster(nil,
		true, createStateMachine, make(chan struct{}), rc,
		pb.RegularStateMachine); err != nil {
		panic(err)
	}
}

func getMTConfig() (config.Config, config.NodeHostConfig) {
	rc := config.Config{
		ElectionRTT:        20,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    5,
		CompactionOverhead: 5,
	}
	nhc := config.NodeHostConfig{
		WALDir:         "nhmt",
		NodeHostDir:    "nhmt",
		RTTMillisecond: 50,
	}
	return rc, nhc
}

func removeMTDirs() {
	os.RemoveAll(mtWorkingDirectory)
}

func prepareMTDirs() []string {
	removeMTDirs()
	dl := getMultiraftMonkeyTestAddrList()
	dirList := make([]string, 0)
	for i := uint64(1); i <= dl.Size(); i++ {
		nn := fmt.Sprintf("node%d", i)
		nd := filepath.Join(mtWorkingDirectory, nn)
		if err := os.MkdirAll(nd, 0755); err != nil {
			panic(err)
		}
		dirList = append(dirList, nd)
	}
	return dirList
}

func createMTNodeHostList() []*mtNodeHost {
	dirList := prepareMTDirs()
	dl := getMultiraftMonkeyTestAddrList()
	result := make([]*mtNodeHost, dl.Size())
	for i := uint64(0); i < dl.Size(); i++ {
		result[i] = &mtNodeHost{
			listIndex: i,
			stopped:   true,
			dir:       dirList[i],
			addresses: dl.Addresses(),
		}
	}
	return result
}

func startAllClusters(nhList []*mtNodeHost) {
	for _, node := range nhList {
		node.Start()
	}
}

func stopMTNodeHosts(nhList []*mtNodeHost) {
	for _, v := range nhList {
		v.Stop()
	}
}

func waitForStableClusters(t *testing.T, nhList []*mtNodeHost, waitSeconds uint64) {
	time.Sleep(3000 * time.Millisecond)
	for {
		done := tryWaitForStableClusters(t, nhList, waitSeconds)
		if !done {
			panic("failed to get a stable network")
		}
		time.Sleep(3 * time.Second)
		done = tryWaitForStableClusters(t, nhList, waitSeconds)
		if done {
			return
		}
		time.Sleep(3 * time.Second)
	}
}

func tryWaitForStableClusters(t *testing.T, nhList []*mtNodeHost,
	waitSeconds uint64) bool {
	waitMilliseconds := waitSeconds * 1000
	totalWait := uint64(0)
	var nodeReady bool
	var leaderReady bool
	for !nodeReady || !leaderReady {
		nodeReady = true
		leaderReady = true
		leaderMap := make(map[uint64]bool)
		time.Sleep(100 * time.Millisecond)
		totalWait += 100
		if totalWait >= waitMilliseconds {
			return false
		}
		for _, n := range nhList {
			if n == nil || n.nh == nil {
				continue
			}
			nh := n.nh
			nh.forEachCluster(func(cid uint64, node *node) bool {
				leaderID, ok, err := nh.GetLeaderID(node.clusterID)
				if err != nil {
					nodeReady = false
					return true
				}
				if ok && leaderID == node.nodeID {
					leaderMap[node.clusterID] = true
				}
				return true
			})
		}
		if uint64(len(leaderMap)) != mtNumOfClusters {
			leaderReady = false
		}
	}
	return true
}

func getRandomStringBytes(sz int) []byte {
	return []byte(random.String(sz))
}

func makeRandomProposal(nhList []*mtNodeHost, count int) {
	hasRunningNode := false
	for _, n := range nhList {
		if n.Running() {
			hasRunningNode = true
		}
	}
	if !hasRunningNode {
		return
	}
	for i := 0; i < count; i++ {
		idx := rand.Int() % len(nhList)
		if nhList[idx].Running() {
			valsz := rand.Int()%16 + 3
			key := fmt.Sprintf("key-%d", rand.Int())
			value := string(getRandomStringBytes(valsz))
			kv := &kvpb.PBKV{
				Key: key,
				Val: value,
			}
			rec, err := kv.Marshal()
			if err != nil {
				panic(err)
			}
			clusterID := rand.Uint64()%mtNumOfClusters + 1
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session, err := nhList[idx].nh.SyncGetSession(ctx, clusterID)
			if err != nil {
				plog.Errorf("couldn't get a proposal client for %d", clusterID)
				continue
			}
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			repeat := (rand.Int() % 3) + 1
			for j := 0; j < repeat; j++ {
				session.ProposalCompleted()
				result, err := nhList[idx].nh.SyncPropose(ctx, session, rec)
				if err != nil {
					plog.Infof(err.Error())
					continue
				} else {
					if result.Value != uint64(len(rec)) {
						plog.Panicf("result %d, want %d", result, len(rec))
					}
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					readIdx := rand.Int() % len(nhList)
					if !nhList[readIdx].Running() {
						resp, err := nhList[readIdx].nh.SyncRead(ctx, clusterID, []byte(kv.Key))
						if err == nil {
							if string(resp.([]byte)) != string(kv.Val) {
								plog.Panicf("got %s, want %s", string(resp.([]byte)), kv.Val)
							}
						}
					}
				}
			}
			if (rand.Int() % 10) > 2 {
				f := func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					nhList[idx].nh.SyncCloseSession(ctx, session)
				}
				f()
			}
		} else {
			plog.Infof("got a dead node, skipping it, %d", i)
			i--
		}
	}
}

func waitLastAppliedToSync(t *testing.T, smList []*mtNodeHost) {
	count := 0
	for {
		appliedMap := make(map[uint64]uint64)
		notSynced := make([]uint64, 0)
		for _, n := range smList {
			nh := n.nh
			nh.forEachCluster(func(clusterID uint64, rn *node) bool {
				lastApplied := rn.sm.GetLastApplied()
				existingLastApplied, ok := appliedMap[clusterID]
				if !ok {
					appliedMap[clusterID] = lastApplied
				} else {
					if existingLastApplied != lastApplied {
						notSynced = append(notSynced, clusterID)
					}
				}
				return true
			})
		}
		if len(notSynced) > 0 {
			time.Sleep(100 * time.Millisecond)
			count++
		} else {
			return
		}
		if count == 1200 {
			for _, n := range smList {
				nh := n.nh
				nh.forEachCluster(func(clusterID uint64, rn *node) bool {
					if lang.ContainsUint64(clusterID, notSynced) {
						plog.Infof("%s rn.lastApplied %d", rn.id(), rn.sm.GetLastApplied())
						rn.dumpRaftInfoToLog()
					}
					return true
				})
			}
			t.Fatalf("failed to sync last applied")
		}
	}
}

func checkStateMachine(t *testing.T, smList []*mtNodeHost) {
	hashMap := make(map[uint64]uint64)
	sessionHashMap := make(map[uint64]uint64)
	waitLastAppliedToSync(t, smList)
	for _, n := range smList {
		nh := n.nh
		nh.forEachCluster(func(clusterID uint64, rn *node) bool {
			hash := rn.getStateMachineHash()
			sessionHash := rn.getSessionHash()
			// check hash
			existingHash, ok := hashMap[clusterID]
			if !ok {
				hashMap[clusterID] = hash
			} else {
				if existingHash != hash {
					t.Errorf("hash mismatch, existing %d, new %d",
						existingHash, hash)
				}
			}
			// check session hash
			existingHash, ok = sessionHashMap[clusterID]
			if !ok {
				sessionHashMap[clusterID] = sessionHash
			} else {
				if existingHash != sessionHash {
					t.Errorf("session hash mismatch, existing %d, new %d",
						existingHash, sessionHash)
				}
			}
			return true
		})
	}
}

func testProposalCanBeMade(t *testing.T, node *mtNodeHost, data []byte) {
	session := tryGetSession(node.nh, 1)
	if session == nil {
		t.Fatalf("failed to get client session")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		node.nh.SyncCloseSession(ctx, session)
	}()
	retry := 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := node.nh.SyncPropose(ctx, session, data)
		cancel()
		if err != nil {
			session.ProposalCompleted()
			if retry == 5 {
				t.Fatalf("failed to make proposal %v", err)
			}
		} else {
			break
		}
	}
}

func testLinearizableReadReturnExpectedResult(t *testing.T, node *mtNodeHost,
	query []byte, expected []byte) {
	retry := 0
	for retry < 5 {
		retry++
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := node.nh.SyncRead(ctx, 1, query)
		cancel()
		if err != nil {
			if retry == 5 {
				t.Fatalf("failed to read, %v", err)
			}
		} else {
			if string(result.([]byte)) != string(expected) {
				t.Errorf("got size %d want size %d", len(result.([]byte)), len(expected))
			}
			break
		}
	}
}

func tryGetSession(nh *NodeHost, clusterID uint64) *client.Session {
	var err error
	var session *client.Session
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		session, err = nh.SyncGetSession(ctx, clusterID)
		if err == nil {
			return session
		}
	}
	return nil
}

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
					t.Errorf("i = %d, for RequestAddNode, got %v want %d",
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
