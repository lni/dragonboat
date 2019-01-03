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
	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	serverConfig "github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/tests"
	"github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/utils/lang"
	"github.com/lni/dragonboat/internal/utils/random"
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
	next      int64
	addresses []string
}

func (n *mtNodeHost) setNext(low int64, high int64) {
	if high <= low {
		panic("high <= low")
	}

	v := (low + rand.Int63()%(high-low)) * 1000000
	plog.Infof("next event for node %d is scheduled in %d second",
		n.listIndex+1, v/1000000000)
	n.next = time.Now().UnixNano() + v
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
	config := serverConfig.NodeHostConfig{}
	config = nhc
	config.NodeHostDir = filepath.Join(n.dir, nhc.NodeHostDir)
	config.WALDir = filepath.Join(n.dir, nhc.WALDir)
	config.RaftAddress = n.addresses[n.listIndex]
	nh := NewNodeHost(config)
	n.nh = nh
	peers := make(map[uint64]string)
	for idx, v := range n.addresses {
		peers[uint64(idx+1)] = v
	}
	createStateMachine := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		ds := tests.NewKVTest(clusterID, nodeID)
		return rsm.NewNativeStateMachine(ds, done)
	}
	for i := uint64(1); i <= mtNumOfClusters; i++ {
		rc.ClusterID = i
		rc.NodeID = n.listIndex + 1

		plog.Infof("starting cluster %d node %d", rc.ClusterID, rc.NodeID)
		if err := n.nh.startCluster(peers,
			false, createStateMachine, make(chan struct{}), rc); err != nil {
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
		return rsm.NewNativeStateMachine(ds, done)
	}
	rc.ClusterID = clusterID
	// the extra 5 is to make it different from the initial
	rc.NodeID = n.listIndex + 1 + 5 // make it different from the initial
	plog.Infof("starting cluster %d node %d on node with list index %d",
		rc.ClusterID, rc.NodeID, n.listIndex)
	if err := n.nh.startCluster(nil,
		true, createStateMachine, make(chan struct{}), rc); err != nil {
		panic(err)
	}
}

func getMTConfig() (config.Config, serverConfig.NodeHostConfig) {
	rc := config.Config{
		ElectionRTT:        20,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    5,
		CompactionOverhead: 5,
	}
	nhc := serverConfig.NodeHostConfig{
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
				Key: &key,
				Val: &value,
			}
			rec, err := proto.Marshal(kv)
			if err != nil {
				panic(err)
			}
			clusterID := rand.Uint64()%mtNumOfClusters + 1
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			session, err := nhList[idx].nh.GetNewSession(ctx, clusterID)
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
					if result != uint64(len(rec)) {
						plog.Panicf("result %d, want %d", result, len(rec))
					}
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					readIdx := rand.Int() % len(nhList)
					if !nhList[readIdx].Running() {
						resp, err := nhList[readIdx].nh.SyncRead(ctx, clusterID, []byte(*kv.Key))
						if err == nil {
							if string(resp) != string(*kv.Val) {
								plog.Panicf("got %s, want %s", string(resp), kv.Val)
							}
						}
					}
				}
			}
			if (rand.Int() % 10) > 2 {
				f := func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					nhList[idx].nh.CloseSession(ctx, session)
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
						plog.Infof("%s rn.lastApplied %d", rn.describe(), rn.sm.GetLastApplied())
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
		node.nh.CloseSession(ctx, session)
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
			if string(result) != string(expected) {
				t.Errorf("got size %d want size %d", len(result), len(expected))
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
		session, err = nh.GetNewSession(ctx, clusterID)
		if err == nil {
			return session
		}
	}
	return nil
}
