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

// +build dragonboat_monkeytest dragonboat_slowtest

package drummer

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/drummer/client"
	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	mr "github.com/lni/dragonboat/internal/drummer/multiraftpb"
	kvpb "github.com/lni/dragonboat/internal/tests/kvpb"
	"github.com/lni/dragonboat/internal/tests/lcm"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftpb"
)

const (
	mtClusterID uint64 = 100
	// these magic values allow us to immediately tell whether an interested node
	// is the initial member of the cluster or joined later at certain point.
	mtNodeID1 uint64 = 2345
	mtNodeID2 uint64 = 6789
	mtNodeID3 uint64 = 9876

	monkeyTestSecondToRun        uint64 = 1200
	testClientWorkerCount        uint64 = 32
	numOfClustersInMonkeyTesting uint64 = 128
	numOfTestDrummerNodes        uint64 = 3
	numOfTestNodeHostNodes       uint64 = 5
	LCMWorkerCount               uint64 = 32
	defaultBasePort              uint64 = 5700
	partitionCycle               uint64 = 60
	partitionMinSecond           uint64 = 20
	partitionMinStartSecond      uint64 = 200
	partitionCycleInterval       uint64 = 60
	partitionCycleMinInterval    uint64 = 30
	maxWaitForStopSecond         int    = 60
	maxWaitForSyncSecond         int    = 120
	waitForStableSecond          uint64 = 25
	// node uptime
	nodeUpTimeLowMillisecond  int64 = 150000
	nodeUpTimeHighMillisecond int64 = 240000
	lowUpTimeLowMillisecond   int64 = 1000
	lowUpTimeHighMillisecond  int64 = 50000
	defaultTestTimeout              = 5 * time.Second
	clusterCheckWaitSecond          = 20 * time.Second
	maxAllowedHeapSize              = 1024 * 1024 * 1024 * 4
)

var (
	mtNodeIDList = []uint64{
		mtNodeID1,
		mtNodeID2,
		mtNodeID3,
	}

	caFile   = "templates/tests/test-root-ca.crt"
	certFile = "templates/tests/localhost.crt"
	keyFile  = "templates/tests/localhost.key"
)

type mtAddressList struct {
	addressList            []string
	nodehostAddressList    []string
	apiAddressList         []string
	nodehostAPIAddressList []string
}

func newDrummerMonkeyTestAddr() *mtAddressList {
	d := &mtAddressList{
		addressList:            make([]string, 0),
		nodehostAddressList:    make([]string, 0),
		apiAddressList:         make([]string, 0),
		nodehostAPIAddressList: make([]string, 0),
	}
	return d
}

func (d *mtAddressList) fill(base uint64) {
	port := base + 1
	for i := uint64(0); i < uint64(numOfTestDrummerNodes); i++ {
		addr := fmt.Sprintf("localhost:%d", port)
		d.addressList = append(d.addressList, addr)
		port++
	}
	for i := uint64(0); i < uint64(numOfTestNodeHostNodes); i++ {
		addr := fmt.Sprintf("localhost:%d", port)
		d.nodehostAddressList = append(d.nodehostAddressList, addr)
		port++
	}
	for i := uint64(0); i < uint64(numOfTestDrummerNodes); i++ {
		addr := fmt.Sprintf("localhost:%d", port)
		d.apiAddressList = append(d.apiAddressList, addr)
		port++
	}
	for i := uint64(0); i < uint64(numOfTestNodeHostNodes); i++ {
		addr := fmt.Sprintf("localhost:%d", port)
		d.nodehostAPIAddressList = append(d.nodehostAPIAddressList, addr)
		port++
	}
}

func getDrummerMonkeyTestAddrList() *mtAddressList {
	base := defaultBasePort
	v := os.Getenv("DRUMMERMTPORT")
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
	dl := newDrummerMonkeyTestAddr()
	dl.fill(base)
	return dl
}

type nodeType uint64

const (
	monkeyTestWorkingDir          = "drummer_mt_pwd_safe_to_delete"
	nodeTypeDrummer      nodeType = iota
	nodeTypeNodehost
)

func (t nodeType) String() string {
	if t == nodeTypeDrummer {
		return "DrummerNode"
	} else if t == nodeTypeNodehost {
		return "NodehostNode"
	} else {
		panic("unknown type")
	}
}

func prepareMonkeyTestDirs(dl *mtAddressList) ([]string, []string) {
	removeMonkeyTestDir()
	drummerDirList := make([]string, 0)
	nodehostDirList := make([]string, 0)
	// drummer first
	for i := uint64(1); i <= uint64(len(dl.addressList)); i++ {
		nn := fmt.Sprintf("drummer-node-%d", i)
		nd := filepath.Join(monkeyTestWorkingDir, nn)
		if err := os.MkdirAll(nd, 0755); err != nil {
			panic(err)
		}
		drummerDirList = append(drummerDirList, nd)
	}
	// nodehost dirs
	for i := uint64(1); i <= uint64(len(dl.nodehostAddressList)); i++ {
		nn := fmt.Sprintf("nodehost-node-%d", i)
		nd := filepath.Join(monkeyTestWorkingDir, nn)
		if err := os.MkdirAll(nd, 0755); err != nil {
			panic(err)
		}
		nodehostDirList = append(nodehostDirList, nd)
	}
	return drummerDirList, nodehostDirList
}

func removeMonkeyTestDir() {
	os.RemoveAll(monkeyTestWorkingDir)
}

func saveMonkeyTestDir() {
	newName := fmt.Sprintf("%s-%d", monkeyTestWorkingDir, rand.Uint64())
	plog.Infof("going to save the monkey test data dir to %s", newName)
	if err := os.Rename(monkeyTestWorkingDir, newName); err != nil {
		panic(err)
	}
}

func getMonkeyTestConfig() (config.Config, config.NodeHostConfig) {
	rc := config.Config{
		ElectionRTT:        20,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    100,
		CompactionOverhead: 100,
	}
	nhc := config.NodeHostConfig{
		WALDir:         "drummermt",
		NodeHostDir:    "drummermt",
		RTTMillisecond: 50,
	}
	return rc, nhc
}

type testNode struct {
	nodeType           nodeType
	listIndex          uint64
	dir                string
	nh                 *dragonboat.NodeHost
	drummer            *Drummer
	apiServer          *NodehostAPI
	drummerNodeHost    *client.NodeHostClient
	drummerStopped     bool
	stopped            bool
	partitionTestNode  bool
	partitionStartTime map[uint64]struct{}
	partitionEndTime   map[uint64]struct{}
	next               int64
	stopper            *syncutil.Stopper
	mutualTLS          bool
}

func (n *testNode) MustBeDrummerNode() {
	if n.nodeType != nodeTypeDrummer {
		panic("not drummer node")
	}
}

func (n *testNode) MustBeNodehostNode() {
	if n.nodeType != nodeTypeNodehost {
		panic("not nodehost node")
	}
}

func (n *testNode) IsDrummerLeader() bool {
	n.MustBeDrummerNode()
	return n.drummer.isLeaderDrummerNode()
}

func (n *testNode) DoPartitionTests(monkeyPlaySeconds uint64) {
	st := partitionMinStartSecond
	et := st
	plog.Infof("node %d is set to use partition test mode", n.listIndex+1)
	n.partitionTestNode = true
	for {
		partitionTime := rand.Uint64() % partitionCycle
		if partitionTime < partitionMinSecond {
			partitionTime = partitionMinSecond
		}
		interval := rand.Uint64() % partitionCycleInterval
		if interval < partitionCycleMinInterval {
			interval = partitionCycleMinInterval
		}
		st = et + interval
		et = st + partitionTime
		if st < monkeyPlaySeconds && et < monkeyPlaySeconds {
			plog.Infof("adding a partition cycle, st %d, et %d", st, et)
			n.partitionStartTime[st] = struct{}{}
			n.partitionEndTime[et] = struct{}{}
		} else {
			return
		}
	}
}

func (n *testNode) IsPartitionTestNode() bool {
	return n.partitionTestNode
}

func (n *testNode) Running() bool {
	if n.nh != nil && !n.stopped {
		return true
	}
	return false
}

func (n *testNode) Stop() {
	if n.stopped {
		panic("already stopped")
	}
	n.stopped = true
	done := uint32(0)
	go func() {
		count := 0
		for {
			time.Sleep(100 * time.Millisecond)
			if atomic.LoadUint32(&done) == 1 {
				break
			}
			count++
			if count == 10*maxWaitForStopSecond {
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
				plog.Panicf("failed to stop the nodehost %s, it is a %s, idx %d",
					n.nh.RaftAddress(), n.nodeType, n.listIndex)
			}
		}
	}()
	addr := n.nh.RaftAddress()
	if n.nodeType == nodeTypeDrummer {
		plog.Infof("going to stop the drummer of %s", addr)
		if n.drummer != nil && !n.drummerStopped {
			n.drummer.Stop()
			n.drummerStopped = true
		}
		plog.Infof("the drummer part of %s stopped", addr)
	}
	plog.Infof("going to stop the nh of %s", addr)
	if n.apiServer != nil {
		n.apiServer.Stop()
	}
	if n.drummerNodeHost != nil {
		n.drummerNodeHost.Stop()
	}
	n.nh.Stop()
	plog.Infof("the nh part of %s stopped", addr)
	if n.stopper != nil {
		plog.Infof("monkey node has a stopper, %s", addr)
		n.stopper.Stop()
		plog.Infof("stopper on monkey %s stopped", addr)
	}
	atomic.StoreUint32(&done, 1)
	n.nh = nil
}

func (n *testNode) Start(dl *mtAddressList) {
	if n.nodeType == nodeTypeDrummer {
		n.startDrummerNode(dl)
	} else if n.nodeType == nodeTypeNodehost {
		n.startNodehostNode(dl)
	} else {
		panic("unknown node type")
	}
}

func (n *testNode) GetMultiClusterAndTick() (*multiCluster, uint64, error) {
	n.MustBeDrummerNode()
	sc, err := n.drummer.getSchedulerContext()
	if err != nil {
		return nil, 0, err
	}
	return sc.ClusterImage, sc.Tick, nil
}

func (n *testNode) GetMultiCluster() (*multiCluster, error) {
	n.MustBeDrummerNode()
	sc, err := n.drummer.getSchedulerContext()
	if err != nil {
		return nil, err
	}
	return sc.ClusterImage, nil
}

func (n *testNode) GetNodeHostInfo() (*multiNodeHost, error) {
	n.MustBeDrummerNode()
	sc, err := n.drummer.getSchedulerContext()
	if err != nil {
		return nil, err
	}
	return sc.NodeHostImage, nil
}

func (n *testNode) setNodeNext(low int64, high int64) {
	if high <= low {
		panic("high <= low")
	}
	var v int64
	ll := rand.Uint64()%10 == 0
	if ll {
		low = lowUpTimeLowMillisecond
		high = lowUpTimeHighMillisecond
	}
	v = (low + rand.Int63()%(high-low)) * 1000000
	plog.Infof("next event for %s %d is scheduled in %d second, %t",
		n.nodeType, n.listIndex+1, v/1000000000, ll)
	n.next = time.Now().UnixNano() + v
}

func (n *testNode) startDrummerNode(dl *mtAddressList) {
	if n.nodeType != nodeTypeDrummer {
		panic("trying to start a drummer on a non-drummer node")
	}
	if !n.stopped {
		panic("already running")
	}
	rc, nhc := getMonkeyTestConfig()
	config := config.NodeHostConfig{}
	config = nhc
	config.NodeHostDir = filepath.Join(n.dir, nhc.NodeHostDir)
	config.WALDir = filepath.Join(n.dir, nhc.WALDir)
	config.RaftAddress = dl.addressList[n.listIndex]
	if n.mutualTLS {
		config.MutualTLS = true
		config.CAFile = caFile
		config.CertFile = certFile
		config.KeyFile = keyFile
	}
	plog.Infof("creating new nodehost for drummer node")
	nh := dragonboat.NewNodeHost(config)
	plog.Infof("nodehost ready for drummer node")
	n.nh = nh
	peers := make(map[uint64]string)
	for idx, v := range dl.addressList {
		peers[uint64(idx+1)] = v
	}
	rc.NodeID = uint64(n.listIndex + 1)
	rc.ClusterID = defaultClusterID
	if err := nh.StartCluster(peers, false, NewDB, rc); err != nil {
		panic(err)
	}
	plog.Infof("creating the drummer server")
	grpcServerStopper := syncutil.NewStopper()
	grpcHost := dl.apiAddressList[n.listIndex]
	drummerServer := NewDrummer(nh, grpcHost)
	drummerServer.Start()
	plog.Infof("drummer server started")
	n.drummer = drummerServer
	n.drummerStopped = false
	n.stopped = false
	n.stopper = grpcServerStopper
}

func (n *testNode) startNodehostNode(dl *mtAddressList) {
	if n.nodeType != nodeTypeNodehost {
		panic("trying to start a drummer on a non-drummer node")
	}
	if !n.stopped {
		panic("already running")
	}
	_, nhc := getMonkeyTestConfig()
	config := config.NodeHostConfig{}
	config = nhc
	config.NodeHostDir = filepath.Join(n.dir, nhc.NodeHostDir)
	config.WALDir = filepath.Join(n.dir, nhc.WALDir)
	config.RaftAddress = dl.nodehostAddressList[n.listIndex]
	apiAddress := dl.nodehostAPIAddressList[n.listIndex]
	if n.mutualTLS {
		config.MutualTLS = true
		config.CAFile = caFile
		config.CertFile = certFile
		config.KeyFile = keyFile
	}
	plog.Infof("creating nodehost for nodehost node")
	nh := dragonboat.NewNodeHost(config)
	plog.Infof("nodehost for nodehost node created")
	n.nh = nh
	n.drummerNodeHost = client.NewNodeHostClient(nh, dl.apiAddressList, apiAddress)
	n.apiServer = NewNodehostAPI(apiAddress, nh)
	n.stopped = false
}

func checkPartitionedNodeHost(t *testing.T, nodes []*testNode) {
	for _, node := range nodes {
		node.MustBeNodehostNode()
		if node.nh.IsPartitioned() {
			t.Fatalf("nodehost is still in partitioned test mode")
		}
	}
}

func checkRateLimiterState(t *testing.T, nodes []*testNode) {
	for _, n := range nodes {
		nh := n.nh
		for _, rn := range nh.Clusters() {
			rl := rn.GetRateLimiter()
			clusterID := rn.ClusterID()
			nodeID := rn.NodeID()
			if rl.GetInMemLogSize() != rn.GetInMemLogSize() {
				t.Fatalf("%s, rl mem log size %d, in mem log size %d",
					logutil.DescribeNode(clusterID, nodeID),
					rl.GetInMemLogSize(), rn.GetInMemLogSize())
			}
		}
	}
}

func checkNodeHostSynced(t *testing.T, nodes []*testNode) {
	count := 0
	for {
		appliedMap := make(map[uint64]uint64)
		notSynced := make(map[uint64]bool)
		for _, n := range nodes {
			nh := n.nh
			for _, rn := range nh.Clusters() {
				clusterID := rn.ClusterID()
				lastApplied := rn.GetLastApplied()
				existingLastApplied, ok := appliedMap[clusterID]
				if !ok {
					appliedMap[clusterID] = lastApplied
				} else {
					if existingLastApplied != lastApplied {
						notSynced[clusterID] = true
					}
				}
			}
		}
		if len(notSynced) > 0 {
			time.Sleep(100 * time.Millisecond)
			count++
		} else {
			return
		}
		// fail the test and dump details to log
		if count == 10*maxWaitForSyncSecond {
			dumpClusterInfoToLog(nodes, notSynced)
			t.Fatalf("%d failed to sync last applied", len(notSynced))
		}
	}
}

func getEntryListHash(entries []raftpb.Entry) uint64 {
	h := md5.New()
	v := make([]byte, 8)
	for _, ent := range entries {
		binary.LittleEndian.PutUint64(v, ent.Index)
		if _, err := h.Write(v); err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(v, ent.Term)
		if _, err := h.Write(v); err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(v, uint64(ent.Type))
		if _, err := h.Write(v); err != nil {
			panic(err)
		}
		if _, err := h.Write(ent.Cmd); err != nil {
			panic(err)
		}
	}
	return binary.LittleEndian.Uint64(h.Sum(nil)[:8])
}

func getEntryHash(ent raftpb.Entry) uint64 {
	h := md5.New()
	_, err := h.Write(ent.Cmd)
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(h.Sum(nil)[:8])
}

func getConfigFromDeployedJson() (config.Config, bool) {
	cfg := config.Config{}
	fn := "dragonboat-drummer.json"
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return config.Config{}, false
	}
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		panic(err)
	}
	return cfg, true
}

func rateLimiterDisabledInRaftConfig() bool {
	cfg, ok := getConfigFromDeployedJson()
	if !ok {
		return true
	}
	return cfg.MaxInMemLogSize == 0
}

func snapshotDisabledInRaftConfig() bool {
	cfg, ok := getConfigFromDeployedJson()
	if !ok {
		return false
	}
	return cfg.SnapshotEntries == 0
}

func printEntryDetails(clusterID uint64,
	nodeID uint64, entries []raftpb.Entry) {
	for _, ent := range entries {
		plog.Infof("%s, idx %d, term %d, type %s, entry len %d, hash %d",
			logutil.DescribeNode(clusterID, nodeID), ent.Index, ent.Term, ent.Type,
			len(ent.Cmd), getEntryHash(ent))
	}
}

func checkLogdbEntriesSynced(t *testing.T, nodes []*testNode) {
	hashMap := make(map[uint64]uint64)
	notSynced := make(map[uint64]bool, 0)
	for _, n := range nodes {
		nh := n.nh
		for _, rn := range nh.Clusters() {
			nodeID := rn.NodeID()
			clusterID := rn.ClusterID()
			lastApplied := rn.GetLastApplied()
			logdb := nh.GetLogDB()
			entries, _, err := logdb.IterateEntries(nil,
				0, clusterID, nodeID, 1, lastApplied+1, math.MaxUint64)
			if err != nil {
				t.Errorf("failed to get entries %v", err)
			}
			hash := getEntryListHash(entries)
			plog.Infof("%s logdb entry hash %d, last applied %d, ent sz %d",
				logutil.DescribeNode(clusterID, nodeID),
				hash, lastApplied, len(entries))
			printEntryDetails(clusterID, nodeID, entries)
			existingHash, ok := hashMap[clusterID]
			if !ok {
				hashMap[clusterID] = hash
			} else {
				if existingHash != hash {
					notSynced[clusterID] = true
				}
			}
		}
	}
	if len(notSynced) > 0 {
		dumpClusterInfoToLog(nodes, notSynced)
		t.Fatalf("%d clusters failed to have logDB synced, %v",
			len(notSynced), notSynced)
	}
}

func dumpClusterInfoToLog(nodes []*testNode, clusterIDMap map[uint64]bool) {
	for _, n := range nodes {
		nh := n.nh
		for _, rn := range nh.Clusters() {
			clusterID := rn.ClusterID()
			_, ok := clusterIDMap[clusterID]
			if ok {
				plog.Infof("%s rn.lastApplied %d",
					logutil.DescribeNode(rn.ClusterID(), rn.NodeID()),
					rn.GetLastApplied())
				rn.DumpRaftInfoToLog()
			}
		}
	}
}

func dumpClusterToRepairInfoToLog(cr []clusterRepair, tick uint64) {
	plog.Infof("cluster to repair info, tick %d", tick)
	for _, c := range cr {
		plog.Infof("cluster id %d, config change idx %d, failed %v, ok %v, to start %v",
			c.clusterID, c.cluster.ConfigChangeIndex, c.failedNodes, c.okNodes, c.nodesToStart)
	}
}

func dumpUnavailableClusterInfoToLog(cl []cluster, tick uint64) {
	plog.Infof("unavailable cluster info, tick %d", tick)
	for _, c := range cl {
		plog.Infof("cluster id %d, config change idx %d, nodes %v",
			c.ClusterID, c.ConfigChangeIndex, c.Nodes)
	}
}

func checkStateMachine(t *testing.T, nodes []*testNode) {
	hashMap := make(map[uint64]uint64)
	sessionHashMap := make(map[uint64]uint64)
	membershipMap := make(map[uint64]uint64)
	inconsistentClusters := make(map[uint64]bool)
	for _, n := range nodes {
		nh := n.nh
		for _, rn := range nh.Clusters() {
			clusterID := rn.ClusterID()
			hash := rn.GetStateMachineHash()
			sessionHash := rn.GetSessionHash()
			membershipHash := rn.GetMembershipHash()
			// check hash
			existingHash, ok := hashMap[clusterID]
			if !ok {
				hashMap[clusterID] = hash
			} else {
				if existingHash != hash {
					inconsistentClusters[clusterID] = true
					t.Errorf("hash mismatch, cluster id %d, existing %d, new %d",
						clusterID, existingHash, hash)
				}
			}
			// check session hash
			existingHash, ok = sessionHashMap[clusterID]
			if !ok {
				sessionHashMap[clusterID] = sessionHash
			} else {
				if existingHash != sessionHash {
					inconsistentClusters[clusterID] = true
					t.Errorf("session hash mismatch, cluster id %d, existing %d, new %d",
						clusterID, existingHash, sessionHash)
				}
			}
			// check membership
			existingHash, ok = membershipMap[clusterID]
			if !ok {
				membershipMap[clusterID] = membershipHash
			} else {
				if existingHash != membershipHash {
					inconsistentClusters[clusterID] = true
					t.Errorf("membership hash mismatch, cluster id %d, %d vs %d",
						clusterID, existingHash, membershipHash)
				}
			}
		}
	}
	// dump details to log
	if len(inconsistentClusters) > 0 {
		dumpClusterInfoToLog(nodes, inconsistentClusters)
	}
	plog.Infof("hash map size %d, session hash map size %d",
		len(hashMap), len(sessionHashMap))
}

func removeNodeHostTestDirForTesting(listIndex uint64) {
	idx := listIndex + 1
	nn := fmt.Sprintf("nodehost-node-%d", idx)
	nd := filepath.Join(monkeyTestWorkingDir, nn)
	plog.Infof("monkey is going to delete nodehost dir at %s for testing", nd)
	if err := os.RemoveAll(nd); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(nd, 0755); err != nil {
		panic(err)
	}
}

func setRegionForNodehostNodes(nodes []*testNode, regions []string) {
	for idx, node := range nodes {
		node.MustBeNodehostNode()
		node.nh.SetRegion(regions[idx])
	}
}

func createTestNodeLists(dl *mtAddressList) ([]*testNode, []*testNode) {
	drummerDirList, nodehostDirList := prepareMonkeyTestDirs(dl)
	drummerNodes := make([]*testNode, len(dl.addressList))
	nodehostNodes := make([]*testNode, len(dl.nodehostAddressList))
	for i := uint64(0); i < uint64(len(dl.addressList)); i++ {
		drummerNodes[i] = &testNode{
			listIndex:          i,
			stopped:            true,
			dir:                drummerDirList[i],
			nodeType:           nodeTypeDrummer,
			partitionStartTime: make(map[uint64]struct{}),
			partitionEndTime:   make(map[uint64]struct{}),
		}
	}
	for i := uint64(0); i < uint64(len(dl.nodehostAddressList)); i++ {
		nodehostNodes[i] = &testNode{
			listIndex:          i,
			stopped:            true,
			dir:                nodehostDirList[i],
			nodeType:           nodeTypeNodehost,
			partitionStartTime: make(map[uint64]struct{}),
			partitionEndTime:   make(map[uint64]struct{}),
		}
	}
	return drummerNodes, nodehostNodes
}

func startTestNodes(nodes []*testNode, dl *mtAddressList) {
	for _, node := range nodes {
		if !node.Running() {
			node.Start(dl)
		}
	}
}

func stopTestNodes(nodes []*testNode) {
	for _, node := range nodes {
		plog.Infof("going to stop test %s %d", node.nodeType, node.listIndex)
		if node.Running() {
			node.Stop()
		} else {
			plog.Infof("%s %d is not running, will not call stop on it",
				node.nodeType, node.listIndex)
		}
	}
}

func waitForStableNodes(nodes []*testNode, seconds uint64) bool {
	waitInBetweenSecond := time.Duration(3)
	time.Sleep(waitInBetweenSecond * time.Second)
	for {
		done := tryWaitForStableNodes(nodes, seconds)
		if !done {
			return false
		}
		time.Sleep(waitInBetweenSecond * time.Second)
		done = tryWaitForStableNodes(nodes, seconds)
		if done {
			return true
		}
		time.Sleep(waitInBetweenSecond * time.Second)
	}
}

func tryWaitForStableNodes(nodes []*testNode, seconds uint64) bool {
	waitMilliseconds := seconds * 1000
	totalWait := uint64(0)
	var nodeReady bool
	var leaderReady bool
	for !nodeReady || !leaderReady {
		nodeReady = true
		leaderReady = true
		leaderMap := make(map[uint64]bool)
		clusterSet := make(map[uint64]bool)
		time.Sleep(100 * time.Millisecond)
		totalWait += 100
		if totalWait >= waitMilliseconds {
			return false
		}
		for _, node := range nodes {
			if node == nil || node.nh == nil {
				continue
			}
			nh := node.nh
			clusters := nh.Clusters()
			for _, rn := range clusters {
				clusterSet[rn.ClusterID()] = true
				isLeader := rn.IsLeader()
				isFollower := rn.IsFollower()
				if !isLeader && !isFollower {
					nodeReady = false
				}

				if isLeader {
					leaderMap[rn.ClusterID()] = true
				}
			}
		}
		if len(leaderMap) != len(clusterSet) {
			leaderReady = false
		}
	}

	return true
}

func brutalMonkeyPlay(nodehosts []*testNode,
	drummerNodes []*testNode, low int64, high int64) {
	tt := rand.Uint64() % 3
	nodes := make([]*testNode, 0)
	if tt == 0 || tt == 2 {
		nodes = append(nodes, nodehosts...)
	}
	if tt == 1 || tt == 2 {
		nodes = append(nodes, drummerNodes...)
	}
	for _, node := range nodes {
		if !node.IsPartitionTestNode() && node.Running() {
			node.Stop()
			plog.Infof("monkey brutally stopped %s %d", node.nodeType, node.listIndex+1)
			node.setNodeNext(low, high)
		}
	}
}

func monkeyPlay(nodes []*testNode, low int64, high int64,
	lt uint64, deleteDataTested bool, dl *mtAddressList) bool {
	now := time.Now().UnixNano()
	for _, node := range nodes {
		if !node.IsPartitionTestNode() {
			// crash mode
			if node.next == 0 {
				node.setNodeNext(low, high)
				continue
			}
			if node.next > now {
				continue
			}
			if node.Running() {
				plog.Infof("monkey is going to stop %s %d",
					node.nodeType, node.listIndex+1)
				node.Stop()
				plog.Infof("monkey stopped %s %d", node.nodeType, node.listIndex+1)
				if rand.Uint64()%5 == 0 && !deleteDataTested && lt < 800 {
					plog.Infof("monkey is going to delete all data belongs to %s %d",
						node.nodeType, node.listIndex+1)
					removeNodeHostTestDirForTesting(node.listIndex)
					deleteDataTested = true
				}
			} else {
				plog.Infof("monkey is going to start %s %d",
					node.nodeType, node.listIndex+1)
				node.Start(dl)
				plog.Infof("monkey restarted %s %d", node.nodeType, node.listIndex+1)
			}
			node.setNodeNext(low, high)
		} else {
			// partition mode
			_, ps := node.partitionStartTime[lt]
			if ps {
				plog.Infof("monkey is going to partition the node %d",
					node.listIndex+1)
				node.nh.PartitionNode()
			}
			_, pe := node.partitionEndTime[lt]
			if pe {
				plog.Infof("monkey is going to restore node %d from partition mode",
					node.listIndex+1)
				node.nh.RestorePartitionedNode()
			}
		}
	}
	return deleteDataTested
}

func stopDrummerActivity(nodehostNodes []*testNode, drummerNodes []*testNode) {
	for _, n := range nodehostNodes {
		n.drummerNodeHost.StopNodeHostInfoReporter()
	}
	for _, n := range drummerNodes {
		n.stopper.Stop()
		n.stopper = nil
		n.drummer.Stop()
		n.drummer.ctx, n.drummer.cancel = context.WithCancel(context.Background())
		n.drummerStopped = true
	}
}

func getLeaderDrummerIndex(nodes []*testNode) int {
	for idx, n := range nodes {
		n.MustBeDrummerNode()
		if n.Running() && n.drummer.isLeaderDrummerNode() {
			return idx
		}
	}
	return -1
}

func moreThanOneDrummerLeader(nodes []*testNode) bool {
	count := 0
	for _, n := range nodes {
		n.MustBeDrummerNode()
		if n.Running() && n.drummer.isLeaderDrummerNode() {
			count++
		}
	}
	return count >= 2
}

func getDrummerClient(drummerAddressList []string,
	mutualTLS bool) (pb.DrummerClient, *client.Connection) {
	pool := client.NewDrummerConnectionPool()
	for _, server := range drummerAddressList {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
		var conn *client.Connection
		var err error
		if mutualTLS {
			conn, err = pool.GetTLSConnection(ctx, server, caFile, certFile, keyFile)
		} else {
			conn, err = pool.GetInsecureConnection(ctx, server)
		}
		cancel()
		if err == nil {
			return pb.NewDrummerClient(conn.ClientConn()), conn
		}
	}

	return nil, nil
}

func submitTestJobs(count uint64,
	appname string, dl *mtAddressList, mutualTLS bool) bool {
	for i := 0; i < 5; i++ {
		dc, connection := getDrummerClient(dl.apiAddressList, mutualTLS)
		if dc == nil {
			continue
		}
		defer connection.Close()
		if err := submitMultipleTestClusters(count, appname, dc); err == nil {
			return true
		}
	}
	return false
}

func submitSimpleTestJob(dl *mtAddressList, mutualTLS bool) bool {
	dc, connection := getDrummerClient(dl.apiAddressList, mutualTLS)
	if dc == nil {
		return false
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := SubmitCreateDrummerChange(ctx,
		dc, mtClusterID, mtNodeIDList, "kvtest"); err != nil {
		plog.Errorf("failed to submit drummer change")
		return false
	}
	regions := pb.Regions{
		Region: []string{"region-1", "region-2", "region-3"},
		Count:  []uint64{1, 1, 1},
	}
	if err := SubmitRegions(ctx, dc, regions); err != nil {
		plog.Errorf("failed to submit region info")
		return false
	}
	plog.Infof("going to set the bootstrapped flag")
	if err := SubmitBootstrappped(ctx, dc); err != nil {
		plog.Errorf("failed to set bootstrapped flag")
		return false
	}
	return true
}

func submitMultipleTestClusters(count uint64,
	appname string, client pb.DrummerClient) error {
	plog.Infof("going to send cluster info to drummer")
	for i := uint64(0); i < count; i++ {
		clusterID := i + 1
		ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
		if err := SubmitCreateDrummerChange(ctx,
			client, clusterID, []uint64{2345, 6789, 9876}, appname); err != nil {
			plog.Errorf("failed to submit drummer change, cluster %d, %v",
				clusterID, err)
			cancel()
			return err
		}
		cancel()
	}
	regions := pb.Regions{
		Region: []string{"region-1", "region-2", "region-3"},
		Count:  []uint64{1, 1, 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	plog.Infof("going to set region")
	if err := SubmitRegions(ctx, client, regions); err != nil {
		plog.Errorf("failed to submit region info")
		return err
	}
	plog.Infof("going to set the bootstrapped flag")
	if err := SubmitBootstrappped(ctx, client); err != nil {
		plog.Errorf("failed to set bootstrapped flag")
		return err
	}

	return nil
}

func checkClustersAreAccessible(t *testing.T,
	clusterCount uint64, dl *mtAddressList) {
	count := 0
	for {
		notSync := make(map[uint64]bool)
		for clusterID := uint64(1); clusterID <= clusterCount; clusterID++ {
			clusterOk := false
			plog.Infof("checking cluster availability for %d", clusterID)
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			if !makeMonkeyTestRequests(ctx, clusterID, dl) {
				notSync[clusterID] = true
			} else {
				clusterOk = true
			}
			cancel()
			plog.Infof("cluster availability check for %d result: %t",
				clusterID, clusterOk)
		}
		if len(notSync) > 0 {
			count++
			if count > 10 {
				t.Fatalf("failed to access clusters %v", notSync)
			} else {
				time.Sleep(clusterCheckWaitSecond)
			}
		} else {
			return
		}
	}
}

func getRequestAddress(ctx context.Context,
	clusterID uint64, dl *mtAddressList) (string, string, bool) {
	p := client.NewDrummerConnectionPool()
	defer p.Close()
	var conn *client.Connection
	var err error
	for idx := 0; idx < len(dl.apiAddressList); idx++ {
		cctx, cancel := context.WithTimeout(ctx, defaultTestTimeout)
		addr := dl.apiAddressList[idx]
		conn, err = p.GetInsecureConnection(cctx, addr)
		cancel()
		if err != nil {
			plog.Warningf("failed to get drummer connection, %s, %v", addr, err)
			continue
		}
	}
	if conn == nil {
		plog.Warningf("failed to get any connection")
		return "", "", false
	}
	client := pb.NewDrummerClient(conn.ClientConn())
	req := &pb.ClusterStateRequest{ClusterIdList: []uint64{clusterID}}
	resp, err := client.GetClusterStates(ctx, req)
	if err != nil {
		plog.Warningf("failed to get cluster info %v", err)
		return "", "", false
	}
	if len(resp.Collection) != 1 {
		plog.Warningf("collection size is not 1")
		return "", "", false
	}
	ci := resp.Collection[0]
	writeNodeAddress := ""
	readNodeAddress := ""
	readNodeIdx := rand.Int() % len(ci.RPCAddresses)
	writeNodeIdx := rand.Int() % len(ci.RPCAddresses)
	nodeIDList := make([]uint64, 0)
	for nodeID := range ci.RPCAddresses {
		nodeIDList = append(nodeIDList, nodeID)
	}
	for nodeID, addr := range ci.RPCAddresses {
		if nodeID == nodeIDList[writeNodeIdx] {
			writeNodeAddress = addr
		}
		if nodeID == nodeIDList[readNodeIdx] {
			readNodeAddress = addr
		}
	}
	if len(readNodeAddress) == 0 || len(writeNodeAddress) == 0 {
		plog.Warningf("failed to set read/write addresses")
		return "", "", false
	}
	return writeNodeAddress, readNodeAddress, true
}

func getMonkeyTestClients(ctx context.Context, p *client.Pool, writeAddress string,
	readAddress string) (mr.NodehostAPIClient, mr.NodehostAPIClient) {
	writeConn, err := p.GetInsecureConnection(ctx, writeAddress)
	if err != nil {
		plog.Warningf("failed to connect to the write nodehost, %v", err)
		return nil, nil
	}
	writeClient := mr.NewNodehostAPIClient(writeConn.ClientConn())
	readConn, err := p.GetInsecureConnection(ctx, readAddress)
	if err != nil {
		plog.Warningf("failed to connect to the read nodehost, %v", err)
		return nil, nil
	}
	readClient := mr.NewNodehostAPIClient(readConn.ClientConn())
	return readClient, writeClient
}

func makeWriteRequest(ctx context.Context,
	client mr.NodehostAPIClient, clusterID uint64, kv *kvpb.PBKV) bool {
	data, err := proto.Marshal(kv)
	if err != nil {
		panic(err)
	}
	// get client session
	req := &mr.SessionRequest{ClusterId: clusterID}
	cs, err := client.GetSession(ctx, req)
	if err != nil {
		plog.Warningf("failed to get client session for cluster %d, %v",
			clusterID, err)
		return false
	}
	defer client.CloseSession(ctx, cs)
	raftProposal := &mr.RaftProposal{
		Session: *cs,
		Data:    data,
	}
	resp, err := client.Propose(ctx, raftProposal)
	if err == nil {
		if resp.Result != uint64(len(data)) {
			plog.Panicf("result %d, want %d", resp.Result, uint64(len(data)))
		}
		if !cs.IsNoOPSession() {
			cs.ProposalCompleted()
		}
	} else {
		plog.Warningf("failed to make proposal %v", err)
		return false
	}
	return true
}

func makeReadRequest(ctx context.Context,
	client mr.NodehostAPIClient, clusterID uint64, kv *kvpb.PBKV) bool {
	ri := &mr.RaftReadIndex{
		ClusterId: clusterID,
		Data:      []byte(kv.Key),
	}
	resp, err := client.Read(ctx, ri)
	if err != nil {
		plog.Warningf("failed to read, %v", err)
		return false
	} else {
		if string(resp.Data) != kv.Val {
			plog.Panicf("inconsistent state, got %s, want %s",
				string(resp.Data), kv.Val)
		} else {
			plog.Infof("test read write comparison completed successfully")
		}
	}
	return true
}

func makeMonkeyTestRequests(ctx context.Context,
	clusterID uint64, dl *mtAddressList) bool {
	writeAddress, readAddress, ok := getRequestAddress(ctx, clusterID, dl)
	if !ok {
		plog.Warningf("failed to get read write address")
		return false
	}
	pool := client.NewConnectionPool()
	defer pool.Close()
	readClient, writeClient := getMonkeyTestClients(ctx,
		pool, writeAddress, readAddress)
	if writeClient == nil || readClient == nil {
		plog.Warningf("failed to get read write client")
		return false
	}
	repeat := rand.Int()%3 + 1
	for i := 0; i < repeat; i++ {
		key := fmt.Sprintf("key-%d", rand.Uint64())
		val := random.String(rand.Int()%16 + 8)
		kv := &kvpb.PBKV{
			Key: key,
			Val: val,
		}
		cctx, cancel := context.WithTimeout(ctx, defaultTestTimeout)
		if makeWriteRequest(cctx, writeClient, clusterID, kv) {
			makeReadRequest(cctx, readClient, clusterID, kv)
		}
		cancel()
	}
	return true
}

func getRandomClusterID(size uint64) uint64 {
	return (rand.Uint64() % size) + 1
}

func startTestRequestWorkers(stopper *syncutil.Stopper, dl *mtAddressList) {
	for i := uint64(0); i < testClientWorkerCount; i++ {
		stopper.RunWorker(func() {
			pt := time.NewTicker(1 * time.Second)
			tick := 0
			lastDone := 0
			defer pt.Stop()
			for {
				select {
				case <-stopper.ShouldStop():
					return
				default:
				}
				select {
				case <-pt.C:
					tick++
					if tick-lastDone > 5 {
						clusterID := getRandomClusterID(numOfClustersInMonkeyTesting)
						ctx, cancel := context.WithTimeout(context.Background(),
							3*defaultTestTimeout)
						if makeMonkeyTestRequests(ctx, clusterID, dl) {
							lastDone = tick
						}
						cancel()
					}
				}
			}
		})
	}
}

func setRandomPacketDropHook(nodes []*testNode, threshold uint64) {
	// both funcs below return a shouldSend boolean value
	hook := func(batch raftpb.MessageBatch) (raftpb.MessageBatch, bool) {
		pd := random.NewProbability(1000 * threshold)
		if pd.Hit() {
			plog.Infof("going to drop a msg batch for testing purpose")
			return raftpb.MessageBatch{}, false
		}
		pdr := random.NewProbability(5000 * threshold)
		if pdr.Hit() && len(batch.Requests) > 1 {
			dropIdx := random.LockGuardedRand.Uint64() % uint64(len(batch.Requests))
			reqs := make([]raftpb.Message, 0)
			for idx, req := range batch.Requests {
				if uint64(idx) != dropIdx {
					reqs = append(reqs, req)
				}
			}
			if len(reqs) != len(batch.Requests)-1 {
				panic("message not internally dropped")
			} else {
				plog.Infof("internally dropped a message at idx %d", dropIdx)
			}
			batch.Requests = reqs
		}
		return batch, true
	}
	snapshotHook := func(c raftpb.SnapshotChunk) (raftpb.SnapshotChunk, bool) {
		sd := random.NewProbability(1000 * threshold)
		if sd.Hit() {
			plog.Infof("going to drop a snapshot chunk for testing purpose")
			return raftpb.SnapshotChunk{}, false
		}
		return c, true
	}
	for _, n := range nodes {
		n.nh.SetTransportDropBatchHook(hook)
		n.nh.SetPreStreamChunkSendHook(snapshotHook)
	}
}

func drummerMonkeyTesting(t *testing.T, appname string) {
	defer func() {
		if t.Failed() {
			plog.Infof("test failed, going to save the monkey test dir")
			saveMonkeyTestDir()
		} else {
			removeMonkeyTestDir()
		}
	}()
	plog.Infof("snapshot disabled in monkey test %t", snapshotDisabledInRaftConfig())
	dl := getDrummerMonkeyTestAddrList()
	drummerNodes, nodehostNodes := createTestNodeLists(dl)
	startTestNodes(drummerNodes, dl)
	startTestNodes(nodehostNodes, dl)
	setRegionForNodehostNodes(nodehostNodes,
		[]string{"region-1", "region-2", "region-3", "region-4", "region-5"})
	defer func() {
		plog.Infof("cleanup called, going to stop drummer nodes")
		stopTestNodes(drummerNodes)
		plog.Infof("cleanup called, drummer nodes stopped")
	}()
	defer func() {
		plog.Infof("cleanup called, going to stop nodehost nodes")
		stopTestNodes(nodehostNodes)
		plog.Infof("cleanup called, nodehost nodes stopped")
	}()
	plog.Infof("waiting for drummer nodes to stablize")
	waitForStableNodes(drummerNodes, waitForStableSecond)
	plog.Infof("waiting for nodehost nodes to stablize")
	waitForStableNodes(nodehostNodes, waitForStableSecond)
	plog.Infof("all nodes are ready")
	// the first nodehost will use partition mode, all other nodes will use crash
	// mode for testing
	partitionTestDuration := monkeyTestSecondToRun - 100
	nodehostNodes[0].DoPartitionTests(partitionTestDuration)
	// with 50% chance, we let more than one nodehostNode to do partition test
	if rand.Uint64()%2 == 0 {
		nodehostNodes[1].DoPartitionTests(partitionTestDuration)
	}
	// start the cluster
	reportInterval := NodeHostInfoReportSecond
	time.Sleep(time.Duration(3*reportInterval) * time.Second)
	plog.Infof("going to submit jobs")
	if !submitTestJobs(numOfClustersInMonkeyTesting, appname, dl, false) {
		t.Fatalf("failed to submit the test job")
	}
	plog.Infof("jobs submitted, waiting for clusters to be launched")
	waitTimeSec := (loopIntervalFactor + 7) * reportInterval
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	plog.Infof("going to check whether all clusters are launched")
	drummerCleaderChecked := false
	for _, node := range drummerNodes {
		if !node.IsDrummerLeader() {
			continue
		}
		drummerCleaderChecked = true
		mc, err := node.GetMultiCluster()
		if err != nil {
			t.Fatalf("failed to get multiCluster from drummer")
		}
		plog.Infof("num of clusters known to drummer %d", mc.size())
		if uint64(mc.size()) != numOfClustersInMonkeyTesting {
			t.Fatalf("cluster count %d, want %d",
				mc.size(), numOfClustersInMonkeyTesting)
		}
	}
	if !drummerCleaderChecked {
		t.Fatalf("failed to locate drummer leader")
	}
	plog.Infof("launched clusters checked")
	// randomly drop some packet
	setRandomPacketDropHook(drummerNodes, 1)
	setRandomPacketDropHook(nodehostNodes, 1)
	// start a list of client workers that will make random write/read requests
	// to the system
	stopper := syncutil.NewStopper()
	startTestRequestWorkers(stopper, dl)
	// start the linearizability checker manager
	checker := lcm.NewCoordinator(context.Background(),
		LCMWorkerCount, 1, dl.apiAddressList)
	checker.Start()
	// start the monkey play
	low := nodeUpTimeLowMillisecond
	high := nodeUpTimeHighMillisecond
	secondToRun := monkeyTestSecondToRun
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	lt := uint64(0)
	deleteDataTested := true
	brutalMonkeyTime := rand.Uint64()%200 + secondToRun/2
	for i := uint64(0); i < secondToRun; i++ {
		if i > 100 && i%10 == 0 {
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			if memStats.HeapAlloc > maxAllowedHeapSize {
				plog.Errorf("heap size reached max allowed limit, aborting")
				mf, _ := os.Create("drummermt_mem_limit.pprof")
				pprof.WriteHeapProfile(mf)
				mf.Close()
				panic("heap size reached max allowed limit")
			}
		}
		select {
		case <-ticker.C:
			lt++
			if lt < 100 {
				continue
			}
			if lt == brutalMonkeyTime {
				plog.Infof("brutal monkey play is going to start, lt %d", lt)
				brutalMonkeyPlay(nodehostNodes, drummerNodes, low, high)
			}
			deleteDataTested = monkeyPlay(nodehostNodes,
				low, high, lt, deleteDataTested, dl)
			plog.Infof("nodehost node monkey play completed, lt %d", lt)
			monkeyPlay(drummerNodes, low, high, lt, true, dl)
			plog.Infof("drummer node monkey play completed, lt %d", lt)
		}
	}
	plog.Infof("going to stop the test clients")
	// stop all client workers
	stopper.Stop()
	plog.Infof("test clients stopped")
	// restore all nodehost instances and wait for long enough
	startTestNodes(nodehostNodes, dl)
	startTestNodes(drummerNodes, dl)
	setRandomPacketDropHook(nodehostNodes, 0)
	setRandomPacketDropHook(drummerNodes, 0)

	plog.Infof("all nodes restarted")
	waitTimeSec = loopIntervalSecond * 18
	time.Sleep(time.Duration(waitTimeSec) * time.Second)
	drummerCleaderChecked = false
	for _, node := range drummerNodes {
		if !node.Running() {
			t.Fatalf("drummer node not running?")
		}
		if !node.IsDrummerLeader() {
			continue
		}
		drummerCleaderChecked = true
	}
	if !drummerCleaderChecked {
		t.Fatalf("failed to locate drummer leader node")
	}
	// stop the NodeHostInfo reporter on nodehost
	// stop the drummer server
	plog.Infof("going to stop drummer activities")
	stopDrummerActivity(nodehostNodes, drummerNodes)
	time.Sleep(50 * time.Second)
	plog.Infof("going to check drummer cluster info")
	// make sure the cluster is stable with 3 raft nodes
	node := drummerNodes[rand.Uint64()%uint64(len(drummerNodes))]
	mc, tick, err := node.GetMultiClusterAndTick()
	if err != nil {
		t.Fatalf("failed to get multiCluster, %v", err)
	}
	if uint64(mc.size()) != numOfClustersInMonkeyTesting {
		t.Errorf("cluster count %d, want %d",
			mc.size(), numOfClustersInMonkeyTesting)
	}
	toFixCluster := make(map[uint64]bool)
	// all cluster are ok - no dead node, no failed to start node etc.
	r := mc.getClusterForRepair(tick)
	if len(r) != 0 {
		for _, cr := range r {
			toFixCluster[cr.clusterID] = true
		}
		t.Errorf("to be repaired cluster %d, want 0", len(r))
		dumpClusterToRepairInfoToLog(r, tick)
	}
	uc := mc.getUnavailableClusters(tick)
	if len(uc) != 0 {
		for _, cr := range uc {
			toFixCluster[cr.ClusterID] = true
		}
		t.Errorf("unavailable cluster %d, want 0", len(uc))
		dumpUnavailableClusterInfoToLog(uc, tick)
	}
	if len(toFixCluster) > 0 {
		dumpClusterInfoToLog(nodehostNodes, toFixCluster)
	}
	// dump the linearizability checker history data to disk
	checker.Stop()
	lcmlog := "drummer-lcm.jepsen"
	ednlog := "drummer-lcm.edn"
	checker.SaveAsJepsenLog(lcmlog)
	checker.SaveAsEDNLog(ednlog)
	// dump a memory profile to disk
	mf, _ := os.Create("drummermt_mem.pprof")
	defer mf.Close()
	pprof.WriteHeapProfile(mf)
	plog.Infof("going to restart drummer servers")
	stopTestNodes(drummerNodes)
	startTestNodes(drummerNodes, dl)
	time.Sleep(30 * time.Second)
	checkPartitionedNodeHost(t, nodehostNodes)
	plog.Infof("going to check nodehost cluster state")
	waitForStableNodes(nodehostNodes, waitForStableSecond)
	plog.Infof("clusters stable check done")
	checkNodeHostSynced(t, nodehostNodes)
	plog.Infof("sync check done")
	checkStateMachine(t, nodehostNodes)
	plog.Infof("state machine check done")
	waitForStableNodes(drummerNodes, waitForStableSecond)
	plog.Infof("drummer nodes stable check done")
	checkNodeHostSynced(t, drummerNodes)
	plog.Infof("drummer sync check done")
	checkStateMachine(t, drummerNodes)
	plog.Infof("check logdb entries")
	if snapshotDisabledInRaftConfig() {
		checkLogdbEntriesSynced(t, nodehostNodes)
	}
	plog.Infof("going to check in mem log sizes")
	if !rateLimiterDisabledInRaftConfig() {
		checkRateLimiterState(t, nodehostNodes)
	}
	plog.Infof("going to check cluster accessibility")
	checkClustersAreAccessible(t, numOfClustersInMonkeyTesting, dl)
	plog.Infof("cluster accessibility check done, test is going to return.")
}
