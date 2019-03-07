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
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"

	pb "github.com/lni/dragonboat/internal/drummer/drummerpb"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/statemachine"
)

const (
	// defaultClusterID is the default cluster id assigned to DB
	defaultClusterID = uint64(0)
	// deploymentIDKey is the key for deployment id value
	deploymentIDKey = "deployment-id"
	// launchedKey is the key for the launched flag.
	launchedKey = "launched-flag"
	// bootstrappedKey is the key for the bootstrapped flag.
	bootstrappedKey = "bootstrapped-flag"
	// electionKey is the key for election related info.
	electionKey = "election-key"
	// regionsKey is the key for regions configuration.
	regionsKey = "regions-key"
)

const (
	// DBUpdated indicates DB has been successfully updated
	DBUpdated uint64 = 0
	// ClusterExists means DB update has been rejected as the cluster to
	// be created already exist.
	ClusterExists uint64 = 1
	// DBBootstrapped means DB update has been rejected as the
	// DB has been bootstrapped.
	DBBootstrapped uint64 = 2
	// Current schema of the Drummer DB
	currentVersion uint64 = 1
)

const (
	// DBKVUpdated means the KV update has been successfully completed
	DBKVUpdated uint64 = 0
	// DBKVFinalized indicates that the KV update is rejected as there is
	// already a finalized record in DB with the specified key
	DBKVFinalized uint64 = 1
	// DBKVRejected indicates that the KV update is rejected
	DBKVRejected       uint64 = 2
	launchDeadlineTick        = settings.LaunchDeadlineTick
)

// DB is the struct used to maintain the raft-backed Drummer DB
type DB struct {
	ClusterID      uint64 `json:"-"`
	NodeID         uint64 `json:"-"`
	Version        uint64
	Tick           uint64
	LaunchDeadline uint64
	Failed         bool
	Clusters       map[uint64]*pb.Cluster
	KVMap          map[string][]byte
	ClusterImage   *multiCluster
	NodeHostImage  *multiNodeHost
	NodeHostInfo   map[string]pb.NodeHostInfo
	Requests       map[string][]pb.NodeHostRequest
	Outgoing       map[string][]pb.NodeHostRequest
}

type schedulerContext struct {
	Tick          uint64
	Clusters      map[uint64]*pb.Cluster
	Regions       *pb.Regions
	ClusterImage  *multiCluster
	NodeHostImage *multiNodeHost
	NodeHostInfo  map[string]pb.NodeHostInfo
}

// NewDB creates a new DB instance.
func NewDB(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
	plog.Infof("drummer DB is being created, cluster id: %d, node id: %d",
		clusterID, nodeID)
	d := &DB{
		Version:       currentVersion,
		ClusterID:     clusterID,
		NodeID:        nodeID,
		Clusters:      make(map[uint64]*pb.Cluster),
		KVMap:         make(map[string][]byte),
		ClusterImage:  newMultiCluster(),
		NodeHostImage: newMultiNodeHost(),
		NodeHostInfo:  make(map[string]pb.NodeHostInfo),
		Requests:      make(map[string][]pb.NodeHostRequest),
		Outgoing:      make(map[string][]pb.NodeHostRequest),
	}

	return d
}

func (d *DB) getLaunchedClusters() map[uint64]struct{} {
	clusters := make(map[uint64]struct{})
	for _, c := range d.ClusterImage.Clusters {
		count := 0
		for _, n := range c.Nodes {
			if n.Tick > 0 {
				count++
			}
		}
		if count == len(c.Nodes) {
			clusters[c.ClusterID] = struct{}{}
		}
	}
	return clusters
}

func (d *DB) onUpdatedClusterInfo() {
	if d.LaunchDeadline > 0 {
		launchedClusters := d.getLaunchedClusters()
		if len(launchedClusters) == len(d.Clusters) {
			plog.Infof("all clusters have been launched")
			d.LaunchDeadline = 0
		} else {
			plog.Infof("waiting for more clusters to be launched")
		}
	}
}

func (d *DB) checkLaunchDeadline() {
	if d.LaunchDeadline > 0 && d.Tick > d.LaunchDeadline {
		d.Failed = true
	}
	d.assertNotFailed()
}

// SaveSnapshot generates a snapshot of the DB
func (d *DB) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection,
	done <-chan struct{}) (uint64, error) {
	d.assertNotFailed()
	data, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	_, err = w.Write(data)
	if err != nil {
		return 0, err
	}
	return uint64(len(data)), nil
}

// RecoverFromSnapshot recovers DB state from a snapshot.
func (d *DB) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile,
	done <-chan struct{}) error {
	d.assertNotFailed()
	db := DB{}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &db); err != nil {
		panic(err)
	}
	if d.Version != db.Version {
		// currently there is only one version
		panic("drummer db schema version mismatch")
	}
	d.Tick = db.Tick
	d.Version = db.Version
	d.Failed = db.Failed
	d.LaunchDeadline = db.LaunchDeadline
	d.Clusters = db.Clusters
	d.KVMap = db.KVMap
	d.ClusterImage = db.ClusterImage
	d.NodeHostImage = db.NodeHostImage
	d.NodeHostInfo = db.NodeHostInfo
	d.Requests = db.Requests
	d.Outgoing = db.Outgoing

	return nil
}

// Close closes the DB instance.
func (d *DB) Close() {}

// GetHash returns the state machine hash.
func (d *DB) GetHash() uint64 {
	d.assertNotFailed()
	data, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	hash := md5.New()
	if _, err = hash.Write(data); err != nil {
		panic(err)
	}
	md5sum := hash.Sum(nil)

	return binary.LittleEndian.Uint64(md5sum[:8])
}

// Update updates the DB instance.
func (d *DB) Update(data []byte) uint64 {
	d.assertNotFailed()
	var c pb.Update
	if err := c.Unmarshal(data); err != nil {
		panic(err)
	}
	if c.Type == pb.Update_CLUSTER {
		return d.applyClusterUpdate(c.Change)
	} else if c.Type == pb.Update_KV {
		return d.applyKVUpdate(c.KvUpdate)
	} else if c.Type == pb.Update_NODEHOST_INFO {
		return d.applyNodeHostInfoUpdate(c.NodehostInfo)
	} else if c.Type == pb.Update_REQUESTS {
		return d.applyRequestsUpdate(c.Requests)
	} else if c.Type == pb.Update_TICK {
		return d.applyTickUpdate()
	}
	panic("Unknown update type")
}

func (d *DB) applyTickUpdate() uint64 {
	d.Tick += tickIntervalSecond
	d.checkLaunchDeadline()
	return d.Tick
}

func (d *DB) applyNodeHostInfoUpdate(nhi pb.NodeHostInfo) uint64 {
	count := uint64(0)
	delete(d.Outgoing, nhi.RaftAddress)
	nhi.LastTick = d.Tick
	d.NodeHostInfo[nhi.RaftAddress] = nhi
	d.ClusterImage.update(nhi)
	d.NodeHostImage.update(nhi)
	d.NodeHostImage.syncClusterInfo(d.ClusterImage)
	reqs, ok := d.Requests[nhi.RaftAddress]
	if ok {
		count = uint64(len(reqs))
		delete(d.Requests, nhi.RaftAddress)
		d.Outgoing[nhi.RaftAddress] = reqs
	}
	d.onUpdatedClusterInfo()
	return count
}

func isLaunchRequests(reqs pb.NodeHostRequestCollection) bool {
	launch := 0
	for _, r := range reqs.Requests {
		if r.Change.Type == pb.Request_CREATE && !r.Join && !r.Restore {
			launch++
		}
	}
	if launch > 0 && launch != len(reqs.Requests) {
		panic("found launch request, but not all requests are launch requests")
	}
	return launch > 0
}

func (d *DB) setLaunched() {
	kv := pb.KV{
		Key:       launchedKey,
		Value:     "true",
		Finalized: true,
	}
	if d.applyKVUpdate(kv) != DBKVUpdated {
		panic("failed to set the launched flag")
	} else {
		plog.Infof("Drummer DB has been marked as launched")
	}
}

func (d *DB) launched() bool {
	_, launched := d.KVMap[launchedKey]
	return launched
}

func (d *DB) applyRequestsUpdate(reqs pb.NodeHostRequestCollection) uint64 {
	launch := isLaunchRequests(reqs)
	launched := d.launched()
	if launched && launch {
		plog.Warningf("trying to set launch requests again, ignored")
		return 0
	}
	requests := make(map[string][]pb.NodeHostRequest)
	for _, r := range reqs.Requests {
		q, ok := requests[r.RaftAddress]
		if !ok {
			q = make([]pb.NodeHostRequest, 0)
		}
		q = append(q, r)
		requests[r.RaftAddress] = q
	}
	for addr, r := range requests {
		d.Requests[addr] = r
	}
	if launch {
		d.setLaunched()
		d.LaunchDeadline = d.Tick + launchDeadlineTick*tickIntervalSecond
	}
	return uint64(len(reqs.Requests))
}

func (d *DB) applyKVUpdate(kv pb.KV) uint64 {
	if len(kv.Key) == 0 || len(kv.Value) == 0 {
		panic("key and value can not be empty")
	}
	mkv, err := kv.Marshal()
	if err != nil {
		panic(err)
	}
	data, ok := d.KVMap[kv.Key]
	if !ok {
		d.KVMap[kv.Key] = mkv
		return DBKVUpdated
	}
	var oldRec pb.KV
	err = oldRec.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	if oldRec.Finalized {
		return DBKVFinalized
	} else if oldRec.InstanceId == kv.InstanceId ||
		oldRec.InstanceId == kv.OldInstanceId {
		d.KVMap[kv.Key] = mkv
		return DBKVUpdated
	}
	return DBKVRejected
}

func (d *DB) applyClusterUpdate(c pb.Change) uint64 {
	if c.Type == pb.Change_CREATE {
		return d.tryCreateCluster(c)
	}
	panic("unknown change type value")
}

func (d *DB) bootstrapped() bool {
	_, ok := d.KVMap[bootstrappedKey]
	return ok
}

func (d *DB) tryCreateCluster(c pb.Change) uint64 {
	if len(c.Members) == 0 {
		panic("DrummerChange.Members should be of size 1 at least")
	}
	if len(c.AppName) == 0 {
		panic("empty app name is not allowed")
	}
	if d.bootstrapped() {
		plog.Errorf("CREATE cluster is not allowed after bootstrap")
		return DBBootstrapped
	}
	if _, ok := d.Clusters[c.ClusterId]; ok {
		return ClusterExists
	}
	members := make([]uint64, len(c.Members))
	copy(members, c.Members)
	d.Clusters[c.ClusterId] = &pb.Cluster{
		Members:   members,
		ClusterId: c.ClusterId,
		AppName:   c.AppName,
	}
	return DBUpdated
}

// Lookup performances local data lookup on the DB.
func (d *DB) Lookup(key []byte) []byte {
	d.assertNotFailed()
	var req pb.LookupRequest
	if err := req.Unmarshal(key); err != nil {
		panic(err)
	}
	if req.Type == pb.LookupRequest_CLUSTER {
		return d.handleClusterLookup(req)
	} else if req.Type == pb.LookupRequest_KV {
		return d.handleKVLookup(req)
	} else if req.Type == pb.LookupRequest_SCHEDULER_CONTEXT {
		return d.handleSchedulerContextLookup()
	} else if req.Type == pb.LookupRequest_REQUESTS {
		return d.handleRequestsLookup(req)
	} else if req.Type == pb.LookupRequest_CLUSTER_STATES {
		return d.handleClusterStatesLookup(req)
	}
	panic("unknown request type")
}

func (d *DB) handleClusterStatesLookup(req pb.LookupRequest) []byte {
	c := &pb.ClusterStates{}
	for _, clusterID := range req.Stats.ClusterIdList {
		ci, err := toClusterState(d.ClusterImage,
			d.NodeHostImage, d.Tick, clusterID)
		if err != nil {
			return nil
		}
		c.Collection = append(c.Collection, ci)
	}
	data, err := c.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *DB) handleSchedulerContextLookup() []byte {
	resp := schedulerContext{
		Tick:          d.Tick,
		Clusters:      d.Clusters,
		ClusterImage:  d.ClusterImage,
		NodeHostImage: d.NodeHostImage,
		NodeHostInfo:  d.NodeHostInfo,
	}
	kvData, ok := d.KVMap[regionsKey]
	if ok {
		var kv pb.KV
		var regions pb.Regions
		err := kv.Unmarshal(kvData)
		if err != nil {
			panic(err)
		}
		err = regions.Unmarshal([]byte(kv.Value))
		if err != nil {
			panic(err)
		}
		resp.Regions = &regions
	}
	data, err := json.Marshal(&resp)
	if err != nil {
		panic(err)
	}
	return data
}

func (d *DB) handleRequestsLookup(req pb.LookupRequest) []byte {
	var resp pb.LookupResponse
	resp.Code = pb.LookupResponse_OK
	reqs, ok := d.Outgoing[req.Address]
	if ok {
		resp.Requests.Requests = reqs
	}
	data, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *DB) handleKVLookup(req pb.LookupRequest) []byte {
	key := req.KvLookup.Key
	if len(key) == 0 {
		panic("empty key is not allowed")
	}
	var resp pb.LookupResponse
	resp.Code = pb.LookupResponse_OK
	v, ok := d.KVMap[key]
	if ok {
		var kv pb.KV
		err := kv.Unmarshal(v)
		if err != nil {
			panic(err)
		}
		resp.KvResult = kv
	}
	result, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return result
}

func (d *DB) handleClusterLookup(req pb.LookupRequest) []byte {
	var resp pb.LookupResponse
	resp.Code = pb.LookupResponse_OK
	clusters := make([]*pb.Cluster, 0)
	for _, v := range d.Clusters {
		m := make([]uint64, len(v.Members))
		copy(m, v.Members)
		cc := pb.Cluster{
			ClusterId: v.ClusterId,
			Members:   m,
			AppName:   v.AppName,
		}
		clusters = append(clusters, &cc)
	}
	resp.Clusters = clusters
	result, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return result
}

func (d *DB) assertNotFailed() {
	if d.Failed {
		panic("Drummer based system failed to launch")
	}
}
