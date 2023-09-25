// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

/*
Package dragonboat is a multi-group Raft implementation.

The NodeHost struct is the facade interface for all features provided by the
dragonboat package. Each NodeHost instance usually runs on a separate host
managing its CPU, storage and network resources. Each NodeHost can manage Raft
nodes from many different Raft groups known as Raft clusters. Each Raft cluster
is identified by its ClusterID and it usually consists of multiple nodes, each
identified its NodeID value. Nodes from the same Raft cluster can be considered
as replicas of the same data, they are suppose to be distributed on different
NodeHost instances across the network, this brings fault tolerance to machine
and network failures as application data stored in the Raft cluster will be
available as long as the majority of its managing NodeHost instances (i.e. its
underlying hosts) are available.

User applications can leverage the power of the Raft protocol implemented in
dragonboat by implementing the IStateMachine or IOnDiskStateMachine component,
as defined in github.com/lni/dragonboat/v3/statemachine. Known as user state
machines, each IStateMachine and IOnDiskStateMachine instance is in charge of
updating, querying and snapshotting application data with minimum exposure to
the complexity of the Raft protocol implementation.

User applications can use NodeHost's APIs to update the state of their
IStateMachine or IOnDiskStateMachine instances, this is called making proposals.
Once accepted by the majority nodes of a Raft cluster, the proposal is
considered as committed and it will be applied on all member nodes of the Raft
cluster. Applications can also make linearizable reads to query the state of the
IStateMachine or IOnDiskStateMachine instances. Dragonboat employs the ReadIndex
protocol invented by Diego Ongaro for fast linearizable reads.

Dragonboat guarantees the linearizability of your I/O when interacting with the
IStateMachine or IOnDiskStateMachine instances. In plain English, writes (via
making proposal) to your Raft cluster appears to be instantaneous, once a write
is completed, all later reads (linearizable read using the ReadIndex protocol
as implemented and provided in dragonboat) should return the value of that write
or a later write. Once a value is returned by a linearizable read, all later
reads should return the same value or the result of a later write.

To strictly provide such guarantee, we need to implement the at-most-once
semantic required by linearizability. For a client, when it retries the proposal
that failed to complete before its deadline during the previous attempt, it has
the risk to have the same proposal committed and applied twice into the user
state machine. Dragonboat prevents this by implementing the client session
concept described in Diego Ongaro's PhD thesis.

Arbitrary number of Raft clusters can be launched across the network
simultaneously to aggregate distributed processing and storage capacities. Users
can also make membership change requests to add or remove nodes from any
interested Raft cluster.

NodeHost APIs for making the above mentioned requests can be loosely classified
into two categories, synchronous and asynchronous APIs. Synchronous APIs will
not return until the completion of the requested operation. Their method names
all start with Sync*. The asynchronous counterparts of such synchronous APIs,
on the other hand, usually return immediately. This allows users to concurrently
initiate multiple such asynchronous operations to save the total amount of time
required to complete all of them.

Dragonboat is a feature complete Multi-Group Raft implementation - snapshotting,
membership change, leadership transfer, non-voting members and disk based state
machine are all provided.

Dragonboat is also extensively optimized. The Raft protocol implementation is
fully pipelined, meaning proposals can start before the completion of previous
proposals. This is critical for system throughput in high latency environment.
Dragonboat is also fully batched, internal operations are batched whenever
possible to maximize the overall throughput.
*/
package dragonboat // github.com/lni/dragonboat/v3

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/id"
	"github.com/lni/dragonboat/v3/internal/invariants"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/transport"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	// DragonboatMajor is the major version number
	DragonboatMajor = 3
	// DragonboatMinor is the minor version number
	DragonboatMinor = 3
	// DragonboatPatch is the patch version number
	DragonboatPatch = 8
	// DEVVersion is a boolean flag indicating whether this is a dev version
	DEVVersion = false
)

var (
	receiveQueueLen   = settings.Soft.ReceiveQueueLength
	requestPoolShards = settings.Soft.NodeHostRequestStatePoolShards
	streamConnections = settings.Soft.StreamConnections
)

var (
	// ErrClosed is returned when a request is made on closed NodeHost instance.
	ErrClosed = errors.New("dragonboat: closed")
	// ErrNodeRemoved indictes that the requested node has been removed.
	ErrNodeRemoved = errors.New("node removed")
	// ErrClusterNotFound indicates that the specified cluster is not found.
	ErrClusterNotFound = errors.New("cluster not found")
	// ErrClusterAlreadyExist indicates that the specified cluster already exist.
	ErrClusterAlreadyExist = errors.New("cluster already exist")
	// ErrClusterNotStopped indicates that the specified cluster is still running
	// and thus prevented the requested operation to be completed.
	ErrClusterNotStopped = errors.New("cluster not stopped")
	// ErrInvalidClusterSettings indicates that cluster settings specified for
	// the StartCluster method are invalid.
	ErrInvalidClusterSettings = errors.New("cluster settings are invalid")
	// ErrClusterNotBootstrapped indicates that the specified cluster has not
	// been boostrapped yet. When starting this node, depending on whether this
	// node is an initial member of the Raft cluster, you must either specify
	// all of its initial members or set the join flag to true.
	// When used correctly, dragonboat only returns this error in the rare
	// situation when you try to restart a node crashed during its previous
	// bootstrap attempt.
	ErrClusterNotBootstrapped = errors.New("cluster not bootstrapped")
	// ErrDeadlineNotSet indicates that the context parameter provided does not
	// carry a deadline.
	ErrDeadlineNotSet = errors.New("deadline not set")
	// ErrInvalidDeadline indicates that the specified deadline is invalid, e.g.
	// time in the past.
	ErrInvalidDeadline = errors.New("invalid deadline")
	// ErrDirNotExist indicates that the specified dir does not exist.
	ErrDirNotExist = errors.New("specified dir does not exist")
)

// ClusterInfo is a record for representing the state of a Raft cluster based
// on the knowledge of the local NodeHost instance.
type ClusterInfo struct {
	// ClusterID is the cluster ID of the Raft cluster node.
	ClusterID uint64
	// NodeID is the node ID of the Raft cluster node.
	NodeID uint64
	// Nodes is a map of member node IDs to their Raft addresses.
	Nodes map[uint64]string
	// ConfigChangeIndex is the current config change index of the Raft node.
	// ConfigChangeIndex is Raft Log index of the last applied membership
	// change entry.
	ConfigChangeIndex uint64
	// StateMachineType is the type of the state machine.
	StateMachineType sm.Type
	// IsLeader indicates whether this is a leader node.
	IsLeader bool
	// IsObserver indicates whether this is a non-voting observer node.
	IsObserver bool
	// IsWitness indicates whether this is a witness node without actual log.
	IsWitness bool
	// Pending is a boolean flag indicating whether details of the cluster node
	// is not available. The Pending flag is set to true usually because the node
	// has not had anything applied yet.
	Pending bool
}

// GossipInfo contains details of the gossip service.
type GossipInfo struct {
	// Enabled is a boolean flag indicating whether the gossip service is enabled.
	Enabled bool
	// AdvertiseAddress is the advertise address used by the gossip service.
	AdvertiseAddress string
	// NumOfLiveNodeHosts is the number of current live NodeHost instances known
	// to the gossip service. Note that the gossip service always knowns the
	// local NodeHost instance itself. When the NumOfKnownNodeHosts value is 1,
	// it means the gossip service doesn't know any other NodeHost instance that
	// is considered as live.
	NumOfKnownNodeHosts int
}

// NodeHostInfo provides info about the NodeHost, including its managed Raft
// cluster nodes and available Raft logs saved in its local persistent storage.
type NodeHostInfo struct {
	// NodeHostID is the unique identifier of the NodeHost instance.
	NodeHostID string
	// RaftAddress is the public address of the NodeHost used for exchanging Raft
	// messages, snapshots and other metadata with other NodeHost instances.
	RaftAddress string
	// Gossip contains gossip service related information.
	Gossip GossipInfo
	// ClusterInfo is a list of all Raft clusters managed by the NodeHost
	ClusterInfoList []ClusterInfo
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost.
	LogInfo []raftio.NodeInfo
}

// NodeHostInfoOption is the option type used when querying NodeHostInfo.
type NodeHostInfoOption struct {
	// SkipLogInfo is the boolean flag indicating whether Raft Log info should be
	// skipped when querying the NodeHostInfo.
	SkipLogInfo bool
}

// DefaultNodeHostInfoOption is the default NodeHostInfoOption value. It
// requests the GetNodeHostInfo method to return all supported info.
var DefaultNodeHostInfoOption NodeHostInfoOption

// SnapshotOption is the options users can specify when requesting a snapshot
// to be generated.
type SnapshotOption struct {
	// CompactionOverhead is the compaction overhead value to use for the request
	// snapshot operation when OverrideCompactionOverhead is true. This field is
	// ignored when exporting a snapshot, that is when Exported is true.
	CompactionOverhead uint64
	// ExportPath is the path where the exported snapshot should be stored, it
	// must point to an existing directory for which the current user has write
	// permission to it.
	ExportPath string
	// Exported is a boolean flag indicating whether the snapshot requested to
	// be generated should be exported. For an exported snapshot, it is users'
	// responsibility to manage the snapshot files. By default, a requested
	// snapshot is not considered as exported, such a regular snapshot is managed
	// the system.
	Exported bool
	// OverrideCompactionOverhead defines whether the requested snapshot operation
	// should override the compaction overhead setting specified in node's config.
	// This field is ignored by the system when exporting a snapshot.
	OverrideCompactionOverhead bool
}

// DefaultSnapshotOption is the default SnapshotOption value to use when
// requesting a snapshot to be generated by using NodeHost's RequestSnapshot
// method. DefaultSnapshotOption causes a regular snapshot to be generated
// and the generated snapshot is managed by the system.
var DefaultSnapshotOption SnapshotOption

// Target is the type used to specify where a node is running. Target is remote
// NodeHost's RaftAddress value when NodeHostConfig.AddressByNodeHostID is not
// set. Target will use NodeHost's ID value when
// NodeHostConfig.AddressByNodeHostID is set.
type Target = string

// NodeHost manages Raft clusters and enables them to share resources such as
// transport and persistent storage etc. NodeHost is also the central thread
// safe access point for Dragonboat functionalities.
type NodeHost struct {
	closed      int32
	partitioned int32
	mu          struct {
		sync.RWMutex
		cci   uint64
		cciCh chan struct{}
		// clusterID -> *node
		clusters sync.Map
		// shardID -> *logDBMetrics
		lm sync.Map
		// mu must be locked when logdb is accessed from a user goroutine
		logdb raftio.ILogDB
	}
	events struct {
		leaderInfoQ *leaderInfoQueue
		raft        raftio.IRaftEventListener
		sys         *sysEventListener
	}
	env          *server.Env
	nhConfig     config.NodeHostConfig
	stopper      *syncutil.Stopper
	nodes        transport.INodeRegistry
	requestPools []*sync.Pool
	engine       *engine
	transport    transport.ITransport
	msgHandler   *messageHandler
	fs           vfs.IFS
	id           *id.NodeHostID
}

var _ nodeLoader = (*NodeHost)(nil)

var dn = logutil.DescribeNode

// NewNodeHost creates a new NodeHost instance. The returned NodeHost instance
// is configured using the specified NodeHostConfig instance. In a typical
// application, it is expected to have one NodeHost on each server.
func NewNodeHost(nhConfig config.NodeHostConfig) (*NodeHost, error) {
	logBuildTagsAndVersion()
	if err := nhConfig.Validate(); err != nil {
		return nil, err
	}
	if err := nhConfig.Prepare(); err != nil {
		return nil, err
	}
	env, err := server.NewEnv(nhConfig, nhConfig.Expert.FS)
	if err != nil {
		return nil, err
	}
	nh := &NodeHost{
		env:      env,
		nhConfig: nhConfig,
		stopper:  syncutil.NewStopper(),
		fs:       nhConfig.Expert.FS,
	}
	// make static check happy
	_ = nh.partitioned
	nh.events.raft = nhConfig.RaftEventListener
	nh.events.sys = newSysEventListener(nhConfig.SystemEventListener,
		nh.stopper.ShouldStop())
	nh.mu.cciCh = make(chan struct{}, 1)
	if nhConfig.RaftEventListener != nil {
		nh.events.leaderInfoQ = newLeaderInfoQueue()
	}
	if nhConfig.RaftEventListener != nil || nhConfig.SystemEventListener != nil {
		nh.stopper.RunWorker(func() {
			nh.handleListenerEvents()
		})
	}
	nh.msgHandler = newNodeHostMessageHandler(nh)
	nh.createPools()
	defer func() {
		if r := recover(); r != nil {
			nh.Stop()
			if r, ok := r.(error); ok {
				panicNow(r)
			}
		}
	}()
	did := nh.nhConfig.GetDeploymentID()
	plog.Infof("DeploymentID set to %d", did)
	if err := nh.createLogDB(); err != nil {
		nh.Stop()
		return nil, err
	}
	if err := nh.loadNodeHostID(); err != nil {
		nh.Stop()
		return nil, err
	}
	plog.Infof("NodeHost ID: %s", nh.id.String())
	if err := nh.createNodeRegistry(); err != nil {
		nh.Stop()
		return nil, err
	}
	errorInjection := false
	if nhConfig.Expert.FS != nil {
		_, errorInjection = nhConfig.Expert.FS.(*vfs.ErrorFS)
		plog.Infof("filesystem error injection mode enabled: %t", errorInjection)
	}
	nh.engine = newExecEngine(nh, nhConfig.Expert.Engine,
		nh.nhConfig.NotifyCommit, errorInjection, nh.env, nh.mu.logdb)
	if err := nh.createTransport(); err != nil {
		nh.Stop()
		return nil, err
	}
	nh.stopper.RunWorker(func() {
		nh.nodeMonitorMain()
	})
	nh.stopper.RunWorker(func() {
		nh.tickWorkerMain()
	})
	nh.logNodeHostDetails()
	return nh, nil
}

// NodeHostConfig returns the NodeHostConfig instance used for configuring this
// NodeHost instance.
func (nh *NodeHost) NodeHostConfig() config.NodeHostConfig {
	return nh.nhConfig
}

// RaftAddress returns the Raft address of the NodeHost instance, it is the
// network address by which the NodeHost can be reached by other NodeHost
// instances for exchanging Raft messages, snapshots and other metadata.
func (nh *NodeHost) RaftAddress() string {
	return nh.nhConfig.RaftAddress
}

// ID returns the string representation of the NodeHost ID value. The NodeHost
// ID is assigned to each NodeHost on its initial creation and it can be used
// to uniquely identify the NodeHost instance for its entire life cycle. When
// the system is running in the AddressByNodeHost mode, it is used as the target
// value when calling the StartCluster, RequestAddNode, RequestAddObserver,
// RequestAddWitness methods.
func (nh *NodeHost) ID() string {
	return nh.id.String()
}

// Stop stops all Raft nodes managed by the NodeHost instance, it also closes
// all internal components such as the transport and LogDB modules.
func (nh *NodeHost) Stop() {
	nh.events.sys.Publish(server.SystemEvent{
		Type: server.NodeHostShuttingDown,
	})
	nh.mu.Lock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		panic("NodeHost.Stop called twice")
	}
	atomic.StoreInt32(&nh.closed, 1)
	nh.mu.Unlock()
	nodes := make([]raftio.NodeInfo, 0)
	nh.forEachCluster(func(cid uint64, node *node) bool {
		nodes = append(nodes, raftio.NodeInfo{
			ClusterID: node.clusterID,
			NodeID:    node.nodeID,
		})
		return true
	})
	for _, node := range nodes {
		if err := nh.stopNode(node.ClusterID, node.NodeID, true); err != nil {
			plog.Errorf("failed to remove cluster %s",
				logutil.ClusterID(node.ClusterID))
		}
	}
	plog.Debugf("%s is stopping the nh stopper", nh.describe())
	nh.stopper.Stop()
	if nh.nodes != nil {
		nh.nodes.Stop()
	}
	plog.Debugf("%s is stopping the tranport module", nh.describe())
	if nh.transport != nil {
		nh.transport.Stop()
	}
	plog.Debugf("%s is stopping the engine module", nh.describe())
	if nh.engine != nil {
		nh.engine.stop()
	}
	plog.Debugf("%s is stopping the logdb module", nh.describe())
	if nh.mu.logdb != nil {
		if err := nh.mu.logdb.Close(); err != nil {
			panicNow(err)
		}
		nh.mu.logdb = nil
	}
	plog.Debugf("%s is stopping the env module", nh.describe())
	nh.env.Stop()
	plog.Debugf("NodeHost %s stopped", nh.describe())
}

// StartCluster adds the specified Raft cluster node to the NodeHost and starts
// the node to make it ready for accepting incoming requests. The node to be
// started is backed by a regular state machine that implements the
// sm.IStateMachine interface.
//
// The input parameter initialMembers is a map of node ID to node target for all
// Raft cluster's initial member nodes. By default, the target is the
// RaftAddress value of the NodeHost where the node will be running. When running
// in the AddressByNodeHostID mode, target should be set to the NodeHostID value
// of the NodeHost where the node will be running. See the godoc of NodeHost's ID
// method for the full definition of NodeHostID. For the same Raft cluster, the
// same initialMembers map should be specified when starting its initial member
// nodes on distributed NodeHost instances.
//
// The join flag indicates whether the node is a new node joining an existing
// cluster. create is a factory function for creating the IStateMachine instance,
// cfg is the configuration instance that will be passed to the underlying Raft
// node object, the cluster ID and node ID of the involved node are specified in
// the ClusterID and NodeID fields of the provided cfg parameter.
//
// Note that this method is not for changing the membership of the specified
// Raft cluster, it launches a node that is already a member of the Raft
// cluster.
//
// As a summary, when -
//   - starting a brand new Raft cluster, set join to false and specify all initial
//     member node details in the initialMembers map.
//   - joining a new node to an existing Raft cluster, set join to true and leave
//     the initialMembers map empty. This requires the joining node to have already
//     been added as a member node of the Raft cluster.
//   - restarting an crashed or stopped node, set join to false and leave the
//     initialMembers map to be empty. This applies to both initial member nodes
//     and those joined later.
func (nh *NodeHost) StartCluster(initialMembers map[uint64]Target,
	join bool, create sm.CreateStateMachineFunc, cfg config.Config) error {
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(clusterID, nodeID)
		return rsm.NewNativeSM(cfg, rsm.NewInMemStateMachine(sm), done)
	}
	return nh.startCluster(initialMembers, join, cf, cfg, pb.RegularStateMachine)
}

// StartConcurrentCluster is similar to the StartCluster method but it is used
// to start a Raft node backed by a concurrent state machine.
func (nh *NodeHost) StartConcurrentCluster(initialMembers map[uint64]Target,
	join bool, create sm.CreateConcurrentStateMachineFunc, cfg config.Config) error {
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(clusterID, nodeID)
		return rsm.NewNativeSM(cfg, rsm.NewConcurrentStateMachine(sm), done)
	}
	return nh.startCluster(initialMembers,
		join, cf, cfg, pb.ConcurrentStateMachine)
}

// StartOnDiskCluster is similar to the StartCluster method but it is used to
// start a Raft node backed by an IOnDiskStateMachine.
func (nh *NodeHost) StartOnDiskCluster(initialMembers map[uint64]Target,
	join bool, create sm.CreateOnDiskStateMachineFunc, cfg config.Config) error {
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(clusterID, nodeID)
		return rsm.NewNativeSM(cfg, rsm.NewOnDiskStateMachine(sm), done)
	}
	return nh.startCluster(initialMembers,
		join, cf, cfg, pb.OnDiskStateMachine)
}

// StopCluster removes and stops the Raft node associated with the specified
// Raft cluster from the NodeHost. The node to be removed and stopped is
// identified by the clusterID value.
//
// Note that this is not the membership change operation to remove the node
// from the Raft cluster.
func (nh *NodeHost) StopCluster(clusterID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	return nh.stopNode(clusterID, 0, false)
}

// StopNode removes the specified Raft cluster node from the NodeHost and
// stops that running Raft node.
//
// Note that this is not the membership change operation to remove the node
// from the Raft cluster.
func (nh *NodeHost) StopNode(clusterID uint64, nodeID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	return nh.stopNode(clusterID, nodeID, true)
}

// SyncPropose makes a synchronous proposal on the Raft cluster specified by
// the input client session object. The specified context parameter must has
// the timeout value set.
//
// SyncPropose returns the result returned by IStateMachine or
// IOnDiskStateMachine's Update method, or the error encountered. The input
// byte slice can be reused for other purposes immediate after the return of
// this method.
//
// After calling SyncPropose, unless NO-OP client session is used, it is
// caller's responsibility to update the client session instance accordingly
// based on SyncPropose's outcome. Basically, when a ErrTimeout error is
// returned, application can retry the same proposal without updating the
// client session instance. When ErrInvalidSession error is returned, it
// usually means the session instance has been evicted from the server side,
// the Raft paper recommends to crash the client in this highly unlikely
// event. When the proposal completed successfully, caller must call
// client.ProposalCompleted() to get it ready to be used in future proposals.
func (nh *NodeHost) SyncPropose(ctx context.Context,
	session *client.Session, cmd []byte) (sm.Result, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return sm.Result{}, err
	}
	rs, err := nh.Propose(session, cmd, timeout)
	if err != nil {
		return sm.Result{}, err
	}
	result, err := getRequestState(ctx, rs)
	if err != nil {
		return sm.Result{}, err
	}
	rs.Release()
	return result, nil
}

// SyncRead performs a synchronous linearizable read on the specified Raft
// cluster. The specified context parameter must has the timeout value set. The
// query byte slice specifies what to query, it will be passed to the Lookup
// method of the IStateMachine or IOnDiskStateMachine after the system
// determines that it is safe to perform the local read on IStateMachine or
// IOnDiskStateMachine. It returns the query result from the Lookup method or
// the error encountered.
func (nh *NodeHost) SyncRead(ctx context.Context, clusterID uint64,
	query interface{}) (interface{}, error) {
	v, err := nh.linearizableRead(ctx, clusterID,
		func(node *node) (interface{}, error) {
			data, err := node.sm.Lookup(query)
			if err == rsm.ErrClusterClosed {
				return nil, ErrClusterClosed
			}
			return data, err
		})
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Membership is the struct used to describe Raft cluster membership query
// results.
type Membership struct {
	// ConfigChangeID is the Raft entry index of the last applied membership
	// change entry.
	ConfigChangeID uint64
	// Nodes is a map of NodeID values to NodeHost Raft addresses for all regular
	// Raft nodes.
	Nodes map[uint64]string
	// Observers is a map of NodeID values to NodeHost Raft addresses for all
	// observers in the Raft cluster.
	Observers map[uint64]string
	// Witnesses is a map of NodeID values to NodeHost Raft addrsses for all
	// witnesses in the Raft cluster.
	Witnesses map[uint64]string
	// Removed is a set of NodeID values that have been removed from the Raft
	// cluster. They are not allowed to be added back to the cluster.
	Removed map[uint64]struct{}
}

// SyncGetClusterMembership is a rsynchronous method that queries the membership
// information from the specified Raft cluster. The specified context parameter
// must has the timeout value set.
//
// SyncGetClusterMembership guarantees that the returned membership information
// is linearizable.
func (nh *NodeHost) SyncGetClusterMembership(ctx context.Context,
	clusterID uint64) (*Membership, error) {
	v, err := nh.linearizableRead(ctx, clusterID,
		func(node *node) (interface{}, error) {
			m := node.sm.GetMembership()
			cm := func(input map[uint64]bool) map[uint64]struct{} {
				result := make(map[uint64]struct{})
				for k := range input {
					result[k] = struct{}{}
				}
				return result
			}
			return &Membership{
				Nodes:          m.Addresses,
				Observers:      m.Observers,
				Witnesses:      m.Witnesses,
				Removed:        cm(m.Removed),
				ConfigChangeID: m.ConfigChangeId,
			}, nil
		})
	if err != nil {
		return nil, err
	}
	return v.(*Membership), nil
}

// GetClusterMembership returns the membership information from the specified
// Raft cluster. The specified context parameter must has the timeout value
// set.
//
// GetClusterMembership guarantees that the returned membership information is
// linearizable. This is a synchronous method meaning it will only return after
// its confirmed completion, failure or timeout.
//
// Deprecated: Use NodeHost.SyncGetClusterMembership instead.
// NodeHost.GetClusterMembership will be removed in v4.0.
func (nh *NodeHost) GetClusterMembership(ctx context.Context,
	clusterID uint64) (*Membership, error) {
	return nh.SyncGetClusterMembership(ctx, clusterID)
}

// GetLeaderID returns the leader node ID of the specified Raft cluster based
// on local node's knowledge. The returned boolean value indicates whether the
// leader information is available.
func (nh *NodeHost) GetLeaderID(clusterID uint64) (uint64, bool, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return 0, false, ErrClosed
	}
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return 0, false, ErrClusterNotFound
	}
	leaderID, valid := v.getLeaderID()
	return leaderID, valid, nil
}

// GetNoOPSession returns a NO-OP client session ready to be used for making
// proposals. The NO-OP client session is a dummy client session that will not
// be checked or enforced. Use this No-OP client session when you want to ignore
// features provided by client sessions. A NO-OP client session is not
// registered on the server side and thus not required to be closed at the end
// of its life cycle.
//
// Returned NO-OP client session instance can be concurrently used in multiple
// goroutines.
//
// Use this NO-OP client session when your IStateMachine provides idempotence in
// its own implementation.
//
// NO-OP client session must be used for making proposals on IOnDiskStateMachine
// based state machine.
func (nh *NodeHost) GetNoOPSession(clusterID uint64) *client.Session {
	return client.NewNoOPSession(clusterID, nh.env.GetRandomSource())
}

// GetNewSession starts a synchronous proposal to create, register and return
// a new client session object for the specified Raft cluster. The specified
// context parameter must has the timeout value set.
//
// A client session object is used to ensure that a retried proposal, e.g.
// proposal retried after timeout, will not be applied more than once into the
// IStateMachine.
//
// Returned client session instance should not be used concurrently. Use
// multiple client sessions when making concurrent proposals.
//
// Deprecated: Use NodeHost.SyncGetSession instead. NodeHost.GetNewSession will
// be removed in v4.0.
func (nh *NodeHost) GetNewSession(ctx context.Context,
	clusterID uint64) (*client.Session, error) {
	return nh.SyncGetSession(ctx, clusterID)
}

// CloseSession closes the specified client session by unregistering it
// from the system. The specified context parameter must has the timeout value
// set. This is a synchronous method meaning it will only return after its
// confirmed completion, failure or timeout.
//
// Closed client session should no longer be used in future proposals.
//
// Deprecated: Use NodeHost.SyncCloseSession instead. NodeHost.CloseSession will
// be removed in v4.0.
func (nh *NodeHost) CloseSession(ctx context.Context,
	session *client.Session) error {
	return nh.SyncCloseSession(ctx, session)
}

// SyncGetSession starts a synchronous proposal to create, register and return
// a new client session object for the specified Raft cluster. The specified
// context parameter must has the timeout value set.
//
// A client session object is used to ensure that a retried proposal, e.g.
// proposal retried after timeout, will not be applied more than once into the
// state machine.
//
// Returned client session instance should not be used concurrently. Use
// multiple client sessions when you need to concurrently start multiple
// proposals.
//
// Client session is not supported by IOnDiskStateMachine based state machine.
// NO-OP client session must be used for making proposals on IOnDiskStateMachine
// based state machine.
func (nh *NodeHost) SyncGetSession(ctx context.Context,
	clusterID uint64) (*client.Session, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cs := client.NewSession(clusterID, nh.env.GetRandomSource())
	cs.PrepareForRegister()
	rs, err := nh.ProposeSession(cs, timeout)
	if err != nil {
		return nil, err
	}
	result, err := getRequestState(ctx, rs)
	if err != nil {
		return nil, err
	}
	if result.Value != cs.ClientID {
		plog.Panicf("unexpected result %d, want %d", result.Value, cs.ClientID)
	}
	cs.PrepareForPropose()
	return cs, nil
}

// SyncCloseSession closes the specified client session by unregistering it
// from the system. The specified context parameter must has the timeout value
// set. This is a synchronous method meaning it will only return after its
// confirmed completion, failure or timeout.
//
// Closed client session should no longer be used in future proposals.
func (nh *NodeHost) SyncCloseSession(ctx context.Context,
	cs *client.Session) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	cs.PrepareForUnregister()
	rs, err := nh.ProposeSession(cs, timeout)
	if err != nil {
		return err
	}
	result, err := getRequestState(ctx, rs)
	if err != nil {
		return err
	}
	if result.Value != cs.ClientID {
		plog.Panicf("unexpected result %d, want %d", result.Value, cs.ClientID)
	}
	return nil
}

// Propose starts an asynchronous proposal on the Raft cluster specified by the
// Session object. The input byte slice can be reused for other purposes
// immediate after the return of this method.
//
// This method returns a RequestState instance or an error immediately.
// Application can wait on the ResultC() channel of the returned RequestState
// instance to get notified for the outcome of the proposal and access to the
// result of the proposal.
//
// After the proposal is completed, i.e. RequestResult is received from the
// ResultC() channel of the returned RequestState, unless NO-OP client session
// is used, it is caller's responsibility to update the Session instance
// accordingly based on the RequestResult.Code value. Basically, when
// RequestTimeout is returned, you can retry the same proposal without updating
// your client session instance, when a RequestRejected value is returned, it
// usually means the session instance has been evicted from the server side,
// the Raft paper recommends you to crash your client in this highly unlikely
// event. When the proposal completed successfully with a RequestCompleted
// value, application must call client.ProposalCompleted() to get the client
// session ready to be used in future proposals.
func (nh *NodeHost) Propose(session *client.Session, cmd []byte,
	timeout time.Duration) (*RequestState, error) {
	return nh.propose(session, cmd, timeout)
}

// ProposeSession starts an asynchronous proposal on the specified cluster
// for client session related operations. Depending on the state of the specified
// client session object, the supported operations are for registering or
// unregistering a client session. Application can select on the ResultC()
// channel of the returned RequestState instance to get notified for the
// completion (RequestResult.Completed() is true) of the operation.
func (nh *NodeHost) ProposeSession(session *client.Session,
	timeout time.Duration) (*RequestState, error) {
	n, ok := nh.getCluster(session.ClusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !n.supportClientSession() && !session.IsNoOPSession() {
		plog.Panicf("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	defer nh.engine.setStepReady(session.ClusterID)
	return n.proposeSession(session, nh.getTimeoutTick(timeout))
}

// ReadIndex starts the asynchronous ReadIndex protocol used for linearizable
// read on the specified cluster. This method returns a RequestState instance
// or an error immediately. Application should wait on the ResultC() channel
// of the returned RequestState object to get notified on the outcome of the
// ReadIndex operation. On a successful completion, the ReadLocal method can
// then be invoked to query the state of the IStateMachine or
// IOnDiskStateMachine to complete the read operation with linearizability
// guarantee.
func (nh *NodeHost) ReadIndex(clusterID uint64,
	timeout time.Duration) (*RequestState, error) {
	rs, _, err := nh.readIndex(clusterID, timeout)
	return rs, err
}

// ReadLocalNode queries the Raft node identified by the input RequestState
// instance. To ensure the IO linearizability, ReadLocalNode should only be
// called after receiving a RequestCompleted notification from the ReadIndex
// method. See ReadIndex's example for more details.
func (nh *NodeHost) ReadLocalNode(rs *RequestState,
	query interface{}) (interface{}, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	rs.mustBeReadyForLocalRead()
	// translate the rsm.ErrClusterClosed to ErrClusterClosed
	// internally, the IManagedStateMachine might obtain a RLock before performing
	// the local read. The critical section is used to make sure we don't read
	// from a destroyed C++ StateMachine object
	data, err := rs.node.sm.Lookup(query)
	if err == rsm.ErrClusterClosed {
		return nil, ErrClusterClosed
	}
	return data, err
}

// NAReadLocalNode is a variant of ReadLocalNode, it uses byte slice as its
// input and output data for read only queries to minimize extra heap
// allocations caused by using interface{}. Users are recommended to use
// ReadLocalNode unless performance is the top priority.
//
// As an optional method, the underlying state machine must implement the
// statemachine.IExtended interface. NAReadLocalNode returns
// statemachine.ErrNotImplemented if the underlying state machine does not
// implement the statemachine.IExtended interface.
func (nh *NodeHost) NAReadLocalNode(rs *RequestState,
	query []byte) ([]byte, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	rs.mustBeReadyForLocalRead()
	data, err := rs.node.sm.NALookup(query)
	if err == rsm.ErrClusterClosed {
		return nil, ErrClusterClosed
	}
	return data, err
}

var staleReadCalled uint32

// StaleRead queries the specified Raft node directly without any
// linearizability guarantee.
//
// Users are recommended to use the SyncRead method or a combination of the
// ReadIndex and ReadLocalNode method to achieve linearizable read.
func (nh *NodeHost) StaleRead(clusterID uint64,
	query interface{}) (interface{}, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	if atomic.CompareAndSwapUint32(&staleReadCalled, 0, 1) {
		plog.Warningf("StaleRead called, linearizability not guaranteed for stale read")
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !n.initialized() {
		return nil, ErrClusterNotInitialized
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	data, err := n.sm.Lookup(query)
	if err == rsm.ErrClusterClosed {
		return nil, ErrClusterClosed
	}
	return data, err
}

// SyncRequestSnapshot is the synchronous variant of the RequestSnapshot
// method. See RequestSnapshot for more details.
//
// The input ctx must has deadline set.
//
// SyncRequestSnapshot returns the index of the created snapshot or the error
// encountered.
func (nh *NodeHost) SyncRequestSnapshot(ctx context.Context,
	clusterID uint64, opt SnapshotOption) (uint64, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	rs, err := nh.RequestSnapshot(clusterID, opt, timeout)
	if err != nil {
		return 0, err
	}
	v, err := getRequestState(ctx, rs)
	if err != nil {
		return 0, err
	}
	return v.Value, nil
}

// RequestSnapshot requests a snapshot to be created asynchronously for the
// specified cluster node. For each node, only one ongoing snapshot operation
// is allowed.
//
// Users can use an option parameter to specify details of the requested
// snapshot. For example, when the input SnapshotOption's Exported field is
// True, a snapshot will be exported to the directory pointed by the ExportPath
// field of the SnapshotOption instance. Such an exported snapshot is not
// managed by the system and it is mainly used to repair the cluster when it
// permanently loses its majority quorum. See the ImportSnapshot method in the
// tools package for more details.
//
// When the Exported field of the input SnapshotOption instance is set to false,
// snapshots created as the result of RequestSnapshot are managed by Dragonboat.
// Users are not suppose to move, copy, modify or delete the generated snapshot.
// Such requested snapshot will also trigger Raft log and snapshot compactions
// similar to automatic snapshotting. Users need to subsequently call
// RequestCompaction(), which can be far more I/O intensive, at suitable time to
// actually reclaim disk spaces used by Raft log entries and snapshot metadata
// records.
//
// When a snapshot is requested on a node backed by an IOnDiskStateMachine, only
// the metadata portion of the state machine will be captured and saved.
// Requesting snapshots on IOnDiskStateMachine based nodes are typically used to
// trigger Raft log and snapshot compactions.
//
// RequestSnapshot returns a RequestState instance or an error immediately.
// Applications can wait on the ResultC() channel of the returned RequestState
// instance to get notified for the outcome of the create snasphot operation.
// The RequestResult instance returned by the ResultC() channel tells the
// outcome of the snapshot operation, when successful, the SnapshotIndex method
// of the returned RequestResult instance reports the index of the created
// snapshot.
//
// Requested snapshot operation will be rejected if there is already an existing
// snapshot in the system at the same Raft log index.
func (nh *NodeHost) RequestSnapshot(clusterID uint64,
	opt SnapshotOption, timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	defer nh.engine.setStepReady(clusterID)
	return n.requestSnapshot(opt, nh.getTimeoutTick(timeout))
}

// RequestCompaction requests a compaction operation to be asynchronously
// executed in the background to reclaim disk spaces used by Raft Log entries
// that have already been marked as removed. This includes Raft Log entries
// that have already been included in created snapshots and Raft Log entries
// that belong to nodes already permanently removed via NodeHost.RemoveData().
//
// By default, compaction is automatically issued after each snapshot is
// captured. RequestCompaction can be used to manually trigger such compaction
// when auto compaction is disabled by the DisableAutoCompactions option in
// config.Config.
//
// The returned *SysOpState instance can be used to get notified when the
// requested compaction is completed. ErrRejected is returned when there is
// nothing to be reclaimed.
func (nh *NodeHost) RequestCompaction(clusterID uint64,
	nodeID uint64) (*SysOpState, error) {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		// assume this is a node that has already been removed via RemoveData
		done, err := nh.mu.logdb.CompactEntriesTo(clusterID, nodeID, math.MaxUint64)
		if err != nil {
			return nil, err
		}
		return &SysOpState{completedC: done}, nil
	}
	if n.nodeID != nodeID {
		return nil, ErrClusterNotFound
	}
	defer nh.engine.setStepReady(clusterID)
	return n.requestCompaction()
}

// SyncRequestDeleteNode is the synchronous variant of the RequestDeleteNode
// method. See RequestDeleteNode for more details.
//
// The input ctx must have its deadline set.
func (nh *NodeHost) SyncRequestDeleteNode(ctx context.Context,
	clusterID uint64, nodeID uint64, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestDeleteNode(clusterID, nodeID, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddNode is the synchronous variant of the RequestAddNode method.
// See RequestAddNode for more details.
//
// The input ctx must have its deadline set.
func (nh *NodeHost) SyncRequestAddNode(ctx context.Context,
	clusterID uint64, nodeID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddNode(clusterID,
		nodeID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddObserver is the synchronous variant of the RequestAddObserver
// method. See RequestAddObserver for more details.
//
// The input ctx must have its deadline set.
func (nh *NodeHost) SyncRequestAddObserver(ctx context.Context,
	clusterID uint64, nodeID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddObserver(clusterID,
		nodeID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddWitness is the synchronous variant of the RequestAddWitness
// method. See RequestAddWitness for more details.
//
// The input ctx must have its deadline set.
func (nh *NodeHost) SyncRequestAddWitness(ctx context.Context,
	clusterID uint64, nodeID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddWitness(clusterID,
		nodeID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// RequestDeleteNode is a Raft cluster membership change method for requesting
// the specified node to be removed from the specified Raft cluster. It starts
// an asynchronous request to remove the node from the Raft cluster membership
// list. Application can wait on the ResultC() channel of the returned
// RequestState instance to get notified for the outcome.
//
// It is not guaranteed that deleted node will automatically close itself and
// be removed from its managing NodeHost instance. It is application's
// responsibility to call RemoveCluster on the right NodeHost instance to
// actually have the cluster node removed from its managing NodeHost instance.
//
// Once a node is successfully deleted from a Raft cluster, it will not be
// allowed to be added back to the cluster with the same node identity.
//
// When the raft cluster is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetClusterMembership method. The requested delete node operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetClusterMembership method.
func (nh *NodeHost) RequestDeleteNode(clusterID uint64,
	nodeID uint64,
	configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	tt := nh.getTimeoutTick(timeout)
	defer nh.engine.setStepReady(clusterID)
	return n.requestDeleteNodeWithOrderID(nodeID, configChangeIndex, tt)
}

// RequestAddNode is a Raft cluster membership change method for requesting the
// specified node to be added to the specified Raft cluster. It starts an
// asynchronous request to add the node to the Raft cluster membership list.
// Application can wait on the ResultC() channel of the returned RequestState
// instance to get notified for the outcome.
//
// If there is already an observer with the same nodeID in the cluster, it will
// be promoted to a regular node with voting power. The target parameter of the
// RequestAddNode call is ignored when promoting an observer to a regular node.
//
// After the node is successfully added to the Raft cluster, it is application's
// responsibility to call StartCluster on the target NodeHost instance to
// actually start the Raft cluster node.
//
// Requesting a removed node back to the Raft cluster will always be rejected.
//
// By default, the target parameter is the RaftAddress of the NodeHost instance
// where the new Raft node will be running. Note that fixed IP or static DNS
// name should be used in RaftAddress in such default mode. When running in the
// AddressByNodeHostID mode, target should be set to NodeHost's ID value which
// can be obtained by calling the ID() method.
//
// When the Raft cluster is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetClusterMembership method. The requested add node operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetClusterMembership method.
func (nh *NodeHost) RequestAddNode(clusterID uint64,
	nodeID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	defer nh.engine.setStepReady(clusterID)
	return n.requestAddNodeWithOrderID(nodeID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestAddObserver is a Raft cluster membership change method for requesting
// the specified node to be added to the specified Raft cluster as an observer
// without voting power. It starts an asynchronous request to add the specified
// node as an observer.
//
// Such observer is able to receive replicated states from the leader node, but
// it is neither allowed to vote for leader, nor considered as a part of the
// quorum when replicating state. An observer can be promoted to a regular node
// with voting power by making a RequestAddNode call using its clusterID and
// nodeID values. An observer can be removed from the cluster by calling
// RequestDeleteNode with its clusterID and nodeID values.
//
// Application should later call StartCluster with config.Config.IsObserver
// set to true on the right NodeHost to actually start the observer instance.
//
// By default, the target parameter is the RaftAddress of the NodeHost instance
// where the new Raft node will be running. Note that fixed IP or static DNS
// name should be used in RaftAddress in such default mode. When running in the
// AddressByNodeHostID mode, target should be set to NodeHost's ID value which
// can be obtained by calling the ID() method.
//
// When the Raft cluster is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetClusterMembership method. The requested add observer operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetClusterMembership method.
func (nh *NodeHost) RequestAddObserver(clusterID uint64,
	nodeID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	defer nh.engine.setStepReady(clusterID)
	return n.requestAddObserverWithOrderID(nodeID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestAddWitness is a Raft cluster membership change method for requesting
// the specified node to be added as a witness to the given Raft cluster. It
// starts an asynchronous request to add the specified node as an witness.
//
// A witness can vote in elections but it doesn't have any Raft log or
// application state machine associated. The witness node can not be used
// to initiate read, write or membership change operations on its Raft cluster.
// Section 11.7.2 of Diego Ongaro's thesis contains more info on such witness
// role.
//
// Application should later call StartCluster with config.Config.IsWitness
// set to true on the right NodeHost to actually start the witness node.
//
// By default, the target parameter is the RaftAddress of the NodeHost instance
// where the new Raft node will be running. Note that fixed IP or static DNS
// name should be used in RaftAddress in such default mode. When running in the
// AddressByNodeHostID mode, target should be set to NodeHost's ID value which
// can be obtained by calling the ID() method.
//
// When the Raft cluster is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetClusterMembership method. The requested add witness operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetClusterMembership method.
func (nh *NodeHost) RequestAddWitness(clusterID uint64,
	nodeID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	defer nh.engine.setStepReady(clusterID)
	return n.requestAddWitnessWithOrderID(nodeID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestLeaderTransfer makes a request to transfer the leadership of the
// specified Raft cluster to the target node identified by targetNodeID. It
// returns an error if the request fails to be started. There is no guarantee
// that such request can be fulfilled, i.e. the leadership transfer can still
// fail after a successful return of the RequestLeaderTransfer method.
func (nh *NodeHost) RequestLeaderTransfer(clusterID uint64,
	targetNodeID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	plog.Debugf("RequestLeaderTransfer called on cluster %d target nodeid %d",
		clusterID, targetNodeID)
	defer nh.engine.setStepReady(clusterID)
	return n.requestLeaderTransfer(targetNodeID)
}

// SyncRemoveData is the synchronous variant of the RemoveData. It waits for
// the specified node to be fully offloaded or until the ctx instance is
// cancelled or timeout.
//
// Similar to RemoveData, calling SyncRemoveData on a node that is still a Raft
// cluster member will corrupt the Raft cluster.
func (nh *NodeHost) SyncRemoveData(ctx context.Context,
	clusterID uint64, nodeID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	if _, ok := nh.getCluster(clusterID); ok {
		return ErrClusterNotStopped
	}
	if ch := nh.engine.destroyedC(clusterID, nodeID); ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return ErrCanceled
			} else if ctx.Err() == context.DeadlineExceeded {
				return ErrTimeout
			}
		}
	}
	err := nh.RemoveData(clusterID, nodeID)
	if errors.Is(err, ErrClusterNotStopped) {
		panic("node not stopped")
	}
	return err
}

// RemoveData tries to remove all data associated with the specified node. This
// method should only be used after the node has been deleted from its Raft
// cluster. Calling RemoveData on a node that is still a Raft cluster member
// will corrupt the Raft cluster.
//
// RemoveData returns ErrClusterNotStopped when the specified node has not been
// fully offloaded from the NodeHost instance.
func (nh *NodeHost) RemoveData(clusterID uint64, nodeID uint64) error {
	n, ok := nh.getCluster(clusterID)
	if ok && n.nodeID == nodeID {
		return ErrClusterNotStopped
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	if nh.engine.nodeLoaded(clusterID, nodeID) {
		return ErrClusterNotStopped
	}
	plog.Debugf("%s called RemoveData", dn(clusterID, nodeID))
	if err := nh.mu.logdb.RemoveNodeData(clusterID, nodeID); err != nil {
		panicNow(err)
	}
	// mark the snapshot dir as removed
	did := nh.nhConfig.GetDeploymentID()
	if err := nh.env.RemoveSnapshotDir(did, clusterID, nodeID); err != nil {
		panicNow(err)
	}
	return nil
}

// GetNodeUser returns an INodeUser instance ready to be used to directly make
// proposals or read index operations without locating the node repeatedly in
// the NodeHost. A possible use case is when loading a large data set say with
// billions of proposals into the dragonboat based system.
func (nh *NodeHost) GetNodeUser(clusterID uint64) (INodeUser, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	return &nodeUser{
		nh:           nh,
		node:         n,
		setStepReady: nh.engine.setStepReady,
	}, nil
}

// HasNodeInfo returns a boolean value indicating whether the specified node
// has been bootstrapped on the current NodeHost instance.
func (nh *NodeHost) HasNodeInfo(clusterID uint64, nodeID uint64) bool {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return false
	}
	if _, err := nh.mu.logdb.GetBootstrapInfo(clusterID, nodeID); err != nil {
		if errors.Is(err, raftio.ErrNoBootstrapInfo) {
			return false
		}
		panicNow(err)
	}
	return true
}

// GetNodeHostInfo returns a NodeHostInfo instance that contains all details
// of the NodeHost, this includes details of all Raft clusters managed by the
// the NodeHost instance.
func (nh *NodeHost) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	nhi := &NodeHostInfo{
		NodeHostID:      nh.ID(),
		RaftAddress:     nh.RaftAddress(),
		Gossip:          nh.getGossipInfo(),
		ClusterInfoList: nh.getClusterInfo(),
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil
	}
	if !opt.SkipLogInfo {
		logInfo, err := nh.mu.logdb.ListNodeInfo()
		if err != nil {
			panicNow(err)
		}
		nhi.LogInfo = logInfo
	}
	return nhi
}

func (nh *NodeHost) getGossipInfo() GossipInfo {
	if r, ok := nh.nodes.(*transport.NodeHostIDRegistry); ok {
		return GossipInfo{
			Enabled:             true,
			AdvertiseAddress:    r.AdvertiseAddress(),
			NumOfKnownNodeHosts: r.NumMembers(),
		}
	}
	return GossipInfo{}
}

func (nh *NodeHost) propose(s *client.Session,
	cmd []byte, timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	v, ok := nh.getCluster(s.ClusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !v.supportClientSession() && !s.IsNoOPSession() {
		panic("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	req, err := v.propose(s, cmd, nh.getTimeoutTick(timeout))
	nh.engine.setStepReady(s.ClusterID)
	return req, err
}

func (nh *NodeHost) readIndex(clusterID uint64,
	timeout time.Duration) (*RequestState, *node, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, nil, ErrClosed
	}
	n, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, nil, ErrClusterNotFound
	}
	req, err := n.read(nh.getTimeoutTick(timeout))
	if err != nil {
		return nil, nil, err
	}
	nh.engine.setStepReady(clusterID)
	return req, n, err
}

func (nh *NodeHost) linearizableRead(ctx context.Context,
	clusterID uint64, f func(n *node) (interface{}, error)) (interface{}, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, node, err := nh.readIndex(clusterID, timeout)
	if err != nil {
		return nil, err
	}
	if _, err := getRequestState(ctx, rs); err != nil {
		return nil, err
	}
	rs.Release()
	return f(node)
}

func (nh *NodeHost) getCluster(clusterID uint64) (*node, bool) {
	n, ok := nh.mu.clusters.Load(clusterID)
	if !ok {
		return nil, false
	}
	return n.(*node), true
}

func (nh *NodeHost) forEachCluster(f func(uint64, *node) bool) uint64 {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	nh.mu.clusters.Range(func(k, v interface{}) bool {
		return f(k.(uint64), v.(*node))
	})
	return nh.mu.cci
}

func (nh *NodeHost) getClusterSetIndex() uint64 {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	return nh.mu.cci
}

// there are three major reasons to bootstrap the cluster
//
//  1. when possible, we check whether user incorrectly specified parameters
//     for the startCluster method, e.g. call startCluster with join=true first,
//     then restart the NodeHost instance and call startCluster again with
//     join=false and len(nodes) > 0
//  2. when restarting a node which is a part of the initial cluster members,
//     for user convenience, we allow the caller not to provide the details of
//     initial members. when the initial cluster member info is required, however
//     we still need to get the initial member info from somewhere. bootstrap is
//     the procedure that records such info.
//  3. the bootstrap record is used as a marker record in our default LogDB
//     implementation to indicate that a certain node exists here
func (nh *NodeHost) bootstrapCluster(initialMembers map[uint64]Target,
	join bool, cfg config.Config,
	smType pb.StateMachineType) (map[uint64]string, bool, error) {
	bi, err := nh.mu.logdb.GetBootstrapInfo(cfg.ClusterID, cfg.NodeID)
	if err == raftio.ErrNoBootstrapInfo {
		if !join && len(initialMembers) == 0 {
			return nil, false, ErrClusterNotBootstrapped
		}
		var members map[uint64]string
		if !join {
			members = initialMembers
		}
		bi = pb.NewBootstrapInfo(join, smType, initialMembers)
		err := nh.mu.logdb.SaveBootstrapInfo(cfg.ClusterID, cfg.NodeID, bi)
		if err != nil {
			return nil, false, err
		}
		return members, !join, nil
	} else if err != nil {
		return nil, false, err
	}
	if !bi.Validate(initialMembers, join, smType) {
		plog.Errorf("bootstrap info validation failed, %s, %v, %t, %v, %t",
			dn(cfg.ClusterID, cfg.NodeID),
			bi.Addresses, bi.Join, initialMembers, join)
		return nil, false, ErrInvalidClusterSettings
	}
	return bi.Addresses, !bi.Join, nil
}

func (nh *NodeHost) startCluster(initialMembers map[uint64]Target,
	join bool, createStateMachine rsm.ManagedStateMachineFactory,
	cfg config.Config, smType pb.StateMachineType) error {
	clusterID := cfg.ClusterID
	nodeID := cfg.NodeID
	validator := nh.nhConfig.GetTargetValidator()
	for _, target := range initialMembers {
		if !validator(target) {
			return ErrInvalidTarget
		}
	}
	if cfg.SnapshotCompressionType == config.Snappy &&
		invariants.Is32BitArch() {
		// see https://github.com/golang/snappy/issues/58
		plog.Warningf("Golang SNAPPY is known to be buggy on 32bit arch")
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	if _, ok := nh.mu.clusters.Load(clusterID); ok {
		return ErrClusterAlreadyExist
	}
	if nh.engine.nodeLoaded(clusterID, nodeID) {
		// node is still loaded in the execution engine, e.g. processing snapshot
		return ErrClusterAlreadyExist
	}
	if join && len(initialMembers) > 0 {
		return ErrInvalidClusterSettings
	}
	peers, im, err := nh.bootstrapCluster(initialMembers, join, cfg, smType)
	if err == ErrInvalidClusterSettings {
		return err
	}
	if err != nil {
		panic(err)
	}
	for k, v := range peers {
		if k != nodeID {
			nh.nodes.Add(clusterID, k, v)
		}
	}
	did := nh.nhConfig.GetDeploymentID()
	if err := nh.env.CreateSnapshotDir(did, clusterID, nodeID); err != nil {
		if errors.Is(err, server.ErrDirMarkedAsDeleted) {
			return ErrNodeRemoved
		}
		panicNow(err)
	}
	getSnapshotDir := func(cid uint64, nid uint64) string {
		return nh.env.GetSnapshotDir(did, cid, nid)
	}
	ss := newSnapshotter(clusterID, nodeID, getSnapshotDir, nh.mu.logdb, nh.fs)
	if err := ss.processOrphans(); err != nil {
		panicNow(err)
	}
	p := server.NewDoubleFixedPartitioner(nh.nhConfig.Expert.Engine.ExecShards,
		nh.nhConfig.Expert.LogDB.Shards)
	shard := p.GetPartitionID(clusterID)
	rn, err := newNode(peers,
		im,
		cfg,
		nh.nhConfig,
		createStateMachine,
		ss,
		nh.engine,
		nh.events.leaderInfoQ,
		nh.transport.GetStreamSink,
		nh.msgHandler.HandleSnapshotStatus,
		nh.sendMessage,
		nh.nodes,
		nh.requestPools[nodeID%requestPoolShards],
		nh.mu.logdb,
		nh.getLogDBMetrics(shard),
		nh.events.sys)
	if err != nil {
		panicNow(err)
	}
	rn.loaded()
	nh.mu.clusters.Store(clusterID, rn)
	nh.mu.cci++
	nh.cciUpdated()
	nh.engine.setCCIReady(clusterID)
	nh.engine.setApplyReady(clusterID)
	return nil
}

func (nh *NodeHost) cciUpdated() {
	select {
	case nh.mu.cciCh <- struct{}{}:
	default:
	}
}

func (nh *NodeHost) loadNodeHostID() error {
	if nh.nhConfig.Expert.TestNodeHostID == 0 {
		nhid, err := nh.env.LoadNodeHostID()
		if err != nil {
			return err
		}
		nh.id = nhid
	} else {
		nhid, err := id.NewNodeHostID(nh.nhConfig.Expert.TestNodeHostID)
		if err != nil {
			return err
		}
		nh.id = nhid
		nh.env.SetNodeHostID(nh.id)
	}
	return nil
}

func (nh *NodeHost) createPools() {
	nh.requestPools = make([]*sync.Pool, requestPoolShards)
	for i := uint64(0); i < requestPoolShards; i++ {
		p := &sync.Pool{}
		p.New = func() interface{} {
			obj := &RequestState{}
			obj.CompletedC = make(chan RequestResult, 1)
			obj.pool = p
			if nh.nhConfig.NotifyCommit {
				obj.committedC = make(chan RequestResult, 1)
			}
			return obj
		}
		nh.requestPools[i] = p
	}
}

func (nh *NodeHost) createLogDB() error {
	did := nh.nhConfig.GetDeploymentID()
	nhDir, walDir, err := nh.env.CreateNodeHostDir(did)
	if err != nil {
		return err
	}
	if err := nh.env.LockNodeHostDir(); err != nil {
		return err
	}
	var lf config.LogDBFactory
	if nh.nhConfig.Expert.LogDBFactory != nil {
		lf = nh.nhConfig.Expert.LogDBFactory
	} else {
		lf = logdb.NewDefaultFactory(nh.fs)
	}
	name := lf.Name()
	if err := nh.env.CheckLogDBType(nh.nhConfig, name); err != nil {
		return err
	}
	ldb, err := lf.Create(nh.nhConfig,
		nh.handleLogDBInfo, []string{nhDir}, []string{walDir})
	if err != nil {
		return err
	}
	nh.mu.logdb = ldb
	ver := ldb.BinaryFormat()
	if err := nh.env.CheckNodeHostDir(nh.nhConfig, ver, name); err != nil {
		return err
	}
	if shardedrdb, ok := ldb.(*logdb.ShardedDB); ok {
		failed, err := shardedrdb.SelfCheckFailed()
		if err != nil {
			return err
		}
		if failed {
			return server.ErrLogDBBrokenChange
		}
	}
	plog.Infof("logdb memory limit: %d MBytes",
		nh.nhConfig.Expert.LogDB.MemorySizeMB())
	return nil
}

func (nh *NodeHost) handleLogDBInfo(info config.LogDBInfo) {
	plog.Infof("LogDB info received, shard %d, busy %t", info.Shard, info.Busy)
	nh.mu.Lock()
	defer nh.mu.Unlock()
	lm := nh.getLogDBMetrics(info.Shard)
	lm.update(info.Busy)
}

func (nh *NodeHost) getLogDBMetrics(shard uint64) *logDBMetrics {
	if v, ok := nh.mu.lm.Load(shard); ok {
		return v.(*logDBMetrics)
	}
	lm := &logDBMetrics{}
	nh.mu.lm.Store(shard, lm)
	return lm
}

type transportEvent struct {
	nh *NodeHost
}

func (te *transportEvent) ConnectionEstablished(addr string, snapshot bool) {
	te.nh.events.sys.Publish(server.SystemEvent{
		Type:               server.ConnectionEstablished,
		Address:            addr,
		SnapshotConnection: snapshot,
	})
}

func (te *transportEvent) ConnectionFailed(addr string, snapshot bool) {
	te.nh.events.sys.Publish(server.SystemEvent{
		Type:               server.ConnectionFailed,
		Address:            addr,
		SnapshotConnection: snapshot,
	})
}

func (nh *NodeHost) createNodeRegistry() error {
	validator := nh.nhConfig.GetTargetValidator()
	// TODO:
	// more tests here required
	if nh.nhConfig.AddressByNodeHostID {
		plog.Infof("AddressByNodeHostID: true, use gossip based node registry")
		r, err := transport.NewNodeHostIDRegistry(nh.ID(),
			nh.nhConfig, streamConnections, validator)
		if err != nil {
			return err
		}
		nh.nodes = r
	} else {
		plog.Infof("using regular node registry")
		nh.nodes = transport.NewNodeRegistry(streamConnections, validator)
	}
	return nil
}

func (nh *NodeHost) createTransport() error {
	getSnapshotDir := func(cid uint64, nid uint64) string {
		return nh.env.GetSnapshotDir(nh.nhConfig.GetDeploymentID(), cid, nid)
	}
	tsp, err := transport.NewTransport(nh.nhConfig,
		nh.msgHandler, nh.env, nh.nodes, getSnapshotDir,
		&transportEvent{nh: nh}, nh.fs)
	if err != nil {
		return err
	}
	nh.transport = tsp
	return nil
}

func (nh *NodeHost) stopNode(clusterID uint64, nodeID uint64, check bool) error {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	v, ok := nh.mu.clusters.Load(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	n := v.(*node)
	if check && n.nodeID != nodeID {
		return ErrClusterNotFound
	}
	nh.mu.clusters.Delete(clusterID)
	nh.mu.cci++
	nh.cciUpdated()
	nh.engine.setCCIReady(clusterID)
	n.close()
	n.offloaded()
	nh.engine.setStepReady(clusterID)
	nh.engine.setCommitReady(clusterID)
	nh.engine.setApplyReady(clusterID)
	nh.engine.setRecoverReady(clusterID)
	return nil
}

func (nh *NodeHost) getClusterInfo() []ClusterInfo {
	clusterInfoList := make([]ClusterInfo, 0)
	nodes := make([]*node, 0)
	nh.forEachCluster(func(cid uint64, node *node) bool {
		nodes = append(nodes, node)
		return true
	})
	for _, n := range nodes {
		clusterInfo := n.getClusterInfo()
		clusterInfoList = append(clusterInfoList, *clusterInfo)
	}
	return clusterInfoList
}

func (nh *NodeHost) tickWorkerMain() {
	tick := uint64(0)
	idx := uint64(0)
	nodes := make([]*node, 0)
	tf := func() {
		tick++
		if idx != nh.getClusterSetIndex() {
			nodes = nodes[:0]
			idx = nh.forEachCluster(func(cid uint64, n *node) bool {
				nodes = append(nodes, n)
				return true
			})
		}
		nh.sendTickMessage(nodes, tick)
		nh.engine.setAllStepReady(nodes)
	}
	td := time.Duration(nh.nhConfig.RTTMillisecond) * time.Millisecond
	ticker := time.NewTicker(td)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tf()
		case <-nh.stopper.ShouldStop():
			return
		}
	}
}

func (nh *NodeHost) handleListenerEvents() {
	var ch chan struct{}
	if nh.events.leaderInfoQ != nil {
		ch = nh.events.leaderInfoQ.workReady()
	}
	for {
		select {
		case <-nh.stopper.ShouldStop():
			return
		case <-ch:
			for {
				v, ok := nh.events.leaderInfoQ.getLeaderInfo()
				if !ok {
					break
				}
				nh.events.raft.LeaderUpdated(v)
			}
		case e := <-nh.events.sys.events:
			nh.events.sys.handle(e)
		}
	}
}

func (nh *NodeHost) sendMessage(msg pb.Message) {
	if nh.isPartitioned() {
		return
	}
	if msg.Type != pb.InstallSnapshot {
		nh.transport.Send(msg)
	} else {
		witness := msg.Snapshot.Witness
		plog.Debugf("%s is sending snapshot to %s, witness %t, index %d, size %d",
			dn(msg.ClusterId, msg.From), dn(msg.ClusterId, msg.To),
			witness, msg.Snapshot.Index, msg.Snapshot.FileSize)
		if n, ok := nh.getCluster(msg.ClusterId); ok {
			if witness || !n.OnDiskStateMachine() {
				nh.transport.SendSnapshot(msg)
			} else {
				n.pushStreamSnapshotRequest(msg.ClusterId, msg.To)
			}
		}
		nh.events.sys.Publish(server.SystemEvent{
			Type:      server.SendSnapshotStarted,
			ClusterID: msg.ClusterId,
			NodeID:    msg.To,
			From:      msg.From,
		})
	}
}

func (nh *NodeHost) sendTickMessage(clusters []*node, tick uint64) {
	for _, n := range clusters {
		m := pb.Message{
			Type: pb.LocalTick,
			To:   n.nodeID,
			From: n.nodeID,
			Hint: tick,
		}
		n.mq.Add(m)
	}
}

func (nh *NodeHost) nodeMonitorMain() {
	for {
		nodes := make([]*node, 0)
		nh.forEachCluster(func(cid uint64, node *node) bool {
			nodes = append(nodes, node)
			return true
		})
		cases := make([]reflect.SelectCase, len(nodes)+2)
		for i, n := range nodes {
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(n.ShouldStop()),
			}
		}
		cases[len(nodes)] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(nh.mu.cciCh),
		}
		cases[len(nodes)+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(nh.stopper.ShouldStop()),
		}
		index, _, ok := reflect.Select(cases)
		if !ok && index < len(nodes) {
			// node closed
			n := nodes[index]
			_ = nh.stopNode(n.clusterID, n.nodeID, true)
		} else if index == len(nodes) {
			// cci change
			continue
		} else if index == len(nodes)+1 {
			// stopped
			return
		} else {
			plog.Panicf("unknown node list change state, %d, %t", index, ok)
		}
	}
}

func (nh *NodeHost) getTimeoutTick(timeout time.Duration) uint64 {
	return uint64(timeout.Milliseconds()) / nh.nhConfig.RTTMillisecond
}

func (nh *NodeHost) describe() string {
	return nh.RaftAddress()
}

func (nh *NodeHost) logNodeHostDetails() {
	plog.Infof("transport type: %s", nh.transport.Name())
	plog.Infof("logdb type: %s", nh.mu.logdb.Name())
	plog.Infof("nodehost address: %s", nh.nhConfig.RaftAddress)
}

func getRequestState(ctx context.Context, rs *RequestState) (sm.Result, error) {
	select {
	case r := <-rs.AppliedC():
		if r.Completed() {
			return r.GetResult(), nil
		} else if r.Rejected() {
			return sm.Result{}, ErrRejected
		} else if r.Timeout() {
			return sm.Result{}, ErrTimeout
		} else if r.Terminated() {
			return sm.Result{}, ErrClusterClosed
		} else if r.Dropped() {
			return sm.Result{}, ErrClusterNotReady
		} else if r.Aborted() {
			return sm.Result{}, ErrAborted
		}
		plog.Panicf("unknown v code %v", r)
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return sm.Result{}, ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return sm.Result{}, ErrTimeout
		}
	}
	panic("should never reach here")
}

// INodeUser is the interface implemented by a Raft node user type. A Raft node
// user can be used to directly initiate proposals or read index operations
// without locating the Raft node in NodeHost's node list first. It is useful
// when doing bulk load operations on selected clusters.
type INodeUser interface {
	// ClusterID is the cluster ID of the node.
	ClusterID() uint64
	// NodeID is the node ID of the node.
	NodeID() uint64
	// Propose starts an asynchronous proposal on the Raft cluster represented by
	// the INodeUser instance. Its semantics is the same as the Propose() method
	// in NodeHost.
	Propose(s *client.Session,
		cmd []byte, timeout time.Duration) (*RequestState, error)
	// ReadIndex starts the asynchronous ReadIndex protocol used for linearizable
	// reads on the Raft cluster represented by the INodeUser instance. Its
	// semantics is the same as the ReadIndex() method in NodeHost.
	ReadIndex(timeout time.Duration) (*RequestState, error)
}

type nodeUser struct {
	nh           *NodeHost
	node         *node
	setStepReady func(clusterID uint64)
}

var _ INodeUser = (*nodeUser)(nil)

func (nu *nodeUser) ClusterID() uint64 {
	return nu.node.clusterID
}

func (nu *nodeUser) NodeID() uint64 {
	return nu.node.nodeID
}

func (nu *nodeUser) Propose(s *client.Session,
	cmd []byte, timeout time.Duration) (*RequestState, error) {
	req, err := nu.node.propose(s, cmd, nu.nh.getTimeoutTick(timeout))
	nu.setStepReady(s.ClusterID)
	return req, err
}

func (nu *nodeUser) ReadIndex(timeout time.Duration) (*RequestState, error) {
	return nu.node.read(nu.nh.getTimeoutTick(timeout))
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, ErrDeadlineNotSet
	}
	now := time.Now()
	if now.After(d) {
		return 0, ErrInvalidDeadline
	}
	return d.Sub(now), nil
}

var (
	streamPushDelayTick      uint64 = 10
	streamConfirmedDelayTick uint64 = 2
)

type messageHandler struct {
	nh *NodeHost
}

var _ transport.IMessageHandler = (*messageHandler)(nil)

func newNodeHostMessageHandler(nh *NodeHost) *messageHandler {
	return &messageHandler{nh: nh}
}

func (h *messageHandler) HandleMessageBatch(msg pb.MessageBatch) (uint64, uint64) {
	nh := h.nh
	snapshotCount := uint64(0)
	msgCount := uint64(0)
	if nh.isPartitioned() {
		keep := false
		// InstallSnapshot is a in-memory local message type that will never be
		// dropped in production as it will never be sent via networks
		for _, req := range msg.Requests {
			if req.Type == pb.InstallSnapshot {
				keep = true
			}
		}
		if !keep {
			return 0, 0
		}
	}
	for _, req := range msg.Requests {
		if req.To == 0 {
			plog.Panicf("to field not set, %s", req.Type)
		}
		if n, ok := nh.getCluster(req.ClusterId); ok {
			if n.nodeID != req.To {
				plog.Warningf("ignored a %s message sent to %s but received by %s",
					req.Type, dn(req.ClusterId, req.To), dn(req.ClusterId, n.nodeID))
				continue
			}
			if req.Type == pb.InstallSnapshot {
				n.mq.MustAdd(req)
				snapshotCount++
			} else if req.Type == pb.SnapshotReceived {
				plog.Debugf("SnapshotReceived received, cluster id %d, node id %d",
					req.ClusterId, req.From)
				n.mq.AddDelayed(pb.Message{
					Type: pb.SnapshotStatus,
					From: req.From,
				}, streamConfirmedDelayTick)
				msgCount++
			} else {
				if added, stopped := n.mq.Add(req); !added || stopped {
					plog.Warningf("dropped an incoming message")
				} else {
					msgCount++
				}
			}
		}
	}
	nh.engine.setStepReadyByMessageBatch(msg)
	return snapshotCount, msgCount
}

func (h *messageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	eventType := server.SendSnapshotCompleted
	if failed {
		eventType = server.SendSnapshotAborted
	}
	h.nh.events.sys.Publish(server.SystemEvent{
		Type:      eventType,
		ClusterID: clusterID,
		NodeID:    nodeID,
	})
	if n, ok := h.nh.getCluster(clusterID); ok {
		n.mq.AddDelayed(pb.Message{
			Type:   pb.SnapshotStatus,
			From:   nodeID,
			Reject: failed,
		}, streamPushDelayTick)
		h.nh.engine.setStepReady(clusterID)
	}
}

func (h *messageHandler) HandleUnreachable(clusterID uint64, nodeID uint64) {
	if n, ok := h.nh.getCluster(clusterID); ok {
		m := pb.Message{
			Type: pb.Unreachable,
			From: nodeID,
			To:   n.nodeID,
		}
		n.mq.MustAdd(m)
		h.nh.engine.setStepReady(clusterID)
	}
}

func (h *messageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
	m := pb.Message{
		To:        from,
		From:      nodeID,
		ClusterId: clusterID,
		Type:      pb.SnapshotReceived,
	}
	h.nh.sendMessage(m)
	plog.Debugf("%s sent SnapshotReceived to %d", dn(clusterID, nodeID), from)
	h.nh.events.sys.Publish(server.SystemEvent{
		Type:      server.SnapshotReceived,
		ClusterID: clusterID,
		NodeID:    nodeID,
		From:      from,
	})
}

func logBuildTagsAndVersion() {
	devstr := "Rel"
	if DEVVersion {
		devstr = "Dev"
	}
	plog.Infof("go version: %s, %s/%s",
		runtime.Version(), runtime.GOOS, runtime.GOARCH)
	plog.Infof("dragonboat version: %d.%d.%d (%s)",
		DragonboatMajor, DragonboatMinor, DragonboatPatch, devstr)
	if !invariants.IsSupportedOS() || !invariants.IsSupportedArch() {
		plog.Warningf("unsupported OS/ARCH %s/%s, don't use for production",
			runtime.GOOS, runtime.GOARCH)
	}
}

func panicNow(err error) {
	if err == nil {
		panic("panicNow called with nil error")
	}
	plog.Panicf("%+v", err)
	panic(err)
}
