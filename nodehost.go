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
Package dragonboat is a feature complete and highly optimized multi-group Raft
implementation for providing consensus in distributed systems.

The NodeHost struct is the facade interface for all features provided by the
dragonboat package. Each NodeHost instance usually runs on a separate server
managing CPU, storage and network resources used for achieving consensus. Each
NodeHost manages Raft nodes from different Raft groups known as Raft shards.
Each Raft shard is identified by its ShardID, it usually consists of
multiple nodes (also known as replicas) each identified by a ReplicaID value.
Nodes from the same Raft shard suppose to be distributed on different NodeHost
instances across the network, this brings fault tolerance for machine and
network failures as application data stored in the Raft shard will be
available as long as the majority of its managing NodeHost instances (i.e. its
underlying servers) are accessible.

Arbitrary number of Raft shards can be launched across the network to
aggregate distributed processing and storage capacities. Users can also make
membership change requests to add or remove nodes from selected Raft shard.

User applications can leverage the power of the Raft protocol by implementing
the IStateMachine or IOnDiskStateMachine component, as defined in
github.com/lni/dragonboat/v4/statemachine. Known as user state machines, each
IStateMachine or IOnDiskStateMachine instance is in charge of updating, querying
and snapshotting application data with minimum exposure to the Raft protocol
itself.

Dragonboat guarantees the linearizability of your I/O when interacting with the
IStateMachine or IOnDiskStateMachine instances. In plain English, writes (via
making proposals) to your Raft shard appears to be instantaneous, once a write
is completed, all later reads (via linearizable read based on Raft's ReadIndex
protocol) should return the value of that write or a later write. Once a value
is returned by a linearizable read, all later reads should return the same value
or the result of a later write.

To strictly provide such guarantee, we need to implement the at-most-once
semantic. For a client, when it retries the proposal that failed to complete by
its deadline, it faces the risk of having the same proposal committed and
applied twice into the user state machine. Dragonboat prevents this by
implementing the client session concept described in Diego Ongaro's PhD thesis.
*/
package dragonboat // github.com/lni/dragonboat/v4

import (
	"context"
	"math"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/id"
	"github.com/lni/dragonboat/v4/internal/invariants"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/registry"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/internal/utils"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	// DragonboatMajor is the major version number
	DragonboatMajor = 4
	// DragonboatMinor is the minor version number
	DragonboatMinor = 0
	// DragonboatPatch is the patch version number
	DragonboatPatch = 0
	// DEVVersion is a boolean flag indicating whether this is a dev version
	DEVVersion = true
)

var (
	receiveQueueLen   = settings.Soft.ReceiveQueueLength
	requestPoolShards = settings.Soft.NodeHostRequestStatePoolShards
	streamConnections = settings.Soft.StreamConnections
)

var (
	// ErrClosed is returned when a request is made on closed NodeHost instance.
	ErrClosed = errors.New("dragonboat: closed")
	// ErrReplicaRemoved indictes that the requested node has been removed.
	ErrReplicaRemoved = errors.New("node removed")
	// ErrShardNotFound indicates that the specified shard is not found.
	ErrShardNotFound = errors.New("shard not found")
	// ErrShardAlreadyExist indicates that the specified shard already exist.
	ErrShardAlreadyExist = errors.New("shard already exist")
	// ErrShardNotStopped indicates that the specified shard is still running
	// and thus prevented the requested operation to be completed.
	ErrShardNotStopped = errors.New("shard not stopped")
	// ErrInvalidShardSettings indicates that shard settings specified for
	// the StartReplica method are invalid.
	ErrInvalidShardSettings = errors.New("shard settings are invalid")
	// ErrShardNotBootstrapped indicates that the specified shard has not
	// been boostrapped yet. When starting this node, depending on whether this
	// node is an initial member of the Raft shard, you must either specify
	// all of its initial members or set the join flag to true.
	// When used correctly, dragonboat only returns this error in the rare
	// situation when you try to restart a node crashed during its previous
	// bootstrap attempt.
	ErrShardNotBootstrapped = errors.New("shard not bootstrapped")
	// ErrDeadlineNotSet indicates that the context parameter provided does not
	// carry a deadline.
	ErrDeadlineNotSet = errors.New("deadline not set")
	// ErrInvalidDeadline indicates that the specified deadline is invalid, e.g.
	// time in the past.
	ErrInvalidDeadline = errors.New("invalid deadline")
	// ErrDirNotExist indicates that the specified dir does not exist.
	ErrDirNotExist = errors.New("specified dir does not exist")
	// ErrLogDBNotCreatedOrClosed indicates that the logdb is not created yet or closed already.
	ErrLogDBNotCreatedOrClosed = errors.New("logdb is not created yet or closed already")
	// ErrInvalidRange indicates that the specified log range is invalid.
	ErrInvalidRange = errors.New("invalid log range")
)

// ShardInfo is a record for representing the state of a Raft shard based
// on the knowledge of the local NodeHost instance.
type ShardInfo = registry.ShardInfo

// ShardView is a record for representing the state of a Raft shard based
// on the knowledge of distributed NodeHost instances as shared by gossip.
type ShardView = registry.ShardView

// GossipInfo contains details of the gossip service.
type GossipInfo struct {
	// AdvertiseAddress is the advertise address used by the gossip service.
	AdvertiseAddress string
	// NumOfKnownNodeHosts is the number of current live NodeHost instances known
	// to the gossip service. Note that the gossip service always knowns the
	// local NodeHost instance itself. When the NumOfKnownNodeHosts value is 1,
	// it means the gossip service doesn't know any other NodeHost instance that
	// is considered as live.
	NumOfKnownNodeHosts int
	// Enabled is a boolean flag indicating whether the gossip service is enabled.
	Enabled bool
}

// NodeHostInfo provides info about the NodeHost, including its managed Raft
// shard nodes and available Raft logs saved in its local persistent storage.
type NodeHostInfo struct {
	// NodeHostID is the unique identifier of the NodeHost instance.
	NodeHostID string
	// RaftAddress is the public address of the NodeHost used for exchanging Raft
	// messages, snapshots and other metadata with other NodeHost instances.
	RaftAddress string
	// Gossip contains gossip service related information.
	Gossip GossipInfo
	// ShardInfo is a list of all Raft shards managed by the NodeHost
	ShardInfoList []ShardInfo
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

// SnapshotOption is the options supported when requesting a snapshot to be
// generated.
type SnapshotOption struct {
	// ExportPath is the path where the exported snapshot should be stored, it
	// must point to an existing directory for which the current user has write
	// permission.
	ExportPath string
	// CompactionOverhead is the compaction overhead value to use for the
	// requested snapshot operation when OverrideCompactionOverhead is set to
	// true. This field is ignored when exporting a snapshot. ErrInvalidOption
	// will be returned if both CompactionOverhead and CompactionIndex are set.
	CompactionOverhead uint64
	// CompactionIndex specifies the raft log index before which all log entries
	// can be compacted after creating the snapshot. This option is only considered
	// when OverrideCompactionOverhead is set to true, ErrInvalidOption will be
	// returned if both CompactionOverhead and CompactionIndex are set.
	CompactionIndex uint64
	// Exported is a boolean flag indicating whether to export the requested
	// snapshot. For an exported snapshot, users are responsible for managing the
	// snapshot files. An exported snapshot is usually used to repair the shard
	// when it permanently loses its majority quorum. See the ImportSnapshot method
	// in the tools package for more details.
	Exported bool
	// OverrideCompactionOverhead defines whether the requested snapshot operation
	// should override the compaction overhead setting specified in node's config.
	// This field is ignored when exporting a snapshot.
	OverrideCompactionOverhead bool
}

// Validate checks the SnapshotOption and return error when there is any
// invalid option found.
func (o SnapshotOption) Validate() error {
	if o.OverrideCompactionOverhead {
		if o.CompactionOverhead > 0 && o.CompactionIndex > 0 {
			plog.Errorf("both CompactionOverhead and CompactionIndex are set")
			return ErrInvalidOption
		}
	} else {
		if o.CompactionOverhead > 0 || o.CompactionIndex > 0 {
			plog.Warningf("CompactionOverhead and CompactionIndex will be ignored")
		}
	}
	return nil
}

// ReadonlyLogReader provides safe readonly access to the underlying logdb.
type ReadonlyLogReader interface {
	// GetRange returns the range of the entries in LogDB.
	GetRange() (uint64, uint64)
	// NodeState returns the state of the node persistent in LogDB.
	NodeState() (pb.State, pb.Membership)
	// Term returns the entry term of the specified entry.
	Term(index uint64) (uint64, error)
	// Entries returns entries between [low, high) with total size of entries
	// limited to maxSize bytes.
	Entries(low uint64, high uint64, maxSize uint64) ([]pb.Entry, error)
	// Snapshot returns the metadata for the most recent snapshot known to the
	// LogDB.
	Snapshot() pb.Snapshot
}

// DefaultSnapshotOption is the default SnapshotOption value to use when
// requesting a snapshot to be generated. This default option causes a regular
// snapshot to be generated.
var DefaultSnapshotOption SnapshotOption

// Target is the type used to specify where a node is running. Target is remote
// NodeHost's RaftAddress value when NodeHostConfig.DefaultNodeRegistryEnabled is not
// set. Target will use NodeHost's ID value when
// NodeHostConfig.DefaultNodeRegistryEnabled is set.
type Target = string

// NodeHost manages Raft shards and enables them to share resources such as
// transport and persistent storage etc. NodeHost is also the central thread
// safe access point for accessing Dragonboat functionalities.
type NodeHost struct {
	mu struct {
		sync.RWMutex
		cci    uint64
		cciCh  chan struct{}
		shards sync.Map
		lm     sync.Map
		logdb  raftio.ILogDB
	}
	events struct {
		leaderInfoQ *leaderInfoQueue
		raft        raftio.IRaftEventListener
		sys         *sysEventListener
	}
	registry     INodeHostRegistry
	nodes        raftio.INodeRegistry
	fs           vfs.IFS
	transport    transport.ITransport
	id           *id.UUID
	stopper      *syncutil.Stopper
	msgHandler   *messageHandler
	env          *server.Env
	engine       *engine
	nhConfig     config.NodeHostConfig
	requestPools []*sync.Pool
	partitioned  int32
	closed       int32
}

var _ nodeLoader = (*NodeHost)(nil)

var dn = logutil.DescribeNode

var firstError = utils.FirstError

// NewNodeHost creates a new NodeHost instance. In a typical application, it is
// expected to have one NodeHost on each server.
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
			nh.Close()
			if r, ok := r.(error); ok {
				panicNow(r)
			}
		}
	}()
	did := nh.nhConfig.GetDeploymentID()
	plog.Infof("DeploymentID set to %d", did)
	if err := nh.createLogDB(); err != nil {
		nh.Close()
		return nil, err
	}
	if err := nh.loadNodeHostID(); err != nil {
		nh.Close()
		return nil, err
	}
	plog.Infof("NodeHost ID: %s", nh.id.String())
	if err := nh.createNodeRegistry(); err != nil {
		nh.Close()
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
		nh.Close()
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

// Close stops all managed Raft nodes and releases all resources owned by the
// NodeHost instance.
func (nh *NodeHost) Close() {
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
	nh.forEachShard(func(cid uint64, node *node) bool {
		nodes = append(nodes, raftio.NodeInfo{
			ShardID:   node.shardID,
			ReplicaID: node.replicaID,
		})
		return true
	})
	for _, node := range nodes {
		if err := nh.stopNode(node.ShardID, node.ReplicaID, true); err != nil {
			plog.Errorf("failed to remove shard %s",
				logutil.ShardID(node.ShardID))
		}
	}
	plog.Debugf("%s is stopping the nh stopper", nh.describe())
	nh.stopper.Stop()
	var err error
	plog.Debugf("%s is stopping the tranport module", nh.describe())
	if nh.transport != nil {
		err = firstError(err, nh.transport.Close())
	}
	if nh.nodes != nil {
		err = firstError(err, nh.nodes.Close())
		nh.nodes = nil
	}
	plog.Debugf("%s is stopping the engine module", nh.describe())
	if nh.engine != nil {
		err = firstError(err, nh.engine.close())
		nh.engine = nil
		nh.transport = nil
	}
	plog.Debugf("%s is stopping the logdb module", nh.describe())
	if nh.mu.logdb != nil {
		err = firstError(err, nh.mu.logdb.Close())
		nh.mu.logdb = nil
	}
	plog.Debugf("%s is stopping the env module", nh.describe())
	err = firstError(err, nh.env.Close())
	plog.Debugf("NodeHost %s stopped", nh.describe())
	if err != nil {
		panicNow(err)
	}
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
// value when calling the StartReplica, RequestAddReplica, RequestAddNonVoting,
// RequestAddWitness methods.
func (nh *NodeHost) ID() string {
	return nh.id.String()
}

// GetNodeHostRegistry returns the NodeHostRegistry instance that can be used
// to query NodeHost details shared between NodeHost instances by gossip.
func (nh *NodeHost) GetNodeHostRegistry() (INodeHostRegistry, bool) {
	return nh.registry, nh.nhConfig.DefaultNodeRegistryEnabled
}

// StartReplica adds the specified Raft replica node to the NodeHost and starts
// the node to make it ready for accepting incoming requests. The node to be
// started is backed by a regular state machine that implements the
// sm.IStateMachine interface.
//
// The input parameter initialMembers is a map of replica ID to replica target for all
// Raft shard's initial member nodes. By default, the target is the
// RaftAddress value of the NodeHost where the node will be running. When running
// in the DefaultNodeRegistryEnabled mode, target should be set to the NodeHostID value
// of the NodeHost where the node will be running. See the godoc of NodeHost's ID
// method for the full definition of NodeHostID. For the same Raft shard, the
// same initialMembers map should be specified when starting its initial member
// nodes on distributed NodeHost instances.
//
// The join flag indicates whether the node is a new node joining an existing
// shard. create is a factory function for creating the IStateMachine instance,
// cfg is the configuration instance that will be passed to the underlying Raft
// node object, the shard ID and replica ID of the involved node are specified in
// the ShardID and ReplicaID fields of the provided cfg parameter.
//
// Note that this method is not for changing the membership of the specified
// Raft shard, it launches a node that is already a member of the Raft shard.
//
// As a summary, when -
//   - starting a brand new Raft shard, set join to false and specify all initial
//     member node details in the initialMembers map.
//   - joining a new node to an existing Raft shard, set join to true and leave
//     the initialMembers map empty. This requires the joining node to have already
//     been added as a member node of the Raft shard.
//   - restarting a crashed or stopped node, set join to false and leave the
//     initialMembers map to be empty. This applies to both initial member nodes
//     and those joined later.
func (nh *NodeHost) StartReplica(initialMembers map[uint64]Target,
	join bool, create sm.CreateStateMachineFunc, cfg config.Config) error {
	cf := func(shardID uint64, replicaID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(shardID, replicaID)
		return rsm.NewNativeSM(cfg, rsm.NewInMemStateMachine(sm), done)
	}
	return nh.startShard(initialMembers, join, cf, cfg, pb.RegularStateMachine)
}

// StartConcurrentReplica is similar to the StartReplica method but it is used
// to start a Raft node backed by a concurrent state machine.
func (nh *NodeHost) StartConcurrentReplica(initialMembers map[uint64]Target,
	join bool, create sm.CreateConcurrentStateMachineFunc, cfg config.Config) error {
	cf := func(shardID uint64, replicaID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(shardID, replicaID)
		return rsm.NewNativeSM(cfg, rsm.NewConcurrentStateMachine(sm), done)
	}
	return nh.startShard(initialMembers,
		join, cf, cfg, pb.ConcurrentStateMachine)
}

// StartOnDiskReplica is similar to the StartReplica method but it is used to
// start a Raft node backed by an IOnDiskStateMachine.
func (nh *NodeHost) StartOnDiskReplica(initialMembers map[uint64]Target,
	join bool, create sm.CreateOnDiskStateMachineFunc, cfg config.Config) error {
	cf := func(shardID uint64, replicaID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := create(shardID, replicaID)
		return rsm.NewNativeSM(cfg, rsm.NewOnDiskStateMachine(sm), done)
	}
	return nh.startShard(initialMembers,
		join, cf, cfg, pb.OnDiskStateMachine)
}

// StopShard stops the local Raft replica associated with the specified Raft
// shard.
//
// Note that this is not the membership change operation required to remove the
// node from the Raft shard.
func (nh *NodeHost) StopShard(shardID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	return nh.stopNode(shardID, 0, false)
}

// StopReplica stops the specified Raft replica.
//
// Note that this is not the membership change operation required to remove the
// node from the Raft shard.
func (nh *NodeHost) StopReplica(shardID uint64, replicaID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	return nh.stopNode(shardID, replicaID, true)
}

// SyncPropose makes a synchronous proposal on the Raft shard specified by
// the input client session object. The specified context parameter must have
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
// shard. The specified context parameter must have the timeout value set. The
// query interface{} specifies what to query, it will be passed to the Lookup
// method of the IStateMachine or IOnDiskStateMachine after the system
// determines that it is safe to perform the local read. It returns the query
// result from the Lookup method or the error encountered.
func (nh *NodeHost) SyncRead(ctx context.Context, shardID uint64,
	query interface{}) (interface{}, error) {
	v, err := nh.linearizableRead(ctx, shardID,
		func(node *node) (interface{}, error) {
			data, err := node.sm.Lookup(query)
			if errors.Is(err, rsm.ErrShardClosed) {
				return nil, ErrShardClosed
			}
			return data, err
		})
	if err != nil {
		return nil, err
	}
	return v, nil
}

// GetLogReader returns a read-only LogDB reader.
func (nh *NodeHost) GetLogReader(shardID uint64) (ReadonlyLogReader, error) {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	if nh.mu.logdb == nil {
		return nil, ErrLogDBNotCreatedOrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrLogDBNotCreatedOrClosed
	}
	return n.logReader, nil
}

// Membership is the struct used to describe Raft shard membership.
type Membership struct {
	// ConfigChangeID is the Raft entry index of the last applied membership
	// change entry.
	ConfigChangeID uint64
	// Nodes is a map of ReplicaID values to NodeHost Raft addresses for all regular
	// Raft nodes.
	Nodes map[uint64]string
	// NonVotings is a map of ReplicaID values to NodeHost Raft addresses for all
	// nonVotings in the Raft shard.
	NonVotings map[uint64]string
	// Witnesses is a map of ReplicaID values to NodeHost Raft addresses for all
	// witnesses in the Raft shard.
	Witnesses map[uint64]string
	// Removed is a set of ReplicaID values that have been removed from the Raft
	// shard. They are not allowed to be added back to the shard.
	Removed map[uint64]struct{}
}

// SyncGetShardMembership is a synchronous method that queries the membership
// information from the specified Raft shard. The specified context parameter
// must have the timeout value set.
func (nh *NodeHost) SyncGetShardMembership(ctx context.Context,
	shardID uint64) (*Membership, error) {
	v, err := nh.linearizableRead(ctx, shardID,
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
				NonVotings:     m.NonVotings,
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

// GetLeaderID returns the leader replica ID of the specified Raft shard based
// on local node's knowledge. The returned boolean value indicates whether the
// leader information is available.
func (nh *NodeHost) GetLeaderID(shardID uint64) (uint64, uint64, bool, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return 0, 0, false, ErrClosed
	}
	v, ok := nh.getShard(shardID)
	if !ok {
		return 0, 0, false, ErrShardNotFound
	}
	leaderID, term, valid := v.getLeaderID()
	return leaderID, term, valid, nil
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
// based user state machines.
func (nh *NodeHost) GetNoOPSession(shardID uint64) *client.Session {
	return client.NewNoOPSession(shardID, nh.env.GetRandomSource())
}

// SyncGetSession starts a synchronous proposal to create, register and return
// a new client session object for the specified Raft shard. The specified
// context parameter must have the timeout value set.
//
// A client session object is used to ensure that a retried proposal, e.g.
// proposal retried after timeout, will not be applied more than once into the
// state machine.
//
// Returned client session instance is not thread safe.
//
// Client session is not supported by IOnDiskStateMachine based user state
// machines. NO-OP client session must be used on IOnDiskStateMachine based
// state machines.
func (nh *NodeHost) SyncGetSession(ctx context.Context,
	shardID uint64) (*client.Session, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cs := client.NewSession(shardID, nh.env.GetRandomSource())
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
// from the system in a synchronous manner. The specified context parameter
// must have the timeout value set.
//
// Closed client session should not be used in future proposals.
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

// QueryRaftLog starts an asynchronous query for raft logs in the specified
// range [firstIndex, lastIndex) on the given Raft shard. The returned
// raft log entries are limited to maxSize in bytes.
//
// This method returns a RequestState instance or an error immediately. User
// can use the CompletedC channel of the returned RequestState to get notified
// when the query result becomes available.
func (nh *NodeHost) QueryRaftLog(shardID uint64, firstIndex uint64,
	lastIndex uint64, maxSize uint64) (*RequestState, error) {
	return nh.queryRaftLog(shardID, firstIndex, lastIndex, maxSize)
}

// Propose starts an asynchronous proposal on the Raft shard specified by the
// Session object. The input byte slice can be reused for other purposes
// immediate after the return of this method.
//
// This method returns a RequestState instance or an error immediately. User can
// wait on the ResultC() channel of the returned RequestState instance to get
// notified for the outcome of the proposal.
//
// After the proposal is completed, i.e. RequestResult is received from the
// ResultC() channel of the returned RequestState, unless NO-OP client session
// is used, it is caller's responsibility to update the Session instance
// accordingly. Basically, when RequestTimeout is returned, you can retry the
// same proposal without updating your client session instance, when a
// RequestRejected value is returned, it usually means the session instance has
// been evicted from the server side as there are too many ongoing client
// sessions, the Raft paper recommends users to crash the client in such highly
// unlikely event. When the proposal completed successfully with a
// RequestCompleted value, application must call client.ProposalCompleted() to
// get the client session ready to be used in future proposals.
func (nh *NodeHost) Propose(session *client.Session, cmd []byte,
	timeout time.Duration) (*RequestState, error) {
	return nh.propose(session, cmd, timeout)
}

// ProposeSession starts an asynchronous proposal on the specified shard
// for client session related operations. Depending on the state of the specified
// client session object, the supported operations are for registering or
// unregistering a client session. Application can select on the ResultC()
// channel of the returned RequestState instance to get notified for the
// completion (RequestResult.Completed() is true) of the operation.
func (nh *NodeHost) ProposeSession(session *client.Session,
	timeout time.Duration) (*RequestState, error) {
	n, ok := nh.getShard(session.ShardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	// witness node is not expected to propose anything
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	if !n.supportClientSession() && !session.IsNoOPSession() {
		plog.Panicf("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	defer nh.engine.setStepReady(session.ShardID)
	return n.proposeSession(session, nh.getTimeoutTick(timeout))
}

// ReadIndex starts the asynchronous ReadIndex protocol used for linearizable
// read on the specified shard. This method returns a RequestState instance
// or an error immediately. Application should wait on the ResultC() channel
// of the returned RequestState object to get notified on the outcome of the
// ReadIndex operation. On a successful completion, the ReadLocalNode method
// can then be invoked to query the state of the IStateMachine or
// IOnDiskStateMachine with linearizability guarantee.
func (nh *NodeHost) ReadIndex(shardID uint64,
	timeout time.Duration) (*RequestState, error) {
	rs, _, err := nh.readIndex(shardID, timeout)
	return rs, err
}

// ReadLocalNode queries the Raft node identified by the input RequestState
// instance. ReadLocalNode is only allowed to be called after receiving a
// RequestCompleted notification from the ReadIndex method.
func (nh *NodeHost) ReadLocalNode(rs *RequestState,
	query interface{}) (interface{}, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	rs.mustBeReadyForLocalRead()
	// translate the rsm.ErrShardClosed to ErrShardClosed
	// internally, the IManagedStateMachine might obtain a RLock before performing
	// the local read. The critical section is used to make sure we don't read
	// from a destroyed C++ StateMachine object
	data, err := rs.node.sm.Lookup(query)
	if errors.Is(err, rsm.ErrShardClosed) {
		return nil, ErrShardClosed
	}
	return data, err
}

// NAReadLocalNode is a no extra heap allocation variant of ReadLocalNode, it
// uses byte slice as its input and output data to avoid extra heap allocations
// caused by using interface{}. Users are recommended to use the ReadLocalNode
// method unless performance is the top priority.
//
// As an optional feature of the state machine, NAReadLocalNode returns
// statemachine.ErrNotImplemented if the underlying state machine does not
// implement the statemachine.IExtended interface.
//
// Similar to ReadLocalNode, NAReadLocalNode is only allowed to be called after
// receiving a RequestCompleted notification from the ReadIndex method.
func (nh *NodeHost) NAReadLocalNode(rs *RequestState,
	query []byte) ([]byte, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	rs.mustBeReadyForLocalRead()
	data, err := rs.node.sm.NALookup(query)
	if errors.Is(err, rsm.ErrShardClosed) {
		return nil, ErrShardClosed
	}
	return data, err
}

var staleReadCalled uint32

// StaleRead queries the specified Raft node directly without any
// linearizability guarantee.
func (nh *NodeHost) StaleRead(shardID uint64,
	query interface{}) (interface{}, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	if atomic.CompareAndSwapUint32(&staleReadCalled, 0, 1) {
		plog.Warningf("StaleRead called, linearizability not guaranteed for stale read")
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	if !n.initialized() {
		return nil, ErrShardNotInitialized
	}
	if n.isWitness() {
		return nil, ErrInvalidOperation
	}
	data, err := n.sm.Lookup(query)
	if errors.Is(err, rsm.ErrShardClosed) {
		return nil, ErrShardClosed
	}
	return data, err
}

// SyncRequestSnapshot is the synchronous variant of the RequestSnapshot
// method. See RequestSnapshot for more details.
//
// The input context object must have deadline set.
//
// SyncRequestSnapshot returns the index of the created snapshot or the error
// encountered.
func (nh *NodeHost) SyncRequestSnapshot(ctx context.Context,
	shardID uint64, opt SnapshotOption) (uint64, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	rs, err := nh.RequestSnapshot(shardID, opt, timeout)
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
// specified shard node. For each node, only one ongoing snapshot operation
// is allowed.
//
// Each requested snapshot will also trigger Raft log and snapshot compactions
// similar to automatic snapshotting. Users need to subsequently call
// RequestCompaction(), which can be far more I/O intensive, at suitable time to
// actually reclaim disk spaces used by Raft log entries and snapshot metadata
// records.
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
func (nh *NodeHost) RequestSnapshot(shardID uint64,
	opt SnapshotOption, timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	defer nh.engine.setStepReady(shardID)
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
func (nh *NodeHost) RequestCompaction(shardID uint64,
	replicaID uint64) (*SysOpState, error) {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		// assume this is a node that has already been removed via RemoveData
		done, err := nh.mu.logdb.CompactEntriesTo(shardID, replicaID, math.MaxUint64)
		if err != nil {
			return nil, err
		}
		return &SysOpState{completedC: done}, nil
	}
	if n.replicaID != replicaID {
		return nil, ErrShardNotFound
	}
	defer nh.engine.setStepReady(shardID)
	return n.requestCompaction()
}

// SyncRequestDeleteReplica is the synchronous variant of the RequestDeleteReplica
// method. See RequestDeleteReplica for more details.
//
// The input context object must have its deadline set.
func (nh *NodeHost) SyncRequestDeleteReplica(ctx context.Context,
	shardID uint64, replicaID uint64, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestDeleteReplica(shardID, replicaID, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddReplica is the synchronous variant of the RequestAddReplica method.
// See RequestAddReplica for more details.
//
// The input context object must have its deadline set.
func (nh *NodeHost) SyncRequestAddReplica(ctx context.Context,
	shardID uint64, replicaID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddReplica(shardID,
		replicaID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddNonVoting is the synchronous variant of the RequestAddNonVoting
// method. See RequestAddNonVoting for more details.
//
// The input context object must have its deadline set.
func (nh *NodeHost) SyncRequestAddNonVoting(ctx context.Context,
	shardID uint64, replicaID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddNonVoting(shardID,
		replicaID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// SyncRequestAddWitness is the synchronous variant of the RequestAddWitness
// method. See RequestAddWitness for more details.
//
// The input context object must have its deadline set.
func (nh *NodeHost) SyncRequestAddWitness(ctx context.Context,
	shardID uint64, replicaID uint64,
	target string, configChangeIndex uint64) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	rs, err := nh.RequestAddWitness(shardID,
		replicaID, target, configChangeIndex, timeout)
	if err != nil {
		return err
	}
	_, err = getRequestState(ctx, rs)
	return err
}

// RequestDeleteReplica is a Raft shard membership change method for requesting
// the specified node to be removed from the specified Raft shard. It starts
// an asynchronous request to remove the node from the Raft shard membership
// list. Application can wait on the ResultC() channel of the returned
// RequestState instance to get notified for the outcome.
//
// It is not guaranteed that deleted node will automatically close itself and
// be removed from its managing NodeHost instance. It is application's
// responsibility to call StopShard on the right NodeHost instance to actually
// have the shard node removed from its managing NodeHost instance.
//
// Once a node is successfully deleted from a Raft shard, it will not be
// allowed to be added back to the shard with the same node identity.
//
// When the Raft shard is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetShardMembership method. The requested delete node operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetShardMembership method.
func (nh *NodeHost) RequestDeleteReplica(shardID uint64,
	replicaID uint64,
	configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	tt := nh.getTimeoutTick(timeout)
	defer nh.engine.setStepReady(shardID)
	return n.requestDeleteNodeWithOrderID(replicaID, configChangeIndex, tt)
}

// RequestAddReplica is a Raft shard membership change method for requesting the
// specified node to be added to the specified Raft shard. It starts an
// asynchronous request to add the node to the Raft shard membership list.
// Application can wait on the ResultC() channel of the returned RequestState
// instance to get notified for the outcome.
//
// If there is already an nonVoting with the same replicaID in the shard, it will
// be promoted to a regular node with voting power. The target parameter of the
// RequestAddReplica call is ignored when promoting an nonVoting to a regular node.
//
// After the node is successfully added to the Raft shard, it is application's
// responsibility to call StartReplica on the target NodeHost instance to
// actually start the Raft shard node.
//
// Requesting a removed node back to the Raft shard will always be rejected.
//
// By default, the target parameter is the RaftAddress of the NodeHost instance
// where the new Raft node will be running. Note that fixed IP or static DNS
// name should be used in RaftAddress in such default mode. When running in the
// DefaultNodeRegistryEnabled mode, target should be set to NodeHost's ID value which
// can be obtained by calling the ID() method.
//
// When the Raft shard is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// SyncGetShardMembership method. The requested add node operation will be
// rejected if other membership change has been applied since that earlier call
// to the SyncGetShardMembership method.
func (nh *NodeHost) RequestAddReplica(shardID uint64,
	replicaID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	defer nh.engine.setStepReady(shardID)
	return n.requestAddNodeWithOrderID(replicaID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestAddNonVoting is a Raft shard membership change method for requesting
// the specified node to be added to the specified Raft shard as an non-voting
// member without voting power. It starts an asynchronous request to add the
// specified node as an non-voting member.
//
// Such nonVoting is able to receive replicated states from the leader node, but
// it is neither allowed to vote for leader, nor considered as a part of the
// quorum when replicating state. An nonVoting can be promoted to a regular node
// with voting power by making a RequestAddReplica call using its shardID and
// replicaID values. An nonVoting can be removed from the shard by calling
// RequestDeleteReplica with its shardID and replicaID values.
//
// Application should later call StartReplica with config.Config.IsNonVoting
// set to true on the right NodeHost to actually start the nonVoting instance.
//
// See the godoc of the RequestAddReplica method for the details of the target and
// configChangeIndex parameters.
func (nh *NodeHost) RequestAddNonVoting(shardID uint64,
	replicaID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	defer nh.engine.setStepReady(shardID)
	return n.requestAddNonVotingWithOrderID(replicaID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestAddWitness is a Raft shard membership change method for requesting
// the specified node to be added as a witness to the given Raft shard. It
// starts an asynchronous request to add the specified node as an witness.
//
// A witness can vote in elections but it doesn't have any Raft log or
// application state machine associated. The witness node can not be used
// to initiate read, write or membership change operations on its Raft shard.
// Section 11.7.2 of Diego Ongaro's thesis contains more info on such witness
// role.
//
// Application should later call StartReplica with config.Config.IsWitness
// set to true on the right NodeHost to actually start the witness node.
//
// See the godoc of the RequestAddReplica method for the details of the target and
// configChangeIndex parameters.
func (nh *NodeHost) RequestAddWitness(shardID uint64,
	replicaID uint64, target Target, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	defer nh.engine.setStepReady(shardID)
	return n.requestAddWitnessWithOrderID(replicaID,
		target, configChangeIndex, nh.getTimeoutTick(timeout))
}

// RequestLeaderTransfer makes a request to transfer the leadership of the
// specified Raft shard to the target node identified by targetReplicaID. It
// returns an error if the request fails to be started. There is no guarantee
// that such request can be fulfilled.
func (nh *NodeHost) RequestLeaderTransfer(shardID uint64,
	targetReplicaID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return ErrShardNotFound
	}
	plog.Debugf("RequestLeaderTransfer called on shard %d target replicaID %d",
		shardID, targetReplicaID)
	defer nh.engine.setStepReady(shardID)
	return n.requestLeaderTransfer(targetReplicaID)
}

// SyncRemoveData is the synchronous variant of the RemoveData. It waits for
// the specified node to be fully offloaded or until the context object instance
// is cancelled or timeout.
//
// Similar to RemoveData, calling SyncRemoveData on a node that is still a Raft
// shard member will corrupt the Raft shard.
func (nh *NodeHost) SyncRemoveData(ctx context.Context,
	shardID uint64, replicaID uint64) error {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadlineNotSet
	}
	if _, ok := nh.getShard(shardID); ok {
		return ErrShardNotStopped
	}
	if ch := nh.engine.destroyedC(shardID, replicaID); ch != nil {
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
	err := nh.RemoveData(shardID, replicaID)
	if errors.Is(err, ErrShardNotStopped) {
		panic("node not stopped")
	}
	return err
}

// RemoveData tries to remove all data associated with the specified node. This
// method should only be used after the node has been deleted from its Raft
// shard. Calling RemoveData on a node that is still a Raft shard member
// will corrupt the Raft shard.
//
// RemoveData returns ErrShardNotStopped when the specified node has not been
// fully offloaded from the NodeHost instance.
func (nh *NodeHost) RemoveData(shardID uint64, replicaID uint64) error {
	n, ok := nh.getShard(shardID)
	if ok && n.replicaID == replicaID {
		return ErrShardNotStopped
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return ErrClosed
	}
	if nh.engine.nodeLoaded(shardID, replicaID) {
		return ErrShardNotStopped
	}
	plog.Debugf("%s called RemoveData", dn(shardID, replicaID))
	if err := nh.mu.logdb.RemoveNodeData(shardID, replicaID); err != nil {
		panicNow(err)
	}
	// mark the snapshot dir as removed
	did := nh.nhConfig.GetDeploymentID()
	if err := nh.env.RemoveSnapshotDir(did, shardID, replicaID); err != nil {
		panicNow(err)
	}
	return nil
}

// GetNodeUser returns an INodeUser instance ready to be used to directly make
// proposals or read index operations without locating the node repeatedly in
// the NodeHost. A possible use case is when loading a large data set say with
// billions of proposals into the dragonboat based system.
func (nh *NodeHost) GetNodeUser(shardID uint64) (INodeUser, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	return &nodeUser{
		nh:           nh,
		node:         n,
		setStepReady: nh.engine.setStepReady,
	}, nil
}

// HasNodeInfo returns a boolean value indicating whether the specified node
// has been bootstrapped on the current NodeHost instance.
func (nh *NodeHost) HasNodeInfo(shardID uint64, replicaID uint64) bool {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	if atomic.LoadInt32(&nh.closed) != 0 {
		return false
	}
	if _, err := nh.mu.logdb.GetBootstrapInfo(shardID, replicaID); err != nil {
		if errors.Is(err, raftio.ErrNoBootstrapInfo) {
			return false
		}
		panicNow(err)
	}
	return true
}

// GetNodeHostInfo returns a NodeHostInfo instance that contains all details
// of the NodeHost, this includes details of all Raft shards managed by the
// the NodeHost instance.
func (nh *NodeHost) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo {
	nhi := &NodeHostInfo{
		NodeHostID:    nh.ID(),
		RaftAddress:   nh.RaftAddress(),
		Gossip:        nh.getGossipInfo(),
		ShardInfoList: nh.getShardInfo(),
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
	if r, ok := nh.nodes.(*registry.GossipRegistry); ok {
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
	v, ok := nh.getShard(s.ShardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	if !v.supportClientSession() && !s.IsNoOPSession() {
		panic("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	req, err := v.propose(s, cmd, nh.getTimeoutTick(timeout))
	nh.engine.setStepReady(s.ShardID)
	return req, err
}

func (nh *NodeHost) readIndex(shardID uint64,
	timeout time.Duration) (*RequestState, *node, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, nil, ErrClosed
	}
	n, ok := nh.getShard(shardID)
	if !ok {
		return nil, nil, ErrShardNotFound
	}
	req, err := n.read(nh.getTimeoutTick(timeout))
	if err != nil {
		return nil, nil, err
	}
	nh.engine.setStepReady(shardID)
	return req, n, err
}

func (nh *NodeHost) queryRaftLog(shardID uint64,
	firstIndex uint64, lastIndex uint64, maxSize uint64) (*RequestState, error) {
	if atomic.LoadInt32(&nh.closed) != 0 {
		return nil, ErrClosed
	}
	v, ok := nh.getShard(shardID)
	if !ok {
		return nil, ErrShardNotFound
	}
	if lastIndex <= firstIndex {
		return nil, ErrInvalidRange
	}
	req, err := v.queryRaftLog(firstIndex, lastIndex, maxSize)
	nh.engine.setStepReady(shardID)
	return req, err
}

func (nh *NodeHost) linearizableRead(ctx context.Context,
	shardID uint64, f func(n *node) (interface{}, error)) (interface{}, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, node, err := nh.readIndex(shardID, timeout)
	if err != nil {
		return nil, err
	}
	if _, err := getRequestState(ctx, rs); err != nil {
		return nil, err
	}
	rs.Release()
	return f(node)
}

func (nh *NodeHost) getShard(shardID uint64) (*node, bool) {
	n, ok := nh.mu.shards.Load(shardID)
	if !ok {
		return nil, false
	}
	return n.(*node), true
}

func (nh *NodeHost) forEachShard(f func(uint64, *node) bool) uint64 {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	nh.mu.shards.Range(func(k, v interface{}) bool {
		return f(k.(uint64), v.(*node))
	})
	return nh.mu.cci
}

func (nh *NodeHost) getShardSetIndex() uint64 {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	return nh.mu.cci
}

// there are three major reasons to bootstrap the shard -
//
//  1. when possible, we check whether user incorrectly specified parameters
//     for the startShard method, e.g. call startShard with join=true first,
//     then restart the NodeHost instance and call startShard again with
//     join=false and len(nodes) > 0
//  2. when restarting a node which is a part of the initial shard members,
//     for user convenience, we allow the caller not to provide the details of
//     initial members. when the initial shard member info is required, however
//     we still need to get the initial member info from somewhere. bootstrap is
//     the procedure that records such info.
//  3. the bootstrap record is used as a marker record in our default LogDB
//     implementation to indicate that a certain node exists here
func (nh *NodeHost) bootstrapShard(initialMembers map[uint64]Target,
	join bool, cfg config.Config,
	smType pb.StateMachineType) (map[uint64]string, bool, error) {
	bi, err := nh.mu.logdb.GetBootstrapInfo(cfg.ShardID, cfg.ReplicaID)
	if errors.Is(err, raftio.ErrNoBootstrapInfo) {
		if !join && len(initialMembers) == 0 {
			return nil, false, ErrShardNotBootstrapped
		}
		var members map[uint64]string
		if !join {
			members = initialMembers
		}
		bi = pb.NewBootstrapInfo(join, smType, initialMembers)
		err := nh.mu.logdb.SaveBootstrapInfo(cfg.ShardID, cfg.ReplicaID, bi)
		if err != nil {
			return nil, false, err
		}
		return members, !join, nil
	} else if err != nil {
		return nil, false, err
	}
	if !bi.Validate(initialMembers, join, smType) {
		plog.Errorf("bootstrap info validation failed, %s, %v, %t, %v, %t",
			dn(cfg.ShardID, cfg.ReplicaID),
			bi.Addresses, bi.Join, initialMembers, join)
		return nil, false, ErrInvalidShardSettings
	}
	return bi.Addresses, !bi.Join, nil
}

func (nh *NodeHost) startShard(initialMembers map[uint64]Target,
	join bool, createStateMachine rsm.ManagedStateMachineFactory,
	cfg config.Config, smType pb.StateMachineType) error {
	shardID := cfg.ShardID
	replicaID := cfg.ReplicaID
	validator := nh.nhConfig.GetTargetValidator()
	for _, target := range initialMembers {
		if !validator(target) {
			return ErrInvalidTarget
		}
	}

	doStart := func() (*node, error) {
		nh.mu.Lock()
		defer nh.mu.Unlock()

		if atomic.LoadInt32(&nh.closed) != 0 {
			return nil, ErrClosed
		}
		if _, ok := nh.mu.shards.Load(shardID); ok {
			return nil, ErrShardAlreadyExist
		}
		if nh.engine.nodeLoaded(shardID, replicaID) {
			// node is still loaded in the execution engine, e.g. processing snapshot
			return nil, ErrShardAlreadyExist
		}
		if join && len(initialMembers) > 0 {
			return nil, ErrInvalidShardSettings
		}
		peers, im, err := nh.bootstrapShard(initialMembers, join, cfg, smType)
		if errors.Is(err, ErrInvalidShardSettings) {
			return nil, err
		}
		if err != nil {
			panicNow(err)
		}
		for k, v := range peers {
			if k != replicaID {
				nh.nodes.Add(shardID, k, v)
			}
		}
		did := nh.nhConfig.GetDeploymentID()
		if err := nh.env.CreateSnapshotDir(did, shardID, replicaID); err != nil {
			if errors.Is(err, server.ErrDirMarkedAsDeleted) {
				return nil, ErrReplicaRemoved
			}
			panicNow(err)
		}
		getSnapshotDir := func(cid uint64, nid uint64) string {
			return nh.env.GetSnapshotDir(did, cid, nid)
		}
		logReader := logdb.NewLogReader(shardID, replicaID, nh.mu.logdb)
		ss := newSnapshotter(shardID, replicaID,
			getSnapshotDir, nh.mu.logdb, logReader, nh.fs)
		logReader.SetCompactor(ss)
		if err := ss.processOrphans(); err != nil {
			panicNow(err)
		}
		p := server.NewDoubleFixedPartitioner(nh.nhConfig.Expert.Engine.ExecShards,
			nh.nhConfig.Expert.LogDB.Shards)
		shard := p.GetPartitionID(shardID)
		rn, err := newNode(peers,
			im,
			cfg,
			nh.nhConfig,
			createStateMachine,
			ss,
			logReader,
			nh.engine,
			nh.events.leaderInfoQ,
			nh.transport.GetStreamSink,
			nh.msgHandler.HandleSnapshotStatus,
			nh.sendMessage,
			nh.nodes,
			nh.requestPools[replicaID%requestPoolShards],
			nh.mu.logdb,
			nh.getLogDBMetrics(shard),
			nh.events.sys)
		if err != nil {
			panicNow(err)
		}
		rn.loaded()
		nh.mu.shards.Store(shardID, rn)
		nh.mu.cci++
		nh.cciUpdated()
		nh.engine.setCCIReady(shardID)
		nh.engine.setApplyReady(shardID)

		return rn, nil
	}

	rn, err := doStart()
	if err != nil {
		return err
	}

	if cfg.WaitReady {
		select {
		case <-rn.initializedC:
		case <-rn.stopC:
		}
	}

	return nil
}

func (nh *NodeHost) cciUpdated() {
	select {
	case nh.mu.cciCh <- struct{}{}:
	default:
	}
}

func (nh *NodeHost) loadNodeHostID() error {
	v, err := nh.env.PrepareNodeHostID(nh.nhConfig.NodeHostID)
	if err != nil {
		return err
	}
	nh.id = v
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
		lf = logdb.NewDefaultFactory()
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
	if nh.nhConfig.DefaultNodeRegistryEnabled {
		// DefaultNodeRegistryEnabled should not be set if a Expert.NodeRegistryFactory
		// is also set.
		if nh.nhConfig.Expert.NodeRegistryFactory != nil {
			return errors.New("DefaultNodeRegistryEnabled and Expert.NodeRegistryFactory should not both be set")
		}
		plog.Infof("DefaultNodeRegistryEnabled: true, use gossip based node registry")
		r, err := registry.NewGossipRegistry(nh.ID(), nh.getShardInfo,
			nh.nhConfig, streamConnections, validator)
		if err != nil {
			return err
		}
		nh.registry = r.GetNodeHostRegistry()
		nh.nodes = r
	} else if nh.nhConfig.Expert.NodeRegistryFactory != nil {
		plog.Infof("Expert.NodeRegistryFactory was set: using custom registry")
		r, err := nh.nhConfig.Expert.NodeRegistryFactory.Create(nh.ID(), streamConnections, validator)
		if err != nil {
			return err
		}
		nh.nodes = r
	} else {
		plog.Infof("using regular node registry")
		nh.nodes = registry.NewNodeRegistry(streamConnections, validator)
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

func (nh *NodeHost) stopNode(shardID uint64, replicaID uint64, check bool) error {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	v, ok := nh.mu.shards.Load(shardID)
	if !ok {
		return ErrShardNotFound
	}
	n := v.(*node)
	if check && n.replicaID != replicaID {
		return ErrShardNotFound
	}
	nh.mu.shards.Delete(shardID)
	nh.mu.cci++
	nh.cciUpdated()
	nh.engine.setCCIReady(shardID)
	n.close()
	n.offloaded()
	nh.engine.setStepReady(shardID)
	nh.engine.setCommitReady(shardID)
	nh.engine.setApplyReady(shardID)
	nh.engine.setRecoverReady(shardID)
	return nil
}

func (nh *NodeHost) getShardInfo() []ShardInfo {
	shardInfoList := make([]ShardInfo, 0)
	nh.forEachShard(func(cid uint64, node *node) bool {
		shardInfoList = append(shardInfoList, node.getShardInfo())
		return true
	})
	return shardInfoList
}

func (nh *NodeHost) tickWorkerMain() {
	tick := uint64(0)
	idx := uint64(0)
	nodes := make([]*node, 0)
	tf := func() {
		tick++
		if idx != nh.getShardSetIndex() {
			nodes = nodes[:0]
			idx = nh.forEachShard(func(cid uint64, n *node) bool {
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
			dn(msg.ShardID, msg.From), dn(msg.ShardID, msg.To),
			witness, msg.Snapshot.Index, msg.Snapshot.FileSize)
		if n, ok := nh.getShard(msg.ShardID); ok {
			if witness || !n.OnDiskStateMachine() {
				nh.transport.SendSnapshot(msg)
			} else {
				n.pushStreamSnapshotRequest(msg.ShardID, msg.To)
			}
		}
		nh.events.sys.Publish(server.SystemEvent{
			Type:      server.SendSnapshotStarted,
			ShardID:   msg.ShardID,
			ReplicaID: msg.To,
			From:      msg.From,
		})
	}
}

func (nh *NodeHost) sendTickMessage(shards []*node, tick uint64) {
	for _, n := range shards {
		m := pb.Message{
			Type: pb.LocalTick,
			To:   n.replicaID,
			From: n.replicaID,
			Hint: tick,
		}
		n.mq.Tick()
		n.mq.Add(m)
	}
}

func (nh *NodeHost) nodeMonitorMain() {
	for {
		nodes := make([]*node, 0)
		nh.forEachShard(func(cid uint64, node *node) bool {
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
			if err := nh.stopNode(n.shardID, n.replicaID, true); err != nil {
				plog.Debugf("stopNode failed %v", err)
			}
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
			return sm.Result{}, ErrShardClosed
		} else if r.Dropped() {
			return sm.Result{}, ErrShardNotReady
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
// when doing bulk load operations on selected shards.
type INodeUser interface {
	// ShardID is the shard ID of the node.
	ShardID() uint64
	// ReplicaID is the replica ID of the node.
	ReplicaID() uint64
	// Propose starts an asynchronous proposal on the Raft shard represented by
	// the INodeUser instance. Its semantics is the same as the Propose() method
	// in NodeHost.
	Propose(s *client.Session,
		cmd []byte, timeout time.Duration) (*RequestState, error)
	// ReadIndex starts the asynchronous ReadIndex protocol used for linearizable
	// reads on the Raft shard represented by the INodeUser instance. Its
	// semantics is the same as the ReadIndex() method in NodeHost.
	ReadIndex(timeout time.Duration) (*RequestState, error)
}

type nodeUser struct {
	nh           *NodeHost
	node         *node
	setStepReady func(shardID uint64)
}

var _ INodeUser = (*nodeUser)(nil)

func (nu *nodeUser) ShardID() uint64 {
	return nu.node.shardID
}

func (nu *nodeUser) ReplicaID() uint64 {
	return nu.node.replicaID
}

func (nu *nodeUser) Propose(s *client.Session,
	cmd []byte, timeout time.Duration) (*RequestState, error) {
	req, err := nu.node.propose(s, cmd, nu.nh.getTimeoutTick(timeout))
	nu.setStepReady(s.ShardID)
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
		if n, ok := nh.getShard(req.ShardID); ok {
			if n.replicaID != req.To {
				plog.Warningf("ignored a %s message sent to %s but received by %s",
					req.Type, dn(req.ShardID, req.To), dn(req.ShardID, n.replicaID))
				continue
			}
			if req.Type == pb.InstallSnapshot {
				n.mq.MustAdd(req)
				snapshotCount++
			} else if req.Type == pb.SnapshotReceived {
				plog.Debugf("SnapshotReceived received, shard id %d, replica id %d",
					req.ShardID, req.From)
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

func (h *messageHandler) HandleSnapshotStatus(shardID uint64,
	replicaID uint64, failed bool) {
	eventType := server.SendSnapshotCompleted
	if failed {
		eventType = server.SendSnapshotAborted
	}
	h.nh.events.sys.Publish(server.SystemEvent{
		Type:      eventType,
		ShardID:   shardID,
		ReplicaID: replicaID,
	})
	if n, ok := h.nh.getShard(shardID); ok {
		n.mq.AddDelayed(pb.Message{
			Type:   pb.SnapshotStatus,
			From:   replicaID,
			Reject: failed,
		}, streamPushDelayTick)
		h.nh.engine.setStepReady(shardID)
	}
}

func (h *messageHandler) HandleUnreachable(shardID uint64, replicaID uint64) {
	if n, ok := h.nh.getShard(shardID); ok {
		m := pb.Message{
			Type: pb.Unreachable,
			From: replicaID,
			To:   n.replicaID,
		}
		n.mq.MustAdd(m)
		h.nh.engine.setStepReady(shardID)
	}
}

func (h *messageHandler) HandleSnapshot(shardID uint64,
	replicaID uint64, from uint64) {
	m := pb.Message{
		To:      from,
		From:    replicaID,
		ShardID: shardID,
		Type:    pb.SnapshotReceived,
	}
	h.nh.sendMessage(m)
	plog.Debugf("%s sent SnapshotReceived to %d", dn(shardID, replicaID), from)
	h.nh.events.sys.Publish(server.SystemEvent{
		Type:      server.SnapshotReceived,
		ShardID:   shardID,
		ReplicaID: replicaID,
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
