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

/*
Package dragonboat is a multi-group Raft implementation.

The NodeHost struct is the facade interface for all features provided by the
dragonboat package. Each NodeHost instance, identified by its RaftAddress
property, usually runs on a separate host managing its CPU, storage and network
resources. Each NodeHost can manage Raft nodes from many different Raft groups
known as Raft clusters. Each Raft cluster is identified by its ClusterID Each
Raft cluster usually consists of multiple nodes, identified by their NodeID
values. Nodes from the same Raft cluster are suppose to be distributed on
different NodeHost instances across the network, this brings fault tolerance
to node failures as application data stored in such a Raft cluster can be
available as long as the majority of its managing NodeHost instances (i.e. its
underlying hosts) are available.

User applications can leverage the power of the Raft protocol implemented in
dragonboat by implementing its IStateMachine or IOnDiskStateMachine component.
IStateMachine and IOnDiskStateMachine is defined in
github.com/lni/dragonboat/statemachine. Each cluster node is associated with an
IStateMachine or IOnDiskStateMachine instance, it is in charge of updating,
querying and snapshotting application data, with minimum exposure to the
complexity of the Raft protocol implementation.

User applications can use NodeHost's APIs to update the state of their
IStateMachine or IOnDiskStateMachine instances, this is called making proposals.
Once accepted by the majority nodes of a Raft cluster, the proposal is considered
as committed and it will be applied on all member nodes of the Raft cluster.
Applications can also make linearizable reads to query the state of their
IStateMachine or IOnDiskStateMachine instances. Dragonboat employs the ReadIndex
protocol invented by Diego Ongaro to implement linearizable reads. Both read and
write operations can be initiated on any member nodes, although initiating from
the leader nodes incurs the lowest overhead.

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
the risk to have the same proposal committed and applied twice into the
IStateMachine. Dragonboat prevents this by implementing the client session
concept described in Diego Ongaro's PhD thesis.

Dragonboat is a feature complete Multi-Group Raft implementation - snapshotting,
membership change, leadership transfer, non-voting members and disk based state
machine are all provided.

Dragonboat is also extensively optimized. The Raft protocol implementation is
fully pipelined, meaning proposals can start before the completion of previous
proposals. This is critical for system throughput in high latency environment.
Dragonboat is also fully batched, it batches internal operations whenever
possible to maximize system throughput.
*/
package dragonboat // github.com/lni/dragonboat

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/client"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/raft"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/lang"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/internal/utils/stringutil"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	unmanagedDeploymentID uint64 = transport.UnmanagedDeploymentID
	// DragonboatMajor is the major version number
	DragonboatMajor = 2
	// DragonboatMinor is the minor version number
	DragonboatMinor = 2
	// DragonboatPatch is the patch version number
	DragonboatPatch = 0
	// DEVVersion is a boolean flag indicating whether this is a dev version
	DEVVersion = true
)

var (
	receiveQueueSize  uint64 = settings.Soft.RaftNodeReceiveQueueLength
	delaySampleRatio  uint64 = settings.Soft.LatencySampleRatio
	rsPoolSize        uint64 = settings.Soft.NodeHostSyncPoolSize
	streamConnections uint64 = settings.Soft.StreamConnections
	monitorInterval          = 100 * time.Millisecond
)

var (
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
	// IsLeader indicates whether this is a leader node.
	IsLeader bool
	// IsObserver indicates whether this is a non-voting observer node.
	IsObserver bool
	// StateMachineType is the type of the state machine.
	StateMachineType sm.Type
	// Nodes is a map of member node IDs to their Raft addresses.
	Nodes map[uint64]string
	// ConfigChangeIndex is the current config change index of the Raft node.
	// ConfigChangeIndex is Raft Log index of the last applied membership
	// change entry.
	ConfigChangeIndex uint64
	// Pending is a boolean flag indicating whether details of the cluster node
	// is not available. The Pending flag is set to true usually because the node
	// has not had anything applied yet.
	Pending bool
}

// NodeHostInfo provides info about the NodeHost, including its managed Raft
// cluster nodes and available Raft logs saved in its local persistent storage.
type NodeHostInfo struct {
	// RaftAddress is the public address and the identifier of the NodeHost.
	RaftAddress string
	// ClusterInfo is a list of all Raft clusters managed by the NodeHost
	ClusterInfoList []ClusterInfo
	// LogInfo is a list of raftio.NodeInfo values representing all Raft logs
	// stored on the NodeHost.
	LogInfo []raftio.NodeInfo
}

// NodeHost manages Raft clusters and enables them to share resources such as
// transport and persistent storage etc. NodeHost is also the central access
// point for Dragonboat functionalities provided to applications.
type NodeHost struct {
	tick     uint64
	msgCount uint64
	testPartitionState
	clusterMu struct {
		sync.RWMutex
		stopped  bool
		csi      uint64
		clusters sync.Map
		requests map[uint64]*server.MessageQueue
	}
	snapshotStatus   *snapshotFeedback
	serverCtx        *server.Context
	nhConfig         config.NodeHostConfig
	stopper          *syncutil.Stopper
	duStopper        *syncutil.Stopper
	nodes            *transport.Nodes
	region           string
	deploymentID     uint64
	rsPool           []*sync.Pool
	execEngine       *execEngine
	logdb            raftio.ILogDB
	transport        transport.ITransport
	msgHandler       *messageHandler
	transportLatency *sample
}

// NewNodeHost creates a new NodeHost instance. The returned NodeHost instance
// is configured using the specified NodeHostConfig instance. In a typical
// application, it is expected to have one NodeHost on each server.
func NewNodeHost(nhConfig config.NodeHostConfig) *NodeHost {
	logBuildTagsAndVersion()
	if err := nhConfig.Validate(); err != nil {
		plog.Panicf("invalid nodehost config, %v", err)
	}
	nh := &NodeHost{
		serverCtx:        server.NewContext(nhConfig),
		nhConfig:         nhConfig,
		stopper:          syncutil.NewStopper(),
		duStopper:        syncutil.NewStopper(),
		nodes:            transport.NewNodes(streamConnections),
		transportLatency: newSample(),
	}
	nh.snapshotStatus = newSnapshotFeedback(nh.pushSnapshotStatus)
	nh.msgHandler = newNodeHostMessageHandler(nh)
	nh.clusterMu.requests = make(map[uint64]*server.MessageQueue)
	nh.createPools()
	nh.createTransport()
	did := unmanagedDeploymentID
	if nhConfig.DeploymentID == 0 {
		plog.Warningf("DeploymentID not set in NodeHostConfig")
		nh.transport.SetUnmanagedDeploymentID()
	} else {
		did = nhConfig.DeploymentID
		nh.transport.SetDeploymentID(did)
	}
	plog.Infof("DeploymentID set to %d", did)
	nh.deploymentID = did
	nh.createLogDB(nhConfig, did)
	nh.execEngine = newExecEngine(nh, nh.serverCtx, nh.logdb, nh.sendNoOPMessage)
	nh.stopper.RunWorker(func() {
		nh.nodeMonitorMain(nhConfig)
	})
	nh.stopper.RunWorker(func() {
		nh.tickWorkerMain()
	})
	nh.logNodeHostDetails()
	return nh
}

// NodeHostConfig returns the NodeHostConfig instance used for configuring this
// NodeHost instance.
func (nh *NodeHost) NodeHostConfig() config.NodeHostConfig {
	return nh.nhConfig
}

// RaftAddress returns the Raft address of the NodeHost instance. The
// returned RaftAddress value is used to identify this NodeHost instance. It is
// also the address used for exchanging Raft messages and snapshots between
// distributed NodeHost instances.
func (nh *NodeHost) RaftAddress() string {
	return nh.nhConfig.RaftAddress
}

// Stop stops all Raft nodes managed by the NodeHost instance, closes the
// transport and persistent storage modules.
func (nh *NodeHost) Stop() {
	nh.clusterMu.Lock()
	nh.clusterMu.stopped = true
	nh.clusterMu.Unlock()
	nh.transport.RemoveMessageHandler()
	allNodes := make([]raftio.NodeInfo, 0)
	nh.forEachCluster(func(cid uint64, node *node) bool {
		nodeInfo := raftio.NodeInfo{
			ClusterID: node.clusterID,
			NodeID:    node.nodeID,
		}
		allNodes = append(allNodes, nodeInfo)
		return true
	})
	for _, node := range allNodes {
		if err := nh.StopNode(node.ClusterID, node.NodeID); err != nil {
			plog.Errorf("failed to remove cluster %s",
				logutil.ClusterID(node.ClusterID))
		}
	}
	plog.Debugf("%s is going to stop the nh stopper", nh.describe())
	if nh.duStopper != nil {
		nh.duStopper.Stop()
	}
	nh.stopper.Stop()
	plog.Debugf("%s is going to stop the exec engine", nh.describe())
	if nh.execEngine != nil {
		nh.execEngine.stop()
	}
	plog.Debugf("%s is going to stop the tranport module", nh.describe())
	nh.transport.Stop()
	plog.Debugf("%s transport module stopped", nh.describe())
	if nh.logdb != nil {
		nh.logdb.Close()
	} else {
		// in standalone mode, when Stop() is called in the same goroutine as
		// NewNodeHost, is nh.longdb == nil above is not going to happen
		plog.Warningf("logdb not closed")
	}
	plog.Debugf("logdb closed, %s is now stopped", nh.describe())
	nh.serverCtx.Stop()
	plog.Debugf("serverCtx stopped on %s", nh.describe())
	if delaySampleRatio > 0 {
		nh.logTransportLatency()
	}
}

// StartCluster adds the specified Raft cluster node to the NodeHost and starts
// the node to make it ready for accepting incoming requests.
//
// The input parameter nodes is a map of node ID to RaftAddress for indicating
// what are initial nodes when the Raft cluster is first created. The join flag
// indicates whether the node is a new node joining an existing cluster.
// createStateMachine is a factory function for creating the IStateMachine
// instance, config is the configuration instance that will be passed to the
// underlying Raft node object, the cluster ID and node ID of the involved node
// is given in the ClusterID and NodeID fields of the config object.
//
// Note that this method is not for changing the membership of the specified
// Raft cluster, it launches a node that is already a member of the Raft
// cluster.
//
// As a summary, when -
//  - starting a brand new Raft cluster with initial member nodes, set join to
//    false and specify all initial member node details in the nodes map.
//  - restarting an crashed or stopped node, set join to false. the content of
//    the nodes map is ignored.
//  - joining a new node to an existing Raft cluster, set join to true and leave
//    the nodes map empty. This requires the joining node to have already been
//    added as a member of the Raft cluster.
func (nh *NodeHost) StartCluster(nodes map[uint64]string,
	join bool, createStateMachine func(uint64, uint64) sm.IStateMachine,
	config config.Config) error {
	stopc := make(chan struct{})
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := createStateMachine(clusterID, nodeID)
		return rsm.NewNativeStateMachine(clusterID,
			nodeID, rsm.NewRegularStateMachine(sm), done)
	}
	return nh.startCluster(nodes, join, cf, stopc, config, pb.RegularStateMachine)
}

// StartConcurrentCluster is similar to the StartCluster method but it is used
// to add and start a Raft node backed by a concurrent state machine.
func (nh *NodeHost) StartConcurrentCluster(nodes map[uint64]string,
	join bool,
	createStateMachine func(uint64, uint64) sm.IConcurrentStateMachine,
	config config.Config) error {
	stopc := make(chan struct{})
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := createStateMachine(clusterID, nodeID)
		return rsm.NewNativeStateMachine(clusterID,
			nodeID, rsm.NewConcurrentStateMachine(sm), done)
	}
	return nh.startCluster(nodes, join, cf, stopc, config, pb.ConcurrentStateMachine)
}

// StartOnDiskCluster is similar to the StartCluster method but it is used to
// add and start a Raft node backed by an IOnDiskStateMachine.
func (nh *NodeHost) StartOnDiskCluster(nodes map[uint64]string,
	join bool,
	createStateMachine func(uint64, uint64) sm.IOnDiskStateMachine,
	config config.Config) error {
	stopc := make(chan struct{})
	cf := func(clusterID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := createStateMachine(clusterID, nodeID)
		return rsm.NewNativeStateMachine(clusterID,
			nodeID, rsm.NewOnDiskStateMachine(sm), done)
	}
	return nh.startCluster(nodes, join, cf, stopc, config, pb.OnDiskStateMachine)
}

// StopCluster removes and stops the Raft node associated with the specified
// Raft cluster from the NodeHost. The node to be removed and stopped is
// identified by the clusterID value.
//
// Note that this is not the membership change operation to remove the node
// from the Raft cluster.
func (nh *NodeHost) StopCluster(clusterID uint64) error {
	return nh.stopNode(clusterID, 0, false)
}

// StopNode removes the specified Raft cluster node from the NodeHost and
// stops that running Raft node.
//
// Note that this is not the membership change operation to remove the node
// from the Raft cluster.
func (nh *NodeHost) StopNode(clusterID uint64, nodeID uint64) error {
	return nh.stopNode(clusterID, nodeID, true)
}

// SyncPropose makes a synchronous proposal on the Raft cluster specified by
// the input client session object. It returns the result code returned by
// IStateMachine or IOnDiskStateMachine's Update method, or the error
// encountered. The input byte slice can be reused for other purposes immediate
// after the return of this method.
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
	select {
	case s := <-rs.CompletedC:
		if s.Timeout() {
			return sm.Result{}, ErrTimeout
		} else if s.Completed() {
			rs.Release()
			return s.GetResult(), nil
		} else if s.Terminated() {
			return sm.Result{}, ErrClusterClosed
		} else if s.Rejected() {
			return sm.Result{}, ErrInvalidSession
		}
		panic("unknown CompletedC value")
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return sm.Result{}, ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return sm.Result{}, ErrTimeout
		}
		panic("unknown ctx error")
	}
}

// SyncRead performs a synchronous linearizable read on the specified Raft
// cluster. The query byte slice specifies what to query, it will be passed to
// the Lookup method of the IStateMachine or IOnDiskStateMachine after the
// system determines that it is safe to perform the local read on IStateMachine
// or IOnDiskStateMachine. It returns the query result from the Lookup method or
// the error encountered.
func (nh *NodeHost) SyncRead(ctx context.Context, clusterID uint64,
	query []byte) ([]byte, error) {
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
	if v == nil {
		return nil, nil
	}
	return v.([]byte), nil
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
	// observers.
	Observers map[uint64]string
	// Removed is a set of NodeID values that have been removed from the Raft
	// cluster. They are not allowed to be added back to the cluster.
	Removed map[uint64]struct{}
}

// GetClusterMembership returns the membership information from the specified
// Raft cluster. This method guarantees that the returned membership
// information is linearizable. This is a synchronous method meaning it will
// only return after its confirmed completion, failure or timeout.
func (nh *NodeHost) GetClusterMembership(ctx context.Context,
	clusterID uint64) (*Membership, error) {
	v, err := nh.linearizableRead(ctx, clusterID,
		func(node *node) (interface{}, error) {
			members, observers, removed, confChangeID := node.sm.GetMembership()
			membership := &Membership{
				Nodes:          members,
				Observers:      observers,
				Removed:        removed,
				ConfigChangeID: confChangeID,
			}
			return membership, nil
		})
	if err != nil {
		return nil, err
	}
	r := v.(*Membership)
	return r, nil
}

// GetLeaderID returns the leader node ID of the specified Raft cluster based
// on local node's knowledge. The returned boolean value indicates whether the
// leader information is available.
func (nh *NodeHost) GetLeaderID(clusterID uint64) (uint64, bool, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return 0, false, ErrClusterNotFound
	}
	nodeID, valid := v.getLeaderID()
	return nodeID, valid, nil
}

// GetNoOPSession returns a NO-OP client session ready to be used for
// making proposals. The NO-OP client session is a dummy client session that
// will not be checked or enforced. Use this No-OP client session when you
// want to ignore features provided by client sessions. A NO-OP client session
// is not registered on the server side and thus not required to be closed at
// the end of its life cycle.
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
	return client.NewNoOPSession(clusterID, nh.serverCtx.GetRandomSource())
}

// GetNewSession starts an synchronous proposal to create, register and return
// a new client session object. A client session object is used to ensure that
// a retried proposal, e.g. proposal retried after timeout, will not be applied
// more than once into the IStateMachine.
//
// Returned client session instance should not be used concurrently. Use
// multiple client sessions when you need to concurrently start multiple
// proposals.
//
// Client session is not supported by IOnDiskStateMachine based state machine.
// NO-OP client session must be used for making proposals on IOnDiskStateMachine
// based state machine.
func (nh *NodeHost) GetNewSession(ctx context.Context,
	clusterID uint64) (*client.Session, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cs := client.NewSession(clusterID, nh.serverCtx.GetRandomSource())
	cs.PrepareForRegister()
	rs, err := nh.ProposeSession(cs, timeout)
	if err != nil {
		return nil, err
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() && r.GetResult().Value == cs.ClientID {
			cs.PrepareForPropose()
			return cs, nil
		} else if r.Rejected() {
			return nil, ErrRejected
		} else if r.Timeout() {
			return nil, ErrTimeout
		} else if r.Terminated() {
			return nil, ErrClusterClosed
		}
		plog.Panicf("unknown code value %v, result %d, client id %d",
			r, r.GetResult(), cs.ClientID)
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return nil, ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
	}
	panic("should never reach here")
}

// CloseSession closes the specified client session by unregistering it
// from the system. This is a synchronous method meaning it will only return
// after its confirmed completion, failure or timeout.
//
// Closed client session should no longer be used in future proposals.
func (nh *NodeHost) CloseSession(ctx context.Context,
	session *client.Session) error {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return err
	}
	session.PrepareForUnregister()
	rs, err := nh.ProposeSession(session, timeout)
	if err != nil {
		return err
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() && r.GetResult().Value == session.ClientID {
			return nil
		} else if r.Rejected() {
			return ErrRejected
		} else if r.Timeout() {
			return ErrTimeout
		} else if r.Terminated() {
			return ErrClusterClosed
		}
		plog.Panicf("unknown v code %v, client id %d",
			r, session.ClientID)
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
	}
	panic("should never reach here")
}

// Propose starts an asynchronous proposal on the Raft cluster specified in the
// Session object. The input byte slice can be reused for other purposes
// immediate after the return of this method.
//
// This method returns a RequestState instance or an error immediately.
// Application can wait on the CompleteC member channel of the returned
// RequestState instance to get notified for the outcome of the proposal and
// access to the result of the proposal.
//
// After the proposal is completed, i.e. RequestResult is received from the
// CompletedC channel of the returned RequestState, unless NO-OP client session
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
	return nh.propose(session, cmd, nil, timeout)
}

// ProposeSession starts an asynchronous proposal on the specified cluster
// for client session related operations. Depending on the state of the client
// session object, the supported operations are for registering or unregistering
// a client session. Application can select on the CompleteC member channel of
// the returned RequestState instance to get notified for the completion and
// result of the proposal.
func (nh *NodeHost) ProposeSession(session *client.Session,
	timeout time.Duration) (*RequestState, error) {
	v, ok := nh.getCluster(session.ClusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !v.supportClientSession() && !session.IsNoOPSession() {
		plog.Panicf("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	req, err := v.proposeSession(session, nil, timeout)
	nh.execEngine.setNodeReady(session.ClusterID)
	return req, err
}

// ReadIndex starts the asynchronous ReadIndex protocol used for linearizable
// read on the specified cluster. This method returns a RequestState instance
// or an error immediately. Application should wait on the CompleteC channel
// of the returned RequestState object to get notified on the outcome of the
// ReadIndex operation. On a successful completion, the ReadLocal method can
// then be invoked to query the state of the IStateMachine or
// IOnDiskStateMachine to complete the read operation with linearizability
// guarantee.
func (nh *NodeHost) ReadIndex(clusterID uint64,
	timeout time.Duration) (*RequestState, error) {
	rs, _, err := nh.readIndex(clusterID, nil, timeout)
	return rs, err
}

// ReadLocal queries the specified Raft node. To ensure the linearizability of
// the I/O, ReadLocal should only be called after receiving a RequestCompleted
// notification from the ReadIndex method.
//
// Deprecated: Applications should use ReadLocalNode instead.
func (nh *NodeHost) ReadLocal(clusterID uint64,
	query []byte) ([]byte, error) {
	v, ok := nh.getClusterNotLocked(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !v.initialized() {
		plog.Panicf("ReadLocal called on %s when not initialized", v.describe())
	}
	// translate the rsm.ErrClusterClosed to ErrClusterClosed
	// internally, the IManagedStateMachine might obtain a RLock before performing
	// the local read. The critical section is used to make sure we don't read
	// from a destroyed C++ StateMachine object
	data, err := v.sm.Lookup(query)
	if err == rsm.ErrClusterClosed {
		return nil, ErrClusterClosed
	}
	return data, err
}

// ReadLocalNode queries the Raft node identified by the input RequestState
// instance. To ensure the IO linearizability, ReadLocalNode should only be
// called after receiving a RequestCompleted notification from the ReadIndex
// method. See ReadIndex's example for more details.
func (nh *NodeHost) ReadLocalNode(rs *RequestState,
	query []byte) ([]byte, error) {
	if rs.node == nil {
		panic("invalid rs")
	}
	if !rs.node.initialized() {
		plog.Panicf("ReadLocalNode called on %s when not initialized",
			rs.node.describe())
	}
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

// RequestSnapshot requests a snapshot to be created for the specified node. For
// each Raft node, only one pending requested snapshot operation is allowed.
//
// This method returns a SnapshotState instance or an error immediately.
// Application can wait on the CompleteC member channel of the returned
// SnapshotState instance to get notified for the outcome of the create snasphot
// operation and access to the result of the operation.
//
// Requested create snapshot operation will be rejected if there is already an
// existing snapshot in the system at the same Raft log index.
//
// Snapshots created as the result of RequestSnapshot are managed by Dragonboat.
// Users are not suppose to move, copy, modify or delete the generated snapshot.
func (nh *NodeHost) RequestSnapshot(clusterID uint64,
	timeout time.Duration) (*SnapshotState, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	req, err := v.requestSnapshot(timeout)
	nh.execEngine.setNodeReady(clusterID)
	return req, err
}

// ExportSnapshot requests a snapshot to be exported into the specified directory.
//
// This method returns a SnapshotState instance or an error immediately.
// Application can wait on the CompleteC member channel of the returned
// SnapshotState instance to get notified for the outcome of the create snasphot
// operation and access to the result of the create snapshot operation.
//
// Requested export snapshot operation will be rejected if there is already an
// existing snapshot in the system at the same index.
//
// Once created, the exported snapshot is owned by the caller, it is caller's
// responsibility to backup or delete the exported snapshot when necessary. The
// exported snapshot is typically used as a backup of the state machine state.
// See Dragonboat's DevOps docs on how to use such exported snapshot.
func (nh *NodeHost) ExportSnapshot(clusterID uint64,
	path string, timeout time.Duration) (*SnapshotState, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	req, err := v.exportSnapshot(path, timeout)
	nh.execEngine.setNodeReady(clusterID)
	return req, err
}

// RequestDeleteNode is a Raft cluster membership change method for requesting
// the specified node to be removed from the specified Raft cluster. It starts
// an asynchronous request to remove the node from the Raft cluster membership
// list. Application can wait on the CompleteC member of the returned
// RequestState instance to get notified for the outcome.
//
// It is not guaranteed that deleted node will automatically close itself and
// be removed from its managing NodeHost instance. It is application's
// responsibility to call RemoveCluster on the right NodeHost instance to
// actually have the cluster node removed from its managing NodeHost instance.
//
// When the raft cluster is created with the OrderedConfigChange config flag
// set as false, the configChangeIndex parameter is ignored. Otherwise, it
// should be set to the most recent Config Change Index value returned by the
// GetClusterMembership method. The requested delete node operation will be
// rejected if other membership change has been applied since the call to
// the GetClusterMembership method.
func (nh *NodeHost) RequestDeleteNode(clusterID uint64,
	nodeID uint64,
	configChangeIndex uint64, timeout time.Duration) (*RequestState, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	req, err := v.requestDeleteNodeWithOrderID(nodeID, configChangeIndex, timeout)
	nh.execEngine.setNodeReady(clusterID)
	return req, err
}

// RequestAddNode is a Raft cluster membership change method for requesting the
// specified node to be added to the specified Raft cluster. It starts an
// asynchronous request to add the node to the Raft cluster membership list.
// Application can wait on the CompleteC member of the returned RequestState
// instance to get notified for the outcome.
//
// If there is already an observer with the same nodeID in the cluster, it will
// be promoted to a regular node with voting power. The address parameter of the
// RequestAddNode call is ignored when promoting an observer to a regular node.
//
// After the node is successfully added to the Raft cluster, it is application's
// responsibility to call StartCluster on the right NodeHost instance to actually
// start the Raft cluster node.
//
// The input address parameter is the RaftAddress of the NodeHost where the new
// Raft node being added will be running. When the raft cluster is created with
// the OrderedConfigChange config flag set as false, the configChangeIndex
// parameter is ignored. Otherwise, it should be set to the most recent Config
// Change Index value returned by the GetClusterMembership method. The requested
// add node operation will be rejected if other membership change has been
// applied since the call to the GetClusterMembership method.
func (nh *NodeHost) RequestAddNode(clusterID uint64,
	nodeID uint64, address string, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	req, err := v.requestAddNodeWithOrderID(nodeID,
		address, configChangeIndex, timeout)
	nh.execEngine.setNodeReady(clusterID)
	return req, err
}

// RequestAddObserver is a Raft cluster membership change method for requesting
// the specified node to be added to the specified Raft cluster as an observer
// without voting power. It starts an asynchronous request to add the specified
// node as an observer.
//
// Such observer is able to receive replicated states from the leader node, but
// it is not allowed to vote for leader, it is not considered as a part of
// the quorum when replicating state. An observer can be promoted to a regular
// node with voting power by making a RequestAddNode call using its clusterID
// and nodeID values. An observer can be removed from the cluster by calling
// RequestDeleteNode with its clusterID and nodeID values.
//
// Application should later call StartCluster with config.Config.IsObserver
// set to true on the right NodeHost to actually start the observer instance.
//
// The input address parameter is the RaftAddress of the NodeHost where the new
// observer being added will be running. When the raft cluster is created with
// the OrderedConfigChange config flag set as false, the configChangeIndex
// parameter is ignored. Otherwise, it should be set to the most recent Config
// Change Index value returned by the GetClusterMembership method. The requested
// add observer operation will be rejected if other membership change has been
// applied since the call to the GetClusterMembership method.
func (nh *NodeHost) RequestAddObserver(clusterID uint64,
	nodeID uint64, address string, configChangeIndex uint64,
	timeout time.Duration) (*RequestState, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	req, err := v.requestAddObserverWithOrderID(nodeID,
		address, configChangeIndex, timeout)
	nh.execEngine.setNodeReady(clusterID)
	return req, err
}

// RequestLeaderTransfer makes a request to transfer the leadership of the
// specified Raft cluster to the target node identified by targetNodeID. It
// returns an error if the request fails to be started. There is no guarantee
// that such request can be fulfilled, i.e. the leadership transfer can still
// fail after a successful return of the RequestLeaderTransfer method.
func (nh *NodeHost) RequestLeaderTransfer(clusterID uint64,
	targetNodeID uint64) error {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	plog.Infof("RequestLeaderTransfer called on cluster %d target nodeid %d",
		clusterID, targetNodeID)
	v.requestLeaderTransfer(targetNodeID)
	return nil
}

// RemoveData removes all data associated with the specified node. This method
// should only be used after the node has been deleted from its Raft cluster.
// Calling RemoveData on a node that is still a Raft cluster member will corrupt
// the Raft cluster.
func (nh *NodeHost) RemoveData(clusterID uint64, nodeID uint64) error {
	plog.Infof("RemoveData called on %s", logutil.DescribeNode(clusterID, nodeID))
	_, ok := nh.getCluster(clusterID)
	if ok {
		return ErrClusterNotStopped
	}
	if err := nh.logdb.RemoveNodeData(clusterID, nodeID); err != nil {
		plog.Panicf("failed to remove data from Raft LogDB %v", err)
	}
	did := nh.deploymentID
	if err := nh.serverCtx.RemoveSnapshotDir(did, clusterID, nodeID); err != nil {
		plog.Panicf("failed to remove snapshot dir %v", err)
	}
	return nil
}

// GetNodeUser returns an INodeUser instance ready to be used to directly make
// proposals or read index operations without locating the node repeatedly in
// the NodeHost. A possible use case is when loading a large data set say with
// billions of proposals into the dragonboat based system.
func (nh *NodeHost) GetNodeUser(clusterID uint64) (INodeUser, error) {
	v, ok := nh.getCluster(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	nu := &nodeUser{
		nh:           nh,
		node:         v,
		setNodeReady: nh.execEngine.setNodeReady,
	}
	return nu, nil
}

// HasNodeInfo returns a boolean value indicating whether the specified node
// has been bootstrapped on the current NodeHost instance.
func (nh *NodeHost) HasNodeInfo(clusterID uint64, nodeID uint64) bool {
	_, err := nh.logdb.GetBootstrapInfo(clusterID, nodeID)
	if err == raftio.ErrNoBootstrapInfo {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}

// GetNodeHostInfo returns a NodeHostInfo instance that contains all details
// of the NodeHost, this includes details of all Raft clusters managed by the
// the NodeHost instance.
func (nh *NodeHost) GetNodeHostInfo() *NodeHostInfo {
	clusterInfoList := nh.getClusterInfo()
	plogInfo, err := nh.logdb.ListNodeInfo()
	if err != nil {
		plog.Panicf("failed to list all logs on logdb %v", err)
	}
	return &NodeHostInfo{
		RaftAddress:     nh.RaftAddress(),
		ClusterInfoList: clusterInfoList,
		LogInfo:         plogInfo,
	}
}

func (nh *NodeHost) propose(s *client.Session,
	cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	var st time.Time
	sampled := delaySampled(s)
	if sampled {
		st = time.Now()
	}
	v, ok := nh.getClusterNotLocked(s.ClusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	if !v.supportClientSession() && !s.IsNoOPSession() {
		plog.Panicf("IOnDiskStateMachine based nodes must use NoOPSession")
	}
	req, err := v.propose(s, cmd, handler, timeout)
	nh.execEngine.setNodeReady(s.ClusterID)
	if sampled {
		nh.execEngine.ProposeDelay(s.ClusterID, st)
	}
	return req, err
}

func (nh *NodeHost) readIndex(clusterID uint64,
	handler ICompleteHandler,
	timeout time.Duration) (*RequestState, *node, error) {
	n, ok := nh.getClusterNotLocked(clusterID)
	if !ok {
		return nil, nil, ErrClusterNotFound
	}
	req, err := n.read(handler, timeout)
	if err != nil {
		return nil, nil, err
	}
	nh.execEngine.setNodeReady(clusterID)
	return req, n, err
}

func (nh *NodeHost) linearizableRead(ctx context.Context,
	clusterID uint64,
	f func(n *node) (interface{}, error)) (interface{}, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, node, err := nh.readIndex(clusterID, nil, timeout)
	if err != nil {
		return nil, err
	}
	select {
	case s := <-rs.CompletedC:
		if s.Timeout() {
			return nil, ErrTimeout
		} else if s.Completed() {
			rs.Release()
			return f(node)
		} else if s.Terminated() {
			return nil, ErrClusterClosed
		}
		panic("unknown completedc code")
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return nil, ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return nil, ErrTimeout
		}
		panic("unknown ctx error")
	}
}

func (nh *NodeHost) getClusterNotLocked(clusterID uint64) (*node, bool) {
	v, ok := nh.clusterMu.clusters.Load(clusterID)
	if !ok {
		return nil, false
	}
	return v.(*node), true
}

func (nh *NodeHost) getClusterAndQueueNotLocked(clusterID uint64) (*node,
	*server.MessageQueue, bool) {
	nh.clusterMu.RLock()
	defer nh.clusterMu.RUnlock()
	v, ok := nh.getClusterNotLocked(clusterID)
	if !ok {
		return nil, nil, false
	}
	q, ok := nh.clusterMu.requests[clusterID]
	if !ok {
		return nil, nil, false
	}
	return v, q, true
}

func (nh *NodeHost) getCluster(clusterID uint64) (*node, bool) {
	nh.clusterMu.RLock()
	v, ok := nh.clusterMu.clusters.Load(clusterID)
	nh.clusterMu.RUnlock()
	if !ok {
		return nil, false
	}
	return v.(*node), true
}

func (nh *NodeHost) forEachClusterRun(bf func() bool,
	af func() bool, f func(uint64, *node) bool) {
	nh.clusterMu.RLock()
	defer nh.clusterMu.RUnlock()
	if bf != nil {
		if !bf() {
			return
		}
	}
	nh.clusterMu.clusters.Range(func(k, v interface{}) bool {
		return f(k.(uint64), v.(*node))
	})
	if af != nil {
		if !af() {
			return
		}
	}
}

func (nh *NodeHost) forEachCluster(f func(uint64, *node) bool) {
	nh.forEachClusterRun(nil, nil, f)
}

// there are two major reasons to bootstrap the cluster
// 1. check whether user is incorrectly specifying the startCluster parameters,
//    e.g. call startCluster with join=true first, restart the NodeHost then
//    call startCluster again with join=false and len(nodes) > 0
// 2. when restarting a node which is a part of the initial cluster members,
//    we should allow the caller not to provide a non-empty nodes with all
//    initial member info in it, but when it is necessary to bootstrap at
//    the raft node level again, we need to get the initial member info from
//    somewhere. bootstrap is the process that records such info
// 3. bootstrap record is used as the node info record in our default Log DB
//    implementation
func (nh *NodeHost) bootstrapCluster(nodes map[uint64]string,
	join bool, config config.Config,
	smType pb.StateMachineType) (map[uint64]string, bool, error) {
	binfo, err := nh.logdb.GetBootstrapInfo(config.ClusterID, config.NodeID)
	// bootstrap the cluster by recording a bootstrap info rec into the LogDB
	if err == raftio.ErrNoBootstrapInfo {
		var members map[uint64]string
		if !join {
			members = nodes
		}
		bootstrap := pb.Bootstrap{
			Join:      join,
			Addresses: make(map[uint64]string),
			Type:      smType,
		}
		for nid, addr := range nodes {
			bootstrap.Addresses[nid] = stringutil.CleanAddress(addr)
		}
		err = nh.logdb.SaveBootstrapInfo(config.ClusterID,
			config.NodeID, bootstrap)
		plog.Infof("bootstrap for %s found node not bootstrapped, %v",
			logutil.DescribeNode(config.ClusterID, config.NodeID), members)
		return members, !join, err
	} else if err != nil {
		return nil, false, err
	}
	if !binfo.Validate(nodes, join, smType) {
		plog.Errorf("bootstrap validation failed for %s, %v, %t, %v, %t",
			logutil.DescribeNode(config.ClusterID, config.NodeID),
			binfo.Addresses, binfo.Join, nodes, join)
		return nil, false, ErrInvalidClusterSettings
	}
	plog.Infof("bootstrap for %s returns %v",
		logutil.DescribeNode(config.ClusterID, config.NodeID), binfo.Addresses)
	return binfo.Addresses, !binfo.Join, nil
}

func (nh *NodeHost) startCluster(nodes map[uint64]string,
	join bool,
	createStateMachine rsm.ManagedStateMachineFactory,
	stopc chan struct{},
	config config.Config,
	smType pb.StateMachineType) error {
	clusterID := config.ClusterID
	nodeID := config.NodeID
	plog.Infof("startCluster called for %s, join %t, nodes %v",
		logutil.DescribeNode(clusterID, nodeID), join, nodes)
	nh.clusterMu.Lock()
	defer nh.clusterMu.Unlock()
	if nh.clusterMu.stopped {
		return ErrSystemStopped
	}
	if _, ok := nh.clusterMu.clusters.Load(clusterID); ok {
		return ErrClusterAlreadyExist
	}
	if join && len(nodes) > 0 {
		plog.Errorf("trying to join %s with initial member list %v",
			logutil.DescribeNode(clusterID, nodeID), nodes)
		return ErrInvalidClusterSettings
	}
	addrs, members, err := nh.bootstrapCluster(nodes, join, config, smType)
	if err == ErrInvalidClusterSettings {
		return ErrInvalidClusterSettings
	}
	if err != nil {
		panic(err)
	}
	plog.Infof("bootstrap for %s returned address list %v",
		logutil.DescribeNode(clusterID, nodeID), addrs)
	queue := server.NewMessageQueue(receiveQueueSize, false, lazyFreeCycle)
	for k, v := range addrs {
		if k != nodeID {
			plog.Infof("AddNode called with node %s, addr %s",
				logutil.DescribeNode(clusterID, k), v)
			nh.nodes.AddNode(clusterID, k, v)
		}
	}
	if _, err := nh.serverCtx.PrepareSnapshotDir(nh.deploymentID,
		clusterID, nodeID); err != nil {
		return err
	}
	getSnapshotDirFunc := func(cid uint64, nid uint64) string {
		return nh.serverCtx.GetSnapshotDir(nh.deploymentID, cid, nid)
	}
	snapshotter := newSnapshotter(clusterID, nodeID,
		getSnapshotDirFunc, nh.logdb, stopc)
	if err := snapshotter.ProcessOrphans(); err != nil {
		panic(err)
	}
	rn := newNode(nh.nhConfig.RaftAddress,
		addrs,
		members,
		snapshotter,
		createStateMachine(clusterID, nodeID, stopc),
		smType,
		nh.execEngine.SetCommitReady,
		nh.asyncSendRaftRequest,
		queue,
		stopc,
		nh.nodes,
		nh.rsPool[nodeID%rsPoolSize],
		config,
		nh.nhConfig.RTTMillisecond,
		nh.logdb)
	nh.clusterMu.clusters.Store(clusterID, rn)
	nh.clusterMu.requests[clusterID] = queue
	nh.clusterMu.csi++
	return nil
}

func (nh *NodeHost) createPools() {
	nh.rsPool = make([]*sync.Pool, rsPoolSize)
	for i := uint64(0); i < rsPoolSize; i++ {
		p := &sync.Pool{}
		p.New = func() interface{} {
			obj := &RequestState{}
			obj.CompletedC = make(chan RequestResult, 1)
			obj.pool = p
			return obj
		}
		nh.rsPool[i] = p
	}
}

func (nh *NodeHost) createLogDB(nhConfig config.NodeHostConfig,
	deploymentID uint64) {
	nhDirs, walDirs := nh.serverCtx.CreateNodeHostDir(deploymentID)
	nh.serverCtx.CheckNodeHostDir(deploymentID, nh.nhConfig.RaftAddress)
	var factory config.LogDBFactoryFunc
	if nhConfig.LogDBFactory != nil {
		factory = nhConfig.LogDBFactory
	} else {
		factory = logdb.OpenLogDB
	}
	logdb, err := factory(nhDirs, walDirs)
	if err != nil {
		panic(err)
	}
	plog.Infof("logdb type name: %s", logdb.Name())
	nh.logdb = logdb
}

func (nh *NodeHost) createTransport() {
	getSnapshotDirFunc := func(cid uint64, nid uint64) string {
		return nh.serverCtx.GetSnapshotDir(nh.deploymentID, cid, nid)
	}
	nh.transport = transport.NewTransport(nh.nhConfig,
		nh.serverCtx, nh.nodes, getSnapshotDirFunc)
	nh.transport.SetMessageHandler(nh.msgHandler)
}

func (nh *NodeHost) stopNode(clusterID uint64,
	nodeID uint64, nodeCheck bool) error {
	nh.clusterMu.Lock()
	defer nh.clusterMu.Unlock()
	v, ok := nh.clusterMu.clusters.Load(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	cluster := v.(*node)
	if nodeCheck && cluster.nodeID != nodeID {
		return ErrClusterNotFound
	}
	nh.clusterMu.clusters.Delete(clusterID)
	delete(nh.clusterMu.requests, clusterID)
	nh.clusterMu.csi++
	cluster.close()
	cluster.notifyOffloaded(rsm.FromNodeHost)
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
	count := uint64(0)
	idx := uint64(0)
	nodes := make([]*node, 0)
	qs := make(map[uint64]*server.MessageQueue)
	tf := func() bool {
		count++
		nh.increaseTick()
		if count%nh.nhConfig.RTTMillisecond == 0 {
			idx, nodes, qs = nh.getCurrentClusters(idx, nodes, qs)
			nh.snapshotStatus.pushReady(nh.getTick())
			nh.sendTickMessage(nodes, qs)
		}
		return false
	}
	lang.RunTicker(time.Millisecond, tf, nh.stopper.ShouldStop(), nil)
}

func (nh *NodeHost) getCurrentClusters(index uint64,
	clusters []*node, queues map[uint64]*server.MessageQueue) (uint64,
	[]*node, map[uint64]*server.MessageQueue) {
	newIndex := nh.getClusterSetIndex()
	if newIndex == index {
		return index, clusters, queues
	}
	newClusters := clusters[:0]
	newQueues := make(map[uint64]*server.MessageQueue)
	nh.forEachCluster(func(cid uint64, node *node) bool {
		newClusters = append(newClusters, node)
		v, ok := nh.clusterMu.requests[cid]
		if !ok {
			panic("inconsistent received messageC map")
		}
		newQueues[cid] = v
		return true
	})
	return newIndex, newClusters, newQueues
}

func (nh *NodeHost) asyncSendRaftRequest(msg pb.Message) {
	if nh.isPartitioned() {
		return
	}
	if msg.Type != pb.InstallSnapshot {
		nh.transport.ASyncSend(msg)
		nh.checkTransportLatency(msg.ClusterId, msg.To, msg.From, msg.Term)
	} else {
		plog.Infof("%s is sending snapshot to %s, index %d, size %d",
			logutil.DescribeNode(msg.ClusterId, msg.From),
			logutil.DescribeNode(msg.ClusterId, msg.To),
			msg.Snapshot.Index, msg.Snapshot.FileSize)
		n, ok := nh.getCluster(msg.ClusterId)
		if !ok {
			return
		}
		if !n.OnDiskStateMachine() {
			nh.transport.ASyncSendSnapshot(msg)
		} else {
			n.publishStreamSnapshotRequest(msg.ClusterId, msg.To)
		}
	}
}

func (nh *NodeHost) sendTickMessage(clusters []*node,
	queues map[uint64]*server.MessageQueue) {
	m := pb.Message{Type: pb.LocalTick}
	for _, n := range clusters {
		q, ok := queues[n.clusterID]
		if !ok || !n.initialized() {
			continue
		}
		q.Add(m)
		nh.execEngine.setNodeReady(n.clusterID)
	}
}

func (nh *NodeHost) checkTransportLatency(clusterID uint64,
	to uint64, from uint64, term uint64) {
	v := atomic.AddUint64(&nh.msgCount, 1)
	if delaySampleRatio > 0 && v%delaySampleRatio == 0 {
		msg := pb.Message{
			Type:      pb.Ping,
			To:        to,
			From:      from,
			ClusterId: clusterID,
			Term:      term,
			Hint:      uint64(time.Now().UnixNano()),
		}
		nh.transport.ASyncSend(msg)
	}
}

func (nh *NodeHost) closeStoppedClusters() {
	chans := make([]<-chan struct{}, 0)
	keys := make([]uint64, 0)
	nodeIDs := make([]uint64, 0)
	nh.forEachCluster(func(cid uint64, node *node) bool {
		chans = append(chans, node.shouldStop())
		keys = append(keys, cid)
		nodeIDs = append(nodeIDs, node.nodeID)
		return true
	})
	if len(chans) == 0 {
		return
	}
	cases := make([]reflect.SelectCase, len(chans)+1)
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	cases[len(chans)] = reflect.SelectCase{Dir: reflect.SelectDefault}
	chosen, _, ok := reflect.Select(cases)
	if !ok && chosen < len(keys) {
		clusterID := keys[chosen]
		nodeID := nodeIDs[chosen]
		if err := nh.StopNode(clusterID, nodeID); err != nil {
			plog.Errorf("failed to remove cluster %d", clusterID)
		}
	}
}

func (nh *NodeHost) nodeMonitorMain(nhConfig config.NodeHostConfig) {
	count := uint64(0)
	tf := func() bool {
		count++
		nh.closeStoppedClusters()
		return false
	}
	lang.RunTicker(monitorInterval, tf, nh.stopper.ShouldStop(), nil)
}

func (nh *NodeHost) pushSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) bool {
	cluster, q, ok := nh.getClusterAndQueueNotLocked(clusterID)
	if ok {
		m := pb.Message{
			Type:   pb.SnapshotStatus,
			From:   nodeID,
			Reject: failed,
		}
		added, stopped := q.Add(m)
		if added {
			nh.execEngine.setNodeReady(clusterID)
			plog.Infof("snapshot status sent to %s",
				logutil.DescribeNode(clusterID, nodeID))
			return true
		}
		if stopped {
			return true
		}
		select {
		case <-nh.stopper.ShouldStop():
			return true
		case <-cluster.shouldStop():
			return true
		default:
			return false
		}
	}
	plog.Warningf("failed to send snapshot status to %s",
		logutil.DescribeNode(clusterID, nodeID))
	return true
}

func (nh *NodeHost) sendNoOPMessage(clusterID uint64, nodeID uint64) {
	batch := pb.MessageBatch{
		Requests: make([]pb.Message, 0),
	}
	msg := pb.Message{
		Type:      pb.NoOP,
		To:        nodeID,
		From:      nodeID,
		ClusterId: clusterID,
	}
	batch.Requests = append(batch.Requests, msg)
	nh.msgHandler.HandleMessageBatch(batch)
}

func (nh *NodeHost) increaseTick() {
	atomic.AddUint64(&nh.tick, 1)
}

func (nh *NodeHost) getTick() uint64 {
	return atomic.LoadUint64(&nh.tick)
}

func (nh *NodeHost) getClusterSetIndex() uint64 {
	nh.clusterMu.RLock()
	v := nh.clusterMu.csi
	nh.clusterMu.RUnlock()
	return v
}

func (nh *NodeHost) describe() string {
	return nh.RaftAddress()
}

func (nh *NodeHost) logNodeHostDetails() {
	if nh.transport != nil {
		plog.Infof("transport type: %s", nh.transport.Name())
	}
	if nh.logdb != nil {
		plog.Infof("logdb type: %s", nh.logdb.Name())
	}
	plog.Infof("nodehost address: %s", nh.nhConfig.RaftAddress)
}

func (nh *NodeHost) logTransportLatency() {
	plog.Infof("transport latency p999 %dμs, p99 %dμs, median %dμs, %d",
		nh.transportLatency.p999(),
		nh.transportLatency.p99(),
		nh.transportLatency.median(), len(nh.transportLatency.samples))
}

// INodeUser is the interface implemented by a Raft node user type. A Raft node
// user can be used to directly initiate proposals or read index operations
// without locating the Raft node in NodeHost's node list first. It is useful
// when doing bulk load operations on selected clusters.
type INodeUser interface {
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

func delaySampled(s *client.Session) bool {
	if delaySampleRatio == 0 {
		return false
	}
	return s.ClientID%delaySampleRatio == 0
}

type nodeUser struct {
	nh           *NodeHost
	node         *node
	setNodeReady func(clusterID uint64)
}

func (nu *nodeUser) Propose(s *client.Session,
	cmd []byte, timeout time.Duration) (*RequestState, error) {
	var st time.Time
	sampled := delaySampled(s)
	if sampled {
		st = time.Now()
	}
	req, err := nu.node.propose(s, cmd, nil, timeout)
	nu.setNodeReady(s.ClusterID)
	if sampled {
		nu.nh.execEngine.ProposeDelay(s.ClusterID, st)
	}
	return req, err
}

func (nu *nodeUser) ReadIndex(timeout time.Duration) (*RequestState, error) {
	rs, err := nu.node.read(nil, timeout)
	return rs, err
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

type messageHandler struct {
	nh *NodeHost
}

func newNodeHostMessageHandler(nh *NodeHost) *messageHandler {
	return &messageHandler{nh: nh}
}

func (h *messageHandler) HandleMessageBatch(msg pb.MessageBatch) {
	nh := h.nh
	mustKeep := false
	if nh.isPartitioned() {
		// InstallSnapshot is a in-memory local message type that will never be
		// dropped in production as it will never be sent via networks
		for _, req := range msg.Requests {
			if req.Type == pb.InstallSnapshot {
				mustKeep = true
			}
		}
		if !mustKeep {
			return
		}
	}
	for _, req := range msg.Requests {
		if req.Type == pb.SnapshotReceived {
			plog.Infof("MsgSnapshotReceived received, cluster id %d, node id %d",
				req.ClusterId, req.From)
			nh.snapshotStatus.confirm(req.ClusterId, req.From, nh.getTick())
			continue
		}
		if req.Type == pb.Ping {
			h.HandlePingMessage(req)
			continue
		}
		if req.Type == pb.Pong {
			h.HandlePongMessage(req)
			continue
		}
		_, q, ok := nh.getClusterAndQueueNotLocked(req.ClusterId)
		if ok {
			if req.Type == pb.InstallSnapshot {
				q.AddSnapshot(req)
			} else {
				if added, stopped := q.Add(req); !added || stopped {
					plog.Warningf("dropped an incoming message")
				}
			}
			nh.execEngine.setNodeReady(req.ClusterId)
		}
	}
}

func (h *messageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	tick := h.nh.getTick()
	h.nh.snapshotStatus.addStatus(clusterID, nodeID, failed, tick)
}

func (h *messageHandler) HandleUnreachable(clusterID uint64,
	nodeID uint64) {
	// this is called from a worker thread that is no longer serving anything
	cluster, q, ok := h.nh.getClusterAndQueueNotLocked(clusterID)
	if ok {
		m := pb.Message{Type: pb.Unreachable, From: nodeID}
		for {
			added, stopped := q.Add(m)
			if added || stopped {
				break
			}
			select {
			case <-h.nh.stopper.ShouldStop():
				return
			case <-cluster.shouldStop():
				return
			default:
			}
			time.Sleep(time.Millisecond)
		}
		h.nh.execEngine.setNodeReady(clusterID)
	}
}

func (h *messageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
	msg := pb.Message{
		To:        from,
		From:      nodeID,
		ClusterId: clusterID,
		Type:      pb.SnapshotReceived,
	}
	plog.Infof("%s is sending MsgSnapshotReceived to %d",
		logutil.DescribeNode(clusterID, nodeID), from)
	h.nh.asyncSendRaftRequest(msg)
}

func (h *messageHandler) HandlePingMessage(msg pb.Message) {
	resp := pb.Message{
		Type:      pb.Pong,
		To:        msg.From,
		From:      msg.To,
		ClusterId: msg.ClusterId,
		Term:      msg.Term,
		Hint:      msg.Hint,
	}
	h.nh.transport.ASyncSend(resp)
}

func (h *messageHandler) HandlePongMessage(msg pb.Message) {
	// not using the monotonic clock here
	// it is probably ok for now as we just want to get a rough idea of the
	// transport latency for manual code analysis/optimization purposes
	ts := h.nh.transportLatency
	startTime := time.Unix(0, int64(msg.Hint))
	ts.record(startTime)
}

func logBuildTagsAndVersion() {
	devstr := "Rel"
	if DEVVersion {
		devstr = "Dev"
	}
	plog.Infof("go version: %s", runtime.Version())
	plog.Infof("dragonboat version: %d.%d.%d (%s), raftlog type: %s, logdb type: %s",
		DragonboatMajor, DragonboatMinor, DragonboatPatch, devstr,
		raft.RaftLogTypeName, logdb.LogDBType)
	plog.Infof("raft entry encoding scheme: %s", pb.RaftEntryEncodingScheme)
	if runtime.GOOS == "darwin" {
		plog.Warningf("Running on darwin, don't use darwin for production purposes")
	}
}
