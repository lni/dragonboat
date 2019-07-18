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

#ifndef BINDING_INCLUDE_DRAGONBOAT_DRAGONBOAT_H_
#define BINDING_INCLUDE_DRAGONBOAT_DRAGONBOAT_H_

#include <map>
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <chrono>
#include <functional>
#include "dragonboat/binding.h"
#include "dragonboat/types.h"
#include "dragonboat/managed.h"

//
// This is the C++11 binding for dragonboat. It is a C++ wrapper for dragonboat
// library in Go. Known issues -
// - The CPP binding is not suitable to be used for benchmarking the performance
//   of dragonboat library, as there are obvious overheads for accessing Go
//   features from C/C++.
// - User applications can not specify what Raft RPC and Log DB implementation
//   to use. The C++ binding always use the built-in TCP based Raft RPC module
//   and the RocksDB/LevelDB based Log DB module.
// - Users can not specify custom logger to use in C++ applications.
//

namespace dragonboat {

using Byte = unsigned char;
// NodeID is the id of each Raft node within the Raft cluster.
using NodeID = uint64_t;
// ClusterID is the id of each Raft group.
using ClusterID = uint64_t;
// UpdateResult is the result code returned by the Update method of the
// replicated state machine.
using UpdateResult = uint64_t;
// SnapshotResultIndex is the result index of the SnapshotResult included in
// SnapshotState
using SnapshotResultIndex = uint64_t;
// Milliseconds is the number of milliseconds usually used for timeout settings
using Milliseconds = std::chrono::milliseconds;

// Logger is the class used to set log levels. Logs in dragonboat are
// structured into multiple packages, you can set different log levels for those
// packages.
class Logger
{
 public:
  // SetLogLevel sets log level for the specified package
  static bool SetLogLevel(std::string packageName, int level) noexcept;
  // supported log levels
  static const int LOG_LEVEL_CRITICAL;
  static const int LOG_LEVEL_ERROR;
  static const int LOG_LEVEL_WARNING;
  static const int LOG_LEVEL_NOTICE;
  static const int LOG_LEVEL_INFO;
  static const int LOG_LEVEL_DEBUG;
  static const int LOG_LEVEL_TRACE;
  // supported package names
  static const char Multiraft[];
  static const char Raft[];
  static const char LogDB[];
  static const char RSM[];
  static const char Transport[];
};

// Config is the configuration for Raft nodes. The Config class is provided as
// the C++ equivalent of the Config struct in github.com/lni/dragonboat/config,
// see the documentation of the Config struct in Go for more details.
class Config
{
 public:
  Config(ClusterID clusterId, NodeID nodeId) noexcept;
  // NodeID is a non-zero uint64 value uniquely identifying a node within a
  // raft cluster. It is user's responsibility to ensure that the value is
  // unqiue within the cluster.
  NodeID NodeId;
  // ClusterID is the unique value used to identify a cluster.
  ClusterID ClusterId;
  // Whether this is an observer.
  bool IsObserver;
  // CheckQuorum specifies whether leader should check quorum status and step
  // down to become a follower when it no longer has quorum.
  bool CheckQuorum;
  // Quiesce specifies whether to use let the cluster enter quiesce mode when
  // there is no cluster activity.
  bool Quiesce;
  // ElectionRTT is the minimum number of tick() invocations between elections.
  uint64_t ElectionRTT;
  // HeartbeatRTT is the number of tick() invocations between heartbeats.
  uint64_t HeartbeatRTT;
  // SnapshotEntries defines how often state machines should be snapshotted. It
  // is defined in terms of number of applied entries.
  uint64_t SnapshotEntries;
  // CompactionOverhead defines the number of entries to keep after compaction.
  uint64_t CompactionOverhead;
  // OrderedConfigChange determines whether Raft membership change is enforced
  // with ordered config change ID.
  bool OrderedConfigChange;
  // MaxInMemLogSize is the maximum bytes size of Raft logs that can be stored
  // in memory.
  uint64_t MaxInMemLogSize;
  // SnapshotCompressionType is the type of the compression used by snapshot.
  CompressionType SnapshotCompressionType;
};

// NodeHostConfig is the configuration for NodeHost. The NodeHostConfig class
// is provided as the C++ equivalent of the NodeHostConfig struct in the
// github.com/lni/dragonboat/config package. See the documentation of the
// NodeHostConfig struct in Go for more details.
class NodeHostConfig
{
 public:
  NodeHostConfig(std::string WALDir, std::string NodeHostDir) noexcept;
  // DeploymentID is used to determine whether two nodehost instances are
  // allowed to communicate with each other, only those with the same
  // deployment ID value are allowed to communicate. This ensures that
  // accidentally misconfigured nodehost instances can not cause data
  // corruption errors by sending messages to unrelated nodehosts.
  // In a typical environment, for a particular application that uses
  // dragonboat, you are expected to set DeploymentID to the same uint64
  // value on all your production nodehost instances, then use different
  // DeploymentID on your staging and dev environment. It is also a good
  // practice to use different DeploymentID values for different dragonboat
  // based applications.
  uint64_t DeploymentID;
  // WALDir is the directory used for storing the WAL of Raft logs. It is
  // recommended to use low latency storage such as NVME SSD with power loss
  // protection to store such WAL data.
  std::string WALDir;
  // NodeHostDir is where everything else is stored.
  std::string NodeHostDir;
  // RTTMillisecond defines how many milliseconds in each logical tick.
  Milliseconds RTTMillisecond;
  // RaftAddress is the address used for exchanging Raft messages.
  std::string RaftAddress;
  // ListenAddress is used by the Raft RPC module to listen on for Raft messages.
  std::string ListenAddress;
  // MutualTLS defines whether to use mutual TLS for authenticating servers
  // and clients. Insecure communication is used when MutualTLS is set to
  // false.
  bool MutualTLS;
  // CAFile is the path of the CA file.
  std::string CAFile;
  // CertFile is the path of the node certificate.
  std::string CertFile;
  // KeyFile is the path of the node key file.
  std::string KeyFile;
  // MaxSendQueueSize is the maximum size in bytes of each send queue.
  uint64_t MaxSendQueueSize;
  // MaxReceiveQueueSize is the maximum size in bytes of each receive queue.
  uint64_t MaxReceiveQueueSize;
  // EnableMetrics determines whether health metrics in Prometheus format should
  // be enabled.
  bool EnableMetrics;
  // RaftEventListener is the listener for Raft events exposed to user space.
  // NodeHost uses a single dedicated goroutine to invoke all RaftEventListener
  // methods one by one, CPU intensive or IO related procedures that can cause
  // long delays should be offloaded to worker goroutines managed by users.
  std::function<void(LeaderInfo)> RaftEventListener;
};

// SnapshotOption is the options users can specify when requesting a snapshot
// to be generated.
class SnapshotOption
{
  public:
  SnapshotOption(bool Exported = false, std::string ExportedPath = "") noexcept;
  // Exported is a boolean flag indicating whether the snapshot requested to
  // be generated should be exported. For an exported snapshot, it is users'
  // responsibility to manage the snapshot files. By default, a requested
  // snapshot is not considered as exported, such a regular snapshot is managed
  // the system.
  bool Exported;
  // ExportPath is the path where the exported snapshot should be stored, it
  // must point to an existing directory for which the current user has write
  // permission to it.
  std::string ExportedPath;
};

// Status is the status returned by various methods for indicating operation
// results.
class Status
{
 public:
  Status() noexcept;
  explicit Status(int code) noexcept;
  // Code returns the status code reported by the requested operation.
  // Supported codes can be found in dragonboat/binding.h
  int Code() const noexcept;
  // OK returns a boolean value indicating whether the operation completed
  // successfully.
  bool OK() const noexcept;
  // get string representation of the error code
  std::string String() const noexcept;
  // Possible Code() values.
  static const int StatusOK;
  static const int ErrClusterNotFound;
  static const int ErrClusterAlreadyExist;
  static const int ErrDeadlineNotSet;
  static const int ErrInvalidDeadline;
  static const int ErrInvalidSession;
  static const int ErrTimeoutTooSmall;
  static const int ErrPayloadTooBig;
  static const int ErrSystemBusy;
  static const int ErrClusterClosed;
  static const int ErrBadKey;
  static const int ErrPendingConfigChangeExist;
  static const int ErrTimeout;
  static const int ErrSystemStopped;
  static const int ErrCanceled;
  static const int ErrResultBufferTooSmall;
  static const int ErrRejected;
  static const int ErrInvalidClusterSettings;
  static const int ErrClusterNotReady;
  static const int ErrClusterNotStopped;
  static const int ErrClusterNotInitialized;
  static const int ErrNodeRemoved;
  static const int ErrDirNotExist;
 private:
  int code_;
};

// Peers maintains the node addresses and IDs of all nodes when adding a
// cluster to the NodeHost.
class Peers
{
 public:
  // AddMember adds a node to the collection.
  void AddMember(std::string address, NodeID nodeID) noexcept;
  // Len returns the number of added nodes.
  size_t Len() const noexcept;
  // GetMembership returns all added nodes.
  std::map<std::string, NodeID> GetMembership() const noexcept;
 private:
  std::map<std::string, NodeID> members_;
};

// Buffer is the data buffer used for I/O related requests.
class Buffer
{
 public:
  // Make a buffer with length n.
  explicit Buffer(size_t n) noexcept;
  // Copies the first n bytes from the data array to the Buffer instance.
  Buffer(const Byte *data, size_t n) noexcept;
  // Data returns the pointer pointing to the data buffer. Caller does not own
  // the returned pointer.
  const Byte *Data() const noexcept;
  // Len returns the number of bytes used in the buffer.
  size_t Len() const noexcept;
  // Capacity returns the number of bytes available in the data buffer.
  size_t Capacity() const noexcept;
  // SetLen sets the number of bytes used in the buffer.
  void SetLen(size_t len) noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(Buffer);
  std::vector<Byte> data_;
  size_t len_;
};

// LeaderID is the class used to represent leader information obtained based
// on node's local knowledge. As it is purely based on node's local knowledge,
// there is no guarantee that the leader information is current.
class LeaderID
{
 public:
  LeaderID() noexcept;
  // GetLeaderID returns the leader node ID.
  NodeID GetLeaderID() const noexcept;
  // HasLeaderInfo returns a boolean flag indicating whether the leader node ID
  // info is available to the local node.
  bool HasLeaderInfo() const noexcept;
 private:
  void SetLeaderID(NodeID leaderID, bool hasLeaderInfo) noexcept;
  NodeID nodeID_;
  bool hasLeaderInfo_;
  friend class NodeHost;
};

// Session is the C++ wrapper of the Go client session struct provided by the
// github.com/lni/dragonboat/client package.
// Client session is required for making proposals on the specified cluster.
// The most important usage of a client session is to avoid having a proposal
// to be committed and applied twice. Consider the following situation -
// let's say a client retries a proposal which was previously timed out, it
// is possible that the proposal has actually been committed and applied
// already. Re-proposing the same entry is going to have it committed and
// applied twice. To avoid that, a client session object is used by each
// client to keep tracking proposals made on the selected cluster. By doing
// this, client invokes ProposalCompleted() before making anyh new proposal,
// the internal ID maintained by the client session instance can thus be used
// by the dragonboat library to check whether the proposal has already been
// applied into the StateMachine.
// Raft thesis section 6.3 contains more detailed description on client
// session.
class Session : public ManagedObject
{
 public:
  ~Session();
  // ProposalCompleted marks the current proposal as completed, this makes
  // the Session instance ready to be used for the next proposal.
  void ProposalCompleted() noexcept;
  void PrepareForRegistration() noexcept;
  void PrepareForUnregistration() noexcept;
  void PrepareForProposal() noexcept;
  static Session *GetNewSession(ClusterID clusterID) noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(Session);
  explicit Session(oid_t oid) noexcept;
  bool GetProposalCompleted() const noexcept;
  bool GetReadyForRegistration() const noexcept;
  bool GetReadyForUnregistration() const noexcept;
  bool GetPreparedForProposal() const noexcept;
  void ClearProposalCompleted() noexcept;
  void ClearReadyForRegistration() noexcept;
  void ClearReadyForUnregistration() noexcept;
  void ClearPrepareForProposal() noexcept;
  bool proposalCompleted_;
  bool readyForRegistration_;
  bool readyForUnregistration_;
  bool prepareForProposal_;
  friend class NodeHost;
};


// Entry is the wrapper of dragonboat sm.Entry used to update the state machine
// Entry::result should be set as the result of the update operation.
struct Entry {
  Entry(uint64_t index, const Byte *cmd, size_t cmdLen, uint64_t &result)
    : index(index), cmd(cmd), cmdLen(cmdLen), result(result) {}
  // the index used to indicate the index of the raft entry.
  uint64_t index;
  const Byte *cmd;
  size_t cmdLen;
  // the result store the result of the update operation.
  uint64_t &result;
};

// RequestResult is the result returned to client to indicate the complete state
// and actual result of the request.
struct RequestResult
{
  // the ResultCode used to indicate the complete state.
  ResultCode code;
  // for proposals, this is the result value returned by the Update method of
  // your data store.
  // for snapshots, this is the result index value of the saved snapshot
  uint64_t result;
};

using RequestResult = struct RequestResult;

// Event is the base class used for passing complete notification from
// dragonboat library to client applications. Dragonboat invokes the Set method
// when the proposal or read index operation is completed, client applications
// can choose to use mechanisms such as condition variable or user event to
// wait for the event instance to be set. When wating for the event to be set,
// it is up to the application on how yield its thread to other clients to
// maximize throughput.
class Event
{
 public:
  Event() noexcept {}
  virtual ~Event() {}
  void Set(int code, uint64_t result) noexcept
  {
    result_.code = static_cast<ResultCode>(code);
    result_.result = result;
    set();
  };
  RequestResult Get() const noexcept { return result_; };
 protected:
  virtual void set() noexcept = 0;
 private:
  RequestResult result_;
  DISALLOW_COPY_MOVE_AND_ASSIGN(Event);
};

class RequestState : public ManagedObject
{
 public:
  ~RequestState();
  RequestResult Get() noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(RequestState);
  explicit RequestState(oid_t oid) noexcept;
  bool hasResult_;
  RequestResult result_;
  friend class NodeHost;
};


// IOService is a wrapper of the IO service class typically seen in async
// frameworks, e.g. io_service in boost's asio.
class IOService
{
 public:
  IOService() {}
  virtual ~IOService() {}
  void Run() { run(); }
 protected:
  virtual void run() noexcept = 0;
};

class ClusterInfo
{
 public:
  // ClusterID is the cluster ID of the Raft cluster node.
  uint64_t ClusterID;
  // NodeID is the node ID of the Raft cluster node.
  uint64_t NodeID;
  // IsLeader indicates whether this is a leader node.
  bool IsLeader;
  // IsObserver indicates whether this is a non-voting observer node.
  bool IsObserver;
  // StateMachineType is the type of the state machine.
  StateMachineType SMType;
  // Nodes is a map of member node IDs to their Raft addresses.
  std::map<uint64_t, std::string> Nodes;
  // ConfigChangeIndex is the current config change index of the Raft node.
  // ConfigChangeIndex is Raft Log index of the last applied membership
  // change entry.
  uint64_t ConfigChangeIndex;
  // Pending is a boolean flag indicating whether details of the cluster node
  // is not available. The Pending flag is set to true usually because the node
  // has not had anything applied yet.
  bool Pending;
};

class NodeInfo
{
 public:
  NodeInfo(uint64_t clusterID, uint64_t nodeID)
    : ClusterID(clusterID), NodeID(nodeID) {}
  uint64_t ClusterID;
  uint64_t NodeID;
};

// NodeHostInfo provides info about the NodeHost, including its managed Raft
// cluster nodes and available Raft logs saved in its local persistent storage.
class NodeHostInfo
{
 public:
  // RaftAddress is the public address and the identifier of the NodeHost.
  std::string RaftAddress;
  // ClusterInfo is a list of all Raft clusters managed by the NodeHost
  std::vector<ClusterInfo> ClusterInfoList;
  // LogInfo is a list of raftio.NodeInfo values representing all Raft logs
  // stored on the NodeHost.
  std::vector<NodeInfo> LogInfo;
};

// NodeHostInfoOption is the option type used when querying NodeHostInfo.
class NodeHostInfoOption
{
 public:
  NodeHostInfoOption() : SkipLogInfo(false) {}
  explicit NodeHostInfoOption(bool skipLogInfo) : SkipLogInfo(skipLogInfo) {}
  bool SkipLogInfo;
};

class RegularStateMachine;
class ConcurrentStateMachine;
class OnDiskStateMachine;
// NodeHost is the C++ wrapper of the Go NodeHost struct provided by the
// github.com/lni/dragonboat/multiraft package.
class NodeHost : public ManagedObject
{
 public:
  explicit NodeHost(const NodeHostConfig &config) noexcept;
  ~NodeHost();
  // GetNodeHostConfig returns the NodeHostConfig instance used for configuring
  // this NodeHost instance.
  NodeHostConfig GetNodeHostConfig() const noexcept;
  // Stop stops this NodeHost instance including all its managed clusters.
  void Stop() noexcept;
  // StartCluster adds the specified Raft cluster node to the NodeHost and
  // starts the node to make it ready for the incoming user requests. The
  // replicas is a map of node ID to node RaftAddress values used to
  // indicate what are the initial nodes when a Raft cluster is first created.
  // The join flag indicating whether the node being added is a new node joining
  // the cluster. config is the configuration object that will be passed to the
  // underlying raft node object, The cluster ID and node ID values of the node
  // being added is specified in the config object.
  // Note that this is not the membership change API to add new node to the Raft
  // cluster.
  // As a summary, when -
  //  - starting a Raft cluster with initial members nodes, set join to false
  //    and specify all initial member node details in the replicas instance
  //  - restarting an crashed or stopped node, set join to false. the content of
  //    the intialPeers instance will be ignored by the system
  //  - joining a new node to an existing Raft cluster, set join to true and
  //    leave replicas empty

  // pluginFile is the path of the state machine plugin .so to use.
  // factoryName is the function name of the exported C function in the .so to
  // create state machine
  // smType is the state machine type, see binding.h::StateMachineType
  // For more details, see statemachine/regular.h.
  Status StartCluster(const Peers& replicas, bool join,
    std::string pluginFile, std::string factoryName, StateMachineType smType,
    Config config) noexcept;
  Status StartCluster(const Peers& replicas, bool join,
    std::function<RegularStateMachine*(uint64_t, uint64_t)> factory,
    Config config) noexcept;
  Status StartCluster(const Peers& replicas, bool join,
    std::function<ConcurrentStateMachine*(uint64_t, uint64_t)> factory,
    Config config) noexcept;
  Status StartCluster(const Peers& replicas, bool join,
    std::function<OnDiskStateMachine*(uint64_t, uint64_t)> factory,
    Config config) noexcept;
  // StopCluster removes the specified cluster node from NodeHost. Note that
  // StopCluster makes the specified node no longer managed by the NodeHost
  // instance, it won't do any membership change to the raft cluster itself.
  Status StopCluster(ClusterID clusterID) noexcept;
  // StopNode removes the specified cluster node from NodeHost. Note that
  // StopNode makes the specified node no longer managed by the NodeHost
  // instance, it won't do any membership change to the raft cluster itself.
  Status StopNode(ClusterID clusterID, NodeID nodeID) noexcept;
  // GetNoOPSession returns a NOOP client session ready to be used for
  // making proposals. The NOOP client session is a dummy client session that
  // will not be enforced. The returned client Session instance is owned by the
  // caller. The NoOP client session is not required to be closed.
  Session *GetNoOPSession(ClusterID clusterID) noexcept;
  // SyncGetSession returns a new client Session instance for the specified
  // cluster and the returned client Session instance is owned by the caller.
  // On success, status::OK() will be true and a new Session instance is
  // returned. On failure, status::Code() carries the error code and nullptr is
  // returned.
  Session *SyncGetSession(ClusterID clusterID,
    Milliseconds timeout, Status *status) noexcept;
  // CloseSession closes a previously created client Session instance.
  // NoOP client session obtained from the GetNoOPSession method can not
  // be closed.
  Status SyncCloseSession(const Session &session,
    Milliseconds timeout) noexcept;
  // SyncPropose makes a synchronous proposal on the cluster specified by the
  // input client session object. Once the proposal is committed and
  // successfully applied to the replicated state machine of the local node,
  // the result value returned by StateMachine's Update method is set to the
  // input result parameter and the returned Status instance will have its OK()
  // method return true. On failure, such as timeout, the status will carry
  // the error code and the result input parameter will not be updated.
  Status SyncPropose(Session *session,
    const Buffer &buf, Milliseconds timeout, UpdateResult *result) noexcept;
  Status SyncPropose(Session *session,
    const Byte *buf, size_t buflen,
    Milliseconds timeout, UpdateResult *result) noexcept;
  // SyncRead performs a synchronous linearizable read on the specified raft
  // cluster. The query buffer contains the data buffer to be received by the
  // lookup method of the StateMachine instance, the query result will be
  // written into the result buffer. It is caller's responsibility to provide a
  // big enough result buffer, or error code indicating the result buffer is too
  // smaller will be returned.
  Status SyncRead(ClusterID clusterID,
    const Buffer &query, Buffer *result, Milliseconds timeout) noexcept;
  Status SyncRead(ClusterID clusterID,
    const Byte *query, size_t queryLen,
    Byte *result, size_t resultLen, size_t *written,
    Milliseconds timeout) noexcept;
  // Proposes starts an asynchronous proposal on the cluster specified by the
  // input client session object. The input event instance will be set by the
  // dragonboat library once the proposal is completed, client applications
  // can then get the result from the event instance to check the outcome of
  // the proposal. The returned Status instance indicates whether the requested
  // asynchronous proposal is successfully launched, it is not guaranteed that
  // a successfully launched proposal can be successfully completed and applied.
  Status Propose(Session *session,
    const Buffer &buf, Milliseconds timeout, Event *event) noexcept;
  Status Propose(Session *session,
    const Byte *buf, size_t buflen,
    Milliseconds timeout, Event *event) noexcept;
  // ReadIndex starts an asynchronous ReadIndex operation required for making
  // linearizable read. The input event will be set with RequestCompleted once
  // the ReadIndex operation is completed successfully. Client application can
  // invoke ReadLocal after that to complete the linearizable read. The returned
  // Status instance indicates whether the requested ReadIndex is successfully
  // launched, there is no guarantee that a successfully launched ReadIndex
  // operation can always be successfully completed.
  Status ReadIndex(ClusterID clusterID, Milliseconds timeout,
    Event *event) noexcept;
  // ReadLocal performs a direct read on the specified raft cluster's
  // StateMachine instance. Note that this method is suppose to be used together
  // with the ReadIndex method, ReadLocal is only suppose to be invoked after a
  // successful completion of ReadIndex to ensure the read is linearizable.
  Status ReadLocal(ClusterID clusterID, const Buffer &query,
    Buffer *result) noexcept;
  Status ReadLocal(ClusterID clusterID,
    const Byte *query, size_t queryLen,
    Byte *result, size_t resultLen, size_t *written) noexcept;
  // StaleRead performs a direct read on the specified raft cluster's
  // StateMachine instance without linearizability guarantee. Note that this
  // method is not required to be invoked after ReadIndex.
  Status StaleRead(ClusterID clusterID, const Buffer &query,
    Buffer *result) noexcept;
  Status StaleRead(ClusterID clusterID,
    const Byte *query, size_t queryLen,
    Byte *result, size_t resultLen, size_t *written) noexcept;
  // SyncRequestSnapshot makes a synchronous request on the specified cluster to
  // generate a snapshot. Once the request is done and a snapshot is
  // successfully generated, the result index of the snapshot is set to the
  // input result parameter and the returned Status instance will have its OK()
  // method return true. On failure, such as timeout, the status will carry
  // the error code and the result input parameter will not be updated.
  Status SyncRequestSnapshot(ClusterID clusterID, SnapshotOption opt,
    Milliseconds timeout, SnapshotResultIndex *result) noexcept;
  // RequestSnapshot makes an asynchronous request on the specified cluster to
  // generate a snapshot. This method returns immediately with the input status
  // parameter set. The status indicates whether the requested is successfully
  // launched, it is not guaranteed that a successfully launched RequestSnapshot
  // operation can always have snapshot generated.
  // The returned RequestState is owned by the caller. On successful launch,
  // the status will have its OK() method return true, and the Get() method of
  // the returned RequestState can then be invoked to wait for the result.
  // On failed launch, the status will carry the error code and nullptr is
  // returned.
  RequestState *RequestSnapshot(ClusterID clusterID, SnapshotOption opt,
    Milliseconds timeout, Status *status) noexcept;
  // ProposeSession makes an asynchronous proposal on the specified cluster
  // for client session related operation. Depending on the state of the client
  // session object, the supported operations are for registering or
  // unregistering a client session. A new client session for a specified
  // raft cluster need to be registered before using it for making proposals.
  // Once done and no longer required, the client session need to be
  // unregistered.
  Status ProposeSession(Session *session,
    Milliseconds timeout, Event *event) noexcept;
  // SyncRequestAddNode makes a synchronous proposal to make a raft membership
  // change to add a new node to the specified raft cluster. If there is already
  // an observer with the same NodeID in the cluster, it will be promoted to a
  // regular node with voting power. After the node is successfully added to
  // the Raft cluster, it is application's responsibility to call StartCluster
  // on the right NodeHost instance to actually start the cluster node.
  Status SyncRequestAddNode(ClusterID clusterID, NodeID nodeID,
    std::string address, Milliseconds timeout) noexcept;
  // RequestAddNode is an asynchronous version of SyncRequestAddNode. This
  // method returns immediately with the input status parameter set. The status
  // indicates whether the requested is successfully launched, it is not
  // guaranteed that a successfully launched RequestAddNode operation can always
  // have a node added. The returned RequestState is owned by the caller. On
  // successful launch, the status will have its OK() method return true, and
  // the Get() method of the returned RequestState can then be invoked to wait
  // for the result. On failed launch, the status will carry the error code and
  // nullptr is returned.
  RequestState *RequestAddNode(ClusterID clusterID, NodeID nodeID,
    std::string address, Milliseconds timeout, Status *status) noexcept;
  // SyncRemoveNode makes a synchronous proposal to make a raft membership
  // change to remove the specified node or observer from the requested cluster.
  // It is not guaranteed that removed nodes will automatically close itself
  // and be removed from its managing NodeHost instance. It is application's
  // responsibility to call StopCluster on the right nodehost instance to
  // actually have the cluster node removed from the managing nodehost.
  Status SyncRequestDeleteNode(ClusterID clusterID, NodeID nodeID,
    Milliseconds timeout) noexcept;
  // RequestDeleteNode is an asynchronous version of SyncRequestDeleteNode. This
  // method returns immediately with the input status parameter set. The status
  // indicates whether the requested is successfully launched, it is not
  // guaranteed that a successfully launched RequestDeleteNode operation can
  // always have a node deleted. The returned RequestState is owned by the
  // caller. On successful launch, the status will have its OK() method return
  // true, and the Get() method of the returned RequestState can then be invoked
  // to wait for the result. On failed launch, the status will carry the error
  // code and nullptr is returned.
  RequestState *RequestDeleteNode(ClusterID clusterID, NodeID nodeID,
    Milliseconds timeout, Status *status) noexcept;
  // SyncRequestAddObserver makes a synchronous proposal to make a raft
  // membership change to add a new observer to the specified raft cluster. An
  // observer is able to get replicated entries from the leader, but it is not
  // allowed to vote for leaders, it will not be consider as a part of the
  // quorum when making proposals. After the observer is successfully added to
  // the Raft cluster, it is application's responsibility to call StartCluster
  // on the right NodeHost instance to actually start the observer instance. An
  // observer can be promoted to a regular node with voting power by calling
  // AddNode on the same NodeID.
  Status SyncRequestAddObserver(ClusterID clusterID, NodeID nodeID,
    std::string address, Milliseconds timeout) noexcept;
  // RequestAddObserver is an asynchronous version of SyncRequestAddObserver.
  // This method returns immediately with the input status parameter set. The
  // status indicates whether the requested is successfully launched, it is not
  // guaranteed that a successfully launched RequestAddObserver operation can
  // always have a observer added. The returned RequestState is owned by the
  // caller. On successful launch, the status will have its OK() method return
  // true, and the Get() method of the returned RequestState can then be invoked
  // to wait for the result. On failed launch, the status will carry the error
  // code and nullptr is returned.
  RequestState *RequestAddObserver(ClusterID clusterID, NodeID nodeID,
    std::string address, Milliseconds timeout, Status *status) noexcept;
  // RequestLeaderTransfer requests to transfer leadership to the specified node
  // on the specified cluster. When returned Status instance has its OK() method
  // equals to true, it indicates that the request is successfully submitted but
  // there is no guarantee that the request will be fulfilled.
  Status RequestLeaderTransfer(ClusterID clusterID,
    NodeID targetNodeID) noexcept;
  // GetClusterMembership gets the membership information for the specified
  // cluster. The returned membership information is guaranteed to be
  // linearizable.
  Status GetClusterMembership(ClusterID clusterID,
    Milliseconds timeout, Peers *p) noexcept;
  // GetLeaderID gets the leader ID of the specified cluster based on local
  // node's current knowledge.
  Status GetLeaderID(ClusterID clusterID, LeaderID *leaderID) noexcept;
  // RemoveData tries to remove all data associated with the specified node.
  // This method should only be used after the node has been deleted from its
  // Raft cluster.
  Status SyncRemoveData(ClusterID clusterID, NodeID nodeID,
    Milliseconds timeout) noexcept;
  Status RemoveData(ClusterID clusterID, NodeID nodeID) noexcept;
  // HasNodeInfo returns a boolean value indicating whether the specified node
  // has been bootstrapped on the current NodeHost instance.
  bool HasNodeInfo(ClusterID clusterID, NodeID nodeID) noexcept;
  // GetNodeHostInfo returns a NodeHostInfo instance that contains all details
  // of the NodeHost, this includes details of all Raft clusters managed by the
  // the NodeHost instance.
  NodeHostInfo GetNodeHostInfo(NodeHostInfoOption option) noexcept;
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(NodeHost);
  NodeHostConfig config_;
};

class IOServiceHandler;

// RunIOServiceInGoRuntime runs IOService on Go's managed thread.
IOServiceHandler *RunIOServiceInGoRuntime(IOService* iosp,
  size_t threadCount) noexcept;

// Class IOServiceHandler is the proxy class for Go threads used for running
// the IOService instance.
class IOServiceHandler : public ManagedObject
{
 public:
  ~IOServiceHandler();
  // Join blocks until all Go threads used for running IOService join. It is
  // up to other user mechanisms to notify the IOService to stop.
  void Join() noexcept;
 private:
  explicit IOServiceHandler(oid_t oid) noexcept;
  DISALLOW_COPY_MOVE_AND_ASSIGN(IOServiceHandler);
  friend IOServiceHandler *RunIOServiceInGoRuntime(IOService* iosp,
    size_t threadCount) noexcept;
};

}  // namespace dragonboat

#endif  // BINDING_INCLUDE_DRAGONBOAT_DRAGONBOAT_H_
