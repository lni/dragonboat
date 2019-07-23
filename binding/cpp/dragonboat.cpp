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

#include <string>
#include <map>
#include <cstring>
#include <memory>
#include <assert.h>
#include "dragonboat/binding.h"
#include "dragonboat/dragonboat.h"

namespace dragonboat {

void FreeMembership(Membership *m)
{
  if (m != NULL)
  {
    delete[] m->nodeIDList;
    delete[] m->nodeAddressList;
    delete m;
  }
}

::DBString toDBString(const std::string &val)
{
  ::DBString r;
  r.str = const_cast<char*>(val.data());
  r.len = val.size();
  return r;
}

bool Logger::SetLogLevel(std::string packageName, int level) noexcept
{
  return CSetLogLevel(toDBString(packageName), level) >= 0;
}

const int Logger::LOG_LEVEL_CRITICAL = ::LOG_LEVEL_CRITICAL;
const int Logger::LOG_LEVEL_ERROR = ::LOG_LEVEL_ERROR;
const int Logger::LOG_LEVEL_WARNING = ::LOG_LEVEL_WARNING;
const int Logger::LOG_LEVEL_INFO = ::LOG_LEVEL_INFO;
const int Logger::LOG_LEVEL_DEBUG = ::LOG_LEVEL_DEBUG;
const char Logger::Multiraft[] = "multiraft";
const char Logger::Raft[] = "raft";
const char Logger::LogDB[] = "logdb";
const char Logger::RSM[] = "rsm";
const char Logger::Transport[] = "transport";

Config::Config(ClusterID clusterId, NodeID nodeId) noexcept
  : NodeId(nodeId), ClusterId(clusterId), IsObserver(false), CheckQuorum(false),
    Quiesce(false), ElectionRTT(10), HeartbeatRTT(1), SnapshotEntries(0),
    CompactionOverhead(0), OrderedConfigChange(false), MaxInMemLogSize(0),
    SnapshotCompressionType(NO_COMPRESSION),
    EntryCompressionType(NO_COMPRESSION)
{
}

void parseConfig(const Config &config, ::RaftConfig &cfg) noexcept
{
  cfg.NodeID = config.NodeId;
  cfg.ClusterID = config.ClusterId;
  cfg.IsObserver = config.IsObserver;
  cfg.CheckQuorum = config.CheckQuorum;
  cfg.Quiesce = config.Quiesce;
  cfg.ElectionRTT = config.ElectionRTT;
  cfg.HeartbeatRTT = config.HeartbeatRTT;
  cfg.SnapshotEntries = config.SnapshotEntries;
  cfg.CompactionOverhead = config.CompactionOverhead;
  cfg.OrderedConfigChange = config.OrderedConfigChange;
  cfg.MaxInMemLogSize = config.MaxInMemLogSize;
  cfg.SnapshotCompressionType = config.SnapshotCompressionType;
  cfg.EntryCompressionType = config.EntryCompressionType;
}

NodeHostConfig::NodeHostConfig(std::string WALDir,
  std::string NodeHostDir) noexcept
  : DeploymentID(1), WALDir(WALDir), NodeHostDir(NodeHostDir),
  RTTMillisecond(200), RaftAddress("localhost:9050"),
  MutualTLS(false), CAFile(""), CertFile(""), KeyFile(""),
  MaxSendQueueSize(0), MaxReceiveQueueSize(0), EnableMetrics(false),
  RaftEventListener()
{
}

SnapshotOption::SnapshotOption(bool Exported, std::string ExportedPath) noexcept
  :  Exported(Exported), ExportedPath(ExportedPath)
{
}

Status::Status() noexcept
  : code_(StatusOK)
{
}

Status::Status(int code) noexcept
  : code_(code)
{
}

int Status::Code() const noexcept
{
  return code_;
}

bool Status::OK() const noexcept
{
  return code_ == 0;
}

std::string Status::String() const noexcept
{
  switch (code_)
  {
  case ::StatusOK:
    return "StatusOK";
  case ::ErrClusterNotFound:
    return "ErrClusterNotFound";
  case ::ErrClusterAlreadyExist:
    return "ErrClusterAlreadyExist";
  case ::ErrDeadlineNotSet:
    return "ErrDeadlineNotSet";
  case ::ErrInvalidDeadline:
    return "ErrInvalidDeadline";
  case ::ErrInvalidSession:
    return "ErrInvalidSession";
  case ::ErrTimeoutTooSmall:
    return "ErrTimeoutTooSmall";
  case ::ErrPayloadTooBig:
    return "ErrPayloadTooBig";
  case ::ErrSystemBusy:
    return "ErrSystemBusy";
  case ::ErrClusterClosed:
    return "ErrClusterClosed";
  case ::ErrBadKey:
    return "ErrBadKey";
  case ::ErrPendingConfigChangeExist:
    return "ErrPendingConfigChangeExist";
  case ::ErrTimeout:
    return "ErrTimeout";
  case ::ErrSystemStopped:
    return "ErrSystemStopped";
  case ::ErrCanceled:
    return "ErrCanceled";
  case ::ErrResultBufferTooSmall:
    return "ErrResultBufferTooSmall";
  case ::ErrRejected:
    return "ErrRejected";
  case ::ErrInvalidClusterSettings:
    return "ErrInvalidClusterSettings";
  case ::ErrClusterNotReady:
    return "ErrClusterNotReady";
  case ::ErrClusterNotStopped:
    return "ErrClusterNotStopped";
  case ::ErrClusterNotInitialized:
    return "ErrClusterNotInitialized";
  case ::ErrNodeRemoved:
    return "ErrNodeRemoved";
  case ::ErrDirNotExist:
    return "ErrDirNotExist";
  default:
    return "Unknown Error Code " + std::to_string(code_);
  }
}

const int Status::StatusOK = ::StatusOK;
const int Status::ErrClusterNotFound = ::ErrClusterNotFound;
const int Status::ErrClusterAlreadyExist = ::ErrClusterAlreadyExist;
const int Status::ErrDeadlineNotSet = ::ErrDeadlineNotSet;
const int Status::ErrInvalidDeadline = ::ErrInvalidDeadline;
const int Status::ErrInvalidSession = ::ErrInvalidSession;
const int Status::ErrTimeoutTooSmall = ::ErrTimeoutTooSmall;
const int Status::ErrPayloadTooBig = ::ErrPayloadTooBig;
const int Status::ErrSystemBusy = ::ErrSystemBusy;
const int Status::ErrClusterClosed = ::ErrClusterClosed;
const int Status::ErrBadKey = ::ErrBadKey;
const int Status::ErrPendingConfigChangeExist = ::ErrPendingConfigChangeExist;
const int Status::ErrTimeout = ::ErrTimeout;
const int Status::ErrSystemStopped = ::ErrSystemStopped;
const int Status::ErrCanceled = ::ErrCanceled;
const int Status::ErrResultBufferTooSmall = ::ErrResultBufferTooSmall;
const int Status::ErrRejected = ::ErrRejected;
const int Status::ErrInvalidClusterSettings = ::ErrInvalidClusterSettings;
const int Status::ErrClusterNotReady = ::ErrClusterNotReady;
const int Status::ErrClusterNotStopped = ::ErrClusterNotStopped;
const int Status::ErrClusterNotInitialized = ::ErrClusterNotInitialized;
const int Status::ErrNodeRemoved = ::ErrNodeRemoved;
const int Status::ErrDirNotExist = ::ErrDirNotExist;

Buffer::Buffer(size_t n) noexcept
  : data_(), len_(n)
{
  static_assert(sizeof(Byte) == 1, "Byte type must be 1 byte long");
  data_.resize(n);
}

Buffer::Buffer(const Byte *data, size_t n) noexcept
  : data_(), len_(n)
{
  data_.resize(n);
  std::memcpy(&(data_[0]), data, n);
}

const Byte *Buffer::Data() const noexcept
{
  return &(data_[0]);
}

size_t Buffer::Len() const noexcept
{
  return len_;
}

size_t Buffer::Capacity() const noexcept
{
  return data_.capacity();
}

void Buffer::SetLen(size_t n) noexcept
{
  if (n <= Capacity())
  {
    len_ = n;
  }
}

void Peers::AddMember(std::string address, NodeID nodeID) noexcept
{
  members_[address] = nodeID;
}

size_t Peers::Len() const noexcept {
  return members_.size();
}

std::map<std::string, NodeID> Peers::GetMembership() const noexcept
{
  return members_;
}

void parsePeers(const Peers &peers, ::DBString strs[], uint64_t nodeIDList[]) noexcept
{
  auto members = peers.GetMembership();
  int i = 0;
  for (auto& kv : members) {
    strs[i] = toDBString(kv.first);
    nodeIDList[i] = kv.second;
    i++;
  }
}

LeaderID::LeaderID() noexcept
  : nodeID_(0), hasLeaderInfo_(false)
{
}

void LeaderID::SetLeaderID(NodeID nodeID, bool hasLeaderInfo) noexcept
{
  nodeID_ = nodeID;
  hasLeaderInfo_ = hasLeaderInfo;
}

bool LeaderID::HasLeaderInfo() const noexcept {
  return hasLeaderInfo_;
}

NodeID LeaderID::GetLeaderID() const noexcept {
  return nodeID_;
}

Session::Session(oid_t oid) noexcept
  : ManagedObject(oid),
    proposalCompleted_(false),
    readyForRegistration_(false),
    readyForUnregistration_(false),
    prepareForProposal_(false)
{
}

Session::~Session()
{
}

void Session::ProposalCompleted() noexcept
{
  proposalCompleted_ = true;
}

void Session::PrepareForProposal() noexcept
{
  prepareForProposal_ = true;
}

void Session::PrepareForRegistration() noexcept
{
  readyForRegistration_ = true;
}

void Session::PrepareForUnregistration() noexcept
{
  readyForUnregistration_ = true;
}

Session *Session::GetNewSession(ClusterID clusterID) noexcept
{
  return new Session(CGetSession(clusterID));
}

bool Session::GetProposalCompleted() const noexcept
{
  return proposalCompleted_;
}

bool Session::GetReadyForRegistration() const noexcept
{
  return readyForRegistration_;
}

bool Session::GetReadyForUnregistration() const noexcept
{
  return readyForUnregistration_;
}

bool Session::GetPreparedForProposal() const noexcept
{
  return prepareForProposal_;
}

void Session::ClearProposalCompleted() noexcept
{
  proposalCompleted_ = false;
}

void Session::ClearReadyForRegistration() noexcept
{
  readyForRegistration_ = false;
}

void Session::ClearReadyForUnregistration() noexcept
{
  readyForUnregistration_ = false;
}

void Session::ClearPrepareForProposal() noexcept
{
  prepareForProposal_ = false;
}

RequestState::RequestState(oid_t oid) noexcept
  : ManagedObject(oid), hasResult_(false)
{
}

RequestState::~RequestState()
{
}

RequestResult RequestState::Get() noexcept
{
  if (!hasResult_) {
    hasResult_ = true;
    ::RequestResult result = CSelectOnRequestState(oid_);
    result_.code = static_cast<ResultCode>(result.errcode);
    result_.result = result.result;
  }
  return result_;
}

NodeHost::NodeHost(const NodeHostConfig &c) noexcept
  : ManagedObject(0), config_(c)
{
  ::NodeHostConfig cfg;
  cfg.DeploymentID = c.DeploymentID;
  cfg.WALDir = toDBString(c.WALDir);
  cfg.NodeHostDir = toDBString(c.NodeHostDir);
  cfg.RTTMillisecond = c.RTTMillisecond.count();
  cfg.RaftAddress = toDBString(c.RaftAddress);
  cfg.ListenAddress = toDBString(c.ListenAddress);
  cfg.MutualTLS = c.MutualTLS == 1;
  cfg.CAFile = toDBString(c.CAFile);
  cfg.CertFile = toDBString(c.CertFile);
  cfg.KeyFile = toDBString(c.KeyFile);
  cfg.MaxSendQueueSize = c.MaxSendQueueSize;
  cfg.MaxReceiveQueueSize = c.MaxReceiveQueueSize;
  cfg.EnableMetrics = c.EnableMetrics == 1;
  if (config_.RaftEventListener) {
    cfg.RaftEventListener = const_cast<std::function<void(LeaderInfo)>*>(
      &config_.RaftEventListener);
  } else {
    cfg.RaftEventListener = nullptr;
  }
  oid_ = CNewNodeHost(cfg);
}

NodeHost::~NodeHost() {}

NodeHostConfig NodeHost::GetNodeHostConfig() const noexcept
{
  return config_;
}

void NodeHost::Stop() noexcept
{
  CStopNodeHost(oid_);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  std::string pluginFile, std::string factoryName, StateMachineType smType,
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartClusterFromPlugin(oid_,
    nodeIDList.get(), strs.get(), peers.Len(), join,
    toDBString(pluginFile), toDBString(factoryName), smType, cfg);
  return Status(code);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  std::function<RegularStateMachine*(uint64_t, uint64_t)> factory,
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(&factory),
    REGULAR_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  std::function<ConcurrentStateMachine*(uint64_t, uint64_t)> factory,
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(&factory),
    CONCURRENT_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  std::function<OnDiskStateMachine*(uint64_t, uint64_t)> factory,
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(&factory),
    ONDISK_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StopCluster(ClusterID clusterID) noexcept
{
  return Status(CNodeHostStopCluster(oid_, clusterID));
}

Status NodeHost::StopNode(ClusterID clusterID, NodeID nodeID) noexcept
{
  return Status(CNodeHostStopNode(oid_, clusterID, nodeID));
}

Session *NodeHost::GetNoOPSession(ClusterID clusterID) noexcept
{
  uint64_t oid = CGetNoOPSession(clusterID);
  return new Session(oid);
}

Session *NodeHost::SyncGetSession(ClusterID clusterID,
  Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::NewSessionResult result;
  result = CNodeHostSyncGetSession(oid_, ts, clusterID);
  *status = Status(result.errcode);
  if (result.csoid == 0) {
    return nullptr;
  }
  return new Session(result.csoid);
}

Status NodeHost::SyncCloseSession(const Session &session,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  int code = CNodeHostSyncCloseSession(oid_, ts, session.OID());
  return Status(code);
}

Status NodeHost::SyncPropose(Session *session,
  const Buffer &buf, Milliseconds timeout, UpdateResult *result) noexcept
{
  return SyncPropose(session, buf.Data(), buf.Len(), timeout, result);
}

Status NodeHost::SyncPropose(Session *session,
  const Byte *buf, size_t buflen,
  Milliseconds timeout, UpdateResult *result) noexcept
{
  auto ts = timeout.count();
  ::RequestResult ret;
  bool proposalCompleted = session->GetProposalCompleted();
  session->ClearProposalCompleted();
  ret = CNodeHostSyncPropose(oid_,
    ts, session->OID(), proposalCompleted, buf, buflen);
  *result = ret.result;
  return Status(ret.errcode);
}

Status NodeHost::SyncRead(ClusterID clusterID,
  const Buffer &query, Buffer *result, Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  size_t written;
  int code = CNodeHostSyncRead(oid_, ts, clusterID,
    const_cast<Byte*>(query.Data()), query.Len(),
    const_cast<Byte*>(result->Data()), result->Capacity(), &written);
  if (code == 0) {
    result->SetLen(written);
  }
  return Status(code);
}

Status NodeHost::SyncRead(ClusterID clusterID,
  const Byte *query, size_t queryLen,
  Byte *result, size_t resultLen, size_t *written,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  int code = CNodeHostSyncRead(oid_, ts, clusterID,
    query, queryLen, result, resultLen, written);
  return Status(code);
}

Status NodeHost::Propose(Session *session,
  const Buffer &buf, Milliseconds timeout, Event *event) noexcept
{
  return Propose(session,
    const_cast<Byte*>(buf.Data()), buf.Len(), timeout, event);
}

Status NodeHost::Propose(Session *session,
  const Byte *buf, size_t buflen, Milliseconds timeout, Event *event) noexcept
{
  auto ts = timeout.count();
  ::ProposeResult result;
  bool proposalCompleted = session->GetProposalCompleted();
  session->ClearProposalCompleted();
  bool prepareForProposal = session->GetPreparedForProposal();
  session->ClearPrepareForProposal();
  result = CNodeHostPropose(oid_, ts, session->OID(),
    proposalCompleted, prepareForProposal, buf, buflen,
    reinterpret_cast<void *>(event), CompleteHandlerCPP);
  return Status(result.errcode);
}

Status NodeHost::ReadIndex(ClusterID clusterID, Milliseconds timeout,
  Event *event) noexcept
{
  auto ts = timeout.count();
  ::ReadIndexResult result;
  result = CNodeHostReadIndex(oid_, ts, clusterID,
    reinterpret_cast<void *>(event), CompleteHandlerCPP);
  return Status(result.errcode);
}

Status NodeHost::ReadLocal(ClusterID clusterID,
  const Buffer &query, Buffer *result) noexcept
{
  size_t written;
  int code = CNodeHostReadLocal(oid_, clusterID,
    query.Data(), query.Len(),
    const_cast<Byte*>(result->Data()), result->Capacity(), &written);
  if (code == 0)
  {
    result->SetLen(written);
  }
  return Status(code);
}

Status NodeHost::ReadLocal(ClusterID clusterID,
  const Byte *query, size_t queryLen,
  Byte *result, size_t resultLen, size_t *written) noexcept
{
  int code = CNodeHostReadLocal(oid_,
    clusterID, query, queryLen, result, resultLen, written);
  return Status(code);
}

Status NodeHost::StaleRead(ClusterID clusterID, const Buffer &query,
  Buffer *result) noexcept
{
  size_t written;
  int code = CNodeHostStaleRead(oid_, clusterID,
    query.Data(), query.Len(),
    const_cast<Byte*>(result->Data()), result->Capacity(), &written);
  if (code == 0) {
    result->SetLen(written);
  }
  return Status(code);
}

Status NodeHost::StaleRead(ClusterID clusterID,
  const Byte *query, size_t queryLen,
  Byte *result, size_t resultLen, size_t *written) noexcept
{
  int code = CNodeHostStaleRead(oid_, clusterID,
    query, queryLen, result, resultLen, written);
  return Status(code);
}

Status NodeHost::SyncRequestSnapshot(ClusterID clusterID, SnapshotOption opt,
  Milliseconds timeout, SnapshotResultIndex *result) noexcept
{
  auto ts = timeout.count();
  ::SnapshotOption option;
  option.Exported = opt.Exported;
  option.ExportedPath = toDBString(opt.ExportedPath);
  ::RequestResult ret = CNodeHostSyncRequestSnapshot(oid_, clusterID, option, ts);
  *result = ret.result;
  return Status(ret.errcode);
}

RequestState *NodeHost::RequestSnapshot(ClusterID clusterID, SnapshotOption opt,
  Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::SnapshotOption option;
  option.Exported = opt.Exported;
  option.ExportedPath = toDBString(opt.ExportedPath);
  ::RequestStateResult result;
  result = CNodeHostRequestSnapshot(oid_, clusterID, option, ts);
  *status = Status(result.errcode);
  if (result.rsoid == 0) {
    return nullptr;
  }
  return new RequestState(result.rsoid);
}

Status NodeHost::ProposeSession(Session *cs,
  Milliseconds timeout, Event *event) noexcept
{
  auto ts = timeout.count();
  ::ProposeResult result;
  bool readyForRegisteration = cs->GetReadyForRegistration();
  bool readyForUnregisteration = cs->GetReadyForUnregistration();
  cs->ClearReadyForRegistration();
  cs->ClearReadyForUnregistration();
  result = CNodeHostProposeSession(oid_, ts, cs->OID(),
    readyForRegisteration, readyForUnregisteration,
    reinterpret_cast<void *>(event), CompleteHandlerCPP);
  return Status(result.errcode);
}

Status NodeHost::SyncRequestAddNode(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  return Status(CNodeHostSyncRequestAddNode(oid_,
    ts, clusterID, nodeID, toDBString(url)));
}

RequestState *NodeHost::RequestAddNode(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::RequestStateResult result;
  result = CNodeHostRequestAddNode(oid_, ts, clusterID, nodeID, toDBString(url));
  *status = Status(result.errcode);
  if (result.rsoid == 0) {
    return nullptr;
  }
  return new RequestState(result.rsoid);
}

Status NodeHost::SyncRequestDeleteNode(ClusterID clusterID, NodeID nodeID,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  return Status(CNodeHostSyncRequestDeleteNode(oid_, ts, clusterID, nodeID));
}

RequestState *NodeHost::RequestDeleteNode(ClusterID clusterID, NodeID nodeID,
  Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::RequestStateResult result;
  result = CNodeHostRequestDeleteNode(oid_, ts, clusterID, nodeID);
  *status = Status(result.errcode);
  if (result.rsoid == 0) {
    return nullptr;
  }
  return new RequestState(result.rsoid);
}

Status NodeHost::SyncRequestAddObserver(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  return Status(CNodeHostSyncRequestAddObserver(oid_,
    ts, clusterID, nodeID, toDBString(url)));
}

RequestState *NodeHost::RequestAddObserver(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::RequestStateResult result;
  result = CNodeHostRequestAddObserver(oid_,
    ts, clusterID, nodeID, toDBString(url));
  *status = Status(result.errcode);
  if (result.rsoid == 0) {
    return nullptr;
  }
  return new RequestState(result.rsoid);
}

Status NodeHost::RequestLeaderTransfer(ClusterID clusterID,
  NodeID targetNodeID) noexcept
{
  int code = CRequestLeaderTransfer(oid_, clusterID, targetNodeID);
  return Status(code);
}

Status NodeHost::GetClusterMembership(ClusterID clusterID,
  Milliseconds timeout, Peers *p) noexcept
{
  auto ts = timeout.count();
  GetMembershipResult r = CNodeHostGetClusterMembership(oid_, ts, clusterID);
  Status s(r.errcode);
  if (!s.OK())
  {
    FreeMembership(r.membership);
    return s;
  }
  for (size_t i = 0; i < r.membership->nodeListLen; i++)
  {
    DBString addr = r.membership->nodeAddressList[i];
    std::string addstr(addr.str, addr.len);
    p->AddMember(addstr, r.membership->nodeIDList[i]);
  }
  FreeMembership(r.membership);
  return s;
}

Status NodeHost::GetLeaderID(ClusterID clusterID, LeaderID *leaderID) noexcept
{
  GetLeaderIDResult r = CNodeHostGetLeaderID(oid_, clusterID);
  leaderID->SetLeaderID(r.nodeID, r.valid);
  return Status(r.errcode);
}

Status NodeHost::SyncRemoveData(ClusterID clusterID, NodeID nodeID,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  return Status(CNodeHostSyncRemoveData(oid_, clusterID, nodeID, ts));
}

Status NodeHost::RemoveData(ClusterID clusterID, NodeID nodeID) noexcept
{
  return Status(CNodeHostRemoveData(oid_, clusterID, nodeID));
}

bool NodeHost::HasNodeInfo(ClusterID clusterID, NodeID nodeID) noexcept
{
  return CNodeHostHasNodeInfo(oid_, clusterID, nodeID);
}

NodeHostInfo NodeHost::GetNodeHostInfo(NodeHostInfoOption option) noexcept
{
  ::NodeHostInfoOption opt;
  opt.SkipLogInfo = option.SkipLogInfo;
  ::NodeHostInfo info;
  CNodeHostGetNodeHostInfo(oid_, opt, &info);
  NodeHostInfo nhi;
  nhi.RaftAddress = config_.RaftAddress;
  for (uint64_t idx = 0; idx < info.ClusterInfoListLen; idx++) {
    ClusterInfo ci;
    ::ClusterInfo *cip = info.ClusterInfoList;
    ci.ClusterID = cip[idx].ClusterID;
    ci.NodeID = cip[idx].NodeID;
    ci.IsLeader = cip[idx].IsLeader;
    ci.IsObserver = cip[idx].IsObserver;
    ci.SMType = static_cast<StateMachineType>(cip[idx].SMType);
    for (uint64_t i = 0; i < cip[idx].NodeAddrPairsNum; i++) {
      ci.Nodes[cip[idx].NodeAddrPairs[i].NodeID] = std::string(cip[idx].NodeAddrPairs[i].RaftAddress);
      free(cip[idx].NodeAddrPairs[i].RaftAddress);
    }
    free(cip[idx].NodeAddrPairs);
    ci.ConfigChangeIndex = cip[idx].ConfigChangeIndex;
    ci.Pending = cip[idx].Pending;
    nhi.ClusterInfoList.emplace_back(std::move(ci));
  }
  free(info.ClusterInfoList);
  for (uint64_t idx = 0; idx < info.LogInfoLen; idx++) {
    nhi.LogInfo.emplace_back(NodeInfo{info.LogInfo[idx].ClusterID, info.LogInfo[idx].NodeID});
  }
  free(info.LogInfo);
  return nhi;
}

IOServiceHandler *RunIOServiceInGoRuntime(IOService* iosp,
  size_t threadCount) noexcept
{
  uint64_t oid = CRunIOServiceInGo(reinterpret_cast<void *>(iosp), threadCount);
  return new IOServiceHandler(oid);
}

IOServiceHandler::IOServiceHandler(oid_t oid) noexcept
  : ManagedObject(oid)
{
}

IOServiceHandler::~IOServiceHandler() {}

void IOServiceHandler::Join() noexcept
{
  CJoinIOServiceThreads(OID());
}



}  // namespace dragonboat
