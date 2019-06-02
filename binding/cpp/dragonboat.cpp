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
    CompactionOverhead(0), OrderedConfigChange(false), MaxInMemLogSize(0)
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
}

NodeHostConfig::NodeHostConfig(std::string WALDir,
  std::string NodeHostDir) noexcept
  : DeploymentID(1), WALDir(WALDir), NodeHostDir(NodeHostDir),
  RTTMillisecond(200), RaftAddress("localhost:9050"),
  MutualTLS(false), CAFile(""), CertFile(""), KeyFile("")
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
  // TODO
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
    readyForRegisteration_(false),
    readyForUnregisteration_(false),
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

void Session::PrepareForRegisteration() noexcept
{
  readyForRegisteration_ = true;
}

void Session::PrepareForUnregisteration() noexcept
{
  readyForUnregisteration_ = true;
}

Session *Session::GetNewSession(ClusterID clusterID) noexcept
{
  return new Session(CGetSession(clusterID));
}

bool Session::GetProposalCompleted() const noexcept
{
  return proposalCompleted_;
}

bool Session::GetReadyForRegisteration() const noexcept
{
  return readyForRegisteration_;
}

bool Session::GetReadyForUnregisteration() const noexcept
{
  return readyForUnregisteration_;
}

bool Session::GetPreparedForProposal() const noexcept
{
  return prepareForProposal_;
}

void Session::ClearProposalCompleted() noexcept
{
  proposalCompleted_ = false;
}

void Session::ClearReadyForRegisteration() noexcept
{
  readyForRegisteration_ = false;
}

void Session::ClearReadyForUnregisteration() noexcept
{
  readyForUnregisteration_ = false;
}

void Session::ClearPrepareForProposal() noexcept
{
  prepareForProposal_ = false;
}

NodeHost::NodeHost(const NodeHostConfig &c) noexcept
  : ManagedObject(0)
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
  oid_ = CNewNodeHost(cfg);
}

NodeHost::~NodeHost() {}

void NodeHost::Stop() noexcept
{
  CStopNodeHost(oid_);
}

//Status NodeHost::StartCluster(const Peers& peers,
//  bool join, std::string pluginFilename, Config config) noexcept
//{
//  ::RaftConfig cfg;
//  parseConfig(config, cfg);
//  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
//  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
//  parsePeers(peers, strs.get(), nodeIDList.get());
//  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
//    peers.Len(), join, toDBString(pluginFilename), cfg);
//  return Status(code);
//}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  RegularStateMachine*(*factory)(uint64_t clusterID, uint64_t nodeID),
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(factory),
    REGULAR_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  ConcurrentStateMachine*(*factory)(uint64_t clusterID, uint64_t nodeID),
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(factory),
    CONCURRENT_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StartCluster(const Peers& peers, bool join,
  OnDiskStateMachine*(*factory)(uint64_t clusterID, uint64_t nodeID),
  Config config) noexcept
{
  ::RaftConfig cfg;
  parseConfig(config, cfg);
  std::unique_ptr<::DBString[]> strs(new ::DBString[peers.Len()]);
  std::unique_ptr<uint64_t[]> nodeIDList(new uint64_t[peers.Len()]);
  parsePeers(peers, strs.get(), nodeIDList.get());
  int code = CNodeHostStartCluster(oid_, nodeIDList.get(), strs.get(),
    peers.Len(), join, reinterpret_cast<void *>(factory),
    ONDISK_STATEMACHINE, cfg);
  return Status(code);
}

Status NodeHost::StopCluster(ClusterID clusterID) noexcept
{
  return Status(CNodeHostStopCluster(oid_, clusterID));
}

Session *NodeHost::GetNoOPSession(ClusterID clusterID) noexcept
{
  uint64_t oid = CGetNoOPSession(clusterID);
  return new Session(oid);
}

Session *NodeHost::GetNewSession(ClusterID clusterID,
  Milliseconds timeout, Status *status) noexcept
{
  auto ts = timeout.count();
  ::NewSessionResult result;
  result = CNodeHostGetNewSession(oid_, ts, clusterID);
  *status = Status(result.errcode);
  if (result.csoid == 0) {
    return nullptr;
  }
  return new Session(result.csoid);
}

Status NodeHost::CloseSession(const Session &session,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  int code = CNodeHostCloseSession(oid_, ts, session.OID());
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
  ::SyncProposeResult ret;
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
  if (code == 0)
  {
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
    const_cast<Byte*>(query.Data()), query.Len(),
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

Status NodeHost::ProposeSession(Session *cs,
  Milliseconds timeout, Event *event) noexcept
{
  auto ts = timeout.count();
  ::ProposeResult result;
  bool readyForRegisteration = cs->GetReadyForRegisteration();
  bool readyForUnregisteration = cs->GetReadyForUnregisteration();
  cs->ClearReadyForRegisteration();
  cs->ClearReadyForUnregisteration();
  result = CNodeHostProposeSession(oid_, ts, cs->OID(),
    readyForRegisteration, readyForUnregisteration,
    reinterpret_cast<void *>(event), CompleteHandlerCPP);
  return Status(result.errcode);
}

Status NodeHost::AddNode(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  ::AddNodeResult result;
  result = CNodeHostRequestAddNode(oid_,
    ts, clusterID, nodeID, toDBString(url));
  Status requestStatus(result.errcode);
  ManagedObject mo(result.rsoid);
  if (!requestStatus.OK()) {
    return requestStatus;
  }
  return Status(CSelectOnRequestStateForMembershipChange(mo.OID()));
}

Status NodeHost::AddObserver(ClusterID clusterID, NodeID nodeID,
  std::string url, Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  ::AddObserverResult result;
  result = CNodeHostRequestAddObserver(oid_,
    ts, clusterID, nodeID, toDBString(url));
  Status requestStatus(result.errcode);
  ManagedObject mo(result.rsoid);
  if (!requestStatus.OK()) {
    return requestStatus;
  }
  return Status(CSelectOnRequestStateForMembershipChange(mo.OID()));
}

Status NodeHost::RemoveNode(ClusterID clusterID, NodeID nodeID,
  Milliseconds timeout) noexcept
{
  auto ts = timeout.count();
  ::DeleteNodeResult result;
  result = CNodeHostRequestDeleteNode(oid_, ts, clusterID, nodeID);
  Status requestStatus(result.errcode);
  ManagedObject mo(result.rsoid);
  if (!requestStatus.OK()) {
    return requestStatus;
  }
  return Status(CSelectOnRequestStateForMembershipChange(mo.OID()));
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
