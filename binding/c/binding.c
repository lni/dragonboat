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

#include "dragonboat/binding.h"
#include "dragonboat/libdragonboat.h"

uint64_t CGetManagedObjectCount()
{
  return GetManagedObjectCount();
}

uint64_t CGetInterestedGoroutines()
{
  return GetInterestedGoroutines();
}

void CAssertNoGoroutineLeak(uint64_t oid)
{
  AssertNoGoroutineLeak(oid);
}

uint64_t CGetSession(uint64_t clusterID)
{
  return CreateSession(clusterID);
}

uint64_t CGetNoOPSession(uint64_t clusterID)
{
  return CreateNoOPSession(clusterID);
}

uint64_t CRunIOServiceInGo(void *ioservice, size_t count)
{
  return RunIOServiceInGo(ioservice, count);
}

void CJoinIOServiceThreads(uint64_t oid)
{
  JoinIOServiceThreads(oid);
}

void CRemoveManagedObject(uint64_t csoid)
{
  RemoveManagedObject(csoid);
}

int CSetLogLevel(DBString package, int level)
{
  return SetLogLevel(package, level);
}

int CSelectOnRequestStateForMembershipChange(uint64_t rsoid)
{
  return SelectOnRequestStateForMembershipChange(rsoid);
}

void CSessionProposalCompleted(uint64_t csoid)
{
  SessionProposalCompleted(csoid); 
}

uint64_t CNewNodeHost(NodeHostConfig cfg)
{
  return NewNodeHost(cfg);
}

void CStopNodeHost(uint64_t oid)
{
  StopNodeHost(oid);
}

int CNodeHostStartClusterFromPlugin(uint64_t oid,
  uint64_t *nodeIDList, DBString *nodeAddressList, size_t nodeListLen,
  Bool join, DBString pluginFile, DBString factoryName,
  int32_t smType, RaftConfig cfg)
{
  return NodeHostStartClusterFromPlugin(oid, nodeIDList, nodeAddressList,
    nodeListLen, join, pluginFile, factoryName, smType, cfg);
}

int CNodeHostStartCluster(uint64_t oid,
  uint64_t *nodeIDList, DBString *nodeAddressList, size_t nodeListLen,
  Bool join, void *factory, int32_t smType, RaftConfig cfg)
{
  return NodeHostStartCluster(oid, nodeIDList, nodeAddressList, nodeListLen,
    join, factory, smType, cfg);
}

int CNodeHostStopCluster(uint64_t oid, uint64_t clusterID)
{
  return NodeHostStopCluster(oid, clusterID);
}

int CNodeHostStopNode(uint64_t oid, uint64_t clusterID, uint64_t nodeID)
{
  return NodeHostStopNode(oid, clusterID, nodeID);
}

NewSessionResult CNodeHostSyncGetSession(uint64_t oid,
  uint64_t timeout, uint64_t clusterID)
{
  struct NodeHostSyncGetSession_return r;
  NewSessionResult result;
  r = NodeHostSyncGetSession(oid, timeout, clusterID);
  result.csoid = r.r0;
  result.errcode = r.r1;
  return result;
}

int CNodeHostSyncCloseSession(uint64_t oid,
  uint64_t timeout, uint64_t csoid)
{
  return NodeHostSyncCloseSession(oid, timeout, csoid);
}

SyncProposeResult CNodeHostSyncPropose(uint64_t oid, uint64_t timeout,
  uint64_t csoid, Bool csupdate, const unsigned char *buf, size_t len)
{
  struct NodeHostSyncPropose_return r;
  SyncProposeResult result;
  r = NodeHostSyncPropose(oid,
    timeout, csoid, csupdate, (unsigned char *)buf, len);
  result.result = r.r0;
  result.errcode = r.r1;
  return result;
}

int CNodeHostSyncRead(uint64_t oid,
  uint64_t timeout, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written)
{
  struct NodeHostSyncRead_return r;
  r = NodeHostSyncRead(oid, timeout, clusterID,
    (unsigned char *)queryBuf, queryBufLen, resultBuf, resultBufLen);
  *written = r.r1;
  return r.r0;
}

int CNodeHostStaleRead(uint64_t oid, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written)
{
  struct NodeHostStaleRead_return r;
  r = NodeHostStaleRead(oid, clusterID,
    (unsigned char *)queryBuf, queryBufLen, resultBuf, resultBufLen);
  *written = r.r1;
  return r.r0;
}

int CNodeHostReadLocal(uint64_t oid, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written)
{
  struct NodeHostReadLocal_return r;
  r = NodeHostReadLocal(oid, clusterID,
    (unsigned char *)queryBuf, queryBufLen, resultBuf, resultBufLen);
  *written = r.r1;
  return r.r0;
}

SyncRequestSnapshotResult CNodeHostSyncRequestSnapshot(uint64_t oid,
  uint64_t clusterID, SnapshotOption opt, uint64_t timeout)
{
  SyncRequestSnapshotResult result;
  struct NodeHostSyncRequestSnapshot_return r;
  r = NodeHostSyncRequestSnapshot(oid, clusterID, opt, timeout);
  result.result = r.r0;
  result.errcode = r.r1;
  return result;
}

RequestSnapshotResult CNodeHostRequestSnapshot(uint64_t oid, uint64_t clusterID,
  SnapshotOption opt, uint64_t timeout, void *handler, int t)
{
  RequestSnapshotResult result;
  struct NodeHostRequestSnapshot_return r;
  r = NodeHostRequestSnapshot(oid, clusterID, opt, timeout, handler, t);
  result.oid = r.r0;
  result.errcode = r.r1;
  return result;
}

int CNodeHostSyncRequestAddNode(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID, DBString url)
{
  return NodeHostSyncRequestAddNode(oid, timeout, clusterID, nodeID, url, 0);
}

int CNodeHostSyncRequestAddObserver(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID, DBString url)
{
  return NodeHostSyncRequestAddObserver(oid, timeout, clusterID, nodeID, url, 0);
}

int CNodeHostSyncRequestDeleteNode(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID)
{
  return NodeHostSyncRequestDeleteNode(oid, timeout, clusterID, nodeID, 0);
}

int CRequestLeaderTransfer(uint64_t oid, uint64_t clusterID, uint64_t nodeID)
{
  return NodeHostRequestLeaderTransfer(oid, clusterID, nodeID);
}

GetMembershipResult CNodeHostGetClusterMembership(uint64_t oid,
  uint64_t timeout, uint64_t clusterID)
{
  GetMembershipResult result;
  struct NodeHostGetClusterMembership_return r;
  r = NodeHostGetClusterMembership(oid, clusterID, timeout);
  result.membership = r.r0;
  result.cci = r.r1;
  result.errcode = r.r2;
  return result;
}

GetLeaderIDResult CNodeHostGetLeaderID(uint64_t oid, uint64_t clusterID)
{
  struct NodeHostGetLeaderID_return r;
  GetLeaderIDResult result;
  r = NodeHostGetLeaderID(oid, clusterID);
  result.nodeID = r.r0;
  result.valid = r.r1;
  result.errcode = r.r2;
  return result;
}

int CNodeHostSyncRemoveData(uint64_t oid,
  uint64_t clusterID, uint64_t nodeID, uint64_t timeout)
{
  return NodeHostSyncRemoveData(oid, clusterID, nodeID, timeout);
}

int CNodeHostRemoveData(uint64_t oid, uint64_t clusterID, uint64_t nodeID)
{
  return NodeHostRemoveData(oid, clusterID, nodeID);
}

ProposeResult CNodeHostProposeSession(uint64_t oid, uint64_t timeout,
  uint64_t csoid, Bool readyForRegisteration, Bool readyForUnregisteration,
  void *handler, int t)
{
  struct NodeHostProposeSession_return r;
  ProposeResult result;
  r = NodeHostProposeSession(oid, timeout, csoid, 
    readyForRegisteration, readyForUnregisteration, handler, t);
  result.oid = r.r0;
  result.errcode = r.r1;
  return result;
}

ProposeResult CNodeHostPropose(uint64_t oid, uint64_t timeout, uint64_t csoid,
  Bool csupdate, Bool prepareForProposal,
  const unsigned char *buf, size_t len, void *handler, int t)
{
  struct NodeHostPropose_return r;
  ProposeResult result;
  r = NodeHostPropose(oid, timeout, csoid,
    csupdate, prepareForProposal, (unsigned char *)buf, len, handler, t);
  result.oid = r.r0;
  result.errcode = r.r1;
  return result;
}

ReadIndexResult CNodeHostReadIndex(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, void *handler, int t)
{
  struct NodeHostReadIndex_return r;
  ReadIndexResult result;
  r = NodeHostReadIndex(oid, timeout, clusterID, handler, t);
  result.oid = r.r0;
  result.errcode = r.r1;
  return result;
}
