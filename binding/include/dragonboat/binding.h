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

#ifndef BINDING_INCLUDE_DRAGONBOAT_BINDING_H_
#define BINDING_INCLUDE_DRAGONBOAT_BINDING_H_

#include <stdint.h>
#include <stddef.h>

// This is not the C binding of dragonboat. This is the private interface used
// by langugage bindings to access the functionalities provided by dragonboat.
// Types and functions here can change at any release without public notice.

#ifdef __cplusplus
extern "C" {
#endif

enum StateMachineType
{
  // StateMachine type, the same as the StateMachineType int raftpb/raft.pb.go
  UNKNOWN_STATEMACHINE = 0,
  REGULAR_STATEMACHINE = 1,
  CONCURRENT_STATEMACHINE = 2,
  ONDISK_STATEMACHINE = 3,
};

enum
{
  // Save snapshot or recover from snapshot completed successfully.
  SNAPSHOT_OK = 0,
  // Failed to recover from the snapshot, e.g. the snapshot data read from the
  // snapshot file is corrupted.
  FAILED_TO_RECOVER_FROM_SNAPSHOT = 2,
  // Failed to save snapshot, e.g. can not serilize the data in StateMachine.
  FAILED_TO_SAVE_SNAPSHOT = 3,
  // Snapshot operation has been stopped by request
  SNAPSHOT_STOPPED = 4,
};

typedef struct DBString
{
  char *str;
  size_t len;
} DBString;

// LogLevel is the log level.
enum LogLevel
{
  LOG_LEVEL_CRITICAL = -1,
  LOG_LEVEL_ERROR = 0,
  LOG_LEVEL_WARNING = 1,
  LOG_LEVEL_INFO = 2,
  LOG_LEVEL_DEBUG = 3,
};

typedef struct
{
  uint64_t *nodeIDList;
  DBString *nodeAddressList;
  size_t nodeListLen;
  size_t index;
} Membership;

// ErrorCode is the error code used by dragonboat's language bindings.
// Error codes here are mostly mapped from exported Go errors in the
// github.com/lni/dragonboat/multiraft
enum ErrorCode
{
  StatusOK = 0,
  ErrClusterNotFound = -1,
  ErrClusterAlreadyExist = -2,
  ErrDeadlineNotSet = -3,
  ErrInvalidDeadline = -4,
  ErrInvalidSession = -5,
  ErrTimeoutTooSmall = -6,
  ErrPayloadTooBig = -7,
  ErrSystemBusy = -8,
  ErrClusterClosed = -9,
  ErrBadKey = -10,
  ErrPendingConfigChangeExist = -11,
  ErrTimeout = -12,
  ErrSystemStopped = -13,
  ErrCanceled = -14,
  ErrResultBufferTooSmall = -15,
  ErrRejected = -16,
  ErrInvalidClusterSettings = -17,
  ErrClusterNotStopped = -18,
  ErrClusterNotInitialized = -19,
};

// CompleteHandlerType is the type of complete handler. CompleteHandlerCPP is
// the only type currently supported.
enum CompleteHandlerType
{
  CompleteHandlerCPP = 0,
  CompleteHandlerPython = 1,
};

typedef char Bool;

// RaftConfig is the configuration for raft nodes. The Config type is provided
// as the cpp equivalent of the Config struct in
// github.com/lni/dragonboat/config,
// see the documentation of the Config struct to get details of all fields.
typedef struct RaftConfig
{
  uint64_t NodeID;
  uint64_t ClusterID;
  Bool IsObserver;
  Bool CheckQuorum;
  Bool Quiesce;
  uint64_t ElectionRTT;
  uint64_t HeartbeatRTT;
  uint64_t SnapshotEntries;
  uint64_t CompactionOverhead;
  Bool OrderedConfigChange;
  uint64_t MaxInMemLogSize;
} RaftConfig;

// NodeHostConfig is the configuration for the NodeHost instance. The
// NodeHostConfig type is provided as the cpp equivalent of the
// NodeHostConfig struct in the github.com/lni/dragonboat/config package.
// See the documentation of the NodeHostConfig struct to get details of all
// fields.
typedef struct NodeHostConfig
{
  uint64_t DeploymentID;
  DBString WALDir;
  DBString NodeHostDir;
  uint64_t RTTMillisecond;
  DBString RaftAddress;
  DBString ListenAddress;
  Bool MutualTLS;
  DBString CAFile;
  DBString CertFile;
  DBString KeyFile;
} NodeHostConfig;

typedef struct
{
  Bool Exported;
  DBString ExportedPath;
} SnapshotOption;

typedef struct
{
  uint64_t result;
  int errcode;
} OpenResult;

typedef struct
{
  char *result;
  size_t size;
} LookupResult;

typedef struct
{
  char *result;
  size_t size;
  int errcode;
} PrepareSnapshotResult;

typedef struct
{
  size_t size;
  int errcode;
} SnapshotResult;

typedef struct
{
  uint64_t result;
  int errcode;
} SyncRequestSnapshotResult;

typedef struct
{
  uint64_t oid;
  int errcode;
} RequestSnapshotResult;

typedef struct
{
  uint64_t csoid;
  int errcode;
} NewSessionResult;

typedef struct
{
  uint64_t result;
  int errcode;
} SyncProposeResult;

typedef struct
{
  uint64_t rsoid;
  int errcode;
} AddNodeResult;

typedef struct
{
  uint64_t rsoid;
  int errcode;
} DeleteNodeResult;

typedef struct
{
  uint64_t rsoid;
  int errcode;
} AddObserverResult;

typedef struct
{
  int errcode;
  uint64_t cci;
  Membership *membership;
} GetMembershipResult;

typedef struct
{
  int errcode;
  uint64_t nodeID;
  char valid;
} GetLeaderIDResult;

typedef struct
{
  int errcode;
  uint64_t oid;
} ProposeResult;

typedef struct
{
  int errcode;
  uint64_t oid;
} ReadIndexResult;

Membership *CreateMembership(size_t sz);
void AddClusterMember(Membership *m, uint64_t nodeID, char *addr, size_t len);
void CPPCompleteHandler(void *event, int code, uint64_t result);

void RunIOService(void *iosp);

uint64_t CGetManagedObjectCount();
uint64_t CGetInterestedGoroutines();
void CAssertNoGoroutineLeak(uint64_t oid);
uint64_t CRunIOServiceInGo(void *ioservice, size_t count);
void CJoinIOServiceThreads(uint64_t oid);
uint64_t CGetSession(uint64_t clusterID);
uint64_t CGetNoOPSession(uint64_t clusterID);
void CRemoveManagedObject(uint64_t csoid);
int CSetLogLevel(DBString package, int level);
int CSelectOnRequestStateForMembershipChange(uint64_t rsoid);
void CSessionProposalCompleted(uint64_t csoid);
uint64_t CNewNodeHost(NodeHostConfig cfg);
void CStopNodeHost(uint64_t oid);
int CNodeHostStartClusterFromPlugin(uint64_t oid,
  uint64_t *nodeIDList, DBString *nodeAddressList, size_t nodeListLen,
  Bool join, DBString pluginFile, DBString factoryName,
  int32_t smType, RaftConfig cfg);
int CNodeHostStartCluster(uint64_t oid,
  uint64_t *nodeIDList, DBString *nodeAddressList, size_t nodeListLen,
  Bool join, void *factory, int32_t smType, RaftConfig cfg);
int CNodeHostStopCluster(uint64_t oid, uint64_t clusterID);
int CNodeHostStopNode(uint64_t oid, uint64_t clusterID, uint64_t nodeID);
NewSessionResult CNodeHostSyncGetSession(uint64_t oid,
  uint64_t timeout, uint64_t clusterID);
int CNodeHostSyncCloseSession(uint64_t oid,
  uint64_t timeout, uint64_t csoid);
SyncProposeResult CNodeHostSyncPropose(uint64_t oid, uint64_t timeout,
  uint64_t csoid, Bool csupdate, const unsigned char *buf, size_t len);
int CNodeHostSyncRead(uint64_t oid,
  uint64_t timeout, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written);
int CNodeHostStaleRead(uint64_t oid, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written);
SyncRequestSnapshotResult CNodeHostSyncRequestSnapshot(uint64_t oid,
  uint64_t clusterID, SnapshotOption opt, uint64_t timeout);
RequestSnapshotResult CNodeHostRequestSnapshot(uint64_t oid, uint64_t clusterID,
  SnapshotOption opt, uint64_t timeout, void *handler, int t);
int CNodeHostSyncRequestAddNode(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID, DBString url);
int CNodeHostSyncRequestDeleteNode(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID);
int CNodeHostSyncRequestAddObserver(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, uint64_t nodeID, DBString url);
int CRequestLeaderTransfer(uint64_t oid, uint64_t clusterID, uint64_t nodeID);
GetMembershipResult CNodeHostGetClusterMembership(uint64_t oid,
  uint64_t timeout, uint64_t clusterID);
GetLeaderIDResult CNodeHostGetLeaderID(uint64_t oid, uint64_t clusterID);
ProposeResult CNodeHostProposeSession(uint64_t oid, uint64_t timeout,
  uint64_t csoid, Bool readyForRegisteration, Bool readyForUnregisteration,
  void *handler, int t);
ProposeResult CNodeHostPropose(uint64_t oid, uint64_t timeout, uint64_t csoid,
  Bool csupdate, Bool prepareForProposal,
  const unsigned char *buf, size_t len, void *handler, int t);
ReadIndexResult CNodeHostReadIndex(uint64_t oid, uint64_t timeout,
  uint64_t clusterID, void *handler, int t);
int CNodeHostReadLocal(uint64_t oid, uint64_t clusterID,
  const unsigned char *queryBuf, size_t queryBufLen,
  unsigned char *resultBuf, size_t resultBufLen, size_t *written);
int CNodeHostSyncRemoveData(uint64_t oid,
  uint64_t clusterID, uint64_t nodeID, uint64_t timeout);
int CNodeHostRemoveData(uint64_t oid, uint64_t clusterID, uint64_t nodeID);

#ifdef __cplusplus
}
#endif

#endif  // BINDING_INCLUDE_DRAGONBOAT_BINDING_H_
