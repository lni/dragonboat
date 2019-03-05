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

package settings

const (
	// EntryNonCmdFieldsSize defines the upper limit of the non-cmd field
	// length in pb.Entry.
	EntryNonCmdFieldsSize = 16 * 8
)

//
// Tuning configuration parameters here will impact the performance of your
// system. It will not corrupt your data. Only tune these parameters when
// you know what you are doing.
//
// To tune these parameters, place a json file named
// dragonboat-soft-settings.json in the current working directory of your
// dragonboat application, all fields in the json file will be applied to
// overwrite the default setting values. e.g. for a json file with the
// following content -
//
// {
//   "GetConnectedTimeoutSecond": 15,
//   "UnknownRegionName": "no-idea-region"
// }
//
// soft.GetConnectedTimeoutSecond will be 15,
// soft.UnknownRegionName will be "no-idea-region"
//
// The application need to be restarted to apply such configuration changes.
//

// Soft is the soft settings that can be changed after the deployment of a
// system.
var Soft = getSoftSettings()

type soft struct {
	// LocalRaftRequestTimeoutMs is the raft request timeout in millisecond.
	LocalRaftRequestTimeoutMs uint64
	// GetConnectedTimeoutSecond is the default timeout value in second when
	// trying to connect to a gRPC based server.
	GetConnectedTimeoutSecond uint64

	//
	// Raft
	//

	// MaxEntrySize defines the max total entry size that can be included in
	// the Replicate message.
	MaxEntrySize uint64
	// InMemEntrySliceSize defines the maximum length of the in memory entry
	// slice.
	InMemEntrySliceSize uint64
	// MinEntrySliceFreeSize defines the minimum length of the free in memory
	// entry slice. A new entry slice of length InMemEntrySliceSize will be
	// allocated once the free entry size in the current slice is less than
	// MinEntrySliceFreeSize.
	MinEntrySliceFreeSize uint64
	// ExpectedMaxInMemLogSize is the minimum MaxInMemLogSize value expected
	// in a raft config.
	ExpectedMaxInMemLogSize uint64

	//
	// Multiraft
	//

	// IncomingReadIndexQueueLength defines the number of pending read index
	// requests allowed for each raft group.
	IncomingReadIndexQueueLength uint64
	// IncomingProposalQueueLength defines the number of pending proposals
	// allowed for each raft group.
	IncomingProposalQueueLength uint64
	// UnknownRegionName defines the region name to use when the region
	// is unknown.
	UnknownRegionName string
	// RaftNodeReceiveQueueLength is the length of the receive queue on each
	// raftNode.
	RaftNodeReceiveQueueLength uint64
	// SnapshotStatusPushDelayMS is the number of millisecond delays we impose
	// before pushing the snapshot results to raftNode.
	SnapshotStatusPushDelayMS uint64
	// NodeCommitChanLength defined the length of each node's commitC channel.
	NodeCommitChanLength uint64
	// SetDeploymentIDTimeoutSecond defines the number of seconds allowed to
	// wait for setting deployment ID.
	SetDeploymentIDTimeoutSecond uint64
	// NodeHostSyncPoolSize defines the number of sync pools.
	NodeHostSyncPoolSize uint64
	// LatencySampleRatio defines the ratio how often latency is sampled.
	// It samples roughly every LatencySampleRatio ops.
	LatencySampleRatio uint64
	// LazyFreeCycle defines how often should entry queue and message queue
	// to be freed.
	LazyFreeCycle uint64
	// PanicOnSizeMismatch defines whether dragonboat should panic when snapshot
	// file size doesn't match the size recorded in snapshot metadata.
	PanicOnSizeMismatch uint64

	//
	// RSM
	//
	BatchedEntryApply bool

	//
	// step engine
	//

	// CommitBatchSize defines the length of the committed batch slice.
	CommitBatchSize uint64
	// NodeReloadMillisecond defines how often step engine should reload
	// nodes, it is defined in number of millisecond.
	NodeReloadMillisecond uint64
	// StepEngineCommitWorkerCount is the number of workers to use to apply
	// proposals (processing committed proposals) to application state
	// machines.
	StepEngineCommitWorkerCount uint64
	// StepEngineSnapshotWorkerCount is the number of workers to take and
	// apply application state machine snapshots.
	StepEngineSnapshotWorkerCount uint64
	// StepEngineNodeReadyChanBufferSize is the size of buffered channels
	// used to notify step engine workers about ready nodes.
	StepEngineNodeReadyChanBufferSize uint64
	// StepEngineCommitReadyChanBufferSize is the size of buffered channels
	// used to notify step engine commit workers about pending committed
	// proposals that can be processed.
	StepEngineCommitReadyChanBufferSize uint64
	// StepEngineLocalKeySize defines how many local keys should be kept
	// by each step engine node worker
	StepEngineLocalKeySize uint64

	//
	// transport
	//

	// SendQueueLength is the length of the send queue used to hold messages
	// exchanged between nodehosts. You may need to increase this value when
	// you want to host large number nodes per nodehost.
	SendQueueLength uint64
	// SnapshotSendQueueLength is the length of the send queue used to hold
	// snapshot chunks exchanged between nodehosts.
	SnapshotSendQueueLength uint64
	// MaxSnapshotCount is the max number of snapshots that can be pending in
	// snapshot queue waiting to be sent.
	MaxSnapshotCount uint64
	// MaxTransportStreamCount is the max number of streams to be handled on
	// each nodehost. Each remote nodehost in the system requires 2 streams.
	MaxTransportStreamCount uint64
	// MaxDrummerServerMsgSize is the max size of messages sent/received on
	// the Drummer side.
	MaxDrummerServerMsgSize uint64
	// MaxDrummerClientMsgSize is the max size of messages sent/received on
	// the nodehost side.
	MaxDrummerClientMsgSize uint64
	// StreamConnections defines how many connections to use for each remote
	// nodehost whene exchanging raft messages
	StreamConnections uint64
	// InitialWindowSize is the initial window size for tranport.
	InitialWindowSize uint64
	// InitialConnWindowSize is the initial connection window size for
	// transport.
	InitialConnWindowSize uint64
	// PerConnBufSize is the size of the per connection buffer used for
	// receiving incoming messages.
	PerConnectionBufferSize uint64
	// PerCpnnectionRecvBufSize is the size of the recv buffer size.
	PerCpnnectionRecvBufSize uint64
	// SnapshotGCTick defines the number of ticks between two snapshot GC
	// operations.
	SnapshotGCTick uint64
	// SnapshotChunkTimeoutTick defines the max time allowed to receive
	// a snapshot.
	SnapshotChunkTimeoutTick uint64

	//
	// Drummer/node scheduling
	//

	// DrummerClientName defines the name of the built-in drummer client.
	DrummerClientName string
	// NodeHostInfoReportSecond defines how often in seconds nodehost report it
	// details to Drummer servers.
	NodeHostInfoReportSecond uint64
	// NodeHostTTL defines the number of seconds without any report from the
	// nodehost required to consider it as dead.
	NodeHostTTL uint64
	// NodeToStartMaxWait is the number of seconds allowed for a new node to stay
	// in the to start state. To start state is the stage when a node has been
	// added to the raft cluster but has not been confirmed to be launched and
	// running on its assigned nodehost.
	NodeToStartMaxWait uint64
	// DrummerLoopIntervalFactor defines how often Drummer need to examine all
	// NodeHost info reported to it measured by the number of nodehost info
	// report cycles.
	DrummerLoopIntervalFactor uint64
	// PersisentLogReportCycle defines how often local persisted log info need
	// to be reported to Drummer server. Each NodeHostInfoReportSecond is called
	// a cycle. PersisentLogReportCycle defines how often each nodehost need to
	// update Drummer servers in terms of NodeHostInfoReportSecond cycles.
	PersisentLogReportCycle uint64
	// LogDBCacheSizePerWorker is the size of each entry cache, there will be
	// a total of StepEngineWorkerCount workers.
	LogDBCacheSizePerWorker uint64

	//
	// LogDB
	//

	// UseRangeDelete determines whether to use range delete when possible.
	UseRangeDelete bool
	// RDBMaxBackgroundCompactions is the MaxBackgroundCompactions parameter
	// directly passed to rocksdb.
	RDBMaxBackgroundCompactions uint64
	// RDBMaxBackgroundFlushes is the MaxBackgroundFlushes parameter directly
	// passed to rocksdb.
	RDBMaxBackgroundFlushes uint64
	// RDBLRUCacheSize is the LRUCacheSize
	RDBLRUCacheSize uint64
}

func getSoftSettings() soft {
	org := getDefaultSoftSettings()
	overwriteSoftSettings(&org)
	return org
}

func getDefaultSoftSettings() soft {
	NodeHostInfoReportSecond := uint64(20)
	return soft{
		PanicOnSizeMismatch:                 1,
		LazyFreeCycle:                       1,
		LatencySampleRatio:                  0,
		BatchedEntryApply:                   true,
		LocalRaftRequestTimeoutMs:           10000,
		GetConnectedTimeoutSecond:           5,
		MaxEntrySize:                        2 * MaxProposalPayloadSize,
		InMemEntrySliceSize:                 512,
		MinEntrySliceFreeSize:               96,
		ExpectedMaxInMemLogSize:             2 * (MaxProposalPayloadSize + EntryNonCmdFieldsSize),
		IncomingReadIndexQueueLength:        4096,
		IncomingProposalQueueLength:         2048,
		UnknownRegionName:                   "UNKNOWN",
		RaftNodeReceiveQueueLength:          1024,
		SnapshotStatusPushDelayMS:           1000,
		NodeCommitChanLength:                1024,
		SetDeploymentIDTimeoutSecond:        5,
		NodeHostSyncPoolSize:                8,
		CommitBatchSize:                     512,
		NodeReloadMillisecond:               200,
		StepEngineCommitWorkerCount:         16,
		StepEngineSnapshotWorkerCount:       64,
		StepEngineNodeReadyChanBufferSize:   8192,
		StepEngineCommitReadyChanBufferSize: 8192,
		StepEngineLocalKeySize:              1024,
		SendQueueLength:                     1024 * 8,
		SnapshotSendQueueLength:             4096 * 16,
		MaxSnapshotCount:                    128,
		MaxTransportStreamCount:             256,
		MaxDrummerServerMsgSize:             256 * 1024 * 1024,
		MaxDrummerClientMsgSize:             256 * 1024 * 1024,
		StreamConnections:                   4,
		InitialWindowSize:                   64 * 1024 * 1024,
		InitialConnWindowSize:               16 * 1024 * 1024,
		PerConnectionBufferSize:             64 * 1024 * 1024,
		PerCpnnectionRecvBufSize:            64 * 1024,
		SnapshotGCTick:                      30,
		SnapshotChunkTimeoutTick:            900,
		DrummerClientName:                   "drummer-client",
		NodeHostInfoReportSecond:            NodeHostInfoReportSecond,
		NodeHostTTL:                         NodeHostInfoReportSecond * 3,
		NodeToStartMaxWait:                  NodeHostInfoReportSecond * 12,
		DrummerLoopIntervalFactor:           1,
		PersisentLogReportCycle:             3,
		LogDBCacheSizePerWorker:             1024 * 1024 * 256,
		RDBMaxBackgroundCompactions:         2,
		RDBMaxBackgroundFlushes:             2,
		RDBLRUCacheSize:                     0,
		UseRangeDelete:                      true,
	}
}
