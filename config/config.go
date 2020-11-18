// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
Package config contains functions and types used for managing dragonboat's
configurations.
*/
package config

import (
	"crypto/tls"
	"errors"
	"path/filepath"
	"reflect"
	"time"

	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/stringutil"

	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	plog = logger.GetLogger("config")
)

const (
	// don't change these, see the comments on ExpertConfig.
	defaultExecShards  uint64 = 16
	defaultLogDBShards uint64 = 16
)

// TransportModule is the interface used to specify the transport module to be
// used by Dragonboat. TransportModule is optional, it is not required to be
// implemented when using Dragonboat's default built-in TCP based transport
// module.
type TransportModule interface {
	Create(NodeHostConfig,
		raftio.RequestHandler, raftio.IChunkHandler) raftio.IRaftRPC
	Validate(string) bool
}

// LogDBInfo is the info provided when LogDBCallback is invoked.
type LogDBInfo struct {
	Shard uint64
	Busy  bool
}

// LogDBCallback is called by the LogDB layer whenever NodeHost is required to
// be notified for the status change of the LogDB.
type LogDBCallback func(LogDBInfo)

// RaftRPCFactoryFunc is the factory function that creates the Raft RPC module
// instance for exchanging Raft messages between NodeHosts.
type RaftRPCFactoryFunc func(NodeHostConfig,
	raftio.RequestHandler, raftio.IChunkHandler) raftio.IRaftRPC

// LogDBFactoryFunc is the factory function that creates NodeHost's persistent
// storage module known as Log DB.
type LogDBFactoryFunc func(NodeHostConfig,
	LogDBCallback, []string, []string) (raftio.ILogDB, error)

// CompressionType is the type of the compression.
type CompressionType = pb.CompressionType

// IFS is the filesystem interface used by tests.
type IFS = vfs.IFS

const (
	// NoCompression is the CompressionType value used to indicate not to use
	// any compression.
	NoCompression CompressionType = pb.NoCompression
	// Snappy is the CompressionType value used to indicate that google snappy
	// is used for data compression.
	Snappy CompressionType = pb.Snappy
)

// Config is used to configure Raft nodes.
type Config struct {
	// NodeID is a non-zero value used to identify a node within a Raft cluster.
	NodeID uint64
	// ClusterID is the unique value used to identify a Raft cluster.
	ClusterID uint64
	// CheckQuorum specifies whether the leader node should periodically check
	// non-leader node status and step down to become a follower node when it no
	// longer has the quorum.
	CheckQuorum bool
	// ElectionRTT is the minimum number of message RTT between elections. Message
	// RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggests it
	// to be a magnitude greater than HeartbeatRTT, which is the interval between
	// two heartbeats. In Raft, the actual interval between elections is
	// randomized to be between ElectionRTT and 2 * ElectionRTT.
	//
	// As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond,
	// to set the election interval to be 1 second, then ElectionRTT should be set
	// to 10.
	//
	// When CheckQuorum is enabled, ElectionRTT also defines the interval for
	// checking leader quorum.
	ElectionRTT uint64
	// HeartbeatRTT is the number of message RTT between heartbeats. Message
	// RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the
	// heartbeat interval to be close to the average RTT between nodes.
	//
	// As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond,
	// to set the heartbeat interval to be every 200 milliseconds, then
	// HeartbeatRTT should be set to 2.
	HeartbeatRTT uint64
	// SnapshotEntries defines how often the state machine should be snapshotted
	// automcatically. It is defined in terms of the number of applied Raft log
	// entries. SnapshotEntries can be set to 0 to disable such automatic
	// snapshotting.
	//
	// When SnapshotEntries is set to N, it means a snapshot is created for
	// roughly every N applied Raft log entries (proposals). This also implies
	// that sending N log entries to a follower is more expensive than sending a
	// snapshot.
	//
	// Once a snapshot is generated, Raft log entries covered by the new snapshot
	// can be compacted. This involves two steps, redundant log entries are first
	// marked as deleted, then they are physically removed from the underlying
	// storage when a LogDB compaction is issued at a later stage. See the godoc
	// on CompactionOverhead for details on what log entries are actually removed
	// and compacted after generating a snapshot.
	//
	// Once automatic snapshotting is disabled by setting the SnapshotEntries
	// field to 0, users can still use NodeHost's RequestSnapshot or
	// SyncRequestSnapshot methods to manually request snapshots.
	SnapshotEntries uint64
	// CompactionOverhead defines the number of most recent entries to keep after
	// each Raft log compaction. Raft log compaction is performance automatically
	// every time when a snapshot is created.
	//
	// For example, when a snapshot is created at let's say index 10,000, then all
	// Raft log entries with index <= 10,000 can be removed from that node as they
	// have already been covered by the created snapshot image. This frees up the
	// maximum storage space but comes at the cost that the full snapshot will
	// have to be sent to the follower if the follower requires any Raft log entry
	// at index <= 10,000. When CompactionOverhead is set to say 500, Dragonboat
	// then compacts the Raft log up to index 9,500 and keeps Raft log entries
	// between index (9,500, 1,0000]. As a result, the node can still replicate
	// Raft log entries between index (9,500, 1,0000] to other peers and only fall
	// back to stream the full snapshot if any Raft log entry with index <= 9,500
	// is required to be replicated.
	CompactionOverhead uint64
	// OrderedConfigChange determines whether Raft membership change is enforced
	// with ordered config change ID.
	OrderedConfigChange bool
	// MaxInMemLogSize is the target size in bytes allowed for storing in memory
	// Raft logs on each Raft node. In memory Raft logs are the ones that have
	// not been applied yet.
	// MaxInMemLogSize is a target value implemented to prevent unbounded memory
	// growth, it is not for precisely limiting the exact memory usage.
	// When MaxInMemLogSize is 0, the target is set to math.MaxUint64. When
	// MaxInMemLogSize is set and the target is reached, error will be returned
	// when clients try to make new proposals.
	// MaxInMemLogSize is recommended to be significantly larger than the biggest
	// proposal you are going to use.
	MaxInMemLogSize uint64
	// SnapshotCompressionType is the compression type to use for compressing
	// generated snapshot data. No compression is used by default.
	SnapshotCompressionType CompressionType
	// EntryCompressionType is the compression type to use for compressing the
	// payload of user proposals. When Snappy is used, the maximum proposal
	// payload allowed is roughly limited to 3.42GBytes.
	EntryCompressionType CompressionType
	// DisableAutoCompactions disables auto compaction used for reclaiming Raft
	// entry storage spaces. By default, compaction is issued every time when
	// a snapshot is captured, this helps to reclaim disk spaces as soon as
	// possible at the cost of higher IO overhead. Users can disable such auto
	// compactions and use NodeHost.RequestCompaction to manually request such
	// compactions when necessary.
	DisableAutoCompactions bool
	// IsObserver indicates whether this is an observer Raft node without voting
	// power. Described as non-voting members in the section 4.2.1 of Diego
	// Ongaro's thesis, observer nodes are usually used to allow a new node to
	// join the cluster and catch up with other existing ndoes without impacting
	// the availability. Extra observer nodes can also be introduced to serve
	// read-only requests without affecting system write throughput.
	//
	// Observer support is currently experimental.
	IsObserver bool
	// IsWitness indicates whether this is a witness Raft node without actual log
	// replication and do not have state machine. It is mentioned in the section
	// 11.7.2 of Diego Ongaro's thesis.
	//
	// Witness support is currently experimental.
	IsWitness bool
	// Quiesce specifies whether to let the Raft cluster enter quiesce mode when
	// there is no cluster activity. Clusters in quiesce mode do not exchange
	// heartbeat messages to minimize bandwidth consumption.
	//
	// Quiesce support is currently experimental.
	Quiesce bool
}

// Validate validates the Config instance and return an error when any member
// field is considered as invalid.
func (c *Config) Validate() error {
	if c.NodeID == 0 {
		return errors.New("invalid NodeID, it must be >= 1")
	}
	if c.HeartbeatRTT == 0 {
		return errors.New("HeartbeatRTT must be > 0")
	}
	if c.ElectionRTT == 0 {
		return errors.New("ElectionRTT must be > 0")
	}
	if c.ElectionRTT <= 2*c.HeartbeatRTT {
		return errors.New("invalid election rtt")
	}
	if c.ElectionRTT < 10*c.HeartbeatRTT {
		plog.Warningf("ElectionRTT is not a magnitude larger than HeartbeatRTT")
	}
	if c.MaxInMemLogSize > 0 &&
		c.MaxInMemLogSize < settings.EntryNonCmdFieldsSize+1 {
		return errors.New("MaxInMemLogSize is too small")
	}
	if c.SnapshotCompressionType != Snappy &&
		c.SnapshotCompressionType != NoCompression {
		return errors.New("unknown compression type")
	}
	if c.EntryCompressionType != Snappy &&
		c.EntryCompressionType != NoCompression {
		return errors.New("unknown compression type")
	}
	if c.IsWitness && c.SnapshotEntries > 0 {
		return errors.New("witness node can not take snapshot")
	}
	if c.IsWitness && c.IsObserver {
		return errors.New("witness node can not be an observer")
	}
	return nil
}

// NodeHostConfig is the configuration used to configure NodeHost instances.
type NodeHostConfig struct {
	// DeploymentID is used to determine whether two NodeHost instances belong to
	// the same deployment and thus allowed to communicate with each other. This
	// helps to prvent accidentially misconfigured NodeHost instances to cause
	// data corruption errors by sending out of context messages to unrelated
	// Raft nodes.
	// For a particular dragonboat based application, you can set DeploymentID
	// to the same uint64 value on all production NodeHost instances, then use
	// different DeploymentID values on your staging and dev environment. It is
	// also recommended to use different DeploymentID values for different
	// dragonboat based applications.
	// When not set, the default value 0 will be used as the deployment ID and
	// thus allowing all NodeHost instances with deployment ID 0 to communicate
	// with each other.
	DeploymentID uint64
	// WALDir is the directory used for storing the WAL of Raft entries. It is
	// recommended to use low latency storage such as NVME SSD with power loss
	// protection to store such WAL data. Leave WALDir to have zero value will
	// have everything stored in NodeHostDir.
	WALDir string
	// NodeHostDir is where everything else is stored.
	NodeHostDir string
	// RTTMillisecond defines the average Rround Trip Time (RTT) in milliseconds
	// between two NodeHost instances. Such a RTT interval is internally used as
	// a logical clock tick, Raft heartbeat and election intervals are both
	// defined in term of how many such RTT intervals.
	// Note that RTTMillisecond is the combined delays between two NodeHost
	// instances including all delays caused by network transmission, delays
	// caused by NodeHost queuing and processing. As an example, when fully
	// loaded, the average Rround Trip Time between two of our NodeHost instances
	// used for benchmarking purposes is up to 500 microseconds when the ping time
	// between them is 100 microseconds. Set RTTMillisecond to 1 when it is less
	// than 1 million in your environment.
	RTTMillisecond uint64
	// RaftAddress is a hostname:port or IP:port address used by the Raft RPC
	// module for exchanging Raft messages and snapshots. This is also the
	// identifier for a NodeHost instance. RaftAddress should be set to the
	// public address that can be accessed from remote NodeHost instances.
	RaftAddress string
	// DynamicRaftAddress indicates that the RaftAddress of the NodeHost is
	// possible to change after restart.
	DynamicRaftAddress bool
	// ListenAddress is a hostname:port or IP:port address used by the Raft RPC
	// module to listen on for Raft message and snapshots. When the ListenAddress
	// field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0
	// is specified as the IP of the ListenAddress, Dragonboat listens to the
	// specified port on all interfaces. When hostname or domain name is
	// specified, it is locally resolved to IP addresses first and Dragonboat
	// listens to all resolved IP addresses.
	ListenAddress string
	// MutualTLS defines whether to use mutual TLS for authenticating servers
	// and clients. Insecure communication is used when MutualTLS is set to
	// False.
	// See https://github.com/lni/dragonboat/wiki/TLS-in-Dragonboat for more
	// details on how to use Mutual TLS.
	MutualTLS bool
	// CAFile is the path of the CA certificate file. This field is ignored when
	// MutualTLS is false.
	CAFile string
	// CertFile is the path of the node certificate file. This field is ignored
	// when MutualTLS is false.
	CertFile string
	// KeyFile is the path of the node key file. This field is ignored when
	// MutualTLS is false.
	KeyFile string
	// MaxSendQueueSize is the maximum size in bytes of each send queue.
	// Once the maximum size is reached, further replication messages will be
	// dropped to restrict memory usage. When set to 0, it means the send queue
	// size is unlimited.
	MaxSendQueueSize uint64
	// MaxReceiveQueueSize is the maximum size in bytes of each receive queue.
	// Once the maximum size is reached, further replication messages will be
	// dropped to restrict memory usage. When set to 0, it means the queue size
	// is unlimited.
	MaxReceiveQueueSize uint64
	// LogDBFactory is the factory function used for creating the Log DB instance
	// used by NodeHost. The default zero value causes the default built-in RocksDB
	// based Log DB implementation to be used.
	LogDBFactory LogDBFactoryFunc
	// RaftRPCFactory is the factory function used for creating the Raft RPC
	// instance for exchanging Raft message between NodeHost instances. The default
	// zero value causes the built-in TCP based RPC module to be used.
	//
	// Depreciated: Use TransportModule instead. NodeHostConig.RaftRPCFactory will
	// be removed in v4.0.
	RaftRPCFactory RaftRPCFactoryFunc
	// TransportModule is an optional field used to specify what transport module
	// to use. By default, the built-in TCP transport module is used.
	TransportModule TransportModule
	// EnableMetrics determines whether health metrics in Prometheus format should
	// be enabled.
	EnableMetrics bool
	// RaftEventListener is the listener for Raft events, such as Raft leadership
	// change, exposed to user space. NodeHost uses a single dedicated goroutine
	// to invoke all RaftEventListener methods one by one, CPU intensive or IO
	// related procedures that can cause long delays should be offloaded to worker
	// goroutines managed by users. See the raftio.IRaftEventListener definition
	// for more details.
	RaftEventListener raftio.IRaftEventListener
	// MaxSnapshotSendBytesPerSecond defines how much snapshot data can be sent
	// every second for all Raft clusters managed by the NodeHost instance.
	// The default value 0 means there is no limit set for snapshot streaming.
	MaxSnapshotSendBytesPerSecond uint64
	// MaxSnapshotRecvBytesPerSecond defines how much snapshot data can be
	// received each second for all Raft clusters managed by the NodeHost instance.
	// The default value 0 means there is no limit for receiving snapshot data.
	MaxSnapshotRecvBytesPerSecond uint64
	// FS is the filesystem used by tests. Dragonboat applications are not
	// required to explicitly set this field.
	FS IFS
	// SystemEventsListener allows users to be notified for system events such
	// as snapshot creation, log compaction and snapshot streaming. It is usually
	// used for testing purposes or for other advanced usages, Dragonboat
	// applications are not required to explicitly set this field.
	SystemEventListener raftio.ISystemEventListener
	// SystemTickerPrecision is the precision of the system ticker. This value is
	// usually set in tests. Dragonboat applications are not required to
	// explicitly set this field.
	SystemTickerPrecision time.Duration
	// LogDB is the configuration object for the LogDB storage engine. LogDB
	// is used for storing Raft Logs and other metadata. This is an option for
	// advanced users when tuning the balance of I/O performance and memory
	// consumption. Regular users are recommdned not to set this field to use it
	// default value.
	LogDB LogDBConfig
	// Expert contains configuration options for expert users who are familiar
	// with the internal of the Dragonboat library. Regular users are expected
	// not to use ExpertConfig. It is important to understand that any change
	// to the ExpertConfig may cause an existing Dragonboat setup unable to
	// restart.
	Expert ExpertConfig
	// NotifyCommit specifies whether clients should be notified when their
	// regular proposals and config change requests are committed. By default,
	// commits are not notified, clients are only notified when their proposals
	// are both committed and applied.
	NotifyCommit bool
}

// Validate validates the NodeHostConfig instance and return an error when
// the configuration is considered as invalid.
func (c *NodeHostConfig) Validate() error {
	if c.RTTMillisecond == 0 {
		return errors.New("invalid RTTMillisecond")
	}
	if len(c.NodeHostDir) == 0 {
		return errors.New("NodeHostConfig.NodeHostDir is empty")
	}
	if !stringutil.IsValidAddress(c.RaftAddress) {
		return errors.New("invalid NodeHost address")
	}
	if len(c.ListenAddress) > 0 && !stringutil.IsValidAddress(c.ListenAddress) {
		return errors.New("invalid ListenAddress")
	}
	if !c.MutualTLS &&
		(len(c.CAFile) > 0 || len(c.CertFile) > 0 || len(c.KeyFile) > 0) {
		plog.Warningf("CAFile/CertFile/KeyFile specified when MutualTLS is disabled")
	}
	if c.MutualTLS {
		if len(c.CAFile) == 0 {
			return errors.New("CA file not specified")
		}
		if len(c.CertFile) == 0 {
			return errors.New("cert file not specified")
		}
		if len(c.KeyFile) == 0 {
			return errors.New("key file not specified")
		}
	}
	if c.MaxSendQueueSize > 0 &&
		c.MaxSendQueueSize < settings.EntryNonCmdFieldsSize+1 {
		return errors.New("MaxSendQueueSize value is too small")
	}
	if c.MaxReceiveQueueSize > 0 &&
		c.MaxReceiveQueueSize < settings.EntryNonCmdFieldsSize+1 {
		return errors.New("MaxReceiveSize value is too small")
	}
	if c.RaftRPCFactory != nil && c.TransportModule != nil {
		return errors.New("both TransportModule and RaftRPCFactory specified")
	}
	if c.DynamicRaftAddress && c.TransportModule == nil {
		return errors.New("NodeHostConfig.TransportModule not set")
	}
	return nil
}

type transportModule struct {
	factory RaftRPCFactoryFunc
}

func (tm *transportModule) Create(nhConfig NodeHostConfig,
	handler raftio.RequestHandler,
	chunkHandler raftio.IChunkHandler) raftio.IRaftRPC {
	return tm.factory(nhConfig, handler, chunkHandler)
}

func (tm *transportModule) Validate(addr string) bool {
	return stringutil.IsValidAddress(addr)
}

// Prepare sets the default value for NodeHostConfig.
func (c *NodeHostConfig) Prepare() error {
	var err error
	c.NodeHostDir, err = filepath.Abs(c.NodeHostDir)
	if err != nil {
		return err
	}
	if len(c.WALDir) > 0 {
		c.WALDir, err = filepath.Abs(c.WALDir)
		if err != nil {
			return err
		}
	}
	if c.FS == nil {
		c.FS = vfs.DefaultFS
	}
	if c.SystemTickerPrecision == 0 {
		plog.Infof("system ticker precision is set to 1ms (default)")
		c.SystemTickerPrecision = time.Millisecond
	}
	if c.LogDB.IsEmpty() {
		plog.Infof("using default LogDBConfig")
		c.LogDB = GetDefaultLogDBConfig()
	}
	if c.RaftRPCFactory != nil && c.TransportModule == nil {
		c.TransportModule = &transportModule{factory: c.RaftRPCFactory}
		c.RaftRPCFactory = nil
	}
	if c.Expert.IsEmpty() {
		plog.Infof("using default ExpertConfig")
		c.Expert = GetDefaultExpertConfig()
	}
	c.LogDB.expert = c.Expert
	return nil
}

// GetListenAddress returns the actual address the RPC module is going to
// listen on.
func (c *NodeHostConfig) GetListenAddress() string {
	if len(c.ListenAddress) > 0 {
		return c.ListenAddress
	}
	return c.RaftAddress
}

// GetServerTLSConfig returns the server tls.Config instance based on the
// TLS settings in NodeHostConfig.
func (c *NodeHostConfig) GetServerTLSConfig() (*tls.Config, error) {
	if c.MutualTLS {
		return netutil.GetServerTLSConfig(c.CAFile, c.CertFile, c.KeyFile)
	}
	return nil, nil
}

// GetClientTLSConfig returns the client tls.Config instance for the specified
// target based on the TLS settings in NodeHostConfig.
func (c *NodeHostConfig) GetClientTLSConfig(target string) (*tls.Config, error) {
	if c.MutualTLS {
		tlsConfig, err := netutil.GetClientTLSConfig("",
			c.CAFile, c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		host, err := netutil.GetHost(target)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			ServerName:   host,
			Certificates: tlsConfig.Certificates,
			RootCAs:      tlsConfig.RootCAs,
		}, nil
	}
	return nil, nil
}

// GetDeploymentID returns the deployment ID to be used.
func (c *NodeHostConfig) GetDeploymentID() uint64 {
	if c.DeploymentID == 0 {
		return settings.UnmanagedDeploymentID
	}
	return c.DeploymentID
}

// IsValidAddress returns a boolean value indicating whether the input address
// is valid.
func IsValidAddress(addr string) bool {
	return stringutil.IsValidAddress(addr)
}

// LogDBConfig is the configuration object for the LogDB storage engine. This
// config option is only for advanced users when tuning the balance of I/O
// performance and memory consumption.
//
// All KV* fields in LogDBConfig had their names derived from RocksDB options,
// please check RocksDB Tuning Guide wiki for more details.
//
// KVWriteBufferSize and KVMaxWriteBufferNumber are two parameters that directly
// affect the upper bound of memory size used by the built-in LogDB storage
// engine.
type LogDBConfig struct {
	expert                             ExpertConfig
	KVKeepLogFileNum                   uint64
	KVMaxBackgroundCompactions         uint64
	KVMaxBackgroundFlushes             uint64
	KVLRUCacheSize                     uint64
	KVWriteBufferSize                  uint64
	KVMaxWriteBufferNumber             uint64
	KVLevel0FileNumCompactionTrigger   uint64
	KVLevel0SlowdownWritesTrigger      uint64
	KVLevel0StopWritesTrigger          uint64
	KVMaxBytesForLevelBase             uint64
	KVMaxBytesForLevelMultiplier       uint64
	KVTargetFileSizeBase               uint64
	KVTargetFileSizeMultiplier         uint64
	KVLevelCompactionDynamicLevelBytes uint64
	KVRecycleLogFileNum                uint64
	KVNumOfLevels                      uint64
	KVBlockSize                        uint64
	SaveBufferSize                     uint64
	MaxSaveBufferSize                  uint64
}

// GetDefaultLogDBConfig returns the default configurations for the LogDB
// storage engine. The default LogDB can use up to 8GBytes memory.
func GetDefaultLogDBConfig() LogDBConfig {
	return getDefaultLogDBConfig()
}

// GetTinyMemLogDBConfig returns a LogDB config aimed for minimizing memory
// size. When using the returned config, LogDB takes up to 256MBytes memory.
func GetTinyMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 4 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetSmallMemLogDBConfig returns a LogDB config aimed to keep memory size at
// low level. When using the returned config, LogDB takes up to 1GBytes memory.
func GetSmallMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 16 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetMediumMemLogDBConfig returns a LogDB config aimed to keep memory size at
// medium level. When using the returned config, LogDB takes up to 4GBytes
// memory.
func GetMediumMemLogDBConfig() LogDBConfig {
	cfg := getDefaultLogDBConfig()
	cfg.KVWriteBufferSize = 64 * 1024 * 1024
	cfg.KVMaxWriteBufferNumber = 4
	return cfg
}

// GetLargeMemLogDBConfig returns a LogDB config aimed to keep memory size to be
// large for good I/O performance. It is the default setting used by the system.
// When using the returned config, LogDB takes up to 8GBytes memory.
func GetLargeMemLogDBConfig() LogDBConfig {
	return getDefaultLogDBConfig()
}

func getDefaultLogDBConfig() LogDBConfig {
	return LogDBConfig{
		KVMaxBackgroundCompactions:         2,
		KVMaxBackgroundFlushes:             2,
		KVLRUCacheSize:                     0,
		KVKeepLogFileNum:                   16,
		KVWriteBufferSize:                  128 * 1024 * 1024,
		KVMaxWriteBufferNumber:             4,
		KVLevel0FileNumCompactionTrigger:   8,
		KVLevel0SlowdownWritesTrigger:      17,
		KVLevel0StopWritesTrigger:          24,
		KVMaxBytesForLevelBase:             4 * 1024 * 1024 * 1024,
		KVMaxBytesForLevelMultiplier:       2,
		KVTargetFileSizeBase:               16 * 1024 * 1024,
		KVTargetFileSizeMultiplier:         2,
		KVLevelCompactionDynamicLevelBytes: 0,
		KVRecycleLogFileNum:                0,
		KVNumOfLevels:                      7,
		KVBlockSize:                        32 * 1024,
		SaveBufferSize:                     32 * 1024,
		MaxSaveBufferSize:                  64 * 1024 * 1024,
	}
}

// MemorySizeMB returns the estimated upper bound memory size used by the LogDB
// storage engine. The returned value is in MBytes.
func (cfg *LogDBConfig) MemorySizeMB() uint64 {
	if cfg.expert.IsEmpty() {
		panic("uninitialized expert config")
	}
	ss := cfg.KVWriteBufferSize * cfg.KVMaxWriteBufferNumber
	bs := ss * cfg.expert.LogDBShards
	return bs / (1024 * 1024)
}

// IsEmpty returns a boolean value indicating whether the LogDBConfig instance
// is empty.
func (cfg *LogDBConfig) IsEmpty() bool {
	empty := LogDBConfig{}
	return reflect.DeepEqual(cfg, &empty)
}

// GetDefaultExpertConfig returns the default ExpertConfig.
func GetDefaultExpertConfig() ExpertConfig {
	return ExpertConfig{
		ExecShards:  defaultExecShards,
		LogDBShards: defaultLogDBShards,
	}
}

// ExpertConfig contains configuration options for expert users who are familiar
// with the internal of the Dragonboat library. Regular users are expected not
// to use ExpertConfig.
type ExpertConfig struct {
	// ExecShards is the number of execution shards in the first stage of the
	// execution engine. Default value is 16.
	ExecShards uint64
	// LogDBShards is the number of LogDB shards. Default value is 16.
	LogDBShards uint64
}

// IsEmpty returns a boolean value indicating whether the ExpertConfig instance
// is a default one with all zero values.
func (e *ExpertConfig) IsEmpty() bool {
	return e.ExecShards == 0 && e.LogDBShards == 0
}
