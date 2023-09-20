// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/stringutil"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/id"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("config")
)

const (
	// don't change these, see the comments on ExpertConfig.
	defaultExecShards  uint64 = 16
	defaultLogDBShards uint64 = 16
)

// CompressionType is the type of the compression.
type CompressionType = pb.CompressionType

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
	// ReplicaID is a non-zero value used to identify a node within a Raft shard.
	ReplicaID uint64
	// ShardID is the unique value used to identify a Raft group that contains
	// multiple replicas.
	ShardID uint64
	// CheckQuorum specifies whether the leader node should periodically check
	// non-leader node status and step down to become a follower node when it no
	// longer has the quorum.
	CheckQuorum bool
	// Whether to use PreVote for this node. PreVote is described in the section
	// 9.7 of the raft thesis.
	PreVote bool
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
	// automatically. It is defined in terms of the number of applied Raft log
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
	// each Raft log compaction. Raft log compaction is performed automatically
	// every time a snapshot is created.
	//
	// For example, when a snapshot is created at let's say index 10,000, then all
	// Raft log entries with index <= 10,000 can be removed from that node as they
	// have already been covered by the created snapshot image. This frees up the
	// maximum storage space but comes at the cost that the full snapshot will
	// have to be sent to the follower if the follower requires any Raft log entry
	// at index <= 10,000. When CompactionOverhead is set to say 500, Dragonboat
	// then compacts the Raft log up to index 9,500 and keeps Raft log entries
	// between index (9,500, 10,000]. As a result, the node can still replicate
	// Raft log entries between index (9,500, 10,000] to other peers and only fall
	// back to stream the full snapshot if any Raft log entry with index <= 9,500
	// is required to be replicated.
	CompactionOverhead uint64
	// OrderedConfigChange determines whether Raft membership change is enforced
	// with ordered config change ID.
	//
	// When set to true, ConfigChangeIndex is required for membership change
	// requests. This behaves like an optimistic write lock forcing clients to
	// linearize membership change requests explicitly. (recommended)
	//
	// When set to false (default), ConfigChangeIndex is ignored for membership
	// change requests. This may cause a client to request a membership change
	// based on stale membership data.
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
	// payload allowed is roughly limited to 3.42GBytes. No compression is used
	// by default.
	EntryCompressionType CompressionType
	// DisableAutoCompactions disables auto compaction used for reclaiming Raft
	// log entry storage spaces. By default, compaction request is issued every
	// time when a snapshot is created, this helps to reclaim disk spaces as
	// soon as possible at the cost of immediate higher IO overhead. Users can
	// disable such auto compactions and use NodeHost.RequestCompaction to
	// manually request such compactions when necessary.
	DisableAutoCompactions bool
	// IsNonVoting indicates whether this is a non-voting Raft node. Described as
	// non-voting members in the section 4.2.1 of Diego Ongaro's thesis, they are
	// used to allow a new node to join the shard and catch up with other
	// existing ndoes without impacting the availability. Extra non-voting nodes
	// can also be introduced to serve read-only requests.
	IsNonVoting bool
	// IsObserver indicates whether this is a non-voting Raft node without voting
	// power.
	//
	// Deprecated: use IsNonVoting instead.
	IsObserver bool
	// IsWitness indicates whether this is a witness Raft node without actual log
	// replication and do not have state machine. It is mentioned in the section
	// 11.7.2 of Diego Ongaro's thesis.
	//
	// Witness support is currently experimental.
	IsWitness bool
	// Quiesce specifies whether to let the Raft shard enter quiesce mode when
	// there is no shard activity. Shards in quiesce mode do not exchange
	// heartbeat messages to minimize bandwidth consumption.
	//
	// Quiesce support is currently experimental.
	Quiesce bool
	// WaitReady specifies whether to wait for the node to transition
	// from recovering to ready state before returning from StartReplica.
	WaitReady bool
}

// Validate validates the Config instance and return an error when any member
// field is considered as invalid.
func (c *Config) Validate() error {
	if c.ReplicaID == 0 {
		return errors.New("invalid ReplicaID, it must be >= 1")
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
	if c.IsObserver {
		c.IsNonVoting = true
	}
	if c.IsWitness && c.IsNonVoting {
		return errors.New("witness node can not be a non-voting node")
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
	// NodeHostID specifies what NodeHostID to use. By default, when NodeHostID
	// is empty, a random UUID will be generated and recorded by the system.
	// Specifying a concrete NodeHostID here will cause the specified NodeHostID
	// value to be used. NodeHostID is only used when DefaultNodeRegistryEnabled is
	// set to true.
	NodeHostID string
	// WALDir is the directory used for storing the WAL of Raft entries. It is
	// recommended to use low latency storage such as NVME SSD with power loss
	// protection to store such WAL data. Leave WALDir to have zero value will
	// have everything stored in NodeHostDir.
	WALDir string
	// NodeHostDir is where everything else is stored.
	NodeHostDir string
	// RTTMillisecond defines the average Round Trip Time (RTT) in milliseconds
	// between two NodeHost instances. Such a RTT interval is internally used as
	// a logical clock tick, Raft heartbeat and election intervals are both
	// defined in terms of how many such logical clock ticks (RTT intervals).
	// Note that RTTMillisecond is the combined delays between two NodeHost
	// instances including all delays caused by network transmission, delays
	// caused by NodeHost queuing and processing. As an example, when fully
	// loaded, the average Round Trip Time between two of our NodeHost instances
	// used for benchmarking purposes is up to 500 microseconds when the ping time
	// between them is 100 microseconds. Set RTTMillisecond to 1 when it is less
	// than 1 million in your environment.
	RTTMillisecond uint64
	// RaftAddress is a DNS name:port or IP:port address used by the transport
	// module for exchanging Raft messages, snapshots and metadata between
	// NodeHost instances. It should be set to the public address that can be
	// accessed from remote NodeHost instances.
	//
	// When the NodeHostConfig.ListenAddress field is empty, NodeHost listens on
	// RaftAddress for incoming Raft messages. When hostname or domain name is
	// used, it will be resolved to IPv4 addresses first and Dragonboat listens
	// to all resolved IPv4 addresses.
	//
	// By default, the RaftAddress value is not allowed to change between NodeHost
	// restarts. DefaultNodeRegistryEnabled should be set to true when the RaftAddress
	// value might change after restart.
	RaftAddress string
	// DefaultNodeRegistryEnabled indicates that NodeHost instances should be addressed
	// by their NodeHostID values. This feature is usually used when only dynamic
	// addresses are available. When enabled, NodeHostID values should be used
	// as the target parameter when calling NodeHost's StartReplica,
	// RequestAddReplica, RequestAddNonVoting and RequestAddWitness methods.
	//
	// Enabling DefaultNodeRegistryEnabled also enables the internal gossip service,
	// NodeHostConfig.Gossip must be configured to control the behaviors of the
	// gossip service.
	//
	// Note that once enabled, the DefaultNodeRegistryEnabled setting can not be later
	// disabled after restarts.
	//
	// Please see the godocs of the NodeHostConfig.Gossip field for a detailed
	// example on how DefaultNodeRegistryEnabled and gossip works.
	DefaultNodeRegistryEnabled bool
	// ListenAddress is an optional field in the hostname:port or IP:port address
	// form used by the transport module to listen on for Raft message and
	// snapshots. When the ListenAddress field is not set, The transport module
	// listens on RaftAddress. If 0.0.0.0 is specified as the IP of the
	// ListenAddress, Dragonboat listens to the specified port on all network
	// interfaces. When hostname or domain name is used, it will be resolved to
	// IPv4 addresses first and Dragonboat listens to all resolved IPv4 addresses.
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
	// LogDBFactory is the factory function used for creating the Log DB instance
	// used by NodeHost. The default zero value causes the default built-in RocksDB
	// based Log DB implementation to be used.
	//
	// Deprecated: Use NodeHostConfig.Expert.LogDBFactory instead.
	LogDBFactory LogDBFactoryFunc
	// RaftRPCFactory is the factory function used for creating the transport
	// instance for exchanging Raft message between NodeHost instances. The default
	// zero value causes the built-in TCP based transport to be used.
	//
	// Deprecated: Use NodeHostConfig.Expert.TransportFactory instead.
	RaftRPCFactory RaftRPCFactoryFunc
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
	// SystemEventsListener allows users to be notified for system events such
	// as snapshot creation, log compaction and snapshot streaming. It is usually
	// used for testing purposes or for other advanced usages, Dragonboat
	// applications are not required to explicitly set this field.
	SystemEventListener raftio.ISystemEventListener
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
	// NotifyCommit specifies whether clients should be notified when their
	// regular proposals and config change requests are committed. By default,
	// commits are not notified, clients are only notified when their proposals
	// are both committed and applied.
	NotifyCommit bool
	// Gossip contains configurations for the gossip service. When the
	// DefaultNodeRegistryEnabled field is set to true, each NodeHost instance will use
	// an internal gossip service to exchange knowledges of known NodeHost
	// instances including their RaftAddress and NodeHostID values. This Gossip
	// field contains configurations that controls how the gossip service works.
	//
	// As an detailed example on how to use the gossip service in the situation
	// where all available machines have dynamically assigned IPs on reboot -
	//
	// Consider that there are three NodeHost instances on three machines, each
	// of them has a dynamically assigned IP address which will change on reboot.
	// NodeHostConfig.RaftAddress should be set to the current address that can be
	// reached by remote NodeHost instance. In this example, we will assume they
	// are
	//
	// 10.0.0.100:24000
	// 10.0.0.200:24000
	// 10.0.0.300:24000
	//
	// To use these machines, first enable the NodeHostConfig.DefaultNodeRegistryEnabled
	// field and start the NodeHost instances. The NodeHostID value of each
	// NodeHost instance can be obtained by calling NodeHost.ID(). Let's say they
	// are
	//
	// "nhid-xxxxx",
	// "nhid-yyyyy",
	// "nhid-zzzzz".
	//
	// All these NodeHostID are fixed, they will never change after reboots.
	//
	// When starting Raft nodes or requesting new nodes to be added, use the above
	// mentioned NodeHostID values as the target parameters (which are of the
	// Target type). Let's say we want to start a Raft Node as a part of a three
	// replica Raft shard, the initialMembers parameter of the StartReplica
	// method can be set to
	//
	// initialMembers := map[uint64]Target {
	// 	 1: "nhid-xxxxx",
	//   2: "nhid-yyyyy",
	//   3: "nhid-zzzzz",
	// }
	//
	// This indicates that node 1 of the shard will be running on the NodeHost
	// instance identified by the NodeHostID value "nhid-xxxxx", node 2 of the
	// same shard will be running on the NodeHost instance identified by the
	// NodeHostID value of "nhid-yyyyy" and so on.
	//
	// The internal gossip service exchanges NodeHost details, including their
	// NodeHostID and RaftAddress values, with all other known NodeHost instances.
	// Thanks to the nature of gossip, it will eventually allow each NodeHost
	// instance to be aware of the current details of all NodeHost instances.
	// As a result, let's say when Raft node 1 wants to send a Raft message to
	// node 2, it first figures out that node 2 is running on the NodeHost
	// identified by the NodeHostID value "nhid-yyyyy", RaftAddress information
	// from the gossip service further shows that "nhid-yyyyy" maps to a machine
	// currently reachable at 10.0.0.200:24000. Raft messages can thus be
	// delivered.
	//
	// The Gossip field here is used to configure how the gossip service works.
	// In this example, let's say we choose to use the following configurations
	// for those three NodeHost instaces.
	//
	// GossipConfig {
	//   BindAddress: "10.0.0.100:24001",
	//   Seed: []string{10.0.0.200:24001},
	// }
	//
	// GossipConfig {
	//   BindAddress: "10.0.0.200:24001",
	//   Seed: []string{10.0.0.300:24001},
	// }
	//
	// GossipConfig {
	//   BindAddress: "10.0.0.300:24001",
	//   Seed: []string{10.0.0.100:24001},
	// }
	//
	// For those three machines, the gossip component listens on
	// "10.0.0.100:24001", "10.0.0.200:24001" and "10.0.0.300:24001" respectively
	// for incoming gossip messages. The Seed field is a list of known gossip end
	// points the local gossip service will try to talk to. The Seed field doesn't
	// need to include all gossip end points, a few well connected nodes in the
	// gossip network is enough.
	//
	// Alternatively, if you wish to use a custom registry but manage it yourself,
	// the Expert.NodeRegistryFactory field can be set to provide a registry that
	// implements the raftio.INodeRegistry interface. A registry is simply a common
	// channel shared between all nodes that allows them to identify each other.
	Gossip GossipConfig

	// Expert contains options for expert users who are familiar with the internals
	// of Dragonboat. Users are recommended not to use this field unless
	// absolutely necessary. It is important to note that any change to this field
	// may cause an existing instance unable to restart, it may also cause negative
	// performance impacts.
	Expert ExpertConfig
}

// IFS is the filesystem interface used by tests.
type IFS = vfs.IFS

// TargetValidator is the validtor used to validate user specified target values.
type TargetValidator func(string) bool

// RaftAddressValidator is the validator used to validate user specified
// RaftAddress values.
type RaftAddressValidator func(string) bool

// LogDBFactory is the interface used for creating custom logdb modules.
type LogDBFactory interface {
	// Create creates a logdb module.
	Create(NodeHostConfig,
		LogDBCallback, []string, []string) (raftio.ILogDB, error)
	// Name returns the type name of the logdb module.
	Name() string
}

// NodeRegistryFactory is the interface used for providing a custom node registry.
// For a short example of how to implement a custom node registry, please see
// TestExternalNodeRegistryFunction in nodehost_test.go.
type NodeRegistryFactory interface {
	Create(nhid string, streamConnections uint64, v TargetValidator) (raftio.INodeRegistry, error)
}

// TransportFactory is the interface used for creating custom transport modules.
type TransportFactory interface {
	// Create creates a transport module.
	Create(NodeHostConfig,
		raftio.MessageHandler, raftio.ChunkHandler) raftio.ITransport
	// Validate validates the RaftAddress of the NodeHost. When using a custom
	// transport module, users are granted full control on what address type to
	// use for the NodeHostConfig.RaftAddress field, it can be of the traditional
	// IP:Port format or any other form. The Validate method is used to validate
	// that a received address is of the valid form.
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

// RaftRPCFactoryFunc is the factory function that creates the transport module
// instance for exchanging Raft messages between NodeHosts.
//
// Deprecated: Use TransportFactory instead.
type RaftRPCFactoryFunc func(NodeHostConfig,
	raftio.MessageHandler, raftio.ChunkHandler) raftio.ITransport

// LogDBFactoryFunc is the factory function that creates NodeHost's persistent
// storage module known as Log DB.
//
// Deprecated: Use LogDBFactory instead.
type LogDBFactoryFunc func(NodeHostConfig,
	LogDBCallback, []string, []string) (raftio.ILogDB, error)

// Validate validates the NodeHostConfig instance and return an error when
// the configuration is considered as invalid.
func (c *NodeHostConfig) Validate() error {
	if c.RTTMillisecond == 0 {
		return errors.New("invalid RTTMillisecond")
	}
	if len(c.NodeHostDir) == 0 {
		return errors.New("NodeHostConfig.NodeHostDir is empty")
	}
	if !c.MutualTLS {
		plog.Warningf("mutual TLS disabled, communication is insecure")
		if len(c.CAFile) > 0 || len(c.CertFile) > 0 || len(c.KeyFile) > 0 {
			plog.Warningf("CAFile/CertFile/KeyFile specified when MutualTLS is disabled")
		}
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
	if c.RaftRPCFactory != nil && c.Expert.TransportFactory != nil {
		return errors.New("both TransportFactory and RaftRPCFactory specified")
	}
	if c.LogDBFactory != nil && c.Expert.LogDBFactory != nil {
		return errors.New("both LogDBFactory and Expert.LogDBFactory specified")
	}
	if c.DefaultNodeRegistryEnabled && c.Gossip.IsEmpty() {
		return errors.New("gossip service not configured")
	}
	validate := c.GetRaftAddressValidator()
	if !validate(c.RaftAddress) {
		return errors.New("invalid NodeHost address")
	}
	if len(c.ListenAddress) > 0 && !validate(c.ListenAddress) {
		return errors.New("invalid ListenAddress")
	}
	if !c.Gossip.IsEmpty() {
		if err := c.Gossip.Validate(); err != nil {
			return err
		}
	}
	if !c.Expert.Engine.IsEmpty() {
		if err := c.Expert.Engine.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type defaultTransport struct {
	factory RaftRPCFactoryFunc
}

func (tm *defaultTransport) Create(nhConfig NodeHostConfig,
	handler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return tm.factory(nhConfig, handler, chunkHandler)
}

func (tm *defaultTransport) Validate(addr string) bool {
	return stringutil.IsValidAddress(addr)
}

type defaultLogDB struct {
	factory LogDBFactoryFunc
}

func (l *defaultLogDB) Create(nhConfig NodeHostConfig,
	cb LogDBCallback, dirs []string, wals []string) (raftio.ILogDB, error) {
	return l.factory(nhConfig, cb, dirs, wals)
}

func (l *defaultLogDB) Name() string {
	fs := vfs.DefaultFS
	dir, err := fileutil.TempDir("", "dragonboat-logdb-test", fs)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fs.RemoveAll(dir); err != nil {
			panic(err)
		}
	}()
	nhc := NodeHostConfig{
		Expert: ExpertConfig{
			LogDB: GetDefaultLogDBConfig(),
			FS:    fs,
		},
	}
	ldb, err := l.factory(nhc, nil, []string{dir}, []string{})
	if err != nil {
		plog.Panicf("failed to create ldb, %v", err)
	}
	defer func() {
		if err := ldb.Close(); err != nil {
			panic(err)
		}
	}()
	return ldb.Name()
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
	if c.Expert.FS == nil {
		c.Expert.FS = vfs.DefaultFS
	}
	if c.Expert.Engine.IsEmpty() {
		plog.Infof("using default EngineConfig")
		c.Expert.Engine = GetDefaultEngineConfig()
	}
	if c.Expert.LogDB.IsEmpty() {
		plog.Infof("using default LogDBConfig")
		c.Expert.LogDB = GetDefaultLogDBConfig()
	}
	if c.RaftRPCFactory != nil && c.Expert.TransportFactory == nil {
		c.Expert.TransportFactory = &defaultTransport{factory: c.RaftRPCFactory}
		c.RaftRPCFactory = nil
	}
	if c.LogDBFactory != nil && c.Expert.LogDBFactory == nil {
		c.Expert.LogDBFactory = &defaultLogDB{factory: c.LogDBFactory}
		c.LogDBFactory = nil
	}
	return nil
}

// NodeRegistryEnabled returns a bool indicating if any node registry is enabled.
func (c *NodeHostConfig) NodeRegistryEnabled() bool {
	return c.DefaultNodeRegistryEnabled || c.Expert.NodeRegistryFactory != nil
}

// GetListenAddress returns the actual address the transport module is going to
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

// GetTargetValidator returns a TargetValidator based on the specified
// NodeHostConfig instance.
func (c *NodeHostConfig) GetTargetValidator() TargetValidator {
	if c.NodeRegistryEnabled() {
		return id.IsNodeHostID
	} else if c.Expert.TransportFactory != nil {
		return c.Expert.TransportFactory.Validate
	}
	return stringutil.IsValidAddress
}

// GetRaftAddressValidator creates a RaftAddressValidator based on the specified
// NodeHostConfig instance.
func (c *NodeHostConfig) GetRaftAddressValidator() RaftAddressValidator {
	if c.Expert.TransportFactory != nil {
		return c.Expert.TransportFactory.Validate
	}
	return stringutil.IsValidAddress
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
	Shards                             uint64
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
// storage engine. The default LogDB configuration use up to 8GBytes memory.
func GetDefaultLogDBConfig() LogDBConfig {
	return GetLargeMemLogDBConfig()
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
		Shards:                             defaultLogDBShards,
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
	ss := cfg.KVWriteBufferSize * cfg.KVMaxWriteBufferNumber
	bs := ss * cfg.Shards
	return bs / (1024 * 1024)
}

// IsEmpty returns a boolean value indicating whether the LogDBConfig instance
// is empty.
func (cfg *LogDBConfig) IsEmpty() bool {
	return reflect.DeepEqual(cfg, &LogDBConfig{})
}

// EngineConfig is the configuration for the execution engine.
type EngineConfig struct {
	// ExecShards is the number of execution shards in the first stage of the
	// execution engine. Default value is 16. Once deployed, this value can not
	// be changed later.
	ExecShards uint64
	// CommitShards is the number of commit shards in the second stage of the
	// execution engine. Default value is 16.
	CommitShards uint64
	// ApplyShards is the number of apply shards in the third stage of the
	// execution engine. Default value is 16.
	ApplyShards uint64
	// SnapshotShards is the number of snapshot shards in the forth stage of the
	// execution engine. Default value is 48.
	SnapshotShards uint64
	// CloseShards is the number of close shards used for closing stopped
	// state machines. Default value is 32.
	CloseShards uint64
}

// GetDefaultEngineConfig returns the default EngineConfig instance.
func GetDefaultEngineConfig() EngineConfig {
	return EngineConfig{
		ExecShards:     defaultExecShards,
		CommitShards:   16,
		ApplyShards:    16,
		SnapshotShards: 48,
		CloseShards:    32,
	}
}

// IsEmpty returns a boolean value indicating whether EngineConfig is an empty
// one.
func (ec EngineConfig) IsEmpty() bool {
	return reflect.DeepEqual(&ec, &EngineConfig{})
}

// Validate return an error value when the EngineConfig is invalid.
func (ec EngineConfig) Validate() error {
	if ec.ExecShards == 0 || ec.CommitShards == 0 || ec.ApplyShards == 0 ||
		ec.SnapshotShards == 0 || ec.CloseShards == 0 {
		return errors.New("invalid engine configuration")
	}
	return nil
}

// GetDefaultExpertConfig returns the default ExpertConfig.
func GetDefaultExpertConfig() ExpertConfig {
	return ExpertConfig{
		Engine: GetDefaultEngineConfig(),
		LogDB:  getDefaultLogDBConfig(),
	}
}

// ExpertConfig contains options for expert users who are familiar with the
// internals of Dragonboat. Users are recommended not to set ExpertConfig
// unless it is absoloutely necessary.
type ExpertConfig struct {
	// LogDBFactory is the factory function used for creating the LogDB instance
	// used by NodeHost. When not set, the default built-in Pebble based LogDB
	// implementation is used.
	LogDBFactory LogDBFactory
	// TransportFactory is an optional factory type used for creating the custom
	// transport module to be used by dragonbaot. When not set, the built-in TCP
	// transport module is used.
	TransportFactory TransportFactory
	// Engine is the configuration for the execution engine.
	Engine EngineConfig
	// LogDB contains configuration options for the LogDB storage engine. LogDB
	// is used for storing Raft Logs and metadata. This optional option is used
	// by advanced users for tuning the balance of I/O performance, memory and
	// disk usages.
	LogDB LogDBConfig
	// FS is the filesystem instance used in tests.
	FS IFS
	// TestGossipProbeInterval defines the probe interval used by the gossip
	// service in tests.
	TestGossipProbeInterval time.Duration
	// NodeRegistryFactory defines a custom node registry function that can be used
	// instead of a static registry or the built in memberlist gossip mechanism.
	NodeRegistryFactory NodeRegistryFactory
}

// GossipConfig contains configurations for the gossip service. Gossip service
// is a fully distributed networked service for exchanging knowledge on
// NodeHost instances. When enabled by the NodeHostConfig.DefaultNodeRegistryEnabled
// field, it is employed to manage NodeHostID to RaftAddress mappings of known
// NodeHost instances.
type GossipConfig struct {
	// BindAddress is the address for the gossip service to bind to and listen on.
	// Both UDP and TCP ports are used by the gossip service. The local gossip
	// service should be able to receive gossip service related messages by
	// binding to and listening on this address. BindAddress is usually in the
	// format of IP:Port, Hostname:Port or DNS Name:Port.
	BindAddress string
	// AdvertiseAddress is the address to advertise to other NodeHost instances
	// used for NAT traversal. Gossip services running on remote NodeHost
	// instances will use AdvertiseAddress to exchange gossip service related
	// messages. AdvertiseAddress is in the format of IP:Port.
	AdvertiseAddress string
	// Seed is a list of AdvertiseAddress of remote NodeHost instances. Local
	// NodeHost instance will try to contact all of them to bootstrap the gossip
	// service. At least one reachable NodeHost instance is required to
	// successfully bootstrap the gossip service. Each seed address is in the
	// format of IP:Port, Hostname:Port or DNS Name:Port.
	//
	// It is ok to include seed addresses that are temporarily unreachable, e.g.
	// when launching the first NodeHost instance in your deployment, you can
	// include AdvertiseAddresses from other NodeHost instances that you plan to
	// launch shortly afterwards.
	Seed []string
	// Meta is the extra metadata to be included in gossip node's Meta field. It
	// will be propagated to all other NodeHost instances via gossip.
	Meta []byte
}

// IsEmpty returns a boolean flag indicating whether the GossipConfig instance
// is empty.
func (g *GossipConfig) IsEmpty() bool {
	return len(g.BindAddress) == 0 &&
		len(g.AdvertiseAddress) == 0 && len(g.Seed) == 0
}

// Validate validates the GossipConfig instance.
func (g *GossipConfig) Validate() error {
	if len(g.BindAddress) > 0 && !stringutil.IsValidAddress(g.BindAddress) {
		return errors.New("invalid GossipConfig.BindAddress")
	} else if len(g.BindAddress) == 0 {
		return errors.New("BindAddress not set")
	}
	if len(g.AdvertiseAddress) > 0 && !isValidAdvertiseAddress(g.AdvertiseAddress) {
		return errors.New("invalid GossipConfig.AdvertiseAddress")
	}
	if len(g.Seed) == 0 {
		return errors.New("seed nodes not set")
	}
	count := 0
	for _, v := range g.Seed {
		if v != g.BindAddress && v != g.AdvertiseAddress {
			count++
		}
		if !stringutil.IsValidAddress(v) {
			return errors.New("invalid GossipConfig.Seed value")
		}
	}
	if count == 0 {
		return errors.New("no valid seed node")
	}
	return nil
}

func isValidAdvertiseAddress(addr string) bool {
	host, sp, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	port, err := strconv.ParseUint(sp, 10, 16)
	if err != nil {
		return false
	}
	if port > 65535 {
		return false
	}
	// the memberlist package doesn't allow hostname or DNS name to be used in
	// advertise address
	return stringutil.IPV4Regex.MatchString(host)
}
