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

/*
Package config contains functions and types used for managing dragonboat's
configurations.
*/
package config

import (
	"crypto/tls"
	"errors"

	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/netutil"
	"github.com/lni/dragonboat/v3/internal/utils/stringutil"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	plog = logger.GetLogger("config")
)

// RaftRPCFactoryFunc is the factory function that creates the Raft RPC module
// instance for exchanging Raft messages between NodeHosts.
type RaftRPCFactoryFunc func(NodeHostConfig,
	raftio.RequestHandler, raftio.ChunkSinkFactory) raftio.IRaftRPC

// LogDBFactoryFunc is the factory function that creates NodeHost's persistent
// storage module known as Log DB.
type LogDBFactoryFunc func(dirs []string,
	lowLatencyDirs []string) (raftio.ILogDB, error)

// Config is used to configure Raft nodes.
type Config struct {
	// NodeID is a non-zero value used to identify a node within a Raft cluster.
	NodeID uint64
	// ClusterID is the unique value used to identify a Raft cluster.
	ClusterID uint64
	// IsObserver indicates whether this is an observer Raft node without voting
	// power.
	IsObserver bool
	// CheckQuorum specifies whether the leader node should periodically check
	// non-leader node status and step down to become a follower node when it no
	// longer has the quorum.
	CheckQuorum bool
	// Quiesce specifies whether to let the Raft cluster enter quiesce mode when
	// there is no cluster activity.
	Quiesce bool
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
	// can be compacted. See the godoc on CompactionOverhead to see what log
	// entries are actually compacted after taking a snapshot.
	//
	// NodeHost.RequestSnapshot can be called to manually request a snapshot to
	// be created to capture current node state.
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
	// MaxInMemLogSize is the maximum bytes size of Raft logs that can be stored in
	// memory. Raft logs waiting to be committed and applied are stored in memory.
	// When MaxInMemLogSize is 0, the limit is set to math.MaxUint64 which
	// basically means no limit. When MaxInMemLogSize is set and the limit is
	// reached, error will be returned when clients try to make any new proposals.
	MaxInMemLogSize uint64
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
	if c.MaxInMemLogSize > 0 &&
		c.MaxInMemLogSize < settings.EntryNonCmdFieldsSize+1 {
		return errors.New("MaxInMemLogSize is too small")
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
	// between them is 100 microseconds.
	RTTMillisecond uint64
	// RaftAddress is a hostname:port or IP:port address used by the Raft RPC
	// module for exchanging Raft messages and snapshots. This is also the
	// identifier for a NodeHost instance. RaftAddress should be set to the
	// public address that can be accessed from remote NodeHost instances.
	RaftAddress string
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
	// See https://github.com/lni/dragonboat/v3/wiki/TLS-in-Dragonboat for more
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
	RaftRPCFactory RaftRPCFactoryFunc
	// EnableMetrics determines whether health metrics in Prometheus format should
	// be enabled.
	EnableMetrics bool
	// RaftEventListener is the listener for Raft events exposed to user space.
	// NodeHost uses a single dedicated goroutine to invoke all RaftEventListener
	// methods one by one, CPU intensive or IO related procedures that can cause
	// long delaies should be offloaded to worker goroutines managed by users.
	RaftEventListener raftio.IRaftEventListener
}

// Validate validates the NodeHostConfig instance and return an error when
// the configuration is considered as invalid.
func (c *NodeHostConfig) Validate() error {
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

// IsValidAddress returns whether the input address is valid.
func IsValidAddress(addr string) bool {
	return stringutil.IsValidAddress(addr)
}
