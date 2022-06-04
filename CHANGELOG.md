## v4.0 (TBD)

Dragonboat v4.0 is a major release with new features, improvements and API changes.

### New features

- Experimental Raft Pre-Vote support.
- Experimental LogDB implementation called tan, it is significantly faster than Key-Value store based approach.

### Improvements

- Better error handling.
- Simplified snapshot compaction.
- Removed dependency on protobuf.
- Fixed snapshot notification.
- Fixed unreachable notification.
- Upgraded to a more recent version of pebble.
- Non-voting node (used to be called observer node) support has been marked as production ready.
- Made the experimental gossip feature a first class citizen of the library.

### Other changes

- Raft observer node has been renamed as non-voting node.
- RocksDB support has been removed as Pebble provides bidirectional compatible with RocksDB. 
- ClusterID/NodeID has been renamed as ShardID/ReplicaID.
- Deprecated v3.x features and APIs have been removed. 

## v3.3 (2021-01-20)

Dragonboat v3.3 is a major release that comes with new features and improvements. All v3.2.x users are recommended to upgrade.

### New features

- Pebble, which is bidirectional compatible with RocksDB, has been made the default engine for storing Raft Logs. RocksDB and CGO are no longer required.
- Added the ability to slow down incoming proposals when the Raft Logs engine is highly loaded.
- Added the option to get notified when proposals and config changes are committed.
- Added an experimental gossip service to allow NodeHosts to use dynamically assigned IP addresses as RaftAddress.
- Added the ability to better control memory footprint.
- Added ARM64/Linux as a new targeted platform.

Note that Pebble provides bidirectional compatibility with RocksDB v6.2.1. Existing Dragonboat applications can upgrade to v3.3 without any conversion unless a newer version of RocksDB was used. RocksDB v6.4.x has been briefly tested and it seems to be compatible with Pebble as well. 

### Improvements

- Optimized the read index implementation.
- Reduced LogDB restart delays.
- Made LogDB configurations accessible programmatically.
- Added protobuf workaround to allow Dragonboat and etcd to be used in the same project.
- Fixed a few data race issues.
- Fixed a potential Raft election deadlock issue when nodes are highly loaded.
- Allow incoming proposals to be rate limited when LogDB is busy.
- Simplified many aspects of the library.

### Breaking changes

- The signature of config.LogDBFactoryFunc has been changed. Your application is not affected unless it uses a custom LogDB implementation.
- Due to lack of user interests, C++ binding is no longer supported.
- LevelDB based LogDB is no longer supported.
- NodeHostConfig's FS and SystemTickerPrecision fields have been moved into NodeHostConfig.Expert.

## v3.2 (2020-03-05)

Dragonboat v3.2 comes with new features and improvements. All v3.1.x users are recommended to upgrade. 

### New features

- Added snappy compression support for Raft entries and snapshots.
- Added experimental witness support.
- Added new API to allow LogDB compaction to be manually triggered.
- Added event listener support to allow users to be notified for certain Raft events.
- Added system event listener support to allow users to be notified for certain system events.
- Added Raft related metrics to exported.
- Added rate limit support to control the maximum bandwidth used for snapshot streaming.
- Updated the C++ binding to cover all v3.1 features. Thanks JasonYuchen for working on that.
- Added a virtual filesystem layer to support more filesystem related tests.
- Added experimental Windows and FreeBSD support.

### Improvements

- Removed the restriction on max proposal payload size.
- Re-enabled the range delete support in LogDB.
- Better handling of concurrent snapshot streams.
- Extensive testing have been done on a high performance native Go KV store called Pebble.
- TolerateCorruptedTailRecords is now the default WAL recovery mode in the RocksDB based LogDB.

### Breaking changes

There is no breaking change for regular users. However, 

 - If you have built customized transport module implementing the raftio.IRaftRPC interface, there is minor change to the config.RaftRPCFactoryFunc type. See github.com/lni/dragoboat/config/config.go for details.
 - The default transport module has been updated, it is no longer compatible with earlier versions of dragonboat. 
 - The default LogDB data format is no longer backward compatible with v3.1 or earlier. 

### Other changes

 - LevelDB support has been marked as depreciated. It will be removed from dragonboat in the next major release. 

## v3.1 (2019-07-04)

Dragonboat v3.1 is a maintenance release with breaking change. All v3.0.x users are recommended to upgrade. Please make sure to carefully read the CHANGELOG below before upgrading.

### Bug fixes

- Fixed ImportSnapshot. 

### New features

- Added NodeHostConfig.RaftEventListener to allow user applications to be notified for certain Raft events.

### Improvements

- Made restarting an existing node faster.
- Immediately return ErrClusterNotReady when requests are dropped for not having a leader.

### Breaking changes

- When upgrading to v3.1.x from v3.0.x, dragonboat requires all streamed or imported snapshots to have been applied. github.com/lni/dragonboat/tools/upgrade310 is provided to check that. See the godoc in github.com/lni/dragonboat/tools/upgrade310 for more details. For users who use NodeHost.RequestSnapshot to export snapshots for backup purposes, we recommend to re-generate all exported snapshots once upgraded to v3.1.

## v3.0 (2019-06-21)

Dragonboat v3.0 is a major release with breaking changes. Please make sure to carefully read the CHANGELOG below before upgrading.

### New features

- Added on disk state machine (statemachine.IOnDiskStateMachine) support. The on disk state machine is close to the concept described in the section 5.2 of Diego Ongaro's Raft thesis. 
- Added new API for requesting a snapshot to be created or exported.
- Added the ability to use exported snapshot to repair permanently damaged cluster that no longer has majority quorum.
- Added new API for cleaning up data and release disk spaces after a node is removed from its Raft cluster.
- Added the ability to limit peak memory usage when disk or network is slow.
- Added Go module support. Go 1.12 is required.

### Improvements

- Further improved self checking on configurations.
- Added snapshot binary format version 2 with block base checksum.
- Synchronous variants have been provided for all asynchronous user request APIs in NodeHost.

### Breaking changes

- The Drummer package has been made invisible from user applications.
- The statemachine.IStateMachine interface has been upgraded to reflect the fact that not all state machine data is stored in memory ([#46](https://github.com/lni/dragonboat/issues/46)).

## v2.1 (2019-02-20)

### New features

- Added support to store Raft Logs in LevelDB.

## v2.0 (2019-01-04)

Initial open source release. 
