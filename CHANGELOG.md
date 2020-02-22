## v3.2 (TBD)

Dragonboat v3.2 comes with new features and improvements. All v3.1.x users are recommended to upgrade. 

### New features

- Added snappy compression support for Raft entires and snapshots.
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

### Breaking changes

There is no breaking change for regular users. However, 

 - If you have built customized transport module implementing the raftio.IRaftRPC interface, there is minor change to the config.RaftRPCFactoryFunc type. See github.com/lni/dragoboat/config/config.go for details.
 - The default transport module has been updated, it is no longer compatible with earlier versions of dragonboat. 
 - The default LogDB data format is no longer backward compactible with v3.1 or earlier. 

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
