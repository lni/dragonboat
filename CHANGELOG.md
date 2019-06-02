## v3.0 (Release date TBD)

Dragonboat v3.0 is a major release with breaking changes. Please make sure to carefully read the CHANGELOG below before upgrading.

### New features

- Added on disk state machine (IOnDiskStateMachine) support. 
- Added new API for requesting a new snapshot to be created or exported.
- Added new API to cleanup data and release disk spaces after a node is removed from a Raft cluster.
- Added the ability to use exported snapshot to repair permanently damaged cluster that no longer has its majority quorum.
- Added the ability to limit peak memory usage when disk or network is slow.
- Go module support has been switched on. 

### Improvements

- Further improved self checking on configurations.
- Implemented snapshot image binary version 2 with block base checksum.
- Synchronous variants have been provided for all NodeHost asynchronous user request APIs.

### Breaking changes

- The Drummer package has been moved to be invisible from user applications.
- The IStateMachine interface has been upgraded to reflect the fact that not all state machine data is stored in memory ([#46](https://github.com/lni/dragonboat/issues/46)).

## v2.1 (Feb 2019)

### New features

- Added support to store Raft Logs in LevelDB.

## v2.0 (Jan 2019)

Initial open source release. 
