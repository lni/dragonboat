## ABOUT ##
This package implements the Raft protocol described in Diego Ongarno's [PhD thesis](https://ramcloud.stanford.edu/~ongaro/thesis.pdf). 

## Features ##
Almost all major features outlined in the Raft thesis have been implemented - 
* leader election and log replication
* membership changes
* snapshotting, streaming and log compaction
* batching and pipelining
* ReadIndex protocol for read-only queries
* quiesce mode
* leadership transfer
* non-voting members
* idempotent updates transparent to applications
* stateful requests

## Third Party Code ##
This package is a new implementation of the Raft protocol with influence from [etcd raft](https://github.com/coreos/etcd/tree/master/raft) -

* all relevant etcd raft tests have been ported to this project
* a similar iterative style interface is employed as it yields high throughput
* various other similar bits 

Check source code files for copyright information.

## Comparison ##
This package is significantly different from etcd raft - 

* brand new implementation
* better bootstrapping procedure
* much higher proposal throughput
* three stage log
* zero disk read when replicating raft log entries
* committed entries are applied in a fully asynchronous manner
* snapshots are applied in a fully asynchronous manner
* replication messages can be serialized and sent in fully asynchronous manner
* pagination support when applying committed entries
* fully batched making proposal implementation
* fully batched ReadIndex implementation
* the quiesce feature requires assistance from the upper layer
* unsafe read-only queries that rely on local clock is not supported
* more pessimistic when handling membership change
* non-voting members are implemented as a special raft state
* non-voting members can initiate both new proposal and ReadIndex requests
* PreVote is being worked on, it is expected to be supported in the next major release

## Status ##
Production ready
