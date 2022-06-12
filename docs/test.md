# Testing #

## Approaches ##
* Test driven development. Relevant [etcd raft](https://github.com/coreos/etcd/tree/master/raft) tests have been ported to dragonboat to ensure all corner cases identified by the etcd project have been handled.
* High test coverage. Extensively tested by unit testing and [monkey testing](https://en.wikipedia.org/wiki/Monkey_testing).
* Linearizability checkers. [Jepsen's](https://github.com/jepsen-io/jepsen) [Knossos](https://github.com/jepsen-io/knossos) and [porcupine](https://github.com/anishathalye/porcupine) are utilized to check whether IOs are linearizable.
* Fuzz testing using [go-fuzz](https://github.com/dvyukov/go-fuzz).
* I/O error injection tests. [charybdefs](https://github.com/scylladb/charybdefs) from [scylladb](http://www.scylladb.com/) is employed to inject I/O errors to the underlying file-system to ensure that Dragonboat handle them correctly.
* Power loss tests. We test the system to see what actually happens after power loss.

## Monkey Testing ##
### Setup ###
* 5 NodeNosts and 3 Drummer servers per process
* hundreds of Raft shards per process
* randomly kill and restart NodeHosts and Drummer servers, each NodeHost usually stay online for a few minutes
* randomly delete all data owned by a certain NodeHost to emulate permanent disk failure
* randomly drop and re-order messages exchanged between NodeHosts
* randomly partition NodeHosts from rest of the network
* for selected instances, snapshotting and log compaction happen all the time in the background
* committed entries are applied with random delays
* snapshots are captured and applied with random delays
* a list of background workers keep writing to/reading from random Raft shards with stale read checks
* client activity history files are verified by linearizability checkers such as Jepsen's Knossos
* run hundreds of above described processes concurrently on each test server, 30 minutes each iteration, many iterations every night
* run concurrently on many servers every night

### Checks ###
* no linearizability violation
* no shard is permanently stuck
* state machines must be in sync
* shard membership must be consistent
* raft log saved in LogDB must be consistent
* no zombie shard node

### Results ###
Some history files in Jepsen's [Knossos](https://github.com/jepsen-io/knossos) edn format have been made publicly [available](https://github.com/lni/knossos-data).

# Benchmark #

## Setup ##
* Three servers each with a single 22-core Intel XEON E5-2696v4 processor, all cores can boost to 2.8Ghz
* 40GE Mellanox NIC
* Intel 900P for storing the RocksDB's WAL and Intel P3700 1.6T for storing all other data
* Ubuntu 16.04 with Spectre and Meltdown patches, ext4 file-system

## Benchmark method ##
* 48 Raft shards on three NodeHost instances across three servers
* Each Raft node is backed by a in-memory Key-Value data store as RSM
* Mostly update operations in the Key-Value store
* All I/O requests are launched from local processes
* Each request is handled in its own goroutine, simple threading model & easy for application debugging
* fsync is strictly honored
* MutualTLS is disabled

## Intel Optane SSD ##
Compared with enterprise NVME SSDs such as Intel P3700, Optane based SSD doesn't increase throughput when payload is 16/128 bytes. It does slightly increase the throughput when the payload size is 1024 byte each. It also improves write latency when the payload size is 1024.
