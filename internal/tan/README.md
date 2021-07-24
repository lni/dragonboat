## Tan ##

Tan is a high performance database for storing Raft log and metadata. 

## Motivation ##

Early versions of [Dragonboat](https://github.com/lni/dragonboat) employed RocksDB style [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree) based Key-Value databases to store Raft log and metadata. Such Key-Value stores are easy to use but such convenience comes at huge costs - 

* redundant MemTables
* redundant keys
* redundant serializations
* storage amplification
* write amplification
* read amplification
* expensive concurrent access control

Another obvious limitation of such off the shelf Key-Value databases such as RocksDB is that we can not easily tweak or optimize its overall behaviour when applied for different types of replication state machines. 

Tan aims to overcome all these issues by providing a specifically designed database for storing Raft log and metadata.

## License ##

Tan is a proprietary library developed and owned by Lei Ni. You are not allowed to store, read, use, modify or redistribute this library without written consent from its copyright owner Lei Ni.

Tan contains [Pebble](https://github.com/cockroachdb/pebble) code and code derived from Pebble. Pebble itself is built from the [golang version](https://github.com/golang/leveldb) of [Level-DB](https://github.com/google/leveldb). Pebble, Level-DB and the golang version of Level-DB are all BSD licensed.
