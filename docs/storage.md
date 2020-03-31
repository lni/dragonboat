# Raft Log Storage #

Dragonboat uses [Pebble](https://github.com/cockroachdb/pebble) to store Raft logs, it also support RocksDB.

## Compatibility ##

Pebble is a new Key-Value store implemented in Go with bidirectional compatibility with RocksDB.

You can choose to use RocksDB for production purposes if that makes you more confortable. You can switch back to Pebble in the future any time you want. 

## Pebble ##

Pebble is used by default, no configuration is required.

## RocksDB ##

To use RocksDB as Dragonboat's storage engine, you need to install RocksDB first. You may also need to set the CGO_CFLAGS and CGO_LDFLAGS environmental variables to point to your RocksDB installation location when building your own applications.

Switch to RocksDB only involves one extra line of code, just set the LogDBFactory field of your config.NodeHostConfig to function rocksdb.NewLogDB available in the github.com/lni/dragonboat/v3/plugin/rocksdb.

## Use custom storage solution ##

You can extend Dragonboat to use your preferred storage solution to store Raft logs -

* implement the ILogDB interface defined in the github.com/lni/dragonboat/raftio package
* pass a factory function that creates such a custom Log DB instance to the LogDBFactory field of your NodeHostConfig instance
