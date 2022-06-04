# Raft Log Storage #

Dragonboat uses [Pebble](https://github.com/cockroachdb/pebble) to store Raft logs, Pebble is a RocksDB compatible Key-Value store.

## Compatibility ##

Pebble is a new Key-Value store implemented in Go with bidirectional compatibility with RocksDB.

## Pebble ##

Pebble is used by default, no configuration is required.

## RocksDB ##

RocksDB support was removed in the v3.4 release. 

## Use custom storage solution ##

You can extend Dragonboat to use your preferred storage solution to store Raft logs -

* implement the ILogDB interface defined in the github.com/lni/dragonboat/v4/raftio package
* pass a factory function that creates such a custom Log DB instance to the LogDBFactory field of your NodeHostConfig.Expert instance

## Tan

Tan is Dragonboat's new Raft Log storage solution, it will be made the default in future releases. Impacts to existing users will be minimized in such planned transition and pebble based log storage will continue to be support.  
