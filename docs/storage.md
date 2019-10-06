# Storage #

Dragonboat uses RocksDB or LevelDB to store Raft logs. We also have the plan to support [Pebble](https://github.com/cockroachdb/pebble) once Pebble is declared as ready for production by its authors. 

## Important Notice ##

* RocksDB is the only recommended storage option for production purposes.
* We don't provide data migration tool for moving your RocksDB based Raft logs to LevelDB formats, or vice versa. 
* When you are not sure which one to choose - always use RocksDB.

## RocksDB ##

RocksDB is the default storage option for Dragonboat. It is recommended to use RocksDB for any production purposes. 

When using RocksDB for storage, you need to install RocksDB first, see the [Getting Started](https://github.com/lni/dragonboat/blob/master/README.md) section of the README.md for details. You will also need to set the CGO_CFLAGS and CGO_LDFLAGS environmental variables to point to your RocksDB installation location when building your own applications, see [README.md](https://github.com/lni/dragonboat/blob/master/README.md) for details.

## LevelDB ##

Dragonboat can use LevelDB to store Raft logs as well. It is currently in BETA status and not recommended for any production use.

A major benefit for choosing LevelDB is that no extra installation step is required. It is also easier to build your own application as setting the CGO_CFLAGS and CGO_LDFLAGS environmental variables is no longer required. 

To use LevelDB to store Raft logs, set the LogDBFactory field of your config.NodeHostConfig to its factory function provided in the github.com/lni/dragonboat/plugin/leveldb package. When RocksDB is not install, the dragonboat_no_rocksdb build tag must be set when building your application.

```
go build -tags dragonboat_no_rocksdb pkg_name
``` 

See [README.md](https://github.com/lni/dragonboat/blob/master/README.md) for details on how to run all built-in tests using LevelDB.

## Pebble ##

[Pebble](https://github.com/cockroachdb/pebble) is a native Go key-value store currently being developed by [cockroachdb](https://github.com/cockroachdb) developers. Pebble is not production ready yet, it is [expected](https://github.com/cockroachdb/pebble/issues/233) to be production ready by Spring 2020.

Dragonboat can already use Pebble to store Raft logs, the pebble support is experimental and not recommended for any production use. As a Go storage engine, Pebble completely avoids all CGO related issues and concerns mentioned above. We plan to eventually switch to use Pebble as the default storage engine once it is stable.

## Use custom storage solution ##

You can extend Dragonboat to use your preferred storage solution to store Raft logs -

* implement the ILogDB interface defined in the github.com/lni/dragonboat/raftio package
* pass a factory function that creates such a custom Log DB instance to the LogDBFactory field of your NodeHostConfig instance
* when building your applications, set the build tag named dragonboat_no_rocksdb so RocksDB won't be required. You don't have to set this build tag if RocksDB is available on your system.
