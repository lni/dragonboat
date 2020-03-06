# Raft Log Storage #

Dragonboat uses RocksDB to store Raft logs, it also has experimental support for [Pebble](https://github.com/cockroachdb/pebble). 

## Important Notice ##

RocksDB is storage option for production purposes.

## RocksDB ##

When using RocksDB for storage, you need to install RocksDB first, see the [Getting Started](https://github.com/lni/dragonboat/blob/master/README.md) section of the README.md for details. You may also need to set the CGO_CFLAGS and CGO_LDFLAGS environmental variables to point to your RocksDB installation location when building your own applications, see [README.md](https://github.com/lni/dragonboat/blob/master/README.md) for details.

## Pebble ##

[Pebble](https://github.com/cockroachdb/pebble) is a native Go key-value store currently being developed by [cockroachdb](https://github.com/cockroachdb) developers. Pebble is not production ready yet, it is [expected](https://github.com/cockroachdb/pebble/issues/233) to be production ready by Spring 2020.

Dragonboat can already use Pebble to store Raft logs, the pebble support is experimental and not recommended for any production use. As a Go storage engine, Pebble avoids all CGO related issues and concerns mentioned above. We plan to eventually switch to use Pebble as the default storage engine once it is stable.

When RocksDB is not install, the dragonboat_no_rocksdb build tag must be set when building your application.

```
go build -tags dragonboat_no_rocksdb pkg_name
```

To use Pebble based Raft Log Storage, set the NewLogDB function from the plugin/pebble package to the config.NodeHostConfig.LogDBFactory field.

See [README.md](https://github.com/lni/dragonboat/blob/master/README.md) for details on how to run all built-in tests using Pebble.

## Use custom storage solution ##

You can extend Dragonboat to use your preferred storage solution to store Raft logs -

* implement the ILogDB interface defined in the github.com/lni/dragonboat/raftio package
* pass a factory function that creates such a custom Log DB instance to the LogDBFactory field of your NodeHostConfig instance
* when building your applications, set the build tag named dragonboat_no_rocksdb so RocksDB won't be required. You don't have to set this build tag if RocksDB is available on your system.
