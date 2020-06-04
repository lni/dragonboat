# 存储 #

Dragonboat使用[Pebble](https://github.com/cockroachdb/pebble)来存储Raft协议的日志数据，它同时支持RocksDB.

## 兼容性 ##

Pebble是一个全新的以Go实现的Key-Value store，它提供与RocksDB的双向数据文件格式兼容。

如有必要，您可选择使用RocksDB来存储生产环境的Dragonboat数据，在未来任何时候您都可以轻松切换回使用Pebble。

## Pebble ##

系统默认使用Pebble，无需设置额外设置。

## RocksDB ##

为了使用RocksDB来存储Raft日志，需要先安装RocksDB库，在编译您的应用的时候，可能需要设置CGO_CFLAGS和CGO_LDFLAGS这两个环境变量以指向RocksDB库的安装位置。

改用RocksDB来存储Dragonboat数据只需要修改一行代码，仅需将config.NodeHostConfig的LogDBFactory项设置为来自github.com/lni/dragonboat/v3/plugin/rocksdb包的rocksdb.NewLogDB即可。

## 使用自定义的存储方案 ##

您可以扩展Dragonboat以使用您所选择的其它存储方案来保存Raft协议的日志数据。您需要实现在github.com/lni/dragonboat/raftio中定义的ILogDB接口，并将其实现以一个factory function的方式提供给NodeHostConfig的LogDBFactory成员。
