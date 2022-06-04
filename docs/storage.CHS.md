# 存储 #

Dragonboat使用[Pebble](https://github.com/cockroachdb/pebble)来存储Raft协议的日志数据。

## 兼容性 ##

Pebble是一个全新的以Go实现的Key-Value store，它提供与RocksDB双向数据文件格式兼容。

## Pebble ##

系统默认使用Pebble，无需设置额外设置。

## RocksDB ##

曾经的RocksDB支持已经在v3.4版本中被移除，请默认使用pebble

## 使用自定义的存储方案 ##

您可以扩展Dragonboat以使用您所选择的其它存储方案来保存Raft协议的日志数据。您需要实现在github.com/lni/dragonboat/v4/raftio中定义的ILogDB接口，并将其实现以一个factory function的方式提供给NodeHostConfig的LogDBFactory成员。

## Tan

Tan是新一代Raft日志数据存储实现，未来Dragonboat将默认使用Tan，该功能切换将会确保老用户不受影响，Pebble支持将长期继续维护。
