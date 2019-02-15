# 存储 #

Dragonboat使用RocksDB或LevelDB来存储Raft协议的日志数据。

## 重要告知 ##

* 建议在生产环境使用RocksDB存储方案。
* 我们不提供RocksDB与LevelDB之间Raft协议日志数据的迁移工具。
* 当您不确定应该选择哪个来存储Raft协议的日志数据，请选择RocksDB。

## RocksDB ##

RocksDB是Dragonboat默认的存储方案。建议在生产环境使用RocksDB来存储Raft日志。

为了使用RocksDB来存储Raft日志，需要先安装RocksDB库，请参考[中文说明](https://github.com/lni/dragonboat/blob/master/README.CHS.md)的“开始使用”一节来查看如何方便地安装。在编译您的应用的时候，需要设置CGO_CFLAGS和CGO_LDFLAGS这两个环境变量以指向RocksDB库的安装位置，具体方法请参考[中文说明](https://github.com/lni/dragonboat/blob/master/README.CHS.md)的“开始使用
”一节。

## LevelDB ##

Dragonboat可以使用LevelDB来存储Raft日志数据。目前LevelDB的支持是BETA状态，不建议在生产环境使用。

使用LevelDB的一大优势是不需要额外的安装步骤，编译您的应用也更简便，因为不再需要设定上述的CGO_CFLAGS和CGO_LDFLAGS环境变量。您仅需要在编译您的应用的时候打开一个名为dragonboat_leveldb的build tags：

```
go build -tags "dragonboat_leveldb" pkgname
```

请参考[中文说明](https://github.com/lni/dragonboat/blob/master/README.CHS.md)的“开始使用”一节以获知如何使用LevelDB来运行内建的测试。

## 使用自定义的存储方案 ##

您可以扩展Dragonboat以使用您所选择的其它存储方案来保存Raft协议的日志数据。您需要实现在github.com/lni/dragonboat/raftio中定义的ILogDB接口，并将其实现以一个factory function的方式提供给NodeHostConfig的LogDBFactory成员。

在使用这样的自定义存储方案时，您可以使用dragonboat_custom_logdb这个build tag来避免对RocksDB库的依赖。

```
go build -tags "dragonboat_custom_logdb" pkgname
```
