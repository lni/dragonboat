// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package rocksdb provides factory functions for creating RocksDB based Log DB.
*/
package rocksdb

import (
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/rocksdb"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
)

// NewLogDB is the factory function for creating RocksDB based Log DB instances.
// Raft entries are stored in its plain format, it uses less memory than the
// batched alternative implementation but comes at the cost of lower throughput.
func NewLogDB(cfg config.LogDBConfig,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	fs := vfs.DefaultFS
	return logdb.NewLogDB(cfg, dirs, lldirs, false, false, fs, rocksdb.NewKVStore)
}

// NewBatchedLogDB is the factory function for creating RocksDB based Log DB
// instances. Raft entries are batched before they get stored into RocksDB, it
// uses more memory than the default RocksDB based Log DB and provides better
// throughput performance.
func NewBatchedLogDB(cfg config.LogDBConfig,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	fs := vfs.DefaultFS
	return logdb.NewLogDB(cfg, dirs, lldirs, true, false, fs, rocksdb.NewKVStore)
}
