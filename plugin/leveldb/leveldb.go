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
Package leveldb provides factory functions for creating LevelDB based Log DB.

LevelDB support is in BETA status, it is recommended to use the RocksDB based
Log DB in production.
*/
package leveldb

import (
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/logdb/kv/leveldb"
	"github.com/lni/dragonboat/raftio"
)

// NewLogDB is the factory function for creating LevelDB based Log DB instances.
// Raft entries are stored in its plain format, it uses less memory than the
// batched alternative implementation but comes at the cost of lower throughput.
func NewLogDB(dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return logdb.NewLogDB(dirs, lldirs, false, false, leveldb.NewKVStore)
}

// NewBatchedLogDB is the factory function for creating LevelDB based Log DB
// instances. Raft entries are batched before they get stored into LevelDB, it
// uses more memory and provides better throughput performance.
func NewBatchedLogDB(dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return logdb.NewLogDB(dirs, lldirs, true, false, leveldb.NewKVStore)
}
