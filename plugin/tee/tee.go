// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

package tee

import (
	"github.com/lni/dragonboat/v3/config"
	tl "github.com/lni/dragonboat/v3/internal/logdb/tee"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
)

// CreateTeeLogDB creates a special Tee LogDB module for testing purposes.
// Tee LogDB uses RocksDB and Pebble side by side, all writes and reads are
// issued to both so read results can be compared to detect any potential
// discrepancies. Assuming the RocksDB implementation as the gold standard,
// the Tee LogDB module helps us to find potential issues in Pebble and our
// LogDB implementation.
//
// CreateTeeLogDB is provided here to allow Drummer to use the Tee LogDB.
func CreateTeeLogDB(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string) (raftio.ILogDB, error) {
	return tl.NewTeeLogDB(cfg, cb, dirs, wals, vfs.DefaultFS)
}
