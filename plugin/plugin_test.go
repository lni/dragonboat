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

package plugin

import (
	"os"
	"testing"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/plugin/pebble"
	"github.com/lni/dragonboat/v3/plugin/rocksdb"
)

var (
	singleNodeHostTestDir = "plugin_test_dir_safe_to_delete"
)

func testLogDBPluginCanBeUsed(t *testing.T, f config.LogDBFactoryFunc) {
	os.RemoveAll(singleNodeHostTestDir)
	defer os.RemoveAll(singleNodeHostTestDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 20,
		RaftAddress:    "localhost:26000",
		LogDBFactory:   f,
		FS:             vfs.DefaultFS,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	defer nh.Stop()
}

func TestLogDBPluginsCanBeUsed(t *testing.T) {
	testLogDBPluginCanBeUsed(t, rocksdb.NewLogDB)
	testLogDBPluginCanBeUsed(t, rocksdb.NewBatchedLogDB)
	testLogDBPluginCanBeUsed(t, pebble.NewBatchedLogDB)
}
