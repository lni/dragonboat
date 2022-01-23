// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"path"
	"testing"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/plugin/rocksdb"
	"github.com/lni/dragonboat/v3/plugin/transport/quic"
)

const (
	singleNodeHostTestDir = "plugin_test_dir_safe_to_delete"
	caFile                = "../internal/transport/tests/test-root-ca.crt"
	certFile              = "../internal/transport/tests/localhost.crt"
	keyFile               = "../internal/transport/tests/localhost.key"
)

func testLogDBPluginCanBeUsed(t *testing.T, f config.LogDBFactory) {
	testDir := path.Join(t.TempDir(), singleNodeHostTestDir)
	defer os.RemoveAll(testDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    testDir,
		RTTMillisecond: 20,
		RaftAddress:    "localhost:26000",
		Expert:         config.ExpertConfig{FS: vfs.DefaultFS, LogDBFactory: f},
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	defer nh.Close()
}

func testTransportPluginCanBeUsed(t *testing.T, f config.TransportFactory) {
	testDir := path.Join(t.TempDir(), singleNodeHostTestDir)
	defer os.RemoveAll(testDir)
	nhc := config.NodeHostConfig{
		NodeHostDir:    testDir,
		RTTMillisecond: 20,
		RaftAddress:    "localhost:26000",
		Expert:         config.ExpertConfig{FS: vfs.DefaultFS, TransportFactory: f},
		MutualTLS:      true,
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	defer nh.Close()
}

func TestLogDBPluginsCanBeUsed(t *testing.T) {
	testLogDBPluginCanBeUsed(t, &rocksdb.Factory{})
}

func TestTransportPluginsCanBeUsed(t *testing.T) {
	testTransportPluginCanBeUsed(t, &quic.TransportFactory{})
}
