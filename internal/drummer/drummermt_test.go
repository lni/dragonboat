// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

// +build dragonboat_monkeytest

package drummer

import (
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
)

func runDrummerMonkeyTest(t *testing.T, appname string) {
	runtime.GOMAXPROCS(10)
	dragonboat.SetSnapshotWorkerCount(8)
	dragonboat.SetWorkerCount(4)
	dragonboat.SetCommitWorkerCount(4)
	dragonboat.DisableLogUnreachable()
	dragonboat.SetIncomingProposalsMaxLen(64)
	dragonboat.SetIncomingReadIndexMaxLen(64)
	dragonboat.SetReceiveQueueSize(64)
	transport.SetPerConnBufferSize(64 * 1024)
	transport.SetSendBufferSize(64)
	transport.SetSnapshotChunkSize(1024)
	transport.SetSnapshotSendBufSize(2048)
	transport.SetPayloadBuffserSize(1024 * 8)
	logdb.SetEntryBatchSize(4)
	logdb.SetLogDBInstanceCount(1)
	logdb.SetRDBContextSize(1)
	useRangeDelete := random.NewProbability(900000)
	if useRangeDelete.Hit() {
		logdb.DisableRangeDelete()
	} else {
		logdb.EnableRangeDelete()
	}
	rand.Seed(int64(os.Getpid()))
	logger.GetLogger("dragonboat").SetLevel(logger.DEBUG)
	logger.GetLogger("transport").SetLevel(logger.DEBUG)
	drummerMonkeyTesting(t, appname)
}

func TestClusterCanSurviveDrummerMonkeyPlay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runDrummerMonkeyTest(t, "kvtest")
}

func TestConcurrentClusterCanSurviveDrummerMonkeyPlay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runDrummerMonkeyTest(t, "concurrentkv")
}

func TestCPPKVCanSurviveDrummerMonkeyPlay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runDrummerMonkeyTest(t, "cpp-cppkvtest")
}
