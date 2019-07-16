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

// +build dragonboat_slowtest

package drummer

import (
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
)

func TestElectionCanStartAndElectLeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	nodes, _ := createTestNodeLists(dl)
	startTestNodes(nodes, dl)
	defer stopTestNodes(nodes)
	waitForStableNodes(nodes, 25)
	count := uint64(0)
	for {
		count++
		idx := getLeaderDrummerIndex(nodes)
		if idx >= 0 {
			plog.Infof("leader found at %d", idx)
			return
		}
		if count > 30 {
			t.Errorf("no leader")
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func TestElectionCanElectNewLeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer removeMonkeyTestDir()
	dl := getDrummerMonkeyTestAddrList()
	nodes, _ := createTestNodeLists(dl)
	startTestNodes(nodes, dl)
	defer stopTestNodes(nodes)
	waitForStableNodes(nodes, 25)
	leaderIdx := int(-1)
	count := uint64(0)
	for {
		count++
		leaderIdx = getLeaderDrummerIndex(nodes)
		if leaderIdx >= 0 {
			break
		}
		if count > 30 {
			t.Errorf("no leader")
			return
		}
		time.Sleep(1 * time.Second)
	}
	plog.Infof("current leader is %d", leaderIdx)
	// stop the leader
	count = 0
	nodes[leaderIdx].Stop()
	// see whether we can get a new leader within allowed time bound
	for {
		count++
		leaderIdx := getLeaderDrummerIndex(nodes)
		if leaderIdx >= 0 {
			plog.Infof("replacement leader is at %d", leaderIdx)
			return
		}
		if count > 100 {
			t.Errorf("no leader")
			return
		}
		time.Sleep(1 * time.Second)
	}
}
