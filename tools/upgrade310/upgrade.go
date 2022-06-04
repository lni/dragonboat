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

package upgrade310

import (
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/utils"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var firstError = utils.FirstError

// CanUpgradeToV310 determines whether your production dataset is safe to use
// the v3.0.3 or higher version of Dragonboat. You need to stop your NodeHost
// before invoking CanUpgradeToV310.
//
// CanUpgradeToV310 checks whether there is any snapshot that has already been
// streamed or imported but has not been fully applied into user state machine
// yet.
//
// The input parameter nhConfig should be the same NodeHostConfig instance you
// use to initiate your NodeHost object. CanUpgradeToV310 returns a boolean flag
// indicating whether it is safe to upgrade. If it returns false, you can
// restart your NodeHost using the existing version of Dragonboat, e.g. v3.0.2,
// to allow pending snapshots to be fully applied. Repeat the above steps until
// CanUpgradeToV310 returns true.
//
// Note that for the vast majority cases, CanUpgradeToV310 is expected to
// return true after its first run, which means it is safe to go ahead and
// upgrade the Dragonboat version.
func CanUpgradeToV310(nhConfig config.NodeHostConfig) (result bool, err error) {
	if nhConfig.DeploymentID == 0 {
		nhConfig.DeploymentID = 1
	}
	if err := nhConfig.Prepare(); err != nil {
		return false, err
	}
	fs := nhConfig.Expert.FS
	env, err := server.NewEnv(nhConfig, fs)
	if err != nil {
		return false, err
	}
	defer func() {
		err = firstError(err, env.Close())
	}()
	if err := env.LockNodeHostDir(); err != nil {
		return false, err
	}
	nhDir, walDir := env.GetLogDBDirs(nhConfig.DeploymentID)
	var ldb raftio.ILogDB
	if nhConfig.Expert.LogDBFactory == nil {
		ldb, err = logdb.NewDefaultLogDB(nhConfig,
			nil, []string{nhDir}, []string{walDir})
	} else {
		ldb, err = nhConfig.Expert.LogDBFactory.Create(nhConfig,
			nil, []string{nhDir}, []string{walDir})
	}
	if err != nil {
		return false, err
	}
	defer func() {
		err = firstError(err, ldb.Close())
	}()
	niList, err := ldb.ListNodeInfo()
	if err != nil {
		return false, err
	}
	for _, ni := range niList {
		ss, err := ldb.GetSnapshot(ni.ShardID, ni.ReplicaID)
		if err != nil {
			return false, err
		}
		if ss.Type == pb.OnDiskStateMachine && ss.OnDiskIndex == 0 {
			shrunk, err := rsm.IsShrunkSnapshotFile(ss.Filepath, fs)
			if err != nil {
				return false, err
			}
			if !shrunk {
				return false, nil
			}
		}
	}
	return true, nil
}
