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

package logdb

import (
	"fmt"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
)

// OpenReadOnlyLogDB opens a Dragonboat LogDB in read-only mode.
// It discovers logdb-N shard directories within dir, auto-detects the
// entry format (plain vs batched), and returns a raftio.ILogDB suitable
// for forensics and inspection.
func OpenReadOnlyLogDB(cfg config.LogDBConfig,
	dir, wal string, fs vfs.IFS, f kvFactory) (raftio.ILogDB, error) {

	const flagFilename = "dragonboat.ds"

	s := raftpb.RaftDataStatus{}
	if err := fileutil.GetFlagFileContent(dir, flagFilename, &s, fs); err != nil {
		return nil, errors.Wrapf(err, "failed to read %s", path.Join(dir, flagFilename))
	}
	cfg.Shards = s.LogdbShardCount

	nodeHostCfg := config.NodeHostConfig{
		NodeHostDir: dir,
		WALDir:      wal,
	}
	env, err := server.NewEnv(nodeHostCfg, fs)
	if err != nil {
		return nil, errors.Wrap(err, "new env")
	}
	env.SetHostname(s.Hostname)

	ddir, dwal := env.GetLogDBDirs(s.DeploymentId)

	shards := make([]*db, 0)
	closeAll := func(all []*db) {
		for _, s := range all {
			s.close()
		}
	}
	for i := uint64(0); i < s.LogdbShardCount; i++ {
		dir := path.Join(ddir, fmt.Sprintf("logdb-%d", i))
		wal := path.Join(dwal, fmt.Sprintf("logdb-%d", i))

		batched, err := detectBatchedEntryFormat(cfg, dir, wal, fs, f)
		if err != nil {
			closeAll(shards)
			return nil, errors.Wrapf(err, "failed to detect entry format for shard %d", i)
		}

		s, err := openRDB(cfg, nil, dir, wal, batched, fs, f)
		if err != nil {
			closeAll(shards)
			return nil, err
		}
		shards = append(shards, s)
	}

	// see OpenShardedDB for why we use StepWorkerCount here in Capacity position.
	partitioner := server.NewDoubleFixedPartitioner(s.StepWorkerCount, s.LogdbShardCount)
	return &ShardedDB{
		config:      cfg,
		shards:      shards,
		partitioner: partitioner,
		stopper:     syncutil.NewStopper(),
	}, nil
}

func detectBatchedEntryFormat(cfg config.LogDBConfig, dir, wal string, fs vfs.IFS, f kvFactory) (bool, error) {
	kvs, err := f(cfg, nil, dir, wal, fs)
	if err != nil {
		return false, err
	}
	defer kvs.Close()

	batched, err := hasEntryRecord(kvs, true)
	if err != nil {
		return false, err
	}
	if batched {
		return true, nil
	}

	plain, err := hasEntryRecord(kvs, false)
	if err != nil {
		return false, err
	}
	if plain {
		return false, nil
	}

	// No entry records found — default to batched (the newer format).
	return true, nil
}
