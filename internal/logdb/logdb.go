// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
Package logdb implements the persistent log storage used by Dragonboat.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package logdb

import (
	"fmt"

	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	plog = logger.GetLogger("logdb")
)

type kvFactory func(config.LogDBConfig,
	kv.LogDBCallback, string, string, vfs.IFS) (kv.IKVStore, error)

// NewDefaultLogDB creates a Log DB instance using the default KV store
// implementation. The created Log DB tries to store entry records in
// plain format but it switches to the batched mode if there is already
// batched entries saved in the existing DB.
func NewDefaultLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string, fs vfs.IFS) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, false, true, fs, newDefaultKVStore)
}

// NewDefaultBatchedLogDB creates a Log DB instance using the default KV store
// implementation with batched entry support.
func NewDefaultBatchedLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string, fs vfs.IFS) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, true, false, fs, newDefaultKVStore)
}

// NewLogDB creates a Log DB instance based on provided configuration
// parameters. The underlying KV store used by the Log DB instance is created
// by the provided factory function.
func NewLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback, dirs []string, lldirs []string,
	batched bool, check bool, fs vfs.IFS, f kvFactory) (raftio.ILogDB, error) {
	checkDirs(config.Expert.LogDB.Shards, dirs, lldirs)
	llDirRequired := len(lldirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < config.Expert.LogDB.Shards; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lldirs = append(lldirs, lldirs[0])
			}
		}
	}
	return OpenShardedDB(config, callback, dirs, lldirs, batched, check, fs, f)
}

func checkDirs(numOfShards uint64, dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfShards {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfShards)
		}
		if len(lldirs) > 0 {
			if len(dirs) != len(lldirs) {
				plog.Panicf("%v regular dirs, but %v low latency dirs", dirs, lldirs)
			}
		}
	} else {
		panic("no regular dir")
	}
}

// GetLogDBInfo returns logdb type name.
func GetLogDBInfo(f config.LogDBFactoryFunc,
	config config.NodeHostConfig,
	nhDirs []string, fs vfs.IFS) (name string, err error) {
	tmpDirs := make([]string, 0)
	for _, dir := range nhDirs {
		tmp := fmt.Sprintf("tmp-%d", random.LockGuardedRand.Uint64())
		td := fs.PathJoin(dir, tmp)
		if err := fileutil.MkdirAll(td, fs); err != nil {
			return "", err
		}
		tmpDirs = append(tmpDirs, td)
	}
	ldb, err := f(config, nil, tmpDirs, tmpDirs)
	if err != nil {
		return "", err
	}
	name = ldb.Name()
	defer func() {
		ldb.Close()
		for _, dir := range tmpDirs {
			if cerr := fs.RemoveAll(dir); err == nil {
				err = cerr
			}
		}
	}()
	return name, nil
}
