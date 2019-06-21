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
Package logdb implements the persistent log storage used by Dragonboat.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package logdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	plog = logger.GetLogger("logdb")
)

type kvFactory func(string, string) (kv.IKVStore, error)

// NewDefaultLogDB creates a Log DB instance using the default KV store
// implementation. The created Log DB tries to store entry records in
// plain format but it switches to the batched mode if there is already
// batched entries saved in the existing DB.
func NewDefaultLogDB(dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(dirs, lldirs, false, true, newDefaultKVStore)
}

// NewDefaultBatchedLogDB creates a Log DB instance using the default KV store
// implementation with batched entry support.
func NewDefaultBatchedLogDB(dirs []string,
	lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(dirs, lldirs, true, false, newDefaultKVStore)
}

// NewLogDB creates a Log DB instance based on provided configuration
// parameters. The underlying KV store used by the Log DB instance is created
// by the provided factory function.
func NewLogDB(dirs []string, lldirs []string,
	batched bool, check bool, f kvFactory) (raftio.ILogDB, error) {
	checkDirs(dirs, lldirs)
	llDirRequired := len(lldirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < numOfRocksDBInstance; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lldirs = append(lldirs, lldirs[0])
			}
		}
	}
	return OpenShardedRDB(dirs, lldirs, batched, check, f)
}

func checkDirs(dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfRocksDBInstance {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfRocksDBInstance)
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
	nhDirs []string) (name string, err error) {
	tmpDirs := make([]string, 0)
	for _, dir := range nhDirs {
		tmp := fmt.Sprintf("tmp-%d", random.LockGuardedRand.Uint64())
		td := filepath.Join(dir, tmp)
		if err := fileutil.Mkdir(td); err != nil {
			return "", err
		}
		tmpDirs = append(tmpDirs, td)
	}
	ldb, err := f(tmpDirs, tmpDirs)
	if err != nil {
		return "", err
	}
	name = ldb.Name()
	defer func() {
		ldb.Close()
		for _, dir := range tmpDirs {
			if cerr := os.RemoveAll(dir); err == nil {
				err = cerr
			}
		}
	}()
	return name, nil
}
