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

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/raftio"
)

var (
	plog = logger.GetLogger("logdb")
)

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
		if err := os.MkdirAll(td, 0755); err != nil {
			return "", err
		}
		tmpDirs = append(tmpDirs, filepath.Join(dir, tmp))
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

// OpenLogDB opens a plain LogDB instance using the default implementation.
func OpenLogDB(dirs []string, lowLatencyDirs []string) (raftio.ILogDB, error) {
	return openLogDB(dirs, lowLatencyDirs, false, true)
}

// OpenBatchedLogDB opens a batched LogDB instance.
func OpenBatchedLogDB(dirs []string,
	lowLatencyDirs []string) (raftio.ILogDB, error) {
	return openLogDB(dirs, lowLatencyDirs, true, false)
}

// OpenPlainLogDB opens a plain LogDB instance.
func OpenPlainLogDB(dirs []string,
	lowLatencyDirs []string) (raftio.ILogDB, error) {
	return openLogDB(dirs, lowLatencyDirs, false, false)
}

func openLogDB(dirs []string,
	lowLatencyDirs []string, batched bool, check bool) (raftio.ILogDB, error) {
	checkDirs(dirs, lowLatencyDirs)
	llDirRequired := len(lowLatencyDirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < numOfRocksDBInstance; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lowLatencyDirs = append(lowLatencyDirs, lowLatencyDirs[0])
			}
		}
	}
	return OpenShardedRDB(dirs, lowLatencyDirs, batched, check)
}
