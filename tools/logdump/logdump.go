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

package main

import (
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/logdb/kv/pebble"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
)

const flagFilename = "dragonboat.ds"

type LogDump struct {
	dir string
	vfs vfs.IFS
	log raftio.ILogDB
}

func OpenLogDumpReadOnly(dir string, ignoreLock bool) (*LogDump, error) {
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return nil, errors.Wrapf(err, "error: invalid data directory %q", dir)
	}

	var f = pebble.PebbleReadOnlyKVStore
	if ignoreLock {
		f = pebble.PebbleReadOnlyNoLockKVStore
	}
	var vfs = vfs.DefaultFS

	db, err := logdb.OpenReadOnlyLogDB(config.GetDefaultLogDBConfig(),
		dir, path.Join(dir, "wal"), vfs, f)
	if err != nil {
		return nil, errors.Wrap(err, "error opening database")
	}

	return &LogDump{
		dir: dir,
		vfs: vfs,
		log: db,
	}, nil
}

func (l *LogDump) VFS() vfs.IFS {
	return l.vfs
}

func (l *LogDump) LogDB() raftio.ILogDB {
	return l.log
}

func (l *LogDump) GetRaftDataStatus() (raftpb.RaftDataStatus, error) {
	s := raftpb.RaftDataStatus{}

	if err := fileutil.GetFlagFileContent(l.dir, flagFilename, &s, l.vfs); err != nil {
		return s, errors.Wrapf(err, "reading flag file content from %s\n", path.Join(l.dir, flagFilename))
	}

	return s, nil
}

func (l *LogDump) Dumper() Dumper {
	return *NewDumper(l.log)
}
