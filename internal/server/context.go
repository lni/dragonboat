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

package server

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/random"
)

var (
	plog = logger.GetLogger("server")
	// ErrHardSettingChanged indicates that one or more of the hard settings
	// changed.
	ErrHardSettingChanged = errors.New("hard setting changed")
	// ErrDirMarkedAsDeleted is the error used to indicate that the directory has
	// been marked as deleted and can not be used again.
	ErrDirMarkedAsDeleted = errors.New("trying to use a dir marked as deleted")
	// ErrHostnameChanged is the error used to indicate that the hostname changed.
	ErrHostnameChanged = errors.New("hostname changed")
	// ErrDeploymentIDChanged is the error used to indicate that the deployment
	// ID changed.
	ErrDeploymentIDChanged = errors.New("Deployment ID changed")
	// ErrLogDBType is the error used to indicate that the LogDB type changed.
	ErrLogDBType = errors.New("logdb type changed")
	// ErrNotOwner indicates that the data directory belong to another NodeHost
	// instance.
	ErrNotOwner = errors.New("not the owner of the data directory")
	// ErrLockDirectory indicates that obtaining exclusive lock to the data
	// directory failed.
	ErrLockDirectory = errors.New("failed to lock data directory")
	// ErrHardSettingsChanged indicates that hard settings changed.
	ErrHardSettingsChanged = errors.New("internal/settings/hard.go settings changed")
	// ErrIncompatibleData indicates that the specified data directory contains
	// incompatible data.
	ErrIncompatibleData = errors.New("incompatible LogDB data format")
	// ErrLogDBBrokenChange indicates that your NodeHost failed to be created as
	// your code is hit by the LogDB breaking change introduced in v3.0. Set your
	// NodeHostConfig.LogDBFactory to rocksdb.OpenBatchedLogDB to continue.
	ErrLogDBBrokenChange = errors.New("using new LogDB on existing Raft Log")
)

const (
	flagFilename = "dragonboat.ds"
	lockFilename = "LOCK"
)

// Context is the server context for NodeHost.
type Context struct {
	hostname     string
	randomSource random.Source
	nhConfig     config.NodeHostConfig
	partitioner  IPartitioner
	flocks       map[string]io.Closer
	fs           vfs.IFS
}

// NewContext creates and returns a new server Context object.
func NewContext(nhConfig config.NodeHostConfig, fs vfs.IFS) (*Context, error) {
	s := &Context{
		randomSource: random.NewLockedRand(),
		nhConfig:     nhConfig,
		partitioner:  NewFixedPartitioner(defaultClusterIDMod),
		flocks:       make(map[string]io.Closer),
		fs:           fs,
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	if len(hostname) == 0 {
		panic("failed to get hostname")
	}
	s.hostname = hostname
	return s, nil
}

// Stop stops the context.
func (sc *Context) Stop() {
	for _, fl := range sc.flocks {
		if err := fl.Close(); err != nil {
			panic(err)
		}
	}
}

// GetRandomSource returns the random source associated with the Nodehost.
func (sc *Context) GetRandomSource() random.Source {
	return sc.randomSource
}

// GetSnapshotDir returns the snapshot directory name.
func (sc *Context) GetSnapshotDir(did uint64, clusterID uint64,
	nodeID uint64) string {
	parts, _, _ := sc.getSnapshotDirParts(did, clusterID, nodeID)
	return sc.fs.PathJoin(parts...)
}

func (sc *Context) getSnapshotDirParts(did uint64,
	clusterID uint64, nodeID uint64) ([]string, string, []string) {
	dd := sc.getDeploymentIDSubDirName(did)
	pd := fmt.Sprintf("snapshot-part-%d", sc.partitioner.GetPartitionID(clusterID))
	sd := fmt.Sprintf("snapshot-%d-%d", clusterID, nodeID)
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	parts := make([]string, 0)
	toBeCreated := make([]string, 0)
	return append(parts, dirs[0], sc.hostname, dd, pd, sd),
		sc.fs.PathJoin(dirs[0], sc.hostname, dd), append(toBeCreated, pd, sd)
}

// GetLogDBDirs returns the directory names for LogDB
func (sc *Context) GetLogDBDirs(did uint64) ([]string, []string) {
	dirs, lldirs := sc.getDataDirs()
	didStr := sc.getDeploymentIDSubDirName(did)
	for i := 0; i < len(dirs); i++ {
		dirs[i] = sc.fs.PathJoin(dirs[i], sc.hostname, didStr)
	}
	if len(sc.nhConfig.WALDir) > 0 {
		for i := 0; i < len(dirs); i++ {
			lldirs[i] = sc.fs.PathJoin(lldirs[i], sc.hostname, didStr)
		}
		return dirs, lldirs
	}
	return dirs, dirs
}

func (sc *Context) getDataDirs() ([]string, []string) {
	lldirs := strings.Split(sc.nhConfig.WALDir, ":")
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	if len(sc.nhConfig.WALDir) > 0 {
		if len(dirs) != len(lldirs) {
			plog.Panicf("%d low latency dirs specified, but there are %d regular dirs",
				len(lldirs), len(dirs))
		}
		return dirs, lldirs
	}
	return dirs, dirs
}

// CreateNodeHostDir creates the top level dirs used by nodehost.
func (sc *Context) CreateNodeHostDir(did uint64) ([]string, []string, error) {
	dirs, lldirs := sc.GetLogDBDirs(did)
	for i := 0; i < len(dirs); i++ {
		if err := fileutil.MkdirAll(dirs[i], sc.fs); err != nil {
			return nil, nil, err
		}
		if err := fileutil.MkdirAll(lldirs[i], sc.fs); err != nil {
			return nil, nil, err
		}
	}
	return dirs, lldirs, nil
}

// CreateSnapshotDir creates the snapshot directory for the specified node.
func (sc *Context) CreateSnapshotDir(did uint64,
	clusterID uint64, nodeID uint64) error {
	_, path, parts := sc.getSnapshotDirParts(did, clusterID, nodeID)
	for _, part := range parts {
		path = sc.fs.PathJoin(path, part)
		exist, err := fileutil.Exist(path, sc.fs)
		if err != nil {
			return err
		}
		if !exist {
			if err := fileutil.Mkdir(path, sc.fs); err != nil {
				return err
			}
		} else {
			deleted, err := fileutil.IsDirMarkedAsDeleted(path, sc.fs)
			if err != nil {
				return err
			}
			if deleted {
				return ErrDirMarkedAsDeleted
			}
		}
	}
	return nil
}

// CheckNodeHostDir checks whether NodeHost dir is owned by the
// current nodehost.
func (sc *Context) CheckNodeHostDir(did uint64,
	addr string, binVer uint32, dbType string) error {
	return sc.checkNodeHostDir(did, addr, sc.hostname, binVer, dbType, false)
}

// CheckLogDBType checks whether LogDB type is compatible.
func (sc *Context) CheckLogDBType(did uint64, dbType string) error {
	return sc.checkNodeHostDir(did, "", "", 0, dbType, true)
}

// LockNodeHostDir tries to lock the NodeHost data directories.
func (sc *Context) LockNodeHostDir() error {
	dirs, lldirs := sc.getDataDirs()
	for i := 0; i < len(dirs); i++ {
		if err := sc.tryLockNodeHostDir(dirs[i]); err != nil {
			return err
		}
		if err := sc.tryLockNodeHostDir(lldirs[i]); err != nil {
			return err
		}
	}
	return nil
}

// RemoveSnapshotDir marks the node snapshot directory as removed and have all
// existing snapshots deleted.
func (sc *Context) RemoveSnapshotDir(did uint64,
	clusterID uint64, nodeID uint64) error {
	dir := sc.GetSnapshotDir(did, clusterID, nodeID)
	exist, err := fileutil.Exist(dir, sc.fs)
	if err != nil {
		return err
	}
	if exist {
		if err := sc.markSnapshotDirRemoved(did, clusterID, nodeID); err != nil {
			return err
		}
		if err := removeSavedSnapshots(dir, sc.fs); err != nil {
			return err
		}
	}
	return nil
}

func (sc *Context) markSnapshotDirRemoved(did uint64, clusterID uint64,
	nodeID uint64) error {
	dir := sc.GetSnapshotDir(did, clusterID, nodeID)
	s := &raftpb.RaftDataStatus{}
	return fileutil.MarkDirAsDeleted(dir, s, sc.fs)
}

func removeSavedSnapshots(dir string, fs vfs.IFS) error {
	files, err := fs.List(dir)
	if err != nil {
		return err
	}
	for _, fn := range files {
		fi, err := fs.Stat(fs.PathJoin(dir, fn))
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			continue
		}
		if SnapshotDirNameRe.Match([]byte(fi.Name())) {
			ssdir := fs.PathJoin(dir, fi.Name())
			if err := fs.RemoveAll(ssdir); err != nil {
				return err
			}
		}
	}
	return fileutil.SyncDir(dir, fs)
}

func (sc *Context) checkNodeHostDir(did uint64,
	addr string, hostname string, binVer uint32, name string, dbto bool) error {
	dirs, lldirs := sc.getDataDirs()
	for i := 0; i < len(dirs); i++ {
		if err := sc.check(dirs[i], did, addr, hostname, binVer, name, dbto); err != nil {
			return err
		}
		if err := sc.check(lldirs[i], did, addr, hostname, binVer, name, dbto); err != nil {
			return err
		}
	}
	return nil
}

func (sc *Context) tryLockNodeHostDir(dir string) error {
	fp := sc.fs.PathJoin(dir, lockFilename)
	_, ok := sc.flocks[fp]
	if !ok {
		c, err := sc.fs.Lock(fp)
		if err != nil {
			return ErrLockDirectory
		}
		sc.flocks[fp] = c
	}
	return nil
}

func (sc *Context) getDeploymentIDSubDirName(did uint64) string {
	return fmt.Sprintf("%020d", did)
}

func (sc *Context) compatibleLogDBType(saved string, name string) bool {
	if len(saved) > 0 && saved != name {
		if !((saved == "rocksdb" && name == "pebble") ||
			(saved == "pebble" && name == "rocksdb")) {
			return false
		}
	}
	return true
}

func (sc *Context) check(dir string,
	did uint64, addr string, hostname string,
	binVer uint32, name string, dbto bool) error {
	fn := flagFilename
	fp := sc.fs.PathJoin(dir, fn)
	se := func(s1 string, s2 string) bool {
		return strings.EqualFold(strings.TrimSpace(s1), strings.TrimSpace(s2))
	}
	if _, err := sc.fs.Stat(fp); vfs.IsNotExist(err) {
		if dbto {
			return nil
		}
		return sc.createFlagFile(dir, did, addr, hostname, binVer, name)
	}
	s := raftpb.RaftDataStatus{}
	if err := fileutil.GetFlagFileContent(dir, fn, &s, sc.fs); err != nil {
		return err
	}
	if !sc.compatibleLogDBType(s.LogdbType, name) {
		return ErrLogDBType
	}
	if !dbto {
		if !se(string(s.Address), addr) {
			return ErrNotOwner
		}
		if len(s.Hostname) > 0 && !se(s.Hostname, hostname) {
			return ErrHostnameChanged
		}
		if s.DeploymentId != 0 && s.DeploymentId != did {
			return ErrDeploymentIDChanged
		}
		if s.BinVer != binVer {
			if s.BinVer == raftio.LogDBBinVersion &&
				binVer == raftio.PlainLogDBBinVersion {
				return ErrLogDBBrokenChange
			}
			plog.Errorf("logdb binary ver changed, %d vs %d", s.BinVer, binVer)
			return ErrIncompatibleData
		}
		if s.HardHash != 0 {
			if s.HardHash != settings.Hard.Hash() {
				return ErrHardSettingsChanged
			}
		} else {
			if s.StepWorkerCount != settings.Hard.StepEngineWorkerCount ||
				s.LogdbShardCount != settings.Hard.LogDBPoolSize ||
				s.MaxSessionCount != settings.Hard.LRUMaxSessionCount ||
				s.EntryBatchSize != settings.Hard.LogDBEntryBatchSize {
				return ErrHardSettingChanged
			}
		}
	}
	return nil
}

func (sc *Context) createFlagFile(dir string,
	did uint64, addr string, hostname string, ver uint32, name string) error {
	s := raftpb.RaftDataStatus{
		Address:         addr,
		BinVer:          ver,
		HardHash:        0,
		LogdbType:       name,
		Hostname:        hostname,
		DeploymentId:    did,
		StepWorkerCount: settings.Hard.StepEngineWorkerCount,
		LogdbShardCount: settings.Hard.LogDBPoolSize,
		MaxSessionCount: settings.Hard.LRUMaxSessionCount,
		EntryBatchSize:  settings.Hard.LogDBEntryBatchSize,
	}
	return fileutil.CreateFlagFile(dir, flagFilename, &s, sc.fs)
}
