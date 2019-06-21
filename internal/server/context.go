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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
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
	ErrHardSettingsChanged = errors.New("settings in internal/settings/hard.go changed")
	// ErrIncompatibleData indicates that the configured data directory contains
	// incompatible data.
	ErrIncompatibleData = errors.New("Incompatible LogDB data format")
	// ErrLogDBBrokenChange indicates that you NodeHost failed to be created as
	// your code is hit by the LogDB broken change introduced in v3.0. Set your
	// NodeHostConfig.LogDBFactory to rocksdb.OpenBatchedLogDB to continue.
	ErrLogDBBrokenChange = errors.New("Using new LogDB implementation on existing Raft Log")
)

const (
	addressFilename = "dragonboat.ds"
	lockFilename    = "LOCK"
)

// Context is the server context for NodeHost.
type Context struct {
	hostname     string
	randomSource random.Source
	nhConfig     config.NodeHostConfig
	partitioner  IPartitioner
	flocks       map[string]*fileutil.Flock
}

// NewContext creates and returns a new server Context object.
func NewContext(nhConfig config.NodeHostConfig) (*Context, error) {
	s := &Context{
		randomSource: random.NewLockedRand(),
		nhConfig:     nhConfig,
		partitioner:  NewFixedPartitioner(defaultClusterIDMod),
		flocks:       make(map[string]*fileutil.Flock),
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
		if err := fl.Unlock(); err != nil {
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
	return filepath.Join(parts...)
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
		filepath.Join(dirs[0], sc.hostname, dd), append(toBeCreated, pd, sd)
}

// GetLogDBDirs returns the directory names for LogDB
func (sc *Context) GetLogDBDirs(did uint64) ([]string, []string) {
	dirs, lldirs := sc.getDataDirs()
	didStr := sc.getDeploymentIDSubDirName(did)
	for i := 0; i < len(dirs); i++ {
		dirs[i] = filepath.Join(dirs[i], sc.hostname, didStr)
	}
	if len(sc.nhConfig.WALDir) > 0 {
		for i := 0; i < len(dirs); i++ {
			lldirs[i] = filepath.Join(lldirs[i], sc.hostname, didStr)
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
		if err := fileutil.MkdirAll(dirs[i]); err != nil {
			return nil, nil, err
		}
		if err := fileutil.MkdirAll(lldirs[i]); err != nil {
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
		path = filepath.Join(path, part)
		exist, err := fileutil.Exist(path)
		if err != nil {
			return err
		}
		if !exist {
			if err := fileutil.Mkdir(path); err != nil {
				return err
			}
		} else {
			deleted, err := fileutil.IsDirMarkedAsDeleted(path)
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
		if err := sc.tryCreateLockFile(dirs[i], lockFilename); err != nil {
			return err
		}
		if err := sc.tryLockNodeHostDir(dirs[i]); err != nil {
			return err
		}
		if err := sc.tryCreateLockFile(lldirs[i], lockFilename); err != nil {
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
	exist, err := fileutil.Exist(dir)
	if err != nil {
		return err
	}
	if exist {
		if err := sc.markSnapshotDirRemoved(did, clusterID, nodeID); err != nil {
			return err
		}
		if err := removeSavedSnapshots(dir); err != nil {
			return err
		}
	}
	return nil
}

func (sc *Context) markSnapshotDirRemoved(did uint64, clusterID uint64,
	nodeID uint64) error {
	dir := sc.GetSnapshotDir(did, clusterID, nodeID)
	s := &raftpb.RaftDataStatus{}
	return fileutil.MarkDirAsDeleted(dir, s)
}

func removeSavedSnapshots(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		if SnapshotDirNameRe.Match([]byte(fi.Name())) {
			ssdir := filepath.Join(dir, fi.Name())
			if err := os.RemoveAll(ssdir); err != nil {
				return err
			}
		}
	}
	return fileutil.SyncDir(dir)
}

func (sc *Context) checkNodeHostDir(did uint64,
	addr string, hostname string, binVer uint32, name string, dbto bool) error {
	dirs, lldirs := sc.getDataDirs()
	for i := 0; i < len(dirs); i++ {
		if err := sc.compatible(dirs[i], did, addr, hostname, binVer, name, dbto); err != nil {
			return err
		}
		if err := sc.compatible(lldirs[i], did, addr, hostname, binVer, name, dbto); err != nil {
			return err
		}
	}
	return nil
}

func (sc *Context) tryCreateLockFile(dir string, fl string) error {
	fp := filepath.Join(dir, fl)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		s := &raftpb.RaftDataStatus{}
		err = fileutil.CreateFlagFile(dir, fl, s)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func (sc *Context) tryLockNodeHostDir(dir string) error {
	fp := filepath.Join(dir, lockFilename)
	if err := sc.tryCreateLockFile(dir, lockFilename); err != nil {
		return err
	}
	var fl *fileutil.Flock
	_, ok := sc.flocks[fp]
	if !ok {
		fl = fileutil.New(fp)
		sc.flocks[fp] = fl
	} else {
		return nil
	}
	locked, err := fl.TryLock()
	if err != nil {
		return err
	}
	if locked {
		return nil
	}
	return ErrLockDirectory
}

func (sc *Context) getDeploymentIDSubDirName(did uint64) string {
	return fmt.Sprintf("%020d", did)
}

func (sc *Context) compatible(dir string,
	did uint64, addr string, hostname string,
	ldbBinVer uint32, name string, dbto bool) error {
	fp := filepath.Join(dir, addressFilename)
	se := func(s1 string, s2 string) bool {
		return strings.ToLower(strings.TrimSpace(s1)) ==
			strings.ToLower(strings.TrimSpace(s2))
	}
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		if dbto {
			return nil
		}
		status := raftpb.RaftDataStatus{
			Address:         addr,
			BinVer:          ldbBinVer,
			HardHash:        0,
			LogdbType:       name,
			Hostname:        hostname,
			DeploymentId:    did,
			StepWorkerCount: settings.Hard.StepEngineWorkerCount,
			LogdbShardCount: settings.Hard.LogDBPoolSize,
			MaxSessionCount: settings.Hard.LRUMaxSessionCount,
			EntryBatchSize:  settings.Hard.LogDBEntryBatchSize,
		}
		err = fileutil.CreateFlagFile(dir, addressFilename, &status)
		if err != nil {
			return err
		}
	} else {
		status := raftpb.RaftDataStatus{}
		err := fileutil.GetFlagFileContent(dir, addressFilename, &status)
		if err != nil {
			return err
		}
		if len(status.LogdbType) > 0 && status.LogdbType != name {
			return ErrLogDBType
		}
		if !dbto {
			if !se(string(status.Address), addr) {
				return ErrNotOwner
			}
			if len(status.Hostname) > 0 && !se(status.Hostname, hostname) {
				return ErrHostnameChanged
			}
			if status.DeploymentId != 0 && status.DeploymentId != did {
				return ErrDeploymentIDChanged
			}
			if status.BinVer != ldbBinVer {
				if status.BinVer == raftio.LogDBBinVersion &&
					ldbBinVer == raftio.PlainLogDBBinVersion {
					return ErrLogDBBrokenChange
				}
				plog.Errorf("binary compatibility version, data dir %d, software %d",
					status.BinVer, ldbBinVer)
				return ErrIncompatibleData
			}
			if status.HardHash != 0 {
				if status.HardHash != settings.Hard.Hash() {
					return ErrHardSettingsChanged
				}
			} else {
				if status.StepWorkerCount != settings.Hard.StepEngineWorkerCount ||
					status.LogdbShardCount != settings.Hard.LogDBPoolSize ||
					status.MaxSessionCount != settings.Hard.LRUMaxSessionCount ||
					status.EntryBatchSize != settings.Hard.LogDBEntryBatchSize {
					return ErrHardSettingChanged
				}
			}
		}
	}
	return nil
}
