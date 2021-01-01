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

package server

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lni/goutils/random"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/id"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
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
	ErrDeploymentIDChanged = errors.New("deployment ID changed")
	// ErrAddressByNodeHostIDChanged is the error used to indicate that the
	// AddressByNodeHostID setting has changed.
	ErrAddressByNodeHostIDChanged = errors.New("AddressByNodeHostID changed")
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
	idFilename   = "NODEHOST.ID"
)

// Env is the server environment for NodeHost.
type Env struct {
	hostname     string
	nhid         *id.NodeHostID
	randomSource random.Source
	nhConfig     config.NodeHostConfig
	partitioner  IPartitioner
	flocks       map[string]io.Closer
	fs           vfs.IFS
}

// NewEnv creates and returns a new server Env object.
func NewEnv(nhConfig config.NodeHostConfig, fs vfs.IFS) (*Env, error) {
	s := &Env{
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

// Stop stops the environment.
func (env *Env) Stop() {
	for _, fl := range env.flocks {
		if err := fl.Close(); err != nil {
			panic(err)
		}
	}
}

// GetRandomSource returns the random source associated with the Nodehost.
func (env *Env) GetRandomSource() random.Source {
	return env.randomSource
}

// GetSnapshotDir returns the snapshot directory name.
func (env *Env) GetSnapshotDir(did uint64, clusterID uint64,
	nodeID uint64) string {
	parts, _, _ := env.getSnapshotDirParts(did, clusterID, nodeID)
	return env.fs.PathJoin(parts...)
}

func (env *Env) getSnapshotDirParts(did uint64,
	clusterID uint64, nodeID uint64) ([]string, string, []string) {
	dd := env.getDeploymentIDSubDirName(did)
	pd := fmt.Sprintf("snapshot-part-%d", env.partitioner.GetPartitionID(clusterID))
	sd := fmt.Sprintf("snapshot-%d-%d", clusterID, nodeID)
	dir := env.nhConfig.NodeHostDir
	parts := make([]string, 0)
	toBeCreated := make([]string, 0)
	return append(parts, dir, env.hostname, dd, pd, sd),
		env.fs.PathJoin(dir, env.hostname, dd), append(toBeCreated, pd, sd)
}

// GetLogDBDirs returns the directory names for LogDB
func (env *Env) GetLogDBDirs(did uint64) (string, string) {
	dir, lldir := env.getDataDirs()
	didStr := env.getDeploymentIDSubDirName(did)
	dir = env.fs.PathJoin(dir, env.hostname, didStr)
	if len(env.nhConfig.WALDir) > 0 {
		lldir = env.fs.PathJoin(lldir, env.hostname, didStr)
		return dir, lldir
	}
	return dir, dir
}

func (env *Env) getDataDirs() (string, string) {
	lldir := env.nhConfig.WALDir
	dir := env.nhConfig.NodeHostDir
	if len(env.nhConfig.WALDir) > 0 {
		return dir, lldir
	}
	return dir, dir
}

// CreateNodeHostDir creates the top level dirs used by nodehost.
func (env *Env) CreateNodeHostDir(did uint64) (string, string, error) {
	dir, lldir := env.GetLogDBDirs(did)
	if err := fileutil.MkdirAll(dir, env.fs); err != nil {
		return "", "", err
	}
	if err := fileutil.MkdirAll(lldir, env.fs); err != nil {
		return "", "", err
	}
	return dir, lldir, nil
}

// CreateSnapshotDir creates the snapshot directory for the specified node.
func (env *Env) CreateSnapshotDir(did uint64,
	clusterID uint64, nodeID uint64) error {
	_, path, parts := env.getSnapshotDirParts(did, clusterID, nodeID)
	for _, part := range parts {
		path = env.fs.PathJoin(path, part)
		exist, err := fileutil.Exist(path, env.fs)
		if err != nil {
			return err
		}
		if !exist {
			if err := fileutil.Mkdir(path, env.fs); err != nil {
				return err
			}
		} else {
			deleted, err := fileutil.IsDirMarkedAsDeleted(path, env.fs)
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

// NodeHostID returns the string representation of the NodeHost ID value.
func (env *Env) NodeHostID() string {
	return env.nhid.String()
}

// LoadNodeHostID loads the NodeHost ID value from the ID file. A new ID file
// will be created with a randomly assigned NodeHostID when running for the
// first time.
func (env *Env) LoadNodeHostID() (*id.NodeHostID, error) {
	dir, _ := env.getDataDirs()
	nhID := id.NewRandomNodeHostID()
	if fileutil.HasFlagFile(dir, idFilename, env.fs) {
		if err := fileutil.GetFlagFileContent(dir,
			idFilename, nhID, env.fs); err != nil {
			return nil, err
		}
	} else {
		if err := fileutil.CreateFlagFile(dir,
			idFilename, nhID, env.fs); err != nil {
			return nil, err
		}
	}
	env.nhid = nhID
	return nhID, nil
}

// SetNodeHostID sets the NodeHostID value recorded in Env. This is typically
// invoked by tests.
func (env *Env) SetNodeHostID(nhid *id.NodeHostID) {
	if env.nhid != nil {
		panic("trying to change NodeHostID")
	}
	env.nhid = nhid
}

// CheckNodeHostDir checks whether NodeHost dir is owned by the
// current nodehost.
func (env *Env) CheckNodeHostDir(cfg config.NodeHostConfig,
	binVer uint32, dbType string) error {
	return env.checkNodeHostDir(cfg, binVer, dbType, false)
}

// CheckLogDBType checks whether LogDB type is compatible.
func (env *Env) CheckLogDBType(cfg config.NodeHostConfig,
	dbType string) error {
	return env.checkNodeHostDir(cfg, 0, dbType, true)
}

// LockNodeHostDir tries to lock the NodeHost data directories.
func (env *Env) LockNodeHostDir() error {
	dir, lldir := env.getDataDirs()
	if err := env.tryLockNodeHostDir(dir); err != nil {
		return err
	}
	if err := env.tryLockNodeHostDir(lldir); err != nil {
		return err
	}
	return nil
}

// RemoveSnapshotDir marks the node snapshot directory as removed and have all
// existing snapshots deleted.
func (env *Env) RemoveSnapshotDir(did uint64,
	clusterID uint64, nodeID uint64) error {
	dir := env.GetSnapshotDir(did, clusterID, nodeID)
	exist, err := fileutil.Exist(dir, env.fs)
	if err != nil {
		return err
	}
	if exist {
		if err := env.markSnapshotDirRemoved(did, clusterID, nodeID); err != nil {
			return err
		}
		if err := removeSavedSnapshots(dir, env.fs); err != nil {
			return err
		}
	}
	return nil
}

func (env *Env) markSnapshotDirRemoved(did uint64, clusterID uint64,
	nodeID uint64) error {
	dir := env.GetSnapshotDir(did, clusterID, nodeID)
	s := &raftpb.RaftDataStatus{}
	return fileutil.MarkDirAsDeleted(dir, s, env.fs)
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

func (env *Env) checkNodeHostDir(cfg config.NodeHostConfig,
	binVer uint32, name string, dbto bool) error {
	dir, lldir := env.getDataDirs()
	if err := env.check(cfg, dir, binVer, name, dbto); err != nil {
		return err
	}
	if err := env.check(cfg, lldir, binVer, name, dbto); err != nil {
		return err
	}
	return nil
}

func (env *Env) tryLockNodeHostDir(dir string) error {
	fp := env.fs.PathJoin(dir, lockFilename)
	if _, ok := env.flocks[fp]; !ok {
		c, err := env.fs.Lock(fp)
		if err != nil {
			return ErrLockDirectory
		}
		env.flocks[fp] = c
	}
	return nil
}

func (env *Env) getDeploymentIDSubDirName(did uint64) string {
	return fmt.Sprintf("%020d", did)
}

func (env *Env) compatibleLogDBType(saved string, name string) bool {
	if saved == name {
		return true
	}
	return (saved == "rocksdb" && name == "pebble") ||
		(saved == "pebble" && name == "rocksdb")
}

func (env *Env) check(cfg config.NodeHostConfig,
	dir string, binVer uint32, name string, dbto bool) error {
	fn := flagFilename
	fp := env.fs.PathJoin(dir, fn)
	se := func(s1 string, s2 string) bool {
		return strings.EqualFold(strings.TrimSpace(s1), strings.TrimSpace(s2))
	}
	if _, err := env.fs.Stat(fp); vfs.IsNotExist(err) {
		if dbto {
			return nil
		}
		return env.createFlagFile(cfg, dir, binVer, name)
	}
	s := raftpb.RaftDataStatus{}
	if err := fileutil.GetFlagFileContent(dir, fn, &s, env.fs); err != nil {
		return err
	}
	if !env.compatibleLogDBType(s.LogdbType, name) {
		return ErrLogDBType
	}
	if !dbto {
		if !cfg.AddressByNodeHostID && !se(s.Address, cfg.RaftAddress) {
			return ErrNotOwner
		}
		if len(s.Hostname) > 0 && !se(s.Hostname, env.hostname) {
			return ErrHostnameChanged
		}
		if s.DeploymentId != 0 && s.DeploymentId != cfg.GetDeploymentID() {
			return ErrDeploymentIDChanged
		}
		if s.AddressByNodeHostId != cfg.AddressByNodeHostID {
			return ErrAddressByNodeHostIDChanged
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
			if s.HardHash != settings.HardHash(cfg.Expert.ExecShards,
				cfg.Expert.LogDB.Shards, settings.Hard.LRUMaxSessionCount,
				settings.Hard.LogDBEntryBatchSize) {
				return ErrHardSettingsChanged
			}
		} else {
			if s.StepWorkerCount != cfg.Expert.ExecShards ||
				s.LogdbShardCount != cfg.Expert.LogDB.Shards ||
				s.MaxSessionCount != settings.Hard.LRUMaxSessionCount ||
				s.EntryBatchSize != settings.Hard.LogDBEntryBatchSize {
				return ErrHardSettingChanged
			}
		}
	}
	return nil
}

func (env *Env) createFlagFile(cfg config.NodeHostConfig,
	dir string, ver uint32, name string) error {
	s := raftpb.RaftDataStatus{
		Address:             cfg.RaftAddress,
		BinVer:              ver,
		HardHash:            0,
		LogdbType:           name,
		Hostname:            env.hostname,
		DeploymentId:        cfg.GetDeploymentID(),
		StepWorkerCount:     cfg.Expert.ExecShards,
		LogdbShardCount:     cfg.Expert.LogDB.Shards,
		MaxSessionCount:     settings.Hard.LRUMaxSessionCount,
		EntryBatchSize:      settings.Hard.LogDBEntryBatchSize,
		AddressByNodeHostId: cfg.AddressByNodeHostID,
	}
	return fileutil.CreateFlagFile(dir, flagFilename, &s, env.fs)
}
