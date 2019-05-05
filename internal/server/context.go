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

package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/internal/utils/random"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
)

var (
	plog = logger.GetLogger("server")
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
	ErrIncompatibleData = errors.New("Incompatible data")
	// ErrLogDBBrokenChange indicates that you NodeHost failed to be created as
	// your code is hit by the LogDB broken change introduced in v3.0. Set your
	// onfig.NodeHostConfig.LogDBFactory to dragonboat.OpenBatchedLogDB to
	// continue.
	ErrLogDBBrokenChange = errors.New("Using new LogDB implementation on existing Raft Log")
)

const (
	addressFilename = "dragonboat.address"
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

// GetServerTLSConfig returns the server TLS config.
func (sc *Context) GetServerTLSConfig() *tls.Config {
	tc, err := sc.nhConfig.GetServerTLSConfig()
	if err != nil {
		panic(err)
	}
	return tc
}

// GetClientTLSConfig returns the client TLS config configured for the
// specified target hostname.
func (sc *Context) GetClientTLSConfig(hostname string) (*tls.Config, error) {
	return sc.nhConfig.GetClientTLSConfig(hostname)
}

// RemoveSnapshotDir removes the snapshot directory belong to the specified
// node.
func (sc *Context) RemoveSnapshotDir(did uint64, clusterID uint64,
	nodeID uint64) error {
	dir := sc.GetSnapshotDir(did, clusterID, nodeID)
	return os.RemoveAll(dir)
}

// GetSnapshotDir returns the snapshot directory name.
func (sc *Context) GetSnapshotDir(did uint64, clusterID uint64,
	nodeID uint64) string {
	dd := sc.getDeploymentIDSubDirName(did)
	pd := fmt.Sprintf("snapshot-part-%d", sc.partitioner.GetPartitionID(clusterID))
	sd := fmt.Sprintf("snapshot-%d-%d", clusterID, nodeID)
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	return filepath.Join(dirs[0], sc.hostname, dd, pd, sd)
}

// GetLogDBDirs returns the directory names for LogDB
func (sc *Context) GetLogDBDirs(did uint64) ([]string, []string) {
	lldirs := strings.Split(sc.nhConfig.WALDir, ":")
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	// low latency dir not empty
	if len(sc.nhConfig.WALDir) > 0 {
		if len(lldirs) != len(dirs) {
			plog.Panicf("%d low latency dirs specified, but there are %d regular dirs",
				len(lldirs), len(dirs))
		}
	}
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

// CreateNodeHostDir creates the top level dirs used by nodehost.
func (sc *Context) CreateNodeHostDir(did uint64) ([]string, []string) {
	nhDirs, walDirs := sc.GetLogDBDirs(did)
	exists := func(path string) (bool, error) {
		_, err := os.Stat(path)
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return true, err
	}
	for i := 0; i < len(nhDirs); i++ {
		walExist, err := exists(walDirs[i])
		if err != nil {
			panic(err)
		}
		nhExist, err := exists(nhDirs[i])
		if err != nil {
			panic(err)
		}
		if !walExist {
			if err := fileutil.MkdirAll(walDirs[i]); err != nil {
				panic(err)
			}
		}
		if !nhExist {
			if err := fileutil.MkdirAll(nhDirs[i]); err != nil {
				panic(err)
			}
		}
	}
	return nhDirs, walDirs
}

// PrepareSnapshotDir creates the snapshot directory for the specified node.
func (sc *Context) PrepareSnapshotDir(did uint64,
	clusterID uint64, nodeID uint64) (string, error) {
	snapshotDir := sc.GetSnapshotDir(did, clusterID, nodeID)
	if err := fileutil.MkdirAll(snapshotDir); err != nil {
		return "", err
	}
	return snapshotDir, nil
}

// CheckNodeHostDir checks whether NodeHost dir is owned by the
// current nodehost.
func (sc *Context) CheckNodeHostDir(did uint64,
	addr string, binVer uint32) error {
	dirs, lldirs := sc.GetLogDBDirs(did)
	for i := 0; i < len(dirs); i++ {
		if err := sc.exclusiveAccessTo(dirs[i], did, addr, binVer); err != nil {
			return err
		}
		if err := sc.exclusiveAccessTo(lldirs[i], did, addr, binVer); err != nil {
			return err
		}
	}
	return nil
}

// LockNodeHostDir tries to lock the NodeHost data directories.
func (sc *Context) LockNodeHostDir(did uint64) error {
	dirs, lldirs := sc.GetLogDBDirs(did)
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

func (sc *Context) exclusiveAccessTo(dir string,
	did uint64, addr string, ldbBinVer uint32) error {
	fp := filepath.Join(dir, addressFilename)
	se := func(s1 string, s2 string) bool {
		return strings.ToLower(strings.TrimSpace(s1)) ==
			strings.ToLower(strings.TrimSpace(s2))
	}
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		status := raftpb.RaftDataStatus{
			Address:  addr,
			BinVer:   ldbBinVer,
			HardHash: settings.Hard.Hash(),
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
		plog.Infof("%s vs %s", status.Address, addr)
		if !se(string(status.Address), addr) {
			return ErrNotOwner
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
		if status.HardHash != settings.Hard.Hash() {
			return ErrHardSettingsChanged
		}
	}
	return nil
}
