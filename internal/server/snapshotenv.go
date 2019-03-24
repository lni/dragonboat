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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/internal/utils/fileutil"
)

var (
	genTmpDirSuffix    = "generating"
	recvTmpDirSuffix   = "receiving"
	snapshotFileSuffix = "gbsnap"
)

// GetSnapshotDirFunc is the function type that returns the snapshot dir
// for the specified raft node.
type GetSnapshotDirFunc func(clusterID uint64, nodeID uint64) string

// Mode is the snapshot env mode.
type Mode uint64

const (
	// SnapshottingMode is the mode used when taking snapshotting.
	SnapshottingMode Mode = iota
	// ReceivingMode is the mode used when receiving snapshots from remote nodes.
	ReceivingMode
)

// GetSnapshotDirName returns the snapshot dir name for the snapshot captured
// at the specified index.
func GetSnapshotDirName(index uint64) string {
	return getSnapshotDirName(index)
}

func GetSnapshotFilename(index uint64) string {
	return getSnapshotFilename(index)
}

func mustBeChild(parent string, child string) error {
	if v, err := filepath.Rel(parent, child); err != nil {
		plog.Panicf("%v", err)
	} else {
		if len(v) == 0 || strings.Contains(v, string(filepath.Separator)) ||
			strings.HasPrefix(v, ".") {
			plog.Panicf("not a direct child, %s", v)
		}
	}
	return nil
}

func getSnapshotDirName(index uint64) string {
	return fmt.Sprintf("snapshot-%016X", index)
}

func getSnapshotFilename(index uint64) string {
	return fmt.Sprintf("snapshot-%016X.%s", index, snapshotFileSuffix)
}

func getTempSnapshotDirName(rootDir string,
	suffix string, index uint64, from uint64) string {
	dir := fmt.Sprintf("%s-%d.%s", getSnapshotDirName(index), from, suffix)
	return filepath.Join(rootDir, dir)
}

func getFinalSnapshotDirName(rootDir string, index uint64) string {
	return filepath.Join(rootDir, getSnapshotDirName(index))
}

// SnapshotEnv is the struct used to manage involved directories for taking or
// receiving snapshots.
type SnapshotEnv struct {
	index uint64
	// rootDir is the parent of all snapshot tmp/final dirs for a specified
	// raft node
	rootDir  string
	tmpDir   string
	finalDir string
	filepath string
}

// NewSnapshotEnv creates and returns a new SnapshotEnv instance.
func NewSnapshotEnv(f GetSnapshotDirFunc,
	clusterID uint64, nodeID uint64, index uint64,
	from uint64, mode Mode) *SnapshotEnv {
	var tmpSuffix string
	if mode == SnapshottingMode {
		tmpSuffix = genTmpDirSuffix
	} else {
		tmpSuffix = recvTmpDirSuffix
	}
	rootDir := f(clusterID, nodeID)
	fp := filepath.Join(getFinalSnapshotDirName(rootDir, index),
		getSnapshotFilename(index))
	return &SnapshotEnv{
		index:    index,
		rootDir:  rootDir,
		tmpDir:   getTempSnapshotDirName(rootDir, tmpSuffix, index, from),
		finalDir: getFinalSnapshotDirName(rootDir, index),
		filepath: fp,
	}
}

// GetTempDir returns the temp snapshot directory.
func (se *SnapshotEnv) GetTempDir() string {
	return se.tmpDir
}

// GetFinalDir returns the final snapshot directory.
func (se *SnapshotEnv) GetFinalDir() string {
	return se.finalDir
}

// GetRootDir returns the root directory. The temp and final snapshot
// directories are children of the root directory.
func (se *SnapshotEnv) GetRootDir() string {
	return se.rootDir
}

// RemoveTempDir removes the temp snapshot directory.
func (se *SnapshotEnv) RemoveTempDir() error {
	return se.removeDir(se.tmpDir)
}

// MustRemoveTempDir removes the temp snapshot directory and panic if there
// is any error.
func (se *SnapshotEnv) MustRemoveTempDir() {
	if err := se.removeDir(se.tmpDir); err != nil {
		if operr, ok := err.(*os.PathError); ok {
			if errno, ok := operr.Err.(syscall.Errno); ok {
				if errno == syscall.ENOENT {
					return
				}
			}
		}
		panic(err)
	}
}

// CreateTempDir creates the temp snapshot directory.
func (se *SnapshotEnv) CreateTempDir() error {
	return se.createDir(se.tmpDir)
}

// RemoveFinalDir removes the final snapshot directory.
func (se *SnapshotEnv) RemoveFinalDir() error {
	return se.removeDir(se.finalDir)
}

// IsFinalDirExists returns a boolean value indicating whether the final
// directory exists.
func (se *SnapshotEnv) IsFinalDirExists() bool {
	if _, err := os.Stat(se.finalDir); os.IsNotExist(err) {
		return false
	}
	return true
}

// see rename() in go/src/os/file_unix.go for details
// checked on golang 1.10/1.11
func isTargetDirExistError(err error) bool {
	e, ok := err.(*os.LinkError)
	if ok {
		return e.Err == syscall.EEXIST || e.Err == syscall.ENOTEMPTY
	}
	return false
}

// RenameTempDirToFinalDir renames the temp directory to the final directory
// name. It return a boolean value indicating whether the operation failed
// due to target directory already exists.
func (se *SnapshotEnv) RenameTempDirToFinalDir() (bool, error) {
	if err := os.Rename(se.tmpDir, se.finalDir); err != nil {
		return isTargetDirExistError(err), err
	}
	return false, fileutil.SyncDir(se.rootDir)
}

// CreateFlagFile creates the flag file in the temp directory.
func (se *SnapshotEnv) CreateFlagFile(msg proto.Message) error {
	return fileutil.CreateFlagFile(se.tmpDir,
		fileutil.SnapshotFlagFilename, msg)
}

// HasFlagFile returns a boolean flag indicating whether the flag file is
// available in the final directory.
func (se *SnapshotEnv) HasFlagFile() bool {
	fp := filepath.Join(se.finalDir, fileutil.SnapshotFlagFilename)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		return false
	}
	return true
}

// RemoveFlagFile removes the flag file from the final directory.
func (se *SnapshotEnv) RemoveFlagFile() error {
	return fileutil.RemoveFlagFile(se.finalDir, fileutil.SnapshotFlagFilename)
}

// GetFilename returns the snapshot filename.
func (se *SnapshotEnv) GetFilename() string {
	return getSnapshotFilename(se.index)
}

// GetFilepath returns the snapshot file path.
func (se *SnapshotEnv) GetFilepath() string {
	return filepath.Join(se.finalDir, getSnapshotFilename(se.index))
}

// GetTempFilepath returns the temp snapshot file path.
func (se *SnapshotEnv) GetTempFilepath() string {
	return filepath.Join(se.tmpDir, getSnapshotFilename(se.index))
}

func (se *SnapshotEnv) createDir(dir string) error {
	if err := mustBeChild(se.rootDir, dir); err != nil {
		return err
	}
	if err := fileutil.MkdirAll(dir); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir)
}

func (se *SnapshotEnv) removeDir(dir string) error {
	if err := mustBeChild(se.rootDir, dir); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir)
}
