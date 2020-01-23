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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"

	"github.com/golang/protobuf/proto"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/vfs"
)

var (
	// ErrSnapshotOutOfDate is the error to indicate that snapshot is out of date.
	ErrSnapshotOutOfDate = errors.New("snapshot out of date")
	// SnapshotMetadataFilename is the filename of a snapshot's metadata file.
	SnapshotMetadataFilename = "snapshot.metadata"
	// SnapshotFileSuffix is the filename suffix of a snapshot file.
	SnapshotFileSuffix = "gbsnap"
	// SnapshotDirNameRe is the regex of snapshot names.
	SnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+$`)
	// GenSnapshotDirNameRe is the regex of temp snapshot directory name used when
	// generating snapshots.
	GenSnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+-[0-9A-F]+\.generating$`)
	// RecvSnapshotDirNameRe is the regex of temp snapshot directory name used when
	// receiving snapshots from remote NodeHosts.
	RecvSnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+-[0-9A-F]+\.receiving$`)
)

var (
	genTmpDirSuffix  = "generating"
	recvTmpDirSuffix = "receiving"
	shrunkSuffix     = "shrunk"
)

var finalizeLock sync.Mutex

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

// GetSnapshotFilename returns the filename of the snapshot file.
func GetSnapshotFilename(index uint64) string {
	return getSnapshotFilename(index)
}

func mustBeChild(parent string, child string) {
	if v, err := filepath.Rel(parent, child); err != nil {
		plog.Panicf("%v", err)
	} else {
		if len(v) == 0 || strings.Contains(v, string(filepath.Separator)) ||
			strings.HasPrefix(v, ".") {
			plog.Panicf("not a direct child, %s", v)
		}
	}
}

func getSnapshotDirName(index uint64) string {
	return fmt.Sprintf("snapshot-%016X", index)
}

func getSnapshotFilename(index uint64) string {
	return fmt.Sprintf("snapshot-%016X.%s", index, SnapshotFileSuffix)
}

func getShrinkedSnapshotFilename(index uint64) string {
	return fmt.Sprintf("snapshot-%016X.%s", index, shrunkSuffix)
}

func getTempSnapshotDirName(rootDir string,
	suffix string, index uint64, from uint64) string {
	dir := fmt.Sprintf("%s-%d.%s", getSnapshotDirName(index), from, suffix)
	return filepath.Join(rootDir, dir)
}

func getFinalSnapshotDirName(rootDir string, index uint64) string {
	return filepath.Join(rootDir, getSnapshotDirName(index))
}

// SSEnv is the struct used to manage involved directories for taking or
// receiving snapshots.
type SSEnv struct {
	index uint64
	// rootDir is the parent of all snapshot tmp/final dirs for a specified
	// raft node
	rootDir  string
	tmpDir   string
	finalDir string
	filepath string
	fs       vfs.IFS
}

// NewSSEnv creates and returns a new SSEnv instance.
func NewSSEnv(f GetSnapshotDirFunc,
	clusterID uint64, nodeID uint64, index uint64,
	from uint64, mode Mode, fs vfs.IFS) *SSEnv {
	var tmpSuffix string
	if mode == SnapshottingMode {
		tmpSuffix = genTmpDirSuffix
	} else {
		tmpSuffix = recvTmpDirSuffix
	}
	rootDir := f(clusterID, nodeID)
	fp := filepath.Join(getFinalSnapshotDirName(rootDir, index),
		getSnapshotFilename(index))
	return &SSEnv{
		index:    index,
		rootDir:  rootDir,
		tmpDir:   getTempSnapshotDirName(rootDir, tmpSuffix, index, from),
		finalDir: getFinalSnapshotDirName(rootDir, index),
		filepath: fp,
		fs:       fs,
	}
}

// GetTempDir returns the temp snapshot directory.
func (se *SSEnv) GetTempDir() string {
	return se.tmpDir
}

// GetFinalDir returns the final snapshot directory.
func (se *SSEnv) GetFinalDir() string {
	return se.finalDir
}

// GetRootDir returns the root directory. The temp and final snapshot
// directories are children of the root directory.
func (se *SSEnv) GetRootDir() string {
	return se.rootDir
}

// RemoveTempDir removes the temp snapshot directory.
func (se *SSEnv) RemoveTempDir() error {
	return se.removeDir(se.tmpDir)
}

// MustRemoveTempDir removes the temp snapshot directory and panic if there
// is any error.
func (se *SSEnv) MustRemoveTempDir() {
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

// FinalizeSnapshot finalizes the snapshot.
func (se *SSEnv) FinalizeSnapshot(msg proto.Message) error {
	finalizeLock.Lock()
	defer finalizeLock.Unlock()
	if err := se.createFlagFile(msg); err != nil {
		return err
	}
	if se.finalDirExists() {
		return ErrSnapshotOutOfDate
	}
	return se.renameTempDirToFinalDir()
}

// CreateTempDir creates the temp snapshot directory.
func (se *SSEnv) CreateTempDir() error {
	return se.createDir(se.tmpDir)
}

// RemoveFinalDir removes the final snapshot directory.
func (se *SSEnv) RemoveFinalDir() error {
	return se.removeDir(se.finalDir)
}

// SaveSSMetadata saves the metadata of the snapshot file.
func (se *SSEnv) SaveSSMetadata(msg proto.Message) error {
	err := fileutil.CreateFlagFile(se.tmpDir,
		SnapshotMetadataFilename, msg, se.fs)
	return err
}

// HasFlagFile returns a boolean flag indicating whether the flag file is
// available in the final directory.
func (se *SSEnv) HasFlagFile() bool {
	fp := filepath.Join(se.finalDir, fileutil.SnapshotFlagFilename)
	if _, err := se.fs.Stat(fp); os.IsNotExist(err) {
		return false
	}
	return true
}

// RemoveFlagFile removes the flag file from the final directory.
func (se *SSEnv) RemoveFlagFile() error {
	return fileutil.RemoveFlagFile(se.finalDir,
		fileutil.SnapshotFlagFilename, se.fs)
}

// GetFilename returns the snapshot filename.
func (se *SSEnv) GetFilename() string {
	return getSnapshotFilename(se.index)
}

// GetFilepath returns the snapshot file path.
func (se *SSEnv) GetFilepath() string {
	return filepath.Join(se.finalDir, getSnapshotFilename(se.index))
}

// GetShrinkedFilepath returns the file path of the shrunk snapshot.
func (se *SSEnv) GetShrinkedFilepath() string {
	return filepath.Join(se.finalDir, getShrinkedSnapshotFilename(se.index))
}

// GetTempFilepath returns the temp snapshot file path.
func (se *SSEnv) GetTempFilepath() string {
	return filepath.Join(se.tmpDir, getSnapshotFilename(se.index))
}

func (se *SSEnv) createDir(dir string) error {
	mustBeChild(se.rootDir, dir)
	return fileutil.Mkdir(dir, se.fs)
}

func (se *SSEnv) removeDir(dir string) error {
	mustBeChild(se.rootDir, dir)
	if err := se.fs.RemoveAll(dir); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir, se.fs)
}

func (se *SSEnv) finalDirExists() bool {
	if _, err := se.fs.Stat(se.finalDir); os.IsNotExist(err) {
		return false
	}
	return true
}

func (se *SSEnv) renameTempDirToFinalDir() error {
	if err := se.fs.Rename(se.tmpDir, se.finalDir); err != nil {
		return err
	}
	return fileutil.SyncDir(se.rootDir, se.fs)
}

func (se *SSEnv) createFlagFile(msg proto.Message) error {
	return fileutil.CreateFlagFile(se.tmpDir,
		fileutil.SnapshotFlagFilename, msg, se.fs)
}
