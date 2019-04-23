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

package tools

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/logger"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

var (
	plog = logger.GetLogger("tools")
)

var (
	ErrNodeExist          = errors.New("node exist")
	ErrInvalidMembers     = errors.New("invalid members")
	ErrPathNotExist       = errors.New("path does not exist")
	ErrIncompleteSnapshot = errors.New("snapshot is incomplete")
)

func ImportSnapshot(nhConfig config.NodeHostConfig,
	srcDir string, memberNodes map[uint64]string, nodeID uint64) error {
	addr, ok := memberNodes[nodeID]
	if !ok {
		return ErrInvalidMembers
	}
	if nhConfig.RaftAddress != addr {
		plog.Errorf("local node address in NodeHostConfig %s, in members %s",
			nhConfig.RaftAddress, addr)
		return ErrInvalidMembers
	}
	ssfp, err := getSnapshotFilepath(srcDir)
	if err != nil {
		return err
	}
	oldss, err := getSnapshotRecord(srcDir, server.SnapshotMetadataFilename)
	if err != nil {
		return err
	}
	ok, err = isCompleteSnapshotImage(ssfp, oldss)
	if err != nil {
		return err
	}
	if !ok {
		return ErrIncompleteSnapshot
	}
	if err := checkMembers(oldss.Membership, memberNodes); err != nil {
		return err
	}
	serverCtx := server.NewContext(nhConfig)
	defer serverCtx.Stop()
	serverCtx.CreateNodeHostDir(nhConfig.DeploymentID)
	serverCtx.CheckNodeHostDir(nhConfig.DeploymentID, nhConfig.RaftAddress)
	getSnapshotDir := func(cid uint64, nid uint64) string {
		return serverCtx.GetSnapshotDir(nhConfig.DeploymentID, cid, nid)
	}
	env := server.NewSnapshotEnv(getSnapshotDir,
		oldss.ClusterId, nodeID, nodeID, oldss.Index, server.SnapshottingMode)
	if err := env.CreateTempDir(); err != nil {
		return err
	}
	dstDir := env.GetTempDir()
	logdb, err := getLogDB(*serverCtx, nhConfig)
	if err != nil {
		return err
	}
	defer logdb.Close()
	ss := getProcessedSnapshotRecord(oldss, memberNodes)
	if err := copySnapshot(ss, srcDir, dstDir); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		return err
	}
	return logdb.ImportSnapshot(ss, nodeID, oldss.Type)
}

func isCompleteSnapshotImage(ssfp string, ss pb.Snapshot) (bool, error) {
	checksum, err := rsm.GetV2PayloadChecksum(ssfp)
	if err != nil {
		return false, err
	}
	return bytes.Compare(checksum, ss.Checksum) == 0, nil
}

func getSnapshotFilepath(dir string) (string, error) {
	if !fileutil.Exist(dir) {
		return "", ErrPathNotExist
	}
	mf := filepath.Join(dir, server.SnapshotMetadataFilename)
	if !fileutil.Exist(mf) {
		return "", ErrIncompleteSnapshot
	}
	files, err := getSnapshotFiles(dir)
	if err != nil {
		return "", err
	}
	if len(files) == 1 {
		return "", ErrIncompleteSnapshot
	}
	return files[0], nil
}

func getSnapshotFiles(path string) ([]string, error) {
	names, err := getSnapshotFilenames(path)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0)
	for _, name := range names {
		results = append(results, filepath.Join(path, name))
	}
	return results, nil
}

func getSnapshotFilenames(path string) ([]string, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), server.SnapshotFileSuffix) {
			results = append(results, file.Name())
		}
	}
	return results, nil
}

func getSnapshotRecord(dir string, filename string) (pb.Snapshot, error) {
	var ss pb.Snapshot
	if err := fileutil.GetFlagFileContent(dir, filename, &ss); err != nil {
		return pb.Snapshot{}, err
	}
	return ss, nil
}

func checkMembers(old pb.Membership, members map[uint64]string) error {
	for nodeID, addr := range members {
		v, ok := old.Addresses[nodeID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		_, ok = old.Removed[nodeID]
		if ok {
			return errors.New("adding a removed node")
		}
		_, ok = old.Observers[nodeID]
		if ok {
			return errors.New("adding an observer as regular node")
		}
	}
	return nil
}

func getProcessedSnapshotRecord(old pb.Snapshot,
	members map[uint64]string) pb.Snapshot {
	ss := pb.Snapshot{
		Filepath: old.Filepath,
		FileSize: old.FileSize,
		Index:    old.Index,
		Term:     old.Term,
		Files:    old.Files,
		Checksum: old.Checksum,
		Dummy:    old.Dummy,
		Membership: pb.Membership{
			Removed:   make(map[uint64]bool),
			Observers: make(map[uint64]string),
			Addresses: make(map[uint64]string),
		},
		Type:      old.Type,
		ClusterId: old.ClusterId,
	}
	for nodeID, _ := range old.Membership.Addresses {
		ss.Membership.Removed[nodeID] = true
	}
	for nodeID, _ := range old.Membership.Observers {
		ss.Membership.Removed[nodeID] = true
	}
	for nodeID := range old.Membership.Removed {
		ss.Membership.Removed[nodeID] = true
	}
	for nodeID, addr := range members {
		ss.Membership.Addresses[nodeID] = addr
	}
	return ss
}

func copySnapshot(ss pb.Snapshot, srcDir string, dstDir string) error {
	names, err := getSnapshotFilenames(srcDir)
	if err != nil {
		return err
	}
	if len(names) != 1 {
		return ErrIncompleteSnapshot
	}
	if err := copySnapshotFile(filepath.Join(srcDir, names[0]),
		filepath.Join(dstDir, names[0])); err != nil {
		return err
	}
	for _, file := range ss.Files {
		fname := filepath.Base(file.Filepath)
		if err := copySnapshotFile(filepath.Join(srcDir, fname),
			filepath.Join(dstDir, fname)); err != nil {
			return err
		}
	}
	return nil
}

func copySnapshotFile(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	fileutil.SyncDir(filepath.Dir(dst))
	return nil
}

func getLogDB(ctx server.Context,
	nhConfig config.NodeHostConfig) (raftio.ILogDB, error) {
	nhDir, walDir := ctx.GetLogDBDirs(nhConfig.DeploymentID)
	return logdb.OpenLogDB(nhDir, walDir)
}
