// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

/*
Package tools provides functions and types typically used to construct DevOps
tools for managing Dragonboat based applications.
*/

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/utils"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("tools")
)

var (
	unmanagedDeploymentID = settings.UnmanagedDeploymentID
	// ErrInvalidMembers indicates that the provided member nodes is invalid.
	ErrInvalidMembers = errors.New("invalid members")
	// ErrPathNotExist indicates that the specified exported snapshot directory
	// do not exist.
	ErrPathNotExist = errors.New("path does not exist")
	// ErrIncompleteSnapshot indicates that the specified exported snapshot
	// directory does not contain a complete snapshot.
	ErrIncompleteSnapshot = errors.New("snapshot is incomplete")
)

var firstError = utils.FirstError

// ImportSnapshot is used to repair the Raft shard already has its quorum
// nodes permanently lost or damaged. Such repair is only required when the
// Raft shard permanently lose its quorum. You are not suppose to use this
// function when the shard still have its majority nodes running or when
// the node failures are not permanent. In our experience, a well monitored
// and managed Dragonboat system can usually avoid using the ImportSnapshot
// tool by always replace permanently dead nodes with available ones in time.
//
// ImportSnapshot imports the exported snapshot available in the specified
// srcDir directory to the system and rewrites the history of node replicaID so
// the node owns the imported snapshot and the membership of the Raft shard
// is rewritten to the details specified in memberNodes.
//
// ImportSnapshot is typically invoked by a DevOps tool separated from the
// Dragonboat based application. The NodeHost instance must be stopped on that
// host when invoking the function ImportSnapshot.
//
// As an example, consider a Raft shard with three nodes with the ReplicaID
// values being 1, 2 and 3, they run on three distributed hostss each with a
// running NodeHost instance and the RaftAddress values are m1, m2 and
// m3. The ShardID value of the Raft shard is 100. Let's say hosts
// identified by m2 and m3 suddenly become permanently gone and thus cause the
// Raft shard to lose its quorum nodes. To repair the shard, we can use the
// ImportSnapshot function to overwrite the state and membership of the Raft
// shard.
//
// Assuming we have two other running hosts identified as m4 and m5, we want to
// have two new nodes with ReplicaID 4 and 5 to replace the permanently lost ndoes
// 2 and 3. In this case, the memberNodes map should contain the following
// content:
//
//	memberNodes: map[uint64]string{
//	  {1: "m1"}, {4: "m4"}, {5: "m5"},
//	}
//
// we first shutdown NodeHost instances on all involved hosts and call the
// ImportSnapshot function from the DevOps tool. Assuming the directory
// /backup/shard100 contains the exported snapshot we previously saved by using
// NodeHost's ExportSnapshot method, then -
//
// on m1, we call -
// ImportSnapshot(nhConfig1, "/backup/shard100", memberNodes, 1)
//
// on m4 -
// ImportSnapshot(nhConfig4, "/backup/shard100", memberNodes, 4)
//
// on m5 -
// ImportSnapshot(nhConfig5, "/backup/shard100", memberNodes, 5)
//
// The nhConfig* value used above should be the same as the one used to start
// your NodeHost instances, they are suppose to be slightly different on m1, m4
// and m5 to reflect the differences between these hosts, e.g. the RaftAddress
// values. srcDir values are all set to "/backup/shard100", that directory
// should contain the exact same snapshot. The memberNodes value should be the
// same across all three hosts.
//
// Once ImportSnapshot is called on all three of those hosts, we end up having
// the history of the Raft shard overwritten to the state in which -
//   - there are 3 nodes in the Raft shard, the ReplicaID values are 1, 4 and 5.
//     they run on hosts m1, m4 and m5.
//   - nodes 2 and 3 are permanently removed from the shard. you should never
//     restart any of them as both hosts m2 and m3 are suppose to be permanently
//     unavailable.
//   - the state captured in the snapshot became the state of the shard. all
//     proposals more recent than the state of the snapshot are lost.
//
// Once the NodeHost instances are restarted on m1, m4 and m5, nodes 1, 4 and 5
// of the Raft shard 100 can be restarted in the same way as after rebooting
// the hosts m1, m4 and m5.
//
// It is your applications's responsibility to let m4 and m5 to be aware that
// node 4 and 5 are now running there.
func ImportSnapshot(nhConfig config.NodeHostConfig,
	srcDir string, memberNodes map[uint64]string, replicaID uint64) (err error) {
	if nhConfig.DeploymentID == 0 {
		plog.Infof("NodeHostConfig.DeploymentID not set, default to %d",
			unmanagedDeploymentID)
		nhConfig.DeploymentID = unmanagedDeploymentID
	}
	if nhConfig.Expert.FS == nil {
		nhConfig.Expert.FS = vfs.DefaultFS
	}
	if err := nhConfig.Prepare(); err != nil {
		return err
	}
	fs := nhConfig.Expert.FS
	if err := checkImportSettings(nhConfig, memberNodes, replicaID); err != nil {
		return err
	}
	ssfp, err := getSnapshotFilepath(srcDir, fs)
	if err != nil {
		return err
	}
	oldss, err := getSnapshotRecord(srcDir, server.MetadataFilename, fs)
	if err != nil {
		return err
	}
	ok, err := isCompleteSnapshotImage(ssfp, oldss, fs)
	if err != nil {
		return err
	}
	if !ok {
		return ErrIncompleteSnapshot
	}
	if err := checkMembers(oldss.Membership, memberNodes); err != nil {
		return err
	}
	env, err := server.NewEnv(nhConfig, fs)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, env.Close())
	}()
	if _, _, err := env.CreateNodeHostDir(nhConfig.DeploymentID); err != nil {
		return err
	}
	logdb, err := getLogDB(*env, nhConfig)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, logdb.Close())
	}()
	if err := env.CheckNodeHostDir(nhConfig,
		logdb.BinaryFormat(), logdb.Name()); err != nil {
		return err
	}
	ssDir := env.GetSnapshotDir(nhConfig.DeploymentID,
		oldss.ShardID, replicaID)
	exist, err := fileutil.Exist(ssDir, fs)
	if err != nil {
		return err
	}
	if exist {
		if err := cleanupSnapshotDir(ssDir, fs); err != nil {
			return err
		}
	} else {
		if err := env.CreateSnapshotDir(nhConfig.DeploymentID,
			oldss.ShardID, replicaID); err != nil {
			return err
		}
	}
	getSnapshotDir := func(cid uint64, nid uint64) string {
		return env.GetSnapshotDir(nhConfig.DeploymentID, cid, nid)
	}
	ssEnv := server.NewSSEnv(getSnapshotDir,
		oldss.ShardID, replicaID, oldss.Index, replicaID, server.SnapshotMode, fs)
	if err := ssEnv.CreateTempDir(); err != nil {
		return err
	}
	dstDir := ssEnv.GetTempDir()
	finalDir := ssEnv.GetFinalDir()
	ss := getProcessedSnapshotRecord(finalDir, oldss, memberNodes, fs)
	if err := copySnapshot(oldss, srcDir, dstDir, fs); err != nil {
		return err
	}
	if err := ssEnv.FinalizeSnapshot(&ss); err != nil {
		return err
	}
	return logdb.ImportSnapshot(ss, replicaID)
}

func cleanupSnapshotDir(dir string, fs vfs.IFS) error {
	files, err := fs.List(dir)
	if err != nil {
		return err
	}
	for _, v := range files {
		fi, err := fs.Stat(fs.PathJoin(dir, v))
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			continue
		}
		name := []byte(fi.Name())
		if server.SnapshotDirNameRe.Match(name) ||
			server.GenSnapshotDirNameRe.Match(name) ||
			server.RecvSnapshotDirNameRe.Match(name) {
			ssdir := fs.PathJoin(dir, fi.Name())
			if err := fs.RemoveAll(ssdir); err != nil {
				return err
			}
		}
	}
	return fileutil.SyncDir(dir, fs)
}

func checkImportSettings(nhConfig config.NodeHostConfig,
	memberNodes map[uint64]string, replicaID uint64) error {
	addr, ok := memberNodes[replicaID]
	if !ok {
		plog.Errorf("node ID not found in the memberNode map")
		return ErrInvalidMembers
	}
	if nhConfig.RaftAddress != addr {
		plog.Errorf("node address in NodeHostConfig %s, in members %s",
			nhConfig.RaftAddress, addr)
		return ErrInvalidMembers
	}
	return nil
}

func isCompleteSnapshotImage(ssfp string,
	ss pb.Snapshot, fs vfs.IFS) (bool, error) {
	checksum, err := rsm.GetV2PayloadChecksum(ssfp, fs)
	if err != nil {
		return false, err
	}
	return bytes.Equal(checksum, ss.Checksum), nil
}

func getSnapshotFilepath(dir string, fs vfs.IFS) (string, error) {
	exist, err := fileutil.Exist(dir, fs)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", ErrPathNotExist
	}
	files, err := getSnapshotFiles(dir, fs)
	if err != nil {
		return "", err
	}
	if len(files) != 1 {
		return "", ErrIncompleteSnapshot
	}
	return files[0], nil
}

func getSnapshotFiles(path string, fs vfs.IFS) ([]string, error) {
	names, err := getSnapshotFilenames(path, fs)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0)
	for _, name := range names {
		results = append(results, fs.PathJoin(path, name))
	}
	return results, nil
}

func getSnapshotFilenames(path string, fs vfs.IFS) ([]string, error) {
	files, err := fs.List(path)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0)
	for _, v := range files {
		file, err := fs.Stat(fs.PathJoin(path, v))
		if err != nil {
			return nil, err
		}
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), server.SnapshotFileSuffix) {
			results = append(results, file.Name())
		}
	}
	return results, nil
}

func getSnapshotRecord(dir string,
	filename string, fs vfs.IFS) (pb.Snapshot, error) {
	var ss pb.Snapshot
	if err := fileutil.GetFlagFileContent(dir, filename, &ss, fs); err != nil {
		return pb.Snapshot{}, err
	}
	return ss, nil
}

func checkMembers(old pb.Membership, members map[uint64]string) error {
	for replicaID, addr := range members {
		v, ok := old.Addresses[replicaID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		v, ok = old.NonVotings[replicaID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		if ok {
			return errors.New("adding an nonVoting as regular node")
		}
		v, ok = old.Witnesses[replicaID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		if ok {
			return errors.New("adding a witness as regular node")
		}
		_, ok = old.Removed[replicaID]
		if ok {
			return errors.New("adding a removed node")
		}
	}
	return nil
}

func getProcessedSnapshotRecord(dstDir string,
	old pb.Snapshot, members map[uint64]string, fs vfs.IFS) pb.Snapshot {
	for _, file := range old.Files {
		file.Filepath = fs.PathJoin(dstDir, fs.PathBase(file.Filepath))
	}
	ss := pb.Snapshot{
		Filepath: fs.PathJoin(dstDir, fs.PathBase(old.Filepath)),
		FileSize: old.FileSize,
		Index:    old.Index,
		Term:     old.Term,
		Checksum: old.Checksum,
		Dummy:    old.Dummy,
		Membership: pb.Membership{
			ConfigChangeId: old.Index,
			Removed:        make(map[uint64]bool),
			NonVotings:     make(map[uint64]string),
			Addresses:      make(map[uint64]string),
			Witnesses:      make(map[uint64]string),
		},
		Files:    old.Files,
		Type:     old.Type,
		ShardID:  old.ShardID,
		Imported: true,
	}
	for nid := range old.Membership.Addresses {
		_, ok := members[nid]
		if !ok {
			ss.Membership.Removed[nid] = true
		}
	}
	for nid := range old.Membership.NonVotings {
		_, ok := members[nid]
		if !ok {
			ss.Membership.Removed[nid] = true
		}
	}
	for nid := range old.Membership.Witnesses {
		_, ok := members[nid]
		if !ok {
			ss.Membership.Removed[nid] = true
		}
	}
	for nid := range old.Membership.Removed {
		ss.Membership.Removed[nid] = true
	}
	for nid, addr := range members {
		ss.Membership.Addresses[nid] = addr
	}
	return ss
}

func copySnapshot(ss pb.Snapshot,
	srcDir string, dstDir string, fs vfs.IFS) error {
	fp, err := getSnapshotFilepath(srcDir, fs)
	if err != nil {
		return err
	}
	dstfp := fs.PathJoin(dstDir, fs.PathBase(fp))
	if err := copyFile(fp, dstfp, fs); err != nil {
		return err
	}
	for _, file := range ss.Files {
		fname := fs.PathBase(file.Filepath)
		if err := copyFile(fs.PathJoin(srcDir, fname),
			fs.PathJoin(dstDir, fname), fs); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src string, dst string, fs vfs.IFS) (err error) {
	in, err := fs.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, in.Close())
	}()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := fs.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, out.Close())
	}()
	if runtime.GOOS != "windows" {
		of, ok := out.(*os.File)
		if ok {
			if err := of.Chmod(fi.Mode()); err != nil {
				return err
			}
		}
	}
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	return fileutil.SyncDir(fs.PathDir(dst), fs)
}

func getLogDB(env server.Env,
	nhConfig config.NodeHostConfig) (raftio.ILogDB, error) {
	nhDir, walDir := env.GetLogDBDirs(nhConfig.DeploymentID)
	if nhConfig.Expert.LogDBFactory != nil {
		return nhConfig.Expert.LogDBFactory.Create(nhConfig,
			nil, []string{nhDir}, []string{walDir})
	}
	return logdb.NewDefaultLogDB(nhConfig, nil, []string{nhDir}, []string{walDir})
}
