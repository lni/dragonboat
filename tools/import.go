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

package tools

/*
Package tools provides functions and types typically used to construct DevOps
tools for managing Dragonboat based applications.
*/

import (
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
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

// ImportSnapshot is used to repair the Raft cluster already has its quorum
// nodes permanently lost or damaged. Such repair is only required when the
// Raft cluster permanently lose its quorum. You are not suppose to use this
// function when the cluster still have its majority nodes running or when
// the node failures are not permanent. In our experience, a well monitored
// and managed Dragonboat system can usually avoid using the ImportSnapshot
// tool by always replace permanently dead nodes with available ones in time.
//
// ImportSnapshot imports the exported snapshot available in the specified
// srcDir directory to the system and rewrites the history of node nodeID so
// the node owns the imported snapshot and the membership of the Raft cluster
// is rewritten to the details specified in memberNodes.
//
// ImportSnapshot is typically invoked by a DevOps tool separated from the
// Dragonboat based application. The NodeHost instance must be stopped on that
// host when invoking the function ImportSnapshot.
//
// As an example, consider a Raft cluster with three nodes with the NodeID
// values being 1, 2 and 3, they run on three distributed hostss each with a
// running NodeHost instance and the RaftAddress values are m1, m2 and
// m3. The ClusterID value of the Raft cluster is 100. Let's say hosts
// identified by m2 and m3 suddenly become permanently gone and thus cause the
// Raft cluster to lose its quorum nodes. To repair the cluster, we can use the
// ImportSnapshot function to overwrite the state and membership of the Raft
// cluster.
//
// Assuming we have two other running hosts identified as m4 and m5, we want to
// have two new nodes with NodeID 4 and 5 to replace the permanently lost ndoes
// 2 and 3. In this case, the memberNodes map should contain the following
// content:
//
// memberNodes: map[uint64]string{
//   {1: "m1"}, {4: "m4"}, {5: "m5"},
// }
//
// we first shutdown NodeHost instances on all involved hosts and call the
// ImportSnapshot function from the DevOps tool. Assuming the directory
// /backup/cluster100 contains the exported snapshot we previously saved by using
// NodeHost's ExportSnapshot method, then -
//
// on m1, we call -
// ImportSnapshot(nhConfig1, "/backup/cluster100", memberNodes, 1)
//
// on m4 -
// ImportSnapshot(nhConfig4, "/backup/cluster100", memberNodes, 4)
//
// on m5 -
// ImportSnapshot(nhConfig5, "/backup/cluster100", memberNodes, 5)
//
// The nhConfig* value used above should be the same as the one used to start
// your NodeHost instances, they are suppose to be slightly different on m1, m4
// and m5 to reflect the differences between these hosts, e.g. the RaftAddress
// values. srcDir values are all set to "/backup/cluster100", that directory
// should contain the exact same snapshot. The memberNodes value should be the
// same across all three hosts.
//
// Once ImportSnapshot is called on all three of those hosts, we end up having
// the history of the Raft cluster overwritten to the state in which -
// * there are 3 nodes in the Raft cluster, the NodeID values are 1, 4 and 5.
//   they run on hosts m1, m4 and m5.
// * nodes 2 and 3 are permanently removed from the cluster. you should never
//   restart any of them as both hosts m2 and m3 are suppose to be permanently
//   unavailable.
// * the state captured in the snapshot became the state of the cluster. all
//   proposals more recent than the state of the snapshot are lost.
//
// Once the NodeHost instances are restarted on m1, m4 and m5, nodes 1, 4 and 5
// of the Raft cluster 100 can be restarted in the same way as after rebooting
// the hosts m1, m4 and m5.
//
// It is your applications's responsibility to let m4 and m5 to be aware that
// node 4 and 5 are now running there.
func ImportSnapshot(nhConfig config.NodeHostConfig,
	srcDir string, memberNodes map[uint64]string, nodeID uint64) error {
	if nhConfig.DeploymentID == 0 {
		plog.Infof("NodeHostConfig.DeploymentID not set, default to %d",
			unmanagedDeploymentID)
		nhConfig.DeploymentID = unmanagedDeploymentID
	}
	if nhConfig.FS == nil {
		nhConfig.FS = vfs.DefaultFS
	}
	if err := nhConfig.Prepare(); err != nil {
		return err
	}
	fs := nhConfig.FS
	if err := checkImportSettings(nhConfig, memberNodes, nodeID); err != nil {
		return err
	}
	ssfp, err := getSnapshotFilepath(srcDir, fs)
	if err != nil {
		return err
	}
	oldss, err := getSnapshotRecord(srcDir, server.SnapshotMetadataFilename, fs)
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
	serverCtx, err := server.NewContext(nhConfig, fs)
	if err != nil {
		return err
	}
	defer serverCtx.Stop()
	if _, _, err := serverCtx.CreateNodeHostDir(nhConfig.DeploymentID); err != nil {
		return err
	}
	logdb, err := getLogDB(*serverCtx, nhConfig, fs)
	if err != nil {
		return err
	}
	defer logdb.Close()

	if err := serverCtx.CheckNodeHostDir(nhConfig.DeploymentID,
		nhConfig.RaftAddress, logdb.BinaryFormat(), logdb.Name()); err != nil {
		return err
	}
	ssDir := serverCtx.GetSnapshotDir(nhConfig.DeploymentID,
		oldss.ClusterId, nodeID)
	exist, err := fileutil.Exist(ssDir, fs)
	if err != nil {
		return err
	}
	if exist {
		if err := cleanupSnapshotDir(ssDir, fs); err != nil {
			return err
		}
	} else {
		if err := serverCtx.CreateSnapshotDir(nhConfig.DeploymentID,
			oldss.ClusterId, nodeID); err != nil {
			return err
		}
	}
	getSnapshotDir := func(cid uint64, nid uint64) string {
		return serverCtx.GetSnapshotDir(nhConfig.DeploymentID, cid, nid)
	}
	env := server.NewSSEnv(getSnapshotDir,
		oldss.ClusterId, nodeID, oldss.Index, nodeID, server.SnapshottingMode, fs)
	if err := env.CreateTempDir(); err != nil {
		return err
	}
	dstDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	ss := getProcessedSnapshotRecord(finalDir, oldss, memberNodes, fs)
	if err := copySnapshot(oldss, srcDir, dstDir, fs); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		return err
	}
	return logdb.ImportSnapshot(ss, nodeID)
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
	memberNodes map[uint64]string, nodeID uint64) error {
	addr, ok := memberNodes[nodeID]
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
	for nodeID, addr := range members {
		v, ok := old.Addresses[nodeID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		v, ok = old.Observers[nodeID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		if ok {
			return errors.New("adding an observer as regular node")
		}
		v, ok = old.Witnesses[nodeID]
		if ok && v != addr {
			return errors.New("node address changed")
		}
		if ok {
			return errors.New("adding a witness as regular node")
		}
		_, ok = old.Removed[nodeID]
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
			Observers:      make(map[uint64]string),
			Addresses:      make(map[uint64]string),
			Witnesses:      make(map[uint64]string),
		},
		Files:     old.Files,
		Type:      old.Type,
		ClusterId: old.ClusterId,
		Imported:  true,
	}
	for nid := range old.Membership.Addresses {
		_, ok := members[nid]
		if !ok {
			ss.Membership.Removed[nid] = true
		}
	}
	for nid := range old.Membership.Observers {
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
		if cerr := in.Close(); err == nil {
			err = cerr
		}
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
		if cerr := out.Close(); err == nil {
			err = cerr
		}
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

func getLogDB(ctx server.Context,
	nhConfig config.NodeHostConfig, fs vfs.IFS) (raftio.ILogDB, error) {
	nhDir, walDir := ctx.GetLogDBDirs(nhConfig.DeploymentID)
	return logdb.NewDefaultLogDB(nhConfig.LogDBConfig,
		nil, []string{nhDir}, []string{walDir}, fs)
}
