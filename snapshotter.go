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

package dragonboat

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	snapshotsToKeep = 3
)

var (
	// ErrNoSnapshot is the error used to indicate that there is no snapshot
	// available.
	ErrNoSnapshot         = errors.New("no snapshot available")
	errSnapshotOutOfDate  = errors.New("snapshot being generated is out of date")
	snapshotDirNameRe     = regexp.MustCompile(`^snapshot-[0-9A-F]+$`)
	genSnapshotDirNameRe  = regexp.MustCompile(`^snapshot-[0-9A-F]+-[0-9A-F]+\.generating$`)
	recvSnapshotDirNameRe = regexp.MustCompile(`^snapshot-[0-9A-F]+-[0-9A-F]+\.receiving$`)
)

type snapshotter struct {
	rootDirFunc server.GetSnapshotDirFunc
	dir         string
	clusterID   uint64
	nodeID      uint64
	logdb       raftio.ILogDB
	stopc       chan struct{}
}

func newSnapshotter(clusterID uint64,
	nodeID uint64, rootDirFunc server.GetSnapshotDirFunc,
	ldb raftio.ILogDB, stopc chan struct{}) *snapshotter {
	return &snapshotter{
		rootDirFunc: rootDirFunc,
		dir:         rootDirFunc(clusterID, nodeID),
		logdb:       ldb,
		clusterID:   clusterID,
		nodeID:      nodeID,
		stopc:       stopc,
	}
}

func (s *snapshotter) StreamSnapshot(streamable rsm.IStreamable,
	meta *rsm.SnapshotMeta, sink pb.IChunkSink) error {
	writer := newChunkWriter(sink, meta)
	if err := streamable.StreamSnapshot(meta.Ctx, writer); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *snapshotter) Save(savable rsm.ISavable,
	meta *rsm.SnapshotMeta) (*pb.Snapshot, *server.SnapshotEnv, error) {
	env := s.getSnapshotEnv(meta.Index)
	if err := env.CreateTempDir(); err != nil {
		return nil, env, err
	}
	files := newFileCollection()
	fp := env.GetTempFilepath()
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		return nil, env, err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()
	session := meta.Session.Bytes()
	sz, err := savable.SaveSnapshot(meta.Ctx, writer, session, files)
	if err != nil {
		return nil, env, err
	}
	fs, err := files.prepareFiles(env.GetTempDir(), env.GetFinalDir())
	if err != nil {
		return nil, env, err
	}
	ss := &pb.Snapshot{
		Filepath:   env.GetFilepath(),
		FileSize:   sz,
		Membership: meta.Membership,
		Index:      meta.Index,
		Term:       meta.Term,
		Files:      fs,
	}
	return ss, env, nil
}

func (s *snapshotter) Load(index uint64,
	loadableSessions rsm.ILoadableSessions,
	loadableSM rsm.ILoadableSM,
	fp string, fs []sm.SnapshotFile) error {
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		return err
	}
	defer func() {
		err = reader.Close()
	}()
	header, err := reader.GetHeader()
	if err != nil {
		return err
	}
	reader.ValidateHeader(header)
	if err := loadableSessions.LoadSessions(reader); err != nil {
		return err
	}
	if err := loadableSM.RecoverFromSnapshot(index, reader, fs); err != nil {
		return err
	}
	reader.ValidatePayload(header)
	return nil
}

func (s *snapshotter) Commit(snapshot pb.Snapshot) error {
	env := s.getSnapshotEnv(snapshot.Index)
	if err := env.CreateFlagFile(&snapshot); err != nil {
		return err
	}
	if readyToReturnTestKnob(s.stopc, "final dir check") {
		return sm.ErrSnapshotStopped
	}
	if env.IsFinalDirExists() {
		return errSnapshotOutOfDate
	}
	if outOfDate, err := env.RenameTempDirToFinalDir(); err != nil {
		if outOfDate {
			return errSnapshotOutOfDate
		}
		return err
	}
	if readyToReturnTestKnob(s.stopc, "saving to logdb") {
		return sm.ErrSnapshotStopped
	}
	if err := s.saveToLogDB(snapshot); err != nil {
		return err
	}
	if readyToReturnTestKnob(s.stopc, "removing flag file") {
		return sm.ErrSnapshotStopped
	}
	return env.RemoveFlagFile()
}

func (s *snapshotter) GetFilePath(index uint64) string {
	env := s.getSnapshotEnv(index)
	return env.GetFilepath()
}

func (s *snapshotter) GetSnapshot(index uint64) (pb.Snapshot, error) {
	snaps, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID)
	if err != nil {
		return pb.Snapshot{}, err
	}
	for _, snap := range snaps {
		if snap.Index == index {
			return snap, nil
		}
	}
	return pb.Snapshot{}, ErrNoSnapshot
}

func (s *snapshotter) GetMostRecentSnapshot() (pb.Snapshot, error) {
	snaps, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID)
	if err != nil {
		return pb.Snapshot{}, err
	}
	if len(snaps) > 0 {
		return snaps[len(snaps)-1], nil
	}
	return pb.Snapshot{}, ErrNoSnapshot
}

func (s *snapshotter) IsNoSnapshotError(e error) bool {
	return e == ErrNoSnapshot
}

func (s *snapshotter) ShrinkSnapshots(shrinkTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID)
	if err != nil {
		return err
	}
	for _, ss := range snapshots {
		if ss.Index <= shrinkTo {
			env := s.getSnapshotEnv(ss.Index)
			fp := env.GetFilepath()
			shrinkedFp := env.GetShrinkedFilepath()
			if err := rsm.ShrinkSnapshot(fp, shrinkedFp); err != nil {
				return err
			}
			if err := rsm.ReplaceSnapshotFile(shrinkedFp, fp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotter) Compaction(clusterID uint64, nodeID uint64,
	removeUpTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID)
	if err != nil {
		return err
	}
	if len(snapshots) <= snapshotsToKeep {
		return nil
	}
	shortListed := snapshots[:len(snapshots)-snapshotsToKeep]
	for _, snap := range shortListed {
		if snap.Index < removeUpTo {
			if err := s.logdb.DeleteSnapshot(clusterID, nodeID, snap.Index); err != nil {
				return err
			}
			env := s.getSnapshotEnv(snap.Index)
			if err := env.RemoveFinalDir(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotter) ProcessOrphans() error {
	fiList, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, fi := range fiList {
		if !fi.IsDir() {
			continue
		}
		fdir := filepath.Join(s.dir, fi.Name())
		if s.isOrphanDir(fi.Name()) {
			plog.Infof("found a orphan snapshot dir %s, %s", fi.Name(), fdir)
			var ss pb.Snapshot
			if err := fileutil.GetFlagFileContent(fdir,
				fileutil.SnapshotFlagFilename, &ss); err != nil {
				return err
			}
			if pb.IsEmptySnapshot(ss) {
				plog.Panicf("empty snapshot found in %s", fdir)
			}
			deleteDir := false
			mrss, err := s.GetMostRecentSnapshot()
			if err != nil {
				if err == ErrNoSnapshot {
					plog.Infof("no snapshot in logdb, delete the folder")
					deleteDir = true
				} else {
					return err
				}
			} else {
				if mrss.Index != ss.Index {
					deleteDir = true
				}
			}
			env := s.getSnapshotEnv(ss.Index)
			if deleteDir {
				plog.Infof("going to delete orphan dir %s", fdir)
				if err := env.RemoveFinalDir(); err != nil {
					return err
				}
			} else {
				plog.Infof("will keep the dir with flag file removed, %s", fdir)
				if err := env.RemoveFlagFile(); err != nil {
					return err
				}
			}
		} else if s.isZombieDir(fi.Name()) {
			plog.Infof("going to delete a zombie dir %s", fdir)
			if err := os.RemoveAll(fdir); err != nil {
				return err
			}
			plog.Infof("going to sync the folder %s", s.dir)
			if err := fileutil.SyncDir(s.dir); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotter) removeFlagFile(index uint64) error {
	env := s.getSnapshotEnv(index)
	return env.RemoveFlagFile()
}

func (s *snapshotter) getSnapshotEnv(index uint64) *server.SnapshotEnv {
	return server.NewSnapshotEnv(s.rootDirFunc,
		s.clusterID, s.nodeID, index, s.nodeID, server.SnapshottingMode)
}

func (s *snapshotter) saveToLogDB(snapshot pb.Snapshot) error {
	rec := pb.Update{
		ClusterID: s.clusterID,
		NodeID:    s.nodeID,
		Snapshot:  snapshot,
	}
	return s.logdb.SaveSnapshots([]pb.Update{rec})
}

func (s *snapshotter) dirNameMatch(dir string) bool {
	return snapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isZombieDir(dir string) bool {
	return genSnapshotDirNameRe.Match([]byte(dir)) ||
		recvSnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isOrphanDir(dir string) bool {
	if !s.dirNameMatch(dir) {
		return false
	}
	fdir := filepath.Join(s.dir, dir)
	return fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename)
}

type files struct {
	files []*pb.SnapshotFile
	idmap map[uint64]struct{}
}

func newFileCollection() *files {
	return &files{
		files: make([]*pb.SnapshotFile, 0),
		idmap: make(map[uint64]struct{}),
	}
}

func (fc *files) AddFile(fileID uint64,
	path string, metadata []byte) {
	if _, ok := fc.idmap[fileID]; ok {
		plog.Panicf("trying to add file %d again", fileID)
	}
	f := &pb.SnapshotFile{
		Filepath: path,
		FileId:   fileID,
		Metadata: metadata,
	}
	fc.files = append(fc.files, f)
	fc.idmap[fileID] = struct{}{}
}

func (fc *files) Size() uint64 {
	return uint64(len(fc.files))
}

func (fc *files) GetFileAt(idx uint64) *pb.SnapshotFile {
	return fc.files[idx]
}

func (fc *files) prepareFiles(tmpdir string,
	finaldir string) ([]*pb.SnapshotFile, error) {
	for _, file := range fc.files {
		fn := file.Filename()
		fp := filepath.Join(tmpdir, fn)
		if err := os.Link(file.Filepath, fp); err != nil {
			return nil, err
		}
		fi, err := os.Stat(fp)
		if err != nil {
			return nil, err
		}
		if fi.IsDir() {
			plog.Panicf("%s is a dir", fp)
		}
		if fi.Size() == 0 {
			plog.Panicf("empty file found, id %d",
				file.FileId)
		}
		file.Filepath = filepath.Join(finaldir, fn)
		file.FileSize = uint64(fi.Size())
	}
	return fc.files, nil
}
