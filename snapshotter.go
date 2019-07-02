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

package dragonboat

import (
	"errors"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	snapshotsToKeep = 3
)

var (
	// ErrNoSnapshot is the error used to indicate that there is no snapshot
	// available.
	ErrNoSnapshot        = errors.New("no snapshot available")
	errSnapshotOutOfDate = errors.New("snapshot being generated is out of date")
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

func (s *snapshotter) id() string {
	return logutil.DescribeNode(s.clusterID, s.nodeID)
}

func (s *snapshotter) Stream(streamable rsm.IStreamable,
	meta *rsm.SnapshotMeta, sink pb.IChunkSink) error {
	writer := rsm.NewChunkWriter(sink, meta)
	if err := streamable.StreamSnapshot(meta.Ctx, writer); err != nil {
		sink.Stop()
		return err
	}
	return writer.Close()
}

func (s *snapshotter) Save(savable rsm.ISavable,
	meta *rsm.SnapshotMeta) (ss *pb.Snapshot, env *server.SnapshotEnv, err error) {
	env = s.getCustomSnapshotEnv(meta)
	if err := env.CreateTempDir(); err != nil {
		return nil, env, err
	}
	files := rsm.NewFileCollection()
	fp := env.GetTempFilepath()
	writer, err := rsm.NewSnapshotWriter(fp, rsm.CurrentSnapshotVersion)
	if err != nil {
		return nil, env, err
	}
	defer func() {
		if cerr := writer.Close(); err == nil {
			err = cerr
		}
		if ss != nil {
			ss.Checksum = writer.GetPayloadChecksum()
		}
	}()
	session := meta.Session.Bytes()
	dummy, sz, err := savable.SaveSnapshot(meta, writer, session, files)
	if err != nil {
		return nil, env, err
	}
	fs, err := files.PrepareFiles(env.GetTempDir(), env.GetFinalDir())
	if err != nil {
		return nil, env, err
	}
	ss = &pb.Snapshot{
		ClusterId:   s.clusterID,
		Filepath:    env.GetFilepath(),
		FileSize:    sz,
		Membership:  meta.Membership,
		Index:       meta.Index,
		Term:        meta.Term,
		OnDiskIndex: meta.OnDiskIndex,
		Files:       fs,
		Dummy:       dummy,
		Type:        meta.Type,
	}
	return ss, env, nil
}

func (s *snapshotter) Load(sessions rsm.ILoadableSessions,
	asm rsm.ILoadableSM, fp string, fs []sm.SnapshotFile) (err error) {
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := reader.Close(); err == nil {
			err = cerr
		}
	}()
	header, err := reader.GetHeader()
	if err != nil {
		return err
	}
	reader.ValidateHeader(header)
	v := (rsm.SnapshotVersion)(header.Version)
	if err := sessions.LoadSessions(reader, v); err != nil {
		return err
	}
	if err := asm.RecoverFromSnapshot(reader, fs); err != nil {
		return err
	}
	reader.ValidatePayload(header)
	return nil
}

func (s *snapshotter) Commit(snapshot pb.Snapshot,
	req rsm.SnapshotRequest) error {
	meta := &rsm.SnapshotMeta{
		Index:   snapshot.Index,
		Request: req,
	}
	env := s.getCustomSnapshotEnv(meta)
	if err := env.SaveSnapshotMetadata(&snapshot); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&snapshot); err != nil {
		if err == server.ErrSnapshotOutOfDate {
			return errSnapshotOutOfDate
		}
		return err
	}
	if !req.IsExportedSnapshot() {
		if err := s.saveToLogDB(snapshot); err != nil {
			return err
		}
	}
	return env.RemoveFlagFile()
}

func (s *snapshotter) GetFilePath(index uint64) string {
	env := s.getSnapshotEnv(index)
	return env.GetFilepath()
}

func (s *snapshotter) GetSnapshot(index uint64) (pb.Snapshot, error) {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, index)
	if err != nil {
		return pb.Snapshot{}, err
	}
	for _, ss := range snapshots {
		if ss.Index == index {
			return ss, nil
		}
	}
	return pb.Snapshot{}, ErrNoSnapshot
}

func (s *snapshotter) GetMostRecentSnapshot() (pb.Snapshot, error) {
	snaps, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, math.MaxUint64)
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

func (s *snapshotter) Shrink(shrinkTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, shrinkTo)
	if err != nil {
		return err
	}
	plog.Infof("%s has %d snapshots to shrink", s.id(), len(snapshots))
	for idx, ss := range snapshots {
		if ss.Index > shrinkTo {
			plog.Panicf("unexpected snapshot found %v, shrink to %d", ss, shrinkTo)
		}
		if !ss.Dummy {
			env := s.getSnapshotEnv(ss.Index)
			fp := env.GetFilepath()
			shrinkedFp := env.GetShrinkedFilepath()
			plog.Infof("%s shrinking snapshot %d, %d", s.id(), ss.Index, idx)
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

func (s *snapshotter) Compact(removeUpTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, removeUpTo)
	if err != nil {
		return err
	}
	if len(snapshots) <= snapshotsToKeep {
		return nil
	}
	selected := snapshots[:len(snapshots)-snapshotsToKeep]
	plog.Infof("%s has %d snapshots to compact", s.id(), len(selected))
	for idx, ss := range selected {
		plog.Infof("%s compacting snapshot %d, %d", s.id(), ss.Index, idx)
		if err := s.logdb.DeleteSnapshot(s.clusterID,
			s.nodeID, ss.Index); err != nil {
			return err
		}
		env := s.getSnapshotEnv(ss.Index)
		if err := env.RemoveFinalDir(); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotter) ProcessOrphans() error {
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
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
			plog.Infof("most recent snapshot: %d, ss index %d", mrss.Index, ss.Index)
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

func (s *snapshotter) getCustomSnapshotEnv(meta *rsm.SnapshotMeta) *server.SnapshotEnv {
	if meta.Request.IsExportedSnapshot() {
		if len(meta.Request.Path) == 0 {
			plog.Panicf("Path is empty when exporting snapshot")
		}
		getPath := func(clusterID uint64, nodeID uint64) string {
			return meta.Request.Path
		}
		return server.NewSnapshotEnv(getPath,
			s.clusterID, s.nodeID, meta.Index, s.nodeID, server.SnapshottingMode)
	}
	return s.getSnapshotEnv(meta.Index)
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
	return server.SnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isZombieDir(dir string) bool {
	return server.GenSnapshotDirNameRe.Match([]byte(dir)) ||
		server.RecvSnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isOrphanDir(dir string) bool {
	if !s.dirNameMatch(dir) {
		return false
	}
	fdir := filepath.Join(s.dir, dir)
	return fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename)
}
