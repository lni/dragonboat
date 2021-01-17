// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"math"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/utils/dio"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	snapshotsToKeep = 3
)

func compressionType(ct pb.CompressionType) dio.CompressionType {
	if ct == pb.NoCompression {
		return dio.NoCompression
	} else if ct == pb.Snappy {
		return dio.Snappy
	} else {
		plog.Panicf("unknown compression type: %d", ct)
	}
	panic("will never reach here")
}

var (
	// ErrNoSnapshot is the error used to indicate that there is no snapshot
	// available.
	ErrNoSnapshot        = errors.New("no snapshot available")
	errSnapshotOutOfDate = errors.New("snapshot being generated is out of date")
)

type snapshotter struct {
	root      server.SnapshotDirFunc
	dir       string
	clusterID uint64
	nodeID    uint64
	logdb     raftio.ILogDB
	fs        vfs.IFS
}

var _ rsm.ISnapshotter = (*snapshotter)(nil)

func newSnapshotter(clusterID uint64, nodeID uint64,
	root server.SnapshotDirFunc, ldb raftio.ILogDB, fs vfs.IFS) *snapshotter {
	return &snapshotter{
		root:      root,
		dir:       root(clusterID, nodeID),
		logdb:     ldb,
		clusterID: clusterID,
		nodeID:    nodeID,
		fs:        fs,
	}
}

func (s *snapshotter) id() string {
	return dn(s.clusterID, s.nodeID)
}

func (s *snapshotter) ssid(index uint64) string {
	return logutil.DescribeSS(s.clusterID, s.nodeID, index)
}

func (s *snapshotter) Shrunk(ss pb.Snapshot) (bool, error) {
	return rsm.IsShrunkSnapshotFile(s.getFilePath(ss.Index), s.fs)
}

func (s *snapshotter) Stream(streamable rsm.IStreamable,
	meta rsm.SSMeta, sink pb.IChunkSink) error {
	ct := compressionType(meta.CompressionType)
	cw := dio.NewCompressor(ct, rsm.NewChunkWriter(sink, meta))
	if err := streamable.Stream(meta.Ctx, cw); err != nil {
		sink.Stop()
		return err
	}
	return cw.Close()
}

func (s *snapshotter) Save(savable rsm.ISavable,
	meta rsm.SSMeta) (ss pb.Snapshot, env server.SSEnv, err error) {
	env = s.getCustomEnv(meta)
	if err := env.CreateTempDir(); err != nil {
		return pb.Snapshot{}, env, err
	}
	files := rsm.NewFileCollection()
	fp := env.GetTempFilepath()
	ct := compressionType(meta.CompressionType)
	w, err := rsm.NewSnapshotWriter(fp, meta.CompressionType, s.fs)
	if err != nil {
		return pb.Snapshot{}, env, err
	}
	cw := dio.NewCountedWriter(w)
	sw := dio.NewCompressor(ct, cw)
	defer func() {
		if cerr := sw.Close(); err == nil {
			err = cerr
		}
		if ss.Index > 0 {
			total := cw.BytesWritten()
			ss.Checksum = w.GetPayloadChecksum()
			ss.FileSize = w.GetPayloadSize(total) + rsm.HeaderSize
		}
	}()
	session := meta.Session.Bytes()
	dummy, err := savable.Save(meta, sw, session, files)
	if err != nil {
		return pb.Snapshot{}, env, err
	}
	fs, err := files.PrepareFiles(env.GetTempDir(), env.GetFinalDir())
	if err != nil {
		return pb.Snapshot{}, env, err
	}
	return pb.Snapshot{
		ClusterId:   s.clusterID,
		Filepath:    env.GetFilepath(),
		Membership:  meta.Membership,
		Index:       meta.Index,
		Term:        meta.Term,
		OnDiskIndex: meta.OnDiskIndex,
		Files:       fs,
		Dummy:       dummy,
		Type:        meta.Type,
	}, env, nil
}

func (s *snapshotter) Load(ss pb.Snapshot,
	sessions rsm.ILoadable, asm rsm.IRecoverable) (err error) {
	fp := s.getFilePath(ss.Index)
	fs := make([]sm.SnapshotFile, 0)
	for _, f := range ss.Files {
		fs = append(fs, sm.SnapshotFile{
			FileID:   f.FileId,
			Filepath: f.Filepath,
			Metadata: f.Metadata,
		})
	}
	reader, err := rsm.NewSnapshotReader(fp, s.fs)
	if err != nil {
		return err
	}
	header, err := reader.GetHeader()
	if err != nil {
		reader.Close()
		return err
	}
	ct := compressionType(header.CompressionType)
	cr := dio.NewDecompressor(ct, reader)
	defer func() {
		if cerr := cr.Close(); err == nil {
			err = cerr
		}
	}()
	v := rsm.SSVersion(header.Version)
	if err := sessions.LoadSessions(cr, v); err != nil {
		return err
	}
	if err := asm.Recover(cr, fs); err != nil {
		return err
	}
	reader.ValidatePayload(header)
	return nil
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
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, math.MaxUint64)
	if err != nil {
		return pb.Snapshot{}, err
	}
	if len(snapshots) > 0 {
		return snapshots[len(snapshots)-1], nil
	}
	return pb.Snapshot{}, ErrNoSnapshot
}

func (s *snapshotter) IsNoSnapshotError(e error) bool {
	return e == ErrNoSnapshot
}

func (s *snapshotter) commit(ss pb.Snapshot, req rsm.SSRequest) error {
	env := s.getCustomEnv(rsm.SSMeta{
		Index:   ss.Index,
		Request: req,
	})
	if err := env.SaveSSMetadata(&ss); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		if err == server.ErrSnapshotOutOfDate {
			return errSnapshotOutOfDate
		}
		return err
	}
	if !req.Exported() {
		if err := s.saveSnapshot(ss); err != nil {
			return err
		}
	}
	return env.RemoveFlagFile()
}

func (s *snapshotter) getFilePath(index uint64) string {
	env := s.getEnv(index)
	return env.GetFilepath()
}

func (s *snapshotter) shrink(shrinkTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, shrinkTo)
	if err != nil {
		return err
	}
	plog.Debugf("%s has %d snapshots to shrink", s.id(), len(snapshots))
	for idx, ss := range snapshots {
		if ss.Index > shrinkTo {
			plog.Panicf("snapshot index %d, shrink to %d", ss.Index, shrinkTo)
		}
		if !ss.Dummy && !ss.Witness {
			env := s.getEnv(ss.Index)
			fp := env.GetFilepath()
			shrunk := env.GetShrinkedFilepath()
			plog.Debugf("%s shrinking %s, %d", s.id(), s.ssid(ss.Index), idx)
			if err := rsm.ShrinkSnapshot(fp, shrunk, s.fs); err != nil {
				return err
			}
			if err := rsm.ReplaceSnapshot(shrunk, fp, s.fs); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotter) compact(removeUpTo uint64) error {
	snapshots, err := s.logdb.ListSnapshots(s.clusterID, s.nodeID, removeUpTo)
	if err != nil {
		return err
	}
	if len(snapshots) <= snapshotsToKeep {
		return nil
	}
	selected := snapshots[:len(snapshots)-snapshotsToKeep]
	plog.Debugf("%s has %d snapshots to compact", s.id(), len(selected))
	for _, ss := range selected {
		plog.Debugf("%s compacting %s", s.id(), s.ssid(ss.Index))
		if err := s.remove(ss.Index); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotter) processOrphans() error {
	files, err := s.fs.List(s.dir)
	if err != nil {
		return err
	}
	noss := false
	mrss, err := s.GetMostRecentSnapshot()
	if err != nil {
		if err == ErrNoSnapshot {
			noss = true
		} else {
			return err
		}
	}
	for _, n := range files {
		fi, err := s.fs.Stat(s.fs.PathJoin(s.dir, n))
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			continue
		}
		fdir := s.fs.PathJoin(s.dir, fi.Name())
		if s.isOrphan(fi.Name()) {
			var ss pb.Snapshot
			if err := fileutil.GetFlagFileContent(fdir,
				fileutil.SnapshotFlagFilename, &ss, s.fs); err != nil {
				return err
			}
			if pb.IsEmptySnapshot(ss) {
				plog.Panicf("empty snapshot found in %s", fdir)
			}
			remove := false
			if noss {
				remove = true
			} else {
				if mrss.Index != ss.Index {
					remove = true
				}
			}
			if remove {
				if err := s.remove(ss.Index); err != nil {
					return err
				}
			} else {
				env := s.getEnv(ss.Index)
				if err := env.RemoveFlagFile(); err != nil {
					return err
				}
			}
		} else if s.isZombie(fi.Name()) {
			if err := s.fs.RemoveAll(fdir); err != nil {
				return err
			}
			if err := fileutil.SyncDir(s.dir, s.fs); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *snapshotter) remove(index uint64) error {
	if err := s.logdb.DeleteSnapshot(s.clusterID, s.nodeID, index); err != nil {
		return err
	}
	env := s.getEnv(index)
	return env.RemoveFinalDir()
}

func (s *snapshotter) removeFlagFile(index uint64) error {
	env := s.getEnv(index)
	return env.RemoveFlagFile()
}

func (s *snapshotter) getEnv(index uint64) server.SSEnv {
	return server.NewSSEnv(s.root,
		s.clusterID, s.nodeID, index, s.nodeID, server.SnapshotMode, s.fs)
}

func (s *snapshotter) getCustomEnv(meta rsm.SSMeta) server.SSEnv {
	if meta.Request.Exported() {
		if len(meta.Request.Path) == 0 {
			plog.Panicf("Path is empty when exporting snapshot")
		}
		gp := func(clusterID uint64, nodeID uint64) string {
			return meta.Request.Path
		}
		return server.NewSSEnv(gp,
			s.clusterID, s.nodeID, meta.Index, s.nodeID, server.SnapshotMode, s.fs)
	}
	return s.getEnv(meta.Index)
}

func (s *snapshotter) saveSnapshot(snapshot pb.Snapshot) error {
	return s.logdb.SaveSnapshots([]pb.Update{{
		ClusterID: s.clusterID,
		NodeID:    s.nodeID,
		Snapshot:  snapshot,
	}})
}

func (s *snapshotter) dirMatch(dir string) bool {
	return server.SnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isZombie(dir string) bool {
	return server.GenSnapshotDirNameRe.Match([]byte(dir)) ||
		server.RecvSnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) isOrphan(dir string) bool {
	if !s.dirMatch(dir) {
		return false
	}
	fdir := s.fs.PathJoin(s.dir, dir)
	return fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename, s.fs)
}
