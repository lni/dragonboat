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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/utils/dio"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
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
	shardID   uint64
	replicaID uint64
	logdb     raftio.ILogDB
	logReader *logdb.LogReader
	fs        vfs.IFS
}

var _ rsm.ISnapshotter = (*snapshotter)(nil)

func newSnapshotter(shardID uint64, replicaID uint64,
	root server.SnapshotDirFunc, ldb raftio.ILogDB,
	logReader *logdb.LogReader, fs vfs.IFS) *snapshotter {
	return &snapshotter{
		shardID:   shardID,
		replicaID: replicaID,
		root:      root,
		dir:       root(shardID, replicaID),
		logdb:     ldb,
		logReader: logReader,
		fs:        fs,
	}
}

func (s *snapshotter) id() string {
	return dn(s.shardID, s.replicaID)
}

func (s *snapshotter) ssid(index uint64) string {
	return logutil.DescribeSS(s.shardID, s.replicaID, index)
}

func (s *snapshotter) Shrunk(ss pb.Snapshot) (bool, error) {
	return rsm.IsShrunkSnapshotFile(s.getFilePath(ss.Index), s.fs)
}

func (s *snapshotter) Stream(streamable rsm.IStreamable,
	meta rsm.SSMeta, sink pb.IChunkSink) error {
	ct := compressionType(meta.CompressionType)
	cw := dio.NewCompressor(ct, rsm.NewChunkWriter(sink, meta))
	if err := streamable.Stream(meta.Ctx, cw); err != nil {
		if cerr := sink.Close(); cerr != nil {
			plog.Errorf("failed to close the sink %v", cerr)
		}
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
		err = firstError(err, sw.Close())
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
		ShardID:     s.shardID,
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
	reader, header, err := rsm.NewSnapshotReader(fp, s.fs)
	if err != nil {
		return err
	}
	ct := compressionType(header.CompressionType)
	cr := dio.NewDecompressor(ct, reader)
	defer func() {
		err = firstError(err, cr.Close())
	}()
	v := rsm.SSVersion(header.Version)
	if err := sessions.LoadSessions(cr, v); err != nil {
		return err
	}
	if err := asm.Recover(cr, fs); err != nil {
		return err
	}
	return nil
}

func (s *snapshotter) GetSnapshot() (pb.Snapshot, error) {
	ss := s.logReader.Snapshot()
	if pb.IsEmptySnapshot(ss) {
		return pb.Snapshot{}, ErrNoSnapshot
	}
	return ss, nil
}

// TODO: update this once the LogDB interface is updated to have the ability to
// query latest snapshot.
func (s *snapshotter) GetSnapshotFromLogDB() (pb.Snapshot, error) {
	snapshot, err := s.logdb.GetSnapshot(s.shardID, s.replicaID)
	if err != nil {
		return pb.Snapshot{}, err
	}
	if !pb.IsEmptySnapshot(snapshot) {
		return snapshot, nil
	}
	return pb.Snapshot{}, ErrNoSnapshot
}

func (s *snapshotter) Shrink(index uint64) error {
	ss, err := s.logdb.GetSnapshot(s.shardID, s.replicaID)
	if err != nil {
		return err
	}
	if ss.Index < index {
		return nil
	}
	if !ss.Dummy && !ss.Witness {
		env := s.getEnv(index)
		fp := env.GetFilepath()
		shrunk := env.GetShrinkedFilepath()
		plog.Infof("%s shrinking %s", s.id(), s.ssid(index))
		if err := rsm.ShrinkSnapshot(fp, shrunk, s.fs); err != nil {
			return err
		}
		return rsm.ReplaceSnapshot(shrunk, fp, s.fs)
	}
	return nil
}

func (s *snapshotter) Compact(index uint64) error {
	ss, err := s.logdb.GetSnapshot(s.shardID, s.replicaID)
	if err != nil {
		return err
	}
	if ss.Index <= index {
		plog.Panicf("%s invalid compaction, LogDB snapshot %d, index %d",
			s.id(), ss.Index, index)
	}
	plog.Debugf("%s called Compact, latest %d, to compact %d",
		s.id(), ss.Index, index)
	if err := s.remove(index); err != nil {
		return err
	}
	return nil
}

func (s *snapshotter) IsNoSnapshotError(err error) bool {
	return errors.Is(err, ErrNoSnapshot)
}

func (s *snapshotter) Commit(ss pb.Snapshot, req rsm.SSRequest) error {
	env := s.getCustomEnv(rsm.SSMeta{
		Index:   ss.Index,
		Request: req,
	})
	if err := env.SaveSSMetadata(&ss); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		if errors.Is(err, server.ErrSnapshotOutOfDate) {
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

func (s *snapshotter) processOrphans() error {
	files, err := s.fs.List(s.dir)
	if err != nil {
		return err
	}
	noss := false
	mrss, err := s.GetSnapshotFromLogDB()
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			noss = true
		} else {
			return err
		}
	}
	removeFolder := func(fdir string) error {
		if err := s.fs.RemoveAll(fdir); err != nil {
			return err
		}
		return fileutil.SyncDir(s.dir, s.fs)
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
			if err := removeFolder(fdir); err != nil {
				return err
			}
		} else if s.isSnapshot(fi.Name()) {
			index := s.parseIndex(fi.Name())
			if noss || index != mrss.Index {
				if err := removeFolder(fdir); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *snapshotter) remove(index uint64) error {
	env := s.getEnv(index)
	return env.RemoveFinalDir()
}

func (s *snapshotter) removeFlagFile(index uint64) error {
	env := s.getEnv(index)
	return env.RemoveFlagFile()
}

func (s *snapshotter) getEnv(index uint64) server.SSEnv {
	return server.NewSSEnv(s.root,
		s.shardID, s.replicaID, index, s.replicaID, server.SnapshotMode, s.fs)
}

func (s *snapshotter) getCustomEnv(meta rsm.SSMeta) server.SSEnv {
	if meta.Request.Exported() {
		if len(meta.Request.Path) == 0 {
			plog.Panicf("Path is empty when exporting snapshot")
		}
		gp := func(shardID uint64, replicaID uint64) string {
			return meta.Request.Path
		}
		return server.NewSSEnv(gp,
			s.shardID, s.replicaID, meta.Index, s.replicaID, server.SnapshotMode, s.fs)
	}
	return s.getEnv(meta.Index)
}

func (s *snapshotter) saveSnapshot(snapshot pb.Snapshot) error {
	return s.logdb.SaveSnapshots([]pb.Update{{
		ShardID:   s.shardID,
		ReplicaID: s.replicaID,
		Snapshot:  snapshot,
	}})
}

func (s *snapshotter) dirMatch(dir string) bool {
	return server.SnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) parseIndex(dir string) uint64 {
	if parts := server.SnapshotDirNamePartsRe.FindStringSubmatch(dir); len(parts) == 2 {
		index, err := strconv.ParseUint(parts[1], 16, 64)
		if err != nil {
			plog.Panicf("failed to parse index %s", parts[1])
		}
		return index
	}
	plog.Panicf("unknown snapshot fold name: %s", dir)
	return 0
}

func (s *snapshotter) isSnapshot(dir string) bool {
	if !s.dirMatch(dir) {
		return false
	}
	fdir := s.fs.PathJoin(s.dir, dir)
	return !fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename, s.fs)
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
