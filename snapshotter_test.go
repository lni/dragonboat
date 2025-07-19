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
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/logdb"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

const (
	tmpSnapshotDirSuffix = "generating"
	recvTmpDirSuffix     = "receiving"
	rdbTestDirectory     = "rdb_test_dir_safe_to_delete"
)

func getNewTestDB(dir string, lldir string, fs vfs.IFS) raftio.ILogDB {
	d := fs.PathJoin(rdbTestDirectory, dir)
	lld := fs.PathJoin(rdbTestDirectory, lldir)
	if err := fs.MkdirAll(d, 0777); err != nil {
		panic(err)
	}
	if err := fs.MkdirAll(lld, 0777); err != nil {
		panic(err)
	}
	cfg := config.NodeHostConfig{
		Expert: config.GetDefaultExpertConfig(),
	}
	cfg.Expert.FS = fs
	db, err := logdb.NewDefaultLogDB(cfg, nil, []string{d}, []string{lld})
	if err != nil {
		panic(err.Error())
	}
	return db
}

func deleteTestRDB(fs vfs.IFS) {
	if err := fs.RemoveAll(rdbTestDirectory); err != nil {
		panic(err)
	}
}

func getTestSnapshotter(ldb raftio.ILogDB, fs vfs.IFS) *snapshotter {
	fp := fs.PathJoin(rdbTestDirectory, "snapshot")
	if err := fs.MkdirAll(fp, 0777); err != nil {
		panic(err)
	}
	f := func(cid uint64, nid uint64) string {
		return fp
	}
	lr := logdb.NewLogReader(1, 1, ldb)
	return newSnapshotter(1, 1, f, ldb, lr, fs)
}

func runSnapshotterTest(t *testing.T,
	fn func(t *testing.T, logdb raftio.ILogDB, snapshotter *snapshotter),
	fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	deleteTestRDB(fs)
	ldb := getNewTestDB(dir, lldir, fs)
	s := getTestSnapshotter(ldb, fs)
	defer deleteTestRDB(fs)
	defer func() {
		require.NoError(t, ldb.Close())
	}()
	fn(t, ldb, s)
}

func TestFinalizeSnapshotReturnExpectedErrorWhenOutOfDate(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getEnv(ss.Index)
		finalSnapDir := env.GetFinalDir()
		require.NoError(t, fs.MkdirAll(finalSnapDir, 0755))
		require.NoError(t, env.CreateTempDir())
		err := s.Commit(ss, rsm.SSRequest{})
		require.ErrorIs(t, err, errSnapshotOutOfDate)
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotCanBeFinalized(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getEnv(ss.Index)
		finalSnapDir := env.GetFinalDir()
		tmpDir := env.GetTempDir()
		require.NoError(t, env.CreateTempDir())
		_, err := fs.Stat(tmpDir)
		require.NoError(t, err)
		testfp := fs.PathJoin(tmpDir, "test.data")
		f, err := fs.Create(testfp)
		require.NoError(t, err)
		_, err = f.Write(make([]byte, 12))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		require.NoError(t, s.Commit(ss, rsm.SSRequest{}))
		snapshot, err := ldb.GetSnapshot(1, 1)
		require.NoError(t, err)
		require.False(t, pb.IsEmptySnapshot(snapshot))
		rs, err := s.GetSnapshotFromLogDB()
		require.NoError(t, err)
		require.Equal(t, uint64(100), rs.Index)
		_, err = fs.Stat(tmpDir)
		require.True(t, vfs.IsNotExist(err))
		fi, err := fs.Stat(finalSnapDir)
		require.NoError(t, err)
		require.True(t, fi.IsDir())
		require.False(t, fileutil.HasFlagFile(finalSnapDir,
			fileutil.SnapshotFlagFilename, fs))
		vfp := fs.PathJoin(finalSnapDir, "test.data")
		fi, err = fs.Stat(vfp)
		require.NoError(t, err)
		require.False(t, fi.IsDir())
		require.Equal(t, int64(12), fi.Size())
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotCanBeSavedToLogDB(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    1,
			Term:     2,
		}
		require.NoError(t, s.saveSnapshot(s1))
		snapshot, err := ldb.GetSnapshot(1, 1)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual(s1, snapshot))
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}

func TestZombieSnapshotDirsCanBeRemoved(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		env1 := s.getEnv(100)
		env2 := s.getEnv(200)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd1 = fd1 + "-100." + tmpSnapshotDirSuffix
		fd2 = fd2 + "-100." + recvTmpDirSuffix
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fs.MkdirAll(fd2, 0755))
		require.NoError(t, s.processOrphans())
		_, err := fs.Stat(fd1)
		require.True(t, vfs.IsNotExist(err))
		_, err = fs.Stat(fd2)
		require.True(t, vfs.IsNotExist(err))
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotsNotInLogDBAreRemoved(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		env1 := s.getEnv(100)
		env2 := s.getEnv(200)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fs.MkdirAll(fd2, 0755))
		require.NoError(t, s.processOrphans())
		_, err := fs.Stat(fd1)
		require.True(t, vfs.IsNotExist(err))
		_, err = fs.Stat(fd2)
		require.True(t, vfs.IsNotExist(err))
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOnlyMostRecentSnapshotIsKept(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		env1 := s.getEnv(100)
		env2 := s.getEnv(200)
		env3 := s.getEnv(300)
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    200,
			Term:     200,
		}
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd3 := env3.GetFinalDir()
		require.NoError(t, s.saveSnapshot(s1))
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fs.MkdirAll(fd2, 0755))
		require.NoError(t, fs.MkdirAll(fd3, 0755))
		require.NoError(t, s.processOrphans())
		_, err := fs.Stat(fd1)
		require.True(t, vfs.IsNotExist(err))
		_, err = fs.Stat(fd2)
		require.False(t, vfs.IsNotExist(err))
		_, err = fs.Stat(fd3)
		require.True(t, vfs.IsNotExist(err))
	}
	runSnapshotterTest(t, fn, fs)
}

func TestFirstSnapshotBecomeOrphanedIsHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getEnv(100)
		fd1 := env.GetFinalDir()
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fileutil.CreateFlagFile(fd1,
			fileutil.SnapshotFlagFilename, &s1, fs))
		require.NoError(t, s.processOrphans())
		_, err := fs.Stat(fd1)
		require.True(t, vfs.IsNotExist(err))
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOrphanedSnapshotRecordIsRemoved(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		s2 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    200,
			Term:     200,
		}
		env1 := s.getEnv(s1.Index)
		env2 := s.getEnv(s2.Index)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fs.MkdirAll(fd2, 0755))
		require.NoError(t, fileutil.CreateFlagFile(fd1,
			fileutil.SnapshotFlagFilename, &s1, fs))
		require.NoError(t, fileutil.CreateFlagFile(fd2,
			fileutil.SnapshotFlagFilename, &s2, fs))
		require.NoError(t, s.saveSnapshot(s1))
		require.NoError(t, s.saveSnapshot(s2))
		// two orphane snapshots, kept the most recent one, and remove the older
		// one including its logdb record.
		require.NoError(t, s.processOrphans())
		_, err := fs.Stat(fd1)
		require.False(t, vfs.IsExist(err))
		_, err = fs.Stat(fd2)
		require.False(t, vfs.IsNotExist(err))
		require.False(t, fileutil.HasFlagFile(fd2,
			fileutil.SnapshotFlagFilename, fs))
		snapshot, err := s.logdb.GetSnapshot(1, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(200), snapshot.Index)
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOrphanedSnapshotsCanBeProcessed(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		s2 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    200,
			Term:     200,
		}
		s3 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    300,
			Term:     200,
		}
		env1 := s.getEnv(s1.Index)
		env2 := s.getEnv(s2.Index)
		env3 := s.getEnv(s3.Index)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd3 := env3.GetFinalDir()
		fd4 := fmt.Sprintf("%s%s", fd3, "xx")
		require.NoError(t, fs.MkdirAll(fd1, 0755))
		require.NoError(t, fs.MkdirAll(fd2, 0755))
		require.NoError(t, fs.MkdirAll(fd4, 0755))
		require.NoError(t, fileutil.CreateFlagFile(fd1,
			fileutil.SnapshotFlagFilename, &s1, fs))
		require.NoError(t, fileutil.CreateFlagFile(fd2,
			fileutil.SnapshotFlagFilename, &s2, fs))
		require.NoError(t, fileutil.CreateFlagFile(fd4,
			fileutil.SnapshotFlagFilename, &s3, fs))
		require.NoError(t, s.saveSnapshot(s1))
		// fd1 has record in logdb. flag file expected to be removed while the fd1
		// foler is expected to be kept
		// fd2 doesn't has its record in logdb, while the most recent snapshot
		// record in logdb is not for fd2, fd2 will be entirely removed
		require.NoError(t, s.processOrphans())
		require.False(t, fileutil.HasFlagFile(fd1,
			fileutil.SnapshotFlagFilename, fs))
		require.False(t, fileutil.HasFlagFile(fd2,
			fileutil.SnapshotFlagFilename, fs))
		require.True(t, fileutil.HasFlagFile(fd4,
			fileutil.SnapshotFlagFilename, fs))
		_, err := fs.Stat(fd1)
		require.False(t, vfs.IsNotExist(err))
		_, err = fs.Stat(fd2)
		require.True(t, vfs.IsNotExist(err))
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotterCompact(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, snapshotter *snapshotter) {
		for i := uint64(1); i <= uint64(3); i++ {
			fn := fmt.Sprintf("f%d.data", i)
			s := pb.Snapshot{
				FileSize: 1234,
				Filepath: fn,
				Index:    i,
				Term:     2,
			}
			env := snapshotter.getEnv(s.Index)
			require.NoError(t, env.CreateTempDir())
			require.NoError(t, snapshotter.Commit(s, rsm.SSRequest{}))
			fp := snapshotter.getFilePath(s.Index)
			f, err := fs.Create(fp)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
		require.NoError(t, snapshotter.Compact(2))
		check := func(index uint64, exist bool) {
			env := snapshotter.getEnv(index)
			snapDir := env.GetFinalDir()
			_, err := fs.Stat(snapDir)
			if exist {
				require.False(t, vfs.IsNotExist(err))
			} else {
				require.True(t, vfs.IsNotExist(err))
			}
		}
		check(1, true)
		check(2, false)
		check(3, true)
	}
	runSnapshotterTest(t, fn, fs)
}

func TestShrinkSnapshots(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb raftio.ILogDB, snapshotter *snapshotter) {
		for i := uint64(1); i <= 3; i++ {
			index := i * 10
			env := snapshotter.getEnv(index)
			fp := env.GetFilepath()
			s := pb.Snapshot{
				Index:    index,
				Term:     2,
				FileSize: 1234,
				Filepath: fp,
			}
			require.NoError(t, env.CreateTempDir())
			require.NoError(t, snapshotter.Commit(s, rsm.SSRequest{}))
			fp = snapshotter.getFilePath(s.Index)
			writer, err := rsm.NewSnapshotWriter(fp, pb.NoCompression, fs)
			require.NoError(t, err)
			sz := make([]byte, 8)
			binary.LittleEndian.PutUint64(sz, 0)
			_, err = writer.Write(sz)
			require.NoError(t, err)
			for j := 0; j < 10; j++ {
				data := make([]byte, 1024*1024)
				_, err = writer.Write(data)
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close())
		}
		require.NoError(t, snapshotter.Shrink(20))
		env1 := snapshotter.getEnv(10)
		env2 := snapshotter.getEnv(20)
		env3 := snapshotter.getEnv(30)
		cf := func(p string, esz uint64) {
			fi, err := fs.Stat(p)
			require.NoError(t, err)
			require.Equal(t, esz, uint64(fi.Size()))
		}
		cf(env1.GetFilepath(), 10486832)
		cf(env2.GetFilepath(), 1060)
		cf(env3.GetFilepath(), 10486832)
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotDirNameMatchWorks(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		tests := []struct {
			dirName string
			valid   bool
		}{
			{"snapshot-AB", true},
			{"snapshot", false},
			{"xxxsnapshot-AB", false},
			{"snapshot-ABd", false},
			{"snapshot-", false},
		}
		for idx, tt := range tests {
			v := s.dirMatch(tt.dirName)
			require.Equal(t, tt.valid, v,
				fmt.Sprintf("dir name %s (%d) failed to match", tt.dirName, idx))
		}
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}

func TestZombieSnapshotDirNameMatchWorks(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		tests := []struct {
			dirName string
			valid   bool
		}{
			{"snapshot-AB", false},
			{"snapshot", false},
			{"xxxsnapshot-AB", false},
			{"snapshot-", false},
			{"snapshot-AB-01.receiving", true},
			{"snapshot-AB-1G.receiving", false},
			{"snapshot-AB.receiving", false},
			{"snapshot-XX.receiving", false},
			{"snapshot-AB.receivingd", false},
			{"dsnapshot-AB.receiving", false},
			{"snapshot-AB.generating", false},
			{"snapshot-AB-01.generating", true},
			{"snapshot-AB-0G.generating", false},
			{"snapshot-XX.generating", false},
			{"snapshot-AB.generatingd", false},
			{"dsnapshot-AB.generating", false},
		}
		for idx, tt := range tests {
			v := s.isZombie(tt.dirName)
			require.Equal(t, tt.valid, v,
				fmt.Sprintf("dir name %s (%d) failed to match", tt.dirName, idx))
		}
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}
