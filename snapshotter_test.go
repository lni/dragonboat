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
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	tmpSnapshotDirSuffix = "generating"
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
	db, err := logdb.NewDefaultLogDB(cfg, nil, []string{d}, []string{lld}, fs)
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
	return newSnapshotter(1, 1, f, ldb, fs)
}

func runSnapshotterTest(t *testing.T,
	fn func(t *testing.T, logdb raftio.ILogDB, snapshotter *snapshotter), fs vfs.IFS) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	deleteTestRDB(fs)
	ldb := getNewTestDB(dir, lldir, fs)
	s := getTestSnapshotter(ldb, fs)
	defer deleteTestRDB(fs)
	defer ldb.Close()
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
		if err := fs.MkdirAll(finalSnapDir, 0755); err != nil {
			t.Errorf("failed to create final snap dir")
		}
		if err := env.CreateTempDir(); err != nil {
			t.Errorf("create tmp snapshot dir failed %v", err)
		}
		if err := s.commit(ss, rsm.SSRequest{}); !errors.Is(err, errSnapshotOutOfDate) {
			t.Errorf("unexpected error result %v", err)
		}
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
		err := env.CreateTempDir()
		if err != nil {
			t.Errorf("create tmp snapshot dir failed %v", err)
		}
		_, err = fs.Stat(tmpDir)
		if err != nil {
			t.Errorf("failed to get stat for tmp dir, %v", err)
		}
		testfp := fs.PathJoin(tmpDir, "test.data")
		f, err := fs.Create(testfp)
		if err != nil {
			t.Errorf("failed to create test file")
		}
		if _, err := f.Write(make([]byte, 12)); err != nil {
			t.Fatalf("write failed %v", err)
		}
		f.Close()
		if err = s.commit(ss, rsm.SSRequest{}); err != nil {
			t.Errorf("finalize snapshot failed %v", err)
		}
		snapshots, err := ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
		}
		if len(snapshots) != 1 {
			t.Errorf("returned %d snapshot records, want 1", len(snapshots))
		}
		rs, err := s.GetSnapshot(100)
		if err != nil {
			t.Errorf("failed to get snapshot")
		}
		if rs.Index != 100 {
			t.Errorf("returned an unexpected snapshot")
		}
		_, err = s.GetSnapshot(200)
		if err != ErrNoSnapshot {
			t.Errorf("unexpected err %v", err)
		}
		if _, err = fs.Stat(tmpDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed, %v", err)
		}
		fi, err := fs.Stat(finalSnapDir)
		if err != nil {
			t.Errorf("failed to get stats, %v", err)
		}
		if !fi.IsDir() {
			t.Errorf("not a dir")
		}
		if fileutil.HasFlagFile(finalSnapDir, fileutil.SnapshotFlagFilename, fs) {
			t.Errorf("flag file not removed")
		}
		vfp := fs.PathJoin(finalSnapDir, "test.data")
		fi, err = fs.Stat(vfp)
		if err != nil {
			t.Errorf("failed to get stat %v", err)
		}
		if fi.IsDir() || fi.Size() != 12 {
			t.Errorf("not the same test file. ")
		}
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
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot record %v", err)
		}
		snapshots, err := ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
		}
		if len(snapshots) != 1 {
			t.Errorf("returned %d snapshot records, want 1", len(snapshots))
		}
		if !reflect.DeepEqual(&s1, &snapshots[0]) {
			t.Errorf("snapshot record changed")
		}
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
		fd1 = fs.PathJoin(fd1, tmpSnapshotDirSuffix)
		fd2 = fs.PathJoin(fd2, ".receiving")
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.processOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); vfs.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
		if _, err := fs.Stat(fd2); vfs.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
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
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fileutil.CreateFlagFile(fd1, fileutil.SnapshotFlagFilename, &s1, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := s.processOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); !vfs.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
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
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fileutil.CreateFlagFile(fd1, fileutil.SnapshotFlagFilename, &s1, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := fileutil.CreateFlagFile(fd2, fileutil.SnapshotFlagFilename, &s2, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		if err := s.saveSnapshot(s2); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		// two orphane snapshots, kept the most recent one, and remove the older
		// one including its logdb record.
		if err := s.processOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); vfs.IsExist(err) {
			t.Errorf("failed to remove fd1")
		}
		if _, err := fs.Stat(fd2); vfs.IsNotExist(err) {
			t.Errorf("unexpectedly removed fd2")
		}
		if fileutil.HasFlagFile(fd2, fileutil.SnapshotFlagFilename, fs) {
			t.Errorf("flag for fd2 not removed")
		}
		snapshots, err := s.logdb.ListSnapshots(1, 1, 200)
		if err != nil {
			t.Fatalf("failed to list snapshot %v", err)
		}
		if len(snapshots) != 1 {
			t.Fatalf("unexpected number of records %d", len(snapshots))
		}
		if snapshots[0].Index != 200 {
			t.Fatalf("unexpected record %v", snapshots[0])
		}
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
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd4, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fileutil.CreateFlagFile(fd1, fileutil.SnapshotFlagFilename, &s1, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := fileutil.CreateFlagFile(fd2, fileutil.SnapshotFlagFilename, &s2, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := fileutil.CreateFlagFile(fd4, fileutil.SnapshotFlagFilename, &s3, fs); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		// fd1 has record in logdb. flag file expected to be removed while the fd1
		// foler is expected to be kept
		// fd2 doesn't has its record in logdb, while the most recent snapshot record
		// in logdb is not for fd2, fd2 will be entirely removed
		if err := s.processOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if fileutil.HasFlagFile(fd1, fileutil.SnapshotFlagFilename, fs) {
			t.Errorf("flag for fd1 not removed")
		}
		if fileutil.HasFlagFile(fd2, fileutil.SnapshotFlagFilename, fs) {
			t.Errorf("flag for fd2 not removed")
		}
		if !fileutil.HasFlagFile(fd4, fileutil.SnapshotFlagFilename, fs) {
			t.Errorf("flag for fd4 is missing")
		}
		if _, err := fs.Stat(fd1); vfs.IsNotExist(err) {
			t.Errorf("fd1 removed by mistake")
		}
		if _, err := fs.Stat(fd2); !vfs.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestRemoveUnusedSnapshotRemoveSnapshots(t *testing.T) {
	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	// normal case
	testRemoveUnusedSnapshotRemoveSnapshots(t, 32, 7, 7, fs)
	// snapshotsToKeep snapshots will be kept
	testRemoveUnusedSnapshotRemoveSnapshots(t, 4, 5, 4, fs)
	// snapshotsToKeep snapshots will be kept
	testRemoveUnusedSnapshotRemoveSnapshots(t, 3, 3, 3, fs)
}

func testRemoveUnusedSnapshotRemoveSnapshots(t *testing.T,
	total uint64, upTo uint64, removed uint64, fs vfs.IFS) {
	fn := func(t *testing.T, ldb raftio.ILogDB, snapshotter *snapshotter) {
		for i := uint64(1); i <= total; i++ {
			fn := fmt.Sprintf("f%d.data", i)
			s := pb.Snapshot{
				FileSize: 1234,
				Filepath: fn,
				Index:    i,
				Term:     2,
			}
			env := snapshotter.getEnv(s.Index)
			if err := env.CreateTempDir(); err != nil {
				t.Errorf("failed to create snapshot dir")
			}
			if err := snapshotter.commit(s, rsm.SSRequest{}); err != nil {
				t.Errorf("failed to save snapshot record")
			}
			fp := snapshotter.getFilePath(s.Index)
			f, err := fs.Create(fp)
			if err != nil {
				t.Errorf("failed to create the file, %v", err)
			}
			f.Close()
		}
		snapshots, err := ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
		}
		if uint64(len(snapshots)) != total {
			t.Errorf("didn't return %d snapshot records", total)
		}
		for i := uint64(1); i < removed; i++ {
			env := snapshotter.getEnv(i)
			snapDir := env.GetFinalDir()
			if _, err = fs.Stat(snapDir); vfs.IsNotExist(err) {
				t.Errorf("snapshot dir didn't get created, %s", snapDir)
			}
		}
		if err = snapshotter.compact(upTo); err != nil {
			t.Errorf("failed to remove unused snapshots, %v", err)
		}
		snapshots, err = ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
		}
		if len(snapshots) == 0 {
			t.Errorf("most recent snapshot also removed")
		}
		if uint64(len(snapshots)) != total-removed+1 {
			t.Errorf("got %d, want %d, first index %d",
				len(snapshots), total-removed+1, snapshots[0].Index)
		}
		for _, s := range snapshots {
			if s.Index < removed {
				t.Errorf("didn't remove snapshot %d", s.Index)
			}
		}
		for i := uint64(0); i < removed; i++ {
			fp := snapshotter.getFilePath(i)
			if _, err := fs.Stat(fp); !vfs.IsNotExist(err) {
				t.Errorf("snapshot file didn't get deleted")
			}
			env := snapshotter.getEnv(i)
			snapDir := env.GetFinalDir()
			if _, err := fs.Stat(snapDir); !vfs.IsNotExist(err) {
				t.Errorf("snapshot dir didn't get removed")
			}
		}
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
			if err := env.CreateTempDir(); err != nil {
				t.Errorf("failed to create snapshot dir")
			}
			if err := snapshotter.commit(s, rsm.SSRequest{}); err != nil {
				t.Errorf("failed to save snapshot record")
			}
			fp = snapshotter.getFilePath(s.Index)
			writer, err := rsm.NewSnapshotWriter(fp, pb.NoCompression, fs)
			if err != nil {
				t.Fatalf("failed to create the snapshot %v", err)
			}
			sz := make([]byte, 8)
			binary.LittleEndian.PutUint64(sz, 0)
			if _, err := writer.Write(sz); err != nil {
				t.Fatalf("failed to write %v", err)
			}
			for j := 0; j < 10; j++ {
				data := make([]byte, 1024*1024)
				if _, err := writer.Write(data); err != nil {
					t.Fatalf("failed to write %v", err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close failed %v", err)
			}
		}
		if err := snapshotter.shrink(20); err != nil {
			t.Fatalf("shrink snapshots failed %v", err)
		}
		env1 := snapshotter.getEnv(10)
		env2 := snapshotter.getEnv(20)
		env3 := snapshotter.getEnv(30)
		cf := func(p string, esz uint64) {
			fi, err := fs.Stat(p)
			if err != nil {
				t.Fatalf("failed to get file st %v", err)
			}
			if uint64(fi.Size()) != esz {
				// 1024 header, 8 size client session size, 8 bytes client session
				// count, 4 bytes crc, 16 bytes tails 1052 bytes in total
				t.Fatalf("unexpected size %d, want %d", fi.Size(), esz)
			}
		}
		cf(env1.GetFilepath(), 1060)
		cf(env2.GetFilepath(), 1060)
		cf(env3.GetFilepath(), 10486832)
		snapshots, err := ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
		}
		if len(snapshots) != 3 {
			t.Errorf("snapshot rec missing")
		}
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
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
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
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
		}
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}
