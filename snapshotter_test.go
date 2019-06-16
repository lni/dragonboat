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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/internal/logdb"
	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

const (
	tmpSnapshotDirSuffix = "generating"
	rdbTestDirectory     = "rdb_test_dir_safe_to_delete"
)

func getNewTestDB(dir string, lldir string) raftio.ILogDB {
	d := filepath.Join(rdbTestDirectory, dir)
	lld := filepath.Join(rdbTestDirectory, lldir)
	if err := os.MkdirAll(d, 0777); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(lld, 0777); err != nil {
		panic(err)
	}
	db, err := logdb.NewDefaultLogDB([]string{d}, []string{lld})
	if err != nil {
		panic(err.Error())
	}
	return db
}

func deleteTestRDB() {
	os.RemoveAll(rdbTestDirectory)
}

func getTestSnapshotter(ldb raftio.ILogDB) *snapshotter {
	fp := filepath.Join(rdbTestDirectory, "snapshot")
	if err := os.MkdirAll(fp, 0777); err != nil {
		panic(err)
	}
	f := func(cid uint64, nid uint64) string {
		return fp
	}
	return newSnapshotter(1, 1, f, ldb, nil)
}

func runSnapshotterTest(t *testing.T,
	fn func(t *testing.T, logdb raftio.ILogDB, snapshotter *snapshotter)) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	deleteTestRDB()
	ldb := getNewTestDB(dir, lldir)
	s := getTestSnapshotter(ldb)
	defer deleteTestRDB()
	defer ldb.Close()
	fn(t, ldb, s)
}

func TestFinalizeSnapshotReturnExpectedErrorWhenOutOfDate(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getSnapshotEnv(ss.Index)
		finalSnapDir := env.GetFinalDir()
		if err := os.MkdirAll(finalSnapDir, 0755); err != nil {
			t.Errorf("failed to create final snap dir")
		}
		if err := env.CreateTempDir(); err != nil {
			t.Errorf("create tmp snapshot dir failed %v", err)
		}
		if err := s.Commit(ss, rsm.SnapshotRequest{}); err != errSnapshotOutOfDate {
			t.Errorf("unexpected error result %v", err)
		}
	}
	runSnapshotterTest(t, fn)
}

func TestSnapshotCanBeFinalized(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		ss := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getSnapshotEnv(ss.Index)
		finalSnapDir := env.GetFinalDir()
		tmpDir := env.GetTempDir()
		err := env.CreateTempDir()
		if err != nil {
			t.Errorf("create tmp snapshot dir failed %v", err)
		}
		_, err = os.Stat(tmpDir)
		if err != nil {
			t.Errorf("failed to get stat for tmp dir, %v", err)
		}
		testfp := filepath.Join(tmpDir, "test.data")
		f, err := os.Create(testfp)
		if err != nil {
			t.Errorf("failed to create test file")
		}
		if _, err := f.Write(make([]byte, 12)); err != nil {
			t.Fatalf("write failed %v", err)
		}
		f.Close()
		if err = s.Commit(ss, rsm.SnapshotRequest{}); err != nil {
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
		if _, err = os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Errorf("tmp dir not removed, %v", err)
		}
		fi, err := os.Stat(finalSnapDir)
		if err != nil {
			t.Errorf("failed to get stats, %v", err)
		}
		if !fi.IsDir() {
			t.Errorf("not a dir")
		}
		if fileutil.HasFlagFile(finalSnapDir, fileutil.SnapshotFlagFilename) {
			t.Errorf("flag file not removed")
		}
		vfp := filepath.Join(finalSnapDir, "test.data")
		fi, err = os.Stat(vfp)
		if err != nil {
			t.Errorf("failed to get stat %v", err)
		}
		if fi.IsDir() || fi.Size() != 12 {
			t.Errorf("not the same test file. ")
		}
	}
	runSnapshotterTest(t, fn)
}

func TestSnapshotCanBeSavedToLogDB(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    1,
			Term:     2,
		}
		if err := s.saveToLogDB(s1); err != nil {
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
	runSnapshotterTest(t, fn)
}

func TestZombieSnapshotDirsCanBeRemoved(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		env1 := s.getSnapshotEnv(100)
		env2 := s.getSnapshotEnv(200)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd1 = filepath.Join(fd1, tmpSnapshotDirSuffix)
		fd2 = filepath.Join(fd2, ".receiving")
		if err := os.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := os.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.ProcessOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := os.Stat(fd1); os.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
		if _, err := os.Stat(fd2); os.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
	}
	runSnapshotterTest(t, fn)
}

func TestFirstSnapshotBecomeOrphanedIsHandled(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, s *snapshotter) {
		s1 := pb.Snapshot{
			FileSize: 1234,
			Filepath: "f2",
			Index:    100,
			Term:     200,
		}
		env := s.getSnapshotEnv(100)
		fd1 := env.GetFinalDir()
		if err := os.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fileutil.CreateFlagFile(fd1, fileutil.SnapshotFlagFilename, &s1); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := s.ProcessOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := os.Stat(fd1); !os.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
	}
	runSnapshotterTest(t, fn)
}

func TestOrphanedSnapshotsCanBeProcessed(t *testing.T) {
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
		env1 := s.getSnapshotEnv(s1.Index)
		env2 := s.getSnapshotEnv(s2.Index)
		env3 := s.getSnapshotEnv(s3.Index)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd3 := env3.GetFinalDir()
		fd4 := fmt.Sprintf("%s%s", fd3, "xx")
		if err := os.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := os.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := os.MkdirAll(fd4, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fileutil.CreateFlagFile(fd1, fileutil.SnapshotFlagFilename, &s1); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := fileutil.CreateFlagFile(fd2, fileutil.SnapshotFlagFilename, &s2); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := fileutil.CreateFlagFile(fd4, fileutil.SnapshotFlagFilename, &s3); err != nil {
			t.Errorf("failed to create flag file %s", err)
		}
		if err := s.saveToLogDB(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		// fd1 has record in logdb. flag file expected to be removed while the fd1
		// foler is expected to be kept
		// fd2 doesn't has its record in logdb, while the most recent snapshot record
		// in logdb is not for fd2, fd2 will be entirely removed
		if err := s.ProcessOrphans(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if fileutil.HasFlagFile(fd1, fileutil.SnapshotFlagFilename) {
			t.Errorf("flag for fd1 not removed")
		}
		if fileutil.HasFlagFile(fd2, fileutil.SnapshotFlagFilename) {
			t.Errorf("flag for fd2 not removed")
		}
		if !fileutil.HasFlagFile(fd4, fileutil.SnapshotFlagFilename) {
			t.Errorf("flag for fd4 is missing")
		}
		if _, err := os.Stat(fd1); os.IsNotExist(err) {
			t.Errorf("fd1 removed by mistake")
		}
		if _, err := os.Stat(fd2); !os.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
	}
	runSnapshotterTest(t, fn)
}

func TestRemoveUnusedSnapshotRemoveSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// normal case
	testRemoveUnusedSnapshotRemoveSnapshots(t, 32, 7, 5)
	// snapshotsToKeep snapshots will be kept
	testRemoveUnusedSnapshotRemoveSnapshots(t, 4, 5, 2)
	// snapshotsToKeep snapshots will be kept
	testRemoveUnusedSnapshotRemoveSnapshots(t, 3, 3, 1)
}

func testRemoveUnusedSnapshotRemoveSnapshots(t *testing.T,
	total uint64, upTo uint64, removed uint64) {
	fn := func(t *testing.T, ldb raftio.ILogDB, snapshotter *snapshotter) {
		for i := uint64(1); i <= total; i++ {
			fn := fmt.Sprintf("f%d.data", i)
			s := pb.Snapshot{
				FileSize: 1234,
				Filepath: fn,
				Index:    i,
				Term:     2,
			}
			env := snapshotter.getSnapshotEnv(s.Index)
			if err := env.CreateTempDir(); err != nil {
				t.Errorf("failed to create snapshot dir")
			}
			if err := snapshotter.Commit(s, rsm.SnapshotRequest{}); err != nil {
				t.Errorf("failed to save snapshot record")
			}
			fp := snapshotter.GetFilePath(s.Index)
			f, err := os.Create(fp)
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
			env := snapshotter.getSnapshotEnv(i)
			snapDir := env.GetFinalDir()
			if _, err = os.Stat(snapDir); os.IsNotExist(err) {
				t.Errorf("snapshot dir didn't get created, %s", snapDir)
			}
		}
		if err = snapshotter.Compact(upTo); err != nil {
			t.Errorf("failed to remove unused snapshots, %v", err)
		}
		snapshots, err = ldb.ListSnapshots(1, 1, math.MaxUint64)
		if err != nil {
			t.Errorf("failed to list snapshot")
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
			fp := snapshotter.GetFilePath(i)
			if _, err := os.Stat(fp); !os.IsNotExist(err) {
				t.Errorf("snapshot file didn't get deleted")
			}
			env := snapshotter.getSnapshotEnv(i)
			snapDir := env.GetFinalDir()
			if _, err := os.Stat(snapDir); !os.IsNotExist(err) {
				t.Errorf("snapshot dir didn't get removed")
			}
		}
	}
	runSnapshotterTest(t, fn)
}

func TestShrinkSnapshots(t *testing.T) {
	fn := func(t *testing.T, ldb raftio.ILogDB, snapshotter *snapshotter) {
		for i := uint64(1); i <= 3; i++ {
			index := i * 10
			env := snapshotter.getSnapshotEnv(index)
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
			if err := snapshotter.Commit(s, rsm.SnapshotRequest{}); err != nil {
				t.Errorf("failed to save snapshot record")
			}
			fp = snapshotter.GetFilePath(s.Index)
			writer, err := rsm.NewSnapshotWriter(fp, rsm.V2SnapshotVersion)
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
		if err := snapshotter.Shrink(20); err != nil {
			t.Fatalf("shrink snapshots failed %v", err)
		}
		env1 := snapshotter.getSnapshotEnv(10)
		env2 := snapshotter.getSnapshotEnv(20)
		env3 := snapshotter.getSnapshotEnv(30)
		cf := func(p string, esz uint64) {
			fi, err := os.Stat(p)
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
	runSnapshotterTest(t, fn)
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
			v := s.dirNameMatch(tt.dirName)
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
		}
	}
	runSnapshotterTest(t, fn)
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
			v := s.isZombieDir(tt.dirName)
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
		}
	}
	runSnapshotterTest(t, fn)
}
