// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors/oserror"

	"github.com/lni/dragonboat/v3/config"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestRemoveEntries(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		buf := make([]byte, 1024)
		for i := uint64(1); i < uint64(100); i++ {
			u := pb.Update{
				ClusterID:     2,
				NodeID:        3,
				State:         pb.State{Commit: i},
				EntriesToSave: []pb.Entry{{Index: i, Term: 1}},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		require.NoError(t, db.removeEntries(2, 3, uint64(99)))
		// FIXME: this is a race
		/*db.mu.Lock()
		require.Equal(t, 98, len(db.mu.versions.obsoleteTables))
		db.mu.Unlock()*/
	}
	runTanTest(t, opts, tf, fs)

	tf = func(t *testing.T, db *db) {
		require.Equal(t, 3, len(db.mu.versions.currentVersion().files))
		var entries []pb.Entry
		entries, _, err := db.getEntries(2, 3, entries, 0, 99, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 0, len(entries))
		var entries1 []pb.Entry
		entries1, _, err = db.getEntries(2, 3, entries1, 0, 98, 98, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 0, len(entries1))

		for j := 0; j < 3000; j++ {
			ls, err := db.opts.FS.List(db.dirname)
			require.NoError(t, err)
			noObsolete := true
			for _, filename := range ls {
				fileType, fileNum, ok := parseFilename(db.opts.FS, filename)
				if !ok {
					continue
				}
				if fileType == fileTypeLog {
					_, ok := db.mu.versions.currentVersion().files[fileNum]
					if !ok {
						noObsolete = false
					}
				}
			}
			if noObsolete {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("failed to remove all obsolete files")
	}
	runTanTest(t, opts, tf, fs)
}

func TestRemovedEntriesMultiplexedLogSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{FS: vfs.NewMem()},
	}
	dirs := []string{"db-dir"}
	ldb, err := CreateLogMultiplexedTan(cfg, nil, dirs, []string{})
	require.NoError(t, err)
	defer ldb.Close()
	for i := uint64(0); i < 16; i++ {
		updates := []pb.Update{
			{
				ClusterID: 1,
				NodeID:    1,
				Snapshot:  pb.Snapshot{Index: i * uint64(100), Term: 10},
				State:     pb.State{Commit: i * uint64(100), Term: 10},
				EntriesToSave: []pb.Entry{
					{Index: i*2 + 1, Term: 10},
					{Index: i*2 + 2, Term: 10},
				},
			},
			{
				ClusterID: 17,
				NodeID:    1,
				Snapshot:  pb.Snapshot{Index: i * uint64(200), Term: 20},
				State:     pb.State{Commit: i * uint64(200), Term: 20},
				EntriesToSave: []pb.Entry{
					{Index: i*3 + 1, Term: 20},
					{Index: i*3 + 2, Term: 20},
					{Index: i*3 + 3, Term: 20},
				},
			},
		}
		require.NoError(t, ldb.SaveRaftState(updates, 1))
		db, err := ldb.shards.getDB(1, 1)
		require.NoError(t, err)
		require.NoError(t, db.switchToNewLog())
	}
	db, err := ldb.shards.getDB(1, 1)
	require.NoError(t, err)
	current := db.mu.versions.currentVersion()
	fileCount := len(current.files)
	// not suppose tp have any log file removed
	require.NoError(t, ldb.RemoveEntriesTo(1, 1, 32))
	current = db.mu.versions.currentVersion()
	require.Equal(t, fileCount, len(current.files))
	// this should trigger log compaction
	require.NoError(t, ldb.RemoveEntriesTo(17, 1, 48))
	current = db.mu.versions.currentVersion()
	// a log file and an empty log file just created by the last switchToNewLog
	require.Equal(t, 2, len(current.files))
}

func TestRemoveAll(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		u := pb.Update{
			ClusterID: 2,
			NodeID:    3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
			},
			Snapshot: pb.Snapshot{
				Index: 100,
				Term:  5,
			},
			EntriesToSave: []pb.Entry{
				{Index: 0, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(100); i++ {
			u.EntriesToSave[0].Index = i
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		require.NoError(t, db.removeAll(2, 3))
		for i := 0; i < 3000; i++ {
			ls, err := db.opts.FS.List(db.dirname)
			require.NoError(t, err)
			unexpectedFile := false
			for _, file := range ls {
				fileType, fileNum, ok := parseFilename(db.opts.FS, file)
				if !ok {
					continue
				}
				if fileType == fileTypeLog {
					if fileNum != db.mu.logNum {
						unexpectedFile = true
					}
				}
				if fileType == fileTypeIndex {
					unexpectedFile = true
				}
			}
			if unexpectedFile {
				time.Sleep(time.Millisecond)
			} else {
				break
			}
		}
	}
	runTanTest(t, opts, tf, fs)
}

func TestInstallSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		u := pb.Update{
			ClusterID: 2,
			NodeID:    3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
			},
			Snapshot: pb.Snapshot{
				Index: 101,
				Term:  5,
			},
			EntriesToSave: []pb.Entry{
				{Index: 0, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(100); i++ {
			u.EntriesToSave[0].Index = i
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		ss := pb.Snapshot{
			ClusterId: 2,
			Index:     50,
			Term:      3,
		}
		require.NoError(t, db.importSnapshot(2, 3, ss))
		for i := uint64(1); i <= uint64(100); i++ {
			var result []pb.Entry
			entries, _, err := db.getEntries(2, 3, result, 0, i, i, 1024)
			require.NoError(t, err)
			require.Equal(t, 0, len(entries))
		}

		rs, err := db.getRaftState(2, 3, 50)
		require.NoError(t, err)
		require.Equal(t, ss.Index, rs.State.Commit)
		require.Equal(t, ss.Term, rs.State.Term)
		snapshot, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, ss, snapshot)
	}
	runTanTest(t, opts, tf, fs)
}

func TestScanObsoleteFiles(t *testing.T) {
	fs := vfs.NewMem()
	var dbdir string
	tf := func(t *testing.T, db *db) { dbdir = db.dirname }
	runTanTest(t, nil, tf, fs)
	manifestFn := "MANIFEST-1000"
	logFn := "10001.log"
	f, err := fs.Create(fs.PathJoin(dbdir, manifestFn))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	f, err = fs.Create(fs.PathJoin(dbdir, logFn))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	runTanTest(t, nil, tf, fs)
	_, err = fs.Stat(manifestFn)
	require.True(t, oserror.IsNotExist(err))
	_, err = fs.Stat(logFn)
	require.True(t, oserror.IsNotExist(err))
}

func TestNodeIndexCompaction(t *testing.T) {
	nodeIndex := nodeIndex{
		entries: index{
			compactedTo: 9,
			entries: []indexEntry{
				{1, 2, 1, 5, 10},
				{3, 4, 2, 5, 10},
				{5, 6, 3, 5, 10},
				{7, 7, 4, 5, 10},
				{8, 10, 5, 5, 10},
				{11, 12, 6, 5, 10},
			},
		},
		state:    indexEntry{7, 0, 4, 5, 100},
		snapshot: indexEntry{5, 0, 3, 5, 100},
	}
	n0 := nodeIndex
	n0.state = indexEntry{}
	n0.snapshot = indexEntry{}
	require.Equal(t, []fileNum{1, 2, 3, 4}, n0.compaction())
	n1 := nodeIndex
	require.Equal(t, []fileNum{1, 2}, n1.compaction())
	n2 := nodeIndex
	n2.entries = index{}
	require.Nil(t, n2.compaction())
	n3 := nodeIndex
	n3.state = indexEntry{}
	require.Equal(t, []fileNum{1, 2}, n3.compaction())
	n4 := nodeIndex
	n4.snapshot = indexEntry{}
	require.Equal(t, []fileNum{1, 2, 3}, n4.compaction())
}
