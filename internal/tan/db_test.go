// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
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

package tan

import (
	"math"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func runTanTest(t *testing.T, opts *Options, tf func(t *testing.T, d *db), fs vfs.FS) {
	defer leaktest.AfterTest(t)()
	if opts == nil {
		opts = &Options{
			MaxManifestFileSize: MaxManifestFileSize,
			MaxLogFileSize:      MaxLogFileSize,
			FS:                  fs,
		}
	} else if opts.FS == nil {
		panic("fs not specified")
	}
	defer vfs.ReportLeakedFD(opts.FS, t)
	dirname := "/Users/lni/db-dir"
	require.NoError(t, fileutil.MkdirAll(dirname, opts.FS))
	db, err := open(dirname, dirname, opts)
	require.NoError(t, err)
	defer func() {
		plog.Infof("going to close")
		db.close()
	}()
	tf(t, db)
}

func TestOpenNewDB(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		rs, err := db.getRaftState(2, 3, 200)
		require.Equal(t, raftio.ErrNoSavedLog, err)
		require.Equal(t, raftio.RaftState{}, rs)
		require.Equal(t, uint64(0), rs.EntryCount)
		require.Equal(t, uint64(0), rs.FirstIndex)
		ss, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.True(t, pb.IsEmptySnapshot(ss))
		var entries []pb.Entry
		entries, size, err := db.getEntries(2, 3, entries, 0, 0, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 0, len(entries))
		require.Equal(t, uint64(0), size)
	}
	runTanTest(t, nil, tf, fs)
}

func TestBasicDBReadWrite(t *testing.T) {
	for _, testSize := range []uint64{0, 1, 1024, 16 * 1024, blockSize, blockSize * 3} {
		size := testSize
		fs := vfs.NewMem()
		tf := func(t *testing.T, db *db) {
			var cmd []byte
			if size > 0 {
				cmd = make([]byte, size)
			}
			u1 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
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
					{Index: 101, Term: 5, Cmd: cmd},
					{Index: 102, Term: 5, Cmd: cmd},
					{Index: 103, Term: 5, Cmd: cmd},
				},
			}
			u2 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: 200,
					Term:   10,
					Vote:   6,
				},
				Snapshot: pb.Snapshot{
					Index: 200,
					Term:  10,
				},
				EntriesToSave: []pb.Entry{
					{Index: 201, Term: 10, Cmd: cmd},
					{Index: 202, Term: 10, Cmd: cmd},
					{Index: 203, Term: 10, Cmd: cmd},
				},
			}
			buf := make([]byte, 1024)
			_, err := db.write(u1, buf)
			require.NoError(t, err)
			_, err = db.write(u2, buf)
			require.NoError(t, err)

			require.Equal(t, u2.State, db.mu.nodeStates.getState(2, 3))
			rs, err := db.getRaftState(2, 3, 200)
			require.NoError(t, err)
			require.Equal(t, u2.State, rs.State)
			require.Equal(t, uint64(201), rs.FirstIndex)
			require.Equal(t, uint64(3), rs.EntryCount)

			snapshot, err := db.getSnapshot(2, 3)
			require.NoError(t, err)
			require.Equal(t, u2.Snapshot, snapshot)

			var result []pb.Entry
			entries, _, err := db.getEntries(2, 3, result, 0, 201, 203, math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, 2, len(entries))
			require.Equal(t, size, uint64(len(entries[0].Cmd)))
		}
		runTanTest(t, nil, tf, fs)
	}
}

func TestEntryOverwrite(t *testing.T) {
	type entry struct {
		index uint64
		term  uint64
	}
	type entryRange struct {
		start uint64
		end   uint64
		term  uint64
	}

	tests := []struct {
		input    []entryRange
		low      uint64
		high     uint64
		expected []entry
	}{
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			101, 102,
			[]entry{{101, 5}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			102, 105,
			[]entry{{102, 10}, {103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			103, 105,
			[]entry{{103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			101, 105,
			[]entry{{101, 5}, {102, 10}, {103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}, {100, 105, 15}},
			100, 105,
			[]entry{{100, 15}, {101, 15}, {102, 15}, {103, 15}, {104, 15}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}, {103, 105, 15}},
			101, 105,
			[]entry{{101, 5}, {102, 10}, {103, 15}, {104, 15}},
		},
	}

	buf := make([]byte, 1024)
	for testIdx, tt := range tests {
		idx := testIdx
		input := tt.input
		expected := tt.expected
		low := tt.low
		high := tt.high
		func() {
			fs := vfs.NewMem()
			tf := func(t *testing.T, db *db) {
				for _, ir := range input {
					u := pb.Update{ShardID: 2, ReplicaID: 3}
					for j := ir.start; j <= ir.end; j++ {
						u.EntriesToSave = append(u.EntriesToSave, pb.Entry{Index: j, Term: ir.term})
					}
					_, err := db.write(u, buf)
					require.NoError(t, err)
				}
			}
			runTanTest(t, nil, tf, fs)

			tf = func(t *testing.T, db *db) {
				var result []pb.Entry
				entries, _, err := db.getEntries(2, 3, result, 0, low, high, 1024)
				require.NoError(t, err)
				require.Equal(t, len(expected), len(entries))
				for j, r := range expected {
					require.Equalf(t, r.index, entries[j].Index, "idx: %d, j: %d", idx, j)
					require.Equalf(t, r.term, entries[j].Term, "idx: %d, j: %d", idx, j)
				}
			}
			runTanTest(t, nil, tf, fs)
		}()
	}
}

func TestEmptyEntryUpdate(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		u1 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: 1, Term: 5},
			},
		}
		u2 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 1,
				Term:  5,
			},
		}
		u3 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 1,
				Term:   5,
			},
		}
		u4 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: 2, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		_, err1 := db.write(u1, buf)
		require.NoError(t, err1)
		_, err2 := db.write(u2, buf)
		require.NoError(t, err2)
		_, err3 := db.write(u3, buf)
		require.NoError(t, err3)
		_, err4 := db.write(u4, buf)
		require.NoError(t, err4)
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 3, 10240)
		require.NoError(t, err)
		require.Equal(t, 2, len(entries))
	}
	runTanTest(t, nil, tf, fs)
}

func TestSnapshotUpdate(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		u1 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 100,
				Term:  5,
			},
		}
		u2 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 90,
				Term:  5,
			},
		}
		u3 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 80,
				Term:  5,
			},
		}
		buf := make([]byte, 1024)
		_, err1 := db.write(u1, buf)
		require.NoError(t, err1)
		_, err2 := db.write(u2, buf)
		require.NoError(t, err2)
		_, err3 := db.write(u3, buf)
		require.NoError(t, err3)
		snapshot, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(100), snapshot.Index)
	}
	runTanTest(t, nil, tf, fs)
}

func TestLogRotation(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		logNum := db.mu.logNum
		fn := makeFilename(opts.FS, db.dirname, fileTypeLog, logNum)
		_, err := opts.FS.Stat(fn)
		require.NoError(t, err)
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
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
		require.NotEqual(t, logNum, db.mu.logNum)
		fn = makeFilename(opts.FS, db.dirname, fileTypeLog, db.mu.logNum)
		_, err = opts.FS.Stat(fn)
		require.NoError(t, err)
		// check we can query across multiple logs
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 99, len(entries))
		for i := uint64(1); i < uint64(100); i++ {
			require.Equal(t, i, entries[i-1].Index)
		}
		// don't assume when the rotation happened, just check again to make sure the
		// db is accessible by writes
		_, err = db.write(u, buf)
		require.NoError(t, err)
	}
	runTanTest(t, opts, tf, fs)
}

func TestDBRestart(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	var logNum fileNum
	tf := func(t *testing.T, db *db) {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
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
		logNum = db.mu.logNum
	}
	runTanTest(t, opts, tf, fs)

	tf = func(t *testing.T, db *db) {
		require.NotEqual(t, logNum, db.mu.logNum)
		// this will write an entry with index 100 term 6
		// query should return this entry
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
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
				{Index: 100, Term: 6},
			},
		}
		buf := make([]byte, 1024)
		_, err := db.write(u, buf)
		require.NoError(t, err)
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 99, len(entries))
		for i := uint64(1); i < uint64(100); i++ {
			require.Equal(t, i, entries[i-1].Index)
		}
		require.Equal(t, uint64(5), entries[len(entries)-1].Term)
		snapshot, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(100), snapshot.Index)

		rs, err := db.getRaftState(2, 3, 1)
		require.NoError(t, err)
		require.Equal(t, u.State, rs.State)
		require.Equal(t, uint64(2), rs.FirstIndex)
		require.Equal(t, uint64(99), rs.EntryCount)
	}
	runTanTest(t, opts, tf, fs)
}

func TestDBConcurrentAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxLogFileSize:      1,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir"
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	db, err := open(dirname, dirname, opts)
	require.NoError(t, err)
	defer db.close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(1000); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: i,
					Term:   6,
				},
				Snapshot: pb.Snapshot{
					Index: i,
					Term:  6,
				},
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 6},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(1000); i++ {
			if i%uint64(10) == 0 {
				require.NoError(t, db.removeAll(2, 3))
			} else if i%uint64(11) == 0 {
				if err := db.removeEntries(2, 3, i); err != nil {
					if err != ErrNoState {
						t.Errorf("failed to remove entries %v", err)
					}
				}
			} else {
				u := pb.Update{
					ShardID:   2,
					ReplicaID: 3,
					State: pb.State{
						Commit: i,
						Term:   5,
					},
					Snapshot: pb.Snapshot{
						Index: i,
						Term:  5,
					},
					EntriesToSave: []pb.Entry{
						{Index: i, Term: 5},
					},
				}
				_, err := db.write(u, buf)
				require.NoError(t, err)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			var result []pb.Entry
			_, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
			require.NoError(t, err)
			_, err = db.getSnapshot(2, 3)
			require.NoError(t, err)
			_, err = db.getRaftState(2, 3, 1)
			require.True(t, err == nil || errors.Is(err, raftio.ErrNoSavedLog))
		}
	}()
	wg.Wait()
}

func TestDBIndexIsSavedOnClose(t *testing.T) {
	fs := vfs.NewMem()
	var logNum fileNum
	var index []indexEntry
	var dirname string
	tf := func(t *testing.T, db *db) {
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(10); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 6},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		dirname = db.dirname
		logNum = db.mu.logNum
		index = db.mu.nodeStates.getIndex(2, 3).currEntries.entries
	}
	runTanTest(t, nil, tf, fs)
	fn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	_, err := fs.Stat(fn)
	require.NoError(t, err)
	require.True(t, len(index) > 0)
}

func TestRebuildIndex(t *testing.T) {
	fs := vfs.NewMem()
	var logNum fileNum
	var savedIndex index
	var snapshot indexEntry
	var dirname string
	tf := func(t *testing.T, db *db) {
		dirname = db.dirname
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(1000); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 5},
				},
				Snapshot: pb.Snapshot{Index: 300, Term: 10},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		require.NoError(t, db.removeEntries(2, 3, 500))
		logNum = db.mu.logNum
		savedIndex = db.mu.nodeStates.getIndex(2, 3).entries
		snapshot = db.mu.nodeStates.getIndex(2, 3).snapshot
	}
	runTanTest(t, nil, tf, fs)

	fn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	require.NoError(t, fs.RemoveAll(fn))

	tf = func(t *testing.T, db *db) {
		require.Equal(t, savedIndex, db.mu.nodeStates.getIndex(2, 3).entries)
		require.Equal(t, snapshot, db.mu.nodeStates.getIndex(2, 3).snapshot)
		require.Equal(t, uint64(500), db.mu.nodeStates.compactedTo(2, 3))
	}
	runTanTest(t, nil, tf, fs)
}

func TestRebuildLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.Default
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir"
	require.NoError(t, fs.RemoveAll(dirname))
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	defer func() {
		require.NoError(t, fs.RemoveAll(dirname))
	}()
	db, err := open(dirname, dirname, opts)
	require.NoError(t, err)
	buf := make([]byte, 1024)
	for i := uint64(1); i <= uint64(20); i++ {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: i, Term: 5, Cmd: make([]byte, 32)},
			},
		}
		_, err := db.write(u, buf)
		require.NoError(t, err)
	}
	logNum := db.mu.logNum
	require.NoError(t, db.close())
	logFn := makeFilename(fs, dirname, fileTypeLog, logNum)
	idxFn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	lf, err := os.OpenFile(logFn, os.O_RDWR, 0755)
	require.NoError(t, err)
	fi, err := lf.Stat()
	require.NoError(t, err)
	// truncate the file, remove the index
	require.NoError(t, lf.Truncate(fi.Size()-16))
	require.NoError(t, lf.Close())
	require.NoError(t, fs.RemoveAll(idxFn))
	db, err = open(dirname, dirname, opts)
	require.NoError(t, err)
	var result []pb.Entry
	result, _, err = db.getEntries(2, 3, result, 0, 1, 21, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, 19, len(result))
	require.Equal(t, uint64(19), result[len(result)-1].Index)
	require.NoError(t, db.close())
	lf, err = os.Open(logFn)
	require.NoError(t, err)
	fi2, err := lf.Stat()
	require.NoError(t, err)
	require.Equal(t, fi.Size()*19/20, fi2.Size())
}

func TestGetEntriesWithMaxSize(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		MaxLogFileSize:      1024,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		cmd := make([]byte, 128)
		buf := make([]byte, 1024)
		for i := 0; i < 128; i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				EntriesToSave: []pb.Entry{
					{Index: 1 + uint64(i), Term: 5, Cmd: cmd},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		entries, _, err := db.getEntries(2, 3, nil, 0, 1, 128, 128)
		require.NoError(t, err)
		require.Equal(t, 1, len(entries))
	}
	runTanTest(t, opts, tf, fs)
}
