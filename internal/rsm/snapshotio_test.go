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

package rsm

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

const (
	testSessionSize      uint64 = 16
	testPayloadSize      uint64 = 8
	testSnapshotFilename        = "testsnapshot_safe_to_delete.tmp"
)

func reportLeakedFD(fs vfs.IFS, t *testing.T) {
	vfs.ReportLeakedFD(fs, t)
}

func TestSnapshotWriterCanBeCreated(t *testing.T) {
	fs := vfs.GetTestFS()
	w, err := NewSnapshotWriter(testSnapshotFilename, pb.NoCompression, fs)
	require.NoError(t, err)
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer func() {
		require.NoError(t, w.Close())
	}()
}

func TestSaveHeaderSavesTheHeader(t *testing.T) {
	fs := vfs.GetTestFS()
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		w, err := NewSnapshotWriter(testSnapshotFilename, pb.NoCompression, fs)
		require.NoError(t, err)
		sessionData := make([]byte, testSessionSize)
		storeData := make([]byte, testPayloadSize)
		_, err = rand.Read(sessionData)
		require.NoError(t, err)
		_, err = rand.Read(storeData)
		require.NoError(t, err)
		n, err := w.Write(sessionData)
		require.NoError(t, err)
		require.Equal(t, len(sessionData), n)
		m, err := w.Write(storeData)
		require.NoError(t, err)
		require.Equal(t, len(storeData), m)
		err = w.Close()
		require.NoError(t, err)
		r, header, err := NewSnapshotReader(testSnapshotFilename, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, r.Close())
		}()
		require.Equal(t, DefaultVersion, SSVersion(header.Version))
		require.Equal(t, DefaultChecksumType, header.ChecksumType)
		storeChecksum := w.vw.GetPayloadSum()
		require.True(t, bytes.Equal(header.PayloadChecksum, storeChecksum))
	}()
	reportLeakedFD(fs, t)
}

func makeTestSnapshotFile(t *testing.T, ssz uint64,
	psz uint64, v SSVersion, fs vfs.IFS) (*SnapshotWriter, []byte, []byte) {
	if err := fs.RemoveAll(testSnapshotFilename); err != nil {
		t.Fatalf("%v", err)
	}
	w, err := newVersionedSnapshotWriter(testSnapshotFilename, v,
		pb.NoCompression, fs)
	require.NoError(t, err)
	sessionData := make([]byte, ssz)
	storeData := make([]byte, psz)
	_, err = rand.Read(sessionData)
	require.NoError(t, err)
	_, err = rand.Read(storeData)
	require.NoError(t, err)
	n, err := w.Write(sessionData)
	require.NoError(t, err)
	require.Equal(t, len(sessionData), n)
	m, err := w.Write(storeData)
	require.NoError(t, err)
	require.Equal(t, len(storeData), m)
	if err := w.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	return w, sessionData, storeData
}

func corruptSnapshotPayload(t *testing.T, fs vfs.IFS) {
	tmpFp := "testsnapshot.writing"
	func() {
		f, err := fs.ReuseForWrite(testSnapshotFilename, tmpFp)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, f.Close())
		}()
		s := (testSessionSize + testPayloadSize) / 2
		data := make([]byte, 1)
		_, err = f.ReadAt(data, int64(HeaderSize+s))
		require.NoError(t, err)
		data[0] = data[0] + 1
		_, err = f.WriteAt(data, int64(HeaderSize+s))
		require.NoError(t, err)
	}()
	if err := fs.Rename(tmpFp, testSnapshotFilename); err != nil {
		t.Fatalf("%v", err)
	}
}

func createTestSnapshotFile(t *testing.T,
	v SSVersion, fs vfs.IFS) (*SnapshotWriter, []byte, []byte) {
	return makeTestSnapshotFile(t, testSessionSize, testPayloadSize, v, fs)
}

func testCorruptedPayloadWillBeDetected(t *testing.T, v SSVersion, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		createTestSnapshotFile(t, v, fs)
		corruptSnapshotPayload(t, fs)
		require.Panics(t, func() {
			r, header, err := NewSnapshotReader(testSnapshotFilename, fs)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, r.Close())
			}()
			_, err = rand.Read(header.PayloadChecksum)
			require.NoError(t, err)
			s := make([]byte, testSessionSize)
			p := make([]byte, testPayloadSize)
			n, err := io.ReadFull(r, s)
			require.NoError(t, err)
			require.Equal(t, testSessionSize, uint64(n))
			n, err = io.ReadFull(r, p)
			require.NoError(t, err)
			require.Equal(t, testPayloadSize, uint64(n))
			r.validatePayload()
		})
	}()
	reportLeakedFD(fs, t)
}

func TestCorruptedPayloadWillBeDetected(t *testing.T) {
	fs := vfs.GetTestFS()
	testCorruptedPayloadWillBeDetected(t, V1, fs)
	testCorruptedPayloadWillBeDetected(t, V2, fs)
}

func testNormalSnapshotCanPassValidation(t *testing.T, v SSVersion, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		_, sessionData, storeData := createTestSnapshotFile(t, v, fs)
		r, _, err := NewSnapshotReader(testSnapshotFilename, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, r.Close())
		}()
		s := make([]byte, testSessionSize)
		p := make([]byte, testPayloadSize)
		n, err := io.ReadFull(r, s)
		require.NoError(t, err)
		require.Equal(t, testSessionSize, uint64(n))
		n, err = io.ReadFull(r, p)
		require.NoError(t, err)
		require.Equal(t, testPayloadSize, uint64(n))
		require.NotPanics(t, func() {
			r.validatePayload()
		})
		require.True(t, bytes.Equal(sessionData, s))
		require.True(t, bytes.Equal(storeData, p))
	}()
	reportLeakedFD(fs, t)
}

func TestNormalSnapshotCanPassValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testNormalSnapshotCanPassValidation(t, V1, fs)
	testNormalSnapshotCanPassValidation(t, V2, fs)
}

func readTestSnapshot(fn string, sz uint64, fs vfs.IFS) ([]byte, error) {
	file, err := fs.Open(fn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	data := make([]byte, sz)
	n, err := file.Read(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func testSingleBlockSnapshotValidation(t *testing.T, sv SSVersion, fs vfs.IFS) {
	createTestSnapshotFile(t, sv, fs)
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	data, err := readTestSnapshot(testSnapshotFilename, 1024*1024, fs)
	require.NoError(t, err)
	v := NewSnapshotValidator()
	require.True(t, v.AddChunk(data, 0))
	require.True(t, v.Validate())
	require.False(t, v.AddChunk(data, 0))
	// intentionally corrupt the data
	data[len(data)-1] = data[len(data)-1] + 1
	v = NewSnapshotValidator()
	require.True(t, v.AddChunk(data, 0))
	require.False(t, v.Validate())
	reportLeakedFD(fs, t)
}

func TestSingleBlockSnapshotValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testSingleBlockSnapshotValidation(t, V1, fs)
	testSingleBlockSnapshotValidation(t, V2, fs)
}

func testMultiBlockSnapshotValidation(t *testing.T, sv SSVersion, fs vfs.IFS) {
	makeTestSnapshotFile(t, 1024*1024, 1024*1024*8, sv, fs)
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	data, err := readTestSnapshot(testSnapshotFilename, 1024*1024*10, fs)
	require.NoError(t, err)
	v := NewSnapshotValidator()
	c1 := data[:1024*1024]
	c2 := data[1024*1024:]
	require.True(t, v.AddChunk(c1, 0))
	require.True(t, v.AddChunk(c2, 1))
	require.True(t, v.Validate())
	v = NewSnapshotValidator()
	c2[len(c2)-1] = c2[len(c2)-1] + 1
	require.True(t, v.AddChunk(c1, 0))
	require.True(t, v.AddChunk(c2, 1))
	require.False(t, v.Validate())
	reportLeakedFD(fs, t)
}

func TestMultiBlockSnapshotValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testMultiBlockSnapshotValidation(t, V1, fs)
	testMultiBlockSnapshotValidation(t, V2, fs)
}

func TestMustInSameDir(t *testing.T) {
	fs := vfs.GetTestFS()
	tests := []struct {
		path1   string
		path2   string
		sameDir bool
	}{
		{"/d1/d2/f1.data", "/d1/d2/f2.data", true},
		{"/d1/d2/f1.data", "/d1/d3/f2.data", false},
		{"/d1/d2/f1.data", "/d1/d2/d3/f2.data", false},
		{"/d1/d2/f1.data", "f2.data", false},
		{"/d1/d2/f1.data", "/d2/f2.data", false},
		{"/d1/d2/f1.data", "d2/f2.data", false},
	}
	for _, tt := range tests {
		func() {
			if tt.sameDir {
				require.NotPanics(t, func() {
					mustInSameDir(tt.path1, tt.path2, fs)
				})
			} else {
				require.Panics(t, func() {
					mustInSameDir(tt.path1, tt.path2, fs)
				})
			}
		}()
	}
}

func TestShrinkSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	snapshotFilename := "test_snapshot_safe_to_delete.data"
	shrunkFilename := "test_snapshot_safe_to_delete.shrunk"
	writer, err := NewSnapshotWriter(snapshotFilename, pb.NoCompression, fs)
	require.NoError(t, err)
	defer func() {
		if err := fs.RemoveAll(snapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, uint64(0))
	_, err = writer.Write(sz)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		data := make([]byte, 1024*1024+i*256)
		_, err := rand.Read(data)
		require.NoError(t, err)
		_, err = writer.Write(data)
		require.NoError(t, err)
	}
	err = writer.Close()
	require.NoError(t, err)
	shrunk, err := IsShrunkSnapshotFile(snapshotFilename, fs)
	require.NoError(t, err)
	require.False(t, shrunk)
	err = ShrinkSnapshot(snapshotFilename, shrunkFilename, fs)
	require.NoError(t, err)
	defer func() {
		if err := fs.RemoveAll(shrunkFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	shrunk, err = IsShrunkSnapshotFile(snapshotFilename, fs)
	require.NoError(t, err)
	require.False(t, shrunk)
	shrunk, err = IsShrunkSnapshotFile(shrunkFilename, fs)
	require.NoError(t, err)
	require.True(t, shrunk)
	fi, err := fs.Stat(shrunkFilename)
	require.NoError(t, err)
	require.Equal(t, int64(1060), fi.Size())
	reader, _, err := NewSnapshotReader(shrunkFilename, fs)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)
	reportLeakedFD(fs, t)
}

func TestReplaceSnapshotFile(t *testing.T) {
	fs := vfs.GetTestFS()
	f1name := "test_snapshot_safe_to_delete.data"
	f2name := "test_snapshot_safe_to_delete.data2"
	createFile := func(fn string, sz uint64) {
		f1, err := fs.Create(fn)
		require.NoError(t, err)
		data := make([]byte, sz)
		_, err = rand.Read(data)
		require.NoError(t, err)
		_, err = f1.Write(data)
		require.NoError(t, err)
		err = f1.Close()
		require.NoError(t, err)
	}
	createFile(f1name, 1024)
	createFile(f2name, 2048)
	defer func() {
		if err := fs.RemoveAll(f1name); err != nil {
			t.Fatalf("%v", err)
		}
		if err := fs.RemoveAll(f2name); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	err := ReplaceSnapshot(f2name, f1name, fs)
	require.NoError(t, err)
	_, err = fs.Stat(f2name)
	require.Error(t, err)
	fi, err := fs.Stat(f1name)
	require.NoError(t, err)
	require.Equal(t, int64(2048), fi.Size())
}

func testV2PayloadChecksumCanBeRead(t *testing.T, sz uint64, fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		makeTestSnapshotFile(t, 0, sz, V2, fs)
		reader, header, err := NewSnapshotReader(testSnapshotFilename, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, reader.Close())
		}()
		crc, err := GetV2PayloadChecksum(testSnapshotFilename, fs)
		require.NoError(t, err)
		require.True(t, bytes.Equal(crc, header.PayloadChecksum))
	}()
	reportLeakedFD(fs, t)
}

func TestV2PayloadChecksumCanBeRead(t *testing.T) {
	fs := vfs.GetTestFS()
	testV2PayloadChecksumCanBeRead(t, blockSize-1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize+1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3-1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3+1, fs)
}

func TestV1SnapshotCanBeLoaded(t *testing.T) {
	// rsm: idList sz 2
	// rsm: client id 15771809973567514624, responded to 4, map[5:128]
	// rsm: client id 6760681031265190231, responded to 2, map[3:128]
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("skipped, the fs can not access the testdata")
	}
	fp := fs.PathJoin("testdata", "v1snapshot.gbsnap")
	reader, header, err := NewSnapshotReader(fp, fs)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()
	require.Equal(t, uint64(1), header.Version)
	v := (SSVersion)(header.Version)
	sm := NewSessionManager()
	err = sm.LoadSessions(reader, v)
	require.NoError(t, err)
	sessions := sm.lru
	s1, ok := sessions.getSession(15771809973567514624)
	require.True(t, ok)
	s2, ok := sessions.getSession(6760681031265190231)
	require.True(t, ok)
	require.Equal(t, RaftClientID(15771809973567514624), s1.ClientID)
	require.Equal(t, RaftSeriesID(4), s1.RespondedUpTo)
	require.Equal(t, RaftClientID(6760681031265190231), s2.ClientID)
	require.Equal(t, RaftSeriesID(2), s2.RespondedUpTo)
	r1, ok := s1.History[5]
	require.True(t, ok)
	require.Equal(t, uint64(128), r1.Value)
	require.Equal(t, 1, len(s1.History))
	r2, ok := s2.History[3]
	require.True(t, ok)
	require.Equal(t, uint64(128), r2.Value)
	require.Equal(t, 1, len(s2.History))
	data := make([]byte, 11)
	_, err = io.ReadFull(reader, data)
	require.NoError(t, err)
	require.Equal(t, "random-data", string(data))
}

func TestValidateHeader(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	require.True(t, validateHeader(data, fourZeroBytes))
	h := newCRC32Hash()
	_, err := h.Write(data)
	require.NoError(t, err)
	crc := h.Sum(nil)
	require.True(t, validateHeader(data, crc))
	crc[0] = crc[0] + 1
	require.False(t, validateHeader(data, crc))
}

func TestGetWitnessSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	d, err := GetWitnessSnapshot(fs)
	require.NoError(t, err)
	data := GetEmptyLRUSession()
	expectedLen := HeaderSize + uint64(len(data)) + 20
	require.Equal(t, expectedLen, uint64(len(d)))
}
