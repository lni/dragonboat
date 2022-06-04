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
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
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
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(testSnapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer w.Close()
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
		if err != nil {
			t.Fatalf("failed to create snapshot writer %v", err)
		}
		sessionData := make([]byte, testSessionSize)
		storeData := make([]byte, testPayloadSize)
		rand.Read(sessionData)
		rand.Read(storeData)
		n, err := w.Write(sessionData)
		if err != nil || n != len(sessionData) {
			t.Fatalf("failed to write the session data")
		}
		m, err := w.Write(storeData)
		if err != nil || m != len(storeData) {
			t.Fatalf("failed to write the store data")
		}
		err = w.Close()
		if err != nil {
			t.Fatalf("%v", err)
		}
		r, header, err := NewSnapshotReader(testSnapshotFilename, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer r.Close()
		if SSVersion(header.Version) != DefaultVersion {
			t.Errorf("invalid version %d, want %d", header.Version, DefaultVersion)
		}
		if header.ChecksumType != DefaultChecksumType {
			t.Errorf("unexpected checksum type %d, want %d",
				header.ChecksumType, DefaultChecksumType)
		}
		storeChecksum := w.vw.GetPayloadSum()
		if !bytes.Equal(header.PayloadChecksum, storeChecksum) {
			t.Errorf("data store checksum mismatch")
		}
	}()
	reportLeakedFD(fs, t)
}

func makeTestSnapshotFile(t *testing.T, ssz uint64,
	psz uint64, v SSVersion, fs vfs.IFS) (*SnapshotWriter, []byte, []byte) {
	if err := fs.RemoveAll(testSnapshotFilename); err != nil {
		t.Fatalf("%v", err)
	}
	w, err := newVersionedSnapshotWriter(testSnapshotFilename, v, pb.NoCompression, fs)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	sessionData := make([]byte, ssz)
	storeData := make([]byte, psz)
	rand.Read(sessionData)
	rand.Read(storeData)
	n, err := w.Write(sessionData)
	if err != nil || n != len(sessionData) {
		t.Fatalf("failed to write the session data")
	}
	m, err := w.Write(storeData)
	if err != nil || m != len(storeData) {
		t.Fatalf("failed to write the store data")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	return w, sessionData, storeData
}

func corruptSnapshotPayload(t *testing.T, fs vfs.IFS) {
	tmpFp := "testsnapshot.writing"
	func() {
		f, err := fs.ReuseForWrite(testSnapshotFilename, tmpFp)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer f.Close()
		s := (testSessionSize + testPayloadSize) / 2
		data := make([]byte, 1)
		if _, err := f.ReadAt(data, int64(HeaderSize+s)); err != nil {
			t.Fatalf("%v", err)
		}
		data[0] = data[0] + 1
		if _, err := f.WriteAt(data, int64(HeaderSize+s)); err != nil {
			t.Fatalf("%v", err)
		}
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
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("validation error not reported")
			}
		}()
		r, header, err := NewSnapshotReader(testSnapshotFilename, fs)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer r.Close()
		rand.Read(header.PayloadChecksum)
		s := make([]byte, testSessionSize)
		p := make([]byte, testPayloadSize)
		n, err := io.ReadFull(r, s)
		if uint64(n) != testSessionSize || err != nil {
			t.Fatalf("failed to get session data %d, %d, %v",
				uint64(n), testSessionSize, err)
		}
		n, err = io.ReadFull(r, p)
		if uint64(n) != testPayloadSize || err != nil {
			t.Fatalf("failed to get payload data")
		}
		r.validatePayload()
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
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer r.Close()
		s := make([]byte, testSessionSize)
		p := make([]byte, testPayloadSize)
		n, err := io.ReadFull(r, s)
		if uint64(n) != testSessionSize || err != nil {
			t.Fatalf("failed to get session data")
		}
		n, err = io.ReadFull(r, p)
		if uint64(n) != testPayloadSize || err != nil {
			t.Fatalf("failed to get payload data")
		}
		r.validatePayload()
		if !bytes.Equal(sessionData, s) {
			t.Errorf("session data changed")
		}
		if !bytes.Equal(storeData, p) {
			t.Errorf("store data changed")
		}
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
	defer file.Close()
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
	if err != nil {
		t.Fatalf("failed to get snapshot data %v", err)
	}
	v := NewSnapshotValidator()
	if !v.AddChunk(data, 0) {
		t.Fatalf("failed to add chunk")
	}
	if !v.Validate() {
		t.Fatalf("validation failed")
	}
	if v.AddChunk(data, 0) {
		t.Fatalf("adding the first chunk again didn't fail")
	}
	// intentionally corrupt the data
	data[len(data)-1] = data[len(data)-1] + 1
	v = NewSnapshotValidator()
	if !v.AddChunk(data, 0) {
		t.Fatalf("failed to add chunk")
	}
	if v.Validate() {
		t.Fatalf("validation failed to picked up corrupted data")
	}
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
	if err != nil {
		t.Fatalf("failed to get snapshot data %v", err)
	}
	v := NewSnapshotValidator()
	c1 := data[:1024*1024]
	c2 := data[1024*1024:]
	if !v.AddChunk(c1, 0) {
		t.Fatalf("failed to add chunk")
	}
	if !v.AddChunk(c2, 1) {
		t.Fatalf("failed to add chunk")
	}
	if !v.Validate() {
		t.Fatalf("validation failed")
	}
	v = NewSnapshotValidator()
	c2[len(c2)-1] = c2[len(c2)-1] + 1
	if !v.AddChunk(c1, 0) {
		t.Fatalf("failed to add chunk")
	}
	if !v.AddChunk(c2, 1) {
		t.Fatalf("failed to add chunk")
	}
	if v.Validate() {
		t.Fatalf("validation failed to pick up the corrupted snapshot")
	}
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
	for idx, tt := range tests {
		func() {
			defer func() {
				r := recover()
				if !tt.sameDir && r == nil {
					t.Errorf("%d, failed to detect not same dir", idx)
				}
			}()
			mustInSameDir(tt.path1, tt.path2, fs)
		}()
	}
}

func TestShrinkSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	snapshotFilename := "test_snapshot_safe_to_delete.data"
	shrunkFilename := "test_snapshot_safe_to_delete.shrunk"
	writer, err := NewSnapshotWriter(snapshotFilename, pb.NoCompression, fs)
	if err != nil {
		t.Fatalf("failed to get writer %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(snapshotFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, uint64(0))
	if _, err := writer.Write(sz); err != nil {
		t.Fatalf("failed to write session size %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, 1024*1024+i*256)
		rand.Read(data)
		if _, err := writer.Write(data); err != nil {
			t.Fatalf("write failed %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed %v", err)
	}
	shrunk, err := IsShrunkSnapshotFile(snapshotFilename, fs)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrunk %v", err)
	}
	if shrunk {
		t.Errorf("incorrectly reported as shrunk")
	}
	if err := ShrinkSnapshot(snapshotFilename, shrunkFilename, fs); err != nil {
		t.Errorf("failed to shrink snapshot %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(shrunkFilename); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	shrunk, err = IsShrunkSnapshotFile(snapshotFilename, fs)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrunk %v", err)
	}
	if shrunk {
		t.Errorf("incorrectly reported as shrunk")
	}
	shrunk, err = IsShrunkSnapshotFile(shrunkFilename, fs)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrunk %v", err)
	}
	if !shrunk {
		t.Errorf("not shrunk")
	}
	fi, err := fs.Stat(shrunkFilename)
	if err != nil {
		t.Fatalf("failed to get file stat")
	}
	if fi.Size() != 1060 {
		t.Errorf("not shrunk according to file size")
	}
	reader, _, err := NewSnapshotReader(shrunkFilename, fs)
	if err != nil {
		t.Fatalf("failed to create snapshot reader %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("failed to close the reader %v", err)
	}
	reportLeakedFD(fs, t)
}

func TestReplaceSnapshotFile(t *testing.T) {
	fs := vfs.GetTestFS()
	f1name := "test_snapshot_safe_to_delete.data"
	f2name := "test_snapshot_safe_to_delete.data2"
	createFile := func(fn string, sz uint64) {
		f1, err := fs.Create(fn)
		if err != nil {
			t.Fatalf("failed to ")
		}
		data := make([]byte, sz)
		rand.Read(data)
		if _, err := f1.Write(data); err != nil {
			t.Fatalf("failed to write data %v", err)
		}
		if err := f1.Close(); err != nil {
			t.Fatalf("failed to close %v", err)
		}
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
	if err := ReplaceSnapshot(f2name, f1name, fs); err != nil {
		t.Fatalf("failed to replace file %v", err)
	}
	_, err := fs.Stat(f2name)
	if err == nil {
		t.Errorf("f2 still exist")
	}
	fi, err := fs.Stat(f1name)
	if err != nil {
		t.Errorf("failed to get file info %v", err)
	}
	if fi.Size() != 2048 {
		t.Errorf("file not replaced")
	}
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
		if err != nil {
			t.Fatalf("failed to create reader %v", err)
		}
		defer func() {
			reader.Close()
		}()
		crc, err := GetV2PayloadChecksum(testSnapshotFilename, fs)
		if err != nil {
			t.Fatalf("failed to get v2 payload checksum %v", err)
		}
		if !bytes.Equal(crc, header.PayloadChecksum) {
			t.Fatalf("crc changed")
		}
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
	if err != nil {
		t.Fatalf("failed to get reader %v", err)
	}
	defer reader.Close()
	if header.Version != 1 {
		t.Fatalf("not a version 1 snapshot file")
	}
	v := (SSVersion)(header.Version)
	sm := NewSessionManager()
	if err := sm.LoadSessions(reader, v); err != nil {
		t.Fatalf("failed to load sessions %v", err)
	}
	sessions := sm.lru
	s1, ok := sessions.getSession(15771809973567514624)
	if !ok {
		t.Fatalf("failed to get session")
	}
	s2, ok := sessions.getSession(6760681031265190231)
	if !ok {
		t.Fatalf("failed to get session")
	}
	if s1.ClientID != 15771809973567514624 || s1.RespondedUpTo != 4 {
		t.Errorf("invalid content")
	}
	if s2.ClientID != 6760681031265190231 || s2.RespondedUpTo != 2 {
		t.Errorf("invalid content")
	}
	r1, ok := s1.History[5]
	if !ok || r1.Value != 128 || len(s1.History) != 1 {
		t.Errorf("unexpected history")
	}
	r2, ok := s2.History[3]
	if !ok || r2.Value != 128 || len(s2.History) != 1 {
		t.Errorf("unexpected history")
	}
	data := make([]byte, 11)
	if _, err := io.ReadFull(reader, data); err != nil {
		t.Fatalf("failed to get snapshot content")
	}
	if string(data) != "random-data" {
		t.Errorf("unexpected content")
	}
}

func TestValidateHeader(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	if !validateHeader(data, fourZeroBytes) {
		t.Errorf("not skipped")
	}
	h := newCRC32Hash()
	if _, err := h.Write(data); err != nil {
		t.Fatalf("write failed %v", err)
	}
	crc := h.Sum(nil)
	if !validateHeader(data, crc) {
		t.Errorf("expected to report valid")
	}
	crc[0] = crc[0] + 1
	if validateHeader(data, crc) {
		t.Errorf("corrupted header data not reported")
	}
}

func TestGetWitnessSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	if d, err := GetWitnessSnapshot(fs); err != nil {
		t.Fatalf("failed to get witness snapshot, %v", err)
	} else {
		data := GetEmptyLRUSession()
		if uint64(len(d)) != HeaderSize+uint64(len(data))+20 {
			t.Errorf("unexpected length, %d, data len %d", len(d), len(data))
		}
	}
}
