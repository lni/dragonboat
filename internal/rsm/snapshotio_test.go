// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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
	"os"
	"path/filepath"
	"testing"
)

const (
	testSessionSize      uint64 = 4
	testPayloadSize      uint64 = 8
	testSnapshotFilename        = "testsnapshot_safe_to_delete.tmp"
)

func TestSnapshotWriterCanBeCreated(t *testing.T) {
	w, err := NewSnapshotWriter(testSnapshotFilename, CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	defer os.RemoveAll(testSnapshotFilename)
	defer w.Close()
	pos, err := w.file.Seek(0, 1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if uint64(pos) != SnapshotHeaderSize {
		t.Errorf("unexpected file position")
	}
}

func TestSaveHeaderSavesTheHeader(t *testing.T) {
	w, err := NewSnapshotWriter(testSnapshotFilename, CurrentSnapshotVersion)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	defer os.RemoveAll(testSnapshotFilename)
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
	if err := w.Flush(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := w.SaveHeader(uint64(n), uint64(m)); err != nil {
		t.Fatalf("%v", err)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer r.Close()
	header, err := r.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r.ValidateHeader(header)
	if header.SessionSize != uint64(len(sessionData)) {
		t.Errorf("session data size mismatch")
	}
	if header.DataStoreSize != uint64(len(storeData)) {
		t.Errorf("data store size mismatch")
	}
	storeChecksum := w.vw.GetPayloadSum()
	if !bytes.Equal(header.PayloadChecksum, storeChecksum) {
		t.Errorf("data store checksum mismatch")
	}
}

func makeTestSnapshotFile(t *testing.T, ssz uint64,
	psz uint64, v SnapshotVersion) (*SnapshotWriter, []byte, []byte) {
	os.RemoveAll(testSnapshotFilename)
	w, err := NewSnapshotWriter(testSnapshotFilename, v)
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
	if err := w.Flush(); err != nil {
		t.Fatalf("flush failed")
	}
	if err := w.SaveHeader(uint64(n), uint64(m)); err != nil {
		t.Fatalf("%v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	return w, sessionData, storeData
}

func corruptSnapshotPayload(t *testing.T) {
	f, err := os.OpenFile(testSnapshotFilename, os.O_RDWR, 0755)
	if err != nil {
		t.Fatalf("%v", err)
	}
	s := (testSessionSize + testPayloadSize) / 2
	if _, err := f.Seek(int64(SnapshotHeaderSize+s), 0); err != nil {
		t.Fatalf("%v", err)
	}
	data := make([]byte, 1)
	if _, err := f.Read(data); err != nil {
		t.Fatalf("%v", err)
	}
	data[0] = byte(data[0] + 1)
	if _, err := f.Seek(int64(SnapshotHeaderSize+s), 0); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("%v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("%v", err)
	}
}

func createTestSnapshotFile(t *testing.T,
	v SnapshotVersion) (*SnapshotWriter, []byte, []byte) {
	return makeTestSnapshotFile(t, testSessionSize, testPayloadSize, v)
}

func testCorruptedHeaderWillBeDetected(t *testing.T, v SnapshotVersion) {
	createTestSnapshotFile(t, v)
	defer os.RemoveAll(testSnapshotFilename)
	r, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer r.Close()
	header, err := r.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	rand.Read(header.HeaderChecksum)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("validation error not reported")
		}
	}()
	r.ValidateHeader(header)
}

func TestCorruptedHeaderWillBeDetected(t *testing.T) {
	testCorruptedHeaderWillBeDetected(t, V1SnapshotVersion)
	testCorruptedHeaderWillBeDetected(t, V2SnapshotVersion)
}

func testCorruptedPayloadWillBeDetected(t *testing.T, v SnapshotVersion) {
	createTestSnapshotFile(t, v)
	corruptSnapshotPayload(t)
	defer os.RemoveAll(testSnapshotFilename)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("validation error not reported")
		}
	}()

	r, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer r.Close()
	header, err := r.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r.ValidateHeader(header)
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
	r.ValidatePayload(header)
}

func TestCorruptedPayloadWillBeDetected(t *testing.T) {
	testCorruptedPayloadWillBeDetected(t, V1SnapshotVersion)
	testCorruptedPayloadWillBeDetected(t, V2SnapshotVersion)
}

func testNormalSnapshotCanPassValidation(t *testing.T, v SnapshotVersion) {
	_, sessionData, storeData := createTestSnapshotFile(t, v)
	defer os.RemoveAll(testSnapshotFilename)
	r, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer r.Close()
	header, err := r.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r.ValidateHeader(header)
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
	r.ValidatePayload(header)
	if !bytes.Equal(sessionData, s) {
		t.Errorf("session data changed")
	}
	if !bytes.Equal(storeData, p) {
		t.Errorf("store data changed")
	}
}

func TestNormalSnapshotCanPassValidation(t *testing.T) {
	testNormalSnapshotCanPassValidation(t, V1SnapshotVersion)
	testNormalSnapshotCanPassValidation(t, V2SnapshotVersion)
}

func readTestSnapshot(fn string, sz uint64) ([]byte, error) {
	file, err := os.Open(fn)
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

func testSingleBlockSnapshotValidation(t *testing.T, sv SnapshotVersion) {
	createTestSnapshotFile(t, sv)
	defer os.RemoveAll(testSnapshotFilename)
	data, err := readTestSnapshot(testSnapshotFilename, 1024*1024)
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
	// intentionally corrupt the data
	data[len(data)-1] = data[len(data)-1] + 1
	v = NewSnapshotValidator()
	if !v.AddChunk(data, 0) {
		t.Fatalf("failed to add chunk")
	}
	if v.Validate() {
		t.Fatalf("validation failed to picked up corrupted data")
	}
}

func TestSingleBlockSnapshotValidation(t *testing.T) {
	testSingleBlockSnapshotValidation(t, V1SnapshotVersion)
	testSingleBlockSnapshotValidation(t, V2SnapshotVersion)
}

func testMultiBlockSnapshotValidation(t *testing.T, sv SnapshotVersion) {
	makeTestSnapshotFile(t, 1024*1024, 1024*1024*8, sv)
	defer os.RemoveAll(testSnapshotFilename)
	data, err := readTestSnapshot(testSnapshotFilename, 1024*1024*10)
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
}

func TestMultiBlockSnapshotValidation(t *testing.T) {
	testMultiBlockSnapshotValidation(t, V1SnapshotVersion)
	testMultiBlockSnapshotValidation(t, V2SnapshotVersion)
}

func TestMustInSameDir(t *testing.T) {
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
			mustInSameDir(tt.path1, tt.path2)
		}()
	}
}

func TestShrinkSnapshot(t *testing.T) {
	snapshotFilename := "test_snapshot_safe_to_delete.data"
	shrinkedFilename := "test_snapshot_safe_to_delete.shrinked"
	writer, err := NewSnapshotWriter(snapshotFilename, V2SnapshotVersion)
	if err != nil {
		t.Fatalf("failed to get writer %v", err)
	}
	defer func() {
		os.RemoveAll(snapshotFilename)
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
	if err := writer.Flush(); err != nil {
		t.Fatalf("failed to flush %v", err)
	}
	if err := writer.SaveHeader(0, 199); err != nil {
		t.Fatalf("failed to write the header %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed %v", err)
	}
	shrinked, err := IsShrinkedSnapshotFile(snapshotFilename)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrinked %v", err)
	}
	if shrinked {
		t.Errorf("incorrectly reported as shrinked")
	}
	if err := ShrinkSnapshot(snapshotFilename, shrinkedFilename); err != nil {
		t.Errorf("failed to shrink snapshot %v", err)
	}
	defer func() {
		os.RemoveAll(shrinkedFilename)
	}()
	shrinked, err = IsShrinkedSnapshotFile(snapshotFilename)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrinked %v", err)
	}
	if shrinked {
		t.Errorf("incorrectly reported as shrinked")
	}
	shrinked, err = IsShrinkedSnapshotFile(shrinkedFilename)
	if err != nil {
		t.Fatalf("failed to check whether snapshot file is shrinked %v", err)
	}
	if !shrinked {
		t.Errorf("not shrinked")
	}
	fi, err := os.Stat(shrinkedFilename)
	if err != nil {
		t.Fatalf("failed to get file stat")
	}
	if fi.Size() != 1060 {
		t.Errorf("not shrinked according to file size")
	}
	reader, err := NewSnapshotReader(shrinkedFilename)
	if err != nil {
		t.Fatalf("failed to create snapshot reader %v", err)
	}
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get header %v", err)
	}
	if header.DataStoreSize != 0 || header.SessionSize != EmptyClientSessionLength {
		t.Fatalf("unexpected header value")
	}
}

func TestReplaceSnapshotFile(t *testing.T) {
	f1name := "test_snapshot_safe_to_delete.data"
	f2name := "test_snapshot_safe_to_delete.data2"
	createFile := func(fn string, sz uint64) {
		f1, err := os.Create(fn)
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
		os.RemoveAll(f1name)
		os.RemoveAll(f2name)
	}()
	if err := ReplaceSnapshotFile(f2name, f1name); err != nil {
		t.Fatalf("failed to replace file %v", err)
	}
	fi, err := os.Stat(f2name)
	if err == nil {
		t.Errorf("f2 still exist")
	}
	fi, err = os.Stat(f1name)
	if err != nil {
		t.Errorf("failed to get file info %v", err)
	}
	if fi.Size() != 2048 {
		t.Errorf("file not replaced")
	}
}

func testV2PayloadChecksumCanBeRead(t *testing.T, sz uint64) {
	makeTestSnapshotFile(t, 0, sz, V2SnapshotVersion)
	defer os.RemoveAll(testSnapshotFilename)
	reader, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("failed to create reader %v", err)
	}
	defer func() {
		reader.Close()
	}()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get header")
	}
	crc, err := GetV2PayloadChecksum(testSnapshotFilename)
	if err != nil {
		t.Fatalf("failed to get v2 payload checksum %v", err)
	}
	if !bytes.Equal(crc, header.PayloadChecksum) {
		t.Fatalf("crc changed")
	}
}

func TestV2PayloadChecksumCanBeRead(t *testing.T) {
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize-1)
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize)
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize+1)
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize*3-1)
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize*3)
	testV2PayloadChecksumCanBeRead(t, snapshotBlockSize*3+1)
}

func TestV1SnapshotCanBeLoaded(t *testing.T) {
	// rsm: idList sz 2
	// rsm: client id 15771809973567514624, responded to 4, map[5:128]
	// rsm: client id 6760681031265190231, responded to 2, map[3:128]
	fp := filepath.Join("testdata", "v1snapshot.gbsnap")
	reader, err := NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to get reader %v", err)
	}
	defer reader.Close()
	header, err := reader.GetHeader()
	if err != nil {
		t.Fatalf("failed to get header %v", err)
	}
	reader.ValidateHeader(header)
	if header.Version != 1 {
		t.Fatalf("not a version 1 snapshot file")
	}
	v := (SnapshotVersion)(header.Version)
	sm := NewSessionManager()
	if err := sm.LoadSessions(reader, v); err != nil {
		t.Fatalf("failed to load sessions %v", err)
	}
	sessions := sm.sessions
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
	reader.ValidatePayload(header)
}
