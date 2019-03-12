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
	"io"
	"math/rand"
	"os"
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
	storeChecksum := w.vw.GetPayloadSum()
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
	err = w.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	return w, sessionData, storeData
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
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("validation error not reported")
		}
	}()
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
