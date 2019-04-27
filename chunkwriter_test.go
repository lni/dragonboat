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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/transport"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

type testSink struct {
	chunks     []pb.SnapshotChunk
	sendFailed bool
	stopped    bool
}

func (s *testSink) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	if s.sendFailed || s.stopped {
		return !s.sendFailed, s.stopped
	}
	s.chunks = append(s.chunks, chunk)
	return true, false
}

func (s *testSink) ClusterID() uint64 {
	return 2000
}

func (s *testSink) ToNodeID() uint64 {
	return 300
}

func getTestSnapshotMeta() *rsm.SnapshotMeta {
	return &rsm.SnapshotMeta{
		Index: 1000,
		Term:  5,
		From:  150,
	}
}

func TestChunkWriterCanBeWritten(t *testing.T) {
	meta := getTestSnapshotMeta()
	cw := newChunkWriter(&testSink{}, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send empty LRU session %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, SnapshotChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Flush(); err != nil {
		t.Fatalf("failed to flush %v", err)
	}
	if len(cw.sink.(*testSink).chunks) != 13 {
		t.Errorf("chunks count %d, want 13", len(cw.sink.(*testSink).chunks))
	}
	for idx, chunk := range cw.sink.(*testSink).chunks {
		if idx == 0 {
			sz := binary.LittleEndian.Uint64(chunk.Data)
			headerData := chunk.Data[8 : 8+sz]
			var header pb.SnapshotHeader
			if err := header.Unmarshal(headerData); err != nil {
				t.Fatalf("failed to unmarshal %v", err)
			}
		} else if idx == 12 {
			if chunk.ChunkCount != transport.LastChunkCount {
				t.Errorf("last chunk not marked")
			}
			if chunk.FileChunkCount != transport.LastChunkCount {
				t.Errorf("last chunk not marked")
			}
		} else {
			if chunk.ChunkCount != 0 {
				t.Errorf("unexpectedly marked last chunk")
			}
		}
	}
}

func TestChunkWriterCanFailWrite(t *testing.T) {
	meta := getTestSnapshotMeta()
	sink := &testSink{}
	cw := newChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send empty LRU session %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, SnapshotChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	sink.sendFailed = true
	data := make([]byte, SnapshotChunkSize)
	_, err = cw.Write(data)
	if err == nil {
		t.Fatalf("writer didn't fail")
	}
	if err != sm.ErrSnapshotStreaming {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestChunkWriterCanBeStopped(t *testing.T) {
	meta := getTestSnapshotMeta()
	sink := &testSink{}
	cw := newChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send empty LRU session %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, SnapshotChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	sink.stopped = true
	data := make([]byte, SnapshotChunkSize)
	_, err = cw.Write(data)
	if err == nil {
		t.Fatalf("writer didn't fail")
	}
	if err != sm.ErrSnapshotStopped {
		t.Fatalf("unexpected err %v", err)
	}
}

type chunkReceiver interface {
	AddChunk(chunk pb.SnapshotChunk) bool
}

type chunks struct {
	received  uint64
	confirmed uint64
}

var (
	testSnapshotDir = "test_snapshot_dir_safe_to_delete"
)

func (c *chunks) onReceive(pb.MessageBatch) {
	c.received++
}

func (c *chunks) confirm(clusterID uint64, nodeID uint64, index uint64) {
	c.confirmed++
}

func (c *chunks) getDeploymentID() uint64 {
	return 0
}

func (c *chunks) getSnapshotDirFunc(clusterID uint64, nodeID uint64) string {
	return testSnapshotDir
}

type testSink2 struct {
	receiver chunkReceiver
}

func (s *testSink2) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	s.receiver.AddChunk(chunk)
	return true, false
}

func (s *testSink2) ClusterID() uint64 {
	return 2000
}

func (s *testSink2) ToNodeID() uint64 {
	return 300
}

func TestChunkWriterOutputCanBeHandledByChunks(t *testing.T) {
	os.RemoveAll(testSnapshotDir)
	c := &chunks{}
	chunks := transport.NewSnapshotChunks(c.onReceive,
		c.confirm, c.getDeploymentID, c.getSnapshotDirFunc)
	sink := &testSink2{receiver: chunks}
	meta := getTestSnapshotMeta()
	cw := newChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send LRU session %v", err)
	}
	defer os.RemoveAll(testSnapshotDir)
	payload := make([]byte, 0)
	payload = append(payload, rsm.GetEmptyLRUSession()...)
	for i := 0; i < 10; i++ {
		data := make([]byte, SnapshotChunkSize)
		rand.Read(data)
		payload = append(payload, data...)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Flush(); err != nil {
		t.Fatalf("failed to flush %v", err)
	}
	if c.received != 1 {
		t.Fatalf("failed to receive the snapshot")
	}
	if c.confirmed != 1 {
		t.Fatalf("failed to confirm")
	}
	fp := path.Join(testSnapshotDir,
		"snapshot-00000000000003E8", "snapshot-00000000000003E8.gbsnap")
	reader, err := rsm.NewSnapshotReader(fp)
	if err != nil {
		t.Fatalf("failed to get a snapshot reader %v", err)
	}
	if _, err = reader.GetHeader(); err != nil {
		t.Fatalf("failed to get header %v", err)
	}
	got := make([]byte, 0)
	buf := make([]byte, 1024*256)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("snapshot content changed")
	}
}

func TestGetTailChunk(t *testing.T) {
	meta := getTestSnapshotMeta()
	sink := &testSink{}
	cw := newChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send LRU session %v", err)
	}
	chunk := cw.getTailChunk()
	if chunk.ChunkCount != transport.LastChunkCount {
		t.Errorf("chunk count %d, want %d",
			chunk.ChunkCount, transport.LastChunkCount)
	}
	if chunk.FileChunkCount != transport.LastChunkCount {
		t.Errorf("file chunk count %d, want %d",
			chunk.FileChunkCount, transport.LastChunkCount)
	}
}

func TestFailChunk(t *testing.T) {
	meta := getTestSnapshotMeta()
	sink := &testSink{}
	cw := newChunkWriter(sink, meta)
	_, err := cw.Write(rsm.GetEmptyLRUSession())
	if err != nil {
		t.Fatalf("failed to send LRU session %v", err)
	}
	cw.Fail()
	chunk := sink.chunks[0]
	if chunk.ChunkCount != transport.LastChunkCount-1 {
		t.Errorf("chunk count %d, want %d",
			chunk.ChunkCount, transport.LastChunkCount-1)
	}
}
