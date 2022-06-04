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
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type testSink struct {
	chunks     []pb.Chunk
	sendFailed bool
	stopped    bool
}

func (s *testSink) Receive(chunk pb.Chunk) (bool, bool) {
	if s.sendFailed || s.stopped {
		return !s.sendFailed, s.stopped
	}
	s.chunks = append(s.chunks, chunk)
	return true, false
}

func (s *testSink) Close() error {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
	return nil
}

func (s *testSink) ShardID() uint64 {
	return 2000
}

func (s *testSink) ToReplicaID() uint64 {
	return 300
}

func getTestSSMeta() SSMeta {
	return SSMeta{
		Index: 1000,
		Term:  5,
		From:  150,
	}
}

func TestChunkWriterCanBeWritten(t *testing.T) {
	meta := getTestSSMeta()
	cw := NewChunkWriter(&testSink{}, meta)
	for i := 0; i < 10; i++ {
		data := make([]byte, ChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	if err := cw.Close(); err != nil {
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
		} else if idx == 11 {
			if chunk.ChunkCount != pb.LastChunkCount {
				t.Errorf("last chunk not marked, %d", idx)
			}
			if chunk.FileChunkCount != pb.LastChunkCount {
				t.Errorf("last chunk not marked, %d", idx)
			}
		} else if idx == 12 {
		} else {
			if chunk.ChunkCount != 0 {
				t.Errorf("unexpectedly marked as last chunk, %d", idx)
			}
		}
	}
}

func TestChunkWriterCanFailWrite(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	for i := 0; i < 10; i++ {
		data := make([]byte, ChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	sink.sendFailed = true
	data := make([]byte, ChunkSize)
	_, err := cw.Write(data)
	if err == nil {
		t.Fatalf("writer didn't fail")
	}
	if err != sm.ErrSnapshotStreaming {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestChunkWriterCanBeStopped(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	for i := 0; i < 10; i++ {
		data := make([]byte, ChunkSize)
		if _, err := cw.Write(data); err != nil {
			t.Fatalf("failed to write the data %v", err)
		}
	}
	sink.stopped = true
	data := make([]byte, ChunkSize)
	_, err := cw.Write(data)
	if err == nil {
		t.Fatalf("writer didn't fail")
	}
	if err != sm.ErrSnapshotStopped {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestGetTailChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	chunk := cw.getTailChunk()
	if chunk.ChunkCount != pb.LastChunkCount {
		t.Errorf("chunk count %d, want %d",
			chunk.ChunkCount, pb.LastChunkCount)
	}
	if chunk.FileChunkCount != pb.LastChunkCount {
		t.Errorf("file chunk count %d, want %d",
			chunk.FileChunkCount, pb.LastChunkCount)
	}
}

func TestCloseChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	cw.Close()
	chunk := sink.chunks[len(sink.chunks)-1]
	if chunk.ChunkCount != pb.LastChunkCount-1 {
		t.Errorf("chunk count %d, want %d",
			chunk.ChunkCount, pb.LastChunkCount-1)
	}
}

func TestFailedChunkWriterWillNotSendTheTailChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	cw.failed = true
	cw.Close()
	for idx, chunk := range cw.sink.(*testSink).chunks {
		if idx == 0 {
			sz := binary.LittleEndian.Uint64(chunk.Data)
			headerdata := chunk.Data[8 : 8+sz]
			crc := chunk.Data[8+sz : 12+sz]
			var header pb.SnapshotHeader
			if err := header.Unmarshal(headerdata); err != nil {
				t.Fatalf("%v", err)
			}
			if uint64(len(chunk.Data)) != HeaderSize+tailSize {
				t.Errorf("unexpected data size")
			}
			checksum := header.HeaderChecksum
			if checksum != nil {
				t.Errorf("not expected to set the checksum field")
			}
			h := newCRC32Hash()
			if _, err := h.Write(headerdata); err != nil {
				panic(err)
			}
			if !bytes.Equal(h.Sum(nil), crc) {
				t.Fatalf("not a valid header")
			}
		} else if idx == 1 {
			if chunk.ChunkCount != pb.PoisonChunkCount {
				t.Fatalf("unexpected chunk count")
			}
		} else {
			t.Fatalf("unexpected chunk received")
		}
	}
}
