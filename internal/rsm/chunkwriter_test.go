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
	"github.com/stretchr/testify/require"
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
		_, err := cw.Write(data)
		require.NoError(t, err, "failed to write the data")
	}
	err := cw.Close()
	require.NoError(t, err, "failed to flush")

	chunks := cw.sink.(*testSink).chunks
	require.Equal(t, 13, len(chunks), "chunks count mismatch")

	for idx, chunk := range chunks {
		switch idx {
		case 0:
			sz := binary.LittleEndian.Uint64(chunk.Data)
			headerData := chunk.Data[8 : 8+sz]
			var header pb.SnapshotHeader
			err := header.Unmarshal(headerData)
			require.NoError(t, err, "failed to unmarshal")
		case 11:
			require.Equal(t, pb.LastChunkCount, chunk.ChunkCount,
				"last chunk not marked, %d", idx)
			require.Equal(t, pb.LastChunkCount, chunk.FileChunkCount,
				"last chunk not marked, %d", idx)
		case 12:
		default:
			require.Equal(t, uint64(0), chunk.ChunkCount,
				"unexpectedly marked as last chunk, %d", idx)
		}
	}
}

func TestChunkWriterCanFailWrite(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	for i := 0; i < 10; i++ {
		data := make([]byte, ChunkSize)
		_, err := cw.Write(data)
		require.NoError(t, err, "failed to write the data")
	}
	sink.sendFailed = true
	data := make([]byte, ChunkSize)
	_, err := cw.Write(data)
	require.Error(t, err, "writer didn't fail")
	require.Equal(t, sm.ErrSnapshotStreaming, err, "unexpected err")
}

func TestChunkWriterCanBeStopped(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	for i := 0; i < 10; i++ {
		data := make([]byte, ChunkSize)
		_, err := cw.Write(data)
		require.NoError(t, err, "failed to write the data")
	}
	sink.stopped = true
	data := make([]byte, ChunkSize)
	_, err := cw.Write(data)
	require.Error(t, err, "writer didn't fail")
	require.Equal(t, sm.ErrSnapshotStopped, err, "unexpected err")
}

func TestGetTailChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	chunk := cw.getTailChunk()
	require.Equal(t, pb.LastChunkCount, chunk.ChunkCount,
		"chunk count mismatch")
	require.Equal(t, pb.LastChunkCount, chunk.FileChunkCount,
		"file chunk count mismatch")
}

func TestCloseChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	require.NoError(t, cw.Close())
	chunk := sink.chunks[len(sink.chunks)-1]
	require.Equal(t, pb.LastChunkCount-1, chunk.ChunkCount,
		"chunk count mismatch")
}

func TestFailedChunkWriterWillNotSendTheTailChunk(t *testing.T) {
	meta := getTestSSMeta()
	sink := &testSink{}
	cw := NewChunkWriter(sink, meta)
	cw.failed = true
	require.NoError(t, cw.Close())

	chunks := cw.sink.(*testSink).chunks
	for idx, chunk := range chunks {
		switch idx {
		case 0:
			sz := binary.LittleEndian.Uint64(chunk.Data)
			headerdata := chunk.Data[8 : 8+sz]
			crc := chunk.Data[8+sz : 12+sz]
			var header pb.SnapshotHeader
			err := header.Unmarshal(headerdata)
			require.NoError(t, err)

			expectedSize := HeaderSize + tailSize
			require.Equal(t, uint64(len(chunk.Data)), expectedSize,
				"unexpected data size")

			checksum := header.HeaderChecksum
			require.Nil(t, checksum, "not expected to set the checksum field")

			h := newCRC32Hash()
			_, err = h.Write(headerdata)
			require.NoError(t, err)
			require.True(t, bytes.Equal(h.Sum(nil), crc), "not a valid header")
		case 1:
			require.Equal(t, pb.PoisonChunkCount, chunk.ChunkCount,
				"unexpected chunk count")
		default:
			require.Fail(t, "unexpected chunk received")
		}
	}
}
