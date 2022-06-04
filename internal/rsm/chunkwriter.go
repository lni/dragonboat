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
	"encoding/binary"
	"io"
	"time"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const (
	// ChunkSize is the size of each snapshot chunk.
	ChunkSize = settings.SnapshotChunkSize
)

var _ io.WriteCloser = (*ChunkWriter)(nil)

// ChunkWriter is an io.WriteCloser type that streams snapshot chunks to its
// intended remote nodes.
type ChunkWriter struct {
	sink    pb.IChunkSink
	bw      IBlockWriter
	meta    SSMeta
	chunkID uint64
	failed  bool
	stopped bool
}

// NewChunkWriter creates and returns a chunk writer instance.
func NewChunkWriter(sink pb.IChunkSink, meta SSMeta) *ChunkWriter {
	cw := &ChunkWriter{
		sink: sink,
		meta: meta,
	}
	cw.bw = NewBlockWriter(ChunkSize, cw.onNewBlock, DefaultChecksumType)
	return cw
}

// Close closes the chunk writer.
func (cw *ChunkWriter) Close() error {
	if err := cw.bw.Close(); err != nil {
		return err
	}
	if !cw.failed {
		if err := cw.onNewChunk(cw.getTailChunk()); err != nil {
			return err
		}
	}
	return cw.sink.Close()
}

// Write writes the specified input data.
func (cw *ChunkWriter) Write(data []byte) (int, error) {
	if cw.stopped {
		return 0, sm.ErrSnapshotStopped
	}
	if cw.failed {
		return 0, sm.ErrSnapshotStreaming
	}
	return cw.bw.Write(data)
}

func (cw *ChunkWriter) onNewBlock(data []byte, crc []byte) error {
	defer func() {
		cw.chunkID = cw.chunkID + 1
	}()
	chunk := cw.getChunk()
	var payload []byte
	if cw.chunkID == 0 {
		payload = cw.getHeader()
	} else {
		payload = make([]byte, 0, len(data)+len(crc))
	}
	payload = append(payload, data...)
	payload = append(payload, crc...)
	chunk.Data = payload
	chunk.ChunkSize = uint64(len(payload))
	return cw.onNewChunk(chunk)
}

func (cw *ChunkWriter) onNewChunk(chunk pb.Chunk) error {
	sent, stopped := cw.sink.Receive(chunk)
	if stopped {
		cw.stopped = true
		return sm.ErrSnapshotStopped
	}
	if !sent {
		cw.failed = true
		return sm.ErrSnapshotStreaming
	}
	return nil
}

func (cw *ChunkWriter) getHeader() []byte {
	header := pb.SnapshotHeader{
		SessionSize:     0,
		DataStoreSize:   0,
		UnreliableTime:  uint64(time.Now().UnixNano()),
		PayloadChecksum: []byte{0, 0, 0, 0},
		ChecksumType:    DefaultChecksumType,
		Version:         uint64(V2),
		CompressionType: cw.meta.CompressionType,
	}
	data := pb.MustMarshal(&header)
	h := newCRC32Hash()
	fileutil.MustWrite(h, data)
	checksum := h.Sum(nil)
	result := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint64(result, uint64(len(data)))
	copy(result[8:], data)
	copy(result[8+len(data):], checksum)
	return result
}

func (cw *ChunkWriter) getChunk() pb.Chunk {
	return pb.Chunk{
		ShardID:     cw.sink.ShardID(),
		ReplicaID:   cw.sink.ToReplicaID(),
		From:        cw.meta.From,
		ChunkId:     cw.chunkID,
		FileChunkId: cw.chunkID,
		Index:       cw.meta.Index,
		Term:        cw.meta.Term,
		OnDiskIndex: cw.meta.OnDiskIndex,
		Membership:  cw.meta.Membership,
		BinVer:      raftio.TransportBinVersion,
		Filepath:    server.GetSnapshotFilename(cw.meta.Index),
	}
}

func (cw *ChunkWriter) getTailChunk() pb.Chunk {
	tailChunk := cw.getChunk()
	tailChunk.ChunkCount = pb.LastChunkCount
	tailChunk.FileChunkCount = pb.LastChunkCount
	return tailChunk
}
