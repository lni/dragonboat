// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
	"time"

	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	// ChunkSize is the size of each snapshot chunk.
	ChunkSize = settings.SnapshotChunkSize
)

// ChunkWriter is an io.WriteCloser type that streams snapshot chunks to its
// intended remote nodes.
type ChunkWriter struct {
	failed  bool
	stopped bool
	chunkID uint64
	first   bool
	sink    pb.IChunkSink
	bw      IBlockWriter
	meta    *SnapshotMeta
}

// NewChunkWriter creates and returns a chunk writer instance.
func NewChunkWriter(sink pb.IChunkSink, meta *SnapshotMeta) *ChunkWriter {
	cw := &ChunkWriter{
		sink:  sink,
		meta:  meta,
		first: true,
	}
	cw.bw = NewBlockWriter(ChunkSize, cw.onNewBlock, DefaultChecksumType)
	return cw
}

// Close closes the chunk writer.
func (cw *ChunkWriter) Close() error {
	if err := cw.flush(); err != nil {
		return err
	}
	cw.sink.Stop()
	return nil
}

// Write writes the specified input data.
func (cw *ChunkWriter) Write(data []byte) (int, error) {
	if cw.stopped {
		return 0, sm.ErrSnapshotStopped
	}
	if cw.failed {
		return 0, sm.ErrSnapshotStreaming
	}
	if cw.first {
		cw.first = false
		if _, err := cw.Write(GetEmptyLRUSession()); err != nil {
			return 0, err
		}
	}
	return cw.bw.Write(data)
}

func (cw *ChunkWriter) flush() error {
	if err := cw.bw.Flush(); err != nil {
		return err
	}
	if !cw.failed {
		return cw.onNewChunk(cw.getTailChunk())
	}
	return nil
}

func (cw *ChunkWriter) onNewBlock(data []byte, crc []byte) error {
	defer func() {
		cw.chunkID = cw.chunkID + 1
	}()
	chunk := cw.getChunk()
	var payload []byte
	if cw.chunkID == 0 {
		payload = cw.getHeader()
	}
	payload = append(payload, data...)
	payload = append(payload, crc...)
	chunk.Data = payload
	chunk.ChunkSize = uint64(len(payload))
	return cw.onNewChunk(chunk)
}

func (cw *ChunkWriter) onNewChunk(chunk pb.SnapshotChunk) error {
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
		Version:         uint64(V2SnapshotVersion),
	}
	data, err := header.Marshal()
	if err != nil {
		panic(err)
	}
	headerHash := GetDefaultChecksum()
	if _, err := headerHash.Write(data); err != nil {
		panic(err)
	}
	headerChecksum := headerHash.Sum(nil)
	header.HeaderChecksum = headerChecksum
	data, err = header.Marshal()
	if err != nil {
		panic(err)
	}
	headerData := make([]byte, SnapshotHeaderSize)
	binary.LittleEndian.PutUint64(headerData, uint64(len(data)))
	copy(headerData[8:], data)
	return headerData
}

func (cw *ChunkWriter) getChunk() pb.SnapshotChunk {
	return pb.SnapshotChunk{
		ClusterId:   cw.sink.ClusterID(),
		NodeId:      cw.sink.ToNodeID(),
		From:        cw.meta.From,
		ChunkId:     cw.chunkID,
		FileChunkId: cw.chunkID,
		Index:       cw.meta.Index,
		Term:        cw.meta.Term,
		OnDiskIndex: cw.meta.OnDiskIndex,
		Membership:  cw.meta.Membership,
		BinVer:      raftio.RPCBinVersion,
		Filepath:    server.GetSnapshotFilename(cw.meta.Index),
	}
}

func (cw *ChunkWriter) getTailChunk() pb.SnapshotChunk {
	tailChunk := cw.getChunk()
	tailChunk.ChunkCount = pb.LastChunkCount
	tailChunk.FileChunkCount = pb.LastChunkCount
	return tailChunk
}
