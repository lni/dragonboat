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

package dragonboat

import (
	"encoding/binary"
	"time"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/transport"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

const (
	SnapshotChunkSize = settings.SnapshotChunkSize
)

type chunkWriter struct {
	failed  bool
	stopped bool
	chunkID uint64
	sink    pb.IChunkSink
	bw      rsm.IBlockWriter
	meta    *rsm.SnapshotMeta
}

func newChunkWriter(sink pb.IChunkSink,
	meta *rsm.SnapshotMeta) (*chunkWriter, error) {
	cw := &chunkWriter{
		sink: sink,
		meta: meta,
	}
	cw.bw = rsm.NewBlockWriter(SnapshotChunkSize, cw.onNewBlock)
	if _, err := cw.Write(rsm.GetEmptyLRUSession()); err != nil {
		return nil, err
	}
	return cw, nil
}

func (cw *chunkWriter) Write(data []byte) (int, error) {
	if cw.stopped {
		return 0, sm.ErrSnapshotStopped
	}
	if cw.failed {
		return 0, sm.ErrSnapshotStreaming
	}
	return cw.bw.Write(data)
}

func (cw *chunkWriter) Flush() error {
	if err := cw.bw.Flush(); err != nil {
		return err
	}
	return cw.onNewChunk(cw.getTailChunk())
}

func (cw *chunkWriter) onNewBlock(data []byte, crc []byte) error {
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

func (cw *chunkWriter) onNewChunk(chunk pb.SnapshotChunk) error {
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

func (cw *chunkWriter) getHeader() []byte {
	header := pb.SnapshotHeader{
		SessionSize:     0,
		DataStoreSize:   0,
		UnreliableTime:  uint64(time.Now().UnixNano()),
		PayloadChecksum: []byte{0, 0, 0, 0},
		ChecksumType:    rsm.DefaultChecksumType,
		Version:         uint64(rsm.V2SnapshotVersion),
	}
	data, err := header.Marshal()
	if err != nil {
		panic(err)
	}
	headerHash := rsm.GetDefaultChecksum()
	if _, err := headerHash.Write(data); err != nil {
		panic(err)
	}
	headerChecksum := headerHash.Sum(nil)
	header.HeaderChecksum = headerChecksum
	data, err = header.Marshal()
	if err != nil {
		panic(err)
	}
	headerData := make([]byte, rsm.SnapshotHeaderSize)
	binary.LittleEndian.PutUint64(headerData, uint64(len(data)))
	copy(headerData[8:], data)
	return headerData
}

func (cw *chunkWriter) getChunk() pb.SnapshotChunk {
	return pb.SnapshotChunk{
		ClusterId:   cw.sink.ClusterID(),
		NodeId:      cw.sink.ToNodeID(),
		From:        cw.meta.From,
		ChunkId:     cw.chunkID,
		FileChunkId: cw.chunkID,
		Index:       cw.meta.Index,
		Term:        cw.meta.Term,
		Membership:  cw.meta.Membership,
		BinVer:      raftio.RPCBinVersion,
		Filepath:    server.GetSnapshotFilename(cw.meta.Index),
		// FIXME: might need to set the filepath field
	}
}

func (cw *chunkWriter) getTailChunk() pb.SnapshotChunk {
	tailChunk := cw.getChunk()
	tailChunk.ChunkCount = transport.LastChunkCount
	tailChunk.FileChunkCount = transport.LastChunkCount
	return tailChunk
}
