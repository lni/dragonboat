// Copyright 2017-2021 Lei Ni (nilei81@gmaij.com) and other contributors.
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

package transport

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

const (
	streamingChanLength = 4
)

var (
	// ErrStopped is the error returned to indicate that the connection has
	// already been stopped.
	ErrStopped = errors.New("connection stopped")
	// ErrStreamSnapshot is the error returned to indicate that snapshot
	// streaming failed.
	ErrStreamSnapshot = errors.New("stream snapshot failed")
)

// Sink is the chunk sink for receiving generated snapshot chunk.
type Sink struct {
	j *job
}

// Receive receives a snapshot chunk.
func (s *Sink) Receive(chunk pb.Chunk) (bool, bool) {
	return s.j.AddChunk(chunk)
}

// Close closes the sink processing.
func (s *Sink) Close() error {
	s.Receive(pb.Chunk{ChunkCount: pb.PoisonChunkCount})
	return nil
}

// ShardID returns the shard ID of the source node.
func (s *Sink) ShardID() uint64 {
	return s.j.shardID
}

// ToReplicaID returns the node ID of the node intended to get and handle the
// received snapshot chunk.
func (s *Sink) ToReplicaID() uint64 {
	return s.j.replicaID
}

type job struct {
	conn         raftio.ISnapshotConnection
	preSend      atomic.Value
	postSend     atomic.Value
	fs           vfs.IFS
	ctx          context.Context
	transport    raftio.ITransport
	ch           chan pb.Chunk
	completed    chan struct{}
	stopc        chan struct{}
	failed       chan struct{}
	deploymentID uint64
	replicaID    uint64
	shardID      uint64
	streaming    bool
}

func newJob(ctx context.Context,
	shardID uint64, replicaID uint64,
	did uint64, streaming bool, sz int, transport raftio.ITransport,
	stopc chan struct{}, fs vfs.IFS) *job {
	j := &job{
		shardID:      shardID,
		replicaID:    replicaID,
		deploymentID: did,
		streaming:    streaming,
		ctx:          ctx,
		transport:    transport,
		stopc:        stopc,
		failed:       make(chan struct{}),
		completed:    make(chan struct{}),
		fs:           fs,
	}
	var chsz int
	if streaming {
		chsz = streamingChanLength
	} else {
		chsz = sz
	}
	j.ch = make(chan pb.Chunk, chsz)
	return j
}

func (j *job) close() {
	if j.conn != nil {
		j.conn.Close()
	}
}

func (j *job) connect(addr string) error {
	conn, err := j.transport.GetSnapshotConnection(j.ctx, addr)
	if err != nil {
		plog.Errorf("failed to get a job to %s, %v", addr, err)
		return err
	}
	j.conn = conn
	return nil
}

func (j *job) addSnapshot(chunks []pb.Chunk) {
	if len(chunks) != cap(j.ch) {
		plog.Panicf("cap of ch is %d, want %d", cap(j.ch), len(chunks))
	}
	for _, chunk := range chunks {
		j.ch <- chunk
	}
}

func (j *job) AddChunk(chunk pb.Chunk) (bool, bool) {
	if !chunk.IsPoisonChunk() {
		plog.Debugf("%s is sending chunk %d to %s",
			logutil.ReplicaID(chunk.From), chunk.ChunkId,
			dn(chunk.ShardID, chunk.ReplicaID))
	} else {
		plog.Debugf("sending a poison chunk to %s", dn(j.shardID, j.replicaID))
	}

	select {
	case j.ch <- chunk:
		return true, false
	case <-j.completed:
		if !chunk.IsPoisonChunk() {
			plog.Panicf("more chunk received for completed job")
		}
		return true, false
	case <-j.failed:
		plog.Warningf("stream snapshot to %s failed", dn(j.shardID, j.replicaID))
		return false, false
	case <-j.stopc:
		return false, true
	}
}

func (j *job) process() error {
	if j.conn == nil {
		panic("nil connection")
	}
	if j.streaming {
		err := j.streamSnapshot()
		if err != nil {
			close(j.failed)
		}
		return err
	}
	return j.sendSnapshot()
}

func (j *job) streamSnapshot() error {
	for {
		select {
		case <-j.stopc:
			plog.Warningf("stream snapshot to %s stopped", dn(j.shardID, j.replicaID))
			return ErrStopped
		case chunk := <-j.ch:
			chunk.DeploymentId = j.deploymentID
			if chunk.IsPoisonChunk() {
				return ErrStreamSnapshot
			}
			if err := j.sendChunk(chunk, j.conn); err != nil {
				plog.Errorf("streaming snapshot chunk to %s failed, %v",
					dn(chunk.ShardID, chunk.ReplicaID), err)
				return err
			}
			if chunk.ChunkCount == pb.LastChunkCount {
				plog.Debugf("node %d just sent all chunks to %s",
					chunk.From, dn(chunk.ShardID, chunk.ReplicaID))
				close(j.completed)
				return nil
			}
		}
	}
}

func (j *job) sendSnapshot() error {
	chunks := make([]pb.Chunk, 0)
	for {
		select {
		case <-j.stopc:
			return ErrStopped
		case chunk := <-j.ch:
			if len(chunks) == 0 && chunk.ChunkId != 0 {
				panic("chunk alignment error")
			}
			chunks = append(chunks, chunk)
			if chunk.ChunkId+1 == chunk.ChunkCount {
				return j.sendChunks(chunks)
			}
		}
	}
}

func (j *job) sendChunks(chunks []pb.Chunk) error {
	chunkData := make([]byte, snapshotChunkSize)
	for _, chunk := range chunks {
		select {
		case <-j.stopc:
			return ErrStopped
		default:
		}
		chunk.DeploymentId = j.deploymentID
		if !chunk.Witness {
			// TODO: add a test for such error
			// TODO: add a test to show that failed sendChunks for other reasons will
			// 			 be reported
			data, err := loadChunkData(chunk, chunkData, j.fs)
			if err != nil {
				panicNow(err)
			}
			chunk.Data = data
		}
		if err := j.sendChunk(chunk, j.conn); err != nil {
			return err
		}
		if f := j.postSend.Load(); f != nil {
			f.(func(pb.Chunk))(chunk)
		}
	}
	return nil
}

func (j *job) sendChunk(c pb.Chunk,
	conn raftio.ISnapshotConnection) error {
	if f := j.preSend.Load(); f != nil {
		updated, shouldSend := f.(StreamChunkSendFunc)(c)
		if !shouldSend {
			plog.Debugf("chunk to %s skipped", dn(c.ShardID, c.ReplicaID))
			return errChunkSendSkipped
		}
		return conn.SendChunk(updated)
	}
	return conn.SendChunk(c)
}
