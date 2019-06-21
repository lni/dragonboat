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

package transport

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	streamingChanLength = 16
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
	l *lane
}

// Receive receives a snapshot chunk.
func (s *Sink) Receive(chunk pb.SnapshotChunk) (bool, bool) {
	return s.l.SendChunk(chunk)
}

// Stop stops the sink processing.
func (s *Sink) Stop() {
	s.Receive(pb.SnapshotChunk{ChunkCount: pb.PoisonChunkCount})
}

// ClusterID returns the cluster ID of the source node.
func (s *Sink) ClusterID() uint64 {
	return s.l.clusterID
}

// ToNodeID returns the node ID of the node intended to get and handle the
// received snapshot chunk.
func (s *Sink) ToNodeID() uint64 {
	return s.l.nodeID
}

type lane struct {
	clusterID          uint64
	nodeID             uint64
	deploymentID       uint64
	streaming          bool
	ctx                context.Context
	rpc                raftio.IRaftRPC
	conn               raftio.ISnapshotConnection
	ch                 chan pb.SnapshotChunk
	stopc              chan struct{}
	failed             chan struct{}
	streamChunkSent    atomic.Value
	preStreamChunkSend atomic.Value
}

func newLane(ctx context.Context,
	clusterID uint64, nodeID uint64,
	did uint64, streaming bool, sz int, rpc raftio.IRaftRPC,
	stopc chan struct{}) *lane {
	l := &lane{
		clusterID:    clusterID,
		nodeID:       nodeID,
		deploymentID: did,
		streaming:    streaming,
		ctx:          ctx,
		rpc:          rpc,
		stopc:        stopc,
		failed:       make(chan struct{}),
	}
	var chsz int
	if streaming {
		chsz = streamingChanLength
	} else {
		chsz = sz
	}
	l.ch = make(chan pb.SnapshotChunk, chsz)
	return l
}

func (l *lane) close() {
	if l.conn != nil {
		l.conn.Close()
	}
}

func (l *lane) connect(addr string) error {
	conn, err := l.rpc.GetSnapshotConnection(l.ctx, addr)
	if err != nil {
		plog.Errorf("failed to get a lane to %s, %v", addr, err)
		return err
	}
	l.conn = conn
	return nil
}

func (l *lane) sendSavedSnapshot(m pb.Message) {
	chunks := splitSnapshotMessage(m)
	if len(chunks) != cap(l.ch) {
		plog.Panicf("cap of ch is %d, want %d", cap(l.ch), len(chunks))
	}
	for _, chunk := range chunks {
		l.ch <- chunk
	}
}

func (l *lane) SendChunk(chunk pb.SnapshotChunk) (bool, bool) {
	plog.Infof("node %d is sending chunk %d to %s",
		chunk.From, chunk.ChunkId,
		logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
	select {
	case l.ch <- chunk:
		return true, false
	case <-l.failed:
		plog.Infof("streaming snapshot to %s terminated, connection failed",
			logutil.DescribeNode(l.clusterID, l.nodeID))
		return false, false
	case <-l.stopc:
		return false, true
	}
}

func (l *lane) process() error {
	if l.conn == nil {
		panic("trying to process on nil ch, not connected?")
	}
	if l.streaming {
		err := l.streamSnapshot()
		if err != nil {
			close(l.failed)
		}
		return err
	}
	return l.processSavedSnapshot()
}

func (l *lane) streamSnapshot() error {
	for {
		select {
		case <-l.stopc:
			plog.Infof("streaming snapshot to %s terminated, stopc signalled",
				logutil.DescribeNode(l.clusterID, l.nodeID))
			return ErrStopped
		case chunk := <-l.ch:
			chunk.DeploymentId = l.deploymentID
			if chunk.IsPoisonChunk() {
				return ErrStreamSnapshot
			}
			if err := l.sendChunk(chunk, l.conn); err != nil {
				plog.Errorf("streaming snapshot chunk to %s failed",
					logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
				return err
			}
			if chunk.ChunkCount == pb.LastChunkCount {
				plog.Infof("node %d just sent last chunk to %s",
					chunk.From, logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
				return nil
			}
		}
	}
}

func (l *lane) processSavedSnapshot() error {
	chunks := make([]pb.SnapshotChunk, 0)
	for {
		select {
		case <-l.stopc:
			return ErrStopped
		case chunk := <-l.ch:
			if len(chunks) == 0 && chunk.ChunkId != 0 {
				panic("chunk alignment error")
			}
			chunks = append(chunks, chunk)
			if chunk.ChunkId+1 == chunk.ChunkCount {
				return l.sendSavedChunks(chunks)
			}
		}
	}
}

func (l *lane) sendSavedChunks(chunks []pb.SnapshotChunk) error {
	for _, chunk := range chunks {
		chunkData := make([]byte, snapshotChunkSize)
		data, err := loadSnapshotChunkData(chunk, chunkData)
		if err != nil {
			plog.Errorf("failed to read the snapshot chunk, %v", err)
			return err
		}
		chunk.Data = data
		chunk.DeploymentId = l.deploymentID
		if err := l.sendChunk(chunk, l.conn); err != nil {
			plog.Debugf("snapshot to %s failed",
				logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
			return err
		}
		if v := l.streamChunkSent.Load(); v != nil {
			v.(func(pb.SnapshotChunk))(chunk)
		}
	}
	return nil
}

func (l *lane) sendChunk(c pb.SnapshotChunk,
	conn raftio.ISnapshotConnection) error {
	if v := l.preStreamChunkSend.Load(); v != nil {
		plog.Infof("pre stream chunk send set")
		updated, shouldSend := v.(StreamChunkSendFunc)(c)
		plog.Infof("shoudSend: %t", shouldSend)
		if !shouldSend {
			plog.Infof("not sending the chunk!")
			return errChunkSendSkipped
		}
		return conn.SendSnapshotChunk(updated)
	}
	return conn.SendSnapshotChunk(c)
}
