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

package transport

import (
	"errors"
	"sync/atomic"

	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

var (
	maxSnapshotCount = settings.Soft.MaxSnapshotCount
)

// ASyncSendSnapshot sends raft snapshot message to its target.
func (t *Transport) ASyncSendSnapshot(m pb.Message) bool {
	if !t.asyncSendSnapshot(m) {
		plog.Debugf("failed to send snapshot to %s",
			logutil.DescribeNode(m.ClusterId, m.To))
		t.sendSnapshotNotification(m.ClusterId, m.To, true)
		return false
	}
	return true
}

// the generic async send Go pattern used in asyncSendSnapshot below is found
// in CockroachDB.
func (t *Transport) asyncSendSnapshot(m pb.Message) bool {
	toNodeID := m.To
	clusterID := m.ClusterId
	if m.Type != pb.InstallSnapshot {
		panic("non-snapshot message received by ASyncSendSnapshot")
	}
	count := uint64(t.increaseSnapshotCount())
	if count > maxSnapshotCount {
		return false
	}
	chunks := splitSnapshotMessage(m)
	if uint64(len(chunks)) > snapSendBufSize {
		// FIXME:
		// what users can do, what we can do to prevent from reaching here?
		panic("len(chunks) > snapSendBufSize, oversized snapshot image")
	}
	addr, key, err := t.resolver.Resolve(clusterID, toNodeID)
	if err != nil {
		return false
	}
	// fail fast
	if !t.GetCircuitBreaker(addr).Ready() {
		return false
	}
	t.mu.Lock()
	ch, ok := t.mu.chunks[key]
	closeCh := t.mu.chunksClose[key]
	if !ok {
		ch = make(chan pb.SnapshotChunk, snapSendBufSize)
		closeCh = make(chan struct{})
		t.mu.chunks[key] = ch
		t.mu.chunksClose[key] = closeCh
	}
	t.mu.Unlock()
	if !ok {
		shutdownQueue := func() {
			t.mu.Lock()
			t.snapshotQueueMu.Lock()
			close(closeCh)
			t.cleanup(ch)
			t.snapshotQueueMu.Unlock()
			delete(t.mu.chunks, key)
			delete(t.mu.chunksClose, key)
			t.mu.Unlock()
		}
		t.stopper.RunWorker(func() {
			t.connectAndProcessSnapshot(clusterID, toNodeID, addr, ch)
			plog.Infof("%s connectAndProcessSnapshot returned",
				logutil.DescribeNode(clusterID, toNodeID))
			shutdownQueue()
		})
	}
	t.snapshotQueueMu.Lock()
	defer t.snapshotQueueMu.Unlock()
	availableSlot := cap(ch) - len(ch)
	if availableSlot < len(chunks) {
		plog.Infof("not enough slots, cap %d, ch len %d, want %d",
			cap(ch), len(ch), len(chunks))
		// fail fast
		// notify raft that the snapshot failed.
		return false
	}
	// now we can ensure everything in ch are continuous chunks from the same
	// snapshot image.
	for i := range chunks {
		select {
		case <-closeCh:
			return false
		default:
		}
		chunk := chunks[i]
		select {
		case ch <- chunk:
		default:
			panic("snapshot channel not suppose to be full")
		}
	}
	return true
}

func (t *Transport) connectAndProcessSnapshot(clusterID uint64,
	toNodeID uint64, remoteHost string, ch <-chan pb.SnapshotChunk) {
	breaker := t.GetCircuitBreaker(remoteHost)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	if err := func() error {
		conn, err := t.raftRPC.GetSnapshotConnection(t.ctx, remoteHost)
		if err != nil {
			plog.Warningf("failed to get snapshot client to %s",
				logutil.DescribeNode(clusterID, toNodeID))
			t.sendSnapshotNotification(clusterID, toNodeID, true)
			return err
		}
		defer conn.Close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			plog.Infof("snapshot stream to %s (%s) established",
				logutil.DescribeNode(clusterID, toNodeID), remoteHost)
		}
		return t.processSnapshotQueue(ch, conn)
	}(); err != nil {
		plog.Warningf("connectAndProcessSnapshot failed: %v", err)
		breaker.Fail()
	}
}

func (t *Transport) sendSnapshotChunk(c pb.SnapshotChunk,
	conn raftio.ISnapshotConnection) error {
	v := t.preStreamChunkSend.Load()
	if v != nil {
		updated, shouldSend := v.(StreamChunkSendFunc)(c)
		if !shouldSend {
			return errChunkSendSkipped
		}
		return conn.SendSnapshotChunk(updated)
	}
	return conn.SendSnapshotChunk(c)
}

func (t *Transport) processSnapshotQueue(ch <-chan pb.SnapshotChunk,
	conn raftio.ISnapshotConnection) error {
	chunks := make([]pb.SnapshotChunk, 0)
	var deploymentIDSet bool
	var deploymentID uint64
	idx := uint64(0)
	for {
		select {
		case <-t.stopper.ShouldStop():
			plog.Infof("processSnapshotQueue is going to exit")
			return nil
		case chunk := <-ch:
			// everything in ch are continuous chunks
			if len(chunks) == 0 && chunk.ChunkId != 0 {
				panic("chunk alignment error")
			}
			chunks = append(chunks, chunk)
			if uint64(len(chunks)) > snapSendBufSize {
				panic("chunk count > snapSendBufSize")
			}
			// all chunks have been pulled from the ch
			if chunk.ChunkId+1 == chunk.ChunkCount {
				// drop the message if deployment id is not set.
				if !deploymentIDSet {
					if t.deploymentIDSet() {
						deploymentIDSet = true
						deploymentID = t.getDeploymentID()
						plog.Infof("Deployment id is %d", deploymentID)
					}
				}
				if !deploymentIDSet {
					plog.Warningf("snapshot to %s dropped, deployment id not set",
						logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
					t.sendSnapshotNotification(chunk.ClusterId, chunk.NodeId, true)
					continue
				}
				failed := false
				for _, curChunk := range chunks {
					chunkData := make([]byte, snapChunkSize)
					idx++
					data, err := loadSnapshotChunkData(curChunk, chunkData)
					if err != nil {
						// for whatever reason, we failed to get the current chunk data
						// report this snapshot as failed. the connection is still good,
						// everything in ch should continue
						plog.Errorf("failed to read the snapshot chunk, %v", err)
						failed = true
						break
					}
					curChunk.Data = data
					curChunk.DeploymentId = deploymentID
					if err := t.sendSnapshotChunk(curChunk, conn); err != nil {
						plog.Debugf("snapshot to %s failed",
							logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
						t.sendSnapshotNotification(chunk.ClusterId, chunk.NodeId, true)
						return err
					}
					// when t.streamChunkSent is set, it is called after each successful
					// chunk sent
					if t.streamChunkSent != nil {
						t.streamChunkSent(curChunk)
					}
				}
				plog.Debugf("snapshot to %s processed, failed to send value %t",
					logutil.DescribeNode(chunk.ClusterId, chunk.NodeId), failed)
				t.sendSnapshotNotification(chunk.ClusterId, chunk.NodeId, failed)
				chunks = make([]pb.SnapshotChunk, 0)
			}
		}
	}
}

func (t *Transport) cleanup(ch <-chan pb.SnapshotChunk) {
	for {
		select {
		case chunk := <-ch:
			if chunk.ChunkId == 0 {
				plog.Debugf("snapshot channel cleanup called, snapshot to %s failed",
					logutil.DescribeNode(chunk.ClusterId, chunk.NodeId))
				t.sendSnapshotNotification(chunk.ClusterId, chunk.NodeId, true)
			}
		default:
			return
		}
	}
}

func (t *Transport) getSnapshotCount() int32 {
	return atomic.LoadInt32(&t.snapshotCount)
}

func (t *Transport) increaseSnapshotCount() int32 {
	return atomic.AddInt32(&t.snapshotCount, 1)
}

func (t *Transport) decreaseSnapshotCount() {
	// the time-of-check-to-time-of-use issue won't cause real problem
	// here as we are trying to enforce an upper limit only and that
	// upper limit doesn't have to be 100% precise
	v := atomic.AddInt32(&t.snapshotCount, -1)
	if v < 0 {
		atomic.StoreInt32(&t.snapshotCount, 0)
	}
}

func splitBySnapshotFile(msg pb.Message,
	filepath string, filesize uint64, startChunkID uint64,
	sf *pb.SnapshotFile) []pb.SnapshotChunk {
	if filesize == 0 {
		panic("empty file included in snapshot")
	}
	results := make([]pb.SnapshotChunk, 0)
	chunkCount := uint64((filesize-1)/snapChunkSize + 1)
	plog.Infof("splitBySnapshotFile called, chunkCount %d, filesize %d",
		chunkCount, filesize)
	for i := uint64(0); i < chunkCount; i++ {
		var csz uint64
		if i == chunkCount-1 {
			csz = filesize - (chunkCount-1)*snapChunkSize
		} else {
			csz = snapChunkSize
		}
		c := pb.SnapshotChunk{
			BinVer:         raftio.RPCBinVersion,
			ClusterId:      msg.ClusterId,
			NodeId:         msg.To,
			From:           msg.From,
			FileChunkId:    i,
			FileChunkCount: chunkCount,
			ChunkId:        startChunkID + i,
			ChunkSize:      csz,
			Index:          msg.Snapshot.Index,
			Term:           msg.Snapshot.Term,
			Membership:     msg.Snapshot.Membership,
			Filepath:       filepath,
			FileSize:       filesize,
		}
		if sf != nil {
			c.HasFileInfo = true
			c.FileInfo = *sf
		}
		results = append(results, c)
	}
	return results
}

func splitSnapshotMessage(m pb.Message) []pb.SnapshotChunk {
	if m.Type != pb.InstallSnapshot {
		panic("not a snapshot message")
	}
	startChunkID := uint64(0)
	results := splitBySnapshotFile(m,
		m.Snapshot.Filepath, m.Snapshot.FileSize, startChunkID, nil)
	startChunkID += uint64(len(results))
	for _, snapshotFile := range m.Snapshot.Files {
		chunks := splitBySnapshotFile(m,
			snapshotFile.Filepath, snapshotFile.FileSize, startChunkID, snapshotFile)
		results = append(results, chunks...)
		startChunkID += uint64(len(chunks))
	}
	for idx := range results {
		results[idx].ChunkCount = uint64(len(results))
	}
	return results
}

func loadSnapshotChunkData(chunk pb.SnapshotChunk,
	data []byte) ([]byte, error) {
	f, err := fileutil.OpenChunkFileForRead(chunk.Filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	offset := chunk.FileChunkId * snapChunkSize
	if _, err = f.SeekFromBeginning(int64(offset)); err != nil {
		return nil, err
	}
	if chunk.ChunkSize != uint64(len(data)) {
		data = make([]byte, chunk.ChunkSize)
	}
	n, err := f.Read(data)
	if err != nil {
		return nil, err
	}
	if uint64(n) != chunk.ChunkSize {
		return nil, errors.New("failed to read the chunk from snapshot file")
	}
	return data, nil
}
