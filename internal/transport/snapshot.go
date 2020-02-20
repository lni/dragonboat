// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//
// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
//
//
// This file contains code derived from CockroachDB. The asyncSendSnapshot method,
// connectAndProcessSnapshot method and the processSnapshotQueue method is similar
// to the one used in CockroachDB.

package transport

import (
	"errors"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	snapshotChunkSize         = settings.SnapshotChunkSize
	maxConnectionCount uint32 = uint32(settings.Soft.MaxSnapshotConnections)
)

// ASyncSendSnapshot sends raft snapshot message to its target.
func (t *Transport) ASyncSendSnapshot(m pb.Message) bool {
	if !t.asyncSendSnapshot(m) {
		plog.Errorf("failed to send snapshot to %s", dn(m.ClusterId, m.To))
		t.sendSnapshotNotification(m.ClusterId, m.To, true)
		return false
	}
	return true
}

// GetStreamConnection returns a connection used for streaming snapshot.
func (t *Transport) GetStreamConnection(clusterID uint64, nodeID uint64) *Sink {
	s := t.getStreamConnection(clusterID, nodeID)
	if s == nil {
		t.sendSnapshotNotification(clusterID, nodeID, true)
	}
	return s
}

func (t *Transport) getStreamConnection(clusterID uint64, nodeID uint64) *Sink {
	addr, _, err := t.resolver.Resolve(clusterID, nodeID)
	if err != nil {
		return nil
	}
	if !t.GetCircuitBreaker(addr).Ready() {
		return nil
	}
	key := raftio.GetNodeInfo(clusterID, nodeID)
	if c := t.tryCreateJob(key, addr, true, 0); c != nil {
		return &Sink{j: c}
	}
	return nil
}

func (t *Transport) asyncSendSnapshot(m pb.Message) bool {
	toNodeID := m.To
	clusterID := m.ClusterId
	if m.Type != pb.InstallSnapshot {
		panic("non-snapshot message received by ASyncSendSnapshot")
	}
	chunks := splitSnapshotMessage(m, t.fs)
	addr, _, err := t.resolver.Resolve(clusterID, toNodeID)
	if err != nil {
		return false
	}
	if !t.GetCircuitBreaker(addr).Ready() {
		t.metrics.snapshotCnnectionFailure()
		return false
	}
	key := raftio.GetNodeInfo(clusterID, toNodeID)
	c := t.tryCreateJob(key, addr, false, len(chunks))
	if c == nil {
		return false
	}
	c.sendSavedSnapshot(m)
	return true
}

func (t *Transport) tryCreateJob(key raftio.NodeInfo,
	addr string, streaming bool, sz int) *job {
	if v := atomic.AddUint32(&t.jobs, 1); v > maxConnectionCount {
		r := atomic.AddUint32(&t.jobs, ^uint32(0))
		plog.Errorf("job count is rate limited %d", r)
		return nil
	}
	return t.createConnection(key, addr, streaming, sz)
}

func (t *Transport) createConnection(key raftio.NodeInfo,
	addr string, streaming bool, sz int) *job {
	c := newJob(t.ctx, key.ClusterID, key.NodeID,
		t.getDeploymentID(), streaming, sz, t.raftRPC, t.stopper.ShouldStop(), t.fs)
	c.streamChunkSent = t.streamChunkSent
	c.preStreamChunkSend = t.preStreamChunkSend
	shutdown := func() {
		atomic.AddUint32(&t.jobs, ^uint32(0))
	}
	t.stopper.RunWorker(func() {
		t.connectAndProcessSnapshot(c, addr)
		shutdown()
	})
	return c
}

func (t *Transport) connectAndProcessSnapshot(c *job, addr string) {
	breaker := t.GetCircuitBreaker(addr)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	clusterID := c.clusterID
	nodeID := c.nodeID
	if err := func() error {
		if err := c.connect(addr); err != nil {
			plog.Warningf("failed to get snapshot client to %s", dn(clusterID, nodeID))
			t.sendSnapshotNotification(clusterID, nodeID, true)
			close(c.failed)
			t.metrics.snapshotCnnectionFailure()
			return err
		}
		defer c.close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			plog.Infof("snapshot stream to %s (%s) established",
				dn(clusterID, nodeID), addr)
			t.sysEvents.ConnectionEstablished(addr, true)
		}
		err := c.process()
		if err != nil {
			plog.Errorf("snapshot chunk processing failed: %v", err)
		}
		t.sendSnapshotNotification(clusterID, nodeID, err != nil)
		return err
	}(); err != nil {
		plog.Warningf("connectAndProcessSnapshot failed: %v", err)
		breaker.Fail()
		t.sysEvents.ConnectionFailed(addr, true)
	}
}

func (t *Transport) sendSnapshotNotification(clusterID uint64,
	nodeID uint64, rejected bool) {
	if rejected {
		t.metrics.snapshotSendFailure()
	} else {
		t.metrics.snapshotSendSuccess()
	}
	handler := t.handler.Load()
	if handler != nil {
		h := handler.(IRaftMessageHandler)
		h.HandleSnapshotStatus(clusterID, nodeID, rejected)
		plog.Debugf("snapshot notification to %s added, reject %t",
			dn(clusterID, nodeID), rejected)
	} else {
		plog.Warningf("snapshot notification to %s ignored", dn(clusterID, nodeID))
	}
}

func splitBySnapshotFile(msg pb.Message,
	filepath string, filesize uint64, startChunkID uint64,
	sf *pb.SnapshotFile) []pb.SnapshotChunk {
	if filesize == 0 {
		panic("empty file")
	}
	results := make([]pb.SnapshotChunk, 0)
	chunkCount := uint64((filesize-1)/snapshotChunkSize + 1)
	for i := uint64(0); i < chunkCount; i++ {
		var csz uint64
		if i == chunkCount-1 {
			csz = filesize - (chunkCount-1)*snapshotChunkSize
		} else {
			csz = snapshotChunkSize
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
			OnDiskIndex:    msg.Snapshot.OnDiskIndex,
			Membership:     msg.Snapshot.Membership,
			Filepath:       filepath,
			FileSize:       filesize,
			Witness:        msg.Snapshot.Witness,
		}
		if sf != nil {
			c.HasFileInfo = true
			c.FileInfo = *sf
		}
		results = append(results, c)
	}
	return results
}

func getChunks(m pb.Message) []pb.SnapshotChunk {
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

func getWitnessChunk(m pb.Message, fs vfs.IFS) []pb.SnapshotChunk {
	ss, err := rsm.GetWitnessSnapshot(fs)
	if err != nil {
		panic(err)
	}
	results := make([]pb.SnapshotChunk, 0)
	results = append(results, pb.SnapshotChunk{
		BinVer:         raftio.RPCBinVersion,
		ClusterId:      m.ClusterId,
		NodeId:         m.To,
		From:           m.From,
		FileChunkId:    0,
		FileChunkCount: 1,
		ChunkId:        0,
		ChunkCount:     1,
		ChunkSize:      uint64(len(ss)),
		Index:          m.Snapshot.Index,
		Term:           m.Snapshot.Term,
		OnDiskIndex:    0,
		Membership:     m.Snapshot.Membership,
		Filepath:       "witness.snapshot",
		FileSize:       uint64(len(ss)),
		Witness:        true,
		Data:           ss,
	})
	return results
}

func splitSnapshotMessage(m pb.Message, fs vfs.IFS) []pb.SnapshotChunk {
	if m.Type != pb.InstallSnapshot {
		panic("not a snapshot message")
	}
	if m.Snapshot.Witness {
		return getWitnessChunk(m, fs)
	}
	return getChunks(m)
}

func loadSnapshotChunkData(chunk pb.SnapshotChunk,
	data []byte, fs vfs.IFS) ([]byte, error) {
	f, err := OpenChunkFileForRead(chunk.Filepath, fs)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	offset := chunk.FileChunkId * snapshotChunkSize
	if chunk.ChunkSize != uint64(len(data)) {
		data = make([]byte, chunk.ChunkSize)
	}
	n, err := f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if uint64(n) != chunk.ChunkSize {
		return nil, errors.New("failed to read the snapshot chunk")
	}
	return data, nil
}
