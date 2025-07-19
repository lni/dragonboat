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

package transport

import (
	"context"
	"testing"

	"github.com/lni/goutils/syncutil"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestSnapshotJobCanBeCreatedInSavedMode(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, false, 201, transport, nil, fs)
	require.Equal(t, 201, cap(c.ch), "unexpected chan length")
}

func TestSnapshotJobCanBeCreatedInStreamingMode(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 201, transport, nil, fs)
	require.Equal(t, streamingChanLength, cap(c.ch), "unexpected chan length")
}

func TestSendSavedSnapshotPutsAllChunksInCh(t *testing.T) {
	fs := vfs.GetTestFS()
	m := pb.Message{
		Type: pb.InstallSnapshot,
		Snapshot: pb.Snapshot{
			FileSize: 1024 * 1024 * 512,
		},
	}
	chunks, err := splitSnapshotMessage(m, fs)
	require.NoError(t, err, "failed to get chunks")
	transport := NewNOOPTransport(config.NodeHostConfig{}, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, false, len(chunks),
		transport, nil, fs)
	require.Equal(t, len(chunks), cap(c.ch), "unexpected chan length")
	c.addSnapshot(chunks)
	require.Equal(t, len(chunks), len(c.ch), "not all chunks pushed to ch")
}

func TestKeepSendingChunksUsingFailedJobWillNotBlock(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 0, transport, nil, fs)
	require.Equal(t, streamingChanLength, cap(c.ch), "unexpected chan length")
	err := c.connect("a1")
	require.NoError(t, err, "connect failed")
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.process()
	})
	noopConn, ok := c.conn.(*NOOPSnapshotConnection)
	require.True(t, ok, "failed to get noopConn")
	noopConn.req.SetToFail(true)
	sent, stopped := c.AddChunk(pb.Chunk{})
	require.True(t, sent, "failed to send")
	require.False(t, stopped, "unexpectedly stopped")
	stopper.Stop()
	require.NotNil(t, perr, "error didn't return from process()")
	for i := 0; i < streamingChanLength*10; i++ {
		c.AddChunk(pb.Chunk{})
	}
	select {
	case <-c.failed:
	default:
		require.Fail(t, "failed chan not closed")
	}
	c.close()
}

func testSpecialChunkCanStopTheProcessLoop(t *testing.T,
	tt uint64, experr error, fs vfs.IFS) {
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 0, transport, nil, fs)
	err := c.connect("a1")
	require.NoError(t, err, "connect failed")
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.process()
	})
	poison := pb.Chunk{
		ChunkCount: tt,
	}
	sent, stopped := c.AddChunk(poison)
	require.True(t, sent, "failed to send")
	require.False(t, stopped, "unexpectedly stopped")
	stopper.Stop()
	require.Equal(t, experr, perr, "unexpected error val")
}

func TestPoisonChunkCanStopTheProcessLoop(t *testing.T) {
	fs := vfs.GetTestFS()
	testSpecialChunkCanStopTheProcessLoop(t,
		pb.PoisonChunkCount, ErrStreamSnapshot, fs)
}

func TestLastChunkCanStopTheProcessLoop(t *testing.T) {
	fs := vfs.GetTestFS()
	testSpecialChunkCanStopTheProcessLoop(t, pb.LastChunkCount, nil, fs)
}
