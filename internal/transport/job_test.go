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

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestSnapshotJobCanBeCreatedInSavedMode(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, false, 201, transport, nil, fs)
	if cap(c.ch) != 201 {
		t.Errorf("unexpected chan length %d, want 201", cap(c.ch))
	}
}

func TestSnapshotJobCanBeCreatedInStreamingMode(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 201, transport, nil, fs)
	if cap(c.ch) != streamingChanLength {
		t.Errorf("unexpected chan length %d, want %d", cap(c.ch), streamingChanLength)
	}
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
	if err != nil {
		t.Fatalf("failed to get chunks %v", err)
	}
	transport := NewNOOPTransport(config.NodeHostConfig{}, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, false, len(chunks), transport, nil, fs)
	if cap(c.ch) != len(chunks) {
		t.Errorf("unexpected chan length %d", cap(c.ch))
	}
	c.addSnapshot(chunks)
	if len(c.ch) != len(chunks) {
		t.Errorf("not all chunks pushed to ch")
	}
}

func TestKeepSendingChunksUsingFailedJobWillNotBlock(t *testing.T) {
	fs := vfs.GetTestFS()
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 0, transport, nil, fs)
	if cap(c.ch) != streamingChanLength {
		t.Errorf("unexpected chan length %d, want %d", cap(c.ch), streamingChanLength)
	}
	if err := c.connect("a1"); err != nil {
		t.Fatalf("connect failed %v", err)
	}
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.process()
	})
	noopConn, ok := c.conn.(*NOOPSnapshotConnection)
	if !ok {
		t.Fatalf("failed to get noopConn")
	}
	noopConn.req.SetToFail(true)
	sent, stopped := c.AddChunk(pb.Chunk{})
	if !sent {
		t.Fatalf("failed to send")
	}
	if stopped {
		t.Errorf("unexpectedly stopped")
	}
	stopper.Stop()
	if perr == nil {
		t.Fatalf("error didn't return from process()")
	}
	for i := 0; i < streamingChanLength*10; i++ {
		c.AddChunk(pb.Chunk{})
	}
	select {
	case <-c.failed:
	default:
		t.Fatalf("failed chan not closed")
	}
	c.close()
}

func testSpecialChunkCanStopTheProcessLoop(t *testing.T,
	tt uint64, experr error, fs vfs.IFS) {
	cfg := config.NodeHostConfig{}
	transport := NewNOOPTransport(cfg, nil, nil)
	c := newJob(context.Background(), 1, 1, 1, true, 0, transport, nil, fs)
	if err := c.connect("a1"); err != nil {
		t.Fatalf("connect failed %v", err)
	}
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.process()
	})
	poison := pb.Chunk{
		ChunkCount: tt,
	}
	sent, stopped := c.AddChunk(poison)
	if !sent {
		t.Fatalf("failed to send")
	}
	if stopped {
		t.Errorf("unexpectedly stopped")
	}
	stopper.Stop()
	if perr != experr {
		t.Errorf("unexpected error val %v", perr)
	}
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
