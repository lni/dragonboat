// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"crypto/rand"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func getTestChunk() []pb.Chunk {
	result := make([]pb.Chunk, 0)
	for chunkID := uint64(0); chunkID < 10; chunkID++ {
		c := pb.Chunk{
			DeploymentId:   settings.UnmanagedDeploymentID,
			BinVer:         raftio.TransportBinVersion,
			ShardID:        100,
			ReplicaID:      2,
			From:           12,
			FileChunkId:    chunkID,
			FileChunkCount: 10,
			ChunkId:        chunkID,
			ChunkSize:      100,
			ChunkCount:     10,
			Index:          1,
			Term:           1,
			Filepath:       "snapshot-0000000000000001.gbsnap",
			FileSize:       10 * rsm.HeaderSize,
		}
		data := make([]byte, rsm.HeaderSize)
		if _, err := rand.Read(data); err != nil {
			panic(err)
		}
		c.Data = data
		result = append(result, c)
	}
	return result
}

func hasSnapshotTempFile(cs *Chunk, c pb.Chunk) bool {
	env := cs.getEnv(c)
	fp := env.GetTempFilepath()
	if _, err := cs.fs.Stat(fp); vfs.IsNotExist(err) {
		return false
	}
	return true
}

func hasExternalFile(cs *Chunk,
	c pb.Chunk, fn string, sz uint64, ifs vfs.IFS) bool {
	env := cs.getEnv(c)
	efp := ifs.PathJoin(env.GetFinalDir(), fn)
	fs, err := cs.fs.Stat(efp)
	if vfs.IsNotExist(err) {
		return false
	}
	return uint64(fs.Size()) == sz
}

func runChunkTest(t *testing.T,
	fn func(*testing.T, *Chunk, *testMessageHandler), fs vfs.IFS) {
	defer func() {
		if err := fs.RemoveAll(snapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer leaktest.AfterTest(t)()
	handler := newTestMessageHandler()
	trans, _, stopper, tt := newTestTransport(handler, false, fs)
	defer func() {
		if err := trans.env.Close(); err != nil {
			t.Fatalf("failed to stop the env %v", err)
		}
	}()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	defer tt.cleanup()
	chunks := NewChunk(trans.handleRequest,
		trans.snapshotReceived, trans.dir, trans.nhConfig.GetDeploymentID(), fs)
	ts := getTestChunk()
	snapDir := chunks.dir(ts[0].ShardID, ts[0].ReplicaID)
	if err := fs.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("%v", err)
	}
	fn(t, chunks, handler)
}

func TestMaxSlotIsEnforced(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		defer func() {
			if err := chunks.fs.RemoveAll(snapshotDir); err != nil {
				t.Fatalf("%v", err)
			}
		}()
		inputs := getTestChunk()
		chunks.validate = false
		v := uint64(1)
		c := inputs[0]
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ShardID = v
			snapDir := chunks.dir(v, c.ReplicaID)
			if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
				t.Fatalf("%v", err)
			}
			if !chunks.addLocked(c) {
				t.Errorf("failed to add chunk")
			}
		}
		count := len(chunks.tracked)
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ShardID = v
			if chunks.addLocked(c) {
				t.Errorf("not rejected")
			}
		}
		assert.Equal(t, count, len(chunks.tracked))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestOutOfOrderChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		key := chunkKey(inputs[0])
		td := chunks.tracked[key]
		next := td.next
		td.next = next + 10
		assert.Nil(t, chunks.record(inputs[1]))
		td = chunks.tracked[key]
		assert.Equal(t, next+10, td.next)
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestChunkFromANewLeaderIsIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		key := chunkKey(inputs[0])
		td := chunks.tracked[key]
		next := td.next
		td.first.From = td.first.From + 1
		assert.Nil(t, chunks.record(inputs[1]))
		td = chunks.tracked[key]
		assert.Equal(t, next, td.next)
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestNotTrackedChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		assert.Nil(t, chunks.record(inputs[1]))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGetOrCreateSnapshotLock(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		l := chunks.getSnapshotLock("k1")
		l1, ok := chunks.locks["k1"]
		assert.True(t, ok)
		assert.Equal(t, l, l1)
		l2 := chunks.getSnapshotLock("k2")
		assert.NotNil(t, l2)
		l3 := chunks.getSnapshotLock("k3")
		assert.NotNil(t, l3)
		ll := chunks.getSnapshotLock("k1")
		assert.Equal(t, l1, ll)
		assert.Equal(t, 3, len(chunks.locks))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestShouldUpdateValidator(t *testing.T) {
	tests := []struct {
		validate    bool
		hasFileInfo bool
		chunkID     uint64
		result      bool
	}{
		{true, true, 0, false},
		{true, false, 0, false},
		{false, true, 0, false},
		{false, false, 0, false},
		{true, true, 1, false},
		{true, false, 1, true},
		{false, true, 1, false},
		{false, false, 1, false},
	}
	for _, tt := range tests {
		c := &Chunk{validate: tt.validate}
		input := pb.Chunk{ChunkId: tt.chunkID, HasFileInfo: tt.hasFileInfo}
		assert.Equal(t, tt.result, c.shouldValidate(input))
	}
}

func TestAddFirstChunkRecordsTheSnapshotAndCreatesTheTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		td, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.True(t, ok)
		assert.Equal(t, chunks.getTick(), td.tick)
		recordedChunk, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.True(t, ok)
		assert.Equal(t, inputs[0], recordedChunk.first)
		assert.True(t, hasSnapshotTempFile(chunks, inputs[0]))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGcRemovesRecordAndTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		assert.True(t, chunks.addLocked(inputs[0]))
		count := chunks.timeout + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.False(t, hasSnapshotTempFile(chunks, inputs[0]))
		assert.Equal(t, uint64(0), handler.getSnapshotCount(100, 2))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestReceivedCompleteChunkWillBeMergedIntoSnapshotFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		for _, c := range inputs {
			assert.True(t, chunks.addLocked(c))
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.False(t, hasSnapshotTempFile(chunks, inputs[0]))
		assert.Equal(t, uint64(1), handler.getSnapshotCount(100, 2))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestChunkAreIgnoredWhenNodeIsRemoved(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		env := chunks.getEnv(inputs[0])
		chunks.validate = false
		assert.True(t, chunks.addLocked(inputs[0]))
		assert.True(t, chunks.addLocked(inputs[1]))
		snapshotDir := env.GetRootDir()
		assert.Nil(t, fileutil.MarkDirAsDeleted(snapshotDir, &pb.Message{}, chunks.fs))
		for idx, c := range inputs {
			if idx <= 1 {
				continue
			}
			assert.False(t, chunks.addLocked(c))
		}
		tmpSnapDir := env.GetTempDir()
		_, err := chunks.fs.Stat(tmpSnapDir)
		assert.True(t, vfs.IsNotExist(err))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

// when there is no flag file
func TestOutOfDateChunkCanBeHandled(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		env := chunks.getEnv(inputs[0])
		snapDir := env.GetFinalDir()
		assert.Nil(t, chunks.fs.MkdirAll(snapDir, 0755))
		chunks.validate = false
		for _, c := range inputs {
			chunks.addLocked(c)
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.Equal(t, uint64(0), handler.getSnapshotCount(100, 2))
		tmpSnapDir := env.GetTempDir()
		_, err := chunks.fs.Stat(tmpSnapDir)
		assert.True(t, vfs.IsNotExist(err))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestSignificantlyDelayedNonFirstChunkAreIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		count := chunks.timeout + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.False(t, hasSnapshotTempFile(chunks, inputs[0]))
		assert.Equal(t, uint64(0), handler.getSnapshotCount(100, 2))
		// now we have the remaining chunks
		for _, c := range inputs[1:] {
			assert.False(t, chunks.addLocked(c))
		}
		_, ok = chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.False(t, hasSnapshotTempFile(chunks, inputs[0]))
		assert.Equal(t, uint64(0), handler.getSnapshotCount(100, 2))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func checkTestSnapshotFile(chunks *Chunk,
	chunk pb.Chunk, size uint64) bool {
	env := chunks.getEnv(chunk)
	finalFp := env.GetFilepath()
	f, err := chunks.fs.Open(finalFp)
	if err != nil {
		plog.Errorf("no final fp file %s", finalFp)
		return false
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	fi, _ := f.Stat()
	if uint64(fi.Size()) != size {
		plog.Errorf("size doesn't match %d, %d, %s", fi.Size(), size, finalFp)
		return false
	}
	return true
}

func TestAddingFirstChunkAgainResetsTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		chunks.addLocked(inputs[1])
		chunks.addLocked(inputs[2])
		inputs = getTestChunk()
		// now add everything
		for _, c := range inputs {
			assert.True(t, chunks.addLocked(c))
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		assert.False(t, ok)
		assert.False(t, hasSnapshotTempFile(chunks, inputs[0]))
		assert.Equal(t, uint64(1), handler.getSnapshotCount(100, 2))
		assert.True(t, checkTestSnapshotFile(chunks, inputs[0], settings.SnapshotHeaderSize*10))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func testSnapshotWithExternalFilesAreHandledByChunk(t *testing.T,
	validate bool, snapshotCount uint64, fs vfs.IFS) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		chunks.validate = validate
		sf1 := &pb.SnapshotFile{
			Filepath: "/data/external1.data",
			FileSize: 100,
			FileId:   1,
			Metadata: make([]byte, 16),
		}
		sf2 := &pb.SnapshotFile{
			Filepath: "/data/external2.data",
			FileSize: snapshotChunkSize + 100,
			FileId:   2,
			Metadata: make([]byte, 32),
		}
		ss := pb.Snapshot{
			Filepath: "filepath.data",
			FileSize: snapshotChunkSize*3 + 100,
			Index:    100,
			Term:     200,
			Files:    []*pb.SnapshotFile{sf1, sf2},
		}
		msg := pb.Message{
			Type:     pb.InstallSnapshot,
			To:       2,
			From:     1,
			ShardID:  100,
			Snapshot: ss,
		}
		inputs, err := splitSnapshotMessage(msg, chunks.fs)
		require.Nil(t, err)
		for _, c := range inputs {
			c.DeploymentId = settings.UnmanagedDeploymentID
			c.Data = make([]byte, c.ChunkSize)
			added := chunks.addLocked(c)
			if snapshotCount == 0 {
				assert.False(t, added)
			} else {
				assert.True(t, added)
			}
		}
		if snapshotCount > 0 {
			assert.Equal(t, uint64(1), handler.getSnapshotCount(100, 2))
			assert.True(t, hasExternalFile(chunks, inputs[0], "external1.data", 100, fs) &&
				hasExternalFile(chunks, inputs[0], "external2.data", snapshotChunkSize+100, fs))
		} else {
			assert.Equal(t, uint64(0), handler.getSnapshotCount(100, 2))
		}
	}
	runChunkTest(t, fn, fs)
}

func TestSnapshotWithExternalFilesAreHandledByChunk(t *testing.T) {
	fs := vfs.GetTestFS()
	testSnapshotWithExternalFilesAreHandledByChunk(t, true, 0, fs)
	testSnapshotWithExternalFilesAreHandledByChunk(t, false, 1, fs)
}

func TestWitnessSnapshotCanBeHandled(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		ss := pb.Snapshot{
			Filepath: "",
			FileSize: 0,
			Index:    100,
			Term:     200,
			Files:    nil,
			Dummy:    false,
			Witness:  true,
		}
		msg := pb.Message{
			Type:     pb.InstallSnapshot,
			To:       2,
			From:     1,
			ShardID:  100,
			Snapshot: ss,
		}
		inputs, err := splitSnapshotMessage(msg, chunks.fs)
		require.Nil(t, err)
		assert.Equal(t, 1, len(inputs))
		chunk := inputs[0]
		assert.Equal(t, raftio.TransportBinVersion, chunk.BinVer)
		assert.True(t, chunk.Witness)
		assert.Equal(t, uint64(100), chunk.ShardID)
		assert.Equal(t, uint64(1), chunk.From)
		assert.Equal(t, uint64(2), chunk.ReplicaID)
		for _, c := range inputs {
			assert.NotEmpty(t, c.Data)
			c.DeploymentId = settings.UnmanagedDeploymentID
			assert.True(t, chunks.addLocked(c))
		}
		assert.Equal(t, uint64(1), handler.getSnapshotCount(100, 2))
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestSnapshotRecordWithoutExternalFilesCanBeSplitIntoChunk(t *testing.T) {
	fs := vfs.GetTestFS()
	ss := pb.Snapshot{
		Filepath: "filepath.data",
		FileSize: snapshotChunkSize*3 + 100,
		Index:    100,
		Term:     200,
	}
	msg := pb.Message{
		Type:     pb.InstallSnapshot,
		To:       2,
		From:     1,
		ShardID:  100,
		Snapshot: ss,
	}
	chunks, err := splitSnapshotMessage(msg, fs)
	require.Nil(t, err)
	assert.Equal(t, 4, len(chunks))
	for _, c := range chunks {
		assert.Equal(t, raftio.TransportBinVersion, c.BinVer)
		assert.Equal(t, uint64(100), c.ShardID)
		assert.Equal(t, uint64(2), c.ReplicaID)
		assert.Equal(t, uint64(1), c.From)
		assert.Equal(t, uint64(100), c.Index)
		assert.Equal(t, uint64(200), c.Term)
	}
	assert.False(t, chunks[0].HasFileInfo)
	assert.False(t, chunks[1].HasFileInfo)
	assert.False(t, chunks[2].HasFileInfo)
	assert.False(t, chunks[3].HasFileInfo)
	assert.Equal(t, uint64(0), chunks[0].FileChunkId)
	assert.Equal(t, uint64(1), chunks[1].FileChunkId)
	assert.Equal(t, uint64(2), chunks[2].FileChunkId)
	assert.Equal(t, uint64(3), chunks[3].FileChunkId)
	assert.Equal(t, uint64(4), chunks[0].FileChunkCount)
	assert.Equal(t, uint64(4), chunks[1].FileChunkCount)
	assert.Equal(t, uint64(4), chunks[2].FileChunkCount)
	assert.Equal(t, uint64(4), chunks[3].FileChunkCount)
	assert.Equal(t, uint64(0), chunks[0].ChunkId)
	assert.Equal(t, uint64(1), chunks[1].ChunkId)
	assert.Equal(t, uint64(2), chunks[2].ChunkId)
	assert.Equal(t, uint64(3), chunks[3].ChunkId)
	assert.Equal(t, ss.FileSize, chunks[0].ChunkSize+chunks[1].ChunkSize+
		chunks[2].ChunkSize+chunks[3].ChunkSize)
}

func TestSnapshotRecordWithTwoExternalFilesCanBeSplitIntoChunk(t *testing.T) {
	fs := vfs.GetTestFS()
	sf1 := &pb.SnapshotFile{
		Filepath: "/data/external1.data",
		FileSize: 100,
		FileId:   1,
		Metadata: make([]byte, 16),
	}
	sf2 := &pb.SnapshotFile{
		Filepath: "/data/external2.data",
		FileSize: snapshotChunkSize + 100,
		FileId:   2,
		Metadata: make([]byte, 32),
	}
	ss := pb.Snapshot{
		Filepath: "filepath.data",
		FileSize: snapshotChunkSize*3 + 100,
		Index:    100,
		Term:     200,
		Files:    []*pb.SnapshotFile{sf1, sf2},
	}
	msg := pb.Message{
		Type:     pb.InstallSnapshot,
		To:       2,
		From:     1,
		ShardID:  100,
		Snapshot: ss,
	}
	chunks, err := splitSnapshotMessage(msg, fs)
	require.Nil(t, err)
	assert.Equal(t, 7, len(chunks))
	total := uint64(0)
	for idx, c := range chunks {
		assert.Equal(t, uint64(idx), c.ChunkId)
		total += c.ChunkSize
	}
	assert.Equal(t, sf1.FileSize+sf2.FileSize+ss.FileSize, total)
	assert.Equal(t, uint64(0), chunks[0].FileChunkId)
	assert.Equal(t, uint64(0), chunks[4].FileChunkId)
	assert.Equal(t, uint64(0), chunks[5].FileChunkId)
	assert.Equal(t, uint64(1), chunks[4].FileChunkCount)
	assert.Equal(t, uint64(2), chunks[5].FileChunkCount)
	assert.Equal(t, uint64(100), chunks[4].FileSize)
	assert.Equal(t, snapshotChunkSize+100, chunks[5].FileSize)
	for idx := range chunks {
		if idx >= 0 && idx < 4 {
			assert.False(t, chunks[idx].HasFileInfo)
		} else {
			assert.True(t, chunks[idx].HasFileInfo)
		}
	}
	assert.Equal(t, sf1.FileId, chunks[4].FileInfo.FileId)
	assert.Equal(t, sf2.FileId, chunks[5].FileInfo.FileId)
	assert.Equal(t, 16, len(chunks[4].FileInfo.Metadata))
	assert.Equal(t, 32, len(chunks[5].FileInfo.Metadata))
}

func TestGetMessageFromChunk(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		sf1 := &pb.SnapshotFile{
			Filepath: "/data/external1.data",
			FileSize: 100,
			FileId:   1,
			Metadata: make([]byte, 16),
		}
		sf2 := &pb.SnapshotFile{
			Filepath: "/data/external2.data",
			FileSize: snapshotChunkSize + 100,
			FileId:   2,
			Metadata: make([]byte, 32),
		}
		files := []*pb.SnapshotFile{sf1, sf2}
		chunk := pb.Chunk{
			ShardID:      123,
			ReplicaID:    3,
			From:         2,
			ChunkId:      0,
			Index:        200,
			Term:         300,
			FileSize:     350,
			DeploymentId: 2345,
			Filepath:     "test.data",
		}
		msg := chunks.toMessage(chunk, files)
		assert.Equal(t, 1, len(msg.Requests))
		assert.Equal(t, chunk.BinVer, msg.BinVer)
		req := msg.Requests[0]
		assert.Equal(t, chunk.DeploymentId, msg.DeploymentId)
		assert.Equal(t, pb.InstallSnapshot, req.Type)
		assert.Equal(t, chunk.From, req.From)
		assert.Equal(t, chunk.ReplicaID, req.To)
		assert.Equal(t, chunk.ShardID, req.ShardID)
		ss := req.Snapshot
		assert.Equal(t, len(files), len(ss.Files))
		assert.Equal(t, chunk.FileSize, ss.FileSize)
		assert.Equal(t, chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
			"snapshot-123-3", "snapshot-00000000000000C8", "test.data"), ss.Filepath)
		assert.Equal(t, len(sf1.Metadata), len(ss.Files[0].Metadata))
		assert.Equal(t, len(sf2.Metadata), len(ss.Files[1].Metadata))
		assert.Equal(t, chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
			"snapshot-123-3", "snapshot-00000000000000C8", "external-file-1"), ss.Files[0].Filepath)
		assert.Equal(t, chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
			"snapshot-123-3", "snapshot-00000000000000C8", "external-file-2"), ss.Files[1].Filepath)
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}
