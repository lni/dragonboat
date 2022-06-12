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
	"math/rand"
	"reflect"
	"testing"

	"github.com/lni/goutils/leaktest"

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
		rand.Read(data)
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
		if len(chunks.tracked) != count {
			t.Errorf("tracked count changed")
		}
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
		if chunks.record(inputs[1]) != nil {
			t.Fatalf("out of order chunk is not rejected")
		}
		td = chunks.tracked[key]
		if next+10 != td.next {
			t.Fatalf("next chunk id unexpected moved")
		}
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
		if chunks.record(inputs[1]) != nil {
			t.Fatalf("chunk from a different leader is not rejected")
		}
		td = chunks.tracked[key]
		if next != td.next {
			t.Fatalf("next chunk id unexpected moved")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestNotTrackedChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		if chunks.record(inputs[1]) != nil {
			t.Errorf("not tracked chunk not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGetOrCreateSnapshotLock(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		l := chunks.getSnapshotLock("k1")
		l1, ok := chunks.locks["k1"]
		if !ok || l != l1 {
			t.Errorf("lock not recorded")
		}
		l2 := chunks.getSnapshotLock("k2")
		l3 := chunks.getSnapshotLock("k3")
		if l2 == nil || l3 == nil {
			t.Errorf("lock not returned")
		}
		ll := chunks.getSnapshotLock("k1")
		if l1 != ll {
			t.Errorf("lock changed")
		}
		if len(chunks.locks) != 3 {
			t.Errorf("%d locks, want 3", len(chunks.locks))
		}
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
	for idx, tt := range tests {
		c := &Chunk{validate: tt.validate}
		input := pb.Chunk{ChunkId: tt.chunkID, HasFileInfo: tt.hasFileInfo}
		if result := c.shouldValidate(input); result != tt.result {
			t.Errorf("%d, result %t, want %t", idx, result, tt.result)
		}
	}
}

func TestAddFirstChunkRecordsTheSnapshotAndCreatesTheTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		td, ok := chunks.tracked[chunkKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record last received time")
		}
		receiveTime := td.tick
		if receiveTime != chunks.getTick() {
			t.Errorf("unexpected time")
		}
		recordedChunk, ok := chunks.tracked[chunkKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record chunk")
		}
		expectedChunk := inputs[0]
		if !reflect.DeepEqual(&expectedChunk, &recordedChunk.first) {
			t.Errorf("chunk changed")
		}
		if !hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("no temp file")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGcRemovesRecordAndTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		chunks.addLocked(inputs[0])
		if !chunks.addLocked(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		count := chunks.timeout + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestReceivedCompleteChunkWillBeMergedIntoSnapshotFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		chunks.validate = false
		for _, c := range inputs {
			if !chunks.addLocked(c) {
				t.Errorf("failed to add chunk")
			}
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestChunkAreIgnoredWhenNodeIsRemoved(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunk()
		env := chunks.getEnv(inputs[0])
		chunks.validate = false
		if !chunks.addLocked(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		if !chunks.addLocked(inputs[1]) {
			t.Fatalf("failed to add chunk")
		}
		snapshotDir := env.GetRootDir()
		if err := fileutil.MarkDirAsDeleted(snapshotDir, &pb.Message{}, chunks.fs); err != nil {
			t.Fatalf("failed to create the delete flag %v", err)
		}
		for idx, c := range inputs {
			if idx <= 1 {
				continue
			}
			if chunks.addLocked(c) {
				t.Fatalf("chunks not rejected")
			}
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
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
		if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		chunks.validate = false
		for _, c := range inputs {
			chunks.addLocked(c)
		}
		if _, ok := chunks.tracked[chunkKey(inputs[0])]; ok {
			t.Errorf("failed to remove last received time")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
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
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
		// now we have the remaining chunks
		for _, c := range inputs[1:] {
			if chunks.addLocked(c) {
				t.Errorf("failed to reject chunks")
			}
		}
		_, ok = chunks.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
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
	defer f.Close()
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
			if !chunks.addLocked(c) {
				t.Errorf("chunk rejected")
			}
		}
		_, ok := chunks.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
		}
		if !checkTestSnapshotFile(chunks, inputs[0], settings.SnapshotHeaderSize*10) {
			t.Errorf("failed to generate the final snapshot file")
		}
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
		if err != nil {
			t.Fatalf("failed to get chunks %v", err)
		}
		for _, c := range inputs {
			c.DeploymentId = settings.UnmanagedDeploymentID
			c.Data = make([]byte, c.ChunkSize)
			added := chunks.addLocked(c)
			if snapshotCount == 0 && added {
				t.Errorf("failed to reject a chunk")
			}
		}
		if snapshotCount > 0 {
			if handler.getSnapshotCount(100, 2) != 1 {
				t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
			}
			if !hasExternalFile(chunks, inputs[0], "external1.data", 100, fs) ||
				!hasExternalFile(chunks, inputs[0], "external2.data", snapshotChunkSize+100, fs) {
				t.Errorf("external file missing")
			}
		} else {
			if handler.getSnapshotCount(100, 2) != 0 {
				t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
			}
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
		if err != nil {
			t.Fatalf("failed to get chunks %v", err)
		}
		if len(inputs) != 1 {
			t.Errorf("got %d chunks, want 1", len(inputs))
		}
		chunk := inputs[0]
		if chunk.BinVer != raftio.TransportBinVersion || !chunk.Witness ||
			chunk.ShardID != 100 || chunk.From != 1 || chunk.ReplicaID != 2 {
			t.Errorf("unexpected chunk %+v", chunk)
		}
		for _, c := range inputs {
			if len(c.Data) == 0 {
				t.Errorf("data is empty")
			}
			c.DeploymentId = settings.UnmanagedDeploymentID
			added := chunks.addLocked(c)
			if !added {
				t.Errorf("failed to add chunk")
			}
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("failed to receive snapshot")
		}
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
	if err != nil {
		t.Fatalf("failed to get chunks %v", err)
	}
	if len(chunks) != 4 {
		t.Errorf("got %d counts, want 4", len(chunks))
	}
	for _, c := range chunks {
		if c.BinVer != raftio.TransportBinVersion {
			t.Errorf("bin ver not set")
		}
		if c.ShardID != msg.ShardID {
			t.Errorf("unexpected shard id")
		}
		if c.ReplicaID != msg.To {
			t.Errorf("unexpected node id")
		}
		if c.From != msg.From {
			t.Errorf("unexpected from value")
		}
		if c.Index != msg.Snapshot.Index {
			t.Errorf("unexpected index")
		}
		if c.Term != msg.Snapshot.Term {
			t.Errorf("unexpected term")
		}
	}
	if chunks[0].HasFileInfo ||
		chunks[1].HasFileInfo ||
		chunks[2].HasFileInfo ||
		chunks[3].HasFileInfo {
		t.Errorf("unexpected has file info value")
	}
	if chunks[0].FileChunkId != 0 ||
		chunks[1].FileChunkId != 1 ||
		chunks[2].FileChunkId != 2 ||
		chunks[3].FileChunkId != 3 {
		t.Errorf("unexpected file chunk id")
	}
	if chunks[0].FileChunkCount != 4 ||
		chunks[1].FileChunkCount != 4 ||
		chunks[2].FileChunkCount != 4 ||
		chunks[3].FileChunkCount != 4 {
		t.Errorf("unexpected file chunk count")
	}
	if chunks[0].ChunkId != 0 ||
		chunks[1].ChunkId != 1 ||
		chunks[2].ChunkId != 2 ||
		chunks[3].ChunkId != 3 {
		t.Errorf("unexpected chunk id")
	}
	if chunks[0].ChunkSize+chunks[1].ChunkSize+
		chunks[2].ChunkSize+chunks[3].ChunkSize != ss.FileSize {
		t.Errorf("chunk size total != ss.FileSize")
	}
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
	if err != nil {
		t.Fatalf("failed to get chunks %v", err)
	}
	if len(chunks) != 7 {
		t.Errorf("unexpected chunk count")
	}
	total := uint64(0)
	for idx, c := range chunks {
		if c.ChunkId != uint64(idx) {
			t.Errorf("unexpected chunk id")
		}
		total += c.ChunkSize
	}
	if total != sf1.FileSize+sf2.FileSize+ss.FileSize {
		t.Errorf("file size count doesn't match")
	}
	if chunks[0].FileChunkId != 0 || chunks[4].FileChunkId != 0 || chunks[5].FileChunkId != 0 {
		t.Errorf("unexpected chunk partitions")
	}
	if chunks[4].FileChunkCount != 1 || chunks[5].FileChunkCount != 2 {
		t.Errorf("unexpected chunk count")
	}
	if chunks[4].FileSize != 100 || chunks[5].FileSize != snapshotChunkSize+100 {
		t.Errorf("unexpected file size")
	}
	for idx := range chunks {
		if idx >= 0 && idx < 4 {
			if chunks[idx].HasFileInfo {
				t.Errorf("unexpected has file info flag")
			}
		} else {
			if !chunks[idx].HasFileInfo {
				t.Errorf("missing file info flag")
			}
		}
	}
	if chunks[4].FileInfo.FileId != sf1.FileId ||
		chunks[5].FileInfo.FileId != sf2.FileId {
		t.Errorf("unexpected file id")
	}
	if len(chunks[4].FileInfo.Metadata) != 16 ||
		len(chunks[5].FileInfo.Metadata) != 32 {
		t.Errorf("unexpected metadata info")
	}
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
		if len(msg.Requests) != 1 {
			t.Errorf("unexpected request count")
		}
		if msg.BinVer != chunk.BinVer {
			t.Errorf("bin ver not copied")
		}
		req := msg.Requests[0]
		if msg.DeploymentId != chunk.DeploymentId {
			t.Errorf("deployment id not set")
		}
		if req.Type != pb.InstallSnapshot {
			t.Errorf("not a snapshot message")
		}
		if req.From != chunk.From || req.To != chunk.ReplicaID || req.ShardID != chunk.ShardID {
			t.Errorf("invalid req fields")
		}
		ss := req.Snapshot
		if len(ss.Files) != len(files) {
			t.Errorf("files count doesn't match")
		}
		if ss.FileSize != chunk.FileSize {
			t.Errorf("file size not set correctly")
		}
		if ss.Filepath != chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
			"snapshot-123-3", "snapshot-00000000000000C8", "test.data") {
			t.Errorf("unexpected file path, %s", ss.Filepath)
		}
		if len(ss.Files[0].Metadata) != len(sf1.Metadata) || len(ss.Files[1].Metadata) != len(sf2.Metadata) {
			t.Errorf("external files not set correctly")
		}
		if ss.Files[0].Filepath != chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
			"snapshot-123-3", "snapshot-00000000000000C8", "external-file-1") ||
			ss.Files[1].Filepath != chunks.fs.PathJoin("gtransport_test_data_safe_to_delete",
				"snapshot-123-3", "snapshot-00000000000000C8", "external-file-2") {
			t.Errorf("file path not set correctly, %s\n, %s", ss.Files[0].Filepath, ss.Files[1].Filepath)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}
