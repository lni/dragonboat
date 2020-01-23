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
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/leaktest"
)

const (
	testDeploymentID uint64 = 0
)

func getTestChunks() []pb.SnapshotChunk {
	result := make([]pb.SnapshotChunk, 0)
	for chunkID := uint64(0); chunkID < 10; chunkID++ {
		c := pb.SnapshotChunk{
			BinVer:         raftio.RPCBinVersion,
			ClusterId:      100,
			NodeId:         2,
			From:           12,
			FileChunkId:    chunkID,
			FileChunkCount: 10,
			ChunkId:        chunkID,
			ChunkSize:      100,
			ChunkCount:     10,
			Index:          1,
			Term:           1,
			Filepath:       "snapshot-0000000000000001.gbsnap",
			FileSize:       10 * rsm.SnapshotHeaderSize,
		}
		data := make([]byte, rsm.SnapshotHeaderSize)
		rand.Read(data)
		c.Data = data
		result = append(result, c)
	}
	return result
}

func hasSnapshotTempFile(cs *Chunks, c pb.SnapshotChunk) bool {
	env := cs.getSSEnv(c)
	fp := env.GetTempFilepath()
	if _, err := cs.fs.Stat(fp); os.IsNotExist(err) {
		return false
	}
	return true
}

func hasExternalFile(cs *Chunks, c pb.SnapshotChunk, fn string, sz uint64) bool {
	ifs := vfs.GetTestFS()
	env := cs.getSSEnv(c)
	efp := ifs.PathJoin(env.GetFinalDir(), fn)
	fs, err := cs.fs.Stat(efp)
	if os.IsNotExist(err) {
		return false
	}
	return uint64(fs.Size()) == sz
}

func getTestDeploymentID() uint64 {
	return testDeploymentID
}

func runChunkTest(t *testing.T, fn func(*testing.T, *Chunks, *testMessageHandler)) {
	fs := vfs.GetTestFS()
	defer fs.RemoveAll(snapshotDir)
	defer leaktest.AfterTest(t)()
	trans, _, stopper, tt := newTestTransport(false)
	defer trans.serverCtx.Stop()
	defer trans.Stop()
	defer stopper.Stop()
	defer tt.cleanup()
	handler := newTestMessageHandler()
	trans.SetMessageHandler(handler)
	chunks := NewSnapshotChunks(trans.handleRequest,
		trans.snapshotReceived, getTestDeploymentID,
		trans.snapshotLocator, fs)
	ts := getTestChunks()
	snapDir := chunks.getSnapshotDir(ts[0].ClusterId, ts[0].NodeId)
	if err := fs.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("%v", err)
	}
	fn(t, chunks, handler)
}

func TestMaxSlotIsEnforced(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		defer chunks.fs.RemoveAll(snapshotDir)
		inputs := getTestChunks()
		chunks.validate = false
		v := uint64(1)
		c := inputs[0]
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ClusterId = v
			snapDir := chunks.getSnapshotDir(v, c.NodeId)
			if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
				t.Fatalf("%v", err)
			}
			if !chunks.addChunk(c) {
				t.Errorf("failed to add chunk")
			}
		}
		count := len(chunks.tracked)
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ClusterId = v
			if chunks.addChunk(c) {
				t.Errorf("not rejected")
			}
		}
		if len(chunks.tracked) != count {
			t.Errorf("tracked count changed")
		}
	}
	runChunkTest(t, fn)
}

func TestOutOfOrderChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		key := snapshotKey(inputs[0])
		td := chunks.tracked[key]
		next := td.nextChunk
		td.nextChunk = next + 10
		if chunks.onNewChunk(inputs[1]) != nil {
			t.Fatalf("out of order chunk is not rejected")
		}
		td = chunks.tracked[key]
		if next+10 != td.nextChunk {
			t.Fatalf("next chunk id unexpected moved")
		}
	}
	runChunkTest(t, fn)
}

func TestChunkFromANewLeaderIsIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		key := snapshotKey(inputs[0])
		td := chunks.tracked[key]
		next := td.nextChunk
		td.firstChunk.From = td.firstChunk.From + 1
		if chunks.onNewChunk(inputs[1]) != nil {
			t.Fatalf("chunk from a different leader is not rejected")
		}
		td = chunks.tracked[key]
		if next != td.nextChunk {
			t.Fatalf("next chunk id unexpected moved")
		}
	}
	runChunkTest(t, fn)
}

func TestNotTrackedChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		if chunks.onNewChunk(inputs[1]) != nil {
			t.Errorf("not tracked chunk not rejected")
		}
	}
	runChunkTest(t, fn)
}

func TestGetOrCreateSnapshotLock(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		l := chunks.getOrCreateSnapshotLock("k1")
		l1, ok := chunks.locks["k1"]
		if !ok || l != l1 {
			t.Errorf("lock not recorded")
		}
		l2 := chunks.getOrCreateSnapshotLock("k2")
		l3 := chunks.getOrCreateSnapshotLock("k3")
		if l2 == nil || l3 == nil {
			t.Errorf("lock not returned")
		}
		ll := chunks.getOrCreateSnapshotLock("k1")
		if l1 != ll {
			t.Errorf("lock changed")
		}
		if len(chunks.locks) != 3 {
			t.Errorf("%d locks, want 3", len(chunks.locks))
		}
	}
	runChunkTest(t, fn)
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
		c := &Chunks{validate: tt.validate}
		input := pb.SnapshotChunk{ChunkId: tt.chunkID, HasFileInfo: tt.hasFileInfo}
		if result := c.shouldUpdateValidator(input); result != tt.result {
			t.Errorf("%d, result %t, want %t", idx, result, tt.result)
		}
	}
}

func TestAddFirstChunkRecordsTheSnapshotAndCreatesTheTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		td, ok := chunks.tracked[snapshotKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record last received time")
		}
		receiveTime := td.tick
		if receiveTime != chunks.getCurrentTick() {
			t.Errorf("unexpected time")
		}
		recordedChunk, ok := chunks.tracked[snapshotKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record chunk")
		}
		expectedChunk := inputs[0]
		if !reflect.DeepEqual(&expectedChunk, &recordedChunk.firstChunk) {
			t.Errorf("chunk changed")
		}
		if !hasSnapshotTempFile(chunks, inputs[0]) {
			t.Errorf("no temp file")
		}
	}
	runChunkTest(t, fn)
}

func TestGcRemovesRecordAndTempFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		if !chunks.addChunk(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		count := chunks.timeoutTick + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.tracked[snapshotKey(inputs[0])]
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
	runChunkTest(t, fn)
}

func TestReceivedCompleteChunksWillBeMergedIntoSnapshotFile(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		for _, c := range inputs {
			if !chunks.addChunk(c) {
				t.Errorf("failed to add chunk")
			}
		}
		_, ok := chunks.tracked[snapshotKey(inputs[0])]
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
	runChunkTest(t, fn)
}

func TestChunksAreIgnoredWhenNodeIsRemoved(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		env := chunks.getSSEnv(inputs[0])
		chunks.validate = false
		if !chunks.addChunk(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		if !chunks.addChunk(inputs[1]) {
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
			if chunks.addChunk(c) {
				t.Fatalf("chunks not rejected")
			}
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !os.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
	}
	runChunkTest(t, fn)
}

// when there is no flag file
func TestOutOfDateSnapshotChunksCanBeHandled(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		env := chunks.getSSEnv(inputs[0])
		snapDir := env.GetFinalDir()
		if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		chunks.validate = false
		for _, c := range inputs {
			chunks.addChunk(c)
		}
		if _, ok := chunks.tracked[snapshotKey(inputs[0])]; ok {
			t.Errorf("failed to remove last received time")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !os.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
	}
	runChunkTest(t, fn)
}

func TestSignificantlyDelayedNonFirstChunksAreIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		count := chunks.timeoutTick + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.tracked[snapshotKey(inputs[0])]
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
			if chunks.addChunk(c) {
				t.Errorf("failed to reject chunks")
			}
		}
		_, ok = chunks.tracked[snapshotKey(inputs[0])]
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
	runChunkTest(t, fn)
}

func checkTestSnapshotFile(chunks *Chunks,
	chunk pb.SnapshotChunk, size uint64) bool {
	env := chunks.getSSEnv(chunk)
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
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.validate = false
		chunks.addChunk(inputs[0])
		chunks.addChunk(inputs[1])
		chunks.addChunk(inputs[2])
		inputs = getTestChunks()
		// now add everything
		for _, c := range inputs {
			if !chunks.addChunk(c) {
				t.Errorf("chunk rejected")
			}
		}
		_, ok := chunks.tracked[snapshotKey(inputs[0])]
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
	runChunkTest(t, fn)
}

func testSnapshotWithExternalFilesAreHandledByChunks(t *testing.T,
	validate bool, snapshotCount uint64) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
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
			Type:      pb.InstallSnapshot,
			To:        2,
			From:      1,
			ClusterId: 100,
			Snapshot:  ss,
		}
		inputs := splitSnapshotMessage(msg, chunks.fs)
		for _, c := range inputs {
			c.Data = make([]byte, c.ChunkSize)
			added := chunks.addChunk(c)
			if snapshotCount == 0 && added {
				t.Errorf("failed to reject a chunk")
			}
		}
		if snapshotCount > 0 {
			if handler.getSnapshotCount(100, 2) != 1 {
				t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
			}
			if !hasExternalFile(chunks, inputs[0], "external1.data", 100) ||
				!hasExternalFile(chunks, inputs[0], "external2.data", snapshotChunkSize+100) {
				t.Errorf("external file missing")
			}
		} else {
			if handler.getSnapshotCount(100, 2) != 0 {
				t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
			}
		}
	}
	runChunkTest(t, fn)
}

func TestSnapshotWithExternalFilesAreHandledByChunks(t *testing.T) {
	testSnapshotWithExternalFilesAreHandledByChunks(t, true, 0)
	testSnapshotWithExternalFilesAreHandledByChunks(t, false, 1)
}

func TestWitnessSnapshotCanBeHandled(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
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
			Type:      pb.InstallSnapshot,
			To:        2,
			From:      1,
			ClusterId: 100,
			Snapshot:  ss,
		}
		inputs := splitSnapshotMessage(msg, chunks.fs)
		if len(inputs) != 1 {
			t.Errorf("got %d chunks, want 1", len(inputs))
		}
		chunk := inputs[0]
		if chunk.BinVer != raftio.RPCBinVersion || !chunk.Witness ||
			chunk.ClusterId != 100 || chunk.From != 1 || chunk.NodeId != 2 {
			t.Errorf("unexpected chunk %+v", chunk)
		}
		for _, c := range inputs {
			if len(c.Data) == 0 {
				t.Errorf("data is empty")
			}
			added := chunks.addChunk(c)
			if !added {
				t.Errorf("failed to add chunk")
			}
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("failed to receive snapshot")
		}
	}
	runChunkTest(t, fn)
}

func TestSnapshotRecordWithoutExternalFilesCanBeSplitIntoChunks(t *testing.T) {
	fs := vfs.GetTestFS()
	ss := pb.Snapshot{
		Filepath: "filepath.data",
		FileSize: snapshotChunkSize*3 + 100,
		Index:    100,
		Term:     200,
	}
	msg := pb.Message{
		Type:      pb.InstallSnapshot,
		To:        2,
		From:      1,
		ClusterId: 100,
		Snapshot:  ss,
	}
	chunks := splitSnapshotMessage(msg, fs)
	if len(chunks) != 4 {
		t.Errorf("got %d counts, want 4", len(chunks))
	}
	for _, c := range chunks {
		if c.BinVer != raftio.RPCBinVersion {
			t.Errorf("bin ver not set")
		}
		if c.ClusterId != msg.ClusterId {
			t.Errorf("unexpected cluster id")
		}
		if c.NodeId != msg.To {
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

func TestSnapshotRecordWithTwoExternalFilesCanBeSplitIntoChunks(t *testing.T) {
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
		Type:      pb.InstallSnapshot,
		To:        2,
		From:      1,
		ClusterId: 100,
		Snapshot:  ss,
	}
	chunks := splitSnapshotMessage(msg, fs)
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
	fn := func(t *testing.T, chunks *Chunks, handler *testMessageHandler) {
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
		chunk := pb.SnapshotChunk{
			ClusterId:    123,
			NodeId:       3,
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
		if req.From != chunk.From || req.To != chunk.NodeId || req.ClusterId != chunk.ClusterId {
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
	runChunkTest(t, fn)
}
