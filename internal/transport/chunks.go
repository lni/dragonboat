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
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/internal/server"
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/fileutil"
	"github.com/lni/dragonboat/internal/utils/logutil"
	"github.com/lni/dragonboat/raftio"
	pb "github.com/lni/dragonboat/raftpb"
)

var (
	// ErrSnapshotOutOfDate is returned when the snapshot being received is
	// considered as out of date.
	ErrSnapshotOutOfDate     = errors.New("snapshot is out of date")
	gcIntervalTick           = settings.Soft.SnapshotGCTick
	snapshotChunkTimeoutTick = settings.Soft.SnapshotChunkTimeoutTick
)

func snapshotKey(c pb.SnapshotChunk) string {
	return fmt.Sprintf("%d:%d:%d", c.ClusterId, c.NodeId, c.Index)
}

type tracked struct {
	firstChunk pb.SnapshotChunk
	extraFiles []*pb.SnapshotFile
	validator  *rsm.SnapshotValidator
	nextChunk  uint64
	tick       uint64
}

// chunks managed on the receiving side
type chunks struct {
	currentTick     uint64
	validate        bool
	getSnapshotDir  server.GetSnapshotDirFunc
	onReceive       func(pb.MessageBatch)
	confirm         func(uint64, uint64, uint64)
	getDeploymentID func() uint64
	tracked         map[string]*tracked
	timeoutTick     uint64
	gcTick          uint64
	mu              sync.Mutex
}

func newSnapshotChunks(onReceive func(pb.MessageBatch),
	confirm func(uint64, uint64, uint64),
	getDeploymentID func() uint64,
	getSnapshotDirFunc server.GetSnapshotDirFunc) *chunks {
	return &chunks{
		validate:        true,
		onReceive:       onReceive,
		confirm:         confirm,
		getDeploymentID: getDeploymentID,
		tracked:         make(map[string]*tracked),
		timeoutTick:     snapshotChunkTimeoutTick,
		gcTick:          gcIntervalTick,
		getSnapshotDir:  getSnapshotDirFunc,
	}
}

func (c *chunks) AddChunk(chunk pb.SnapshotChunk) {
	did := c.getDeploymentID()
	if chunk.DeploymentId != did ||
		chunk.BinVer != raftio.RPCBinVersion {
		return
	}
	c.addChunk(chunk)
}

func (c *chunks) Tick() {
	ct := atomic.AddUint64(&c.currentTick, 1)
	if ct%c.gcTick == 0 {
		c.gc()
	}
}

func (c *chunks) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tracked {
		c.deleteTempChunkDir(t.firstChunk)
	}
}

func (c *chunks) gc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	tick := c.getCurrentTick()
	for k, td := range c.tracked {
		if tick-td.tick >= c.timeoutTick {
			c.deleteTempChunkDir(td.firstChunk)
			c.resetSnapshotLocked(k)
		}
	}
}

func (c *chunks) getCurrentTick() uint64 {
	return atomic.LoadUint64(&c.currentTick)
}

func (c *chunks) resetSnapshot(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetSnapshotLocked(key)
}

func (c *chunks) resetSnapshotLocked(key string) {
	delete(c.tracked, key)
}

// returns whether this incoming chunk needs to be processed
func (c *chunks) onNewChunk(key string,
	td *tracked, chunk pb.SnapshotChunk) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if chunk.ChunkId == 0 {
		plog.Infof("new snapshot chunk 0, key %s", key)
		if td != nil {
			plog.Warningf("removing unclaimed chunks %s", key)
			c.deleteTempChunkDir(td.firstChunk)
		}
		td = &tracked{
			firstChunk: chunk,
			validator:  rsm.NewSnapshotValidator(),
			nextChunk:  1,
			extraFiles: make([]*pb.SnapshotFile, 0),
		}
		c.tracked[key] = td
	} else {
		if td == nil {
			// not tracked,
			// the receiving end probably had an recoverable error and restarted
			return false
		}
		if td.nextChunk != chunk.ChunkId {
			return false
		}
		td.nextChunk = chunk.ChunkId + 1
	}
	if chunk.FileChunkId == 0 && chunk.HasFileInfo {
		td.extraFiles = append(td.extraFiles, &chunk.FileInfo)
	}
	td.tick = c.getCurrentTick()
	return true
}

func (c *chunks) addChunk(chunk pb.SnapshotChunk) {
	key := snapshotKey(chunk)
	td := c.tracked[key]
	if !c.onNewChunk(key, td, chunk) {
		plog.Warningf("ignored a chunk belongs to %s", key)
		return
	}
	td = c.tracked[key]
	if c.validate && !td.validator.AddChunk(chunk.Data, chunk.ChunkId) {
		plog.Warningf("ignored a invalid chunk %s", key)
		return
	}
	if err := c.saveChunk(chunk); err != nil {
		plog.Errorf("failed to save a chunk %s, %v", key, err)
		c.deleteTempChunkDir(chunk)
		panic(err)
	}
	if chunk.ChunkCount == chunk.ChunkId+1 {
		plog.Infof("last chunk %s received", key)
		defer c.resetSnapshot(key)
		if c.validate && !chunk.HasFileInfo && !td.validator.Validate() {
			plog.Warningf("dropped an invalid snapshot %s", key)
			c.deleteTempChunkDir(chunk)
			return
		}
		if err := c.finalizeSnapshotFile(chunk, td); err != nil {
			c.deleteTempChunkDir(chunk)
			if err != ErrSnapshotOutOfDate {
				plog.Panicf("%s failed when finalizing, %v", key, err)
			}
			if !c.flagFileExists(chunk) {
				plog.Warningf("out-of-date chunk without flag file %s", key)
				return
			}
		}
		snapshotMessage := c.toMessage(td.firstChunk, td.extraFiles)
		plog.Infof("%s received snapshot from %d, idx %d, term %d",
			logutil.DescribeNode(chunk.ClusterId, chunk.NodeId),
			chunk.From, chunk.Index, chunk.Term)
		c.onReceive(snapshotMessage)
		c.confirm(chunk.ClusterId, chunk.NodeId, chunk.From)
	}
}

func (c *chunks) saveChunk(chunk pb.SnapshotChunk) error {
	env := c.getSnapshotEnv(chunk)
	if chunk.ChunkId == 0 {
		if err := env.CreateTempDir(); err != nil {
			return err
		}
	}
	fn := filepath.Base(chunk.Filepath)
	fp := filepath.Join(env.GetTempDir(), fn)
	var f *fileutil.ChunkFile
	var err error
	if chunk.FileChunkId == 0 {
		f, err = fileutil.CreateChunkFile(fp)
	} else {
		f, err = fileutil.OpenChunkFileForAppend(fp)
	}
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := f.Write(chunk.Data)
	if err != nil {
		return err
	}
	if len(chunk.Data) != n {
		return io.ErrShortWrite
	}
	if chunk.FileChunkId+1 == chunk.FileChunkCount {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (c *chunks) generateFlagFile(td *tracked, env *server.SnapshotEnv) error {
	msg := c.toMessage(td.firstChunk, td.extraFiles)
	if len(msg.Requests) != 1 || msg.Requests[0].Type != pb.InstallSnapshot {
		panic("invalid message")
	}
	return env.CreateFlagFile(&msg.Requests[0].Snapshot)
}

func (c *chunks) getSnapshotEnv(chunk pb.SnapshotChunk) *server.SnapshotEnv {
	return server.NewSnapshotEnv(c.getSnapshotDir,
		chunk.ClusterId, chunk.NodeId, chunk.Index, chunk.From,
		server.ReceivingMode)
}

func (c *chunks) flagFileExists(chunk pb.SnapshotChunk) bool {
	env := c.getSnapshotEnv(chunk)
	return env.HasFlagFile()
}

func (c *chunks) finalizeSnapshotFile(chunk pb.SnapshotChunk,
	td *tracked) error {
	env := c.getSnapshotEnv(chunk)
	if err := c.generateFlagFile(td, env); err != nil {
		return err
	}
	if !env.IsFinalDirExists() {
		if outOfDate, err := env.RenameTempDirToFinalDir(); err != nil {
			if outOfDate {
				return ErrSnapshotOutOfDate
			}
			return err
		}
	} else {
		plog.Warningf("snapshot dir %s exists, remove the tmp dir",
			env.GetFinalDir())
		return ErrSnapshotOutOfDate
	}
	return nil
}

func (c *chunks) deleteTempChunkDir(chunk pb.SnapshotChunk) {
	env := c.getSnapshotEnv(chunk)
	env.MustRemoveTempDir()
}

func (c *chunks) toMessage(chunk pb.SnapshotChunk,
	files []*pb.SnapshotFile) pb.MessageBatch {
	if chunk.ChunkId != 0 {
		panic("first chunk must be used to reconstruct the snapshot")
	}
	env := c.getSnapshotEnv(chunk)
	snapDir := env.GetFinalDir()
	m := pb.Message{}
	m.Type = pb.InstallSnapshot
	m.From = chunk.From
	m.To = chunk.NodeId
	m.ClusterId = chunk.ClusterId
	s := pb.Snapshot{}
	s.Index = chunk.Index
	s.Term = chunk.Term
	s.Membership = chunk.Membership
	fn := filepath.Base(chunk.Filepath)
	s.Filepath = filepath.Join(snapDir, fn)
	s.FileSize = chunk.FileSize
	m.Snapshot = s
	m.Snapshot.Files = files
	for idx := range m.Snapshot.Files {
		fp := filepath.Join(snapDir, m.Snapshot.Files[idx].Filename())
		m.Snapshot.Files[idx].Filepath = fp
	}
	return pb.MessageBatch{
		BinVer:       chunk.BinVer,
		DeploymentId: chunk.DeploymentId,
		Requests:     []pb.Message{m},
	}
}
