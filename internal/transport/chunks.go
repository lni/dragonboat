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
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	// ErrSnapshotOutOfDate is returned when the snapshot being received is
	// considered as out of date.
	ErrSnapshotOutOfDate     = errors.New("snapshot is out of date")
	gcIntervalTick           = settings.Soft.SnapshotGCTick
	snapshotChunkTimeoutTick = settings.Soft.SnapshotChunkTimeoutTick
	maxConcurrentSlot        = settings.Soft.MaxConcurrentStreamingSnapshot
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

type snapshotLock struct {
	mu sync.Mutex
}

func (l *snapshotLock) lock() {
	l.mu.Lock()
}

func (l *snapshotLock) unlock() {
	l.mu.Unlock()
}

// Chunks managed on the receiving side
type Chunks struct {
	currentTick     uint64
	validate        bool
	getSnapshotDir  server.GetSnapshotDirFunc
	onReceive       func(pb.MessageBatch)
	confirm         func(uint64, uint64, uint64)
	getDeploymentID func() uint64
	tracked         map[string]*tracked
	locks           map[string]*snapshotLock
	timeoutTick     uint64
	gcTick          uint64
	mu              sync.Mutex
}

// NewSnapshotChunks creates and returns a new snapshot chunks instance.
func NewSnapshotChunks(onReceive func(pb.MessageBatch),
	confirm func(uint64, uint64, uint64),
	getDeploymentID func() uint64,
	getSnapshotDirFunc server.GetSnapshotDirFunc) *Chunks {
	return &Chunks{
		validate:        true,
		onReceive:       onReceive,
		confirm:         confirm,
		getDeploymentID: getDeploymentID,
		tracked:         make(map[string]*tracked),
		locks:           make(map[string]*snapshotLock),
		timeoutTick:     snapshotChunkTimeoutTick,
		gcTick:          gcIntervalTick,
		getSnapshotDir:  getSnapshotDirFunc,
	}
}

// AddChunk adds an received trunk to chunks.
func (c *Chunks) AddChunk(chunk pb.SnapshotChunk) bool {
	did := c.getDeploymentID()
	if chunk.DeploymentId != did ||
		chunk.BinVer != raftio.RPCBinVersion {
		return false
	}
	return c.addChunk(chunk)
}

// Tick moves the internal logical clock forward.
func (c *Chunks) Tick() {
	ct := atomic.AddUint64(&c.currentTick, 1)
	if ct%c.gcTick == 0 {
		c.gc()
	}
}

// Close closes the chunks instance.
func (c *Chunks) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tracked {
		c.deleteTempChunkDir(t.firstChunk)
	}
}

func (c *Chunks) gc() {
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

func (c *Chunks) getCurrentTick() uint64 {
	return atomic.LoadUint64(&c.currentTick)
}

func (c *Chunks) resetSnapshot(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetSnapshotLocked(key)
}

func (c *Chunks) resetSnapshotLocked(key string) {
	delete(c.tracked, key)
}

func (c *Chunks) getOrCreateSnapshotLock(key string) *snapshotLock {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locks[key]
	if !ok {
		l = &snapshotLock{}
		c.locks[key] = l
	}
	return l
}

func (c *Chunks) canAddNewTracked() bool {
	return uint64(len(c.tracked)) < maxConcurrentSlot
}

func (c *Chunks) onNewChunk(chunk pb.SnapshotChunk) *tracked {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := snapshotKey(chunk)
	td := c.tracked[key]
	if chunk.ChunkId == 0 {
		plog.Infof("received the first chunk of a snapshot, key %s", key)
		if td != nil {
			plog.Warningf("removing unclaimed chunks %s", key)
			c.deleteTempChunkDir(td.firstChunk)
		} else {
			if !c.canAddNewTracked() {
				plog.Errorf("max slot count reached, dropped a chunk %s", key)
				return nil
			}
		}
		validator := rsm.NewSnapshotValidator()
		if c.validate && !chunk.HasFileInfo {
			if !validator.AddChunk(chunk.Data, chunk.ChunkId) {
				return nil
			}
		}
		td = &tracked{
			nextChunk:  1,
			firstChunk: chunk,
			validator:  validator,
			extraFiles: make([]*pb.SnapshotFile, 0),
		}
		c.tracked[key] = td
	} else {
		if td == nil {
			plog.Errorf("ignored a not tracked chunk %s, chunk id %d",
				key, chunk.ChunkId)
			return nil
		}
		if td.nextChunk != chunk.ChunkId {
			plog.Errorf("ignored out of order chunk %s, want chunk id %d, got %d",
				key, td.nextChunk, chunk.ChunkId)
			return nil
		}
		if td.firstChunk.From != chunk.From {
			plog.Errorf("ignored chunk %s, expected from %d, from %d",
				key, td.firstChunk.From, chunk.From)
			return nil
		}
		td.nextChunk = chunk.ChunkId + 1
	}
	if chunk.FileChunkId == 0 && chunk.HasFileInfo {
		td.extraFiles = append(td.extraFiles, &chunk.FileInfo)
	}
	td.tick = c.getCurrentTick()
	return td
}

func (c *Chunks) shouldUpdateValidator(chunk pb.SnapshotChunk) bool {
	return c.validate && !chunk.HasFileInfo && chunk.ChunkId != 0
}

func (c *Chunks) addChunk(chunk pb.SnapshotChunk) bool {
	key := snapshotKey(chunk)
	lock := c.getOrCreateSnapshotLock(key)
	lock.lock()
	defer lock.unlock()
	td := c.onNewChunk(chunk)
	if td == nil {
		plog.Warningf("ignored a chunk belongs to %s", key)
		return false
	}
	if c.shouldUpdateValidator(chunk) {
		if !td.validator.AddChunk(chunk.Data, chunk.ChunkId) {
			plog.Warningf("ignored a invalid chunk %s", key)
			return false
		}
	}
	removed, err := c.nodeRemoved(chunk)
	if err != nil {
		panic(err)
	}
	if removed {
		c.deleteTempChunkDir(chunk)
		plog.Warningf("node removed, ignored chunk %s", key)
		return false
	}
	if err := c.saveChunk(chunk); err != nil {
		plog.Errorf("failed to save a chunk %s, %v", key, err)
		c.deleteTempChunkDir(chunk)
		panic(err)
	}
	if chunk.IsLastChunk() {
		plog.Infof("last chunk %s received", key)
		defer c.resetSnapshot(key)
		if c.validate {
			if !td.validator.Validate() {
				plog.Warningf("dropped an invalid snapshot %s", key)
				c.deleteTempChunkDir(chunk)
				return false
			}
		}
		if err := c.finalizeSnapshot(chunk, td); err != nil {
			c.deleteTempChunkDir(chunk)
			if err != ErrSnapshotOutOfDate {
				plog.Panicf("%s failed when finalizing, %v", key, err)
			}
			return false
		}
		snapshotMessage := c.toMessage(td.firstChunk, td.extraFiles)
		plog.Infof("%s received snapshot from %d, idx %d, term %d",
			logutil.DescribeNode(chunk.ClusterId, chunk.NodeId),
			chunk.From, chunk.Index, chunk.Term)
		c.onReceive(snapshotMessage)
		c.confirm(chunk.ClusterId, chunk.NodeId, chunk.From)
	}
	return true
}

func (c *Chunks) nodeRemoved(chunk pb.SnapshotChunk) (bool, error) {
	env := c.getSnapshotEnv(chunk)
	dir := env.GetRootDir()
	return fileutil.IsDirMarkedAsDeleted(dir)
}

func (c *Chunks) saveChunk(chunk pb.SnapshotChunk) error {
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
	if chunk.IsLastChunk() || chunk.IsLastFileChunk() {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunks) getSnapshotEnv(chunk pb.SnapshotChunk) *server.SnapshotEnv {
	return server.NewSnapshotEnv(c.getSnapshotDir,
		chunk.ClusterId, chunk.NodeId, chunk.Index, chunk.From,
		server.ReceivingMode)
}

func (c *Chunks) finalizeSnapshot(chunk pb.SnapshotChunk, td *tracked) error {
	env := c.getSnapshotEnv(chunk)
	msg := c.toMessage(td.firstChunk, td.extraFiles)
	if len(msg.Requests) != 1 || msg.Requests[0].Type != pb.InstallSnapshot {
		panic("invalid message")
	}
	ss := &msg.Requests[0].Snapshot
	err := env.FinalizeSnapshot(ss)
	if err == server.ErrSnapshotOutOfDate {
		return ErrSnapshotOutOfDate
	}
	return err
}

func (c *Chunks) deleteTempChunkDir(chunk pb.SnapshotChunk) {
	env := c.getSnapshotEnv(chunk)
	env.MustRemoveTempDir()
}

func (c *Chunks) toMessage(chunk pb.SnapshotChunk,
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
	s.OnDiskIndex = chunk.OnDiskIndex
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
