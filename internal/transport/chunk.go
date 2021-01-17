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
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/vfs"
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

func chunkKey(c pb.Chunk) string {
	return fmt.Sprintf("%d:%d:%d", c.ClusterId, c.NodeId, c.Index)
}

type tracked struct {
	tick      uint64
	next      uint64
	first     pb.Chunk
	files     []*pb.SnapshotFile
	validator *rsm.SnapshotValidator
}

type ssLock struct {
	mu sync.Mutex
}

func (l *ssLock) lock() {
	l.mu.Lock()
}

func (l *ssLock) unlock() {
	l.mu.Unlock()
}

// Chunk managed on the receiving side
type Chunk struct {
	did       uint64
	tick      uint64
	validate  bool
	dir       server.SnapshotDirFunc
	onReceive func(pb.MessageBatch)
	confirm   func(uint64, uint64, uint64)
	tracked   map[string]*tracked
	locks     map[string]*ssLock
	timeout   uint64
	gcTick    uint64
	fs        vfs.IFS
	mu        sync.Mutex
}

// NewChunk creates and returns a new snapshot chunks instance.
func NewChunk(onReceive func(pb.MessageBatch),
	confirm func(uint64, uint64, uint64), dir server.SnapshotDirFunc,
	did uint64, fs vfs.IFS) *Chunk {
	return &Chunk{
		did:       did,
		validate:  true,
		onReceive: onReceive,
		confirm:   confirm,
		tracked:   make(map[string]*tracked),
		locks:     make(map[string]*ssLock),
		timeout:   snapshotChunkTimeoutTick,
		gcTick:    gcIntervalTick,
		dir:       dir,
		fs:        fs,
	}
}

// Add adds a received trunk to chunks.
func (c *Chunk) Add(chunk pb.Chunk) bool {
	if chunk.DeploymentId != c.did ||
		chunk.BinVer != raftio.TransportBinVersion {
		plog.Errorf("invalid did or binver, %d, %d, %d, %d",
			chunk.DeploymentId, c.did, chunk.BinVer, raftio.TransportBinVersion)
		return false
	}
	key := chunkKey(chunk)
	lock := c.getSnapshotLock(key)
	lock.lock()
	defer lock.unlock()
	return c.addLocked(chunk)
}

// Tick moves the internal logical clock forward.
func (c *Chunk) Tick() {
	ct := atomic.AddUint64(&c.tick, 1)
	if ct%c.gcTick == 0 {
		c.gc()
	}
}

// Close closes the chunks instance.
func (c *Chunk) Close() {
	tracked := c.getTracked()
	for key, td := range tracked {
		func() {
			l := c.getSnapshotLock(key)
			l.lock()
			defer l.unlock()
			c.removeTempDir(td.first)
			c.reset(key)
		}()
	}
}

func (c *Chunk) gc() {
	tracked := c.getTracked()
	tick := c.getTick()
	for key, td := range tracked {
		func() {
			l := c.getSnapshotLock(key)
			l.lock()
			defer l.unlock()
			if tick-td.tick >= c.timeout {
				c.removeTempDir(td.first)
				c.reset(key)
			}
		}()
	}
}

func (c *Chunk) getTick() uint64 {
	return atomic.LoadUint64(&c.tick)
}

func (c *Chunk) reset(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked(key)
}

func (c *Chunk) getTracked() map[string]*tracked {
	m := make(map[string]*tracked)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.tracked {
		m[k] = v
	}
	return m
}

func (c *Chunk) resetLocked(key string) {
	delete(c.tracked, key)
}

func (c *Chunk) getSnapshotLock(key string) *ssLock {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locks[key]
	if !ok {
		l = &ssLock{}
		c.locks[key] = l
	}
	return l
}

func (c *Chunk) full() bool {
	return uint64(len(c.tracked)) >= maxConcurrentSlot
}

func (c *Chunk) record(chunk pb.Chunk) *tracked {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := chunkKey(chunk)
	td := c.tracked[key]
	if chunk.ChunkId == 0 {
		plog.Debugf("first chunk of %s received", c.ssid(chunk))
		if td != nil {
			plog.Warningf("removing unclaimed chunks %s", key)
			c.removeTempDir(td.first)
		} else {
			if c.full() {
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
			next:      1,
			first:     chunk,
			validator: validator,
			files:     make([]*pb.SnapshotFile, 0),
		}
		c.tracked[key] = td
	} else {
		if td == nil {
			plog.Errorf("not tracked chunk %s ignored, id %d", key, chunk.ChunkId)
			return nil
		}
		if td.next != chunk.ChunkId {
			plog.Errorf("out of order, %s, want %d, got %d",
				key, td.next, chunk.ChunkId)
			return nil
		}
		from := chunk.From
		want := td.first.From
		if want != from {
			from := chunk.From
			want := td.first.From
			plog.Errorf("ignored %s, from %d, want %d", key, from, want)
			return nil
		}
		td.next = chunk.ChunkId + 1
	}
	if chunk.FileChunkId == 0 && chunk.HasFileInfo {
		td.files = append(td.files, &chunk.FileInfo)
	}
	td.tick = c.getTick()
	return td
}

func (c *Chunk) shouldValidate(chunk pb.Chunk) bool {
	return c.validate && !chunk.HasFileInfo && chunk.ChunkId != 0
}

func (c *Chunk) addLocked(chunk pb.Chunk) bool {
	key := chunkKey(chunk)
	td := c.record(chunk)
	if td == nil {
		plog.Warningf("ignored a chunk belongs to %s", key)
		return false
	}
	removed, err := c.nodeRemoved(chunk)
	if err != nil {
		panic(err)
	}
	if removed {
		c.removeTempDir(chunk)
		plog.Warningf("node removed, ignored chunk %s", key)
		return false
	}
	if c.shouldValidate(chunk) {
		if !td.validator.AddChunk(chunk.Data, chunk.ChunkId) {
			plog.Warningf("ignored a invalid chunk %s", key)
			return false
		}
	}
	if err := c.save(chunk); err != nil {
		plog.Errorf("failed to save a chunk %s, %v", key, err)
		c.removeTempDir(chunk)
		panic(err)
	}
	if chunk.IsLastChunk() {
		plog.Debugf("last chunk %s received", key)
		defer c.reset(key)
		if c.validate {
			if !td.validator.Validate() {
				plog.Warningf("dropped an invalid snapshot %s", key)
				c.removeTempDir(chunk)
				return false
			}
		}
		if err := c.finalize(chunk, td); err != nil {
			c.removeTempDir(chunk)
			if err != ErrSnapshotOutOfDate {
				plog.Panicf("%s failed when finalizing, %v", key, err)
			}
			return false
		}
		snapshotMessage := c.toMessage(td.first, td.files)
		plog.Debugf("%s received from %d, term %d",
			c.ssid(chunk), chunk.From, chunk.Term)
		c.onReceive(snapshotMessage)
		c.confirm(chunk.ClusterId, chunk.NodeId, chunk.From)
	}
	return true
}

func (c *Chunk) nodeRemoved(chunk pb.Chunk) (bool, error) {
	env := c.getEnv(chunk)
	dir := env.GetRootDir()
	return fileutil.IsDirMarkedAsDeleted(dir, c.fs)
}

func (c *Chunk) save(chunk pb.Chunk) (err error) {
	env := c.getEnv(chunk)
	if chunk.ChunkId == 0 {
		if err := env.CreateTempDir(); err != nil {
			return err
		}
	}
	fn := c.fs.PathBase(chunk.Filepath)
	fp := c.fs.PathJoin(env.GetTempDir(), fn)
	var f *ChunkFile
	if chunk.FileChunkId == 0 {
		f, err = CreateChunkFile(fp, c.fs)
	} else {
		f, err = OpenChunkFileForAppend(fp, c.fs)
	}
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
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

func (c *Chunk) getEnv(chunk pb.Chunk) server.SSEnv {
	return server.NewSSEnv(c.dir, chunk.ClusterId, chunk.NodeId,
		chunk.Index, chunk.From, server.ReceivingMode, c.fs)
}

func (c *Chunk) finalize(chunk pb.Chunk, td *tracked) error {
	env := c.getEnv(chunk)
	msg := c.toMessage(td.first, td.files)
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

func (c *Chunk) removeTempDir(chunk pb.Chunk) {
	env := c.getEnv(chunk)
	env.MustRemoveTempDir()
}

func (c *Chunk) toMessage(chunk pb.Chunk,
	files []*pb.SnapshotFile) pb.MessageBatch {
	if chunk.ChunkId != 0 {
		panic("not first chunk")
	}
	env := c.getEnv(chunk)
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
	fn := c.fs.PathBase(chunk.Filepath)
	s.Filepath = c.fs.PathJoin(snapDir, fn)
	s.FileSize = chunk.FileSize
	s.Witness = chunk.Witness
	m.Snapshot = s
	m.Snapshot.Files = files
	for idx := range m.Snapshot.Files {
		fp := c.fs.PathJoin(snapDir, m.Snapshot.Files[idx].Filename())
		m.Snapshot.Files[idx].Filepath = fp
	}
	return pb.MessageBatch{
		BinVer:       chunk.BinVer,
		DeploymentId: chunk.DeploymentId,
		Requests:     []pb.Message{m},
	}
}

func (c *Chunk) ssid(chunk pb.Chunk) string {
	return logutil.DescribeSS(chunk.ClusterId, chunk.NodeId, chunk.Index)
}
