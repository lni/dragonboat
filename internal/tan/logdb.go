// Copyright 2017-2021 Lei Ni (nilei81@gmail.com)
//
// This is a proprietary library. You are not allowed to store, read, use,
// modify or redistribute this library without written consent from its
// copyright owners.

package tan

import (
	"io"
	"sync"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/vfs"
)

const (
	defaultBufferSize = 1024 * 1024 * 4
	defaultDBName     = "tandb"
	bootstrapDirname  = "bootstrap"
	defaultShards     = 16
	tanLogDBName      = "Tan"
)

var _ raftio.ILogDB = (*LogDB)(nil)

// Factory is the default LogDB factory instance used for creating tan DB
// instances.
var Factory = factory{}

type factory struct{}

// Create creates a new tan instance.
func (factory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback, dirs []string, wals []string) (raftio.ILogDB, error) {
	return CreateTan(cfg, cb, dirs, wals)
}

// MultiplexedLogFactory is a LogDB factory instance used for creating an
// tan DB with multiplexed logs.
var MultiplexedLogFactory = multiplexLogFactory{}

type multiplexLogFactory struct{}

// Create creates a tan instance that uses multiplexed log files.
func (multiplexLogFactory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback, dirs []string, wals []string) (raftio.ILogDB, error) {
	return CreateLogMultiplexedTan(cfg, cb, dirs, wals)
}

// Name returns the name of the tan instance.
func (factory) Name() string {
	return tanLogDBName
}

// LogDB is the tan ILogDB type
type LogDB struct {
	mu        sync.Mutex
	fileLock  io.Closer
	dirname   string
	dir       vfs.File
	bsDirname string
	bsDir     vfs.File
	fs        vfs.FS
	buffers   [][]byte
	wgs       []*sync.WaitGroup
	shards    *shards
}

// CreateTan creates and return a tan instance.
func CreateTan(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string) (*LogDB, error) {
	return createTan(cfg, cb, dirs, wals, true)
}

// CreateLogMultiplexedTan creates and returns a tan instance that uses
// multiplexed log files.
func CreateLogMultiplexedTan(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string) (*LogDB, error) {
	return createTan(cfg, cb, dirs, wals, false)
}

func createTan(cfg config.NodeHostConfig, cb config.LogDBCallback,
	dirs []string, wals []string, singleNodeLog bool) (*LogDB, error) {
	if cfg.Expert.FS == nil {
		panic("fs not set")
	}
	dirname := cfg.Expert.FS.PathJoin(dirs[0], defaultDBName)
	ldb := &LogDB{
		dirname: dirname,
		fs:      cfg.Expert.FS,
		buffers: make([][]byte, defaultShards),
		wgs:     make([]*sync.WaitGroup, defaultShards),
		shards:  newShards(dirname, cfg.Expert.FS, singleNodeLog),
	}

	for i := 0; i < len(ldb.buffers); i++ {
		ldb.buffers[i] = make([]byte, defaultBufferSize)
	}
	for i := 0; i < len(ldb.wgs); i++ {
		ldb.wgs[i] = new(sync.WaitGroup)
	}
	var err error
	if err := fileutil.MkdirAll(ldb.dirname, ldb.fs); err != nil {
		return nil, err
	}
	bs := ldb.fs.PathJoin(ldb.dirname, bootstrapDirname)
	if err := fileutil.MkdirAll(bs, ldb.fs); err != nil {
		return nil, err
	}
	ldb.bsDirname = bs
	if err := ldb.cleanupBootstrapDir(); err != nil {
		return nil, err
	}
	ldb.dir, err = ldb.fs.Open(ldb.dirname)
	if err != nil {
		return nil, err
	}
	ldb.bsDir, err = ldb.fs.Open(ldb.bsDirname)
	if err != nil {
		return nil, err
	}
	lockFilename := makeFilename(ldb.fs, ldb.dirname, fileTypeLock, 0)
	fileLock, err := ldb.fs.Lock(lockFilename)
	if err != nil {
		return nil, err
	}
	ldb.fileLock = fileLock
	return ldb, nil
}

func (l *LogDB) cleanupBootstrapDir() error {
	ls, err := l.fs.List(l.bsDirname)
	if err != nil {
		return err
	}
	for _, filename := range ls {
		if tmpBSFilenameRe.MatchString(filename) {
			if err := l.fs.Remove(l.fs.PathJoin(l.bsDirname, filename)); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: remove the following two methods

// DeleteSnapshot ...
func (l *LogDB) DeleteSnapshot(clusterID uint64,
	nodeID uint64, index uint64) error {
	panic("depreciated")
}

// ListSnapshots lists available snapshots associated with the specified
// Raft node for index range (0, index].
func (l *LogDB) ListSnapshots(clusterID uint64,
	nodeID uint64, index uint64) ([]pb.Snapshot, error) {
	panic("depreciated")
}

// Name returns the type name of the ILogDB instance.
func (l *LogDB) Name() string {
	return tanLogDBName
}

// Close closes the ILogDB instance.
func (l *LogDB) Close() (err error) {
	func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		err = firstError(err, l.shards.iterate(func(db *db) error {
			return db.close()
		}))
	}()
	err = firstError(err, l.bsDir.Close())
	err = firstError(err, l.dir.Close())
	return firstError(err, l.fileLock.Close())
}

// BinaryFormat returns an constant uint32 value representing the binary
// format version compatible with the ILogDB instance.
func (l *LogDB) BinaryFormat() uint32 {
	return raftio.PlainLogDBBinVersion
}

// ListNodeInfo lists all available NodeInfo found in the log DB.
func (l *LogDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	files, err := l.fs.List(l.bsDirname)
	if err != nil {
		return nil, err
	}
	result := make([]raftio.NodeInfo, 0)
	for _, file := range files {
		clusterID, nodeID, ok := parseBootstrapFilename(file)
		if ok {
			result = append(result, raftio.NodeInfo{ClusterID: clusterID, NodeID: nodeID})
		}
	}
	return result, nil
}

// SaveBootstrapInfo saves the specified bootstrap info to the log DB.
func (l *LogDB) SaveBootstrapInfo(clusterID uint64,
	nodeID uint64, rec pb.Bootstrap) error {
	return saveBootstrap(l.fs, l.bsDirname, l.bsDir, clusterID, nodeID, rec)
}

// GetBootstrapInfo returns saved bootstrap info from log DB. It returns
// ErrNoBootstrapInfo when there is no previously saved bootstrap info for
// the specified node.
func (l *LogDB) GetBootstrapInfo(clusterID uint64,
	nodeID uint64) (pb.Bootstrap, error) {
	return getBootstrap(l.fs, l.bsDirname, clusterID, nodeID)
}

// SaveRaftState atomically saves the Raft states, log entries and snapshots
// metadata found in the pb.Update list to the log DB.
func (l *LogDB) SaveRaftState(updates []pb.Update, shardID uint64) error {
	if l.shards.multiplexedLog() {
		return l.concurrentSaveState(updates, shardID)
	}
	return l.sequentialSaveState(updates, shardID)
}

func (l *LogDB) concurrentSaveState(updates []pb.Update, shardID uint64) error {
	var buf []byte
	if shardID-1 < uint64(len(l.buffers)) {
		buf = l.buffers[shardID-1]
	} else {
		buf = make([]byte, defaultBufferSize)
	}
	syncLog := false
	var shard *db
	var usedShardID uint64
	for idx, ud := range updates {
		ml := l.shards.shards.(*multiplexedLogShard)
		if idx == 0 {
			usedShardID = ml.shardID(ud.ClusterID)
		} else {
			if usedShardID != ml.shardID(ud.ClusterID) {
				panic("shard ID changed")
			}
		}
		db, err := l.getDB(ud.ClusterID, ud.NodeID)
		if err != nil {
			return err
		}
		if shard == nil {
			shard = db
		}
		sync, err := db.write(ud, buf)
		if err != nil {
			return err
		}
		if sync {
			syncLog = true
		}
	}
	if syncLog && shard != nil {
		if err := shard.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (l *LogDB) sequentialSaveState(updates []pb.Update, shardID uint64) error {
	var wg *sync.WaitGroup
	var buf []byte
	if shardID-1 < uint64(len(l.buffers)) {
		buf = l.buffers[shardID-1]
	} else {
		buf = make([]byte, defaultBufferSize)
	}
	if shardID-1 < uint64(len(l.wgs)) {
		wg = l.wgs[shardID-1]
	} else {
		wg = new(sync.WaitGroup)
	}
	for _, ud := range updates {
		db, err := l.getDB(ud.ClusterID, ud.NodeID)
		if err != nil {
			return err
		}
		sync, err := db.write(ud, buf)
		if err != nil {
			return err
		}
		if sync {
			wg.Add(1)
			go func() {
				if err := db.sync(); err != nil {
					panicNow(err)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	return nil
}

// IterateEntries returns the continuous Raft log entries of the specified
// Raft node between the index value range of [low, high) up to a max size
// limit of maxSize bytes. It returns the located log entries, their total
// size in bytes and the occurred error.
func (l *LogDB) IterateEntries(ents []pb.Entry,
	size uint64, clusterID uint64, nodeID uint64, low uint64,
	high uint64, maxSize uint64) ([]pb.Entry, uint64, error) {
	db, err := l.getDB(clusterID, nodeID)
	if err != nil {
		return nil, 0, err
	}
	return db.getEntries(clusterID, nodeID, ents, size, low, high, maxSize)
}

// ReadRaftState returns the persistented raft state found in Log DB.
func (l *LogDB) ReadRaftState(clusterID uint64,
	nodeID uint64, lastIndex uint64) (raftio.RaftState, error) {
	db, err := l.getDB(clusterID, nodeID)
	if err != nil {
		return raftio.RaftState{}, err
	}
	return db.getRaftState(clusterID, nodeID, lastIndex)
}

// RemoveEntriesTo removes entries between (0, index].
func (l *LogDB) RemoveEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) error {
	db, err := l.getDB(clusterID, nodeID)
	if err != nil {
		return err
	}
	if err := db.removeEntries(clusterID, nodeID, index); err != nil {
		return err
	}
	return db.sync()
}

// CompactEntriesTo reclaims underlying storage space used for storing
// entries up to the specified index.
func (l *LogDB) CompactEntriesTo(clusterID uint64,
	nodeID uint64, index uint64) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch, nil
}

// SaveSnapshots saves all snapshot metadata found in the pb.Update list.
func (l *LogDB) SaveSnapshots(updates []pb.Update) error {
	buf := make([]byte, 1024*32)
	for _, ud := range updates {
		if pb.IsEmptySnapshot(ud.Snapshot) {
			continue
		}
		db, err := l.getDB(ud.ClusterID, ud.NodeID)
		if err != nil {
			return err
		}
		wu := pb.Update{
			ClusterID: ud.ClusterID,
			NodeID:    ud.NodeID,
			Snapshot:  ud.Snapshot,
		}
		if _, err := db.write(wu, buf); err != nil {
			return err
		}
		if err := db.sync(); err != nil {
			return err
		}
	}
	return nil
}

// GetSnapshot lists available snapshots associated with the specified
// Raft node for index range (0, index].
func (l *LogDB) GetSnapshot(clusterID uint64,
	nodeID uint64) (pb.Snapshot, error) {
	db, err := l.getDB(clusterID, nodeID)
	if err != nil {
		return pb.Snapshot{}, err
	}
	return db.getSnapshot(clusterID, nodeID)
}

// RemoveNodeData removes all data associated with the specified node.
func (l *LogDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	db, err := l.getDB(clusterID, nodeID)
	if err != nil {
		return err
	}
	if err := db.removeAll(clusterID, nodeID); err != nil {
		return err
	}
	if err := removeBootstrap(l.fs,
		l.bsDirname, l.bsDir, clusterID, nodeID); err != nil {
		return err
	}
	return db.sync()
}

// ImportSnapshot imports the specified snapshot by creating all required
// metadata in the logdb.
func (l *LogDB) ImportSnapshot(snapshot pb.Snapshot, nodeID uint64) error {
	bs := pb.Bootstrap{
		Join: true,
		Type: snapshot.Type,
	}
	if err := saveBootstrap(l.fs,
		l.bsDirname, l.bsDir, snapshot.ClusterId, nodeID, bs); err != nil {
		return err
	}
	db, err := l.getDB(snapshot.ClusterId, nodeID)
	if err != nil {
		return err
	}
	if err := db.importSnapshot(snapshot.ClusterId, nodeID, snapshot); err != nil {
		return err
	}
	return db.sync()
}

func (l *LogDB) getDB(clusterID uint64, nodeID uint64) (*db, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shards.getDB(clusterID, nodeID)
}
