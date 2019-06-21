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

// +build !dragonboat_no_rocksdb
// +build !dragonboat_pebble_test
// +build !dragonboat_leveldb_test

package tests

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lni/dragonboat/v3/internal/logdb/kv/rocksdb/gorocksdb"
	"github.com/lni/dragonboat/v3/internal/tests/kvpb"
	"github.com/lni/dragonboat/v3/internal/utils/fileutil"
	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/internal/utils/random"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	appliedIndexKey    string = "disk_kv_applied_index"
	testDBDirName      string = "test_rocksdb_db_safe_to_delete"
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updating"
)

type inmem struct {
	mu    sync.Mutex
	kv    sync.Map
	index uint64
}

func (im *inmem) put(key []byte, val []byte, index uint64) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.kv.Store(string(key), string(val))
	im.index = index
}

func (im *inmem) get(key []byte) ([]byte, bool) {
	im.mu.Lock()
	defer im.mu.Unlock()
	val, ok := im.kv.Load(string(key))
	if ok {
		return []byte(val.(string)), true
	}
	return nil, false
}

func (im *inmem) getIndex() uint64 {
	im.mu.Lock()
	defer im.mu.Unlock()
	return im.index
}

func (im *inmem) deepCopy() *inmem {
	im.mu.Lock()
	defer im.mu.Unlock()
	nim := &inmem{
		index: im.index,
	}
	r := func(k, v interface{}) bool {
		nim.kv.Store(k, v)
		return true
	}
	im.kv.Range(r)
	return nim
}

type rocksdb struct {
	mu     sync.RWMutex
	db     *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	syncwo *gorocksdb.WriteOptions
	opts   *gorocksdb.Options
	inmem  *inmem
	closed bool
}

func (r *rocksdb) lookup(query []byte) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errors.New("db already closed")
	}
	v, ok := r.inmem.get(query)
	if ok {
		return v, nil
	}
	val, err := r.db.Get(r.ro, query)
	if err != nil {
		return nil, err
	}
	defer val.Free()
	data := val.Data()
	if len(data) == 0 {
		return []byte(""), nil
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	return buf, nil
}

func (r *rocksdb) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.db != nil {
		r.db.Close()
	}
	if r.opts != nil {
		r.opts.Destroy()
	}
	if r.wo != nil {
		r.wo.Destroy()
	}
	if r.syncwo != nil {
		r.syncwo.Destroy()
	}
	if r.ro != nil {
		r.ro.Destroy()
	}
	r.db = nil
}

var dbmu sync.Mutex

func createDB(dbdir string) (*rocksdb, error) {
	dbmu.Lock()
	defer dbmu.Unlock()
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetWholeKeyFiltering(true)
	bbto.SetBlockSize(1024)
	bbto.SetNoBlockCache(true)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetManifestPreallocationSize(1024 * 32)
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	opts.SetCompression(gorocksdb.NoCompression)
	// rocksdb perallocates size for its log file and the size is calculated
	// based on the write buffer size.
	opts.SetWriteBufferSize(1024)
	wo := gorocksdb.NewDefaultWriteOptions()
	syncwo := gorocksdb.NewDefaultWriteOptions()
	syncwo.SetSync(true)
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTotalOrderSeek(true)
	db, err := gorocksdb.OpenDb(opts, dbdir)
	if err != nil {
		return nil, err
	}
	return &rocksdb{
		db:     db,
		ro:     ro,
		wo:     wo,
		syncwo: syncwo,
		opts:   opts,
		inmem:  &inmem{},
	}, nil
}

func isNewRun(dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		return true
	}
	return false
}

func getNodeDBDirName(clusterID uint64, nodeID uint64) string {
	part := fmt.Sprintf("%d_%d", clusterID, nodeID)
	return filepath.Join(testDBDirName, part)
}

func getNewRandomDBDirName(dir string) string {
	part := "%d_%d"
	rn := random.LockGuardedRand.Uint64()
	ct := time.Now().UnixNano()
	return filepath.Join(dir, fmt.Sprintf(part, rn, ct))
}

func replaceCurrentDBFile(dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := os.Rename(tmpFp, fp); err != nil {
		return err
	}
	return fileutil.SyncDir(dir)
}

func saveCurrentDBDirName(dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}
	fp := filepath.Join(dir, updatingDBFilename)
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
		if err := fileutil.SyncDir(dir); err != nil {
			panic(err)
		}
	}()
	if _, err := f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err := f.Write([]byte(dbdir)); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func getCurrentDBDirName(dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := os.OpenFile(fp, os.O_RDONLY, 0755)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		panic("corrupted content")
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err := h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		panic("corrupted content with not matched crc")
	}
	return string(content), nil
}

func createNodeDataDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

func cleanupNodeDataDir(dir string) error {
	os.RemoveAll(filepath.Join(dir, updatingDBFilename))
	dbdir, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		fmt.Printf("dbdir %s, fi.name %s, dir %s\n", dbdir, fi.Name(), dir)
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != dbdir {
			fmt.Printf("removing %s\n", toDelete)
			if err := os.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}
	return nil
}

// DiskKVTest is a state machine used for testing on disk kv.
type DiskKVTest struct {
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	db          unsafe.Pointer
	closed      bool
	aborted     bool
}

// NewDiskKVTest creates a new disk kv test state machine.
func NewDiskKVTest(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	d := &DiskKVTest{
		clusterID: clusterID,
		nodeID:    nodeID,
	}
	fmt.Printf("[DKVE] %s is being created\n", d.id())
	return d
}

func (d *DiskKVTest) id() string {
	id := logutil.DescribeNode(d.clusterID, d.nodeID)
	return fmt.Sprintf("%s %s", time.Now().Format("2006-01-02 15:04:05.000000"), id)
}

func (d *DiskKVTest) queryAppliedIndex(db *rocksdb) (uint64, error) {
	idx := db.inmem.getIndex()
	if idx > 0 {
		return idx, nil
	}
	val, err := db.db.Get(db.ro, []byte(appliedIndexKey))
	if err != nil {
		fmt.Printf("[DKVE] %s failed to query applied index\n", d.id())
		return 0, err
	}
	defer val.Free()
	data := val.Data()
	if len(data) == 0 {
		fmt.Printf("[DKVE] %s does not have applied index stored yet\n", d.id())
		return 0, nil
	}
	return strconv.ParseUint(string(data), 10, 64)
}

// Open opens the state machine.
func (d *DiskKVTest) Open(stopc <-chan struct{}) (uint64, error) {
	fmt.Printf("[DKVE] %s is being opened\n", d.id())
	generateRandomDelay()
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	if err := createNodeDataDir(dir); err != nil {
		panic(err)
	}
	var dbdir string
	if !isNewRun(dir) {
		if err := cleanupNodeDataDir(dir); err != nil {
			return 0, err
		}
		var err error
		dbdir, err = getCurrentDBDirName(dir)
		if err != nil {
			return 0, err
		}
		if _, err := os.Stat(dbdir); err != nil {
			if os.IsNotExist(err) {
				panic("db dir unexpectedly deleted")
			}
		}
		fmt.Printf("[DKVE] %s being re-opened at %s\n", d.id(), dbdir)
	} else {
		fmt.Printf("[DKVE] %s doing a new run\n", d.id())
		dbdir = getNewRandomDBDirName(dir)
		if err := saveCurrentDBDirName(dir, dbdir); err != nil {
			return 0, err
		}
		if err := replaceCurrentDBFile(dir); err != nil {
			return 0, err
		}
	}
	fmt.Printf("[DKVE] %s going to create db at %s\n", d.id(), dbdir)
	db, err := createDB(dbdir)
	if err != nil {
		fmt.Printf("[DKVE] %s failed to create db\n", d.id())
		return 0, err
	}
	fmt.Printf("[DKVE] %s returned from create db\n", d.id())
	atomic.SwapPointer(&d.db, unsafe.Pointer(db))
	appliedIndex, err := d.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[DKVE] %s opened its disk sm, index %d\n",
		d.id(), appliedIndex)
	d.lastApplied = appliedIndex
	return appliedIndex, nil
}

// Lookup queries the state machine.
func (d *DiskKVTest) Lookup(key interface{}) (interface{}, error) {
	db := (*rocksdb)(atomic.LoadPointer(&d.db))
	if db != nil {
		v, err := db.lookup(key.([]byte))
		if err == nil && d.closed {
			panic("lookup returned valid result when DiskKVTest is already closed")
		}
		return v, err
	}
	return nil, errors.New("db closed")
}

// Update updates the state machine.
func (d *DiskKVTest) Update(ents []sm.Entry) ([]sm.Entry, error) {
	if d.aborted {
		panic("update() called after abort set to true")
	}
	if d.closed {
		panic("update called after Close()")
	}
	generateRandomDelay()
	db := (*rocksdb)(atomic.LoadPointer(&d.db))
	for idx, e := range ents {
		dataKv := &kvpb.PBKV{}
		if err := dataKv.Unmarshal(e.Cmd); err != nil {
			panic(err)
		}
		key := dataKv.GetKey()
		val := dataKv.GetVal()
		db.inmem.put([]byte(key), []byte(val), e.Index)
		ents[idx].Result = sm.Result{Value: uint64(len(ents[idx].Cmd))}
	}
	// idx := make([]byte, 8)
	// binary.LittleEndian.PutUint64(idx, ents[len(ents)-1].Index)
	// wb.Put([]byte(appliedIndexKey), idx)
	fmt.Printf("[DKVE] %s applied index recorded as %d\n", d.id(), ents[len(ents)-1].Index)
	if d.lastApplied >= ents[len(ents)-1].Index {
		fmt.Printf("[DKVE] %s last applied not moving forward %d,%d\n",
			d.id(), ents[len(ents)-1].Index, d.lastApplied)
		panic("lastApplied not moving forward")
	}
	d.lastApplied = ents[len(ents)-1].Index
	return ents, nil
}

// Sync synchronizes state machine's in-core state with that on disk.
func (d *DiskKVTest) Sync() error {
	if d.aborted {
		panic("update() called after abort set to true")
	}
	if d.closed {
		panic("update called after Close()")
	}
	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()
	db := (*rocksdb)(atomic.LoadPointer(&d.db))
	im := db.inmem.deepCopy()
	r := func(k, v interface{}) bool {
		wb.Put([]byte(k.(string)), []byte(v.(string)))
		return true
	}
	im.kv.Range(r)
	if im.index > 0 {
		idx := fmt.Sprintf("%d", im.index)
		wb.Put([]byte(appliedIndexKey), []byte(idx))
	}
	return db.db.Write(db.syncwo, wb)
}

type diskKVCtx struct {
	db       *rocksdb
	snapshot *gorocksdb.Snapshot
	inmem    *inmem
}

// PrepareSnapshot prepares snapshotting.
func (d *DiskKVTest) PrepareSnapshot() (interface{}, error) {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	db := (*rocksdb)(atomic.LoadPointer(&d.db))
	return &diskKVCtx{
		db:       db,
		snapshot: db.db.NewSnapshot(),
		inmem:    db.inmem.deepCopy(),
	}, nil
}

func iteratorIsValid(iter *gorocksdb.Iterator) bool {
	v, err := iter.IsValid()
	if err != nil {
		panic(err)
	}
	return v
}

func (d *DiskKVTest) saveToWriter(db *rocksdb,
	inmem *inmem, ss *gorocksdb.Snapshot, w io.Writer) error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(ss)
	ro.SetFillCache(false)
	ro.SetTotalOrderSeek(true)
	iter := db.db.NewIterator(ro)
	defer iter.Close()
	var dataMap sync.Map
	values := make([]*kvpb.PBKV, 0)
	for iter.SeekToFirst(); iteratorIsValid(iter); iter.Next() {
		key, ok := iter.OKey()
		if !ok {
			panic("failed to get key")
		}
		val, ok := iter.OValue()
		if !ok {
			panic("failed to get value")
		}
		dataMap.Store(string(key.Data()), string(val.Data()))
	}
	r := func(k, v interface{}) bool {
		dataMap.Store(k, v)
		return true
	}
	inmem.kv.Range(r)
	applied := inmem.getIndex()
	if applied > 0 {
		idx := fmt.Sprintf("%d", applied)
		dataMap.Store(appliedIndexKey, idx)
	}
	toList := func(k, v interface{}) bool {
		kv := &kvpb.PBKV{
			Key: k.(string),
			Val: v.(string),
		}
		values = append(values, kv)
		return true
	}
	dataMap.Range(toList)
	sort.Slice(values, func(i, j int) bool {
		return strings.Compare(values[i].Key, values[j].Key) < 0
	})
	count := uint64(len(values))
	fmt.Printf("[DKVE] %s have %d pairs of KV\n", d.id(), count)
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, count)
	if _, err := w.Write(sz); err != nil {
		return err
	}
	for _, dataKv := range values {
		if dataKv.Key == appliedIndexKey {
			v, err := strconv.ParseUint(string(dataKv.Val), 10, 64)
			if err != nil {
				panic(err)
			}
			fmt.Printf("[DKVE] %s saving appliedIndexKey as %d\n", d.id(), v)
		}
		data, err := dataKv.Marshal()
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(sz, uint64(len(data)))
		if _, err := w.Write(sz); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshot saves the state machine state.
func (d *DiskKVTest) SaveSnapshot(ctx interface{},
	w io.Writer, done <-chan struct{}) error {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	delay := getLargeRandomDelay(d.clusterID)
	fmt.Printf("random delay %d ms\n", delay)
	for delay > 0 {
		delay -= 10
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			return sm.ErrSnapshotStopped
		default:
		}
	}
	rsz := uint64(1024 * 1024 * 6)
	rubbish := make([]byte, rsz)
	for i := 0; i < 512; i++ {
		idx := random.LockGuardedRand.Uint64() % rsz
		rubbish[idx] = byte(random.LockGuardedRand.Uint64())
	}
	_, err := w.Write(rubbish)
	if err != nil {
		return err
	}
	ctxdata := ctx.(*diskKVCtx)
	db := ctxdata.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	ss := ctxdata.snapshot
	inmem := ctxdata.inmem
	return d.saveToWriter(db, inmem, ss, w)
}

// RecoverFromSnapshot recovers the state machine state from snapshot.
func (d *DiskKVTest) RecoverFromSnapshot(r io.Reader,
	done <-chan struct{}) error {
	if d.closed {
		panic("recover from snapshot called after Close()")
	}
	delay := getLargeRandomDelay(d.clusterID)
	fmt.Printf("random delay %d ms\n", delay)
	for delay > 0 {
		delay -= 10
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			d.aborted = true
			return sm.ErrSnapshotStopped
		default:
		}
	}
	rubbish := make([]byte, 1024*1024*6)
	if _, err := io.ReadFull(r, rubbish); err != nil {
		return err
	}
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	fmt.Printf("[DKVE] %s is creating a tmp db at %s\n", d.id(), dbdir)
	db, err := createDB(dbdir)
	if err != nil {
		return err
	}
	sz := make([]byte, 8)
	if _, err := io.ReadFull(r, sz); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(sz)
	fmt.Printf("[DKVE] %s recovering from a snapshot with %d pairs of KV\n", d.id(), total)
	wb := gorocksdb.NewWriteBatch()
	for i := uint64(0); i < total; i++ {
		if _, err := io.ReadFull(r, sz); err != nil {
			return err
		}
		toRead := binary.LittleEndian.Uint64(sz)
		data := make([]byte, toRead)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		dataKv := &kvpb.PBKV{}
		if err := dataKv.Unmarshal(data); err != nil {
			panic(err)
		}
		if dataKv.Key == appliedIndexKey {
			v, err := strconv.ParseUint(dataKv.Val, 10, 64)
			if err != nil {
				panic(err)
			}
			fmt.Printf("[DKVE] %s recovering appliedIndexKey to %d\n", d.id(), v)
		}
		wb.Put([]byte(dataKv.Key), []byte(dataKv.Val))
	}
	if err := db.db.Write(db.syncwo, wb); err != nil {
		return err
	}
	if err := saveCurrentDBDirName(dir, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(dir); err != nil {
		return err
	}
	fmt.Printf("[DKVE] %s replaced db %s with %s\n", d.id(), oldDirName, dbdir)
	newLastApplied, err := d.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	// when d.lastApplied == newLastApplied, it probably means there were some
	// dummy entries or membership change entries as part of the new snapshot
	// that never reached the SM and thus never moved the last applied index
	// in the SM snapshot.
	if d.lastApplied > newLastApplied {
		fmt.Printf("[DKVE] %s last applied in snapshot not moving forward %d,%d\n",
			d.id(), d.lastApplied, newLastApplied)
		panic("last applied not moving forward")
	}
	d.lastApplied = newLastApplied
	old := (*rocksdb)(atomic.SwapPointer(&d.db, unsafe.Pointer(db)))
	if old != nil {
		old.close()
	}
	fmt.Printf("[DKVE] %s to delete olddb at %s\n", d.id(), oldDirName)
	return os.RemoveAll(oldDirName)
}

// Close closes the state machine.
func (d *DiskKVTest) Close() error {
	fmt.Printf("[DKVE] %s called close\n", d.id())
	db := (*rocksdb)(atomic.SwapPointer(&d.db, unsafe.Pointer(nil)))
	if db != nil {
		d.closed = true
		db.close()
	} else {
		if d.closed {
			panic("close called twice")
		}
	}
	return nil
}

// GetHash returns a hash value representing the state of the state machine.
func (d *DiskKVTest) GetHash() (uint64, error) {
	fmt.Printf("[DKVE] %s called GetHash\n", d.id())
	h := md5.New()
	db := (*rocksdb)(atomic.LoadPointer(&d.db))
	ss := db.db.NewSnapshot()
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := d.saveToWriter(db, db.inmem, ss, h); err != nil {
		return 0, err
	}
	md5sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8]), nil
}
