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

/*
Package cpp implements a C++11 wrapper to allow IStateMachine to be implemented
in C++11.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package cpp

// initially the wrapper was implemented as a go plugin, this helps to make sure
// that libdragonboatcpp.a is not required for almost every target in Makefile.
// the drawback is also obvious - using a plugin to load another (CPP based)
// plugin is not that cool.

/*
#cgo CFLAGS: -I../../binding/include
#cgo CXXFLAGS: -std=c++11 -O3 -I../../binding/include
#cgo LDFLAGS: -ldl
#include <stdlib.h>
#include "wrapper.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/lni/dragonboat/internal/rsm"
	"github.com/lni/dragonboat/logger"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

var (
	plog = logger.GetLogger("cpp")
)

func getErrnoFromError(err error) int {
	if err == nil {
		panic("nil err")
	}
	operr, ok := err.(*os.PathError)
	if ok {
		errno, ok := operr.Err.(syscall.Errno)
		if ok {
			return int(errno)
		}
	}
	return int(syscall.EIO)
}

//
// exported go functions to be called by C code.
//

// RemoveManagedGoObject removes the managed object specified by the oid value.
//export RemoveManagedGoObject
func RemoveManagedGoObject(oid uint64) {
	RemoveManagedObject(oid)
}

// WriteToManagedWriter is a go helper function exported to C code to allow C
// code to use the Go snapshot writer.
//export WriteToManagedWriter
func WriteToManagedWriter(oid uint64, data []byte) (bool, int) {
	wi, ok := GetManagedObject(oid)
	if !ok {
		panic("failed to get writer")
	}
	w := wi.(io.Writer)
	_, err := w.Write(data)
	if err != nil {
		plog.Errorf("got err %+v when writing to the snapshot writer", err)
		return false, getErrnoFromError(err)
	}
	return true, 0
}

// ReadFromManagedReader is a go helper function exported to C to allow C code
// to use the Go snapshot reader.
//export ReadFromManagedReader
func ReadFromManagedReader(oid uint64, data []byte) (int, int) {
	ri, ok := GetManagedObject(oid)
	if !ok {
		panic("failed to get reader")
	}
	r := ri.(io.Reader)
	n, err := r.Read(data)
	if err != nil {
		if err == io.EOF {
			return 0, 0
		}
		plog.Errorf("got err %+v when reading from the snapshot reader", err)
		return -1, getErrnoFromError(err)
	}
	return n, 0
}

// DoneChanClosed is a go helper function exported to C to allow C code to
// check whether the specified done channel has been closed.
//export DoneChanClosed
func DoneChanClosed(oid uint64) bool {
	ci, ok := GetManagedObject(oid)
	if !ok {
		panic("failed to get the done chan")
	}
	c := ci.(<-chan struct{})
	select {
	case <-c:
		return true
	default:
	}
	return false
}

// AddToSnapshotFileCollection adds the details of an external snapshot file to
// the specified managed file collection instance.
//export AddToSnapshotFileCollection
func AddToSnapshotFileCollection(oid uint64,
	fileID uint64, path []byte, metadata []byte) {
	fci, ok := GetManagedObject(oid)
	if !ok {
		panic("failed to get the file collection")
	}
	fc := fci.(sm.ISnapshotFileCollection)
	filePath := string(path)
	data := make([]byte, len(metadata))
	copy(data, metadata)
	fc.AddFile(fileID, filePath, data)
}

func getSnapshotErrorFromErrNo(errno int) error {
	if errno == 0 {
		return nil
	} else if errno == 1 {
		return errors.New("failed to access snapshot file")
	} else if errno == 2 {
		return errors.New("failed to recover from snapshot")
	} else if errno == 3 {
		return errors.New("failed to save snapshot")
	} else if errno == 4 {
		return sm.ErrSnapshotStopped
	} else if errno == 100 {
		return errors.New("other snapshot error")
	}
	return fmt.Errorf("snapshot error with errno %d", errno)
}

func getOpenErrorFromErrNo(errno int) error {
	if errno == 0 {
		return nil
	} else if errno == 100 {
		return errors.New("other open error")
	}
	return fmt.Errorf("open error with errno %d", errno)
}

// NewStateMachineWrapperFromPlugin creates and returns the new plugin based
// NewStateMachineWrapper instance.
func NewStateMachineWrapperFromPlugin(clusterID uint64, nodeID uint64,
	dsname string, fname string, smType pb.StateMachineType,
	done <-chan struct{}) rsm.IManagedStateMachine {
	cDSName := C.CString(dsname)
	cFName := C.CString(fname)
	defer C.free(unsafe.Pointer(cDSName))
	defer C.free(unsafe.Pointer(cFName))
	factory := unsafe.Pointer(C.LoadFactoryFromPlugin(cDSName, cFName))
	return NewStateMachineWrapper(clusterID, nodeID, factory, 1, smType, done)
}

// NewStateMachineWrapper creates and returns the new NewStateMachineWrapper
// instance.
func NewStateMachineWrapper(clusterID uint64, nodeID uint64,
	factory unsafe.Pointer, style uint64, smType pb.StateMachineType,
	done <-chan struct{}) rsm.IManagedStateMachine {
	cClusterID := C.uint64_t(clusterID)
	cNodeID := C.uint64_t(nodeID)
	cFactory := factory
	cStyle := C.uint64_t(style)
	switch smType {
	case pb.RegularStateMachine:
		return &RegularStateMachineWrapper{
			dataStore: C.CreateDBRegularStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
	case pb.ConcurrentStateMachine:
		return &ConcurrentStateMachineWrapper{
			dataStore: C.CreateDBConcurrentStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
	case pb.OnDiskStateMachine:
		return &OnDiskStateMachineWrapper{
			dataStore: C.CreateDBOnDiskStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
	default:
		panic("unkown statemachine type")
	}
}

// RegularStateMachineWrapper is the IManagedStateMachine managing C++ data store.
type RegularStateMachineWrapper struct {
	rsm.OffloadedStatus
	// void * points to the actual data store
	dataStore *C.CPPRegularStateMachine
	done      <-chan struct{}
	mu        sync.RWMutex
}

func (ds *RegularStateMachineWrapper) destroy() {
	C.DestroyDBRegularStateMachine(ds.dataStore)
}

// Open opens the state machine.
func (ds *RegularStateMachineWrapper) Open() (uint64, error) {
	panic("Open not suppose to be called on RegularStateMachineWrapper")
}

// Offloaded offloads the data store from the specified part of the system.
func (ds *RegularStateMachineWrapper) Offloaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
	}
}

// Loaded marks the data store as loaded by the specified component.
func (ds *RegularStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// BatchedUpdate applies committed entries in a batch to hide latency. This
// method is not supported in the C++ wrapper.
func (ds *RegularStateMachineWrapper) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	panic("not supported")
}

// Update updates the data store.
func (ds *RegularStateMachineWrapper) Update(session *rsm.Session,
	e pb.Entry) (sm.Result, error) {
	ds.ensureNotDestroyed()
	var dp *C.uchar
	if len(e.Cmd) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&e.Cmd[0]))
	}
	v := C.UpdateDBRegularStateMachine(ds.dataStore, dp, C.size_t(len(e.Cmd)))
	if session != nil {
		session.AddResponse((rsm.RaftSeriesID)(e.SeriesID), sm.Result{Value: uint64(v)})
	}
	return sm.Result{Value: uint64(v)}, nil
}

// Lookup queries the data store.
func (ds *RegularStateMachineWrapper) Lookup(query interface{}) (interface{}, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query.([]byte)
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBRegularStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBRegularStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

// NALookup queries the data store.
func (ds *RegularStateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBRegularStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBRegularStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

// Sync synchronizes the state machine's in-core state with that on disk.
func (ds *RegularStateMachineWrapper) Sync() error {
	panic("Sync not suppose to be called on RegularStateMachineWrapper")
}

// GetHash returns an integer value representing the state of the data store.
func (ds *RegularStateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBRegularStateMachine(ds.dataStore)
	return uint64(v), nil
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (ds *RegularStateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	panic("PrepareSnapshot not suppose to be called on RegularStateMachineWrapper")
}

// StreamSnapshot streams the snapshot to the remote node.
func (ds *RegularStateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer *rsm.ChunkWriter) error {
	panic("StreamSnapshot not suppose to be called on RegularStateMachineWrapper")
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *RegularStateMachineWrapper) SaveSnapshot(meta *rsm.SnapshotMeta,
	writer *rsm.SnapshotWriter,
	session []byte,
	collection sm.ISnapshotFileCollection) (bool, uint64, error) {
	ds.ensureNotDestroyed()
	n, err := writer.Write(session)
	if err != nil {
		return false, 0, err
	}
	if n != len(session) {
		return false, 0, io.ErrShortWrite
	}
	smsz := uint64(len(session))
	writerOID := AddManagedObject(writer)
	collectionOID := AddManagedObject(collection)
	doneChOID := AddManagedObject(ds.done)
	defer func() {
		RemoveManagedObject(writerOID)
		RemoveManagedObject(collectionOID)
		RemoveManagedObject(doneChOID)
	}()
	r := C.SaveSnapshotDBRegularStateMachine(ds.dataStore,
		C.uint64_t(writerOID), C.uint64_t(collectionOID), C.uint64_t(doneChOID))
	errno := int(r.errcode)
	err = getSnapshotErrorFromErrNo(errno)
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, 0, err
	}
	sz := uint64(r.size)
	actualSz := writer.GetPayloadSize(sz + smsz)
	return false, actualSz + rsm.SnapshotHeaderSize, nil
}

// ConcurrentSnapshot returns a boolean flag indicating whether the state
// machine is capable of taking concurrent snapshots.
func (ds *RegularStateMachineWrapper) ConcurrentSnapshot() bool {
	return false
}

// OnDiskStateMachine returns a boolean flag indicating whether the state
// machine is an on disk state machine.
func (ds *RegularStateMachineWrapper) OnDiskStateMachine() bool {
	return false
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *RegularStateMachineWrapper) RecoverFromSnapshot(index uint64,
	reader *rsm.SnapshotReader,
	files []sm.SnapshotFile) error {
	ds.ensureNotDestroyed()
	cf := C.GetCollectedFile()
	defer C.FreeCollectedFile(cf)
	for _, file := range files {
		fpdata := []byte(file.Filepath)
		metadata := file.Metadata
		C.AddToCollectedFile(cf, C.uint64_t(file.FileID),
			(*C.char)(unsafe.Pointer(&fpdata[0])), C.size_t(len(fpdata)),
			(*C.uchar)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	}
	readerOID := AddManagedObject(reader)
	doneChOID := AddManagedObject(ds.done)
	r := C.RecoverFromSnapshotDBRegularStateMachine(ds.dataStore,
		cf, C.uint64_t(readerOID), C.uint64_t(doneChOID))
	return getSnapshotErrorFromErrNo(int(r))
}

// StateMachineType returns the state machine type.
func (ds *RegularStateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.RegularStateMachine
}

func (ds *RegularStateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}

func getCPPSOFileName(dsname string) string {
	d := strings.ToLower(dsname)
	return fmt.Sprintf("./dragonboat-cpp-plugin-%s.so", d)
}

type ConcurrentStateMachineWrapper struct {
	rsm.OffloadedStatus
	dataStore *C.CPPConcurrentStateMachine
	done      <-chan struct{}
	mu        sync.RWMutex
}

func (ds *ConcurrentStateMachineWrapper) destroy() {
	C.DestroyDBConcurrentStateMachine(ds.dataStore)
}

func (ds *ConcurrentStateMachineWrapper) Open() (uint64, error) {
	panic("Open not suppose to be called on ConcurrentStateMachineWrapper")
}

func (ds *ConcurrentStateMachineWrapper) Update(session *rsm.Session,
	e pb.Entry) (sm.Result, error) {
	ds.ensureNotDestroyed()
	var dp *C.uchar
	if len(e.Cmd) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&e.Cmd[0]))
	}
	v := C.UpdateDBConcurrentStateMachine(ds.dataStore, dp, C.size_t(len(e.Cmd)))
	if session != nil {
		session.AddResponse((rsm.RaftSeriesID)(e.SeriesID), sm.Result{Value: uint64(v)})
	}
	return sm.Result{Value: uint64(v)}, nil
}

func (ds *ConcurrentStateMachineWrapper) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	panic("not supported")
}

func (ds *ConcurrentStateMachineWrapper) Lookup(query interface{}) (interface{}, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query.([]byte)
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBConcurrentStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBConcurrentStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *ConcurrentStateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBConcurrentStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBConcurrentStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *ConcurrentStateMachineWrapper) Sync() error {
	panic("Sync not suppose to be called on ConcurrentStateMachineWrapper")
}

func (ds *ConcurrentStateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBConcurrentStateMachine(ds.dataStore)
	return uint64(v), nil
}

func (ds *ConcurrentStateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	r := C.PrepareSnapshotDBConcurrentStateMachine(ds.dataStore)
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreePrepareSnapshotResultDBConcurrentStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *ConcurrentStateMachineWrapper) SaveSnapshot(meta *rsm.SnapshotMeta,
	writer *rsm.SnapshotWriter,
	session []byte,
	collection sm.ISnapshotFileCollection) (bool, uint64, error) {
	n, err := writer.Write(session)
	if err != nil {
		return false, 0, err
	}
	if n != len(session) {
		return false, 0, io.ErrShortWrite
	}
	smsz := uint64(len(session))
	writerOID := AddManagedObject(writer)
	collectionOID := AddManagedObject(collection)
	doneChOID := AddManagedObject(ds.done)
	defer func() {
		RemoveManagedObject(writerOID)
		RemoveManagedObject(collectionOID)
		RemoveManagedObject(doneChOID)
	}()
	var dp *C.uchar
	ssctx := meta.Ctx.([]byte)
	if len(ssctx) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&ssctx[0]))
	}
	r := C.SaveSnapshotDBConcurrentStateMachine(ds.dataStore, dp,
		C.size_t(len(ssctx)), C.uint64_t(writerOID), C.uint64_t(collectionOID),
		C.uint64_t(doneChOID))
	errno := int(r.errcode)
	err = getSnapshotErrorFromErrNo(errno)
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, 0, err
	}
	sz := uint64(r.size)
	actualSz := writer.GetPayloadSize(sz + smsz)
	return false, actualSz + rsm.SnapshotHeaderSize, nil
}

func (ds *ConcurrentStateMachineWrapper) RecoverFromSnapshot(index uint64,
	reader *rsm.SnapshotReader,
	files []sm.SnapshotFile) error {
	ds.ensureNotDestroyed()
	cf := C.GetCollectedFile()
	defer C.FreeCollectedFile(cf)
	for _, file := range files {
		fpdata := []byte(file.Filepath)
		metadata := file.Metadata
		C.AddToCollectedFile(cf, C.uint64_t(file.FileID),
			(*C.char)(unsafe.Pointer(&fpdata[0])), C.size_t(len(fpdata)),
			(*C.uchar)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	}
	readerOID := AddManagedObject(reader)
	doneChOID := AddManagedObject(ds.done)
	r := C.RecoverFromSnapshotDBConcurrentStateMachine(ds.dataStore,
		cf, C.uint64_t(readerOID), C.uint64_t(doneChOID))
	return getSnapshotErrorFromErrNo(int(r))
}

func (ds *ConcurrentStateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer *rsm.ChunkWriter) error {
	panic("StreamSnapshot not suppose to be called on ConcurrentStateMachineWrapper")
}

func (ds *ConcurrentStateMachineWrapper) Offloaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
	}
}

func (ds *ConcurrentStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

func (ds *ConcurrentStateMachineWrapper) ConcurrentSnapshot() bool {
	return true
}

func (ds *ConcurrentStateMachineWrapper) OnDiskStateMachine() bool {
	return false
}

func (ds *ConcurrentStateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.ConcurrentStateMachine
}

func (ds *ConcurrentStateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}

type OnDiskStateMachineWrapper struct {
	rsm.OffloadedStatus
	dataStore    *C.CPPOnDiskStateMachine
	done         <-chan struct{}
	mu           sync.RWMutex
	opened       bool
	initialIndex uint64
	applied      uint64
}

func (ds *OnDiskStateMachineWrapper) destroy() {
	C.DestroyDBOnDiskStateMachine(ds.dataStore)
}

func (ds *OnDiskStateMachineWrapper) Open() (uint64, error) {
	if ds.opened {
		panic("Open called more than once on OnDiskStateMachineWrapper")
	}
	ds.opened = true
	doneChOID := AddManagedObject(ds.done)
	defer func() {
		RemoveManagedObject(doneChOID)
	}()
	r := C.OpenDBOnDiskStateMachine(ds.dataStore, C.uint64_t(doneChOID))
	applied := uint64(r.result)
	errno := int(r.errcode)
	err := getOpenErrorFromErrNo(errno)
	if err != nil {
		return 0, err
	}
	ds.initialIndex = applied
	ds.applied = applied
	return applied, nil
}

func (ds *OnDiskStateMachineWrapper) Update(session *rsm.Session,
	e pb.Entry) (sm.Result, error) {
	if !ds.opened {
		panic("Update called when not opened")
	}
	ds.ensureNotDestroyed()
	if e.Index <= ds.initialIndex {
		plog.Panicf("last entry index to apply %d, initial index %d",
			e.Index, ds.initialIndex)
	}
	if e.Index <= ds.applied {
		plog.Panicf("last entry index to apply %d, applied %d",
			e.Index, ds.applied)
	}
	ds.applied = e.Index
	var dp *C.uchar
	if len(e.Cmd) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&e.Cmd[0]))
	}
	v := C.UpdateDBOnDiskStateMachine(ds.dataStore, dp, C.size_t(len(e.Cmd)))
	if session != nil {
		session.AddResponse((rsm.RaftSeriesID)(e.SeriesID), sm.Result{Value: uint64(v)})
	}
	return sm.Result{Value: uint64(v)}, nil
}

func (ds *OnDiskStateMachineWrapper) BatchedUpdate(entries []sm.Entry) ([]sm.Entry, error) {
	if !ds.opened {
		panic("BatchedUpdate called when not opened")
	}
	ds.ensureNotDestroyed()
	if entries[0].Index <= ds.initialIndex {
		plog.Panicf("first entry index to apply %d, initial index %d",
			entries[0].Index, ds.initialIndex)
	}
	if entries[0].Index <= ds.applied {
		plog.Panicf("first entry index to apply %d, applied %d",
			entries[0].Index, ds.applied)
	}
	for idx, ent := range entries {
		var dp *C.uchar
		data := ent.Cmd
		if len(data) > 0 {
			dp = (*C.uchar)(unsafe.Pointer(&data[0]))
		}
		v := C.UpdateDBOnDiskStateMachine(ds.dataStore, dp, C.size_t(len(data)))
		entries[idx].Result = sm.Result{Value: uint64(v)}
	}
	ds.applied = entries[len(entries)-1].Index
	return entries, nil
}

func (ds *OnDiskStateMachineWrapper) Lookup(query interface{}) (interface{}, error) {
	if !ds.opened {
		panic("Lookup called when not opened")
	}
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query.([]byte)
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBOnDiskStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBOnDiskStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *OnDiskStateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	if !ds.opened {
		panic("NALookup called when not opened")
	}
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	var dp *C.uchar
	data := query
	if len(data) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&data[0]))
	}
	r := C.LookupDBOnDiskStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResultDBOnDiskStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *OnDiskStateMachineWrapper) Sync() error {
	if !ds.opened {
		panic("Sync called when not opened")
	}
	ds.ensureNotDestroyed()
	errno := C.SyncDBOnDiskStateMachine(ds.dataStore)
	if errno != 0 {
		return fmt.Errorf("sync error with errno %d", errno)
	}
	return nil
}

func (ds *OnDiskStateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBOnDiskStateMachine(ds.dataStore)
	return uint64(v), nil
}

func (ds *OnDiskStateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	if !ds.opened {
		panic("PrepareSnapshot called when not opened")
	}
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, rsm.ErrClusterClosed
	}
	ds.ensureNotDestroyed()
	r := C.PrepareSnapshotDBOnDiskStateMachine(ds.dataStore)
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreePrepareSnapshotResultDBOnDiskStateMachine(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

func (ds *OnDiskStateMachineWrapper) SaveSnapshot(meta *rsm.SnapshotMeta,
	writer *rsm.SnapshotWriter,
	session []byte,
	collection sm.ISnapshotFileCollection) (bool, uint64, error) {
	if !ds.opened {
		panic("SaveSnapshot called when not opened")
	}
	if !meta.Request.IsExportedSnapshot() {
		sz, err := ds.saveDummySnapshot(writer, session)
		return true, sz, err
	}
	n, err := writer.Write(session)
	if err != nil {
		return false, 0, err
	}
	if n != len(session) {
		return false, 0, io.ErrShortWrite
	}
	smsz := uint64(len(session))
	writerOID := AddManagedObject(writer)
	doneChOID := AddManagedObject(ds.done)
	defer func() {
		RemoveManagedObject(writerOID)
		RemoveManagedObject(doneChOID)
	}()
	var dp *C.uchar
	ssctx := meta.Ctx.([]byte)
	if len(ssctx) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&ssctx[0]))
	}
	r := C.SaveSnapshotDBOnDiskStateMachine(ds.dataStore, dp,
		C.size_t(len(ssctx)), C.uint64_t(writerOID), C.uint64_t(doneChOID))
	errno := int(r.errcode)
	err = getSnapshotErrorFromErrNo(errno)
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, 0, err
	}
	sz := uint64(r.size)
	actualSz := writer.GetPayloadSize(sz + smsz)
	return false, actualSz + rsm.SnapshotHeaderSize, nil
}

func (ds *OnDiskStateMachineWrapper) saveDummySnapshot(
	writer *rsm.SnapshotWriter, session []byte) (uint64, error) {
	_, err := writer.Write(session)
	if err != nil {
		return 0, err
	}
	sz := rsm.EmptyClientSessionLength
	return writer.GetPayloadSize(sz) + rsm.SnapshotHeaderSize, nil
}

func (ds *OnDiskStateMachineWrapper) RecoverFromSnapshot(index uint64,
	reader *rsm.SnapshotReader,
	files []sm.SnapshotFile) error {
	if !ds.opened {
		panic("RecoverFromSnapshot called when not opened")
	}
	ds.ensureNotDestroyed()
	if index <= ds.applied {
		plog.Panicf("recover snapshot moving applied index backwards, %d, %d",
			index, ds.applied)
	}
	readerOID := AddManagedObject(reader)
	doneChOID := AddManagedObject(ds.done)
	r := C.RecoverFromSnapshotDBOnDiskStateMachine(ds.dataStore,
		C.uint64_t(readerOID), C.uint64_t(doneChOID))
	return getSnapshotErrorFromErrNo(int(r))
}

func (ds *OnDiskStateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer *rsm.ChunkWriter) error {
	if !ds.opened {
		panic("StreamSnapshot called when not opened")
	}
	ds.ensureNotDestroyed()
	writerOID := AddManagedObject(writer)
	doneChOID := AddManagedObject(ds.done)
	defer func() {
		RemoveManagedGoObject(writerOID)
		RemoveManagedGoObject(doneChOID)
	}()
	var dp *C.uchar
	ssctxb := ssctx.([]byte)
	if len(ssctxb) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&ssctxb[0]))
	}
	r := C.SaveSnapshotDBOnDiskStateMachine(ds.dataStore, dp,
		C.size_t(len(ssctxb)), C.uint64_t(writerOID), C.uint64_t(doneChOID))
	errno := int(r.errcode)
	err := getSnapshotErrorFromErrNo(errno)
	if err != nil {
		plog.Errorf("stream snapshot failed, %v", err)
		// FIXME: writer.failed = true
	}
	return err
}

func (ds *OnDiskStateMachineWrapper) Offloaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
	}
}

func (ds *OnDiskStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

func (ds *OnDiskStateMachineWrapper) ConcurrentSnapshot() bool {
	return true
}

func (ds *OnDiskStateMachineWrapper) OnDiskStateMachine() bool {
	return true
}

func (ds *OnDiskStateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.OnDiskStateMachine
}

func (ds *OnDiskStateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}
