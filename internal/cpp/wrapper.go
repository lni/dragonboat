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
	"sync"
	"syscall"
	"unsafe"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
	sm "github.com/lni/dragonboat/v3/statemachine"
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

func getPrepareSnapshotErrorFromErrNo(errno int) error {
	if errno == 0 {
		return nil
	} else if errno == 1 {
		return errors.New("failed to prepare snapshot")
	}
	return fmt.Errorf("prepare snapshot error with errno %d", errno)
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
	} else if errno == 1 {
		return errors.New("failed to open on-disk state machine")
	} else if errno == 2 {
		return sm.ErrOpenStopped
	} else if errno == 100 {
		return errors.New("other open error")
	}
	return fmt.Errorf("open error with errno %d", errno)
}

func getCEntries(entries []sm.Entry) unsafe.Pointer {
	eps := C.malloc(C.sizeof_struct_Entry * C.size_t(len(entries)))
	for idx, ent := range entries {
		data := ent.Cmd
		ep := (*C.struct_Entry)(unsafe.Pointer(
			uintptr(unsafe.Pointer(eps)) + uintptr(C.sizeof_struct_Entry*C.size_t(idx))))
		ep.index = C.uint64_t(ent.Index)
		ep.cmd = (*C.uchar)(unsafe.Pointer(&data[0]))
		ep.cmdLen = C.size_t(len(data))
		ep.result = C.uint64_t(0)
	}
	return eps
}

func setResultsFromCEntries(entries []sm.Entry, eps unsafe.Pointer) {
	for idx := 0; idx < len(entries); idx++ {
		ep := (*C.struct_Entry)(unsafe.Pointer(
			uintptr(eps) + uintptr(C.sizeof_struct_Entry*C.size_t(idx))))
		entries[idx].Result = sm.Result{Value: uint64(ep.result)}
	}
}

func getCCollectedFiles(files []sm.SnapshotFile) *C.struct_CollectedFiles {
	cf := C.GetCollectedFile()
	for _, file := range files {
		fpdata := []byte(file.Filepath)
		metadata := file.Metadata
		C.AddToCollectedFile(cf, C.uint64_t(file.FileID),
			(*C.char)(unsafe.Pointer(&fpdata[0])), C.size_t(len(fpdata)),
			(*C.uchar)(unsafe.Pointer(&metadata[0])), C.size_t(len(metadata)))
	}
	return cf
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

var _ rsm.IManagedStateMachine = &RegularStateMachineWrapper{}
var _ rsm.IManagedStateMachine = &ConcurrentStateMachineWrapper{}
var _ rsm.IManagedStateMachine = &OnDiskStateMachineWrapper{}

// NewStateMachineWrapper creates and returns the new NewStateMachineWrapper
// instance.
func NewStateMachineWrapper(clusterID uint64, nodeID uint64,
	factory unsafe.Pointer, style uint64, smType pb.StateMachineType,
	done <-chan struct{}) rsm.IManagedStateMachine {
	cClusterID := C.uint64_t(clusterID)
	cNodeID := C.uint64_t(nodeID)
	cFactory := factory
	cStyle := C.uint64_t(style)
	destroyedC := make(chan struct{})
	switch smType {
	case pb.RegularStateMachine:
		w := &RegularStateMachineWrapper{
			dataStore: C.CreateDBRegularStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
		w.OffloadedStatus.DestroyedC = destroyedC
		return w
	case pb.ConcurrentStateMachine:
		w := &ConcurrentStateMachineWrapper{
			dataStore: C.CreateDBConcurrentStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
		w.OffloadedStatus.DestroyedC = destroyedC
		return w
	case pb.OnDiskStateMachine:
		w := &OnDiskStateMachineWrapper{
			dataStore: C.CreateDBOnDiskStateMachine(cClusterID, cNodeID, cFactory, cStyle),
			done:      done,
		}
		w.OffloadedStatus.DestroyedC = destroyedC
		return w
	default:
		panic("unknown statemachine type")
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
func (ds *RegularStateMachineWrapper) Offloaded(from rsm.From) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
		return true
	}
	return false
}

// Loaded marks the data store as loaded by the specified component.
func (ds *RegularStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// DestroyedC returns a chan struct{} used to indicate whether the SM has been
// fully offloaded.
func (ds *RegularStateMachineWrapper) DestroyedC() <-chan struct{} {
	return ds.OffloadedStatus.DestroyedC
}

// Update updates the data store.
func (ds *RegularStateMachineWrapper) Update(e sm.Entry) (sm.Result, error) {
	ds.ensureNotDestroyed()
	var dp *C.uchar
	if len(e.Cmd) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&e.Cmd[0]))
	}
	v := C.UpdateDBRegularStateMachine(
		ds.dataStore, C.uint64_t(e.Index), dp, C.size_t(len(e.Cmd)))
	return sm.Result{Value: uint64(v)}, nil
}

// BatchedUpdate updates the data store in batches.
func (ds *RegularStateMachineWrapper) BatchedUpdate(entries []sm.Entry) ([]sm.Entry, error) {
	panic("BatchedUpdate not supported in C++ regular state machine")
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
	panic("NALookup not supported in C++ state machine")
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
	writer io.Writer) error {
	panic("StreamSnapshot not suppose to be called on RegularStateMachineWrapper")
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *RegularStateMachineWrapper) SaveSnapshot(meta *rsm.SSMeta,
	writer io.Writer, session []byte,
	collection sm.ISnapshotFileCollection) (bool, error) {
	ds.ensureNotDestroyed()
	_, err := writer.Write(session)
	if err != nil {
		return false, err
	}
	OIDs := AddManagedObjects(writer, collection, ds.done)
	defer RemoveManagedObjects(OIDs...)
	r := C.SaveSnapshotDBRegularStateMachine(ds.dataStore,
		C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]), C.uint64_t(OIDs[2]))
	err = getSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, err
	}
	return false, nil
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
func (ds *RegularStateMachineWrapper) RecoverFromSnapshot(
	reader io.Reader, files []sm.SnapshotFile) error {
	ds.ensureNotDestroyed()
	cf := getCCollectedFiles(files)
	defer C.FreeCollectedFile(cf)
	OIDs := AddManagedObjects(reader, ds.done)
	r := C.RecoverFromSnapshotDBRegularStateMachine(ds.dataStore,
		cf, C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]))
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

// ConcurrentStateMachineWrapper ...
type ConcurrentStateMachineWrapper struct {
	rsm.OffloadedStatus
	dataStore *C.CPPConcurrentStateMachine
	done      <-chan struct{}
	mu        sync.RWMutex
}

func (ds *ConcurrentStateMachineWrapper) destroy() {
	C.DestroyDBConcurrentStateMachine(ds.dataStore)
}

// Open ...
func (ds *ConcurrentStateMachineWrapper) Open() (uint64, error) {
	panic("Open not suppose to be called on ConcurrentStateMachineWrapper")
}

// Update ...
func (ds *ConcurrentStateMachineWrapper) Update(e sm.Entry) (sm.Result, error) {
	results, err := ds.BatchedUpdate([]sm.Entry{e})
	if err != nil {
		return sm.Result{}, err
	}
	return results[0].Result, nil
}

// BatchedUpdate ...
func (ds *ConcurrentStateMachineWrapper) BatchedUpdate(entries []sm.Entry) ([]sm.Entry, error) {
	ds.ensureNotDestroyed()
	eps := getCEntries(entries)
	defer C.free(eps)
	C.BatchedUpdateDBConcurrentStateMachine(
		ds.dataStore, (*C.struct_Entry)(eps), C.size_t(len(entries)))
	setResultsFromCEntries(entries, eps)
	return entries, nil
}

// Lookup ...
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

// DestroyedC returns a chan struct{} used to indicate whether the SM has been
// fully offloaded.
func (ds *ConcurrentStateMachineWrapper) DestroyedC() <-chan struct{} {
	return ds.OffloadedStatus.DestroyedC
}

// NALookup ...
func (ds *ConcurrentStateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	panic("NALookup not supported in C++ state machine")
}

// Sync ...
func (ds *ConcurrentStateMachineWrapper) Sync() error {
	panic("Sync not suppose to be called on ConcurrentStateMachineWrapper")
}

// GetHash ...
func (ds *ConcurrentStateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBConcurrentStateMachine(ds.dataStore)
	return uint64(v), nil
}

// PrepareSnapshot ...
func (ds *ConcurrentStateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	ds.ensureNotDestroyed()
	r := C.PrepareSnapshotDBConcurrentStateMachine(ds.dataStore)
	err := getPrepareSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("Prepare snapshot failed, %v", err)
		return nil, err
	}
	result := unsafe.Pointer(r.result)
	return result, nil
}

// SaveSnapshot ...
func (ds *ConcurrentStateMachineWrapper) SaveSnapshot(meta *rsm.SSMeta,
	writer io.Writer, session []byte,
	collection sm.ISnapshotFileCollection) (bool, error) {
	if _, err := writer.Write(session); err != nil {
		return false, err
	}
	OIDs := AddManagedObjects(writer, collection, ds.done)
	defer RemoveManagedObjects(OIDs...)
	ssctx := meta.Ctx.(unsafe.Pointer)
	r := C.SaveSnapshotDBConcurrentStateMachine(ds.dataStore, ssctx,
		C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]), C.uint64_t(OIDs[2]))
	err := getSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, err
	}
	return false, nil
}

// RecoverFromSnapshot ...
func (ds *ConcurrentStateMachineWrapper) RecoverFromSnapshot(
	reader io.Reader, files []sm.SnapshotFile) error {
	ds.ensureNotDestroyed()
	cf := getCCollectedFiles(files)
	defer C.FreeCollectedFile(cf)
	OIDs := AddManagedObjects(reader, ds.done)
	r := C.RecoverFromSnapshotDBConcurrentStateMachine(ds.dataStore,
		cf, C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]))
	return getSnapshotErrorFromErrNo(int(r))
}

// StreamSnapshot ...
func (ds *ConcurrentStateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer io.Writer) error {
	panic("StreamSnapshot not suppose to be called on ConcurrentStateMachineWrapper")
}

// Offloaded ...
func (ds *ConcurrentStateMachineWrapper) Offloaded(from rsm.From) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
		return true
	}
	return false
}

// Loaded ...
func (ds *ConcurrentStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// ConcurrentSnapshot ...
func (ds *ConcurrentStateMachineWrapper) ConcurrentSnapshot() bool {
	return true
}

// OnDiskStateMachine ...
func (ds *ConcurrentStateMachineWrapper) OnDiskStateMachine() bool {
	return false
}

// StateMachineType ...
func (ds *ConcurrentStateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.ConcurrentStateMachine
}

func (ds *ConcurrentStateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}

// OnDiskStateMachineWrapper ...
type OnDiskStateMachineWrapper struct {
	rsm.OffloadedStatus
	dataStore *C.CPPOnDiskStateMachine
	done      <-chan struct{}
	mu        sync.RWMutex
	opened    bool
}

func (ds *OnDiskStateMachineWrapper) destroy() {
	C.DestroyDBOnDiskStateMachine(ds.dataStore)
}

// Open ...
func (ds *OnDiskStateMachineWrapper) Open() (uint64, error) {
	if ds.opened {
		panic("Open called more than once on OnDiskStateMachineWrapper")
	}
	ds.opened = true
	doneChOID := AddManagedObject(ds.done)
	defer RemoveManagedObject(doneChOID)
	r := C.OpenDBOnDiskStateMachine(ds.dataStore, C.uint64_t(doneChOID))
	applied := uint64(r.result)
	err := getOpenErrorFromErrNo(int(r.errcode))
	if err != nil {
		return 0, err
	}
	return applied, nil
}

// Update ...
func (ds *OnDiskStateMachineWrapper) Update(e sm.Entry) (sm.Result, error) {
	results, err := ds.BatchedUpdate([]sm.Entry{e})
	if err != nil {
		return sm.Result{}, err
	}
	return results[0].Result, nil
}

// DestroyedC returns a chan struct{} used to indicate whether the SM has been
// fully offloaded.
func (ds *OnDiskStateMachineWrapper) DestroyedC() <-chan struct{} {
	return ds.OffloadedStatus.DestroyedC
}

// BatchedUpdate ...
func (ds *OnDiskStateMachineWrapper) BatchedUpdate(entries []sm.Entry) ([]sm.Entry, error) {
	if !ds.opened {
		panic("BatchedUpdate called when not opened")
	}
	ds.ensureNotDestroyed()
	eps := getCEntries(entries)
	defer C.free(eps)
	C.BatchedUpdateDBOnDiskStateMachine(
		ds.dataStore, (*C.struct_Entry)(eps), C.size_t(len(entries)))
	setResultsFromCEntries(entries, eps)
	return entries, nil
}

// Lookup ...
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

// NALookup ...
func (ds *OnDiskStateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	panic("NALookup not supported in C++ state machine")
}

// Sync ...
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

// GetHash ...
func (ds *OnDiskStateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBOnDiskStateMachine(ds.dataStore)
	return uint64(v), nil
}

// PrepareSnapshot ...
func (ds *OnDiskStateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	if !ds.opened {
		panic("PrepareSnapshot called when not opened")
	}
	ds.ensureNotDestroyed()
	r := C.PrepareSnapshotDBOnDiskStateMachine(ds.dataStore)
	err := getPrepareSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("Prepare snapshot failed, %v", err)
		return nil, err
	}
	result := unsafe.Pointer(r.result)
	return result, nil
}

// SaveSnapshot ...
func (ds *OnDiskStateMachineWrapper) SaveSnapshot(meta *rsm.SSMeta,
	writer io.Writer, session []byte,
	collection sm.ISnapshotFileCollection) (bool, error) {
	if !ds.opened {
		panic("SaveSnapshot called when not opened")
	}
	if !meta.Request.IsExportedSnapshot() {
		return true, ds.saveDummySnapshot(writer, session)
	}
	if _, err := writer.Write(session); err != nil {
		return false, err
	}
	OIDs := AddManagedObjects(writer, ds.done)
	defer RemoveManagedObjects(OIDs...)
	ssctx := meta.Ctx.(unsafe.Pointer)
	r := C.SaveSnapshotDBOnDiskStateMachine(ds.dataStore, ssctx,
		C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]))
	err := getSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("save snapshot failed, %v", err)
		return false, err
	}
	return false, nil
}

func (ds *OnDiskStateMachineWrapper) saveDummySnapshot(
	writer io.Writer, session []byte) error {
	_, err := writer.Write(session)
	return err
}

// RecoverFromSnapshot ...
func (ds *OnDiskStateMachineWrapper) RecoverFromSnapshot(
	reader io.Reader, files []sm.SnapshotFile) error {
	if !ds.opened {
		panic("RecoverFromSnapshot called when not opened")
	}
	ds.ensureNotDestroyed()
	OIDs := AddManagedObjects(reader, ds.done)
	r := C.RecoverFromSnapshotDBOnDiskStateMachine(ds.dataStore,
		C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]))
	return getSnapshotErrorFromErrNo(int(r))
}

// StreamSnapshot ...
func (ds *OnDiskStateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer io.Writer) error {
	if !ds.opened {
		panic("StreamSnapshot called when not opened")
	}
	if _, err := writer.Write(rsm.GetEmptyLRUSession()); err != nil {
		return err
	}
	ds.ensureNotDestroyed()
	OIDs := AddManagedObjects(writer, ds.done)
	defer RemoveManagedObjects(OIDs...)
	r := C.SaveSnapshotDBOnDiskStateMachine(ds.dataStore,
		ssctx.(unsafe.Pointer), C.uint64_t(OIDs[0]), C.uint64_t(OIDs[1]))
	err := getSnapshotErrorFromErrNo(int(r.errcode))
	if err != nil {
		plog.Errorf("stream snapshot failed, %v", err)
	}
	return err
}

// Offloaded ...
func (ds *OnDiskStateMachineWrapper) Offloaded(from rsm.From) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
		return true
	}
	return false
}

// Loaded ...
func (ds *OnDiskStateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// ConcurrentSnapshot ...
func (ds *OnDiskStateMachineWrapper) ConcurrentSnapshot() bool {
	return true
}

// OnDiskStateMachine ...
func (ds *OnDiskStateMachineWrapper) OnDiskStateMachine() bool {
	return true
}

// StateMachineType ...
func (ds *OnDiskStateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.OnDiskStateMachine
}

func (ds *OnDiskStateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}

type raftListenerWrapper struct {
	listener unsafe.Pointer
}

func (lw *raftListenerWrapper) LeaderUpdated(info raftio.LeaderInfo) {
	cinfo := C.struct_LeaderInfo{
		ClusterID: C.uint64_t(info.ClusterID),
		NodeID:    C.uint64_t(info.NodeID),
		Term:      C.uint64_t(info.Term),
		LeaderID:  C.uint64_t(info.LeaderID),
	}
	C.LeaderUpdated(lw.listener, cinfo)
}

// NewRaftEventListener creates and returns the new raftio.IRaftEventListener
// instance to handle cpp listener.
func NewRaftEventListener(
	listener unsafe.Pointer) raftio.IRaftEventListener {
	if listener == nil {
		return nil
	}
	return &raftListenerWrapper{listener: listener}
}
