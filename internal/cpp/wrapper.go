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
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/lni/dragonboat/v3/internal/rsm"
	"github.com/lni/dragonboat/v3/logger"
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

func getErrorFromErrNo(errno int) error {
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

// StateMachineWrapper is the IManagedStateMachine managing C++ data store.
type StateMachineWrapper struct {
	rsm.OffloadedStatus
	// void * points to the actual data store
	dataStore *C.CPPStateMachine
	done      <-chan struct{}
	mu        sync.RWMutex
}

// NewStateMachineWrapper creates and returns the new NewStateMachineWrapper
// instance.
func NewStateMachineWrapper(clusterID uint64, nodeID uint64,
	dsname string, done <-chan struct{}) rsm.IManagedStateMachine {
	cClusterID := C.uint64_t(clusterID)
	cNodeID := C.uint64_t(nodeID)
	cDSName := C.CString(getCPPSOFileName(dsname))
	defer C.free(unsafe.Pointer(cDSName))
	return &StateMachineWrapper{
		dataStore: C.CreateDBStateMachine(cClusterID, cNodeID, cDSName),
		done:      done,
	}
}

// NewStateMachineFromFactoryWrapper creates and returns the new NewStateMachineWrapper
// instance.
func NewStateMachineFromFactoryWrapper(clusterID uint64, nodeID uint64,
	factory unsafe.Pointer, done <-chan struct{}) rsm.IManagedStateMachine {
	cClusterID := C.uint64_t(clusterID)
	cNodeID := C.uint64_t(nodeID)
	cFactory := factory
	return &StateMachineWrapper{
		dataStore: C.CreateDBStateMachineFromFactory(cClusterID, cNodeID, cFactory),
		done:      done,
	}
}

func (ds *StateMachineWrapper) destroy() {
	C.DestroyDBStateMachine(ds.dataStore)
}

// Open opens the state machine.
func (ds *StateMachineWrapper) Open() (uint64, error) {
	panic("Open() called on StateMachineWrapper")
}

// Offloaded offloads the data store from the specified part of the system.
func (ds *StateMachineWrapper) Offloaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.destroy()
		ds.SetDestroyed()
	}
}

// Loaded marks the data store as loaded by the specified component.
func (ds *StateMachineWrapper) Loaded(from rsm.From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// BatchedUpdate applies committed entries in a batch to hide latency. This
// method is not supported in the C++ wrapper.
func (ds *StateMachineWrapper) BatchedUpdate(ents []sm.Entry) ([]sm.Entry, error) {
	panic("not supported")
}

// Update updates the data store.
func (ds *StateMachineWrapper) Update(session *rsm.Session,
	e pb.Entry) (sm.Result, error) {
	ds.ensureNotDestroyed()
	var dp *C.uchar
	if len(e.Cmd) > 0 {
		dp = (*C.uchar)(unsafe.Pointer(&e.Cmd[0]))
	}
	v := C.UpdateDBStateMachine(ds.dataStore, dp, C.size_t(len(e.Cmd)))
	if session != nil {
		session.AddResponse((rsm.RaftSeriesID)(e.SeriesID), sm.Result{Value: uint64(v)})
	}
	return sm.Result{Value: uint64(v)}, nil
}

// Lookup queries the data store.
func (ds *StateMachineWrapper) Lookup(query interface{}) (interface{}, error) {
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
	r := C.LookupDBStateMachine(ds.dataStore, dp, C.size_t(len(data)))
	result := C.GoBytes(unsafe.Pointer(r.result), C.int(r.size))
	C.FreeLookupResult(ds.dataStore, r)
	ds.mu.RUnlock()
	return result, nil
}

// NALookup queries the data store.
func (ds *StateMachineWrapper) NALookup(query []byte) ([]byte, error) {
	panic("not implemented")
}

// Sync synchronizes the state machine's in-core state with that on disk.
func (ds *StateMachineWrapper) Sync() error {
	panic("Sync not suppose to be called")
}

// GetHash returns an integer value representing the state of the data store.
func (ds *StateMachineWrapper) GetHash() (uint64, error) {
	ds.ensureNotDestroyed()
	v := C.GetHashDBStateMachine(ds.dataStore)
	return uint64(v), nil
}

// PrepareSnapshot makes preparations for taking concurrent snapshot.
func (ds *StateMachineWrapper) PrepareSnapshot() (interface{}, error) {
	panic("PrepareSnapshot not suppose to be called")
}

// StreamSnapshot streams the snapshot to the remote node.
func (ds *StateMachineWrapper) StreamSnapshot(ssctx interface{},
	writer *rsm.ChunkWriter) error {
	panic("StreamSnapshot not suppose to be called")
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *StateMachineWrapper) SaveSnapshot(meta *rsm.SnapshotMeta,
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
	r := C.SaveSnapshotDBStateMachine(ds.dataStore,
		C.uint64_t(writerOID), C.uint64_t(collectionOID), C.uint64_t(doneChOID))
	errno := int(r.error)
	err = getErrorFromErrNo(errno)
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
func (ds *StateMachineWrapper) ConcurrentSnapshot() bool {
	return false
}

// OnDiskStateMachine returns a boolean flag indicating whether the state
// machine is an on disk state machine.
func (ds *StateMachineWrapper) OnDiskStateMachine() bool {
	return false
}

// RecoverFromSnapshot recovers the state of the data store from the snapshot
// file specified by the fp input string.
func (ds *StateMachineWrapper) RecoverFromSnapshot(reader *rsm.SnapshotReader,
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
	r := C.RecoverFromSnapshotDBStateMachine(ds.dataStore,
		cf, C.uint64_t(readerOID), C.uint64_t(doneChOID))
	return getErrorFromErrNo(int(r))
}

// StateMachineType returns the state machine type.
func (ds *StateMachineWrapper) StateMachineType() pb.StateMachineType {
	return pb.RegularStateMachine
}

func (ds *StateMachineWrapper) ensureNotDestroyed() {
	if ds.Destroyed() {
		panic("using a destroyed data store instance detected")
	}
}

func getCPPSOFileName(dsname string) string {
	d := strings.ToLower(dsname)
	return fmt.Sprintf("./dragonboat-cpp-plugin-%s.so", d)
}
