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

package statemachine

import (
	"errors"

	"io"
)

var (
	// ErrSnapshotStreaming is the error returned when the snapshot data being
	// generated can not be streamed to its intended destination.
	ErrSnapshotStreaming = errors.New("failed to stream the snapshot")
	ErrOpenStopped       = errors.New("open method did not complete")
)

// IOnDiskStateMachine is the interface to be implemented by application's
// state machine when the state of the state machine is always persisted on
// disks.
//
// Similar to IConcurrentStateMachine, concurrent access to the state machine
// is allowed. An IOnDiskStateMachine type allows its Update method to be
// concurrently invoked when there are ongoing calls to the Lookup or
// the SaveSnapshot method. Lookup is also allowed when the RecoverFromSnapshot
// or Close methods are being invoked. Invocations to the Update,
// PrepareSnapshot and RecoverFromSnapshot methods are guarded by the system
// to ensure mutual exclusion.
//
// When created, IOnDiskStateMachine should always start from an empty state,
// its Open method will be invoked to open and use its saved state on disk so
// the IOnDiskStateMachine instance can continue from its saved state on disk.
//
// Different from the IStateMachine and IConcurrentStateMachine types which
// rely on periodical snapshotting to capture the state of the entire state
// machine, IOnDiskStateMachine already has its state always kept on disk. Its
// SaveSnapshot method is thus only invoked on demand when a snapshot is
// required to be streamed to a remote node to help the remote node to catch up.
type IOnDiskStateMachine interface {
	// Open opens the existing on disk state machine to be used or it creates a
	// new state machine if it does not exist. Open returns the most recent index
	// value of the Raft log it has processed, or it returns 0 when the state
	// machine is new.
	//
	// The provided stopc channel is used to notify the Open method that node has
	// been stopped and the Open method can choose to abort by returning an
	// ErrOpenStopped error.
	//
	// Open is called shortly after the Raft node is started. The Update method
	// and the Lookup method will not be called until call to the Open method is
	// successfully completed.
	Open(stopc <-chan struct{}) (uint64, error)
	// Update updates the IOnDiskStateMachine instance. The input Entry slice
	// is a list of continuous proposed and committed commands from clients, they
	// are provided together as a batch so the IOnDiskStateMachine
	// implementation can choose to batch them and apply together to hide latency.
	// Update returns the input entry slice with the Result field of all its
	// members set.
	//
	// The Index field of each input Entry instance is the Raft log index of each
	// entry, it is IOnDiskStateMachine's responsibility to persist the Index
	// value of the most recent processed entry. It is the index value to be
	// returned by the Open method when the IOnDiskStateMachine instance reboots.
	//
	// The Update method must persist all state changes to its permanent storage
	// so the the state machine can continue from its latest state after node
	// reboot. In many cases, synchronizing the in-core state of all involved
	// files with that on disk is required.
	//
	// The Update() method must be deterministic, meaning given the same initial
	// state of IOnDiskStateMachine and the same input sequence, it should
	// reach to the same updated state and outputs the same results. The input
	// entry slice should be the only input to this method. Reading from the
	// system clock, random number generator or other similar external data
	// sources will violate the deterministic requirement of the Update() method.
	//
	// Concurrent call to the Lookup method and the SaveSnapshot method is
	// allowed when the state machine is being updated via the Update method.
	//
	// The IOnDiskStateMachine implementation should not keep a reference to
	// the input entry slice after the return of the Update() method.
	Update([]Entry) []Entry
	// Lookup queries the state of the IOnDiskStateMachine instance and
	// returns the query result as a byte slice. The input byte slice specifies
	// what to query, it is up to the IOnDiskStateMachine implementation to
	// interpret the input byte slice. The returned byte slice contains the query
	// result provided by the IOnDiskStateMachine implementation.
	//
	// When an error is returned by the Lookup() method, the error will be passed
	// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
	// handled. A typical scenario for returning an error is that the state
	// machine has already been closed or aborted from a RecoverFromSnapshot()
	// procedure when Lookup() is being handled.
	//
	// Concurrent call to the Update and RecoverFromSnapshot method is possible
	// when call to the Lookup method is being processed.
	//
	// The IOnDiskStateMachine implementation should not keep a reference of
	// the input byte slice after the return of the Lookup() method.
	//
	// The Lookup() method is a read only method, it should never change the state
	// of IOnDiskStateMachine.
	Lookup([]byte) ([]byte, error)
	// PrepareSnapshot prepares the snapshot to be concurrently captured and saved.
	// PrepareSnapshot() is invoked before SaveSnapshot() is called and it is
	// invoked with mutual exclusion protection from the Update and
	// RecoverFromSnapshot method.
	//
	// PrepareSnapshot in general saves a state identifier of the current state,
	// such state identifier is usually a version number, a sequence number, a
	// change ID or some other in memory small data structure used for describing
	// the point in time state of the state machine. The state identifier is
	// returned as an interface{} and it is provided to the SaveSnapshot() method
	// so a snapshot of the state machine state in that identified point in time
	// can be saved when SaveSnapshot() is invoked.
	//
	// PrepareSnapshot returns an error when there is unrecoverable error for
	// preparing the snapshot.
	PrepareSnapshot() (interface{}, error)
	// SaveSnapshot saves the point in time state of the IOnDiskStateMachine
	// instance identified by the input state identifier to the provided
	// io.Writer. The io.Writer is a connection to a remote node usually
	// significantly behind in terms of its state progress. It is important to
	// understand that SaveSnapshot() should never be implemented to save the
	// current latest state of the state machine when it is invoked. The latest
	// state is not what suppose to be saved as the state might have already
	// been concurrently updated by the Update() method after the completion
	// of PrepareSnapshot().
	//
	// It is application's responsibility to save the complete state to the
	// provided io.Writer. This is a read-only method, it should never change the
	// state of the IOnDiskStateMachine.
	//
	// When there is any connectivity error between the local node and the remote
	// node, an ErrSnapshotStreaming will be returned by io.Writer's Write method
	// when trying to write data to it. The SaveSnapshot method should return
	// ErrSnapshotStreaming to its caller and terminate early.
	//
	// It is SaveSnapshot's responsibility to free the resources owned by the
	// input state identifier when it is done.
	//
	// The provided read-only chan struct{} is provided to notify the SaveSnapshot
	// method that the associated Raft node is being closed so the
	// IOnDiskStateMachine can choose to abort the SaveSnapshot procedure and
	// return ErrSnapshotStopped immediately.
	//
	// The SaveSnapshot method is possible be invoked when there is concurrent
	// call to the Update method.
	SaveSnapshot(interface{}, io.Writer, <-chan struct{}) (uint64, error)
	// RecoverFromSnapshot recovers the state of the IOnDiskStateMachine
	// instance from a snapshot captured by the SaveSnapshot() method. The saved
	// snapshot is provided as an io.Reader backed by a file on disk.
	//
	// Dragonboat ensures that the Update and Close methods are not be invoked
	// when RecoverFromSnapshot() is in progress.
	//
	// The provided read-only chan struct{} is provided to notify the
	// RecoverFromSnapshot method that the associated Raft node has been closed.
	// On receiving such notification, RecoverFromSnapshot() can choose to
	// abort recovering from the snapshot and return an ErrSnapshotStopped error
	// immediately. Other than ErrSnapshotStopped, IOnDiskStateMachine should
	// only return a non-nil error when the system need to be immediately halted
	// for non-recoverable error, e.g. disk error preventing you from reading the
	// complete saved snapshot.
	//
	// RecoverFromSnapshot is invoked when the node's progress is significantly
	// behind its leader.
	RecoverFromSnapshot(io.Reader, <-chan struct{}) error
	// Close closes the IOnDiskStateMachine instance.
	//
	// Close() allows the application to finalize resources to a state easier to
	// be re-opened and restarted in the future. It is important to understand
	// that Close() is not guaranteed to be always called, e.g. node can crash at
	// any time without calling Close(). IOnDiskStateMachine should be designed
	// in a way that the safety and integrity of its on disk data doesn't rely
	// on the Close() method.
	//
	// Other than setting up some internal flags to indicate that the
	// IOnDiskStateMachine instance has been closed, the Close() method is
	// not allowed to update the state of IOnDiskStateMachine visible to the
	// Lookup() method.
	Close()
	// GetHash returns a uint64 value used to represent the current state of the
	// IOnDiskStateMachine, usually generated by hashing
	// IOnDiskStateMachine's serialized state.
	//
	// The feature backed by this method is optional, application can ignore this
	// testing related capability by always returning a constant uint64 value from
	// this method.
	//
	// GetHash is a read only method on the IOnDiskStateMachine instance.
	GetHash() uint64
}
