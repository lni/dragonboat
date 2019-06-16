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

package statemachine

import (
	"errors"

	"io"
)

var (
	// ErrSnapshotStreaming is the error returned when the snapshot data being
	// generated can not be streamed to its intended destination.
	ErrSnapshotStreaming = errors.New("failed to stream the snapshot")
	// ErrOpenStopped is the error returned by the Open method of an
	// IOnDiskStateMachine type when it chooses to abort from its Open method.
	ErrOpenStopped = errors.New("open method did not complete")
)

// IOnDiskStateMachine is the interface to be implemented by application's
// state machine when the state machine state is always persisted on disks.
// IOnDiskStateMachine basically matches the state machine type described
// in the section 5.2 of the Raft thesis.
//
// For IOnDiskStateMachine types, concurrent access to the state machine is
// supported. An IOnDiskStateMachine type allows its Update method to be
// concurrently invoked when there are ongoing calls to the Lookup or the
// SaveSnapshot method. Lookup is also allowed when the RecoverFromSnapshot or
// the Close methods are being invoked. Invocations to the Update, Sync,
// PrepareSnapshot, RecoverFromSnapshot and Close methods are guarded by the
// system to ensure mutual exclusion.
//
// Once created, the Open method is immediately invoked to open and use the
// persisted state on disk to continue from the persisted state. This makes
// IOnDiskStateMachine quite different from the IStateMachine types which
// require the state machine state to be fully reconstructed from saved
// snapshots and Raft Log entries.
//
// As IOnDiskStateMachine always has most of its state kept on disk by design.
// Its SaveSnapshot method is thus only invoked on demand when a full snapshot
// is required to be streamed to a remote node to help it to catch up.
//
// Applications that implement IOnDiskStateMachine are still recommended to
// setup periodic snapshotting, that only triggers the state machine's
// metadata to be periodically snapshotted and thus adds negligible overheads
// to the system. It also provides an opportunities for the system to signal
// Raft Log compaction to free up disk spaces.
type IOnDiskStateMachine interface {
	// Open opens the existing on disk state machine to be used or it creates a
	// new state machine with empty state if it does not exist. Open returns the
	// most recent index value of the Raft log that has been persisted, or it
	// returns 0 when the state machine is a new one.
	//
	// The provided read only chan struct{} channel is used to notify the Open
	// method that the node has been stopped and the Open method can choose to
	// abort by returning an ErrOpenStopped error.
	//
	// Open is called shortly after the Raft node is started. The Update method
	// and the Lookup method will not be called before the completion of the Open
	// method.
	Open(stopc <-chan struct{}) (uint64, error)
	// Update updates the IOnDiskStateMachine instance. The input Entry slice
	// is a list of continuous proposed and committed commands from clients, they
	// are provided together as a batch so the IOnDiskStateMachine implementation
	// can choose to batch them and apply together to hide latency. Update returns
	// the input entry slice with the Result field of all its members set.
	//
	// The read only Index field of each input Entry instance is the Raft log
	// index of each entry, it is IOnDiskStateMachine's responsibility to
	// atomically persist the Index value together with the corresponding state
	// update.
	//
	// The Update method can choose to synchronize all of its in-core state with
	// that on disk. This can minimize the number of committed Raft entries that
	// need to be re-applied after reboot. Update is also allowed to postpone such
	// synchronization until the Sync method is invoked, this approach produces
	// higher throughput during fault free running at the cost that some of the
	// most recent Raft entries not synchronized onto disks will have to be
	// re-applied after reboot.
	//
	// When the Update method does not synchronize its in-core state with that on
	// disk, the implementation must ensure that after a reboot there is no
	// applied entry in the State Machine more recent than any entry that was
	// lost during reboot. For example, consider a state machine with 3 applied
	// entries, let's assume their index values to be 1, 2 and 3. Once they have
	// been applied into the state machine without synchronizing the in-core state
	// with that on disk, it is okay to lose the data associated with the applied
	// entry 3, but it is strictly forbidden to have the data associated with the
	// applied entry 3 available in the state machine while the one with index
	// value 2 got lost during reboot.
	//
	// The Update method must be deterministic, meaning given the same initial
	// state of IOnDiskStateMachine and the same input sequence, it should reach
	// to the same updated state and outputs the same results. The input entry
	// slice should be the only input to this method. Reading from the system
	// clock, random number generator or other similar external data sources will
	// violate the deterministic requirement of the Update method.
	//
	// Concurrent call to the Lookup method and the SaveSnapshot method is allowed
	// when the state machine is being updated by the Update method.
	//
	// The IOnDiskStateMachine implementation should not keep a reference to the
	// input entry slice after return.
	//
	// Update returns an error when there is unrecoverable error when updating the
	// on disk state machine.
	Update([]Entry) ([]Entry, error)
	// Lookup queries the state of the IOnDiskStateMachine instance and returns
	// the query result as an interface{}. The input interface{} specifies what to
	// query, it is up to the IOnDiskStateMachine implementation to interpret the
	// input interface{} parameter. The returned interface{} contains the query
	// result.
	//
	// When an error is returned by the Lookup method, the error will be passed
	// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
	// handled. A typical scenario for returning an error is that the state
	// machine has already been closed or aborted from a RecoverFromSnapshot
	// procedure before Lookup is handled. You implementation is expected to
	// panic when there is any unrecoverable error during Lookup.
	//
	// Concurrent call to the Update and RecoverFromSnapshot method should be
	// allowed when call to the Lookup method is being processed.
	//
	// The IOnDiskStateMachine implementation should not keep a reference of
	// the instance pointed by the input interface{} after return.
	//
	// The Lookup method is a read only method, it should never change the state
	// of IOnDiskStateMachine.
	Lookup(interface{}) (interface{}, error)
	// Sync synchronizes all in-core state of the state machine to permanent
	// storage so the state machine can continue from its latest state after
	// reboot.
	//
	// Sync is always invoked with mutual exclusion protection from the Update,
	// PrepareSnapshot, RecoverFromSnapshot and Close method.
	//
	// Sync returns an error when there is unrecoverable error for synchronizing
	// the in-core state.
	Sync() error
	// PrepareSnapshot prepares the snapshot to be concurrently captured and
	// streamed. PrepareSnapshot is invoked before SaveSnapshot is called and it
	// is always invoked with mutual exclusion protection from the Update, Sync,
	// RecoverFromSnapshot and Close methods.
	//
	// PrepareSnapshot in general saves a state identifier of the current state,
	// such state identifier can be a version number, a sequence number, a change
	// ID or some other in memory small data structure used for describing the
	// point in time state of the state machine. The state identifier is returned
	// as an interface{} before being passed to the SaveSnapshot() method.
	//
	// PrepareSnapshot returns an error when there is unrecoverable error for
	// preparing the snapshot.
	PrepareSnapshot() (interface{}, error)
	// SaveSnapshot saves the point in time state of the IOnDiskStateMachine
	// instance identified by the input state identifier to the provided
	// io.Writer. The io.Writer is a connection to a remote node usually
	// significantly behind in terms of Raft log progress.
	//
	// It is important to understand that SaveSnapshot should never be implemented
	// to save the current latest state of the state machine when it is invoked.
	// SaveSnapshot must be implemented to save the point in time state identified
	// by the input state identifier.
	//
	// It is application's responsibility to save the complete state to the
	// provided io.Writer in a deterministic manner. That is for the same state
	// machine state, when SaveSnapshot is invoked multiple times with the same
	// input, the content written to the provided io.Writer should always be the
	// same.
	//
	// When there is any connectivity error between the local node and the remote
	// node, an ErrSnapshotStreaming will be returned by io.Writer's Write method
	// when trying to write data to it. The SaveSnapshot method should return
	// ErrSnapshotStreaming to terminate early.
	//
	// It is SaveSnapshot's responsibility to free the resources owned by the
	// input state identifier when it is done.
	//
	// The provided read-only chan struct{} is provided to notify the SaveSnapshot
	// method that the associated Raft node is being closed so the
	// IOnDiskStateMachine can choose to abort the SaveSnapshot procedure and
	// return ErrSnapshotStopped immediately.
	//
	// The SaveSnapshot method is allowed to be invoked when there is concurrent
	// call to the Update method. SaveSnapshot is a read-only method, it should
	// never change the state of the IOnDiskStateMachine.
	SaveSnapshot(interface{}, io.Writer, <-chan struct{}) error
	// RecoverFromSnapshot recovers the state of the IOnDiskStateMachine instance
	// from a snapshot captured by the SaveSnapshot() method on a remote node. The
	// saved snapshot is provided as an io.Reader backed by a file already fully
	// available on disk.
	//
	// Dragonboat ensures that the Update, Sync, PrepareSnapshot, SaveSnapshot and
	// Close methods will not be invoked when RecoverFromSnapshot() is in
	// progress.
	//
	// The provided read-only chan struct{} is provided to notify the
	// RecoverFromSnapshot method that the associated Raft node has been closed.
	// On receiving such notification, RecoverFromSnapshot() can choose to
	// abort recovering from the snapshot and return an ErrSnapshotStopped error
	// immediately. Other than ErrSnapshotStopped, IOnDiskStateMachine should
	// only return a non-nil error when the system need to be immediately halted
	// for non-recoverable error.
	//
	// RecoverFromSnapshot is not required to synchronize its in-core state with
	// that on disk.
	RecoverFromSnapshot(io.Reader, <-chan struct{}) error
	// Close closes the IOnDiskStateMachine instance.
	//
	// Close allows the application to finalize resources to a state easier to
	// be re-opened and restarted in the future. It is important to understand
	// that Close is not guaranteed to be always called, e.g. node can crash at
	// any time without calling Close. IOnDiskStateMachine should be designed
	// in a way that the safety and integrity of its on disk data doesn't rely
	// on whether Close is eventually called or not.
	//
	// Other than setting up some internal flags to indicate that the
	// IOnDiskStateMachine instance has been closed, the Close method is not
	// allowed to update the state of IOnDiskStateMachine visible to the outside.
	Close() error
}
