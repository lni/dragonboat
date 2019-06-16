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
	"io"
)

// IConcurrentStateMachine is the interface to be implemented by application's
// state machine when concurrent access to the state machine is required. For
// a typical IConcurrentStateMachine type, most of its managed data is expected
// to be fitted into memory, Dragonboat manages its captured snapshots and
// saved Raft Logs to ensure such in-memory state machine state can be restored
// after reboot.
//
// The Update method is always invoked from the same goroutine. The Lookup,
// SaveSnapshot and GetHash methods can be invoked concurrent to the Update
// method. The Lookup method is also allowed to be invoked concurrent to the
// RecoverFromSnapshot method. It is user's responsibility to implement the
// IConcurrentStateMachine instance with such supported concurrency while also
// protecting the state machine data integrity. Invocations to the Update,
// PrepareSnapshot, RecoverFromSnapshot and the Close methods are guarded by
// the system to ensure mutual exclusion.
//
// When created, an IConcurrentStateMachine instance should always start from an
// empty state. Dragonboat will use saved snapshots and Raft Logs to help a
// restarted IConcurrentStateMachine instance to catch up to its previous state.
//
// IConcurrentStateMachine is provided as an alternative option to the
// IStateMachine interface which also keeps state machine data mostly in memory
// but without concurrent access support. Users are recommended to use the
// IStateMachine interface whenever possible.
type IConcurrentStateMachine interface {
	// Update updates the IConcurrentStateMachine instance. The input Entry slice
	// is list of continuous proposed and committed commands from clients, they
	// are provided together as a batch so the IConcurrentStateMachine
	// implementation can choose to batch them and apply together to hide latency.
	//
	// The Update method must be deterministic, meaning that given the same initial
	// state of IConcurrentStateMachine and the same input sequence, it should
	// reach to the same updated state and outputs the same returned results. The
	// input entry slice should be the only input to this method. Reading from
	// the system clock, random number generator or other similar external data
	// sources will violate the deterministic requirement of the Update method.
	//
	// The IConcurrentStateMachine implementation should not keep a reference to
	// the input entry slice after return.
	//
	// Update returns the input entry slice with the Result field of all its
	// members set.
	//
	// Update returns an error when there is unrecoverable error for updating the
	// on disk state machine, e.g. disk failure when trying to update the state
	// machine.
	Update([]Entry) ([]Entry, error)
	// Lookup queries the state of the IConcurrentStateMachine instance and
	// returns the query result as a byte slice. The input byte slice specifies
	// what to query, it is up to the IConcurrentStateMachine implementation to
	// interpret the input byte slice.
	//
	// When an error is returned by the Lookup() method, the error will be passed
	// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
	// handled. A typical scenario for returning an error is that the state
	// machine has already been closed or aborted from a RecoverFromSnapshot
	// procedure when Lookup is being handled.
	//
	// The IConcurrentStateMachine implementation should not keep a reference of
	// the input byte slice after return.
	//
	// The Lookup() method is a read only method, it should never change the state
	// of the IConcurrentStateMachine instance.
	Lookup(interface{}) (interface{}, error)
	// PrepareSnapshot prepares the snapshot to be concurrently captured and saved.
	// PrepareSnapshot is invoked before SaveSnapshot is called and it is invoked
	// with mutual exclusion protection from the Update method.
	//
	// PrepareSnapshot in general saves a state identifier of the current state,
	// such state identifier is usually a version number, a sequence number, a
	// change ID or some other in memory small data structure used for describing
	// the point in time state of the state machine. The state identifier is
	// returned as an interface{} and it is provided to the SaveSnapshot() method
	// so the state machine state at that identified point in time can be saved
	// when SaveSnapshot is invoked.
	//
	// PrepareSnapshot returns an error when there is unrecoverable error for
	// preparing the snapshot.
	PrepareSnapshot() (interface{}, error)
	// SaveSnapshot saves the point in time state of the IConcurrentStateMachine
	// identified by the input state identifier to the provided io.Writer backed
	// by a file on disk and the provided ISnapshotFileCollection instance. This
	// is a read only method that should never change the state of the
	// IConcurrentStateMachine instance.
	//
	// It is important to understand that SaveSnapshot should never save the
	// current latest state. The point in time state identified by the input state
	// identifier is what suppose to be saved, the latest state might be different
	// from such specified point in time state as the state machine might have
	// already been updated by the Update() method after the completion of
	// the call to PrepareSnapshot.
	//
	// It is SaveSnapshot's responsibility to free the resources owned by the
	// input state identifier when it is done.
	//
	// The ISnapshotFileCollection instance is used to record finalized external
	// files that should also be included as a part of the snapshot. All other
	// state data should be saved into the io.Writer backed by snapshot file on
	// disk. It is application's responsibility to save the complete state so
	// that the recovered IConcurrentStateMachine state is considered as
	// identical to the original state.
	//
	// The provided read-only chan struct{} is to notify the SaveSnapshot method
	// that the associated Raft node is being closed so the IConcurrentStateMachine
	// can choose to abort the SaveSnapshot procedure and return
	// ErrSnapshotStopped immediately.
	//
	// SaveSnapshot returns the encountered error when generating the snapshot.
	// Other than the above mentioned ErrSnapshotStopped error, the
	// IConcurrentStateMachine implementation should only return a non-nil error
	// when the system need to be immediately halted for critical errors, e.g.
	// disk error preventing you from saving the snapshot.
	SaveSnapshot(interface{},
		io.Writer, ISnapshotFileCollection, <-chan struct{}) error
	// RecoverFromSnapshot recovers the state of the IConcurrentStateMachine
	// instance from a previously saved snapshot captured by the SaveSnapshot()
	// method. The saved snapshot is provided as an io.Reader backed by a file
	// on disk together with a list of files previously recorded into the
	// ISnapshotFileCollection in SaveSnapshot().
	//
	// Dragonboat ensures that Update() and Close() will not be invoked when
	// RecoverFromSnapshot() is in progress.
	//
	// The provided read-only chan struct{} is provided to notify the
	// RecoverFromSnapshot() method that the associated Raft node is being closed.
	// On receiving such notification, RecoverFromSnapshot() can choose to
	// abort recovering from the snapshot and return an ErrSnapshotStopped error
	// immediately. Other than ErrSnapshotStopped, IConcurrentStateMachine should
	// only return a non-nil error when the system need to be immediately halted
	// for non-recoverable error, e.g. disk error preventing you from reading the
	// complete saved snapshot.
	//
	// RecoverFromSnapshot is invoked when restarting from a previously saved
	// state or when the Raft node is significantly behind its leader.
	RecoverFromSnapshot(io.Reader, []SnapshotFile, <-chan struct{}) error
	// Close closes the IConcurrentStateMachine instance.
	//
	// The Close method is not allowed to update the state of the
	// IConcurrentStateMachine visible to the Lookup() method.
	//
	// Close allows the application to finalize resources to a state easier to
	// be re-opened and used in the future. It is important to understand that
	// Close is not guaranteed to be always called, e.g. node might crash at any
	// time. IConcurrentStateMachine should be designed in a way that the
	// safety and integrity of its managed data doesn't rely on whether the Close
	// method is called or not.
	//
	// Other than setting up some internal flags to indicate that the
	// IConcurrentStateMachine instance has been closed, the Close method is not
	// allowed to update the state of IConcurrentStateMachine visible to the
	// Lookup method.
	Close() error
}
