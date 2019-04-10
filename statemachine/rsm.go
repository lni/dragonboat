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
Package statemachine contains the definitions of the IStateMachine and
IConcurrentStateMachine interfaces required to be implemented by dragonboat
based applications.

Dragonboat users should determine whether the application state machine should
implement the IStateMachine or IConcurrentStateMachine interface based on
concurrent access requirements -

For in memory state machines which in general have low read/write latencies,
IStateMachine is usually employed. The major drawback is that each IStateMachine
is internally guarded by a sync.RWMutex so read/write accesses are not allowed
to be concurrently invoked on IStateMachine. Multiple read requests can be
concurrently invoked on the same IStateMachine instance.

For IConcurrentStateMachine based application state machine, the major
difference is that read/write accesses can be concurrently invoked. This allows
Lookup() to be invoked concurrent to Update(), or Update() can be executed when
SaveSnapshot() is in progress. It is up to the user implementation of such
IConcurrentStateMachine type to correctly and safely maintain its internal data
structures during such concurrent accesses.

When you are not sure which one to choose, the rule of thumb is to implement
IStateMachine for your application and upgrade it to a IConcurrentStateMachine
when necessary.
*/
package statemachine

import (
	"errors"
	"io"
)

var (
	// ErrSnapshotStopped is returned by the SaveSnapshot and RecoverFromSnapshot
	// methods of the IStateMachine interface to indicate that those two snapshot
	// related operations have been aborted as the associated raft node is being
	// closed.
	ErrSnapshotStopped = errors.New("snapshot stopped")
)

// Type is the state machine type.
type Type uint64

const (
	// RegularStateMachine is the state machine type that implements
	// IStateMachine.
	RegularStateMachine = 1
	// ConcurrentStateMachine is the state machine type that implements
	// IConcurrentStateMachine
	ConcurrentStateMachine = 2
	// OnDiskStateMachine is the state machine type that implements
	// IOnDiskStateMachine
	OnDiskStateMachine = 3
)

// SnapshotFile is the struct used to describe external files included in a
// snapshot.
type SnapshotFile struct {
	// FileID is the ID of the file provided to ISnapshotFileCollection.AddFile().
	FileID uint64
	// Filepath is the current full path of the file.
	Filepath string
	// Metadata is the metadata provided to ISnapshotFileCollection.AddFile().
	Metadata []byte
}

// ISnapshotFileCollection is the interface used by the
// IStateMachine.SaveSnapshot() method for recording external files that should
// be included as a part of the snapshot being created.
//
// For example, consider you have a IStateMachine implementation internally
// backed by a NoSQL DB, when creating a snapshot of the IStateMachine instance,
// the state of the NoSQL DB need to be captured and saved as well. When the
// NoSQL has its own built-in feature to snapshot the current state into some
// on-disk files, these files should be included in IStateMachine's snapshot so
// they can be used to reconstruct the current state of the NoSQL DB when the
// IStateMachine instance recovers from the saved snapshot.
type ISnapshotFileCollection interface {
	// AddFile adds an external file to the snapshot being currently generated.
	// The file must has been finalized meaning its content will not change in
	// the future. It is your application's responsibility to make sure that the
	// file being added can be accessible from the current process and it is
	// possible to create a hard link to it from the NodeHostDir directory
	// specified in NodeHost's NodeHostConfig. The file to be added is identified
	// by the specified fileID. The metadata byte slice is the metadata of the
	// file being added, it can be the checksum of the file, file type, file name,
	// other file hierarchy information, or a serialized combination of such
	// metadata.
	AddFile(fileID uint64, path string, metadata []byte)
}

// IStateMachine is the interface required to be implemented by application's
// state machine, it defines how application data should be internally stored,
// updated and queried.
//
// Each IStateMachine instance is associated with a single Raft group, which
// usually has multiple replicas known as Raft nodes distributed across the
// network. All update request, known as proposals, are arranged as a Raft log.
// Raft protocol is used to determine the sequence of such a Raft log. Once a
// proposal is accepted by the majority of Raft nodes, it is considered as
// committed. Committed proposals will be passed to the Update() method of all
// IStateMachine replicas across the network in the exact same sequence. This
// is known as the replicated state machine approach in distributed systems.
//
// The Update() method must be deterministic, meaning given the same initial
// state of an IStateMachine and the same update sequence guaranteed by the
// Raft protocol, IStateMachine should reach to the same updated state and
// generates the same returned value for each update request.
//
// Dragonboat uses a sync.RWMutex reader/writer mutual exclusion lock to guard
// the IStateMachine instance when invoking IStateMachine methods. Update(),
// RecoverFromSnapshot() and Close() are invoked when the write lock is
// acquired, other methods are invoked when the read lock is acquired.
//
// When created, an IStateMachine instance should always start from an empty
// state. Dragonboat will use saved snapshots and Raft Logs to help a restarted
// IStateMachine instance to catch up to its previous state.
type IStateMachine interface {
	// Update updates the IStateMachine instance. The input data slice is the
	// proposed command from client that has been committed by the Raft cluster.
	// It is up to the IStateMachine implementation to interpret this input byte
	// slice and update the IStateMachine instance accordingly.
	//
	// The Update() method must be deterministic, meaning given the same initial
	// state of IStateMachine and the same input, it should reach to the same
	// updated state and outputs the same returned value. The input byte slice
	// should be the only input to this method. Reading from system clock, random
	// number generator or other similar data sources will violate the
	// deterministic requirement of the Update() method.
	//
	// The IStateMachine implementation should not keep a reference to the input
	// byte slice after the return of the Update() method.
	//
	// Update returns an uint64 value used to indicate the result of the update
	// operation.
	Update([]byte) uint64
	// Lookup queries the state of the IStateMachine instance and returns the
	// query result as a byte slice. The input byte slice specifies what to query,
	// it is up to the IStateMachine implementation to interpret the input byte
	// slice. The returned byte slice contains the query result provided by the
	// IStateMachine implementation.
	//
	// The IStateMachine implementation should not keep a reference of the input
	// byte slice after the return of the Lookup() method.
	//
	// The Lookup method is a read only method, it should never change the state
	// of IStateMachine.
	Lookup([]byte) []byte
	// SaveSnapshot saves the state of the IStateMachine instance to the provided
	// io.Writer backed by a file on disk and the provided ISnapshotFileCollection
	// instance. This is a read only operation on the IStateMachine instance.
	//
	// The data saved into the io.Writer is usually the in-memory data, while the
	// ISnapshotFileCollection instance is used to record finalized external files
	// that should also be included as a part of the snapshot. It is application's
	// responsibility to save the complete state so that the recovered
	// IStateMachine state is considered as identical to the original state.
	//
	// The provided read-only chan struct{} is used to notify the SaveSnapshot
	// method that the associated Raft node is being closed so the IStateMachine
	// can choose to abort the SaveSnapshot procedure and return
	// ErrSnapshotStopped immediately.
	//
	// SaveSnapshot returns the number of bytes written to the provided io.Write
	// and the encountered error when generating the snapshot. Other than the
	// above mentioned ErrSnapshotStopped error, the IStateMachine implementation
	// should only return a non-nil error when the system need to be immediately
	// halted for critical errors, e.g. disk error preventing you from saving the
	// snapshot.
	SaveSnapshot(io.Writer,
		ISnapshotFileCollection, <-chan struct{}) (uint64, error)
	// RecoverFromSnapshot recovers the state of the IStateMachine object from a
	// previously saved snapshot captured by the SaveSnapshot() method. The
	// saved snapshot is provided as an io.Reader backed by a file on disk and
	// a list of files previously recorded into the ISnapshotFileCollection in
	// SaveSnapshot().
	//
	// The provided read-only chan struct{} is used to notify the
	// RecoverFromSnapshot() method that the associated Raft node is being closed.
	// On receiving such notification, RecoverFromSnapshot() can choose to
	// abort recovering from the snapshot and return an ErrSnapshotStopped error
	// immediately. Other than ErrSnapshotStopped, IStateMachine should only
	// return a non-nil error when the system need to be immediately halted for
	// non-recoverable error, e.g. disk error preventing you from reading the
	// complete saved snapshot.
	//
	// RecoverFromSnapshot is invoked when restarting from a previously saved
	// state or when the raft node is significantly behind its leader.
	RecoverFromSnapshot(io.Reader, []SnapshotFile, <-chan struct{}) error
	// Close closes the IStateMachine instance.
	//
	// The Close method is not allowed to update the state of the IStateMachine
	// visible to IStateMachine's Lookup method.
	//
	// This allows the application to finalize resources to a state easier to be
	// re-opened and used in the future. It is important to understand that Close()
	// is not guaranteed to be always called, e.g. node might crash at any time.
	// IStateMachine should be designed in a way that the safety and integrity of
	// its managed data doesn't rely on the Close() method.
	Close()
	// GetHash returns a uint64 value used to represent the current state of the
	// IStateMachine, usually generated by hashing IStateMachine's serialized
	// state.
	// This method is an optional method, application can ignore this testing
	// related capability by returning a constant uint64 value from this method.
	//
	// GetHash is a read only operation on the IStateMachine instance.
	GetHash() uint64
}

// Entry represents a Raft log entry that is going to be provided to the Update
// method of an IConcurrentStateMachine instance.
type Entry struct {
	// Index is the Raft log index of the entry. The field is set by the
	// Dragonboat library and it is read-only for user IConcurrentStateMachine
	// instances.
	Index uint64
	// Result is the result value obtained from the Update method of an
	// IConcurrentStateMachine instance. This field is set by user
	// IConcurrentStateMachine instances.
	Result uint64
	// Cmd is the proposed command. This field is read-only for user
	// IConcurrentStateMachine instances.
	Cmd []byte
}

// IConcurrentStateMachine is the interface to be implemented by application's
// state machine when concurrent access to the state machine is required. An
// IConcurrentStateMachine instance allows Lookup() and Update() to be invoked
// concurrently, it also allows Update() to be concurrently executed when
// SaveSnapshot() is in progress. It defines how application data should be
// internally stored, updated and queried concurrently.
//
// The Update() method is always invoked from the same goroutine. The Lookup(),
// SaveSnapshot() and GetHash() methods can be invoked concurrent to the
// Update() method. It is user's responsibility to implement the
// IConcurrentStateMachine instance with such concurrency. Invocations to the
// Update(), PrepareSnapshot() and RecoverFromSnapshot() methods are guarded by
// the system to ensure mutual exclusion.
//
// When created, an IConcurrentStateMachine instance should always start from an
// empty state. Dragonboat will use saved snapshots and Raft Logs to help a
// restarted IConcurrentStateMachine instance to catch up to its previous state.
type IConcurrentStateMachine interface {
	// Update updates the IConcurrentStateMachine instance. The input Entry slice
	// is list of continuous proposed and committed commands from clients, they
	// are provided together as a batch so the IConcurrentStateMachine
	// implementation can choose to batch them and apply together to hide latency.
	//
	// The Update() method must be deterministic, meaning given the same initial
	// state of IConcurrentStateMachine and the same input sequence, it should
	// reach to the same updated state and outputs the same returned value. The
	// input entry slice should be the only input to this method. Reading from
	// the system clock, random number generator or other similar external data
	// sources will violate the deterministic requirement of the Update() method.
	//
	// The IConcurrentStateMachine implementation should not keep a reference to
	// the input entry slice after the return of the Update() method.
	//
	// Update returns the input entry slice with the Result field of all its
	// members set.
	Update([]Entry) []Entry
	// Lookup queries the state of the IConcurrentStateMachine instance and
	// returns the query result as a byte slice. The input byte slice specifies
	// what to query, it is up to the IConcurrentStateMachine implementation to
	// interpret the input byte slice. The returned byte slice contains the query
	// result provided by the IConcurrentStateMachine implementation.
	//
	// When an error is returned by the Lookup() method, the error will be passed
	// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
	// handled. A typical scenario for returning an error is that the state
	// machine has already been closed or aborted from a RecoverFromSnapshot()
	// procedure when Lookup() is being handled.
	//
	// The IConcurrentStateMachine implementation should not keep a reference of
	// the input byte slice after the return of the Lookup() method.
	//
	// The Lookup() method is a read only method, it should never change the state
	// of IConcurrentStateMachine.
	Lookup([]byte) ([]byte, error)
	// PrepareSnapshot prepares the snapshot to be concurrently captured and saved.
	// PrepareSnapshot() is invoked before SaveSnapshot() is called and it is
	// invoked with mutual exclusion protection from the Update() method.
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
	// SaveSnapshot saves the point in time state of the IConcurrentStateMachine
	// identified by the input state identifier to the provided io.Writer backed
	// by a file on disk and the provided ISnapshotFileCollection instance. This
	// is a read only method that should never change the state of the
	// IConcurrentStateMachine instance. It is important to understand that
	// SaveSnapshot() should never be implemented to save the current latest state
	// of the state machine when it is invoked. The latest state is not what
	// suppose to be saved as the state might have already been concurrently
	// updated by the Update() method after the completion of PrepareSnapshot().
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
	// The provided read-only chan struct{} is provided to notify the SaveSnapshot
	// method that the associated Raft node is being closed so the
	// IConcurrentStateMachine can choose to abort the SaveSnapshot procedure and
	// return ErrSnapshotStopped immediately.
	//
	// SaveSnapshot returns the number of bytes written to the provided io.Write
	// and the encountered error when generating the snapshot. Other than the
	// above mentioned ErrSnapshotStopped error, the IConcurrentStateMachine
	// implementation should only return a non-nil error when the system need to
	// be immediately halted for critical errors, e.g. disk error preventing you
	// from saving the snapshot.
	SaveSnapshot(interface{},
		io.Writer, ISnapshotFileCollection, <-chan struct{}) (uint64, error)
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
	// Close() allows the application to finalize resources to a state easier to
	// be re-opened and used in the future. It is important to understand that
	// Close() is not guaranteed to be always called, e.g. node might crash at
	// any time. IConcurrentStateMachine should be designed in a way that the
	// safety and integrity of its managed data doesn't rely on the Close()
	// method.
	//
	// Other than setting up some internal flags to indicate that the
	// IConcurrentStateMachine instance has been closed, the Close() method is
	// not allowed to update the state of IConcurrentStateMachine visible to the
	// Lookup() method.
	Close()
	// GetHash returns a uint64 value used to represent the current state of the
	// IConcurrentStateMachine, usually generated by hashing
	// IConcurrentStateMachine's serialized state.
	//
	// The feature backed by this method is optional, application can ignore this
	// testing related capability by always returning a constant uint64 value from
	// this method.
	//
	// GetHash is a read only method on the IConcurrentStateMachine instance.
	GetHash() uint64
}
