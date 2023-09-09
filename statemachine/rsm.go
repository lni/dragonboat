// Copyright 2018-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
IOnDiskStateMachine interfaces for supporting the replicated state machine
approach.

User services are provided with fault tolerance when they are implemented
as IStateMachine or IOnDiskStateMachine instances. The Update method is
used for update operations for the services, the Lookup method is used for
handling read only queries to the services, snapshot related methods,
including PrepareSnapshot, SaveSnapshot and RecoverFromSnapshot, are used
to update service state based on snapshots.
*/
package statemachine

import (
	"io"

	"github.com/cockroachdb/errors"
)

var (
	// ErrSnapshotStopped is returned by state machine's SaveSnapshot and
	// RecoverFromSnapshot methods to indicate that those two snapshot operations
	// have been aborted as the associated raft node is being closed.
	ErrSnapshotStopped = errors.New("snapshot stopped")
	// ErrSnapshotAborted is returned by state machine's SaveSnapshot method to
	// indicate that the SaveSnapshot operation is aborted by the user.
	ErrSnapshotAborted = errors.New("snapshot aborted")
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
	// The file must have been finalized meaning its content will not change in
	// the future. It is your application's responsibility to make sure that the
	// file being added is accessible from the current process and it is
	// possible to create a hard link to it from the NodeHostDir directory
	// specified in NodeHost's NodeHostConfig. The file to be added is identified
	// by the specified fileID. The metadata byte slice is the metadata of the
	// file being added. It can be the checksum of the file, file type, file name,
	// other file hierarchy information, or a serialized combination of such
	// metadata.
	AddFile(fileID uint64, path string, metadata []byte)
}

// Result is the result type generated and returned by the Update method in
// IStateMachine and IOnDiskStateMachine types.
type Result struct {
	// Value is a 64 bits integer value used to indicate the outcome of the
	// update operation.
	Value uint64
	// Data is an optional byte slice created and returned by the update
	// operation. It is useful for CompareAndSwap style updates in which an
	// arbitrary length of bytes need to be returned.
	// Users are strongly recommended to use the query methods supported by
	// NodeHost to query the state of their IStateMachine and IOnDiskStateMachine
	// types, proposal based queries are known to work but are not recommended.
	Data []byte
}

// Entry represents a Raft log entry that is going to be provided to the Update
// method of an IConcurrentStateMachine or IOnDiskStateMachine instance.
type Entry struct {
	// Index is the Raft log index of the entry. The field is set by the
	// Dragonboat library and it is strictly read-only.
	Index uint64
	// Cmd is the proposed command. This field is strictly read-only.
	Cmd []byte
	// Result is the result value obtained from the Update method of an
	// IConcurrentStateMachine or IOnDiskStateMachine instance.
	Result Result
}

// IStateMachine is the interface to be implemented by application's state
// machine when most of the state machine data is stored in memory. It is the
// basic state machine type described in the Raft thesis.
//
// A sync.RWMutex is used internally by dragonboat as a reader/writer mutual
// exclusion lock to guard the IStateMachine instance when accessing
// IStateMachine's member methods. The Update, RecoverFromSnapshot and Close
// methods are invoked when the write lock is acquired, other methods are
// invoked when the shared read lock is acquired.
//
// As the state is mostly in memory, snapshots are usually periodically captured
// to save its state to disk. After each reboot, IStateMachine state must be
// reconstructed from the empty state based on the latest snapshot and saved
// Raft logs.
//
// When created, an IStateMachine instance should always start from an empty
// state. Saved snapshots and Raft logs will be used to help a rebooted
// IStateMachine instance to catch up to its previous state.
type IStateMachine interface {
	// Update updates the IStateMachine instance. The input data slice is the
	// proposed command from client, it is up to the IStateMachine implementation
	// to interpret this byte slice and update the IStateMachine instance
	// accordingly.
	//
	// The Update method must be deterministic, meaning given the same initial
	// state of IStateMachine and the same input, it should always reach to the
	// same updated state and outputs the same returned value. This requires the
	// input byte slice should be the only input to this method. Reading from
	// system clock, random number generator or other similar data sources will
	// likely violate the deterministic requirement of the Update method.
	//
	// The IStateMachine implementation should not keep a reference to the input
	// byte slice after return.
	//
	// Update returns a Result value used to indicate the outcome of the update
	// operation. An error is returned when there is unrecoverable error, such
	// error will cause the program to panic.
	Update(Entry) (Result, error)
	// Lookup queries the state of the IStateMachine instance. The input
	// interface{} specifies what to query, it is up to the IStateMachine
	// implementation to interpret such input interface{}. The returned
	// interface{} is the query result provided by the IStateMachine
	// implementation.
	//
	// The IStateMachine implementation should not keep a reference of the input
	// interface{} after return. The Lookup method is a read only method, it
	// should never change the state of IStateMachine.
	//
	// When an error is returned by the Lookup method, it will be passed to the
	// user client.
	Lookup(interface{}) (interface{}, error)
	// SaveSnapshot saves the current state of the IStateMachine instance to the
	// provided io.Writer backed by an on disk file. SaveSnapshot is a read only
	// operation.
	//
	// The data saved into the io.Writer is usually the in-memory data, while the
	// ISnapshotFileCollection instance is used to record immutable files that
	// should also be included as a part of the snapshot. It is application's
	// responsibility to save the complete state so that the reconstructed
	// IStateMachine state based on such saved snapshot will be considered as
	// identical to the original state.
	//
	// The provided read-only chan struct{} is used to notify that the associated
	// Raft node is being closed so the implementation can choose to abort the
	// SaveSnapshot procedure and return ErrSnapshotStopped immediately.
	//
	// SaveSnapshot is allowed to abort the snapshotting operation at any time by
	// returning ErrSnapshotAborted.
	//
	// Other than the above mentioned ErrSnapshotStopped and ErrSnapshotAborted
	// errors, the IStateMachine implementation should only return a non-nil error
	// when the system need to be immediately halted for critical errors, e.g.
	// disk error preventing you from saving the snapshot.
	SaveSnapshot(io.Writer, ISnapshotFileCollection, <-chan struct{}) error
	// RecoverFromSnapshot recovers the state of an IStateMachine instance from a
	// previously saved snapshot captured by the SaveSnapshot method. The saved
	// snapshot is provided as an io.Reader backed by an on disk file and
	// a list of immutable files previously recorded into the
	// ISnapshotFileCollection by the SaveSnapshot method.
	//
	// The provided read-only chan struct{} is used to notify the
	// RecoverFromSnapshot() method that the associated Raft node is being closed.
	// On receiving such notification, RecoverFromSnapshot() can choose to
	// abort and return ErrSnapshotStopped immediately. Other than
	// ErrSnapshotStopped, IStateMachine should only return a non-nil error when
	// the system must be immediately halted for non-recoverable error, e.g. disk
	// error preventing you from reading the complete saved snapshot.
	RecoverFromSnapshot(io.Reader, []SnapshotFile, <-chan struct{}) error
	// Close closes the IStateMachine instance.
	//
	// The Close method is not allowed to update the state of the IStateMachine
	// visible to IStateMachine's Lookup method.
	//
	// This allows the application to finalize resources to a state easier to be
	// re-opened and used in the future. It is important to understand that Close
	// is not guaranteed to be always called, e.g. node might crash at any time.
	// IStateMachine should be designed in a way that the safety and integrity of
	// its managed data doesn't rely on whether the Close method is called or not.
	Close() error
}

// CreateStateMachineFunc is a factory function type for creating IStateMachine
// instances.
type CreateStateMachineFunc func(shardID uint64, replicaID uint64) IStateMachine
