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
Package statemachine contains the definitions of the IStateMachine and
IOnDiskStateMachine interfaces to be implemented by application state machine
types.

IStateMachine or IOnDiskStateMachine are the interfaces used by Dragonboat to
interact with applications. Each IStateMachine or IOnDiskStateMachine instance
is associated with a single Raft node replica from a certain Raft group. All
update requests, known as proposals, are arranged by the Raft protocol into a
Raft Log, once a proposal is accepted by the majority of member Raft nodes, it
is considered as committed. Committed proposals are passed to the Update method
of the IStateMachine or IOnDiskStateMachine types to be applied into application
state machines. Read only queries will eventually hit the Lookup method to query
the state of the state machine. The PrepareSnapshot, SaveSnapshot and
RecoverFromSnapshot methods are for generating and restoring from snapshots,
snapshots are the mechanism to apply the entire state of a state machine without
applying an arbitrarily large number of Raft Log entries.

Dragonboat users should first determine whether the application state machine
should implement the IStateMachine or IOnDiskStateMachine interface -

For in memory state machines which in general have low read/write latencies and
relatively small amount of managed data, IStateMachine is usually employed. It
is the default type of state machine described by the Raft thesis. The major
drawback is that each IStateMachine is internally guarded by a sync.RWMutex so
read/write accesses are not allowed to be concurrently invoked on IStateMachine.
Multiple read requests can be concurrently invoked on the same IStateMachine
instance. As the state is in-memory, it needs to be reconstructed from saved
Snapshots and Raft Logs after each reboot. The state is usually perioidically
snapshotted to disks. CPU, disk bandwidth and disk space overheads are thus
introduced for generating and storing such periodic snapshots.

For IOnDiskStateMachine based application state machine, the major difference is
that the state machine state is persisted on disk so it can surivive reboot
without reconstruction from saved snapshots and Raft Logs. As the state is
mostly persisted on disk by design, there is no need to periodically save the
entire state to disk again as full snapshots. A full snapshot is only generated
and streamed to a remote node from the leader on demand when the remote node is
significantly behind the progress of its leader. Read and Write accesses can be
concurrently invoked on IOnDiskStateMachine types. This allows the Lookup method
to be concurrently invoked when the state machine is being updated. The Update
method can also be completed when SaveSnapshot is being executed. It is up to
the user implementation to correctly and safely maintain its internal data
structures during such concurrent accesses. IOnDiskStateMachine is basically the
on disk state machine type described in the section 5.2 of the Raft thesis.

When you are not sure whether to IStateMachine or IOnDiskStateMachine in your
project, the rule of thumb is to implement IStateMachine first and upgrade it
to an IOnDiskStateMachine later when necessary.

IConcurrentStateMachine is another supported state machine interface, it aims to
represent in memory state machine with concurrent read and write capabilities.
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
// machine when most of the state machine data is stored in memory. It
// is the most commonly used state machine type in Dragonboat, it also matches
// the default state machine type described in the Raft thesis.
//
// Dragonboat uses a sync.RWMutex as its reader/writer mutual exclusion lock to
// guard the IStateMachine instance when invoking IStateMachine's member methods.
// The Update, RecoverFromSnapshot and Close methods are invoked when the write
// lock is acquired, other methods are invoked when the shared read lock is
// acquired.
//
// As the state is mostly in memory, snapshots are usually periodically captured
// to save its state to disk. CPU, disk bandwidth and disk space overheads are
// thus introduced by such periodical snapshots. After each reboot,
// IStateMachine state will be reconstructed from an empty state based on the
// latest snapshot and its saved Raft Log.
//
// When created, an IStateMachine instance should always start from an empty
// state. Saved snapshots and Raft Logs will be used to help a rebooted
// IStateMachine instance to catch up to its previous state.
type IStateMachine interface {
	// Update updates the IStateMachine instance. The input data slice is the
	// proposed command from client that has been committed by the Raft cluster.
	// It is up to the IStateMachine implementation to interpret this input byte
	// slice and update the IStateMachine instance accordingly.
	//
	// The Update method must be deterministic, meaning given the same initial
	// state of IStateMachine and the same input, it should reach to the same
	// updated state and outputs the same returned value. The input byte slice
	// should be the only input to this method. Reading from system clock, random
	// number generator or other similar data sources will violate the
	// deterministic requirement of the Update method.
	//
	// The IStateMachine implementation should not keep a reference to the input
	// byte slice after return.
	//
	// Update returns a Result value used to indicate the result of the update
	// operation. An error is returned when there is unrecoverable error when
	// updating the state machine.
	Update([]byte) (Result, error)
	// Lookup queries the state of the IStateMachine instance and returns the
	// query result as a byte slice. The input byte slice specifies what to query,
	// it is up to the IStateMachine implementation to interpret the input byte
	// slice. The returned byte slice contains the query result provided by the
	// IStateMachine implementation.
	//
	// The IStateMachine implementation should not keep a reference of the input
	// byte slice after return.
	//
	// The Lookup method is a read only method, it should never change the state
	// of IStateMachine. An error is returned when there is unrecoverable error
	// when querying the state machine.
	//
	// When an error is returned by the Lookup method, the error will be passed
	// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
	// handled.
	Lookup(interface{}) (interface{}, error)
	// SaveSnapshot saves the current state of the IStateMachine instance to the
	// provided io.Writer backed by a file on disk and the provided
	// ISnapshotFileCollection instance. This is a read only operation on the
	// IStateMachine instance.
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
	// SaveSnapshot returns the encountered error when generating the snapshot.
	// Other than the above mentioned ErrSnapshotStopped error, the IStateMachine
	// implementation should only return a non-nil error when the system need to
	// be immediately halted for critical errors, e.g. disk error preventing you
	// from saving the snapshot.
	SaveSnapshot(io.Writer, ISnapshotFileCollection, <-chan struct{}) error
	// RecoverFromSnapshot recovers the state of the IStateMachine object from a
	// previously saved snapshot captured by the SaveSnapshot method. The saved
	// snapshot is provided as an io.Reader backed by a file on disk and
	// a list of files previously recorded into the ISnapshotFileCollection in
	// SaveSnapshot.
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
	// re-opened and used in the future. It is important to understand that Close
	// is not guaranteed to be always called, e.g. node might crash at any time.
	// IStateMachine should be designed in a way that the safety and integrity of
	// its managed data doesn't rely on whether the Close method is called or not.
	Close() error
}
