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
Package statemachine contains the definition of the IStateMachine interface
required to be implemented by dragonboat based applications.
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
	// byte slice after the return of the Update() method.
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
