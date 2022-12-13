// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
Package raftio contains structs, interfaces and function definitions required
to build custom persistent Raft log storage and transport modules.

Structs, interfaces and functions defined in the raftio package are only
required when building your custom persistent Raft log storage or transport
modules. Skip this package if you plan to use the default built-in LogDB and
transport modules provided by Dragonboat.

Structs, interfaces and functions defined in the raftio package are not
considered as a part of Dragonboat's public APIs. Breaking changes might
happen in the coming minor releases.
*/
package raftio

import (
	"context"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

// MessageHandler is the handler function type for handling received message
// batches. Received message batches should be passed to the message handler to
// be processed.
type MessageHandler func(pb.MessageBatch)

// ChunkHandler is the handler function type for handling received snapshot
// chunks. It adds the new snapshot chunk to the snapshot chunk sink. Chunks
// from the same snapshot are combined into the snapshot image and then
// be passed to dragonboat.
//
// ChunkHandler returns a boolean value indicating whether the snapshot
// connection is still valid for accepting future snapshot chunks.
type ChunkHandler func(pb.Chunk) bool

// IConnection is the interface used by the transport module for sending Raft
// messages. Each IConnection works for a specified target NodeHost instance,
// it is possible for a target to have multiple concurrent IConnection
// instances in use.
type IConnection interface {
	// Close closes the IConnection instance.
	Close()
	// SendMessageBatch sends the specified message batch to the target. It is
	// recommended to deliver the message batch to the target in order to enjoy
	// the best possible performance, but out of order delivery is allowed at the
	// cost of reduced performance.
	SendMessageBatch(batch pb.MessageBatch) error
}

// ISnapshotConnection is the interface used by the transport module for sending
// snapshot chunks. Each ISnapshotConnection works for a specified target
// NodeHost instance.
type ISnapshotConnection interface {
	// Close closes the ISnapshotConnection instance.
	Close()
	// SendChunk sends the snapshot chunk to the target. It is
	// recommended to have the snapshot chunk delivered in order for the best
	// performance, but out of order delivery is allowed at the cost of reduced
	// performance.
	SendChunk(chunk pb.Chunk) error
}

// ITransport is the interface to be implemented by a customized transport
// module. A transport module is responsible for exchanging Raft messages,
// snapshots and other metadata between NodeHost instances.
type ITransport interface {
	// Name returns the type name of the ITransport instance.
	Name() string
	// Start launches the transport module and make it ready to start sending and
	// receiving Raft messages. If necessary, ITransport may take this opportunity
	// to start listening for incoming data.
	Start() error
	// Close closes the transport module.
	Close() error
	// GetConnection returns an IConnection instance used for sending messages
	// to the specified target NodeHost instance.
	GetConnection(ctx context.Context, target string) (IConnection, error)
	// GetSnapshotConnection returns an ISnapshotConnection instance used for
	// sending snapshot chunks to the specified target NodeHost instance.
	GetSnapshotConnection(ctx context.Context,
		target string) (ISnapshotConnection, error)
}
