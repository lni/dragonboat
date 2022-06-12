// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"github.com/cockroachdb/errors"
)

var (
	// ErrNotImplemented indicates that the requested optional feature is not
	// implemented by the state machine.
	ErrNotImplemented = errors.New("requested feature not implemented")
)

// IHash is an optional interface to be implemented by a user state machine type
// when the ability to generate state machine hash is required.
type IHash interface {
	// GetHash returns a uint64 value used to represent the current state of the
	// state machine. The hash should be generated in a deterministic manner
	// which means nodes from the same Raft shard are suppose to return the
	// same hash result when they have the same Raft Log entries applied.
	//
	// GetHash is a read-only operation.
	GetHash() (uint64, error)
}

// IExtended is an optional interface to be implemented by a user state machine
// type, most of its member methods are for performance optimization purposes.
type IExtended interface {
	// NALookup is similar to user state machine's Lookup method, it tries to
	// minimize extra heap allocation by taking a byte slice as the input and the
	// returned query result is also provided as a byte slice. The input byte
	// slice specifies what to query, it is up to the implementation to interpret
	// the input byte slice.
	//
	// Lookup method is a read-only method, it should never change state machine's
	// state.
	NALookup([]byte) ([]byte, error)
}
