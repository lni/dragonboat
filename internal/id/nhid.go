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

package id

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/random"
)

var (
	nhidRe = regexp.MustCompile("^nhid-([0-9]*)$")
	// ErrInvalidNodeHostID indicates that the NodeHost ID value provided is
	// invalid
	ErrInvalidNodeHostID = errors.New("invalid NodeHost ID value")
)

// NodeHostID is the NodeHost ID type used for identifying a NodeHost instance.
type NodeHostID struct {
	id uint64
}

// IsNodeHostID returns a boolean value indicating whether the specified value
// is a valid string representation of NodeHostID.
func IsNodeHostID(v string) bool {
	if _, err := ParseNodeHostID(v); err != nil {
		return false
	}
	return true
}

// ParseNodeHostID creates a NodeHostID instance based on the specified string
// representation of the NodeHost ID.
func ParseNodeHostID(v string) (*NodeHostID, error) {
	match := nhidRe.FindStringSubmatch(v)
	if len(match) != 2 {
		return nil, ErrInvalidNodeHostID
	}
	id, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return nil, ErrInvalidNodeHostID
	}
	if id == 0 {
		return nil, ErrInvalidNodeHostID
	}
	return &NodeHostID{id: id}, nil
}

// NewNodeHostID creates a NodeHostID instance based on the specified id value.
func NewNodeHostID(id uint64) (*NodeHostID, error) {
	if id == 0 {
		return nil, ErrInvalidNodeHostID
	}
	return &NodeHostID{id: id}, nil
}

// NewRandomNodeHostID creates a NodeHostID with randomly assigned ID value.
func NewRandomNodeHostID() *NodeHostID {
	v := uint64(0)
	for v == 0 {
		v = random.LockGuardedRand.Uint64()
	}
	return &NodeHostID{id: v}
}

// Value returns the uint64 representation of the NodeHostID.
func (n *NodeHostID) Value() uint64 {
	return n.id
}

// String returns a string representation of the NodeHostID instance.
func (n *NodeHostID) String() string {
	return fmt.Sprintf("nhid-%d", n.id)
}

// Marshal marshals the NodeHostID instances.
func (n *NodeHostID) Marshal() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, n.id)
	return buf, nil
}

// MarshalTo is not implemented
func (n *NodeHostID) MarshalTo(result []byte) (int, error) {
	panic("not implemented")
}

// Unmarshal unmarshals the NodeHostID instance.
func (n *NodeHostID) Unmarshal(data []byte) error {
	if len(data) != 8 {
		panic("failed to get NodeHost id")
	}
	n.id = binary.LittleEndian.Uint64(data)
	return nil
}
