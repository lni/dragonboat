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
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

// IsNodeHostID returns a boolean value indicating whether the specified value
// is a valid string representation of NodeHostID.
func IsNodeHostID(v string) bool {
	_, err := uuid.Parse(v)
	return err == nil
}

func NewUUID(v string) (*UUID, error) {
	u, err := uuid.Parse(v)
	if err != nil {
		return nil, err
	}
	return &UUID{v: u}, nil
}

func New() *UUID {
	return &UUID{v: uuid.New()}
}

type UUID struct {
	v uuid.UUID
}

var _ pb.Marshaler = (*UUID)(nil)
var _ pb.Unmarshaler = (*UUID)(nil)

func (u UUID) String() string {
	return u.v.String()
}

func (u *UUID) Marshal() ([]byte, error) {
	return u.v.MarshalBinary()
}

func (u *UUID) MarshalTo(data []byte) (int, error) {
	v, err := u.v.MarshalBinary()
	if err != nil {
		return 0, err
	}
	if len(data) < len(v) {
		return 0, errors.New("input slice too short")
	}
	copy(data, v)
	return len(v), nil
}

func (u *UUID) Unmarshal(data []byte) error {
	return u.v.UnmarshalBinary(data)
}
