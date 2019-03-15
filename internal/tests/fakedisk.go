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

package tests

import (
	"encoding/binary"
	"io"

	sm "github.com/lni/dragonboat/statemachine"
)

type FakeDiskSM struct {
	initialApplied uint64
	count          uint64
}

func NewFakeDiskSM(initialApplied uint64) *FakeDiskSM {
	return &FakeDiskSM{initialApplied: initialApplied}
}

func (f *FakeDiskSM) Open() (uint64, error) {
	return f.initialApplied, nil
}

func (f *FakeDiskSM) Update(ents []sm.Entry) []sm.Entry {
	for _, e := range ents {
		if e.Index <= f.initialApplied {
			panic("already applied index received again")
		} else {
			f.count = f.count + 1
			e.Result = f.count
		}
	}
	return ents
}

func (f *FakeDiskSM) Lookup(query []byte) ([]byte, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, f.count)
	return result, nil
}

func (f *FakeDiskSM) PrepareSnapshot() (interface{}, error) {
	return struct{}{}, nil
}

func (f *FakeDiskSM) CreateSnapshot(io.Writer, <-chan struct{}) error {
	return nil
}

func (f *FakeDiskSM) RecoverFromSnapshot(io.Reader, <-chan struct{}) error {
	return nil
}

func (f *FakeDiskSM) Close() {
}

func (f *FakeDiskSM) GetHash() uint64 {
	return 0
}
