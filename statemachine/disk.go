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

package statemachine

import (
	"io"
)

type IAllDiskStateMachine interface {
	Open() (uint64, error)
	Update([]Entry) []Entry
	Lookup([]byte) ([]byte, error)
	PrepareSnapshot() (interface{}, error)
	CreateSnapshot(io.Writer, <-chan struct{}) error
	RecoverFromSnapshot(io.Reader, <-chan struct{}) error
	Close()
	GetHash() uint64
}
