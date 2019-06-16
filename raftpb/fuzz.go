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

// +build gofuzz

package raftpb

import (
	"fmt"
	"reflect"
)

func Fuzz(data []byte) int {
	e := Entry{}
	if err := e.Unmarshal(data); err != nil {
		return 0
	}
	m, err := e.Marshal()
	if err != nil {
		panic(err)
	}
	e2 := Entry{}
	if err := e2.Unmarshal(m); err != nil {
		panic(err)
	}
	if len(e.Data) == 0 {
		e.Data = nil
	}
	if len(e2.Data) == 0 {
		e.Data = nil
	}
	if !reflect.DeepEqual(&e2, &e) {
		msg := fmt.Sprintf("\ne1: %v\ne2: %v\n", e, e2)
		panic(msg)
	}

	return 1
}
