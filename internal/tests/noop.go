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
	"encoding/json"
	"io"
	"io/ioutil"
	"time"

	sm "github.com/lni/dragonboat/statemachine"
)

// NoOP is a IStateMachine struct used for testing purpose.
type NoOP struct {
	MillisecondToSleep uint64
}

// Lookup locally looks up the data.
func (n *NoOP) Lookup(key []byte) []byte {
	return make([]byte, 1)
}

// Update updates the object.
func (n *NoOP) Update(data []byte) sm.Result {
	if n.MillisecondToSleep > 0 {
		time.Sleep(time.Duration(n.MillisecondToSleep) * time.Millisecond)
	}
	return sm.Result{Value: uint64(len(data))}
}

// SaveSnapshot saves the state of the object to the provided io.Writer object.
func (n *NoOP) SaveSnapshot(w io.Writer,
	fileCollection sm.ISnapshotFileCollection,
	done <-chan struct{}) error {
	data, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// RecoverFromSnapshot recovers the object from the snapshot specified by the
// io.Reader object.
func (n *NoOP) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{}) error {
	var sn NoOP
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &sn)
	if err != nil {
		panic("failed to unmarshal snapshot")
	}

	return nil
}

// Close closes the NoOP IStateMachine.
func (n *NoOP) Close() {}

// GetHash returns a uint64 value representing the current state of the object.
func (n *NoOP) GetHash() uint64 {
	// the hash value is always 0, so it is of course always consistent
	return 0
}
