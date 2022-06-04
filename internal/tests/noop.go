// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// NoOP is a IStateMachine struct used for testing purpose.
type NoOP struct {
	MillisecondToSleep uint64
	NoAlloc            bool
}

// SetSleepTime sets the sleep time of the state machine.
func (n *NoOP) SetSleepTime(v uint64) {
	atomic.StoreUint64(&n.MillisecondToSleep, v)
}

// Lookup locally looks up the data.
func (n *NoOP) Lookup(key interface{}) (interface{}, error) {
	return make([]byte, 1), nil
}

// NALookup locally looks up the data.
func (n *NoOP) NALookup(key []byte) ([]byte, error) {
	return key, nil
}

// Update updates the object.
func (n *NoOP) Update(e sm.Entry) (sm.Result, error) {
	sleep := atomic.LoadUint64(&n.MillisecondToSleep)
	if sleep > 0 {
		time.Sleep(time.Duration(sleep) * time.Millisecond)
	}
	if n.NoAlloc {
		return sm.Result{Value: uint64(len(e.Cmd))}, nil
	}
	v := make([]byte, len(e.Cmd))
	copy(v, e.Cmd)
	return sm.Result{Value: uint64(len(e.Cmd)), Data: v}, nil
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
	data, err := fileutil.ReadAll(r)
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
func (n *NoOP) Close() error { return nil }

// GetHash returns a uint64 value representing the current state of the object.
func (n *NoOP) GetHash() (uint64, error) {
	return 0, nil
}
