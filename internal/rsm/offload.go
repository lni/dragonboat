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

package rsm

import (
	"sync/atomic"
)

// OffloadedStatus is used for tracking whether the managed data store has been
// offloaded from various system components.
type OffloadedStatus struct {
	DestroyedC  chan struct{}
	shardID     uint64
	replicaID   uint64
	loadedCount uint64
	destroyed   bool
}

// Destroyed returns a boolean value indicating whether the belonging object
// has been destroyed.
func (o *OffloadedStatus) Destroyed() bool {
	select {
	case <-o.DestroyedC:
		return true
	default:
		return false
	}
}

// SetDestroyed set the destroyed flag to be true
func (o *OffloadedStatus) SetDestroyed() {
	o.destroyed = true
	close(o.DestroyedC)
}

// SetLoaded marks the managed data store as loaded by a user component.
func (o *OffloadedStatus) SetLoaded() {
	atomic.AddUint64(&o.loadedCount, 1)
}

// SetOffloaded marks the managed data store as offloaded from a user
// component.
func (o *OffloadedStatus) SetOffloaded() uint64 {
	return atomic.AddUint64(&o.loadedCount, ^uint64(0))
}
