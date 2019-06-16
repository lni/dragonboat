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

package syncutil

// Lock is a channel based lock with TryLock() supported, it is designed for
// very low contention scenario
type Lock struct {
	ch chan struct{}
}

// NewLock creates a new Lock instance.
func NewLock() *Lock {
	return &Lock{ch: make(chan struct{}, 1)}
}

// Lock blocks the calling thread until the lock is acquired.
func (l *Lock) Lock() {
	l.ch <- struct{}{}
}

// Unlock unlocks the lock.
func (l *Lock) Unlock() {
	select {
	case <-l.ch:
	default:
		panic("unlock called when not locked")
	}
}

// TryLock tries to acquire the lock and returns a boolean value to indicate
// whether the lock is successfully acquired.
func (l *Lock) TryLock() bool {
	select {
	case l.ch <- struct{}{}:
		return true
	default:
		return false
	}
}
