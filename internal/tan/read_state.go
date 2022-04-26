// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
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

package tan

import "sync/atomic"

// readState encapsulates the state needed for reading (the current version and
// list of memtables). Loading the readState is done without grabbing
// DB.mu. Instead, a separate DB.readState.RWMutex is used for
// synchronization. This mutex solely covers the current readState object which
// means it is rarely or ever contended.
//
// Note that various fancy lock-free mechanisms can be imagined for loading the
// readState, but benchmarking showed the ones considered to purely be
// pessimizations. The RWMutex version is a single atomic increment for the
// RLock and an atomic decrement for the RUnlock. It is difficult to do better
// than that without something like thread-local storage which isn't available
// in Go.
type readState struct {
	db         *db
	refcnt     int32
	version    *version
	nodeStates *nodeStates
}

// ref adds a reference to the readState.
func (s *readState) ref() {
	atomic.AddInt32(&s.refcnt, 1)
}

// unref removes a reference to the readState. If this was the last reference,
// the reference the readState holds on the version is released. Requires DB.mu
// is NOT held as version.unref() will acquire it. See unrefLocked() if DB.mu
// is held by the caller.
func (s *readState) unref() {
	if atomic.AddInt32(&s.refcnt, -1) != 0 {
		return
	}
	s.version.unref()

	// TODO:
	// re-enable the obsolete file deletion
	//
	// The last reference to the readState was released. Check to see if there
	// are new obsolete tables to delete.
	// s.db.maybeScheduleObsoleteTableDeletion()
}

// unrefLocked removes a reference to the readState. If this was the last
// reference, the reference the readState holds on the version is
// released. Requires DB.mu is held as version.unrefLocked() requires it. See
// unref() if DB.mu is NOT held by the caller.
func (s *readState) unrefLocked() {
	if atomic.AddInt32(&s.refcnt, -1) != 0 {
		return
	}
	s.version.unrefLocked()

	// NB: Unlike readState.unref(), we don't attempt to cleanup newly obsolete
	// tables as unrefLocked() is only called during DB shutdown to release the
	// current readState.
}

// loadReadState returns the current readState. The returned readState must be
// unreferenced when the caller is finished with it.
func (d *db) loadReadState() *readState {
	d.readState.RLock()
	state := d.readState.val
	state.ref()
	d.readState.RUnlock()
	return state
}

// updateReadStateLocked creates a new readState from the current version and
// list of memtables. Requires DB.mu is held. If checker is not nil, it is
// called after installing the new readState
func (d *db) updateReadStateLocked(checker func(*db) error) {
	s := &readState{
		db:         d,
		refcnt:     1,
		version:    d.mu.versions.currentVersion(),
		nodeStates: d.mu.nodeStates,
	}
	s.version.ref()

	d.readState.Lock()
	old := d.readState.val
	d.readState.val = s
	d.readState.Unlock()
	if checker != nil {
		if err := checker(d); err != nil {
			panic(err)
		}
	}
	if old != nil {
		old.unrefLocked()
	}
}
