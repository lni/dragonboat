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
//
//
// remote.go is used for tracking the state of remote raft node. it is derived
// from etcd raft's flow control code.
//
// Copyright 2015 The etcd Authors
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

package raft

import (
	"fmt"
)

type snapshotAck struct {
	ctick    uint64
	rejected bool
}

func (a *snapshotAck) tick() bool {
	if a.ctick > 0 {
		a.ctick--
		return a.ctick == 0
	}
	return false
}

type remoteStateType uint64

const (
	remoteRetry remoteStateType = iota
	remoteWait
	remoteReplicate
	remoteSnapshot
)

var remoteNames = []string{
	"Retry",
	"Wait",
	"Replicate",
	"Snapshot",
}

func (r remoteStateType) String() string {
	return remoteNames[uint64(r)]
}

type remote struct {
	// called matchIndex/nextIndex in the etcd raft paper
	match         uint64
	next          uint64
	snapshotIndex uint64
	state         remoteStateType
	active        bool
	delayed       snapshotAck
}

func (r *remote) String() string {
	return fmt.Sprintf("match:%d,next:%d,state:%s,si:%d",
		r.match, r.next, r.state, r.snapshotIndex)
}

func (r *remote) clearSnapshotAck() {
	r.delayed = snapshotAck{}
}

func (r *remote) setSnapshotAck(tick uint64, rejected bool) {
	if r.state == remoteSnapshot {
		r.delayed.ctick, r.delayed.rejected = tick, rejected
	} else {
		panic("setting snapshot ack when not in snapshot state")
	}
}

func (r *remote) reset() {
	r.snapshotIndex = 0
}

func (r *remote) becomeRetry() {
	if r.state == remoteSnapshot {
		r.next = max(r.match+1, r.snapshotIndex+1)
	} else {
		r.next = r.match + 1
	}
	r.reset()
	r.state = remoteRetry
}

func (r *remote) retryToWait() {
	if r.state == remoteRetry {
		r.state = remoteWait
	}
}

func (r *remote) waitToRetry() {
	if r.state == remoteWait {
		r.state = remoteRetry
	}
}

func (r *remote) becomeWait() {
	r.clearSnapshotAck()
	r.becomeRetry()
	r.retryToWait()
}

func (r *remote) becomeReplicate() {
	r.next = r.match + 1
	r.reset()
	r.state = remoteReplicate
}

func (r *remote) becomeSnapshot(index uint64) {
	r.reset()
	r.snapshotIndex = index
	r.state = remoteSnapshot
}

func (r *remote) clearPendingSnapshot() {
	r.snapshotIndex = 0
}

func (r *remote) tryUpdate(index uint64) bool {
	if r.next < index+1 {
		r.next = index + 1
	}
	if r.match < index {
		r.waitToRetry()
		r.match = index
		return true
	}
	return false
}

func (r *remote) progress(lastIndex uint64) {
	if r.state == remoteReplicate {
		r.next = lastIndex + 1
	} else if r.state == remoteRetry {
		r.retryToWait()
	} else {
		panic("unexpected remote state")
	}
}

func (r *remote) respondedTo() {
	if r.state == remoteRetry {
		r.becomeReplicate()
	} else if r.state == remoteSnapshot {
		if r.match >= r.snapshotIndex {
			r.becomeRetry()
		}
	}
}

func (r *remote) decreaseTo(rejected uint64, last uint64) bool {
	if r.state == remoteReplicate {
		if rejected <= r.match {
			// stale msg
			return false
		}
		r.next = r.match + 1
		return true
	}
	if r.next-1 != rejected {
		// stale
		return false
	}
	r.waitToRetry()
	r.next = max(1, min(rejected, last+1))
	return true
}

func (r *remote) isPaused() bool {
	switch r.state {
	case remoteRetry:
		return false
	case remoteWait:
		return true
	case remoteReplicate:
		return false
	case remoteSnapshot:
		return true
	default:
		panic("unexpected remote state")
	}
}

func (r *remote) isActive() bool {
	return r.active
}

func (r *remote) setActive() {
	r.active = true
}

func (r *remote) setNotActive() {
	r.active = false
}
