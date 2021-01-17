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

package raft

import (
	"reflect"
	"testing"
)

func TestSnapshotAckTick(t *testing.T) {
	a := snapshotAck{ctick: 10}
	for i := 0; i < 10; i++ {
		r := a.tick()
		if (i == 9) != r {
			t.Errorf("unexpected tick result, i %d, tick %d, r %t", i, a.ctick, r)
		}
	}
}

func TestRemoteClearSnapshotAck(t *testing.T) {
	r := remote{delayed: snapshotAck{ctick: 10, rejected: true}}
	r.clearSnapshotAck()
	if r.delayed.ctick != 0 || r.delayed.rejected {
		t.Errorf("snapshot ack not cleared")
	}
}

func TestRemoteSetSnapshotAck(t *testing.T) {
	r := remote{
		state:   remoteSnapshot,
		delayed: snapshotAck{ctick: 10, rejected: true},
	}
	r.setSnapshotAck(20, false)
	if r.delayed.ctick != 20 || r.delayed.rejected {
		t.Errorf("setSnapshotAck failed to set values")
	}
}

func TestSetSnapshotAckWhenNotInSnapshotStateIsNotAllowed(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("failed to trigger panic")
		}
	}()
	r := remote{}
	r.setSnapshotAck(20, false)
}

func TestRemoteString(t *testing.T) {
	for _, tt := range []remoteStateType{remoteRetry,
		remoteReplicate, remoteSnapshot} {
		if tt.String() != remoteNames[uint64(tt)] {
			t.Errorf("unexpected string name")
		}
	}
}

func TestRemoteReset(t *testing.T) {
	r := &remote{
		state:         remoteSnapshot,
		snapshotIndex: 100,
		match:         100,
		next:          101,
	}
	exp := &remote{
		state:         remoteSnapshot,
		snapshotIndex: 0,
		match:         100,
		next:          101,
	}
	r.reset()
	if !reflect.DeepEqual(&r, &exp) {
		t.Errorf("unexpected state %+v", r)
	}
}

func TestRemoteActiveFlag(t *testing.T) {
	r := remote{}
	if r.isActive() {
		t.Errorf("unexpected active state1")
	}
	r.setActive()
	if !r.isActive() {
		t.Errorf("unexpected active state2")
	}
	r.setNotActive()
	if r.isActive() {
		t.Errorf("unexpected active state3")
	}
}

func TestRemoteBecomeRetry(t *testing.T) {
	r := remote{state: remoteReplicate}
	r.becomeRetry()
	if r.next != r.match+1 {
		t.Errorf("unexpected next")
	}
	if r.state != remoteRetry {
		t.Errorf("unexpected state %+v", r)
	}
}

func TestRemoteBecomeRetryFromSnapshot(t *testing.T) {
	r := remote{state: remoteSnapshot, snapshotIndex: 100}
	r.becomeRetry()
	if r.next != 101 {
		t.Errorf("unexpected next")
	}
	if r.state != remoteRetry {
		t.Errorf("unexpected state %+v", r)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("snapshotIndex not reset")
	}
	r = remote{state: remoteSnapshot, match: 10, snapshotIndex: 0}
	r.becomeRetry()
	if r.next != 11 {
		t.Errorf("unexpected next")
	}
	if r.state != remoteRetry {
		t.Errorf("unexpected state %+v", r)
	}
	if r.snapshotIndex != 0 {
		t.Errorf("snapshotIndex not reset")
	}
}

func testRemoteBecomeSnapshot(t *testing.T, st remoteStateType) {
	r := &remote{state: st, match: 10, next: 11}
	r.becomeSnapshot(12)
	if r.state != remoteSnapshot {
		t.Errorf("unexpected state %+v", r)
	}
	if r.match != 10 || r.snapshotIndex != 12 {
		t.Errorf("unexpected state %+v", r)
	}
}

func TestRemoteBecomeSnapshot(t *testing.T) {
	testRemoteBecomeSnapshot(t, remoteReplicate)
	testRemoteBecomeSnapshot(t, remoteRetry)
	testRemoteBecomeSnapshot(t, remoteSnapshot)
}

func TestRemoteBecomeReplication(t *testing.T) {
	r := &remote{state: remoteRetry, match: 10, next: 11}
	r.becomeReplicate()
	if r.state != remoteReplicate {
		t.Errorf("unexpected state %+v", r)
	}
	if r.match != 10 || r.next != 11 {
		t.Errorf("unexpected match/next %+v", r)
	}
}

func TestRemoteProgress(t *testing.T) {
	r := &remote{state: remoteReplicate, match: 10, next: 11}
	r.progress(12)
	if r.next != 13 {
		t.Errorf("unexpected next: %d", r.next)
	}
	if r.match != 10 {
		t.Errorf("match unexpectedly moved")
	}
	r = &remote{state: remoteRetry, match: 10, next: 11}
	if r.isPaused() {
		t.Errorf("unexpectedly in paused state")
	}
	r.progress(12)
	if !r.isPaused() {
		t.Errorf("not paused")
	}
	if r.next != 11 || r.match != 10 {
		t.Errorf("unexpected state %+v", r)
	}
}

func TestRemoteProgressInSnapshotStateCausePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r := &remote{state: remoteSnapshot, match: 10, next: 11}
	r.progress(12)
}

func TestRemotePanicWhenInInvalidState(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	r := &remote{state: remoteStateType(100)}
	r.isPaused()
}

func TestRemoteIsPaused(t *testing.T) {
	tests := []struct {
		st  remoteStateType
		exp bool
	}{
		{remoteRetry, false},
		{remoteWait, true},
		{remoteReplicate, false},
		{remoteSnapshot, true},
	}

	for idx, tt := range tests {
		r := &remote{state: tt.st}
		if r.isPaused() != tt.exp {
			t.Errorf("%d, paused is true, st %s isPaused: %t", idx, tt.st, r.isPaused())
		}
	}
}

func TestRemoteRespondedTo(t *testing.T) {
	tests := []struct {
		st            remoteStateType
		match         uint64
		next          uint64
		snapshotIndex uint64
		expSt         remoteStateType
		expNext       uint64
	}{
		{remoteRetry, 10, 12, 0, remoteReplicate, 11},
		{remoteReplicate, 10, 12, 0, remoteReplicate, 12},
		{remoteSnapshot, 10, 12, 8, remoteRetry, 11},
		{remoteSnapshot, 10, 11, 12, remoteSnapshot, 11},
	}
	for idx, tt := range tests {
		r := &remote{
			state:         tt.st,
			match:         tt.match,
			next:          tt.next,
			snapshotIndex: tt.snapshotIndex,
		}
		r.respondedTo()
		if r.state != tt.expSt {
			t.Errorf("%d, state %s, exp %s", idx, r.state, tt.expSt)
		}
		if r.next != tt.expNext {
			t.Errorf("%d, next %d, exp %d", idx, r.next, tt.expNext)
		}
	}
}

func TestRemoteTryUpdate(t *testing.T) {
	match := uint64(10)
	next := uint64(20)
	tests := []struct {
		index      uint64
		paused     bool
		expMatch   uint64
		expNext    uint64
		expPaused  bool
		expUpdated bool
	}{
		{next, false, next, next + 1, false, true},
		{next, true, next, next + 1, false, true},
		{next - 2, false, next - 2, next, false, true},
		{next - 2, true, next - 2, next, false, true},
		{next - 1, false, next - 1, next, false, true},
		{next - 1, true, next - 1, next, false, true},
		{match - 1, false, match, next, false, false},
		{match - 1, true, match, next, true, false},
	}
	for idx, tt := range tests {
		r := &remote{
			match: match,
			next:  next,
		}
		if tt.paused {
			r.retryToWait()
		}
		updated := r.tryUpdate(tt.index)
		if updated != tt.expUpdated {
			t.Errorf("%d, updated %t, want %t", idx, updated, tt.expUpdated)
		}
		if r.next != tt.expNext || r.match != tt.expMatch {
			t.Errorf("%d, unexpected state %+v", idx, r)
		}
		if tt.expPaused {
			if r.state != remoteWait {
				t.Errorf("st %s, want remoteWait", r.state)
			}
		}
	}
}

func TestRemoteDecreaseToInReplicateState(t *testing.T) {
	tests := []struct {
		match     uint64
		next      uint64
		rejected  uint64
		decreased bool
		expNext   uint64
	}{
		{10, 15, 9, false, 15},
		{10, 15, 10, false, 15},
		{10, 15, 12, true, 11},
	}
	for idx, tt := range tests {
		r := &remote{match: tt.match, next: tt.next, state: remoteReplicate}
		decreased := r.decreaseTo(tt.rejected, 100)
		if decreased != tt.decreased {
			t.Errorf("%d, unexpected return value", idx)
		}
		if r.next != tt.expNext {
			t.Errorf("%d, next %d, exp %d", idx, r.next, tt.expNext)
		}
	}
}

func TestRemoteDecreaseToNotReplicateState(t *testing.T) {
	tests := []struct {
		match     uint64
		next      uint64
		rejected  uint64
		last      uint64
		decreased bool
		expNext   uint64
	}{
		{10, 15, 20, 100, false, 15},
		{10, 15, 14, 100, true, 14},
		{10, 15, 14, 10, true, 11},
	}
	for idx, tt := range tests {
		for _, st := range []remoteStateType{remoteRetry, remoteSnapshot} {
			r := &remote{match: tt.match, next: tt.next, state: st}
			r.retryToWait()
			decreased := r.decreaseTo(tt.rejected, tt.last)
			if decreased != tt.decreased {
				t.Errorf("%d, unexpected return value", idx)
			}
			if r.next != tt.expNext {
				t.Errorf("%d, next %d, exp %d", idx, r.next, tt.expNext)
			}
			if tt.decreased {
				if r.state == remoteWait {
					t.Errorf("not resumed")
				}
			}
		}
	}
}

func TestRemoteTryUpdateCauseResume(t *testing.T) {
	r := &remote{next: 5}
	r.retryToWait()
	r.decreaseTo(4, 4)
	if r.state == remoteWait {
		t.Errorf("still paused")
	}
	r.retryToWait()
	r.tryUpdate(5)
	if r.state == remoteWait {
		t.Errorf("still paused")
	}
}
