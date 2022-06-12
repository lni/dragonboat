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

package client

import (
	"testing"

	"github.com/lni/goutils/random"
)

func TestNoOPSessionHasExpectedSeriesID(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	if cs.SeriesID != NoOPSeriesID {
		t.Errorf("series id unexpected")
	}
	if cs.ShardID != 120 {
		t.Errorf("shard id unexpected")
	}
}

func testNoOPSessionNotAllowedForSessionOps(t *testing.T, f func()) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("not panic")
		}
	}()
	f()
}

func TestNoOPSessionNotAllowedForSessionOps(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	testNoOPSessionNotAllowedForSessionOps(t, cs.PrepareForRegister)
	testNoOPSessionNotAllowedForSessionOps(t, cs.PrepareForUnregister)
	testNoOPSessionNotAllowedForSessionOps(t, cs.PrepareForPropose)
}

func TestProposalCompleted(t *testing.T) {
	cs := NewSession(120, random.LockGuardedRand)
	cs.PrepareForPropose()
	for i := 0; i < 128; i++ {
		cs.ProposalCompleted()
	}
	if cs.SeriesID != SeriesIDFirstProposal+128 {
		t.Errorf("unexpected series id")
	}
	if cs.SeriesID != cs.RespondedTo+1 {
		t.Errorf("unexpected responded to value")
	}
}

func TestInvalidNoOPSessionIsReported(t *testing.T) {
	cs := Session{
		SeriesID: NoOPSeriesID,
		ClientID: NotSessionManagedClientID,
	}
	if cs.ValidForProposal(0) {
		t.Errorf("failed to indentify invalid client session")
	}
}

func TestValidForProposal(t *testing.T) {
	cs := NewSession(120, random.LockGuardedRand)
	cs.PrepareForRegister()
	if cs.ValidForProposal(120) {
		t.Errorf("bad ValidForProposal result")
	}
	cs.PrepareForUnregister()
	if cs.ValidForProposal(120) {
		t.Errorf("bad ValidForProposal result")
	}
	cs.RespondedTo = 200
	cs.SeriesID = 100
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("panic not triggered")
		}
	}()
	cs.ValidForProposal(120)
}

func TestValidForSessionOp(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	if cs.ValidForSessionOp(120) {
		t.Errorf("vad ValidForSessionOp result")
	}
	cs = NewSession(120, random.LockGuardedRand)
	cs.ClientID = NotSessionManagedClientID
	if cs.ValidForSessionOp(120) {
		t.Errorf("vad ValidForSessionOp result")
	}
	cs = NewSession(120, random.LockGuardedRand)
	cs.PrepareForPropose()
	if cs.ValidForSessionOp(120) {
		t.Errorf("vad ValidForSessionOp result")
	}
	cs.PrepareForUnregister()
	if !cs.ValidForSessionOp(120) {
		t.Errorf("vad ValidForSessionOp result")
	}
	cs.PrepareForRegister()
	if !cs.ValidForSessionOp(120) {
		t.Errorf("vad ValidForSessionOp result")
	}
}

func TestIsNoOPSession(t *testing.T) {
	s := NewNoOPSession(1, random.LockGuardedRand)
	if !s.IsNoOPSession() {
		t.Errorf("not considered as a noop session")
	}
	s.ClientID++
	if !s.IsNoOPSession() {
		t.Errorf("not considered as a noop session")
	}
	s.SeriesID++
	if s.IsNoOPSession() {
		t.Errorf("still considered as a noop session")
	}
}
