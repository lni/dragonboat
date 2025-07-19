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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoOPSessionHasExpectedSeriesID(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	assert.Equal(t, NoOPSeriesID, cs.SeriesID, "series id unexpected")
	assert.Equal(t, uint64(120), cs.ShardID, "shard id unexpected")
}

func TestNoOPSessionNotAllowedForSessionOps(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	require.Panics(t, func() { cs.PrepareForRegister() })
	require.Panics(t, func() { cs.PrepareForUnregister() })
	require.Panics(t, func() { cs.PrepareForPropose() })
}

func TestProposalCompleted(t *testing.T) {
	cs := NewSession(120, random.LockGuardedRand)
	cs.PrepareForPropose()
	for i := 0; i < 128; i++ {
		cs.ProposalCompleted()
	}
	expectedSeriesID := SeriesIDFirstProposal + 128
	assert.Equal(t, expectedSeriesID, cs.SeriesID, "unexpected series id")
	assert.Equal(t, cs.SeriesID, cs.RespondedTo+1,
		"unexpected responded to value")
}

func TestInvalidNoOPSessionIsReported(t *testing.T) {
	cs := Session{
		SeriesID: NoOPSeriesID,
		ClientID: NotSessionManagedClientID,
	}
	assert.False(t, cs.ValidForProposal(0),
		"failed to identify invalid client session")
}

func TestValidForProposal(t *testing.T) {
	cs := NewSession(120, random.LockGuardedRand)
	cs.PrepareForRegister()
	assert.False(t, cs.ValidForProposal(120), "bad ValidForProposal result")
	cs.PrepareForUnregister()
	assert.False(t, cs.ValidForProposal(120), "bad ValidForProposal result")
	cs.RespondedTo = 200
	cs.SeriesID = 100
	require.Panics(t, func() { cs.ValidForProposal(120) })
}

func TestValidForSessionOp(t *testing.T) {
	cs := NewNoOPSession(120, random.LockGuardedRand)
	assert.False(t, cs.ValidForSessionOp(120), "bad ValidForSessionOp result")

	cs = NewSession(120, random.LockGuardedRand)
	cs.ClientID = NotSessionManagedClientID
	assert.False(t, cs.ValidForSessionOp(120), "bad ValidForSessionOp result")

	cs = NewSession(120, random.LockGuardedRand)
	cs.PrepareForPropose()
	assert.False(t, cs.ValidForSessionOp(120), "bad ValidForSessionOp result")

	cs.PrepareForUnregister()
	assert.True(t, cs.ValidForSessionOp(120), "bad ValidForSessionOp result")

	cs.PrepareForRegister()
	assert.True(t, cs.ValidForSessionOp(120), "bad ValidForSessionOp result")
}

func TestIsNoOPSession(t *testing.T) {
	s := NewNoOPSession(1, random.LockGuardedRand)
	assert.True(t, s.IsNoOPSession(), "not considered as a noop session")

	s.ClientID++
	assert.True(t, s.IsNoOPSession(), "not considered as a noop session")

	s.SeriesID++
	assert.False(t, s.IsNoOPSession(), "still considered as a noop session")
}
