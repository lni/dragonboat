// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package dragonboat

import (
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/assert"
)

func getTestQuiesce() quiesceState {
	return quiesceState{
		electionTick: 10,
		enabled:      true,
	}
}

func TestIncreaseTickCanEnterQuiesce(t *testing.T) {
	q := getTestQuiesce()
	threshold := q.threshold()
	tests := []struct {
		tick     uint64
		quiesced bool
	}{
		{threshold / 2, false},
		{threshold, false},
		{threshold + 1, true},
	}
	for i, tt := range tests {
		q := getTestQuiesce()
		for k := uint64(0); k < tt.tick; k++ {
			q.tick()
		}
		assert.Equal(t, tt.quiesced, q.quiesced(),
			"test case %d", i)
	}
}

func TestQuiesceCanBeDisabled(t *testing.T) {
	q := getTestQuiesce()
	threshold := q.threshold()
	tests := []struct {
		tick     uint64
		quiesced bool
	}{
		{threshold / 2, false},
		{threshold, false},
		{threshold + 1, false},
	}
	for i, tt := range tests {
		q := getTestQuiesce()
		// disable it
		q.enabled = false
		for k := uint64(0); k < tt.tick; k++ {
			q.tick()
		}
		assert.Equal(t, tt.quiesced, q.quiesced(),
			"test case %d", i)
	}
}

func TestExitFromQuiesceWhenActivityIsRecorded(t *testing.T) {
	tests := []pb.MessageType{
		pb.Replicate,
		pb.ReplicateResp,
		pb.RequestVote,
		pb.RequestVoteResp,
		pb.InstallSnapshot,
		pb.Propose,
		pb.ReadIndex,
		pb.ConfigChangeEvent,
	}
	for i, tt := range tests {
		q := getTestQuiesce()
		for k := uint64(0); k < q.threshold()+1; k++ {
			q.tick()
		}
		assert.True(t, q.quiesced(),
			"test case %d: should be quiesced", i)
		q.record(tt)
		assert.False(t, q.quiesced(),
			"test case %d: should not be quiesced", i)
		assert.Equal(t, q.currentTick, q.idleSince,
			"test case %d: idleSince should equal currentTick", i)
	}
}

func TestMsgHeartbeatWillNotStopEnteringQuiesce(t *testing.T) {
	q := getTestQuiesce()
	threshold := q.threshold()
	tests := []struct {
		tick     uint64
		quiesced bool
	}{
		{threshold / 2, false},
		{threshold, false},
		{threshold + 1, true},
	}
	for i, tt := range tests {
		q := getTestQuiesce()
		for k := uint64(0); k < tt.tick; k++ {
			q.tick()
			q.record(pb.Heartbeat)
		}
		assert.Equal(t, tt.quiesced, q.quiesced(),
			"test case %d", i)
	}
}

func TestWillNotExitFromQuiesceForDelayedMsgHeartbeatMsg(t *testing.T) {
	q := getTestQuiesce()
	for k := uint64(0); k < q.threshold()+1; k++ {
		q.tick()
	}
	assert.True(t, q.quiesced(), "should be quiesced")
	assert.True(t, q.newToQuiesce(), "should be new to quiesce")
	for q.newToQuiesce() {
		q.record(pb.Heartbeat)
		assert.True(t, q.quiesced(), "should remain quiesced")
		q.tick()
	}
	// no longer considered as recently entered quiesce
	q.record(pb.Heartbeat)
	assert.False(t, q.quiesced(), "should not be quiesced")
}
