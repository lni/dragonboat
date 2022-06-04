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
		if q.quiesced() != tt.quiesced {
			t.Errorf("i %d, got %t, want %t", i, q.quiesced(), tt.quiesced)
		}
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
		if q.quiesced() != tt.quiesced {
			t.Errorf("i %d, got %t, want %t", i, q.quiesced(), tt.quiesced)
		}
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
		if !q.quiesced() {
			t.Errorf("i %d, got %t, want %t", i, q.quiesced(), true)
		}
		q.record(tt)
		if q.quiesced() {
			t.Errorf("i %d, got %t, want %t", i, q.quiesced(), false)
		}
		if q.idleSince != q.currentTick {
			t.Errorf("i %d, q.idleSince %d, want %d", i, q.idleSince, q.currentTick)
		}
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
		if q.quiesced() != tt.quiesced {
			t.Errorf("i %d, got %t, want %t", i, q.quiesced(), tt.quiesced)
		}
	}
}

func TestWillNotExitFromQuiesceForDelayedMsgHeartbeatMsg(t *testing.T) {
	q := getTestQuiesce()
	for k := uint64(0); k < q.threshold()+1; k++ {
		q.tick()
	}
	if !q.quiesced() {
		t.Errorf("got %t, want %t", q.quiesced(), true)
	}
	if !q.newToQuiesce() {
		t.Errorf("got %t, want %t", q.newToQuiesce(), true)
	}
	for q.newToQuiesce() {
		q.record(pb.Heartbeat)
		if !q.quiesced() {
			t.Errorf("got %t, want true", q.quiesced())
		}
		q.tick()
	}
	// no longer considered as recently entered quiesce
	q.record(pb.Heartbeat)
	if q.quiesced() {
		t.Errorf("got %t, want false", q.quiesced())
	}
}
