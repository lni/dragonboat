// Copyright 2024 Specula contributors.
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
	"strconv"
)

// TraceLogger is the interface for emitting trace events for TLA+ trace
// validation. Implementations must be safe to call from within the raft
// state machine (i.e., while raftMu is held).
type TraceLogger interface {
	Log(event TraceEvent)
}

// TraceEvent is the top-level trace event envelope.
type TraceEvent struct {
	Tag   string     `json:"tag"`
	Event TraceInner `json:"event"`
}

// TraceInner carries the event name, acting node, state snapshot, and
// optional message fields.
type TraceInner struct {
	Name  string     `json:"name"`
	Nid   string     `json:"nid"`
	State TraceState `json:"state"`
	Msg   *TraceMsg  `json:"msg,omitempty"`
}

// TraceState is a snapshot of the node's Raft state at the time of the event.
type TraceState struct {
	Term         uint64 `json:"term"`
	Role         string `json:"role"`
	CommitIndex  uint64 `json:"commitIndex"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
	VotedFor     string `json:"votedFor"`
	Applied      uint64 `json:"applied"`
	PendingCC    bool   `json:"pendingCC"`
}

// TraceMsg carries message-related fields for RPC events.
type TraceMsg struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Term        uint64 `json:"term"`
	LogIndex    uint64 `json:"logIndex,omitempty"`
	LogTerm     uint64 `json:"logTerm,omitempty"`
	CommitIndex uint64 `json:"commitIndex,omitempty"`
	Reject      bool   `json:"reject,omitempty"`
	Hint        uint64 `json:"hint,omitempty"`
}

// emitTrace emits a trace event with the current node state snapshot.
// If r.traceLogger is nil, this is a no-op.
func (r *raft) emitTrace(name string, msg *TraceMsg) {
	if r.traceLogger == nil {
		return
	}
	var lastLogTerm uint64
	if li := r.log.lastIndex(); li > 0 {
		// lastTerm() calls term(lastIndex()) which is always available
		// in the inmem layer while the raft state machine is active.
		if t, err := r.log.lastTerm(); err == nil {
			lastLogTerm = t
		}
	}
	vf := ""
	if r.vote != NoLeader {
		vf = strconv.FormatUint(r.vote, 10)
	}
	r.traceLogger.Log(TraceEvent{
		Tag: "trace",
		Event: TraceInner{
			Name: name,
			Nid:  strconv.FormatUint(r.replicaID, 10),
			State: TraceState{
				Term:         r.term,
				Role:         r.state.String(),
				CommitIndex:  r.log.committed,
				LastLogIndex: r.log.lastIndex(),
				LastLogTerm:  lastLogTerm,
				VotedFor:     vf,
				Applied:      r.applied,
				PendingCC:    r.pendingConfigChange,
			},
			Msg: msg,
		},
	})
}

// traceMsg builds a TraceMsg from numeric node IDs.
func traceMsg(from, to uint64) *TraceMsg {
	return &TraceMsg{
		From: strconv.FormatUint(from, 10),
		To:   strconv.FormatUint(to, 10),
	}
}
