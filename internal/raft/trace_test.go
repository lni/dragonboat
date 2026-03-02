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
	"os"
	"path/filepath"
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

// memTraceLogger collects trace events in memory for inspection.
type memTraceLogger struct {
	events []TraceEvent
}

func (l *memTraceLogger) Log(e TraceEvent) {
	l.events = append(l.events, e)
}

// attachTraceLoggers attaches a shared NDJSONTraceLogger and per-node
// memTraceLoggers to every raft node in a network.
// Returns the per-node collectors keyed by replica ID.
func attachTraceLoggers(nw *network, file *NDJSONTraceLogger) map[uint64]*memTraceLogger {
	collectors := make(map[uint64]*memTraceLogger)
	for id, p := range nw.peers {
		r, ok := p.(*raft)
		if !ok {
			continue
		}
		ml := &memTraceLogger{}
		collectors[id] = ml
		// multiTraceLogger fans out to both the file logger and the in-memory collector.
		r.traceLogger = &multiTraceLogger{loggers: []TraceLogger{file, ml}}
	}
	return collectors
}

// multiTraceLogger broadcasts events to multiple TraceLogger implementations.
type multiTraceLogger struct {
	loggers []TraceLogger
}

func (m *multiTraceLogger) Log(e TraceEvent) {
	for _, l := range m.loggers {
		l.Log(e)
	}
}

// TestTraceElectionAndReplication drives a 3-node cluster through an election,
// client requests, heartbeats, and replication, writing an NDJSON trace file
// suitable for TLC trace validation.
func TestTraceElectionAndReplication(t *testing.T) {
	// Output directory: <repo>/case-studies/dragonboat/traces/
	traceDir := filepath.Join("..", "..", "..", "..", "traces")
	if err := os.MkdirAll(traceDir, 0755); err != nil {
		t.Fatalf("failed to create traces dir: %v", err)
	}
	tracePath := filepath.Join(traceDir, "election_replication.ndjson")

	fl, err := NewNDJSONTraceLogger(tracePath)
	if err != nil {
		t.Fatalf("failed to create trace logger: %v", err)
	}
	defer fl.Close()

	// --- Create a 3-node network ---
	nw := newNetwork(nil, nil, nil)
	collectors := attachTraceLoggers(nw, fl)

	// --- Phase 1: Election ---
	// Trigger election on node 1. This sends Election to node 1 which makes it
	// a candidate. The network delivers RequestVoteRequest to nodes 2,3 and
	// their RequestVoteResponse back to node 1, which becomes leader.
	nw.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	lead := nw.peers[1].(*raft)
	if !lead.isLeader() {
		t.Fatalf("node 1 should be leader, got state=%s", lead.state)
	}

	// --- Phase 2: Client requests ---
	// Propose 3 entries from the leader. Each proposal triggers replication.
	for i := 0; i < 3; i++ {
		nw.send(pb.Message{
			From:    1,
			To:      1,
			Type:    pb.Propose,
			Entries: []pb.Entry{{Cmd: []byte("cmd")}},
		})
	}

	// --- Phase 3: Heartbeats ---
	// Trigger a round of heartbeats from the leader. The heartbeat tick
	// generates SendHeartbeat events for each follower, and their responses
	// come back as HandleHeartbeatResponse.
	for i := uint64(0); i < lead.heartbeatTimeout; i++ {
		if err := lead.Handle(pb.Message{Type: pb.LocalTick, Reject: false}); err != nil {
			t.Fatalf("tick failed: %v", err)
		}
	}
	// Deliver heartbeat messages
	msgs := lead.readMessages()
	for _, m := range msgs {
		nw.send(m)
	}

	// --- Phase 4: Another client request to trigger AdvanceCommitIndex ---
	nw.send(pb.Message{
		From:    1,
		To:      1,
		Type:    pb.Propose,
		Entries: []pb.Entry{{Cmd: []byte("final")}},
	})

	// --- Summary ---
	totalEvents := 0
	for id, c := range collectors {
		t.Logf("node %d: %d trace events", id, len(c.events))
		totalEvents += len(c.events)
	}
	t.Logf("total trace events: %d", totalEvents)
	t.Logf("trace written to: %s", tracePath)

	if totalEvents == 0 {
		t.Fatal("no trace events were collected")
	}

	// Verify we have the key event types
	eventCounts := make(map[string]int)
	for _, c := range collectors {
		for _, e := range c.events {
			eventCounts[e.Event.Name]++
		}
	}

	requiredEvents := []string{
		"BecomeCandidate",
		"BecomeLeader",
		"HandleRequestVoteRequest",
		"HandleRequestVoteResponse",
		"ClientRequest",
		"SendReplicateEntries",
		"HandleReplicateRequest",
		"HandleReplicateResponse",
		"AdvanceCommitIndex",
		"SendHeartbeat",
		"HandleHeartbeatRequest",
		"HandleHeartbeatResponse",
	}
	for _, name := range requiredEvents {
		if eventCounts[name] == 0 {
			t.Errorf("missing required trace event: %s", name)
		}
	}

	t.Log("event counts:")
	for name, count := range eventCounts {
		t.Logf("  %-30s %d", name, count)
	}
}

// TestTraceCheckQuorum drives a scenario that triggers the CheckQuorum logic.
// Node 1 becomes leader, then we isolate it (no heartbeat responses) and
// advance the election timeout to trigger CheckQuorum.
func TestTraceCheckQuorum(t *testing.T) {
	traceDir := filepath.Join("..", "..", "..", "..", "traces")
	if err := os.MkdirAll(traceDir, 0755); err != nil {
		t.Fatalf("failed to create traces dir: %v", err)
	}
	tracePath := filepath.Join(traceDir, "check_quorum.ndjson")

	fl, err := NewNDJSONTraceLogger(tracePath)
	if err != nil {
		t.Fatalf("failed to create trace logger: %v", err)
	}
	defer fl.Close()

	// Create a 3-node network.
	nw := newNetwork(nil, nil, nil)
	collectors := attachTraceLoggers(nw, fl)

	// Make node 1 the leader.
	nw.send(pb.Message{From: 1, To: 1, Type: pb.Election})

	lead := nw.peers[1].(*raft)
	if !lead.isLeader() {
		t.Fatalf("node 1 should be leader, got state=%s", lead.state)
	}

	// Enable CheckQuorum on the leader directly.
	lead.checkQuorum = true

	// Isolate the leader so it gets no heartbeat responses.
	nw.isolate(1)

	// First election timeout cycle: clears active flags (leader counts itself +
	// remotes that were active from the initial replication).
	// leaderHasQuorum() clears active flags as a side effect.
	for i := uint64(0); i < lead.electionTimeout; i++ {
		if err := lead.Handle(pb.Message{Type: pb.LocalTick, Reject: false}); err != nil {
			t.Fatalf("tick failed (cycle 1): %v", err)
		}
	}

	// Second election timeout cycle: with all active flags cleared and no
	// heartbeat responses coming back, the leader should step down.
	for i := uint64(0); i < lead.electionTimeout; i++ {
		if err := lead.Handle(pb.Message{Type: pb.LocalTick, Reject: false}); err != nil {
			t.Fatalf("tick failed (cycle 2): %v", err)
		}
	}

	// After two CheckQuorum cycles, the leader should have stepped down.
	if lead.isLeader() {
		t.Errorf("leader should have stepped down, but is still leader")
	}

	totalEvents := 0
	for id, c := range collectors {
		t.Logf("node %d: %d trace events", id, len(c.events))
		totalEvents += len(c.events)
	}
	t.Logf("total trace events: %d", totalEvents)
	t.Logf("trace written to: %s", tracePath)

	// Verify CheckQuorum event was produced
	found := false
	for _, c := range collectors {
		for _, e := range c.events {
			if e.Event.Name == "CheckQuorum" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Error("missing CheckQuorum trace event")
	}
}
