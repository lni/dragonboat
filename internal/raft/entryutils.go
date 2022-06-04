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

package raft

import (
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func countConfigChange(entries []pb.Entry) int {
	c := 0
	for i := range entries {
		if entries[i].Type == pb.ConfigChangeEntry {
			c++
		}
	}
	return c
}

func newEntrySlice(ents []pb.Entry) []pb.Entry {
	var n []pb.Entry
	return append(n, ents...)
}

func checkEntriesToAppend(ents []pb.Entry, toAppend []pb.Entry) {
	if len(ents) == 0 || len(toAppend) == 0 {
		return
	}
	if ents[len(ents)-1].Index+1 != toAppend[0].Index {
		plog.Panicf("found a hole, last %d, first to append %d",
			ents[len(ents)-1].Index, toAppend[0].Index)
	}
	if ents[len(ents)-1].Term > toAppend[0].Term {
		plog.Panicf("term value not expected, %d vs %d",
			ents[len(ents)-1].Term, toAppend[0].Term)
	}
}

func limitSize(ents []pb.Entry, limit uint64) []pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	total := ents[0].SizeUpperLimit()
	var inc int
	for inc = 1; inc < len(ents); inc++ {
		total += ents[inc].SizeUpperLimit()
		if uint64(total) > limit {
			break
		}
	}
	return ents[:inc]
}

func min(x uint64, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

func max(x uint64, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func getEntrySliceInMemSize(ents []pb.Entry) uint64 {
	return pb.GetEntrySliceInMemSize(ents)
}

func getEntrySliceSize(ents []pb.Entry) uint64 {
	return pb.GetEntrySliceSize(ents)
}

// IsLocalMessageType returns a boolean value indicating whether the specified
// message type is a local message type.
func IsLocalMessageType(t pb.MessageType) bool {
	return isLocalMessageType(t)
}

func isLocalMessageType(t pb.MessageType) bool {
	return t == pb.Election ||
		t == pb.LeaderHeartbeat ||
		t == pb.Unreachable ||
		t == pb.SnapshotStatus ||
		t == pb.CheckQuorum ||
		t == pb.LocalTick ||
		t == pb.BatchedReadIndex
}

func isResponseMessageType(t pb.MessageType) bool {
	return t == pb.ReplicateResp ||
		t == pb.RequestVoteResp ||
		t == pb.HeartbeatResp ||
		t == pb.ReadIndexResp ||
		t == pb.Unreachable ||
		t == pb.SnapshotStatus ||
		t == pb.LeaderTransfer
}
