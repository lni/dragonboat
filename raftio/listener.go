// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

package raftio

const (
	// NoLeader is a special leader ID value to indicate that there is currently
	// no leader or leader ID is unknown.
	NoLeader uint64 = 0
)

// LeaderInfo contains info on Raft leader.
type LeaderInfo struct {
	ClusterID uint64
	NodeID    uint64
	Term      uint64
	LeaderID  uint64
}

// IRaftEventListener is the interface to allow users to get notified for
// certain Raft events.
type IRaftEventListener interface {
	LeaderUpdated(info LeaderInfo)
}
