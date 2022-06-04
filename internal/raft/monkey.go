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

//go:build dragonboat_monkeytest
// +build dragonboat_monkeytest

package raft

import (
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/logger"
)

var (
	mplog = logger.GetLogger("raft-mt")
)

// DumpRaftInfoToLog prints the raft state to log for debugging purposes.
func (p *Peer) DumpRaftInfoToLog(addrMap map[uint64]string) {
	p.raft.dumpRaftInfoToLog(addrMap)
}

func (p *Peer) GetInMemLogSize() uint64 {
	ents := p.raft.log.inmem.entries
	if len(ents) > 0 {
		if ents[0].Index == p.raft.applied {
			ents = ents[1:]
		}
	}
	return getEntrySliceInMemSize(ents)
}

func (p *Peer) GetRateLimiter() *server.InMemRateLimiter {
	return p.raft.log.inmem.rl
}

func (r *raft) dumpRaftInfoToLog(addrs map[uint64]string) {
	var flag string
	if r.leaderID == r.replicaID {
		flag = "***"
	} else {
		flag = "###"
	}
	mplog.Infof("%s Raft node %s, %d remote nodes",
		flag, r.describe(), len(r.remotes))
	for id, rp := range r.remotes {
		if v, ok := addrs[id]; !ok {
			mplog.Infof("---> node %d is missing", id)
		} else {
			mplog.Infof(" %s,addr:%s,match:%d,next:%d,state:%s,paused:%v,ra:%v,ps:%d",
				ReplicaID(id), v, rp.match, rp.next, rp.state, rp.isPaused(),
				rp.isActive(), rp.snapshotIndex)
		}
	}
}
