// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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
	"sync"
	"sync/atomic"

	"github.com/lni/dragonboat/v3/raftio"
)

var (
	streamPushDelayTick      uint64 = 10
	streamConfirmedDelayTick uint64 = 2
	streamRetryDelayTick     uint64 = 3
)

type stream struct {
	clusterID uint64
	nodeID    uint64
	tick      uint64
	failed    bool
}

type pushfunc func(clusterID uint64, nodeID uint64, failed bool) bool

type streamState struct {
	mu          sync.Mutex
	currentTick uint64
	streams     map[raftio.NodeInfo]stream
	pf          pushfunc
}

func newStreamState(f pushfunc) *streamState {
	return &streamState{
		streams: make(map[raftio.NodeInfo]stream),
		pf:      f,
	}
}

func (s *streamState) tick() {
	tick := s.increaseTick()
	ready := s.getReady(tick)
	if len(ready) == 0 {
		return
	}
	retry := make([]stream, 0)
	for _, r := range ready {
		if !s.pf(r.clusterID, r.nodeID, r.failed) {
			plog.Debugf("snapshot to %s failed", dn(r.clusterID, r.nodeID))
			retry = append(retry, r)
		}
	}
	s.retry(retry, tick)
}

func (s *streamState) increaseTick() uint64 {
	return atomic.AddUint64(&s.currentTick, 1)
}

func (s *streamState) getTick() uint64 {
	return atomic.LoadUint64(&s.currentTick)
}

func (s *streamState) getReady(tick uint64) []stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r []stream
	for _, p := range s.streams {
		if p.tick < tick {
			r = append(r, p)
			delete(s.streams, raftio.GetNodeInfo(p.clusterID, p.nodeID))
		}
	}
	return r
}

func (s *streamState) add(clusterID uint64, nodeID uint64, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plog.Debugf("snapshot status to %s added", dn(clusterID, nodeID))
	s.streams[raftio.GetNodeInfo(clusterID, nodeID)] = stream{
		clusterID: clusterID,
		nodeID:    nodeID,
		tick:      s.getTick() + streamPushDelayTick,
		failed:    failed,
	}
}

func (s *streamState) retry(retry []stream, tick uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range retry {
		r.tick = tick + streamRetryDelayTick
		s.streams[raftio.GetNodeInfo(r.clusterID, r.nodeID)] = r
	}
}

func (s *streamState) confirm(clusterID uint64, nodeID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plog.Debugf("snapshot status to %s confirmed", dn(clusterID, nodeID))
	s.streams[raftio.GetNodeInfo(clusterID, nodeID)] = stream{
		clusterID: clusterID,
		nodeID:    nodeID,
		tick:      s.getTick() + streamConfirmedDelayTick,
		failed:    false,
	}
}
