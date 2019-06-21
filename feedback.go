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

package dragonboat

import (
	"sync"

	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	feedbackPushDelayTick      uint64 = 20000
	feedbackConfirmedDelayTick uint64 = 1500
	feedbackRetryDelayTick     uint64 = 200
)

type snapshotStatus struct {
	clusterID   uint64
	nodeID      uint64
	releaseTick uint64
	failed      bool
}

type pushfunc func(clusterID uint64, nodeID uint64, failed bool) bool

type snapshotFeedback struct {
	mu       sync.Mutex
	pendings map[raftio.NodeInfo]snapshotStatus
	pf       pushfunc
}

func newSnapshotFeedback(f pushfunc) *snapshotFeedback {
	p := &snapshotFeedback{
		pendings: make(map[raftio.NodeInfo]snapshotStatus),
		pf:       f,
	}
	return p
}

func (p *snapshotFeedback) pushReady(tick uint64) {
	ready := p.getReady(tick)
	if len(ready) == 0 {
		return
	}
	notDelivered := make([]snapshotStatus, 0)
	for _, s := range ready {
		if !p.pf(s.clusterID, s.nodeID, s.failed) {
			plog.Debugf("snapshot status to %s not delivered",
				logutil.DescribeNode(s.clusterID, s.nodeID))
			notDelivered = append(notDelivered, s)
		} else {
			plog.Debugf("snapshot status to %s pushed",
				logutil.DescribeNode(s.clusterID, s.nodeID))
		}
	}
	p.addRetry(notDelivered, tick)
}

func (p *snapshotFeedback) getReady(tick uint64) []snapshotStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	var results []snapshotStatus
	for _, st := range p.pendings {
		if st.releaseTick < tick {
			results = append(results, st)
		}
	}
	for _, st := range results {
		key := raftio.GetNodeInfo(st.clusterID, st.nodeID)
		delete(p.pendings, key)
	}
	return results
}

func (p *snapshotFeedback) addStatus(clusterID uint64,
	nodeID uint64, failed bool, tick uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plog.Debugf("snapshot status to %s has been added",
		logutil.DescribeNode(clusterID, nodeID))
	s := snapshotStatus{
		clusterID:   clusterID,
		nodeID:      nodeID,
		releaseTick: tick + feedbackPushDelayTick,
		failed:      failed,
	}
	cn := raftio.GetNodeInfo(clusterID, nodeID)
	p.pendings[cn] = s
}

func (p *snapshotFeedback) addRetry(skipped []snapshotStatus, tick uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, st := range skipped {
		s := snapshotStatus{
			clusterID:   st.clusterID,
			nodeID:      st.nodeID,
			releaseTick: tick + feedbackRetryDelayTick,
			failed:      st.failed,
		}
		cn := raftio.GetNodeInfo(st.clusterID, st.nodeID)
		p.pendings[cn] = s
	}
}

func (p *snapshotFeedback) confirm(clusterID uint64,
	nodeID uint64, tick uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plog.Debugf("snapshot status to %s has been confirmed",
		logutil.DescribeNode(clusterID, nodeID))
	cn := raftio.GetNodeInfo(clusterID, nodeID)
	s := snapshotStatus{
		clusterID:   clusterID,
		nodeID:      nodeID,
		releaseTick: tick + feedbackConfirmedDelayTick,
		failed:      false,
	}
	p.pendings[cn] = s
}
