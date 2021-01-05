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
	"testing"

	"github.com/lni/dragonboat/v3/internal/rsm"
)

func TestWorkReadyCanBeCreated(t *testing.T) {
	wr := newWorkReady(4)
	if len(wr.maps) != 4 || len(wr.channels) != 4 {
		t.Errorf("unexpected ready list len")
	}
	if wr.count != 4 {
		t.Errorf("unexpected count value")
	}
}

func TestPartitionerWorksAsExpected(t *testing.T) {
	wr := newWorkReady(4)
	p := wr.getPartitioner()
	vals := make(map[uint64]struct{})
	for i := uint64(0); i < uint64(128); i++ {
		idx := p.GetPartitionID(i)
		vals[idx] = struct{}{}
	}
	if len(vals) != 4 {
		t.Errorf("unexpected partitioner outcome")
	}
}

func TestAllClustersReady(t *testing.T) {
	wr := newWorkReady(4)
	nodes := make([]*node, 0)
	for i := uint64(0); i < uint64(4); i++ {
		nodes = append(nodes, &node{clusterID: i})
	}
	wr.allClustersReady(nodes)
	for i := uint64(0); i < uint64(4); i++ {
		ch := wr.channels[i]
		select {
		case <-ch:
		default:
			t.Errorf("channel not ready")
		}
		rc := wr.maps[i]
		m := rc.getReadyClusters()
		if len(m) != 1 {
			t.Errorf("unexpected map size")
		}
		if _, ok := m[i]; !ok {
			t.Errorf("cluster not set")
		}
	}
	nodes = nodes[:0]
	nodes = append(nodes, []*node{{clusterID: 0}, {clusterID: 2}, {clusterID: 3}}...)
	wr.allClustersReady(nodes)
	ch := wr.channels[1]
	select {
	case <-ch:
		t.Errorf("channel unexpectedly set as ready")
	default:
	}
	rc := wr.maps[1]
	m := rc.getReadyClusters()
	if len(m) != 0 {
		t.Errorf("cluster map unexpected set")
	}
}

func TestWorkCanBeSetAsReady(t *testing.T) {
	wr := newWorkReady(4)
	select {
	case <-wr.waitCh(1):
		t.Errorf("ready signaled")
	case <-wr.waitCh(2):
		t.Errorf("ready signaled")
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
	}
	wr.clusterReady(0)
	select {
	case <-wr.waitCh(1):
	case <-wr.waitCh(2):
		t.Errorf("ready signaled")
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
		t.Errorf("ready not signaled")
	}
	wr.clusterReady(9)
	select {
	case <-wr.waitCh(1):
		t.Errorf("ready signaled")
	case <-wr.waitCh(2):
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
		t.Errorf("ready not signaled")
	}
}

func TestReturnedReadyMapContainsReadyClusterID(t *testing.T) {
	wr := newWorkReady(4)
	wr.clusterReady(0)
	wr.clusterReady(4)
	wr.clusterReady(129)
	ready := wr.getReadyMap(1)
	if len(ready) != 2 {
		t.Errorf("unexpected ready map size, sz: %d", len(ready))
	}
	_, ok := ready[0]
	_, ok2 := ready[4]
	if !ok || !ok2 {
		t.Errorf("missing cluster id")
	}
	ready = wr.getReadyMap(2)
	if len(ready) != 1 {
		t.Errorf("unexpected ready map size")
	}
	_, ok = ready[129]
	if !ok {
		t.Errorf("missing cluster id")
	}
	ready = wr.getReadyMap(3)
	if len(ready) != 0 {
		t.Errorf("unexpected ready map size")
	}
}

func TestLoadedNodes(t *testing.T) {
	lns := newLoadedNodes()
	if lns.get(2, 3) != nil {
		t.Errorf("unexpectedly returned true")
	}
	nodes := make(map[uint64]*node)
	n := &node{}
	n.nodeID = 3
	nodes[2] = n
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if lns.get(2, 3) == nil {
		t.Errorf("unexpectedly returned false")
	}
	n.nodeID = 4
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if lns.get(2, 3) != nil {
		t.Errorf("unexpectedly returned true")
	}
	nodes = make(map[uint64]*node)
	nodes[5] = n
	n.nodeID = 3
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if lns.get(2, 3) != nil {
		t.Errorf("unexpectedly returned true")
	}
}

func TestBusyMapKeyIsIgnoredWhenUpdatingLoadedNodes(t *testing.T) {
	m := make(map[uint64]*ssNode)
	m[1] = &ssNode{n: &node{clusterID: 100, nodeID: 100}}
	m[2] = &ssNode{n: &node{clusterID: 200, nodeID: 200}}
	l := newLoadedNodes()
	l.updateFromBusySSNodes(2, rsm.FromSnapshotWorker, m)
	nm := l.nodes[nodeType{workerID: 2, from: rsm.FromSnapshotWorker}]
	if len(nm) != 2 {
		t.Errorf("unexpected map len")
	}
	if n, ok := nm[100]; !ok || n.clusterID != 100 {
		t.Errorf("failed to locate the node")
	}
	if n, ok := nm[200]; !ok || n.clusterID != 200 {
		t.Errorf("failed to locate the node")
	}
}

/*
func TestWPRemoveFromPending(t *testing.T) {
	tests := []struct {
		length uint64
		idx    uint64
	}{
		{1, 0},
		{5, 0},
		{5, 1},
		{5, 4},
	}
	for idx, tt := range tests {
		w := &workerPool{}
		for i := uint64(0); i < tt.length; i++ {
			cid := uint64(1)
			if i == tt.idx {
				cid = uint64(0)
			}
			r := tsn{task: rsm.Task{ClusterID: cid}}
			w.pending = append(w.pending, r)
		}
		if uint64(len(w.pending)) != tt.length {
			t.Errorf("unexpected length")
		}
		w.removeFromPending(int(tt.idx))
		if uint64(len(w.pending)) != tt.length-1 {
			t.Errorf("unexpected length")
		}
		for _, p := range w.pending {
			if p.task.ClusterID == 0 {
				t.Errorf("%d, pending not removed, %+v", idx, w.pending)
			}
		}
	}
}*/
