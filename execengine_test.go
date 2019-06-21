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

// +build !dragonboat_slowtest
// +build !dragonboat_errorinjectiontest

package dragonboat

import (
	"testing"

	"github.com/lni/dragonboat/v3/internal/rsm"
)

func TestWorkReadyCanBeCreated(t *testing.T) {
	wr := newWorkReady(4)
	if len(wr.readyMapList) != 4 || len(wr.readyChList) != 4 {
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
	if lns.loaded(2, 3) {
		t.Errorf("unexpectedly returned true")
	}
	nodes := make(map[uint64]*node)
	n := &node{}
	n.nodeID = 3
	nodes[2] = n
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if !lns.loaded(2, 3) {
		t.Errorf("unexpectedly returned false")
	}
	n.nodeID = 4
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if lns.loaded(2, 3) {
		t.Errorf("unexpectedly returned true")
	}
	nodes = make(map[uint64]*node)
	nodes[5] = n
	n.nodeID = 3
	lns.update(1, rsm.FromSnapshotWorker, nodes)
	if lns.loaded(2, 3) {
		t.Errorf("unexpectedly returned true")
	}

}
