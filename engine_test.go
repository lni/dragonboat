// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
)

func TestBitmapAdd(t *testing.T) {
	var b bitmap
	for i := uint64(0); i < 64; i++ {
		if b.contains(i) {
			t.Errorf("unexpectedly contains value %d", i)
		}
		b.add(i)
		if !b.contains(i) {
			t.Errorf("failed to add value %d", i)
		}
	}
}

func TestBitmapContains(t *testing.T) {
	var b bitmap
	b.add(1)
	b.add(3)
	if !b.contains(1) {
		t.Errorf("contains 1 failed")
	}
	if !b.contains(3) {
		t.Errorf("contains 3 failed")
	}
	if b.contains(2) {
		t.Errorf("contains 2 failed")
	}
}

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

func TestAllShardsReady(t *testing.T) {
	wr := newWorkReady(4)
	nodes := make([]*node, 0)
	for i := uint64(0); i < uint64(4); i++ {
		nodes = append(nodes, &node{shardID: i})
	}
	wr.allShardsReady(nodes)
	for i := uint64(0); i < uint64(4); i++ {
		ch := wr.channels[i]
		select {
		case <-ch:
		default:
			t.Errorf("channel not ready")
		}
		rc := wr.maps[i]
		m := rc.getReadyShards()
		if len(m) != 1 {
			t.Errorf("unexpected map size")
		}
		if _, ok := m[i]; !ok {
			t.Errorf("shard not set")
		}
	}
	nodes = nodes[:0]
	nodes = append(nodes, []*node{{shardID: 0}, {shardID: 2}, {shardID: 3}}...)
	wr.allShardsReady(nodes)
	ch := wr.channels[1]
	select {
	case <-ch:
		t.Errorf("channel unexpectedly set as ready")
	default:
	}
	rc := wr.maps[1]
	m := rc.getReadyShards()
	if len(m) != 0 {
		t.Errorf("shard map unexpected set")
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
	wr.shardReady(0)
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
	wr.shardReady(9)
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

func TestReturnedReadyMapContainsReadyShardID(t *testing.T) {
	wr := newWorkReady(4)
	wr.shardReady(0)
	wr.shardReady(4)
	wr.shardReady(129)
	ready := wr.getReadyMap(1)
	if len(ready) != 2 {
		t.Errorf("unexpected ready map size, sz: %d", len(ready))
	}
	_, ok := ready[0]
	_, ok2 := ready[4]
	if !ok || !ok2 {
		t.Errorf("missing shard id")
	}
	ready = wr.getReadyMap(2)
	if len(ready) != 1 {
		t.Errorf("unexpected ready map size")
	}
	_, ok = ready[129]
	if !ok {
		t.Errorf("missing shard id")
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
	n.replicaID = 3
	nodes[2] = n
	lns.update(1, fromStepWorker, nodes)
	if lns.get(2, 3) == nil {
		t.Errorf("unexpectedly returned false")
	}
	n.replicaID = 4
	lns.update(1, fromStepWorker, nodes)
	if lns.get(2, 3) != nil {
		t.Errorf("unexpectedly returned true")
	}
	nodes = make(map[uint64]*node)
	nodes[5] = n
	n.replicaID = 3
	lns.update(1, fromStepWorker, nodes)
	if lns.get(2, 3) != nil {
		t.Errorf("unexpectedly returned true")
	}
}

func TestBusyMapKeyIsIgnoredWhenUpdatingLoadedNodes(t *testing.T) {
	m := make(map[uint64]*node)
	m[1] = &node{shardID: 100, replicaID: 100}
	m[2] = &node{shardID: 200, replicaID: 200}
	l := newLoadedNodes()
	l.updateFromBusySSNodes(m)
	nm := l.nodes[nodeType{workerID: 0, from: fromWorker}]
	if len(nm) != 2 {
		t.Errorf("unexpected map len")
	}
	if n, ok := nm[100]; !ok || n.shardID != 100 {
		t.Errorf("failed to locate the node")
	}
	if n, ok := nm[200]; !ok || n.shardID != 200 {
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
			r := tsn{task: rsm.Task{ShardID: cid}}
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
			if p.task.ShardID == 0 {
				t.Errorf("%d, pending not removed, %+v", idx, w.pending)
			}
		}
	}
}*/
