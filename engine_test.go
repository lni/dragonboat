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

	"github.com/stretchr/testify/require"
)

func TestBitmapAdd(t *testing.T) {
	var b bitmap
	for i := uint64(0); i < 64; i++ {
		require.False(t, b.contains(i),
			"unexpectedly contains value %d", i)
		b.add(i)
		require.True(t, b.contains(i),
			"failed to add value %d", i)
	}
}

func TestBitmapContains(t *testing.T) {
	var b bitmap
	b.add(1)
	b.add(3)
	require.True(t, b.contains(1), "contains 1 failed")
	require.True(t, b.contains(3), "contains 3 failed")
	require.False(t, b.contains(2), "contains 2 failed")
}

func TestWorkReadyCanBeCreated(t *testing.T) {
	wr := newWorkReady(4)
	require.Equal(t, 4, len(wr.maps), "unexpected ready list len")
	require.Equal(t, 4, len(wr.channels), "unexpected ready list len")
	require.Equal(t, uint64(4), wr.count, "unexpected count value")
}

func TestPartitionerWorksAsExpected(t *testing.T) {
	wr := newWorkReady(4)
	p := wr.getPartitioner()
	vals := make(map[uint64]struct{})
	for i := uint64(0); i < uint64(128); i++ {
		idx := p.GetPartitionID(i)
		vals[idx] = struct{}{}
	}
	require.Equal(t, 4, len(vals), "unexpected partitioner outcome")
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
			require.Fail(t, "channel not ready")
		}
		rc := wr.maps[i]
		m := rc.getReadyShards()
		require.Equal(t, 1, len(m), "unexpected map size")
		_, ok := m[i]
		require.True(t, ok, "shard not set")
	}
	nodes = nodes[:0]
	nodes = append(nodes, []*node{{shardID: 0}, {shardID: 2},
		{shardID: 3}}...)
	wr.allShardsReady(nodes)
	ch := wr.channels[1]
	select {
	case <-ch:
		require.Fail(t, "channel unexpectedly set as ready")
	default:
	}
	rc := wr.maps[1]
	m := rc.getReadyShards()
	require.Equal(t, 0, len(m), "shard map unexpected set")
}

func TestWorkCanBeSetAsReady(t *testing.T) {
	wr := newWorkReady(4)
	select {
	case <-wr.waitCh(1):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(2):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(3):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(4):
		require.Fail(t, "ready signaled")
	default:
	}
	wr.shardReady(0)
	select {
	case <-wr.waitCh(1):
	case <-wr.waitCh(2):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(3):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(4):
		require.Fail(t, "ready signaled")
	default:
		require.Fail(t, "ready not signaled")
	}
	wr.shardReady(9)
	select {
	case <-wr.waitCh(1):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(2):
	case <-wr.waitCh(3):
		require.Fail(t, "ready signaled")
	case <-wr.waitCh(4):
		require.Fail(t, "ready signaled")
	default:
		require.Fail(t, "ready not signaled")
	}
}

func TestReturnedReadyMapContainsReadyShardID(t *testing.T) {
	wr := newWorkReady(4)
	wr.shardReady(0)
	wr.shardReady(4)
	wr.shardReady(129)
	ready := wr.getReadyMap(1)
	require.Equal(t, 2, len(ready),
		"unexpected ready map size, sz: %d", len(ready))
	_, ok := ready[0]
	_, ok2 := ready[4]
	require.True(t, ok && ok2, "missing shard id")
	ready = wr.getReadyMap(2)
	require.Equal(t, 1, len(ready), "unexpected ready map size")
	_, ok = ready[129]
	require.True(t, ok, "missing shard id")
	ready = wr.getReadyMap(3)
	require.Equal(t, 0, len(ready), "unexpected ready map size")
}

func TestLoadedNodes(t *testing.T) {
	lns := newLoadedNodes()
	require.Nil(t, lns.get(2, 3), "unexpectedly returned true")
	nodes := make(map[uint64]*node)
	n := &node{}
	n.replicaID = 3
	nodes[2] = n
	lns.update(1, fromStepWorker, nodes)
	require.NotNil(t, lns.get(2, 3), "unexpectedly returned false")
	n.replicaID = 4
	lns.update(1, fromStepWorker, nodes)
	require.Nil(t, lns.get(2, 3), "unexpectedly returned true")
	nodes = make(map[uint64]*node)
	nodes[5] = n
	n.replicaID = 3
	lns.update(1, fromStepWorker, nodes)
	require.Nil(t, lns.get(2, 3), "unexpectedly returned true")
}

func TestBusyMapKeyIsIgnoredWhenUpdatingLoadedNodes(t *testing.T) {
	m := make(map[uint64]*node)
	m[1] = &node{shardID: 100, replicaID: 100}
	m[2] = &node{shardID: 200, replicaID: 200}
	l := newLoadedNodes()
	l.updateFromBusySSNodes(m)
	nm := l.nodes[nodeType{workerID: 0, from: fromWorker}]
	require.Equal(t, 2, len(nm), "unexpected map len")
	n, ok := nm[100]
	require.True(t, ok, "failed to locate the node")
	require.Equal(t, uint64(100), n.shardID, "failed to locate the node")
	n, ok = nm[200]
	require.True(t, ok, "failed to locate the node")
	require.Equal(t, uint64(200), n.shardID, "failed to locate the node")
}
