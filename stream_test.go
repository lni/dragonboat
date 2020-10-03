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

	"github.com/lni/dragonboat/v3/raftio"
)

func TestStreamStateAdd(t *testing.T) {
	p := newStreamState(nil)
	p.add(100, 1, true)
	p.add(100, 2, true)
	if len(p.streams) != 2 {
		t.Errorf("got %d, want 2", len(p.streams))
	}
	p.add(100, 1, true)
	if len(p.streams) != 2 {
		t.Errorf("got %d, want 2", len(p.streams))
	}
	v, ok := p.streams[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("failed to get rec")
	}
	if v.tick != streamPushDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestStreamStateGetReady(t *testing.T) {
	p := newStreamState(nil)
	p.add(100, 1, true)
	p.add(100, 2, true)
	if len(p.streams) != 2 {
		t.Errorf("got %d, want 2", len(p.streams))
	}
	r := p.getReady(1 + streamPushDelayTick)
	if len(r) != 2 {
		t.Errorf("got %d, want 2", len(r))
	}
	if len(p.streams) != 0 {
		t.Errorf("pending state not removed")
	}

	p = newStreamState(nil)
	p.add(100, 1, true)
	p.tick()
	p.add(100, 2, true)
	r = p.getReady(1 + streamPushDelayTick)
	if len(r) != 1 {
		t.Errorf("got %d, want 1", len(r))
	}
	if _, ok := p.streams[raftio.GetNodeInfo(100, 1)]; ok {
		t.Errorf("pending state not removed")
	}
	if _, ok := p.streams[raftio.GetNodeInfo(100, 2)]; !ok {
		t.Errorf("pending state unexpectedly removed")
	}
}

func TestStreamAddRetry(t *testing.T) {
	p := newStreamState(nil)
	p.add(100, 1, true)
	s1 := stream{
		clusterID: 100,
		nodeID:    1,
		tick:      500,
		failed:    true,
	}
	s2 := stream{
		clusterID: 100,
		nodeID:    2,
		tick:      600,
		failed:    false,
	}
	p.retry([]stream{s1, s2}, 1000)
	if len(p.streams) != 2 {
		t.Errorf("got %d, want 2", len(p.streams))
	}
	v, ok := p.streams[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.tick != 1000+streamRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
	v, ok = p.streams[raftio.GetNodeInfo(100, 2)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.tick != 1000+streamRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestStreamAddConfirmation(t *testing.T) {
	p := newStreamState(nil)
	p.add(100, 1, true)
	p.confirm(100, 1)
	p.confirm(100, 2)
	if len(p.streams) != 2 {
		t.Errorf("got %d, want 2", len(p.streams))
	}
	v, ok := p.streams[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.tick != streamConfirmedDelayTick {
		t.Errorf("unexpected release tick")
	}
	v, ok = p.streams[raftio.GetNodeInfo(100, 2)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.tick != streamConfirmedDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestStreamTickIncreaseTick(t *testing.T) {
	p := newStreamState(nil)
	for i := 0; i < 128; i++ {
		p.tick()
	}
	if p.getTick() != 128 {
		t.Errorf("tick value not updated")
	}
}

func TestSnapshotStatusPushReady(t *testing.T) {
	pushed := 0
	pf := func(clusterID uint64, nodeID uint64, failed bool) bool {
		pushed++
		return true
	}
	p := newStreamState(pf)
	p.add(100, 1, true)
	for i := uint64(0); i < streamPushDelayTick+1; i++ {
		p.tick()
	}
	if pushed != 1 {
		t.Errorf("rec not pushed")
	}
	if len(p.streams) != 0 {
		t.Errorf("unexpected rec in pendings")
	}
	pushed = 0
	pfr := func(clusterID uint64, nodeID uint64, failed bool) bool {
		pushed++
		return false
	}
	p = newStreamState(pfr)
	p.add(100, 1, true)
	for i := uint64(0); i < streamPushDelayTick+1; i++ {
		p.tick()
	}
	if pushed != 1 {
		t.Errorf("rec not pushed")
	}
	if len(p.streams) != 1 {
		t.Errorf("rec not added back")
	}
	v, ok := p.streams[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.tick != p.getTick()+streamRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
}
