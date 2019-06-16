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

	"github.com/lni/dragonboat/raftio"
)

func TestSnapshotStatusAddStatus(t *testing.T) {
	p := newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	p.addStatus(100, 2, true, 100)
	if len(p.pendings) != 2 {
		t.Errorf("got %d, want 2", len(p.pendings))
	}
	p.addStatus(100, 1, true, 200)
	if len(p.pendings) != 2 {
		t.Errorf("got %d, want 2", len(p.pendings))
	}
	v, ok := p.pendings[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("failed to get rec")
	}
	if v.releaseTick != 200+feedbackPushDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestSnapshotStatusGetReady(t *testing.T) {
	p := newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	p.addStatus(100, 2, true, 200)
	if len(p.pendings) != 2 {
		t.Errorf("got %d, want 2", len(p.pendings))
	}
	r := p.getReady(150 + feedbackPushDelayTick)
	if len(r) != 1 {
		t.Errorf("got %d, want 1", len(r))
	}
	p = newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	p.addStatus(100, 2, true, 200)
	r = p.getReady(200 + feedbackPushDelayTick)
	if len(r) != 1 {
		t.Errorf("got %d, want 1", len(r))
	}
	p = newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	p.addStatus(100, 2, true, 200)
	r = p.getReady(201 + feedbackPushDelayTick)
	if len(r) != 2 {
		t.Errorf("got %d, want 2", len(r))
	}
}

func TestSnapshotStatusAddRetry(t *testing.T) {
	p := newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	s1 := snapshotStatus{
		clusterID:   100,
		nodeID:      1,
		releaseTick: 500,
		failed:      true,
	}
	s2 := snapshotStatus{
		clusterID:   100,
		nodeID:      2,
		releaseTick: 600,
		failed:      false,
	}
	p.addRetry([]snapshotStatus{s1, s2}, 1000)
	if len(p.pendings) != 2 {
		t.Errorf("got %d, want 2", len(p.pendings))
	}
	v, ok := p.pendings[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.releaseTick != 1000+feedbackRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
	v, ok = p.pendings[raftio.GetNodeInfo(100, 2)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.releaseTick != 1000+feedbackRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestSnapshotStatusAddConfirmation(t *testing.T) {
	p := newSnapshotFeedback(nil)
	p.addStatus(100, 1, true, 100)
	p.confirm(100, 1, 120)
	p.confirm(100, 2, 130)
	if len(p.pendings) != 2 {
		t.Errorf("got %d, want 2", len(p.pendings))
	}
	v, ok := p.pendings[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.releaseTick != 120+feedbackConfirmedDelayTick {
		t.Errorf("unexpected release tick")
	}
	v, ok = p.pendings[raftio.GetNodeInfo(100, 2)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.releaseTick != 130+feedbackConfirmedDelayTick {
		t.Errorf("unexpected release tick")
	}
}

func TestSnapshotStatusPushReady(t *testing.T) {
	pushed := 0
	pf := func(clusterID uint64, nodeID uint64, failed bool) bool {
		pushed++
		return true
	}
	p := newSnapshotFeedback(pf)
	p.addStatus(100, 1, true, 100)
	p.pushReady(101 + feedbackPushDelayTick)
	if pushed != 1 {
		t.Errorf("rec not pushed")
	}
	if len(p.pendings) != 0 {
		t.Errorf("unexpected rec in pendings")
	}
	pushed = 0
	pfr := func(clusterID uint64, nodeID uint64, failed bool) bool {
		pushed++
		return false
	}
	p = newSnapshotFeedback(pfr)
	p.addStatus(100, 1, true, 100)
	p.pushReady(101 + feedbackPushDelayTick)
	if pushed != 1 {
		t.Errorf("rec not pushed")
	}
	if len(p.pendings) != 1 {
		t.Errorf("rec not added back")
	}
	v, ok := p.pendings[raftio.GetNodeInfo(100, 1)]
	if !ok {
		t.Fatalf("rec not found")
	}
	if v.releaseTick != 101+feedbackPushDelayTick+feedbackRetryDelayTick {
		t.Errorf("unexpected release tick")
	}
}
