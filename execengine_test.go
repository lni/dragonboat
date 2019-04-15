// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

	"github.com/lni/dragonboat/internal/rsm"
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
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

func TestProcessUninitilizedNode(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	rec := processUninitializedNode(n)
	if rec == nil {
		t.Errorf("failed to returned the recover request")
	}
	if !n.ss.recoveringFromSnapshot() {
		t.Errorf("not marked as recovering")
	}
	n2 := &node{ss: &snapshotState{}}
	n2.initializedMu.initialized = true
	rec2 := processUninitializedNode(n2)
	if rec2 != nil {
		t.Errorf("unexpected recover from snapshot request")
	}
}

func TestProcessRecoveringNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	if processRecoveringNode(n) {
		t.Errorf("processRecoveringNode not skipped")
	}
}

func TestProcessTakingSnapshotNodeCanBeSkipped(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	if processTakingSnapshotNode(n) {
		t.Errorf("processTakingSnapshotNode not skipped")
	}
}

func TestRecoveringFromSnapshotNodeCanComplete(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setRecoveringFromSnapshot()
	n.ss.notifySnapshotStatus(false, true, false, true, 100)
	if processRecoveringNode(n) {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.recoveringFromSnapshot() {
		t.Errorf("still recovering")
	}
	if !n.initialized() {
		t.Errorf("not marked as initialized")
	}
	if n.ss.snapshotIndex != 100 {
		t.Errorf("unexpected snapshot index %d, want 100", n.ss.snapshotIndex)
	}
}

func TestNotReadyRecoveringFromSnapshotNode(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setRecoveringFromSnapshot()
	if !processRecoveringNode(n) {
		t.Errorf("not skipped")
	}
}

func TestTakingSnapshotNodeCanComplete(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.ss.setTakingSnapshot()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	n.initializedMu.initialized = true
	if processTakingSnapshotNode(n) {
		t.Errorf("node unexpectedly skipped")
	}
	if n.ss.takingSnapshot() {
		t.Errorf("still taking snapshot")
	}
}

func TestTakingSnapshotOnUninitializedNodeWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	n := &node{ss: &snapshotState{}}
	n.ss.setTakingSnapshot()
	n.ss.notifySnapshotStatus(true, false, false, false, 0)
	processTakingSnapshotNode(n)
}

type testDummyNodeProxy struct{}

func (np *testDummyNodeProxy) RestoreRemotes(pb.Snapshot)                        {}
func (np *testDummyNodeProxy) ApplyUpdate(pb.Entry, sm.Result, bool, bool, bool) {}
func (np *testDummyNodeProxy) ApplyConfigChange(pb.ConfigChange)                 {}
func (np *testDummyNodeProxy) ConfigChangeProcessed(uint64, bool)                {}
func (np *testDummyNodeProxy) NodeID() uint64                                    { return 1 }
func (np *testDummyNodeProxy) ClusterID() uint64                                 { return 1 }

func TestNotReadyTakingSnapshotNodeIsSkippedWhenConcurrencyIsNotSupported(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeStateMachine(1, 1, &rsm.RegularStateMachine{}, nil), nil, false, &testDummyNodeProxy{})
	if n.concurrentSnapshot() {
		t.Errorf("concurrency not suppose to be supported")
	}
	n.ss.setTakingSnapshot()
	n.initializedMu.initialized = true
	if !processTakingSnapshotNode(n) {
		t.Fatalf("node not skipped")
	}
}

func TestNotReadyTakingSnapshotNodeIsNotSkippedWhenConcurrencyIsSupported(t *testing.T) {
	n := &node{ss: &snapshotState{}}
	n.sm = rsm.NewStateMachine(
		rsm.NewNativeStateMachine(1, 1, &rsm.ConcurrentStateMachine{}, nil), nil, false, &testDummyNodeProxy{})
	if !n.concurrentSnapshot() {
		t.Errorf("concurrency not supported")
	}
	n.ss.setTakingSnapshot()
	n.initializedMu.initialized = true
	if processTakingSnapshotNode(n) {
		t.Fatalf("node unexpectedly skipped")
	}

}
