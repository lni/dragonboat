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

package rsm

import (
	"testing"

	pb "github.com/lni/dragonboat/v3/raftpb"
)

func TestAddressEqual(t *testing.T) {
	tests := []struct {
		addr1 string
		addr2 string
		equal bool
	}{
		{"v1", "v2", false},
		{"v1", "v1", true},
		{"v1", "V2", false},
		{"v1", "V1", true},
		{"v1", " v1", true},
		{"v1", "  v1", true},
		{"v1", "  v1   ", true},
		{"  v1  ", " V1 ", true},
	}
	for idx, tt := range tests {
		result := addressEqual(tt.addr1, tt.addr2)
		if result != tt.equal {
			t.Errorf("%d, got %t, want %t", idx, result, tt.equal)
		}
	}
}

func TestDeepCopyMembership(t *testing.T) {
	m := pb.Membership{
		ConfigChangeId: 101,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		Observers:      make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
	}
	copied := deepCopyMembership(m)
	m.ConfigChangeId = 102
	m.Addresses[1] = "addr1"
	m.Removed[1] = true
	m.Observers[1] = "addr1"
	m.Witnesses[1] = "addr1"
	if copied.ConfigChangeId != 101 ||
		len(copied.Addresses) != 0 ||
		len(copied.Removed) != 0 ||
		len(copied.Observers) != 0 {
		t.Fatalf("copied membership changed")
	}
	copied2 := deepCopyMembership(m)
	if copied2.ConfigChangeId != 102 ||
		len(copied2.Addresses) != 1 ||
		len(copied2.Removed) != 1 ||
		len(copied2.Observers) != 1 ||
		len(copied2.Witnesses) != 1 {
		t.Fatalf("unexpected copied membership data")
	}
}

func TestMembershipCanBeCreated(t *testing.T) {
	m := newMembership(1, 2, true)
	if !m.ordered {
		t.Errorf("not ordered")
	}
	if len(m.members.Addresses) != 0 ||
		len(m.members.Observers) != 0 ||
		len(m.members.Removed) != 0 ||
		len(m.members.Witnesses) != 0 {
		t.Errorf("unexpected data")
	}
}

func TestMembershipCanBeSet(t *testing.T) {
	m := pb.Membership{
		ConfigChangeId: 101,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		Observers:      make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
	}
	m.Addresses[1] = "addr1"
	m.Removed[2] = true
	m.Observers[3] = "addr2"
	m.Witnesses[4] = "addr3"
	o := newMembership(1, 2, true)
	o.set(m)
	if len(o.members.Addresses) != 1 ||
		len(o.members.Observers) != 1 ||
		len(o.members.Removed) != 1 ||
		len(o.members.Witnesses) != 1 ||
		o.members.ConfigChangeId != 101 {
		t.Errorf("membership not set")
	}
	m.ConfigChangeId = 200
	m.Addresses[5] = "addr4"
	if o.members.ConfigChangeId != 101 || len(o.members.Addresses) != 1 {
		t.Fatalf("membership changed")
	}
}

func TestMembershipIsEmpty(t *testing.T) {
	o := newMembership(1, 2, true)
	if !o.isEmpty() {
		t.Errorf("not marked as empty")
	}
	o.members.Observers[1] = "addr1"
	if !o.isEmpty() {
		t.Errorf("not marked as empty")
	}
	o.members.Addresses[1] = "addr2"
	if o.isEmpty() {
		t.Errorf("still marked as empty")
	}
}

func TestIsDeletingOnlyNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[1] = "a1"
	cc := pb.ConfigChange{
		Type:   pb.RemoveNode,
		NodeID: 1,
	}
	cc2 := pb.ConfigChange{
		Type:   pb.AddNode,
		NodeID: 1,
	}
	if !o.isDeletingOnlyNode(cc) {
		t.Errorf("not considered as deleting only node")
	}
	if o.isDeletingOnlyNode(cc2) {
		t.Errorf("not even a delete node op")
	}
	o.members.Observers[2] = "a2"
	if !o.isDeletingOnlyNode(cc) {
		t.Errorf("not considered as deleting only node")
	}
	o.members.Addresses[3] = "a3"
	if o.isDeletingOnlyNode(cc) {
		t.Errorf("still considered as deleting only node")
	}
}

func TestIsAddingRemovedNode(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{}
	cc.Type = pb.AddNode
	cc.NodeID = 1
	if o.isAddingRemovedNode(cc) {
		t.Errorf("incorrect result")
	}
	cc.Type = pb.AddObserver
	if o.isAddingRemovedNode(cc) {
		t.Errorf("incorrect result")
	}
	cc.Type = pb.RemoveNode
	if o.isAddingRemovedNode(cc) {
		t.Errorf("incorrect result")
	}
	cc.Type = pb.AddWitness
	if o.isAddingRemovedNode(cc) {
		t.Errorf("incorrect result")
	}
	o.members.Removed[1] = true
	cc.Type = pb.AddNode
	if !o.isAddingRemovedNode(cc) {
		t.Errorf("not rejected")
	}
	cc.Type = pb.AddWitness
	if !o.isAddingRemovedNode(cc) {
		t.Errorf("not rejected")
	}
	cc.Type = pb.AddObserver
	cc.NodeID = 2
	if o.isAddingRemovedNode(cc) {
		t.Errorf("incorrectly rejected")
	}
}

func TestIsAddingNodeAsObserver(t *testing.T) {
	tests := []struct {
		t      pb.ConfigChangeType
		nodeID uint64
		addrs  []uint64
		result bool
	}{
		{pb.AddNode, 1, []uint64{1}, false},
		{pb.AddNode, 1, []uint64{}, false},
		{pb.AddWitness, 1, []uint64{1}, false},
		{pb.AddWitness, 1, []uint64{}, false},
		{pb.RemoveNode, 1, []uint64{1}, false},
		{pb.RemoveNode, 1, []uint64{}, false},
		{pb.AddObserver, 1, []uint64{1}, true},
		{pb.AddObserver, 1, []uint64{1, 2}, true},
		{pb.AddObserver, 1, []uint64{2}, false},
		{pb.AddObserver, 1, []uint64{}, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		cc := pb.ConfigChange{
			Type:   tt.t,
			NodeID: tt.nodeID,
		}
		for _, v := range tt.addrs {
			o.members.Addresses[v] = "addr"
		}
		result := o.isAddingNodeAsObserver(cc)
		if result != tt.result {
			t.Errorf("%d failed", idx)
		}
	}
}

func TestIsConfChangeUpToDate(t *testing.T) {
	tests := []struct {
		ordered    bool
		initialize bool
		ccid       uint64
		iccid      uint64
		result     bool
	}{
		{true, true, 1, 1, true},
		{true, false, 1, 1, true},
		{false, false, 1, 1, true},
		{false, true, 1, 1, true},
		{true, true, 1, 2, true},
		{true, false, 1, 2, false},
		{false, false, 1, 2, true},
		{false, true, 1, 2, true},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, tt.ordered)
		o.members.ConfigChangeId = tt.ccid
		cc := pb.ConfigChange{
			Initialize:     tt.initialize,
			ConfigChangeId: tt.iccid,
		}
		if result := o.isConfChangeUpToDate(cc); result != tt.result {
			t.Errorf("%d, got %t, want %t", idx, result, tt.result)
		}
	}
}

func TestIsAddingExistingMember(t *testing.T) {
	tests := []struct {
		t         pb.ConfigChangeType
		addrs     map[uint64]string
		observers map[uint64]string
		addr      string
		nodeID    uint64
		result    bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a3", 1, true},
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a1", 1, true},
		{pb.AddNode, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 2, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a3", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a1", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 2, true},
		{pb.AddObserver, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddObserver, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddObserver, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a4", 2, true},
		{pb.AddObserver, map[uint64]string{1: "a1"}, map[uint64]string{2: "a2"}, "a2", 2, true},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.addrs {
			o.members.Addresses[i] = v
		}
		for i, v := range tt.observers {
			o.members.Observers[i] = v
		}
		cc := pb.ConfigChange{
			Type:    tt.t,
			Address: tt.addr,
			NodeID:  tt.nodeID,
		}
		if result := o.isAddingExistingMember(cc); result != tt.result {
			t.Errorf("%d, got %t, want %t", idx, result, tt.result)
		}
	}
}

func TestIsPromotingObserver(t *testing.T) {
	tests := []struct {
		t         pb.ConfigChangeType
		observers map[uint64]string
		addr      string
		nodeID    uint64
		result    bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a1", 1, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.observers {
			o.members.Observers[i] = v
		}
		cc := pb.ConfigChange{
			Type:    tt.t,
			Address: tt.addr,
			NodeID:  tt.nodeID,
		}
		if result := o.isPromotingObserver(cc); result != tt.result {
			t.Errorf("%d, got %t, want %t", idx, result, tt.result)
		}
	}
}

func TestIsInvalidObserverPromotion(t *testing.T) {
	tests := []struct {
		t         pb.ConfigChangeType
		observers map[uint64]string
		addr      string
		nodeID    uint64
		result    bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a2", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddObserver, map[uint64]string{1: "a1"}, "a1", 1, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.observers {
			o.members.Observers[i] = v
		}
		cc := pb.ConfigChange{
			Type:    tt.t,
			Address: tt.addr,
			NodeID:  tt.nodeID,
		}
		if result := o.isInvalidObserverPromotion(cc); result != tt.result {
			t.Errorf("%d, got %t, want %t", idx, result, tt.result)
		}
	}
}

func TestApplyAddNode(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{
		Type:    pb.AddNode,
		Address: "a1",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
	if o.members.ConfigChangeId != 1000 {
		t.Errorf("ccid not updated")
	}
	v, ok := o.members.Addresses[100]
	if !ok || v != "a1" || len(o.members.Addresses) != 1 {
		t.Errorf("node not added")
	}
}

func TestAddNodeCanPromoteObserverToNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Observers[100] = "a2"
	cc := pb.ConfigChange{
		Type:    pb.AddNode,
		Address: "a2",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
	v, ok := o.members.Addresses[100]
	if !ok || v != "a2" || len(o.members.Addresses) != 1 {
		t.Errorf("node not added")
	}
	_, ok = o.members.Observers[100]
	if ok {
		t.Errorf("promoted observer not removed")
	}
}

func TestApplyAddObserver(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{
		Type:    pb.AddObserver,
		Address: "a1",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
	if o.members.ConfigChangeId != 1000 {
		t.Errorf("ccid not updated")
	}
	v, ok := o.members.Observers[100]
	if !ok || v != "a1" || len(o.members.Observers) != 1 {
		t.Errorf("node not added")
	}
}

func TestAddingExistingNodeAsObserverIsNotAllowed(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:    pb.AddObserver,
		Address: "a1",
		NodeID:  100,
	}
	if o.handleConfigChange(cc, 0) {
		t.Errorf("ading existing node as observer is not rejected")
	}
}

func TestAddingExistingNodeAsObserverWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("not panic")
		}
	}()
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:    pb.AddObserver,
		Address: "a1",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
}

func TestAddingExistingNodeAsWitnessWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("not panic")
		}
	}()
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:    pb.AddWitness,
		Address: "a1",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
}

func TestAddingExistingObserverAsWitnessWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("not panic")
		}
	}()
	o := newMembership(1, 2, true)
	o.members.Observers[100] = "a1"
	cc := pb.ConfigChange{
		Type:    pb.AddWitness,
		Address: "a1",
		NodeID:  100,
	}
	o.applyConfigChange(cc, 1000)
}

func TestApplyRemoveNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	o.members.Observers[100] = "a1"
	o.members.Witnesses[100] = "a1"
	cc := pb.ConfigChange{
		Type:   pb.RemoveNode,
		NodeID: 100,
	}
	o.applyConfigChange(cc, 1000)
	if len(o.members.Addresses) != 0 ||
		len(o.members.Observers) != 0 ||
		len(o.members.Witnesses) != 0 {
		t.Errorf("node not removed")
	}
	_, ok := o.members.Removed[100]
	if !ok {
		t.Errorf("not recorded as removed")
	}
}
