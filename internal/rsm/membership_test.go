// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		assert.Equal(t, tt.equal, result, "test case %d", idx)
	}
}

func TestDeepCopyMembership(t *testing.T) {
	m := pb.Membership{
		ConfigChangeId: 101,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		NonVotings:     make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
	}
	copied := deepCopyMembership(m)
	m.ConfigChangeId = 102
	m.Addresses[1] = "addr1"
	m.Removed[1] = true
	m.NonVotings[1] = "addr1"
	m.Witnesses[1] = "addr1"
	assert.Equal(t, uint64(101), copied.ConfigChangeId)
	assert.Empty(t, copied.Addresses)
	assert.Empty(t, copied.Removed)
	assert.Empty(t, copied.NonVotings)
	assert.Empty(t, copied.Witnesses)

	copied2 := deepCopyMembership(m)
	assert.Equal(t, uint64(102), copied2.ConfigChangeId)
	assert.Len(t, copied2.Addresses, 1)
	assert.Len(t, copied2.Removed, 1)
	assert.Len(t, copied2.NonVotings, 1)
	assert.Len(t, copied2.Witnesses, 1)
}

func TestMembershipCanBeCreated(t *testing.T) {
	m := newMembership(1, 2, true)
	assert.True(t, m.ordered)
	assert.Empty(t, m.members.Addresses)
	assert.Empty(t, m.members.NonVotings)
	assert.Empty(t, m.members.Removed)
	assert.Empty(t, m.members.Witnesses)
}

func TestMembershipCanBeSet(t *testing.T) {
	m := pb.Membership{
		ConfigChangeId: 101,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		NonVotings:     make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
	}
	m.Addresses[1] = "addr1"
	m.Removed[2] = true
	m.NonVotings[3] = "addr2"
	m.Witnesses[4] = "addr3"
	o := newMembership(1, 2, true)
	o.set(m)
	assert.Len(t, o.members.Addresses, 1)
	assert.Len(t, o.members.NonVotings, 1)
	assert.Len(t, o.members.Removed, 1)
	assert.Len(t, o.members.Witnesses, 1)
	assert.Equal(t, uint64(101), o.members.ConfigChangeId)

	m.ConfigChangeId = 200
	m.Addresses[5] = "addr4"
	assert.Equal(t, uint64(101), o.members.ConfigChangeId)
	assert.Len(t, o.members.Addresses, 1)
}

func TestMembershipIsEmpty(t *testing.T) {
	o := newMembership(1, 2, true)
	assert.True(t, o.isEmpty())

	o.members.NonVotings[1] = "addr1"
	assert.True(t, o.isEmpty())

	o.members.Addresses[1] = "addr2"
	assert.False(t, o.isEmpty())
}

func TestIsDeletingOnlyNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[1] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.RemoveNode,
		ReplicaID: 1,
	}
	cc2 := pb.ConfigChange{
		Type:      pb.AddNode,
		ReplicaID: 1,
	}
	assert.True(t, o.isDeleteOnlyNode(cc))
	assert.False(t, o.isDeleteOnlyNode(cc2))

	o.members.NonVotings[2] = "a2"
	assert.True(t, o.isDeleteOnlyNode(cc))

	o.members.Addresses[3] = "a3"
	assert.False(t, o.isDeleteOnlyNode(cc))
}

func TestIsAddingRemovedNode(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{}
	cc.Type = pb.AddNode
	cc.ReplicaID = 1
	assert.False(t, o.isAddRemovedNode(cc))

	cc.Type = pb.AddNonVoting
	assert.False(t, o.isAddRemovedNode(cc))

	cc.Type = pb.RemoveNode
	assert.False(t, o.isAddRemovedNode(cc))

	cc.Type = pb.AddWitness
	assert.False(t, o.isAddRemovedNode(cc))

	o.members.Removed[1] = true
	cc.Type = pb.AddNode
	assert.True(t, o.isAddRemovedNode(cc))

	cc.Type = pb.AddWitness
	assert.True(t, o.isAddRemovedNode(cc))

	cc.Type = pb.AddNonVoting
	cc.ReplicaID = 2
	assert.False(t, o.isAddRemovedNode(cc))
}

func TestIsAddingNodeAsNonVoting(t *testing.T) {
	tests := []struct {
		t         pb.ConfigChangeType
		replicaID uint64
		addrs     []uint64
		result    bool
	}{
		{pb.AddNode, 1, []uint64{1}, false},
		{pb.AddNode, 1, []uint64{}, false},
		{pb.AddWitness, 1, []uint64{1}, false},
		{pb.AddWitness, 1, []uint64{}, false},
		{pb.RemoveNode, 1, []uint64{1}, false},
		{pb.RemoveNode, 1, []uint64{}, false},
		{pb.AddNonVoting, 1, []uint64{1}, true},
		{pb.AddNonVoting, 1, []uint64{1, 2}, true},
		{pb.AddNonVoting, 1, []uint64{2}, false},
		{pb.AddNonVoting, 1, []uint64{}, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		cc := pb.ConfigChange{
			Type:      tt.t,
			ReplicaID: tt.replicaID,
		}
		for _, v := range tt.addrs {
			o.members.Addresses[v] = "addr"
		}
		result := o.isAddNodeAsNonVoting(cc)
		assert.Equal(t, tt.result, result, "test case %d", idx)
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
		result := o.isUpToDate(cc)
		assert.Equal(t, tt.result, result, "test case %d", idx)
	}
}

func TestIsAddingExistingMember(t *testing.T) {
	tests := []struct {
		t          pb.ConfigChangeType
		addrs      map[uint64]string
		nonVotings map[uint64]string
		addr       string
		replicaID  uint64
		result     bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a3", 1, true},
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a1", 1, true},
		{pb.AddNode, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 2, false},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a3", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a1", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 2, true},
		{pb.AddNonVoting, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a1", 3, true},
		{pb.AddNonVoting, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 4, true},
		{pb.AddNonVoting, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a3", 3, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a4", 2, true},
		{pb.AddNonVoting, map[uint64]string{1: "a1"},
			map[uint64]string{2: "a2"}, "a2", 2, true},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.addrs {
			o.members.Addresses[i] = v
		}
		for i, v := range tt.nonVotings {
			o.members.NonVotings[i] = v
		}
		cc := pb.ConfigChange{
			Type:      tt.t,
			Address:   tt.addr,
			ReplicaID: tt.replicaID,
		}
		result := o.isAddExistingMember(cc)
		assert.Equal(t, tt.result, result, "test case %d", idx)
	}
}

func TestIsPromotingNonVoting(t *testing.T) {
	tests := []struct {
		t          pb.ConfigChangeType
		nonVotings map[uint64]string
		addr       string
		replicaID  uint64
		result     bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a1", 1, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.nonVotings {
			o.members.NonVotings[i] = v
		}
		cc := pb.ConfigChange{
			Type:      tt.t,
			Address:   tt.addr,
			ReplicaID: tt.replicaID,
		}
		result := o.isPromoteNonVoting(cc)
		assert.Equal(t, tt.result, result, "test case %d", idx)
	}
}

func TestIsInvalidNonVotingPromotion(t *testing.T) {
	tests := []struct {
		t          pb.ConfigChangeType
		nonVotings map[uint64]string
		addr       string
		replicaID  uint64
		result     bool
	}{
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNode, map[uint64]string{1: "a1"}, "a2", 1, true},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddWitness, map[uint64]string{1: "a1"}, "a1", 1, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a1", 3, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a2", 1, false},
		{pb.AddNonVoting, map[uint64]string{1: "a1"}, "a1", 1, false},
	}
	for idx, tt := range tests {
		o := newMembership(1, 2, true)
		for i, v := range tt.nonVotings {
			o.members.NonVotings[i] = v
		}
		cc := pb.ConfigChange{
			Type:      tt.t,
			Address:   tt.addr,
			ReplicaID: tt.replicaID,
		}
		result := o.isInvalidNonVotingPromotion(cc)
		assert.Equal(t, tt.result, result, "test case %d", idx)
	}
}

func TestApplyAddNode(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{
		Type:      pb.AddNode,
		Address:   "a1",
		ReplicaID: 100,
	}
	o.apply(cc, 1000)
	assert.Equal(t, uint64(1000), o.members.ConfigChangeId)
	v, ok := o.members.Addresses[100]
	assert.True(t, ok)
	assert.Equal(t, "a1", v)
	assert.Len(t, o.members.Addresses, 1)
}

func TestAddNodeCanPromoteNonVotingToNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.NonVotings[100] = "a2"
	cc := pb.ConfigChange{
		Type:      pb.AddNode,
		Address:   "a2",
		ReplicaID: 100,
	}
	o.apply(cc, 1000)
	v, ok := o.members.Addresses[100]
	assert.True(t, ok)
	assert.Equal(t, "a2", v)
	assert.Len(t, o.members.Addresses, 1)
	_, ok = o.members.NonVotings[100]
	assert.False(t, ok)
}

func TestApplyAddNonVoting(t *testing.T) {
	o := newMembership(1, 2, true)
	cc := pb.ConfigChange{
		Type:      pb.AddNonVoting,
		Address:   "a1",
		ReplicaID: 100,
	}
	o.apply(cc, 1000)
	assert.Equal(t, uint64(1000), o.members.ConfigChangeId)
	v, ok := o.members.NonVotings[100]
	assert.True(t, ok)
	assert.Equal(t, "a1", v)
	assert.Len(t, o.members.NonVotings, 1)
}

func TestAddingExistingNodeAsNonVotingIsNotAllowed(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.AddNonVoting,
		Address:   "a1",
		ReplicaID: 100,
	}
	assert.False(t, o.handleConfigChange(cc, 0))
}

func TestAddingExistingNodeAsNonVotingWillPanic(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.AddNonVoting,
		Address:   "a1",
		ReplicaID: 100,
	}
	require.Panics(t, func() {
		o.apply(cc, 1000)
	})
}

func TestAddingExistingNodeAsWitnessWillPanic(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.AddWitness,
		Address:   "a1",
		ReplicaID: 100,
	}
	require.Panics(t, func() {
		o.apply(cc, 1000)
	})
}

func TestAddingExistingNonVotingAsWitnessWillPanic(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.NonVotings[100] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.AddWitness,
		Address:   "a1",
		ReplicaID: 100,
	}
	require.Panics(t, func() {
		o.apply(cc, 1000)
	})
}

func TestApplyRemoveNode(t *testing.T) {
	o := newMembership(1, 2, true)
	o.members.Addresses[100] = "a1"
	o.members.NonVotings[100] = "a1"
	o.members.Witnesses[100] = "a1"
	cc := pb.ConfigChange{
		Type:      pb.RemoveNode,
		ReplicaID: 100,
	}
	o.apply(cc, 1000)
	assert.Empty(t, o.members.Addresses)
	assert.Empty(t, o.members.NonVotings)
	assert.Empty(t, o.members.Witnesses)
	_, ok := o.members.Removed[100]
	assert.True(t, ok)
}
