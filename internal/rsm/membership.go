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
	"crypto/md5"
	"encoding/binary"
	"sort"
	"strings"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

func addressEqual(addr1 string, addr2 string) bool {
	return strings.EqualFold(strings.TrimSpace(addr1),
		strings.TrimSpace(addr2))
}

func deepCopyMembership(m pb.Membership) pb.Membership {
	c := pb.Membership{
		ConfigChangeId: m.ConfigChangeId,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		NonVotings:     make(map[uint64]string),
		Witnesses:      make(map[uint64]string),
	}
	for nid, addr := range m.Addresses {
		c.Addresses[nid] = addr
	}
	for nid := range m.Removed {
		c.Removed[nid] = true
	}
	for nid, addr := range m.NonVotings {
		c.NonVotings[nid] = addr
	}
	for nid, addr := range m.Witnesses {
		c.Witnesses[nid] = addr
	}
	return c
}

type membership struct {
	members   pb.Membership
	shardID   uint64
	replicaID uint64
	ordered   bool
}

func newMembership(shardID uint64, replicaID uint64, ordered bool) membership {
	return membership{
		shardID:   shardID,
		replicaID: replicaID,
		ordered:   ordered,
		members: pb.Membership{
			Addresses:  make(map[uint64]string),
			NonVotings: make(map[uint64]string),
			Removed:    make(map[uint64]bool),
			Witnesses:  make(map[uint64]string),
		},
	}
}

func (m *membership) id() string {
	return logutil.DescribeSM(m.shardID, m.replicaID)
}

func (m *membership) set(n pb.Membership) {
	m.members = deepCopyMembership(n)
}

func (m *membership) get() pb.Membership {
	return deepCopyMembership(m.members)
}

func (m *membership) getHash() uint64 {
	vals := make([]uint64, 0)
	for v := range m.members.Addresses {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	vals = append(vals, m.members.ConfigChangeId)
	data := make([]byte, 8)
	hash := md5.New()
	for _, v := range vals {
		binary.LittleEndian.PutUint64(data, v)
		fileutil.MustWrite(hash, data)
	}
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8])
}

func (m *membership) isEmpty() bool {
	return len(m.members.Addresses) == 0
}

func (m *membership) isUpToDate(cc pb.ConfigChange) bool {
	if !m.ordered || cc.Initialize {
		return true
	}
	if m.members.ConfigChangeId == cc.ConfigChangeId {
		return true
	}
	return false
}

func (m *membership) isAddRemovedNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode ||
		cc.Type == pb.AddNonVoting ||
		cc.Type == pb.AddWitness {
		_, ok := m.members.Removed[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) isPromoteNonVoting(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		oa, ok := m.members.NonVotings[cc.ReplicaID]
		return ok && addressEqual(oa, cc.Address)
	}
	return false
}

func (m *membership) isInvalidNonVotingPromotion(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		oa, ok := m.members.NonVotings[cc.ReplicaID]
		return ok && !addressEqual(oa, cc.Address)
	}
	return false
}

func (m *membership) isAddExistingMember(cc pb.ConfigChange) bool {
	// try to add again with the same node ID
	if cc.Type == pb.AddNode {
		_, ok := m.members.Addresses[cc.ReplicaID]
		if ok {
			return true
		}
	}
	if cc.Type == pb.AddNonVoting {
		_, ok := m.members.NonVotings[cc.ReplicaID]
		if ok {
			return true
		}
	}
	if cc.Type == pb.AddWitness {
		_, ok := m.members.Witnesses[cc.ReplicaID]
		if ok {
			return true
		}
	}
	if m.isPromoteNonVoting(cc) {
		return false
	}
	if cc.Type == pb.AddNode ||
		cc.Type == pb.AddNonVoting ||
		cc.Type == pb.AddWitness {
		for _, addr := range m.members.Addresses {
			if addressEqual(addr, cc.Address) {
				return true
			}
		}
		for _, addr := range m.members.NonVotings {
			if addressEqual(addr, cc.Address) {
				return true
			}
		}
		for _, addr := range m.members.Witnesses {
			if addressEqual(addr, cc.Address) {
				return true
			}
		}
	}
	return false
}

func (m *membership) isAddNodeAsNonVoting(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNonVoting {
		_, ok := m.members.Addresses[cc.ReplicaID]
		return ok
	}
	return false
}
func (m *membership) isAddNodeAsWitness(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddWitness {
		_, ok := m.members.Addresses[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) isAddWitnessAsNonVoting(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNonVoting {
		_, ok := m.members.Witnesses[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) isAddWitnessAsNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		_, ok := m.members.Witnesses[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) isAddNonVotingAsWitness(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddWitness {
		_, ok := m.members.NonVotings[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) isDeleteOnlyNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.RemoveNode && len(m.members.Addresses) == 1 {
		_, ok := m.members.Addresses[cc.ReplicaID]
		return ok
	}
	return false
}

func (m *membership) apply(cc pb.ConfigChange, index uint64) {
	m.members.ConfigChangeId = index
	switch cc.Type {
	case pb.AddNode:
		nodeAddr := cc.Address
		delete(m.members.NonVotings, cc.ReplicaID)
		if _, ok := m.members.Witnesses[cc.ReplicaID]; ok {
			panic("not suppose to reach here")
		}
		m.members.Addresses[cc.ReplicaID] = nodeAddr
	case pb.AddNonVoting:
		if _, ok := m.members.Addresses[cc.ReplicaID]; ok {
			panic("not suppose to reach here")
		}
		m.members.NonVotings[cc.ReplicaID] = cc.Address
	case pb.AddWitness:
		if _, ok := m.members.Addresses[cc.ReplicaID]; ok {
			panic("not suppose to reach here")
		}
		if _, ok := m.members.NonVotings[cc.ReplicaID]; ok {
			panic("not suppose to reach here")
		}
		m.members.Witnesses[cc.ReplicaID] = cc.Address
	case pb.RemoveNode:
		delete(m.members.Addresses, cc.ReplicaID)
		delete(m.members.NonVotings, cc.ReplicaID)
		delete(m.members.Witnesses, cc.ReplicaID)
		m.members.Removed[cc.ReplicaID] = true
	default:
		panic("unknown config change type")
	}
}

var nid = logutil.ReplicaID

func (m *membership) handleConfigChange(cc pb.ConfigChange, index uint64) bool {
	// order id requested by user
	ccid := cc.ConfigChangeId
	nodeBecomingNonVoting := m.isAddNodeAsNonVoting(cc)
	nodeBecomingWitness := m.isAddNodeAsWitness(cc)
	witnessBecomingNode := m.isAddWitnessAsNode(cc)
	witnessBecomingNonVoting := m.isAddWitnessAsNonVoting(cc)
	nonVotingBecomingWitness := m.isAddNonVotingAsWitness(cc)
	alreadyMember := m.isAddExistingMember(cc)
	addRemovedNode := m.isAddRemovedNode(cc)
	upToDateCC := m.isUpToDate(cc)
	deleteOnlyNode := m.isDeleteOnlyNode(cc)
	invalidPromotion := m.isInvalidNonVotingPromotion(cc)
	accepted := upToDateCC &&
		!addRemovedNode &&
		!alreadyMember &&
		!nodeBecomingNonVoting &&
		!nodeBecomingWitness &&
		!witnessBecomingNode &&
		!witnessBecomingNonVoting &&
		!nonVotingBecomingWitness &&
		!deleteOnlyNode &&
		!invalidPromotion
	if accepted {
		// current entry index, it will be recorded as the conf change id of the members
		m.apply(cc, index)
		if cc.Type == pb.AddNode {
			plog.Infof("%s applied ADD ccid %d (%d), %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if cc.Type == pb.RemoveNode {
			plog.Infof("%s applied REMOVE ccid %d (%d), %s",
				m.id(), ccid, index, nid(cc.ReplicaID))
		} else if cc.Type == pb.AddNonVoting {
			plog.Infof("%s applied ADD OBSERVER ccid %d (%d), %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if cc.Type == pb.AddWitness {
			plog.Infof("%s applied ADD WITNESS ccid %d (%d), %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else {
			plog.Panicf("unknown cc.Type value %d", cc.Type)
		}
	} else {
		if !upToDateCC {
			plog.Warningf("%s rej out-of-order ConfChange ccid %d (%d), type %s",
				m.id(), ccid, index, cc.Type)
		} else if addRemovedNode {
			plog.Warningf("%s rej add removed ccid %d (%d), %s",
				m.id(), ccid, index, nid(cc.ReplicaID))
		} else if alreadyMember {
			plog.Warningf("%s rej add exist ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if nodeBecomingNonVoting {
			plog.Warningf("%s rej add exist as nonVoting ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if nodeBecomingWitness {
			plog.Warningf("%s rej add exist as witness ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if witnessBecomingNode {
			plog.Warningf("%s rej add witness as node ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if witnessBecomingNonVoting {
			plog.Warningf("%s rej add witness as nonVoting ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if nonVotingBecomingWitness {
			plog.Warningf("%s rej add nonVoting as witness ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else if deleteOnlyNode {
			plog.Warningf("%s rej remove the only node %s", m.id(), nid(cc.ReplicaID))
		} else if invalidPromotion {
			plog.Warningf("%s rej invalid nonVoting promotion ccid %d (%d) %s (%s)",
				m.id(), ccid, index, nid(cc.ReplicaID), cc.Address)
		} else {
			plog.Panicf("config change rejected for unknown reasons")
		}
	}
	return accepted
}
