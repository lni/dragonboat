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

package rsm

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"strings"

	"github.com/lni/dragonboat/v3/internal/utils/logutil"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

func addressEqual(addr1 string, addr2 string) bool {
	return strings.ToLower(strings.TrimSpace(addr1)) ==
		strings.ToLower(strings.TrimSpace(addr2))
}

func deepCopyMembership(m pb.Membership) pb.Membership {
	c := pb.Membership{
		ConfigChangeId: m.ConfigChangeId,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
		Observers:      make(map[uint64]string),
	}
	for nid, addr := range m.Addresses {
		c.Addresses[nid] = addr
	}
	for nid := range m.Removed {
		c.Removed[nid] = true
	}
	for nid, addr := range m.Observers {
		c.Observers[nid] = addr
	}
	return c
}

type membership struct {
	clusterID uint64
	nodeID    uint64
	ordered   bool
	members   *pb.Membership
}

func newMembership(clusterID uint64, nodeID uint64, ordered bool) *membership {
	return &membership{
		clusterID: clusterID,
		nodeID:    nodeID,
		ordered:   ordered,
		members: &pb.Membership{
			Addresses: make(map[uint64]string),
			Observers: make(map[uint64]string),
			Removed:   make(map[uint64]bool),
		},
	}
}

func (m *membership) id() string {
	return logutil.DescribeSM(m.clusterID, m.nodeID)
}

func (m *membership) set(n pb.Membership) {
	cm := deepCopyMembership(n)
	m.members = &cm
}

func (m *membership) get() (map[uint64]string,
	map[uint64]string, map[uint64]struct{}, uint64) {
	members := make(map[uint64]string)
	observers := make(map[uint64]string)
	removed := make(map[uint64]struct{})
	for nid, addr := range m.members.Addresses {
		members[nid] = addr
	}
	for nid, addr := range m.members.Observers {
		observers[nid] = addr
	}
	for nid := range m.members.Removed {
		removed[nid] = struct{}{}
	}
	return members, observers, removed, m.members.ConfigChangeId
}

func (m *membership) getMembership() pb.Membership {
	return deepCopyMembership(*m.members)
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
		if _, err := hash.Write(data); err != nil {
			panic(err)
		}
	}
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8])
}

func (m *membership) isEmpty() bool {
	return len(m.members.Addresses) == 0
}

func (m *membership) isConfChangeUpToDate(cc pb.ConfigChange) bool {
	if !m.ordered || cc.Initialize {
		return true
	}
	if m.members.ConfigChangeId == cc.ConfigChangeId {
		return true
	}
	return false
}

func (m *membership) isAddingRemovedNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode || cc.Type == pb.AddObserver {
		_, ok := m.members.Removed[cc.NodeID]
		return ok
	}
	return false
}

func (m *membership) isPromotingObserver(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		oa, ok := m.members.Observers[cc.NodeID]
		return ok && addressEqual(oa, string(cc.Address))
	}
	return false
}

func (m *membership) isInvalidObserverPromotion(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddNode {
		oa, ok := m.members.Observers[cc.NodeID]
		return ok && !addressEqual(oa, string(cc.Address))
	}
	return false
}

func (m *membership) isAddingExistingMember(cc pb.ConfigChange) bool {
	// try to add again with the same node ID
	if cc.Type == pb.AddNode {
		_, ok := m.members.Addresses[cc.NodeID]
		if ok {
			return true
		}
	}
	if cc.Type == pb.AddObserver {
		_, ok := m.members.Observers[cc.NodeID]
		if ok {
			return true
		}
	}
	if m.isPromotingObserver(cc) {
		return false
	}
	if cc.Type == pb.AddNode || cc.Type == pb.AddObserver {
		plog.Infof("%s adding node %d:%s, existing members: %v",
			m.id(), cc.NodeID, string(cc.Address), m.members.Addresses)
		for _, addr := range m.members.Addresses {
			if addressEqual(addr, string(cc.Address)) {
				return true
			}
		}
		for _, addr := range m.members.Observers {
			if addressEqual(addr, string(cc.Address)) {
				return true
			}
		}
	}
	return false
}

func (m *membership) isAddingNodeAsObserver(cc pb.ConfigChange) bool {
	if cc.Type == pb.AddObserver {
		_, ok := m.members.Addresses[cc.NodeID]
		return ok
	}
	return false
}

func (m *membership) isDeletingOnlyNode(cc pb.ConfigChange) bool {
	if cc.Type == pb.RemoveNode && len(m.members.Addresses) == 1 {
		_, ok := m.members.Addresses[cc.NodeID]
		return ok
	}
	return false
}

func (m *membership) applyConfigChange(cc pb.ConfigChange, index uint64) {
	m.members.ConfigChangeId = index
	switch cc.Type {
	case pb.AddNode:
		nodeAddr := string(cc.Address)
		if _, ok := m.members.Observers[cc.NodeID]; ok {
			delete(m.members.Observers, cc.NodeID)
		}
		m.members.Addresses[cc.NodeID] = nodeAddr
	case pb.AddObserver:
		if _, ok := m.members.Addresses[cc.NodeID]; ok {
			panic("not suppose to reach here")
		}
		m.members.Observers[cc.NodeID] = string(cc.Address)
	case pb.RemoveNode:
		delete(m.members.Addresses, cc.NodeID)
		delete(m.members.Observers, cc.NodeID)
		m.members.Removed[cc.NodeID] = true
	default:
		panic("unknown config change type")
	}
}

func (m *membership) handleConfigChange(cc pb.ConfigChange, index uint64) bool {
	accepted := false
	// order id requested by user
	ccid := cc.ConfigChangeId
	nodeBecomingObserver := m.isAddingNodeAsObserver(cc)
	alreadyMember := m.isAddingExistingMember(cc)
	addRemovedNode := m.isAddingRemovedNode(cc)
	upToDateCC := m.isConfChangeUpToDate(cc)
	deleteOnlyNode := m.isDeletingOnlyNode(cc)
	invalidPromotion := m.isInvalidObserverPromotion(cc)
	if upToDateCC && !addRemovedNode && !alreadyMember &&
		!nodeBecomingObserver && !deleteOnlyNode && !invalidPromotion {
		// current entry index, it will be recorded as the conf change id of the members
		m.applyConfigChange(cc, index)
		if cc.Type == pb.AddNode {
			plog.Infof("%s applied ConfChange Add ccid %d, node %s index %d address %s",
				m.id(), ccid, logutil.NodeID(cc.NodeID), index, string(cc.Address))
		} else if cc.Type == pb.RemoveNode {
			plog.Infof("%s applied ConfChange Remove ccid %d, node %s, index %d",
				m.id(), ccid, logutil.NodeID(cc.NodeID), index)
		} else if cc.Type == pb.AddObserver {
			plog.Infof("%s applied ConfChange Add Observer ccid %d, node %s index %d address %s",
				m.id(), ccid, logutil.NodeID(cc.NodeID), index, string(cc.Address))
		} else {
			plog.Panicf("unknown cc.Type value")
		}
		accepted = true
	} else {
		if !upToDateCC {
			plog.Warningf("%s rejected out-of-order ConfChange ccid %d, type %s, index %d",
				m.id(), ccid, cc.Type, index)
		} else if addRemovedNode {
			plog.Warningf("%s rejected adding removed node ccid %d, node id %d, index %d",
				m.id(), ccid, cc.NodeID, index)
		} else if alreadyMember {
			plog.Warningf("%s rejected adding existing member to raft cluster ccid %d "+
				"node id %d, index %d, address %s",
				m.id(), ccid, cc.NodeID, index, cc.Address)
		} else if nodeBecomingObserver {
			plog.Warningf("%s rejected adding existing member as observer ccid %d "+
				"node id %d, index %d, address %s",
				m.id(), ccid, cc.NodeID, index, cc.Address)
		} else if deleteOnlyNode {
			plog.Warningf("%s rejected removing the only node %d from the cluster",
				m.id(), cc.NodeID)
		} else if invalidPromotion {
			plog.Warningf("%s rejected invalid observer promotion change ccid %d "+
				"node id %d, index %d, address %s",
				m.id(), ccid, cc.NodeID, index, cc.Address)
		} else {
			plog.Panicf("config change rejected for unknown reasons")
		}
	}
	return accepted
}
