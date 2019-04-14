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

package dragonboat

import (
	pb "github.com/lni/dragonboat/raftpb"
	sm "github.com/lni/dragonboat/statemachine"
)

// as indicated by its name, this is just a proxy type.
type nodeProxy struct {
	rn *node
}

func newNodeProxy(rn *node) *nodeProxy {
	return &nodeProxy{rn: rn}
}

func (n *nodeProxy) ApplyUpdate(ent pb.Entry,
	result sm.Result, rejected bool, ignored bool, notifyReadClient bool) {
	n.rn.applyUpdate(ent, result, rejected, ignored, notifyReadClient)
}

func (n *nodeProxy) ApplyConfigChange(cc pb.ConfigChange) {
	n.rn.applyConfigChange(cc)
}

func (n *nodeProxy) RestoreRemotes(snapshot pb.Snapshot) {
	n.rn.restoreRemotes(snapshot)
}

func (n *nodeProxy) ConfigChangeProcessed(key uint64, accepted bool) {
	if accepted {
		n.rn.pendingConfigChange.apply(key, false)
		n.rn.captureClusterConfig()
	} else {
		n.rn.node.RejectConfigChange()
		n.rn.pendingConfigChange.apply(key, true)
	}
}

func (n *nodeProxy) NodeID() uint64 {
	return n.rn.nodeID
}

func (n *nodeProxy) ClusterID() uint64 {
	return n.rn.clusterID
}
