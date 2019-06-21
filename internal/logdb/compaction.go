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

package logdb

import (
	"sync"

	"github.com/lni/dragonboat/v3/raftio"
)

type task struct {
	clusterID uint64
	nodeID    uint64
	index     uint64
}

type compactions struct {
	mu       sync.Mutex
	pendings map[raftio.NodeInfo]uint64
}

func newCompactions() *compactions {
	return &compactions{
		pendings: make(map[raftio.NodeInfo]uint64),
	}
}

func (p *compactions) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pendings)
}

func (p *compactions) addTask(task task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := raftio.NodeInfo{
		ClusterID: task.clusterID,
		NodeID:    task.nodeID,
	}
	v, ok := p.pendings[key]
	if ok && v > task.index {
		panic("existing index > task.index")
	}
	p.pendings[key] = task.index
}

func (p *compactions) getTask() (task, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hasTask := false
	task := task{}
	for k, v := range p.pendings {
		task.clusterID = k.ClusterID
		task.nodeID = k.NodeID
		task.index = v
		hasTask = true
		break
	}
	if hasTask {
		key := raftio.NodeInfo{
			ClusterID: task.clusterID,
			NodeID:    task.nodeID,
		}
		delete(p.pendings, key)
	}
	return task, hasTask
}
