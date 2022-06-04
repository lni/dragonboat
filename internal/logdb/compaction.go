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

package logdb

import (
	"sync"

	"github.com/lni/dragonboat/v4/raftio"
)

type task struct {
	done      chan struct{}
	shardID   uint64
	replicaID uint64
	index     uint64
}

type compactionInfo struct {
	done  chan struct{}
	index uint64
}

type compactions struct {
	pendings map[raftio.NodeInfo]compactionInfo
	mu       sync.Mutex
}

func newCompactions() *compactions {
	return &compactions{
		pendings: make(map[raftio.NodeInfo]compactionInfo),
	}
}

func (p *compactions) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pendings)
}

func (p *compactions) addTask(task task) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := raftio.NodeInfo{
		ShardID:   task.shardID,
		ReplicaID: task.replicaID,
	}
	ci := compactionInfo{index: task.index}
	v, ok := p.pendings[key]
	if ok && v.index > task.index {
		panic("existing index > task.index")
	}
	if ok {
		ci.done = v.done
	} else {
		ci.done = make(chan struct{})
	}
	p.pendings[key] = ci
	return ci.done
}

func (p *compactions) getTask() (task, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.pendings {
		task := task{
			shardID:   k.ShardID,
			replicaID: k.ReplicaID,
			index:     v.index,
			done:      v.done,
		}
		delete(p.pendings, k)
		return task, true
	}
	return task{}, false
}
