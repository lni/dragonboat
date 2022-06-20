// Copyright 2018-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"sync"

	"github.com/hashicorp/memberlist"
)

type sliceEventDelegate struct {
	ch chan struct{}

	mu struct {
		sync.Mutex
		events []memberlist.NodeEvent
	}
}

var _ memberlist.EventDelegate = (*sliceEventDelegate)(nil)

func newSliceEventDelegate() *sliceEventDelegate {
	return &sliceEventDelegate{
		ch: make(chan struct{}, 1),
	}
}

func (e *sliceEventDelegate) notify() {
	select {
	case e.ch <- struct{}{}:
	default:
	}
}

func (e *sliceEventDelegate) get() []memberlist.NodeEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	events := e.mu.events
	e.mu.events = make([]memberlist.NodeEvent, 0)
	return events
}

func (e *sliceEventDelegate) put(typ memberlist.NodeEventType, n *memberlist.Node) {
	node := *n
	node.Meta = make([]byte, len(n.Meta))
	copy(node.Meta, n.Meta)
	e.notify()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.events = append(e.mu.events, memberlist.NodeEvent{typ, &node})
}

func (e *sliceEventDelegate) NotifyJoin(n *memberlist.Node) {
	e.put(memberlist.NodeJoin, n)
}

func (e *sliceEventDelegate) NotifyLeave(n *memberlist.Node) {
	e.put(memberlist.NodeLeave, n)
}

func (e *sliceEventDelegate) NotifyUpdate(n *memberlist.Node) {
	e.put(memberlist.NodeUpdate, n)
}
