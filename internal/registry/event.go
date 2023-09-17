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
	"github.com/hashicorp/memberlist"
)

// sliceEventDelegate is used to hook into memberlist to get notification
// about nodes joining and leaving.
type sliceEventDelegate struct {
	store *metaStore
}

var _ memberlist.EventDelegate = (*sliceEventDelegate)(nil)

func newSliceEventDelegate(store *metaStore) *sliceEventDelegate {
	return &sliceEventDelegate{
		store: store,
	}
}

func (e *sliceEventDelegate) put(eventType memberlist.NodeEventType,
	n *memberlist.Node) {
	if eventType == memberlist.NodeJoin || eventType == memberlist.NodeUpdate {
		var m meta
		if m.unmarshal(n.Meta) {
			e.store.put(n.Name, m)
		}
	} else if eventType == memberlist.NodeLeave {
		e.store.delete(n.Name)
	} else {
		panic("unknown event type")
	}
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
