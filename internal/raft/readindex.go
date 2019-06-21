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

package raft

import (
	"github.com/lni/dragonboat/v3/raftpb"
)

type readStatus struct {
	index     uint64
	from      uint64
	ctx       raftpb.SystemCtx
	confirmed map[uint64]struct{}
}

// readIndex is the struct that implements the ReadIndex protocol described in
// section 6.4 (with the idea in section 6.4.1 excluded) of Diego Ongaro's PhD
// thesis.
type readIndex struct {
	pending map[raftpb.SystemCtx]*readStatus
	queue   []raftpb.SystemCtx
}

func newReadIndex() *readIndex {
	return &readIndex{
		pending: make(map[raftpb.SystemCtx]*readStatus),
		queue:   make([]raftpb.SystemCtx, 0),
	}
}

func (r *readIndex) addRequest(index uint64,
	ctx raftpb.SystemCtx, from uint64) {
	if _, ok := r.pending[ctx]; ok {
		return
	}
	// index is the committed value of the cluster, it should never move
	// backward, check it here
	if len(r.queue) > 0 {
		p, ok := r.pending[r.peepCtx()]
		if !ok {
			panic("inconsistent pending and queue")
		}
		if index < p.index {
			plog.Panicf("index moved backward in readIndex, %d:%d",
				index, p.index)
		}
	}
	r.queue = append(r.queue, ctx)
	r.pending[ctx] = &readStatus{
		index:     index,
		from:      from,
		ctx:       ctx,
		confirmed: make(map[uint64]struct{}),
	}
}

func (r *readIndex) hasPendingRequest() bool {
	return len(r.queue) > 0
}

func (r *readIndex) peepCtx() raftpb.SystemCtx {
	return r.queue[len(r.queue)-1]
}

func (r *readIndex) confirm(ctx raftpb.SystemCtx,
	from uint64, quorum int) []*readStatus {
	p, ok := r.pending[ctx]
	if !ok {
		return nil
	}
	p.confirmed[from] = struct{}{}
	if len(p.confirmed)+1 < quorum {
		return nil
	}
	done := 0
	cs := []*readStatus{}
	for _, pctx := range r.queue {
		done++
		s, ok := r.pending[pctx]
		if !ok {
			panic("inconsistent pending and queue content")
		}
		cs = append(cs, s)
		if pctx == ctx {
			for _, v := range cs {
				if v.index > s.index {
					panic("v.index > s.index is unexpected")
				}
				// re-write the index for extra safety.
				// we don't know what we don't know.
				v.index = s.index
			}
			r.queue = r.queue[done:]
			for _, v := range cs {
				delete(r.pending, v.ctx)
			}
			if len(r.queue) != len(r.pending) {
				panic("inconsistent length")
			}
			return cs
		}
	}
	return nil
}
