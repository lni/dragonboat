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
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

const (
	updateSliceLen = 256
)

// context is an IContext implementation suppose to be owned and used
// by a single thread throughout its life time.
type context struct {
	size    uint64
	eb      pb.EntryBatch
	lb      pb.EntryBatch
	key     *Key
	val     []byte
	updates []pb.Update
	wb      kv.IWriteBatch
}

// newContext creates a new RDB context instance.
func newContext(size uint64, wb kv.IWriteBatch) *context {
	ctx := &context{
		size:    size,
		key:     newKey(maxKeySize, nil),
		val:     make([]byte, size),
		updates: make([]pb.Update, 0, updateSliceLen),
		wb:      wb,
	}
	ctx.lb.Entries = make([]pb.Entry, 0, batchSize)
	ctx.eb.Entries = make([]pb.Entry, 0, batchSize)
	return ctx
}

func (c *context) Destroy() {
	if c.wb != nil {
		c.wb.Destroy()
	}
	c.val = nil
	c.updates = nil
	c.lb.Entries = nil
	c.eb.Entries = nil
}

func (c *context) Reset() {
	if c.wb != nil {
		c.wb.Clear()
	}
}

func (c *context) GetKey() raftio.IReusableKey {
	return c.key
}

func (c *context) GetValueBuffer(sz uint64) []byte {
	if sz <= c.size {
		return c.val
	}
	val := make([]byte, sz)
	if sz < RDBContextValueSize {
		c.size = sz
		c.val = val
	}
	return val
}

func (c *context) GetUpdates() []pb.Update {
	return c.updates
}

func (c *context) GetEntryBatch() pb.EntryBatch {
	return c.eb
}

func (c *context) GetLastEntryBatch() pb.EntryBatch {
	return c.lb
}

func (c *context) GetWriteBatch() interface{} {
	return c.wb
}
