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

// rdbcontext is an IContext implementation suppose to be owned and used
// by a single thread throughout its life time.
type rdbcontext struct {
	valSize uint64
	eb      pb.EntryBatch
	lb      pb.EntryBatch
	key     *PooledKey
	val     []byte
	updates []pb.Update
	wb      kv.IWriteBatch
}

// newRDBContext creates a new RDB context instance.
func newRDBContext(valSize uint64, wb kv.IWriteBatch) *rdbcontext {
	ctx := &rdbcontext{
		valSize: valSize,
		key:     newKey(maxKeySize, nil),
		val:     make([]byte, valSize),
		updates: make([]pb.Update, 0, updateSliceLen),
		wb:      wb,
	}
	ctx.lb.Entries = make([]pb.Entry, 0, batchSize)
	ctx.eb.Entries = make([]pb.Entry, 0, batchSize)
	return ctx
}

func (c *rdbcontext) Destroy() {
	if c.wb != nil {
		c.wb.Destroy()
	}
}

func (c *rdbcontext) Reset() {
	if c.wb != nil {
		c.wb.Clear()
	}
}

func (c *rdbcontext) GetKey() raftio.IReusableKey {
	return c.key
}

func (c *rdbcontext) GetValueBuffer(sz uint64) []byte {
	if sz <= c.valSize {
		return c.val
	}
	return make([]byte, sz)
}

func (c *rdbcontext) GetUpdates() []pb.Update {
	return c.updates
}

func (c *rdbcontext) GetEntryBatch() pb.EntryBatch {
	return c.eb
}

func (c *rdbcontext) GetLastEntryBatch() pb.EntryBatch {
	return c.lb
}

func (c *rdbcontext) GetWriteBatch() interface{} {
	return c.wb
}
