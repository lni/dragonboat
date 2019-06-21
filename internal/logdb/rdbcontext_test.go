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
	"testing"

	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

func TestRDBContextCanBeCreated(t *testing.T) {
	ctx := newRDBContext(128, kv.NewSimpleWriteBatch())
	if ctx.key == nil || len(ctx.val) != 128 {
		t.Errorf("unexpected key/value")
	}
	if ctx.wb.Count() != 0 {
		t.Errorf("wb not empty")
	}
}

func TestRDBContextCaBeDestroyed(t *testing.T) {
	ctx := newRDBContext(128, kv.NewSimpleWriteBatch())
	ctx.Destroy()
}

func TestRDBContextCaBeReset(t *testing.T) {
	ctx := newRDBContext(128, kv.NewSimpleWriteBatch())
	ctx.wb.Put([]byte("key"), []byte("val"))
	if ctx.wb.Count() != 1 {
		t.Errorf("unexpected count")
	}
	ctx.Reset()
	if ctx.wb.Count() != 0 {
		t.Errorf("wb not cleared")
	}
}

func TestGetValueBuffer(t *testing.T) {
	ctx := newRDBContext(128, kv.NewSimpleWriteBatch())
	buf := ctx.GetValueBuffer(100)
	if cap(buf) != 128 {
		t.Errorf("didn't return the default buffer")
	}
	buf = ctx.GetValueBuffer(1024)
	if cap(buf) != 1024 {
		t.Errorf("didn't return a new buffer")
	}
}

func TestGetUpdates(t *testing.T) {
	ctx := newRDBContext(128, kv.NewSimpleWriteBatch())
	v := ctx.GetUpdates()
	if cap(v) != updateSliceLen {
		t.Errorf("unexpected updates cap")
	}
	if len(v) != 0 {
		t.Errorf("unexpected len")
	}
	v2 := append(v, pb.Update{})
	if len(v2) != 1 {
		t.Errorf("unexpected len")
	}
	v = ctx.GetUpdates()
	if cap(v) != updateSliceLen {
		t.Errorf("unexpected updates cap")
	}
	if len(v) != 0 {
		t.Errorf("unexpected len")
	}
}
