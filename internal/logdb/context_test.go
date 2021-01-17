// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
)

type kvpair struct {
	delete bool
	key    []byte
	val    []byte
}

type testWriteBatch struct {
	vals []kvpair
}

func newtestWriteBatch() *testWriteBatch {
	return &testWriteBatch{vals: make([]kvpair, 0)}
}

func (wb *testWriteBatch) Destroy() {
	wb.vals = nil
}

func (wb *testWriteBatch) Put(key []byte, val []byte) {
	k := make([]byte, len(key))
	v := make([]byte, len(val))
	copy(k, key)
	copy(v, val)
	wb.vals = append(wb.vals, kvpair{key: k, val: v})
}

func (wb *testWriteBatch) Delete(key []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	wb.vals = append(wb.vals, kvpair{key: k, val: nil, delete: true})
}

func (wb *testWriteBatch) Clear() {
	wb.vals = make([]kvpair, 0)
}

func (wb *testWriteBatch) Count() int {
	return len(wb.vals)
}

func TestRDBContextCanBeCreated(t *testing.T) {
	ctx := newContext(128, 128)
	if ctx.key == nil || len(ctx.val) != 128 {
		t.Errorf("unexpected key/value")
	}
	if ctx.wb != nil {
		t.Errorf("wb not nil")
	}
}

func TestRDBContextCaBeDestroyed(t *testing.T) {
	ctx := newContext(128, 128)
	ctx.Destroy()
}

func TestRDBContextCaBeReset(t *testing.T) {
	ctx := newContext(128, 128)
	ctx.SetWriteBatch(newtestWriteBatch())
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
	ctx := newContext(128, 128)
	buf := ctx.GetValueBuffer(100)
	if cap(buf) != 128 {
		t.Errorf("didn't return the default buffer")
	}
	buf = ctx.GetValueBuffer(1024)
	if cap(buf) != 1024 {
		t.Errorf("didn't return a new buffer")
	}
}
