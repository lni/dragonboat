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

package logdb

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

func TestEntryKeysOrdered(t *testing.T) {
	p := newLogdbKeyPool()
	for i := uint64(0); i < 65536+10; i++ {
		k1 := p.get()
		k1.SetEntryKey(100, 100, i)
		k2 := p.get()
		k2.SetEntryKey(100, 100, i+1)
		if bytes.Compare(k1.Key(), k2.Key()) >= 0 {
			t.Errorf("unexpected order, i %d", i)
		}
	}
	k1 := p.get()
	k1.SetEntryKey(100, 100, 0)
	k2 := p.get()
	k2.SetEntryKey(100, 100, 1)
	k3 := p.get()
	k3.SetEntryKey(100, 100, math.MaxUint64)
	if bytes.Compare(k1.Key(), k2.Key()) >= 0 || bytes.Compare(k2.Key(), k3.Key()) >= 0 {
		t.Errorf("unexpected order")
	}
}

func TestSnapshotKeysOrdered(t *testing.T) {
	p := newLogdbKeyPool()
	for i := uint64(0); i < 65536+10; i++ {
		k1 := p.get()
		k1.setSnapshotKey(100, 100, i)
		k2 := p.get()
		k2.setSnapshotKey(100, 100, i+1)
		if bytes.Compare(k1.Key(), k2.Key()) >= 0 {
			t.Errorf("unexpected order, i %d", i)
		}
	}
	k1 := p.get()
	k1.setSnapshotKey(100, 100, 0)
	k2 := p.get()
	k2.setSnapshotKey(100, 100, 1)
	k3 := p.get()
	k3.setSnapshotKey(100, 100, math.MaxUint64)
	if bytes.Compare(k1.Key(), k2.Key()) >= 0 || bytes.Compare(k2.Key(), k3.Key()) >= 0 {
		t.Errorf("unexpected order")
	}
}

func TestNodeInfoKeyCanBeParsed(t *testing.T) {
	p := newLogdbKeyPool()
	for i := 0; i < 1024; i++ {
		k1 := p.get()
		cid := rand.Uint64()
		nid := rand.Uint64()
		k1.setNodeInfoKey(cid, nid)
		v1, v2 := parseNodeInfoKey(k1.Key())
		if v1 != cid || v2 != nid {
			t.Errorf("failed to parse the node info key")
		}
	}
}
