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
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryKeysOrdered(t *testing.T) {
	p := newLogDBKeyPool()
	for i := uint64(0); i < 65536+10; i++ {
		k1 := p.get()
		k1.SetEntryKey(100, 100, i)
		k2 := p.get()
		k2.SetEntryKey(100, 100, i+1)
		require.True(t, bytes.Compare(k1.Key(), k2.Key()) < 0)
	}
	k1 := p.get()
	k1.SetEntryKey(100, 100, 0)
	k2 := p.get()
	k2.SetEntryKey(100, 100, 1)
	k3 := p.get()
	k3.SetEntryKey(100, 100, math.MaxUint64)
	require.True(t, bytes.Compare(k1.Key(), k2.Key()) < 0)
	require.True(t, bytes.Compare(k2.Key(), k3.Key()) < 0)
}

func TestSnapshotKeysOrdered(t *testing.T) {
	p := newLogDBKeyPool()
	k1 := p.get()
	k1.setSnapshotKey(100, 100, 0)
	k2 := p.get()
	k2.setSnapshotKey(100, 100, 1)
	require.True(t, bytes.Compare(k1.Key(), k2.Key()) < 0)
}

func TestNodeInfoKeyCanBeParsed(t *testing.T) {
	p := newLogDBKeyPool()
	for i := 0; i < 1024; i++ {
		k1 := p.get()
		cid := rand.Uint64()
		nid := rand.Uint64()
		k1.setNodeInfoKey(cid, nid)
		v1, v2 := parseNodeInfoKey(k1.Key())
		require.Equal(t, cid, v1)
		require.Equal(t, nid, v2)
	}
}
