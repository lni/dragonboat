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

package tests

import (
	"bytes"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestSimDiskSMCanBeOpened(t *testing.T) {
	sm := NewSimDiskSM(102)
	v, err := sm.Open(nil)
	require.NoError(t, err, "open failed")
	require.Equal(t, uint64(102), v, "unexpected return value")
}

func TestSimDiskSMCanBeUpdatedAndQueried(t *testing.T) {
	m := NewSimDiskSM(1)
	ents := []sm.Entry{{Index: 2}, {Index: 3}, {Index: 4}}
	_, _ = m.Update(ents)
	require.Equal(t, uint64(4), m.applied, "sm not updated")
	v, err := m.Lookup(nil)
	require.NoError(t, err, "lookup failed")
	require.Equal(t, uint64(4), v.(uint64), "unexpected result")
}

func TestSimDiskSnapshotWorks(t *testing.T) {
	m := NewSimDiskSM(1)
	ents := []sm.Entry{{Index: 2}, {Index: 3}, {Index: 4}}
	_, _ = m.Update(ents)
	require.Equal(t, uint64(4), m.applied, "sm not updated")
	ctx, err := m.PrepareSnapshot()
	require.NoError(t, err, "prepare snapshot failed")
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	err = m.SaveSnapshot(ctx, buf, nil)
	require.NoError(t, err, "save snapshot failed")
	reader := bytes.NewBuffer(buf.Bytes())
	m2 := NewSimDiskSM(100)
	v, err := m2.Lookup(nil)
	require.NoError(t, err, "lookup failed")
	require.Equal(t, uint64(100), v.(uint64), "unexpected result")
	err = m2.RecoverFromSnapshot(reader, nil)
	require.NoError(t, err, "recover from snapshot failed")
	v, err = m2.Lookup(nil)
	require.NoError(t, err, "lookup failed")
	require.Equal(t, uint64(4), v.(uint64), "unexpected result")
}
