// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package rsm

import (
	"bytes"
	"testing"

	"github.com/lni/dragonboat/v4/internal/tests"
	"github.com/stretchr/testify/require"
)

func TestOnDiskSMCanBeOpened(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	idx, err := od.Open(nil)
	require.NoError(t, err, "failed to open")
	require.Equal(t, applied, idx, "unexpected idx")
}

func TestOnDiskSMCanNotBeOpenedMoreThanOnce(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	idx, err := od.Open(nil)
	require.NoError(t, err, "failed to open")
	require.Equal(t, applied, idx, "unexpected idx")

	require.Panics(t, func() {
		_, err := od.Open(nil)
		require.NoError(t, err, "open failed")
	}, "expected panic when opening twice")
}

func TestLookupCanBeCalledOnceOnDiskSMIsOpened(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	require.NoError(t, err, "failed to open")
	_, err = od.Lookup(nil)
	require.NoError(t, err, "lookup failed")
}

func TestRecoverFromSnapshotCanComplete(t *testing.T) {
	applied := uint64(123)
	fd := tests.NewFakeDiskSM(applied)
	od := NewOnDiskStateMachine(fd)
	_, err := od.Open(nil)
	require.NoError(t, err, "failed to open")
	buf := make([]byte, 16)
	reader := bytes.NewBuffer(buf)
	stopc := make(chan struct{})
	err = od.Recover(reader, nil, stopc)
	require.NoError(t, err, "recover from snapshot failed")
}
