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

package logdb

import (
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

func TestLogReaderNewLogReader(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	require.Equal(t, uint64(1), lr.length)
}

func TestInitialState(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	lr.SetCompactor(testCompactor)
	ps := pb.State{
		Term:   100,
		Vote:   112,
		Commit: 123,
	}
	lr.SetState(ps)
	ss := pb.Snapshot{
		Index: 123,
		Term:  124,
		Membership: pb.Membership{
			ConfigChangeId: 1234,
			Addresses:      make(map[uint64]string),
		},
	}
	ss.Membership.Addresses[123] = "address123"
	ss.Membership.Addresses[234] = "address234"
	err := lr.CreateSnapshot(ss)
	require.NoError(t, err)
	rps, m := lr.NodeState()
	require.Equal(t, &ss.Membership, &m)
	require.Equal(t, &rps, &ps)
}

func TestApplySnapshotUpdateMarkerIndexTerm(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	lr.SetCompactor(testCompactor)
	ss := pb.Snapshot{
		Index: 123,
		Term:  124,
		Membership: pb.Membership{
			ConfigChangeId: 1234,
			Addresses:      make(map[uint64]string),
		},
	}
	ss.Membership.Addresses[123] = "address123"
	ss.Membership.Addresses[234] = "address234"
	err := lr.ApplySnapshot(ss)
	require.NoError(t, err)
	require.Equal(t, uint64(123), lr.markerIndex)
	require.Equal(t, uint64(124), lr.markerTerm)
	require.Equal(t, uint64(1), lr.length)
	require.Equal(t, ss.Index, lr.snapshot.Index)
	rs := lr.Snapshot()
	require.Equal(t, ss.Index, rs.Index)
}

func TestLogReaderIndexRange(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	lr.SetCompactor(testCompactor)
	ss := pb.Snapshot{
		Index: 123,
		Term:  124,
		Membership: pb.Membership{
			ConfigChangeId: 1234,
			Addresses:      make(map[uint64]string),
		},
	}
	ss.Membership.Addresses[123] = "address123"
	ss.Membership.Addresses[234] = "address234"
	err := lr.ApplySnapshot(ss)
	require.NoError(t, err)
	first := lr.firstIndex()
	require.Equal(t, uint64(124), first)
	last := lr.lastIndex()
	require.Equal(t, uint64(123), last)
	// last < first here, see internal/raft/log.go for details
	fi, li := lr.GetRange()
	require.Equal(t, first, fi)
	require.Equal(t, last, li)
}

func TestSetRange(t *testing.T) {
	tests := []struct {
		marker    uint64
		length    uint64
		index     uint64
		idxLength uint64
		expLength uint64
	}{
		{1, 10, 1, 1, 10},
		{1, 10, 1, 0, 10},
		{10, 10, 8, 10, 8},
		{10, 10, 20, 10, 20},
	}
	for idx, tt := range tests {
		lr := LogReader{
			markerIndex: tt.marker,
			length:      tt.length,
		}
		lr.SetRange(tt.index, tt.idxLength)
		require.Equal(t, tt.expLength, lr.length,
			"test case %d: unexpected length", idx)
	}
}

func TestSetRangePanicWhenThereIsIndexHole(t *testing.T) {
	lr := LogReader{
		markerIndex: 10,
		length:      10,
	}
	require.Panics(t, func() {
		lr.SetRange(100, 100)
	})
}
