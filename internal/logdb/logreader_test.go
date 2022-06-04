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
	"reflect"
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestLogReaderNewLogReader(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	if lr.length != 1 {
		t.Errorf("unexpected length")
	}
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
	if err := lr.CreateSnapshot(ss); err != nil {
		t.Fatalf("%v", err)
	}
	rps, m := lr.NodeState()
	if !reflect.DeepEqual(&ss.Membership, &m) ||
		!reflect.DeepEqual(&rps, &ps) {
		t.Errorf("unexpected state")
	}
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
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("%v", err)
	}
	if lr.markerIndex != 123 || lr.markerTerm != 124 {
		t.Errorf("unexpected marker index/term, %d/%d",
			lr.markerIndex, lr.markerTerm)
	}
	if lr.length != 1 {
		t.Errorf("unexpected length %d", lr.length)
	}
	if ss.Index != lr.snapshot.Index {
		t.Errorf("snapshot not updated")
	}
	rs := lr.Snapshot()
	if ss.Index != rs.Index {
		t.Errorf("snapshot not updated")
	}
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
	if err := lr.ApplySnapshot(ss); err != nil {
		t.Fatalf("%v", err)
	}
	first := lr.firstIndex()
	if first != 124 {
		t.Errorf("unexpected first index %d", first)
	}
	last := lr.lastIndex()
	if last != 123 {
		t.Errorf("unexpected last index %d", last)
	}
	// last < first here, see internal/raft/log.go for details
	fi, li := lr.GetRange()
	if fi != first || li != last {
		t.Errorf("unexpected index")
	}
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
		if lr.length != tt.expLength {
			t.Errorf("%d, unexpected length %d, want %d", idx, lr.length, tt.expLength)
		}
	}
}

func TestSetRangePanicWhenThereIsIndexHole(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	lr := LogReader{
		markerIndex: 10,
		length:      10,
	}
	lr.SetRange(100, 100)
}
