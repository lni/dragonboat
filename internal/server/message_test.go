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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

func TestMessageQueueCanBeCreated(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	if len(q.left) != 8 || len(q.right) != 8 {
		t.Errorf("unexpected size")
	}
}

func TestMessageCanBeAddedAndGet(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 8; i++ {
		added, stopped := q.Add(pb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	add, stopped := q.Add(pb.Message{})
	add2, stopped2 := q.Add(pb.Message{})
	if add || add2 {
		t.Errorf("failed to drop message")
	}
	if stopped || stopped2 {
		t.Errorf("unexpectedly stopped")
	}
	if q.idx != 8 {
		t.Errorf("unexpected idx %d", q.idx)
	}
	lr := q.leftInWrite
	q.Get()
	if q.idx != 0 {
		t.Errorf("unexpected idx %d", q.idx)
	}
	if lr == q.leftInWrite {
		t.Errorf("lr flag not updated")
	}
	add, stopped = q.Add(pb.Message{})
	add2, stopped2 = q.Add(pb.Message{})
	if !add || !add2 {
		t.Errorf("failed to add message")
	}
	if stopped || stopped2 {
		t.Errorf("unexpectedly stopped")
	}
}

func TestNonSnapshotMsgByCallingMustAddWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("didn't panic")
	}()
	q := NewMessageQueue(8, false, 0, 0)
	q.MustAdd(pb.Message{})
}

func TestSnapshotCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 1024; i++ {
		n := len(q.nodrop)
		if !q.MustAdd(pb.Message{Type: pb.InstallSnapshot}) {
			t.Errorf("failed to add snapshot")
		}
		if len(q.nodrop) != n+1 {
			t.Errorf("unexpected count")
		}
	}
}

func TestUnreachableMsgCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 1024; i++ {
		n := len(q.nodrop)
		if !q.MustAdd(pb.Message{Type: pb.Unreachable}) {
			t.Errorf("failed to add snapshot")
		}
		if len(q.nodrop) != n+1 {
			t.Errorf("unexpected count")
		}
	}
}

func TestAddedSnapshotWillBeReturned(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	if !q.MustAdd(pb.Message{Type: pb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	if !q.MustAdd(pb.Message{Type: pb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	if !q.MustAdd(pb.Message{Type: pb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	msgs := q.Get()
	if len(msgs) != 11 {
		t.Errorf("failed to return all messages")
	}
	count := 0
	for _, msg := range msgs {
		if msg.Type == pb.InstallSnapshot {
			count++
		}
	}
	if count != 3 {
		t.Errorf("failed to get all snapshot messages")
	}
	if len(q.nodrop) != 0 {
		t.Errorf("snapshot list not empty")
	}
}

func TestMessageQueueCanBeStopped(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	q.Close()
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		if added || !stopped {
			t.Errorf("unexpectedly added msg")
		}
	}
	if q.MustAdd(pb.Message{Type: pb.InstallSnapshot}) {
		t.Errorf("unexpectedly added snapshot")
	}
}

func TestRateLimiterCanBeEnabledInMessageQueue(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	if q.rl.Enabled() {
		t.Errorf("rl unexpectedly enabled")
	}
	q = NewMessageQueue(8, false, 0, 1024)
	if !q.rl.Enabled() {
		t.Errorf("rl not enabled")
	}
}

func TestSingleMessageCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(10000, false, 0, 1024)
	e := pb.Entry{Index: 1, Cmd: make([]byte, 2048)}
	m := pb.Message{
		Type:    pb.Replicate,
		Entries: []pb.Entry{e},
	}
	added, stopped := q.Add(m)
	if !added {
		t.Errorf("not added")
	}
	if stopped {
		t.Errorf("stopped")
	}
	if !q.rl.RateLimited() {
		t.Errorf("not rate limited")
	}
}

func TestAddMessageIsRateLimited(t *testing.T) {
	q := NewMessageQueue(10000, false, 0, 1024)
	for i := 0; i < 10000; i++ {
		e := pb.Entry{Index: uint64(i + 1)}
		m := pb.Message{
			Type:    pb.Replicate,
			Entries: []pb.Entry{e},
		}
		if q.rl.RateLimited() {
			added, stopped := q.Add(m)
			if !added && !stopped {
				return
			}
		} else {
			sz := q.rl.Get()
			added, stopped := q.Add(m)
			if added {
				if q.rl.Get() != sz+pb.GetEntrySliceInMemSize([]pb.Entry{e}) {
					t.Errorf("failed to update rate limit")
				}
			}
			if !added || stopped {
				t.Errorf("failed to add")
			}
		}
	}
	t.Fatalf("failed to observe any rate limited message")
}

func TestGetWillResetTheRateLimiterSize(t *testing.T) {
	q := NewMessageQueue(10000, false, 0, 1024)
	for i := 0; i < 8; i++ {
		e := pb.Entry{Index: uint64(i + 1)}
		m := pb.Message{
			Type:    pb.Replicate,
			Entries: []pb.Entry{e},
		}
		added, stopped := q.Add(m)
		if !added && stopped {
			t.Fatalf("failed to add message")
		}
	}
	if q.rl.Get() == 0 {
		t.Errorf("rate limiter size is 0")
	}
	q.Get()
	if q.rl.Get() != 0 {
		t.Fatalf("failed to reset the rate limiter")
	}
}

func TestGetDelayed(t *testing.T) {
	q := NewMessageQueue(10, false, 0, 1024)
	q.AddDelayed(pb.Message{From: 1, Type: pb.SnapshotStatus}, 2)
	q.AddDelayed(pb.Message{From: 2, Type: pb.SnapshotStatus}, 10)
	require.Equal(t, 2, len(q.delayed))
	result := q.getDelayed()
	require.Equal(t, 0, len(result))
	q.Tick()
	q.Tick()
	q.Tick()
	result = q.getDelayed()
	require.Equal(t, 1, len(result))
	require.Equal(t, 1, len(q.delayed))
	require.Equal(t, uint64(1), result[0].From)
	require.Equal(t, uint64(2), q.delayed[0].m.From)
}

func TestDelayedMessage(t *testing.T) {
	q := NewMessageQueue(10, false, 0, 1024)
	rm := pb.Message{}
	mm := pb.Message{Type: pb.InstallSnapshot}
	q.Add(rm)
	q.MustAdd(mm)
	dm1 := pb.Message{From: 1, Type: pb.SnapshotStatus}
	dm2 := pb.Message{From: 2, Type: pb.SnapshotStatus}
	q.AddDelayed(dm1, 2)
	q.AddDelayed(dm2, 10)
	q.Tick()
	q.Tick()
	q.Tick()
	result := q.Get()
	require.Equal(t, 1, len(q.delayed))
	require.Equal(t, dm2, q.delayed[0].m)
	require.Equal(t, 3, len(result))
	require.Equal(t, mm, result[0])
	require.Equal(t, dm1, result[1])
	require.Equal(t, rm, result[2])
}
