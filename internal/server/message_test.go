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
	require.Equal(t, 8, len(q.left))
	require.Equal(t, 8, len(q.right))
}

func TestMessageCanBeAddedAndGet(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 8; i++ {
		added, stopped := q.Add(pb.Message{})
		require.True(t, added)
		require.False(t, stopped)
	}
	add, stopped := q.Add(pb.Message{})
	add2, stopped2 := q.Add(pb.Message{})
	require.False(t, add)
	require.False(t, add2)
	require.False(t, stopped)
	require.False(t, stopped2)
	require.Equal(t, uint64(8), q.idx)
	lr := q.leftInWrite
	q.Get()
	require.Equal(t, uint64(0), q.idx)
	require.NotEqual(t, lr, q.leftInWrite)
	add, stopped = q.Add(pb.Message{})
	add2, stopped2 = q.Add(pb.Message{})
	require.True(t, add)
	require.True(t, add2)
	require.False(t, stopped)
	require.False(t, stopped2)
}

func TestNonSnapshotMsgByCallingMustAddWillPanic(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	require.Panics(t, func() {
		q.MustAdd(pb.Message{})
	})
}

func TestSnapshotCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 1024; i++ {
		n := len(q.nodrop)
		added := q.MustAdd(pb.Message{Type: pb.InstallSnapshot})
		require.True(t, added)
		require.Equal(t, n+1, len(q.nodrop))
	}
}

func TestUnreachableMsgCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 1024; i++ {
		n := len(q.nodrop)
		added := q.MustAdd(pb.Message{Type: pb.Unreachable})
		require.True(t, added)
		require.Equal(t, n+1, len(q.nodrop))
	}
}

func TestAddedSnapshotWillBeReturned(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	added := q.MustAdd(pb.Message{Type: pb.InstallSnapshot})
	require.True(t, added)
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		require.True(t, added)
		require.False(t, stopped)
	}
	added = q.MustAdd(pb.Message{Type: pb.InstallSnapshot})
	require.True(t, added)
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		require.True(t, added)
		require.False(t, stopped)
	}
	added = q.MustAdd(pb.Message{Type: pb.InstallSnapshot})
	require.True(t, added)
	msgs := q.Get()
	require.Equal(t, 11, len(msgs))
	count := 0
	for _, msg := range msgs {
		if msg.Type == pb.InstallSnapshot {
			count++
		}
	}
	require.Equal(t, 3, count)
	require.Equal(t, 0, len(q.nodrop))
}

func TestMessageQueueCanBeStopped(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	q.Close()
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(pb.Message{})
		require.False(t, added)
		require.True(t, stopped)
	}
	added := q.MustAdd(pb.Message{Type: pb.InstallSnapshot})
	require.False(t, added)
}

func TestRateLimiterCanBeEnabledInMessageQueue(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	require.False(t, q.rl.Enabled())
	q = NewMessageQueue(8, false, 0, 1024)
	require.True(t, q.rl.Enabled())
}

func TestSingleMessageCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(10000, false, 0, 1024)
	e := pb.Entry{Index: 1, Cmd: make([]byte, 2048)}
	m := pb.Message{
		Type:    pb.Replicate,
		Entries: []pb.Entry{e},
	}
	added, stopped := q.Add(m)
	require.True(t, added)
	require.False(t, stopped)
	require.True(t, q.rl.RateLimited())
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
				expected := sz + pb.GetEntrySliceInMemSize([]pb.Entry{e})
				require.Equal(t, expected, q.rl.Get())
			}
			require.True(t, added)
			require.False(t, stopped)
		}
	}
	require.Fail(t, "failed to observe any rate limited message")
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
		require.True(t, added || stopped)
	}
	require.NotEqual(t, 0, q.rl.Get())
	q.Get()
	require.Equal(t, uint64(0), q.rl.Get())
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
