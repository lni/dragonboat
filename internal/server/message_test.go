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

package server

import (
	"testing"

	"github.com/lni/dragonboat/v3/raftpb"
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
		added, stopped := q.Add(raftpb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	add, stopped := q.Add(raftpb.Message{})
	add2, stopped2 := q.Add(raftpb.Message{})
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
	add, stopped = q.Add(raftpb.Message{})
	add2, stopped2 = q.Add(raftpb.Message{})
	if !add || !add2 {
		t.Errorf("failed to add message")
	}
	if stopped || stopped2 {
		t.Errorf("unexpectedly stopped")
	}
}

func TestNonSnapshotMsgByCallingAddSnapshotWillPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("didn't panic")
	}()
	q := NewMessageQueue(8, false, 0, 0)
	q.AddSnapshot(raftpb.Message{})
}

func TestSnapshotCanAlwaysBeAdded(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	for i := 0; i < 1024; i++ {
		if !q.AddSnapshot(raftpb.Message{Type: raftpb.InstallSnapshot}) {
			t.Errorf("failed to add snapshot")
		}
	}
}

func TestAddedSnapshotWillBeReturned(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	if !q.AddSnapshot(raftpb.Message{Type: raftpb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(raftpb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	if !q.AddSnapshot(raftpb.Message{Type: raftpb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(raftpb.Message{})
		if !added || stopped {
			t.Errorf("failed to add")
		}
	}
	if !q.AddSnapshot(raftpb.Message{Type: raftpb.InstallSnapshot}) {
		t.Errorf("failed to add snapshot")
	}
	msgs := q.Get()
	if len(msgs) != 11 {
		t.Errorf("failed to return all messages")
	}
	count := 0
	for _, msg := range msgs {
		if msg.Type == raftpb.InstallSnapshot {
			count++
		}
	}
	if count != 3 {
		t.Errorf("failed to get all snapshot messages")
	}
	if len(q.snapshot) != 0 {
		t.Errorf("snapshot list not empty")
	}
}

func TestMessageQueueCanBeStopped(t *testing.T) {
	q := NewMessageQueue(8, false, 0, 0)
	q.Close()
	for i := 0; i < 4; i++ {
		added, stopped := q.Add(raftpb.Message{})
		if added || !stopped {
			t.Errorf("unexpectedly added msg")
		}
	}
	if q.AddSnapshot(raftpb.Message{Type: raftpb.InstallSnapshot}) {
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

func TestAddMessageIsRateLimited(t *testing.T) {
	q := NewMessageQueue(10000, false, 0, 1024)
	for i := 0; i < 10000; i++ {
		e := raftpb.Entry{Index: uint64(i + 1)}
		m := raftpb.Message{
			Type:    raftpb.Replicate,
			Entries: []raftpb.Entry{e},
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
				if q.rl.Get() != sz+uint64(e.SizeUpperLimit()) {
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
		e := raftpb.Entry{Index: uint64(i + 1)}
		m := raftpb.Message{
			Type:    raftpb.Replicate,
			Entries: []raftpb.Entry{e},
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
