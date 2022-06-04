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

package rsm

import (
	"bytes"
	"testing"

	"github.com/lni/goutils/cache"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRegisteriAndUnregisterClient(t *testing.T) {
	sm := NewSessionManager()
	h1 := sm.GetSessionHash()
	_, ok := sm.ClientRegistered(123)
	if ok {
		t.Errorf("already has client with client id 123")
	}
	sm.RegisterClientID(123)
	_, ok = sm.ClientRegistered(123)
	if !ok {
		t.Errorf("client not registered")
	}
	h2 := sm.GetSessionHash()
	v := sm.UnregisterClientID(123)
	if v.Value != 123 {
		t.Errorf("failed to unregister client")
	}
	_, ok = sm.ClientRegistered(123)
	if ok {
		t.Errorf("still has client with client id 123")
	}
	h3 := sm.GetSessionHash()
	if h1 == h2 {
		t.Errorf("hash does not change")
	}
	if h1 != h3 {
		t.Errorf("hash unexpectedly changed")
	}
}

func TestSessionSaveOrderWithEviction(t *testing.T) {
	sm1 := &SessionManager{lru: newLRUSession(4)}
	sm2 := &SessionManager{lru: newLRUSession(4)}

	for i := uint64(0); i < sm1.lru.size; i++ {
		sm1.RegisterClientID(i)
	}
	// touch the oldest session to make it the most recently accessed
	s, ok := sm1.ClientRegistered(uint64(0))
	if !ok {
		t.Fatalf("failed to get client session")
	}
	sm1.AddResponse(s, 1, sm.Result{Value: 123456})
	ss := &bytes.Buffer{}
	if err := sm1.SaveSessions(ss); err != nil {
		t.Fatalf("failed to save snapshot %v", err)
	}
	rs := bytes.NewBuffer(ss.Bytes())
	if err := sm2.LoadSessions(rs, V2); err != nil {
		t.Fatalf("failed to restore snapshot %v", err)
	}
	// client with the same client id (1 and 2 here) expected to be evicted
	sm1.RegisterClientID(sm1.lru.size)
	sm2.RegisterClientID(sm1.lru.size)
	sm1.RegisterClientID(sm1.lru.size + 1)
	sm2.RegisterClientID(sm1.lru.size + 1)
	s1 := &bytes.Buffer{}
	if err := sm1.SaveSessions(s1); err != nil {
		t.Fatalf("failed to save snapshot %v", err)
	}
	s2 := &bytes.Buffer{}
	if err := sm2.SaveSessions(s2); err != nil {
		t.Fatalf("failed to save snapshot %v", err)
	}
	if !bytes.Equal(s1.Bytes(), s2.Bytes()) {
		t.Fatalf("different snapshot")
	}
	check := func(c *cache.OrderedCache) {
		keys := make(map[uint64]struct{})
		c.OrderedDo(func(k, v interface{}) {
			clientID := k.(*RaftClientID)
			keys[uint64(*clientID)] = struct{}{}
		})
		if _, ok := keys[0]; !ok {
			t.Errorf("client 0 not in the session list")
		}
		if _, ok := keys[1]; ok {
			t.Errorf("client 1 unexpectedly still in the session list")
		}
		if _, ok := keys[2]; ok {
			t.Errorf("client 2 unexpectedly still in the session list")
		}
	}
	check(sm1.lru.sessions)
	check(sm2.lru.sessions)
}
