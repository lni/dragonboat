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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestRegisteriAndUnregisterClient(t *testing.T) {
	sm := NewSessionManager()
	h1 := sm.GetSessionHash()
	_, ok := sm.ClientRegistered(123)
	assert.False(t, ok, "already has client with client id 123")
	sm.RegisterClientID(123)
	_, ok = sm.ClientRegistered(123)
	assert.True(t, ok, "client not registered")
	h2 := sm.GetSessionHash()
	v := sm.UnregisterClientID(123)
	assert.Equal(t, uint64(123), v.Value, "failed to unregister client")
	_, ok = sm.ClientRegistered(123)
	assert.False(t, ok, "still has client with client id 123")
	h3 := sm.GetSessionHash()
	assert.NotEqual(t, h1, h2, "hash does not change")
	assert.Equal(t, h1, h3, "hash unexpectedly changed")
}

func TestSessionSaveOrderWithEviction(t *testing.T) {
	sm1 := &SessionManager{lru: newLRUSession(4)}
	sm2 := &SessionManager{lru: newLRUSession(4)}

	for i := uint64(0); i < sm1.lru.size; i++ {
		sm1.RegisterClientID(i)
	}
	// touch the oldest session to make it the most recently accessed
	s, ok := sm1.ClientRegistered(uint64(0))
	require.True(t, ok, "failed to get client session")
	sm1.AddResponse(s, 1, sm.Result{Value: 123456})
	ss := &bytes.Buffer{}
	err := sm1.SaveSessions(ss)
	require.NoError(t, err, "failed to save snapshot")
	rs := bytes.NewBuffer(ss.Bytes())
	err = sm2.LoadSessions(rs, V2)
	require.NoError(t, err, "failed to restore snapshot")
	// client with the same client id (1 and 2 here) expected to be evicted
	sm1.RegisterClientID(sm1.lru.size)
	sm2.RegisterClientID(sm1.lru.size)
	sm1.RegisterClientID(sm1.lru.size + 1)
	sm2.RegisterClientID(sm1.lru.size + 1)
	s1 := &bytes.Buffer{}
	err = sm1.SaveSessions(s1)
	require.NoError(t, err, "failed to save snapshot")
	s2 := &bytes.Buffer{}
	err = sm2.SaveSessions(s2)
	require.NoError(t, err, "failed to save snapshot")
	assert.Equal(t, s1.Bytes(), s2.Bytes(), "different snapshot")
	check := func(c *cache.OrderedCache) {
		keys := make(map[uint64]struct{})
		c.OrderedDo(func(k, v interface{}) {
			clientID := k.(*RaftClientID)
			keys[uint64(*clientID)] = struct{}{}
		})
		_, ok := keys[0]
		assert.True(t, ok, "client 0 not in the session list")
		_, ok = keys[1]
		assert.False(t, ok, "client 1 unexpectedly still in the session list")
		_, ok = keys[2]
		assert.False(t, ok, "client 2 unexpectedly still in the session list")
	}
	check(sm1.lru.sessions)
	check(sm2.lru.sessions)
}
