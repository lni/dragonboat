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
	"math/rand"
	"reflect"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecCanBeEvicted(t *testing.T) {
	m := newLRUSession(3)
	for i := RaftClientID(0); i < 3; i++ {
		s := &Session{ClientID: i}
		m.addSession(i, *s)
	}
	// client id 1 used here
	r, ok := m.getSession(RaftClientID(0))
	require.True(t, ok, "session object not returned")
	assert.Equal(t, RaftClientID(0), r.ClientID, "client id mismatch")
	// client id 1 is the LRU target to be evicted
	i := RaftClientID(3)
	s := &Session{ClientID: i}
	m.addSession(i, *s)
	_, ok = m.getSession(RaftClientID(1))
	assert.False(t, ok, "didn't evict the first session object")
	// client id 2 is the LRU target to be evicted
	i = RaftClientID(4)
	s = &Session{ClientID: i}
	m.addSession(i, *s)
	_, ok = m.getSession(RaftClientID(2))
	assert.False(t, ok, "didn't evict the first session object")
	_, ok = m.getSession(RaftClientID(0))
	assert.True(t, ok, "client session with id 0 is expected to stay")
}

func TestSessionIsMutable(t *testing.T) {
	m := newLRUSession(1)
	for i := RaftClientID(0); i < 1; i++ {
		s := &Session{ClientID: i, History: make(map[RaftSeriesID]sm.Result)}
		m.addSession(i, *s)
	}
	// client id 1 used here
	r, ok := m.getSession(RaftClientID(0))
	require.True(t, ok, "session object not returned")
	assert.Equal(t, RaftClientID(0), r.ClientID, "client id mismatch")
	r.History[RaftSeriesID(100)] = sm.Result{Value: 200}
	r, ok = m.getSession(RaftClientID(0))
	require.True(t, ok, "session object not returned")
	assert.Equal(t, RaftClientID(0), r.ClientID, "client id mismatch")
	assert.Equal(t, 1, len(r.History), "history size mismatch")
}

func TestOrderedDoIsLRUOrdered(t *testing.T) {
	m := newLRUSession(100)
	for i := RaftClientID(0); i < 100; i++ {
		s := newSession(i)
		m.addSession(i, *s)
	}
	for i := 0; i < 100; i++ {
		idx := rand.Int() % 100
		m.getSession(RaftClientID(idx))

		idList := make([]RaftClientID, 0)
		m.sessions.OrderedDo(func(k, v interface{}) {
			key := k.(*RaftClientID)
			idList = append(idList, *key)
		})

		assert.Equal(t, 100, len(idList), "size mismatch")
		assert.Equal(t, RaftClientID(idx), idList[99], "last element mismatch")
	}
}

func TestLRUSessionCanBeSavedAndRestoredWithLRUOrderPreserved(t *testing.T) {
	m := newLRUSession(100)
	for i := RaftClientID(0); i < 100; i++ {
		count := rand.Int() % 100
		s := newSession(i)
		for j := 0; j < count; j++ {
			s.addResponse(RaftSeriesID(j), sm.Result{Value: uint64(j)})
		}
		m.addSession(i, *s)
	}
	for i := 0; i < 100; i++ {
		idx := rand.Int() % 100
		m.getSession(RaftClientID(idx))
	}
	oldList := make([]RaftClientID, 0)
	m.sessions.OrderedDo(func(k, v interface{}) {
		key := k.(*RaftClientID)
		oldList = append(oldList, *key)
	})
	snapshot := &bytes.Buffer{}
	err := m.save(snapshot)
	require.NoError(t, err, "save failed")
	data := snapshot.Bytes()
	toRecover := bytes.NewBuffer(data)
	newLRUSession := newLRUSession(5)
	err = newLRUSession.load(toRecover, V2)
	require.NoError(t, err, "load failed")
	newList := make([]RaftClientID, 0)
	newLRUSession.sessions.OrderedDo(func(k, v interface{}) {
		key := k.(*RaftClientID)
		newList = append(newList, *key)
	})
	assert.Equal(t, len(oldList), len(newList), "size mismatch")
	for idx := range oldList {
		assert.Equal(t, oldList[idx], newList[idx], "order is different")
	}
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			idx := rand.Int() % 100
			m.getSession(RaftClientID(idx))
			newLRUSession.getSession(RaftClientID(idx))
		} else {
			v := RaftClientID(10000 * i)
			s1 := newSession(v)
			s2 := newSession(v)
			m.addSession(v, *s1)
			newLRUSession.addSession(v, *s2)
		}
	}
	oldList = make([]RaftClientID, 0)
	m.sessions.OrderedDo(func(k, v interface{}) {
		key := k.(*RaftClientID)
		oldList = append(oldList, *key)
	})
	newList = make([]RaftClientID, 0)
	newLRUSession.sessions.OrderedDo(func(k, v interface{}) {
		key := k.(*RaftClientID)
		newList = append(newList, *key)
	})
	assert.Equal(t, len(oldList), len(newList), "size mismatch")
	for idx := range oldList {
		assert.Equal(t, oldList[idx], newList[idx], "order is different")
	}
}

func TestLRUSessionCanBeSavedAndRestored(t *testing.T) {
	m := newLRUSession(3)
	for i := RaftClientID(0); i < 3; i++ {
		s := newSession(i)
		if i == RaftClientID(1) {
			s.addResponse(100, sm.Result{Value: 200})
			s.addResponse(200, sm.Result{Value: 300})
		} else if i == RaftClientID(2) {
			s.addResponse(300, sm.Result{Value: 500})
			s.addResponse(400, sm.Result{Value: 300})
			s.addResponse(500, sm.Result{Value: 700})
		}
		m.addSession(i, *s)
	}
	snapshot := &bytes.Buffer{}
	err := m.save(snapshot)
	require.NoError(t, err, "save failed")
	data := snapshot.Bytes()
	toRecover := bytes.NewBuffer(data)
	// set to a different size value
	newLRUSession := newLRUSession(5)
	err = newLRUSession.load(toRecover, V2)
	require.NoError(t, err, "load failed")
	oldHash := m.getHash()
	newHash := newLRUSession.getHash()
	assert.Equal(t, oldHash, newHash, "hash mismatch")
	assert.Equal(t, m.sessions.Len(), newLRUSession.sessions.Len(),
		"Len mismatch")
	assert.Equal(t, m.size, newLRUSession.size, "size mismatch")
	testSession := newSession(9)
	testKey := RaftClientID(1)
	assert.True(t, newLRUSession.sessions.ShouldEvict(4, &testKey, testSession),
		"should evict function not adjusted")
	assert.False(t, newLRUSession.sessions.ShouldEvict(3, &testKey, testSession),
		"should evict function not adjusted")
	for i := RaftClientID(0); i < 3; i++ {
		s1, ok1 := m.getSession(i)
		s2, ok2 := newLRUSession.getSession(i)
		assert.Equal(t, ok1, ok2, "ok mismatch")
		assert.True(t, reflect.DeepEqual(s1, s2), "session mismatch")
	}
}

func TestGetEmptyLRUSession(t *testing.T) {
	s := newLRUSession(LRUMaxSessionCount)
	buf := bytes.NewBuffer(make([]byte, 0))
	err := s.save(buf)
	require.NoError(t, err, "failed to save")
	data := buf.Bytes()
	assert.Equal(t, EmptyClientSessionLength, uint64(len(data)),
		"unexpected length")
	assert.True(t, bytes.Equal(data, GetEmptyLRUSession()), "unexpected data")
}
