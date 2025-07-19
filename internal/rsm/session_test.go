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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestResponseCanBeAdded(t *testing.T) {
	tests := []struct {
		seriesNumList  []RaftSeriesID
		valueList      []uint64
		size           int
		testSeriesNum  RaftSeriesID
		expectedValue  uint64
		expectedResult bool
	}{
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 3, 1, 100, true},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 3, 3, 300, true},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 3, 4, 0, false},
	}
	for i, tt := range tests {
		s := newSession(0)
		for idx := range tt.seriesNumList {
			s.addResponse(tt.seriesNumList[idx],
				sm.Result{Value: tt.valueList[idx]})
		}
		require.Equal(t, tt.size, len(s.History),
			"i %d, size mismatch", i)
		v, ok := s.getResponse(tt.testSeriesNum)
		require.Equal(t, tt.expectedValue, v.Value,
			"i %d, value mismatch", i)
		require.Equal(t, tt.expectedResult, ok,
			"i %d, result mismatch", i)
	}
}

func TestCachedResponseDataCanBeCleared(t *testing.T) {
	tests := []struct {
		seriesNumList  []RaftSeriesID
		valueList      []uint64
		clearTo        RaftSeriesID
		sizeAfterClear int
		testSeriesNum  RaftSeriesID
		expectedResult bool
	}{
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 2, 1, 2, false},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 2, 1, 3, true},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 3, 0, 3, false},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 4, 0, 3, false},
		{[]RaftSeriesID{3, 4, 5}, []uint64{100, 200, 300}, 2, 3, 3, true},
		{[]RaftSeriesID{3, 4, 5}, []uint64{100, 200, 300}, 6, 0, 5, false},
	}
	for i, tt := range tests {
		s := newSession(0)
		for idx := range tt.seriesNumList {
			s.addResponse(tt.seriesNumList[idx],
				sm.Result{Value: tt.valueList[idx]})
		}
		s.clearTo(tt.clearTo)
		require.Equal(t, tt.sizeAfterClear, len(s.History),
			"i %d, size after clear mismatch", i)
		_, ok := s.getResponse(tt.testSeriesNum)
		require.Equal(t, tt.expectedResult, ok,
			"i %d, response result mismatch", i)
	}
}

func TestWhetherResponsedCanBeReturned(t *testing.T) {
	tests := []struct {
		seriesNumList  []RaftSeriesID
		valueList      []uint64
		clearTo        RaftSeriesID
		testID         RaftSeriesID
		expectedResult bool
	}{
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 2, 1, true},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 2, 2, true},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 2, 3, false},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}, 3, 3, true},
		{[]RaftSeriesID{3, 4, 5}, []uint64{100, 200, 300}, 2, 1, true},
		{[]RaftSeriesID{3, 4, 5}, []uint64{100, 200, 300}, 2, 2, true},
		{[]RaftSeriesID{3, 4, 5}, []uint64{100, 200, 300}, 2, 3, false},
	}
	for i, tt := range tests {
		s := newSession(0)
		for idx := range tt.seriesNumList {
			s.addResponse(tt.seriesNumList[idx],
				sm.Result{Value: tt.valueList[idx]})
		}

		s.clearTo(tt.clearTo)
		ok := s.hasResponded(tt.testID)
		require.Equal(t, tt.expectedResult, ok,
			"i %d, response check mismatch", i)
	}
}

func TestSessionCanBeSavedAndRestored(t *testing.T) {
	tests := []struct {
		seriesNumList []RaftSeriesID
		valueList     []uint64
	}{
		{[]RaftSeriesID{}, []uint64{}},
		{[]RaftSeriesID{1}, []uint64{100}},
		{[]RaftSeriesID{1, 2, 3}, []uint64{100, 200, 300}},
	}
	for i, tt := range tests {
		s := newSession(0)
		for idx := range tt.seriesNumList {
			cmd := make([]byte, 1234)
			_, err := rand.Read(cmd)
			require.NoError(t, err)
			s.addResponse(tt.seriesNumList[idx],
				sm.Result{Value: tt.valueList[idx], Data: cmd})
		}
		snapshot := &bytes.Buffer{}
		err := s.save(snapshot)
		require.NoError(t, err, "save failed")
		data := snapshot.Bytes()
		toRecover := bytes.NewBuffer(data)
		newS := &Session{}
		err = newS.recoverFromSnapshot(toRecover, V2)
		require.NoError(t, err, "failed to create session from snapshot")
		require.True(t, reflect.DeepEqual(newS, s),
			"i %d, session mismatch", i)
	}
}

func TestSessionCanBeRestoredFromV1Snapshot(t *testing.T) {
	session := &v1session{
		ClientID:      123,
		RespondedUpTo: 456789,
		History:       make(map[RaftSeriesID]uint64),
	}
	session.History[1234] = 324
	session.History[5678] = 458
	ss := &bytes.Buffer{}
	data, err := json.Marshal(session)
	require.NoError(t, err)
	sz := len(data)
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(sz))
	_, err = ss.Write(lenbuf)
	require.NoError(t, err, "failed to write length buffer")
	_, err = ss.Write(data)
	require.NoError(t, err, "failed to write data")
	newS := &Session{}
	data = ss.Bytes()
	toRecover := bytes.NewBuffer(data)
	err = newS.recoverFromSnapshot(toRecover, V1)
	require.NoError(t, err, "recover from snapshot failed")
	require.Equal(t, session.ClientID, newS.ClientID,
		"ClientID field changed")
	require.Equal(t, session.RespondedUpTo, newS.RespondedUpTo,
		"RespondedUpTo field changed")
	v1, ok := newS.History[1234]
	require.True(t, ok, "v1 not found")
	require.Equal(t, uint64(324), v1.Value, "unexpected v1 value")
	v2, ok := newS.History[5678]
	require.True(t, ok, "v2 not found")
	require.Equal(t, uint64(458), v2.Value, "unexpected v2 value")
}

func TestUnknownVersionCausePanicWhenRecoverSessionFromSnapshot(t *testing.T) {
	session := &v1session{
		ClientID:      123,
		RespondedUpTo: 456789,
		History:       make(map[RaftSeriesID]uint64),
	}
	ss := &bytes.Buffer{}
	data, err := json.Marshal(session)
	require.NoError(t, err)
	sz := len(data)
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(sz))
	_, err = ss.Write(lenbuf)
	require.NoError(t, err, "failed to write length buffer")
	_, err = ss.Write(data)
	require.NoError(t, err, "failed to write data")
	newS := &Session{}
	data = ss.Bytes()
	toRecover := bytes.NewBuffer(data)
	require.Panics(t, func() {
		err := newS.recoverFromSnapshot(toRecover, SSVersion(3))
		require.NoError(t, err, "recover from snapshot failed")
	}, "panic should be triggered for unknown version")
}
