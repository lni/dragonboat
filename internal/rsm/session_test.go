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
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
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
			s.addResponse(tt.seriesNumList[idx], sm.Result{Value: tt.valueList[idx]})
		}
		if len(s.History) != tt.size {
			t.Errorf("i %d, size %d, want %d", i, len(s.History), tt.size)
		}
		v, ok := s.getResponse(tt.testSeriesNum)
		if v.Value != tt.expectedValue {
			t.Errorf("i %d, v %d, want %d", i, v, tt.expectedValue)
		}
		if ok != tt.expectedResult {
			t.Errorf("i %d, v %t, want %t", i, ok, tt.expectedResult)
		}
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
			s.addResponse(tt.seriesNumList[idx], sm.Result{Value: tt.valueList[idx]})
		}
		s.clearTo(tt.clearTo)
		if len(s.History) != tt.sizeAfterClear {
			t.Errorf("i %d, size %d, want %d", i, len(s.History), tt.sizeAfterClear)
		}
		_, ok := s.getResponse(tt.testSeriesNum)
		if ok != tt.expectedResult {
			t.Errorf("i %d, resp %t, want %t", i, ok, tt.expectedResult)
		}
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
			s.addResponse(tt.seriesNumList[idx], sm.Result{Value: tt.valueList[idx]})
		}

		s.clearTo(tt.clearTo)
		ok := s.hasResponded(tt.testID)
		if ok != tt.expectedResult {
			t.Errorf("i %d, resp %t, want %t", i, ok, tt.expectedResult)
		}
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
			rand.Read(cmd)
			s.addResponse(tt.seriesNumList[idx], sm.Result{Value: tt.valueList[idx], Data: cmd})
		}
		snapshot := &bytes.Buffer{}
		if err := s.save(snapshot); err != nil {
			t.Fatalf("save failed %v", err)
		}
		data := snapshot.Bytes()
		toRecover := bytes.NewBuffer(data)
		newS := &Session{}
		err := newS.recoverFromSnapshot(toRecover, V2)
		if err != nil {
			t.Fatalf("failed to create session from snapshot, %v", err)
		}
		if !reflect.DeepEqual(newS, s) {
			t.Errorf("i %d, got %v, want %v", i, newS, s)
		}
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
	if err != nil {
		panic(err)
	}
	sz := len(data)
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(sz))
	_, err = ss.Write(lenbuf)
	if err != nil {
		t.Errorf("failed to write %v", err)
	}
	_, err = ss.Write(data)
	if err != nil {
		t.Errorf("failed to write %v", err)
	}
	newS := &Session{}
	data = ss.Bytes()
	toRecover := bytes.NewBuffer(data)
	if err := newS.recoverFromSnapshot(toRecover, V1); err != nil {
		t.Errorf("recover from ss %v", err)
	}
	if newS.ClientID != session.ClientID || newS.RespondedUpTo != session.RespondedUpTo {
		t.Errorf("field changed")
	}
	v1, ok := newS.History[1234]
	if !ok || v1.Value != 324 {
		t.Errorf("unexpected v1")
	}
	v2, ok := newS.History[5678]
	if !ok || v2.Value != 458 {
		t.Errorf("unexpected v2")
	}
}

func TestUnknownVersionCausePanicWhenRecoverSessionFromSnapshot(t *testing.T) {
	session := &v1session{
		ClientID:      123,
		RespondedUpTo: 456789,
		History:       make(map[RaftSeriesID]uint64),
	}
	ss := &bytes.Buffer{}
	data, err := json.Marshal(session)
	if err != nil {
		panic(err)
	}
	sz := len(data)
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(sz))
	_, err = ss.Write(lenbuf)
	if err != nil {
		t.Errorf("failed to write %v", err)
	}
	_, err = ss.Write(data)
	if err != nil {
		t.Errorf("failed to write %v", err)
	}
	newS := &Session{}
	data = ss.Bytes()
	toRecover := bytes.NewBuffer(data)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic not triggered")
		}
	}()
	if err := newS.recoverFromSnapshot(toRecover, SSVersion(3)); err != nil {
		t.Errorf("recover from ss %v", err)
	}
}
