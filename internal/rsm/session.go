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

package rsm

import (
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/lni/dragonboat/v3/internal/utils/cache/biogo/store/llrb"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

// RaftClientID is the type used as client id in sessions.
type RaftClientID uint64

// RaftSeriesID is the type used as series id in sessions.
type RaftSeriesID uint64

// Compare implements the llrb.Comparable interface.
func (a *RaftClientID) Compare(b llrb.Comparable) int {
	bk := b.(*RaftClientID)
	aval := *a
	bval := *bk
	switch {
	case aval < bval:
		return -1
	case aval > bval:
		return 1
	default:
		return 0
	}
}

// Session is the session object maintained on the raft side.
type Session struct {
	ClientID      RaftClientID
	RespondedUpTo RaftSeriesID
	History       map[RaftSeriesID]sm.Result
}

// v1session is the session type used in v1 snapshot format.
type v1session struct {
	ClientID      RaftClientID
	RespondedUpTo RaftSeriesID
	History       map[RaftSeriesID]uint64
}

func newSession(id RaftClientID) *Session {
	return &Session{
		ClientID: id,
		History:  make(map[RaftSeriesID]sm.Result),
	}
}

// AddResponse adds a response.
func (s *Session) AddResponse(id RaftSeriesID, result sm.Result) {
	s.addResponse(id, result)
}

func (s *Session) getResponse(id RaftSeriesID) (sm.Result, bool) {
	v, ok := s.History[id]
	return v, ok
}

func (s *Session) addResponse(id RaftSeriesID, result sm.Result) {
	_, ok := s.History[id]
	if !ok {
		s.History[id] = result
	} else {
		panic("adding a duplicated response")
	}
}

func (s *Session) clearTo(to RaftSeriesID) {
	if to <= s.RespondedUpTo {
		return
	}
	if to == s.RespondedUpTo+1 {
		delete(s.History, to)
		s.RespondedUpTo = to
		return
	}
	s.RespondedUpTo = to
	for k := range s.History {
		if k <= to {
			delete(s.History, k)
		}
	}
}

func (s *Session) hasResponded(id RaftSeriesID) bool {
	return id <= s.RespondedUpTo
}

func (s *Session) save(writer io.Writer) (uint64, error) {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	sz := len(data)
	lenbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenbuf, uint64(sz))
	n, err := writer.Write(lenbuf)
	if err != nil {
		return 0, err
	}
	if n != len(lenbuf) {
		return 0, io.ErrShortWrite
	}
	n, err = writer.Write(data)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, io.ErrShortWrite
	}
	return uint64(len(data) + 8), nil
}

func (s *Session) recoverFromSnapshot(reader io.Reader,
	v SnapshotVersion) error {
	lenbuf := make([]byte, 8)
	n, err := io.ReadFull(reader, lenbuf)
	if err != nil {
		return err
	}
	if n != len(lenbuf) {
		return io.ErrUnexpectedEOF
	}
	sz := binary.LittleEndian.Uint64(lenbuf)
	data := make([]byte, sz)
	n, err = io.ReadFull(reader, data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrUnexpectedEOF
	}
	if v == V1SnapshotVersion {
		s.recoverFromV1Snapshot(data)
	} else if v == V2SnapshotVersion {
		if err := json.Unmarshal(data, s); err != nil {
			panic(err)
		}
	} else {
		plog.Panicf("unknown version number %d", v)
	}
	return nil
}

func (s *Session) recoverFromV1Snapshot(data []byte) {
	v := &v1session{}
	if err := json.Unmarshal(data, v); err != nil {
		panic(err)
	}
	s.ClientID = v.ClientID
	s.RespondedUpTo = v.RespondedUpTo
	s.History = make(map[RaftSeriesID]sm.Result)
	for key, val := range v.History {
		s.History[key] = sm.Result{Value: val}
	}
}
