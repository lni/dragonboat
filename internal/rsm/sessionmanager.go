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
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// SessionManager is the wrapper struct that implements client session related
// functionalities used in the IManagedStateMachine interface.
type SessionManager struct {
	lru *lrusession
}

var _ ILoadable = (*SessionManager)(nil)

// NewSessionManager returns a new SessionManager instance.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		lru: newLRUSession(LRUMaxSessionCount),
	}
}

// GetSessionHash returns an uint64 integer representing the state of the
// session manager.
func (ds *SessionManager) GetSessionHash() uint64 {
	return ds.lru.getHash()
}

// UpdateRespondedTo updates the responded to value of the specified
// client session.
func (ds *SessionManager) UpdateRespondedTo(session *Session,
	respondedTo uint64) {
	session.clearTo(RaftSeriesID(respondedTo))
}

// RegisterClientID registers a new client, it returns the input client id
// if it is previously unknown, or 0 when the client has already been
// registered.
func (ds *SessionManager) RegisterClientID(clientID uint64) sm.Result {
	es, ok := ds.lru.getSession(RaftClientID(clientID))
	if ok {
		if es.ClientID != RaftClientID(clientID) {
			plog.Panicf("returned an expected session, got id %d, want %d",
				es.ClientID, clientID)
		}
		plog.Warningf("client ID %d already exist", clientID)
		return sm.Result{}
	}
	s := newSession(RaftClientID(clientID))
	ds.lru.addSession(RaftClientID(clientID), *s)
	return sm.Result{Value: clientID}
}

// UnregisterClientID removes the specified client session from the system.
// It returns the client id if the client is successfully removed, or 0
// if the client session does not exist.
func (ds *SessionManager) UnregisterClientID(clientID uint64) sm.Result {
	es, ok := ds.lru.getSession(RaftClientID(clientID))
	if !ok {
		return sm.Result{}
	}
	if es.ClientID != RaftClientID(clientID) {
		plog.Panicf("returned an expected session, got id %d, want %d",
			es.ClientID, clientID)
	}
	ds.lru.delSession(RaftClientID(clientID))
	return sm.Result{Value: clientID}
}

// ClientRegistered returns whether the specified client exists in the system.
func (ds *SessionManager) ClientRegistered(clientID uint64) (*Session, bool) {
	es, ok := ds.lru.getSession(RaftClientID(clientID))
	if ok {
		if es.ClientID != RaftClientID(clientID) {
			plog.Panicf("returned an expected session, got id %d, want %d",
				es.ClientID, clientID)
		}
	}
	return es, ok
}

// UpdateRequired return a tuple of request result, responded before,
// update required.
func (ds *SessionManager) UpdateRequired(session *Session,
	seriesID uint64) (sm.Result, bool, bool) {
	if session.hasResponded(RaftSeriesID(seriesID)) {
		return sm.Result{}, true, false
	}
	v, ok := session.getResponse(RaftSeriesID(seriesID))
	if ok {
		return v, false, false
	}
	return sm.Result{}, false, true
}

// MustHaveClientSeries checks whether the session manager contains a client
// session identified as clientID and whether it has seriesID responded.
func (ds *SessionManager) MustHaveClientSeries(session *Session,
	seriesID uint64) {
	_, ok := session.getResponse(RaftSeriesID(seriesID))
	if ok {
		panic("already has response in session")
	}
}

// AddResponse adds the specified result to the session.
func (ds *SessionManager) AddResponse(session *Session,
	seriesID uint64, result sm.Result) {
	session.addResponse(RaftSeriesID(seriesID), result)
}

// SaveSessions saves the sessions to the provided io.writer.
func (ds *SessionManager) SaveSessions(writer io.Writer) error {
	return ds.lru.save(writer)
}

// LoadSessions loads and restores sessions from io.Reader.
func (ds *SessionManager) LoadSessions(reader io.Reader, v SSVersion) error {
	return ds.lru.load(reader, v)
}
