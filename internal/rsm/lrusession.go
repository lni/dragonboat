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
	"crypto/md5"
	"encoding/binary"
	"io"
	"sync"

	"github.com/lni/goutils/cache"

	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/internal/settings"
)

const (
	// EmptyClientSessionLength defines the length of an empty sessions instance.
	EmptyClientSessionLength uint64 = 16
)

var (
	// LRUMaxSessionCount is the largest number of client sessions that can be
	// concurrently managed by a LRUSession instance.
	LRUMaxSessionCount = settings.Hard.LRUMaxSessionCount
)

// GetEmptyLRUSession returns an marshaled empty sessions instance.
func GetEmptyLRUSession() []byte {
	v := make([]byte, 0)
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, LRUMaxSessionCount)
	total := make([]byte, 8)
	binary.LittleEndian.PutUint64(total, 0)
	v = append(v, sz...)
	v = append(v, total...)
	return v
}

// lrusession is a session manager that keeps up to size number of client
// sessions. LRU is the policy for evicting old ones.
type lrusession struct {
	sessions  *cache.OrderedCache
	size      uint64
	searchKey RaftClientID
	sync.Mutex
}

// Newlrusession returns a new lrusession instance that can hold up to size
// client sessions.
func newLRUSession(size uint64) *lrusession {
	if size == 0 {
		panic("lrusession size must be > 0")
	}
	rec := &lrusession{
		size:     size,
		sessions: cache.NewOrderedCache(cache.Config{Policy: cache.CacheLRU}),
	}
	rec.sessions.Config.ShouldEvict = func(n int, k, v interface{}) bool {
		if uint64(n) > rec.size {
			clientID := k.(*RaftClientID)
			plog.Warningf("session with client id %d evicted, overloaded", *clientID)
			return true
		}
		return false
	}
	rec.sessions.Config.OnEvicted = func(k, v, e interface{}) {}
	return rec
}

// GetSession returns the client session identified by the key.
func (rec *lrusession) getSession(key RaftClientID) (*Session, bool) {
	rec.Lock()
	defer rec.Unlock()
	return rec.getSessionLocked(key)
}

// Save checkpoints the state of the lrusession and save the checkpointed
// state into the writer.
func (rec *lrusession) save(writer io.Writer) error {
	rec.Lock()
	defer rec.Unlock()
	idList := make([]RaftClientID, 0)
	rec.sessions.OrderedDo(func(k, v interface{}) {
		key := k.(*RaftClientID)
		idList = append(idList, *key)
	})
	totalbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(totalbuf, rec.size)
	if _, err := writer.Write(totalbuf); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(totalbuf, uint64(len(idList)))
	if _, err := writer.Write(totalbuf); err != nil {
		return err
	}
	for _, key := range idList {
		session, ok := rec.getSessionLocked(key)
		if !ok || session == nil {
			panic("bad state")
		}
		if err := session.save(writer); err != nil {
			return err
		}
	}
	return nil
}

// Load restores the state the of lrusession from the provided reader.
// reader contains lrusession state previously checkpointed.
func (rec *lrusession) load(reader io.Reader, v SSVersion) error {
	rec.Lock()
	defer rec.Unlock()
	sessionList := make([]*Session, 0)
	sizebuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, sizebuf); err != nil {
		return err
	}
	sz := binary.LittleEndian.Uint64(sizebuf)
	if _, err := io.ReadFull(reader, sizebuf); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(sizebuf)
	for i := uint64(0); i < total; i++ {
		s := &Session{}
		err := s.recoverFromSnapshot(reader, v)
		if err != nil {
			return err
		}
		sessionList = append(sessionList, s)
	}
	newRec := newLRUSession(sz)
	rec.sessions = newRec.sessions
	rec.size = sz
	for _, s := range sessionList {
		rec.addSessionLocked(s.ClientID, *s)
	}
	return nil
}

func (rec *lrusession) makeEntry(key RaftClientID,
	value Session) *cache.Entry {
	alloc := struct {
		entry cache.Entry
		value Session
		key   RaftClientID
	}{
		key:   key,
		value: value,
	}
	alloc.entry.Key = &alloc.key
	alloc.entry.Value = &alloc.value
	return &alloc.entry
}

func (rec *lrusession) addSession(key RaftClientID, s Session) {
	rec.Lock()
	defer rec.Unlock()
	rec.addSessionLocked(key, s)
}

func (rec *lrusession) addSessionLocked(key RaftClientID, s Session) {
	entry := rec.makeEntry(key, s)
	rec.sessions.AddEntry(entry)
}

func (rec *lrusession) getSessionLocked(key RaftClientID) (*Session, bool) {
	rec.searchKey = key
	v, ok := rec.sessions.Get(&rec.searchKey)
	if ok {
		return v.(*Session), ok
	}
	return nil, ok
}

func (rec *lrusession) delSession(key RaftClientID) {
	rec.Lock()
	defer rec.Unlock()
	rec.sessions.Del(&key)
}

func (rec *lrusession) getHash() uint64 {
	snapshot := &bytes.Buffer{}
	if err := rec.save(snapshot); err != nil {
		panic(err)
	}
	data := snapshot.Bytes()
	hash := md5.New()
	fileutil.MustWrite(hash, data)
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8])
}
