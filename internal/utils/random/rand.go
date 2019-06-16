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

package random

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// LockedRand a mutex protected random source.
type LockedRand struct {
	source rand.Source64
	mu     sync.Mutex
}

// Source is the interface for returning random Uint64 and Int values.
type Source interface {
	Uint64() uint64
	Int() int
}

// LockGuardedRand is a global lock protected random source ready to be used.
var LockGuardedRand = NewLockedRand()

// NewLockedRand returns a new LockedRand instance.
func NewLockedRand() *LockedRand {
	hostname := "unknown-host"
	if name, err := os.Hostname(); err == nil {
		hostname = name
	}
	s := fmt.Sprintf("%d-%d-%s",
		os.Getpid(), time.Now().UTC().UnixNano(), hostname)
	m := md5.New()
	if _, err := m.Write([]byte(s)); err != nil {
		panic(err)
	}
	md5sum := m.Sum(nil)
	seed := binary.LittleEndian.Uint64(md5sum)
	r := &LockedRand{
		source: rand.New(rand.NewSource(int64(seed))),
	}

	return r
}

// Uint64 returns a new random uint64 value.
func (r *LockedRand) Uint64() uint64 {
	var v uint64
	r.mu.Lock()
	for v == 0 {
		v = r.source.Uint64()
	}
	r.mu.Unlock()
	return v
}

// Int returns a new random int value.
func (r *LockedRand) Int() int {
	r.mu.Lock()
	u := uint(r.source.Int63())
	r.mu.Unlock()
	return int(u << 1 >> 1)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// String returns a random string with the specified sz length.
func String(sz int) string {
	b := make([]byte, sz)
	for i := range b {
		b[i] = letterBytes[LockGuardedRand.Int()%len(letterBytes)]
	}

	return string(b)
}

// ShuffleStringList shuffles the specified string slice.
func ShuffleStringList(a []string) {
	for i := range a {
		j := LockGuardedRand.Int() % (i + 1)
		a[i], a[j] = a[j], a[i]
	}
}
