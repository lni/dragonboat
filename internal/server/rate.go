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

package server

import (
	"math"
	"sync/atomic"
)

const (
	gcTick uint64 = 3
	// ChangeTickThreashold is the minimum number of ticks required to update
	// the state of the rate limiter.
	ChangeTickThreashold uint64 = 10
)

type followerState struct {
	tick         uint64
	inMemLogSize uint64
}

// RateLimiter is the struct used to keep tracking consumed memory size.
type RateLimiter struct {
	size    uint64
	maxSize uint64
}

// NewRateLimiter creates and returns a rate limiter instance.
func NewRateLimiter(max uint64) *RateLimiter {
	return &RateLimiter{
		maxSize: max,
	}
}

// Enabled returns a boolean flag indicating whether the rate limiter is
// enabled.
func (r *RateLimiter) Enabled() bool {
	return r.maxSize > 0 && r.maxSize != math.MaxUint64
}

// Increase increases the recorded in memory log size by sz bytes.
func (r *RateLimiter) Increase(sz uint64) {
	atomic.AddUint64(&r.size, sz)
}

// Decrease decreases the recorded in memory log size by sz bytes.
func (r *RateLimiter) Decrease(sz uint64) {
	atomic.AddUint64(&r.size, ^(sz - 1))
}

// Set sets the recorded in memory log size to sz bytes.
func (r *RateLimiter) Set(sz uint64) {
	atomic.StoreUint64(&r.size, sz)
}

// Get returns the recorded in memory log size.
func (r *RateLimiter) Get() uint64 {
	return atomic.LoadUint64(&r.size)
}

// RateLimited returns a boolean flag indicating whether the node is rate
// limited.
func (r *RateLimiter) RateLimited() bool {
	if !r.Enabled() {
		return false
	}
	v := r.Get()
	if v > r.maxSize {
		plog.Infof("rate limited, v: %d, maxSize %d", v, r.maxSize)
		return true
	}
	return false
}

// InMemRateLimiter is the struct used to keep tracking the in memory rate log size.
type InMemRateLimiter struct {
	followerSizes map[uint64]followerState
	rl            RateLimiter
	tick          uint64
	tickLimited   uint64
	limited       bool
}

// NewInMemRateLimiter creates and returns a rate limiter instance.
func NewInMemRateLimiter(maxSize uint64) *InMemRateLimiter {
	return &InMemRateLimiter{
		// so tickLimited won't be 0
		tick:          1,
		rl:            RateLimiter{maxSize: maxSize},
		followerSizes: make(map[uint64]followerState),
	}
}

// Enabled returns a boolean flag indicating whether the rate limiter is
// enabled.
func (r *InMemRateLimiter) Enabled() bool {
	return r.rl.Enabled()
}

// Tick advances the internal logical clock.
func (r *InMemRateLimiter) Tick() {
	r.tick++
}

// GetTick returns the internal logical clock value.
func (r *InMemRateLimiter) GetTick() uint64 {
	return r.tick
}

// Increase increases the recorded in memory log size by sz bytes.
func (r *InMemRateLimiter) Increase(sz uint64) {
	r.rl.Increase(sz)
}

// Decrease decreases the recorded in memory log size by sz bytes.
func (r *InMemRateLimiter) Decrease(sz uint64) {
	r.rl.Decrease(sz)
}

// Set sets the recorded in memory log size to sz bytes.
func (r *InMemRateLimiter) Set(sz uint64) {
	r.rl.Set(sz)
}

// Get returns the recorded in memory log size.
func (r *InMemRateLimiter) Get() uint64 {
	return r.rl.Get()
}

// Reset clears all recorded follower states.
func (r *InMemRateLimiter) Reset() {
	r.followerSizes = make(map[uint64]followerState)
}

// SetFollowerState sets the follower rate identiified by replicaID to sz bytes.
func (r *InMemRateLimiter) SetFollowerState(replicaID uint64, sz uint64) {
	r.followerSizes[replicaID] = followerState{
		tick:         r.tick,
		inMemLogSize: sz,
	}
}

// RateLimited returns a boolean flag indicating whether the node is rate
// limited.
func (r *InMemRateLimiter) RateLimited() bool {
	limited := r.limitedByInMemSize()
	if limited != r.limited {
		if r.tickLimited == 0 || r.tick-r.tickLimited > ChangeTickThreashold {
			r.limited = limited
			r.tickLimited = r.tick
		}
	}
	return r.limited
}

func (r *InMemRateLimiter) limitedByInMemSize() bool {
	if !r.Enabled() {
		return false
	}
	maxInMemSize := uint64(0)
	gc := false
	for _, v := range r.followerSizes {
		if r.tick-v.tick > gcTick {
			gc = true
			continue
		}
		if v.inMemLogSize > maxInMemSize {
			maxInMemSize = v.inMemLogSize
		}
	}
	sz := r.Get()
	if sz > maxInMemSize {
		maxInMemSize = sz
	}
	if gc {
		r.gc()
	}
	if !r.limited {
		return maxInMemSize > r.rl.maxSize
	}
	return maxInMemSize >= (r.rl.maxSize * 7 / 10)
}

func (r *InMemRateLimiter) gc() {
	followerStates := make(map[uint64]followerState)
	for nid, v := range r.followerSizes {
		if r.tick-v.tick > gcTick {
			continue
		}
		followerStates[nid] = v
	}
	r.followerSizes = followerStates
}
