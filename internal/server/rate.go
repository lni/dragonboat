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

package server

import (
	"math"
	"sync/atomic"
)

const (
	gcTick uint64 = 2
)

type followerState struct {
	tick         uint64
	inMemLogSize uint64
}

// RateLimiter is the struct used to keep tracking the in memory rate log size.
type RateLimiter struct {
	size          uint64
	tick          uint64
	maxSize       uint64
	followerSizes map[uint64]followerState
}

// NewRateLimiter creates and returns a rate limiter instance.
func NewRateLimiter(maxSize uint64) *RateLimiter {
	return &RateLimiter{
		maxSize:       maxSize,
		followerSizes: make(map[uint64]followerState),
	}
}

// Enabled returns a boolean flag indicating whether the rate limiter is
// enabled.
func (r *RateLimiter) Enabled() bool {
	return r.maxSize > 0 && r.maxSize != math.MaxUint64
}

// HeartbeatTick advances the internal logical clock.
func (r *RateLimiter) HeartbeatTick() {
	r.tick++
}

// GetHeartbeatTick returns the internal logical clock value.
func (r *RateLimiter) GetHeartbeatTick() uint64 {
	return r.tick
}

// Increase increases the recorded in memory log size by sz bytes.
func (r *RateLimiter) Increase(sz uint64) {
	atomic.AddUint64(&r.size, sz)
}

// Decrease decreases the recorded in memory log size by sz bytes.
func (r *RateLimiter) Decrease(sz uint64) {
	atomic.AddUint64(&r.size, ^uint64(sz-1))
}

// Set sets the recorded in memory log size to sz bytes.
func (r *RateLimiter) Set(sz uint64) {
	atomic.StoreUint64(&r.size, sz)
}

// Get returns the recorded in memory log size.
func (r *RateLimiter) Get() uint64 {
	return atomic.LoadUint64(&r.size)
}

// ResetFollowerState clears all recorded follower states.
func (r *RateLimiter) ResetFollowerState() {
	r.followerSizes = make(map[uint64]followerState)
}

// SetFollowerState sets the follower rate identiified by nodeID to sz bytes.
func (r *RateLimiter) SetFollowerState(nodeID uint64, sz uint64) {
	state := followerState{
		tick:         r.tick,
		inMemLogSize: sz,
	}
	r.followerSizes[nodeID] = state
}

// RateLimited returns a boolean flag indicating whether the node is rate
// limited.
func (r *RateLimiter) RateLimited() bool {
	return r.limitedByInMemSize()
}

func (r *RateLimiter) limitedByInMemSize() bool {
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
	return maxInMemSize > r.maxSize
}

func (r *RateLimiter) gc() {
	followerStates := make(map[uint64]followerState)
	for nid, v := range r.followerSizes {
		if r.tick-v.tick > gcTick {
			continue
		}
		followerStates[nid] = v
	}
	r.followerSizes = followerStates
}
