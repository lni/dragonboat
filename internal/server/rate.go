// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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
)

const (
	gcTick uint64 = 2
)

type followerState struct {
	tick         uint64
	inMemLogSize uint64
}

type RateLimiter struct {
	tick                  uint64
	inMemLogSize          uint64
	maxSize               uint64
	followerInMemLogSizes map[uint64]followerState
}

func NewRateLimiter(maxSize uint64) *RateLimiter {
	return &RateLimiter{
		maxSize:               maxSize,
		followerInMemLogSizes: make(map[uint64]followerState),
	}
}

func (r *RateLimiter) Enabled() bool {
	return r.maxSize > 0 && r.maxSize != math.MaxUint64
}

func (r *RateLimiter) HeartbeatTick() {
	r.tick++
}

func (r *RateLimiter) GetHeartbeatTick() uint64 {
	return r.tick
}

func (r *RateLimiter) IncreaseInMemLogSize(sz uint64) {
	r.inMemLogSize += sz
}

func (r *RateLimiter) DecreaseInMemLogSize(sz uint64) {
	r.inMemLogSize -= sz
}

func (r *RateLimiter) SetInMemLogSize(sz uint64) {
	r.inMemLogSize = sz
}

func (r *RateLimiter) GetInMemLogSize() uint64 {
	return r.inMemLogSize
}

func (r *RateLimiter) ResetFollowerState() {
	r.followerInMemLogSizes = make(map[uint64]followerState)
}

func (r *RateLimiter) SetFollowerState(nodeID uint64, sz uint64) {
	state := followerState{
		tick:         r.tick,
		inMemLogSize: sz,
	}
	r.followerInMemLogSizes[nodeID] = state
}

func (r *RateLimiter) RateLimited() bool {
	return r.limitedByInMemSize()
}

func (r *RateLimiter) limitedByInMemSize() bool {
	if !r.Enabled() {
		return false
	}
	maxInMemSize := uint64(0)
	gc := false
	for _, v := range r.followerInMemLogSizes {
		if r.tick-v.tick > gcTick {
			gc = true
			continue
		}
		if v.inMemLogSize > maxInMemSize {
			maxInMemSize = v.inMemLogSize
		}
	}
	//plog.Infof("inMemLogSize %d, limit %d, max size %d",
	//	r.inMemLogSize, r.maxSize, maxInMemSize)
	if r.inMemLogSize > maxInMemSize {
		maxInMemSize = r.inMemLogSize
	}
	if gc {
		r.gc()
	}
	return maxInMemSize > r.maxSize
}

func (r *RateLimiter) gc() {
	followerStates := make(map[uint64]followerState)
	for nid, v := range r.followerInMemLogSizes {
		if r.tick-v.tick > gcTick {
			continue
		}
		followerStates[nid] = v
	}
	r.followerInMemLogSizes = followerStates
}
