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
	"testing"
)

func TestRateLimiterCanBeEnabled(t *testing.T) {
	tests := []struct {
		maxSize uint64
		enabled bool
	}{
		{0, false},
		{math.MaxUint64, false},
		{1, true},
		{math.MaxUint64 - 1, true},
	}
	for idx, tt := range tests {
		r := NewRateLimiter(tt.maxSize)
		if r.Enabled() != tt.enabled {
			t.Errorf("%d, enabled %t, want %t", idx, r.Enabled(), tt.enabled)
		}
	}
}

func TestInMemLogSizeIsAccessible(t *testing.T) {
	r := NewRateLimiter(100)
	if r.Get() != 0 {
		t.Errorf("sz %d, want 0", r.Get())
	}
	r.Increase(100)
	if r.Get() != 100 {
		t.Errorf("sz %d, want 100", r.Get())
	}
	r.Decrease(10)
	if r.Get() != 90 {
		t.Errorf("sz %d, want 90", r.Get())
	}
	r.Set(243)
	if r.Get() != 243 {
		t.Errorf("sz %d, want 243", r.Get())
	}
}

func TestRateLimiterTick(t *testing.T) {
	r := NewRateLimiter(100)
	for i := 0; i < 100; i++ {
		r.HeartbeatTick()
		if r.tick != uint64(i+1) {
			t.Errorf("tick %d, want %d", r.tick, i+1)
		}
	}
}

func TestFollowerStateCanBeSet(t *testing.T) {
	r := NewRateLimiter(100)
	r.SetFollowerState(100, 1)
	r.SetFollowerState(101, 2)
	r.HeartbeatTick()
	r.HeartbeatTick()
	r.SetFollowerState(101, 4)
	r.SetFollowerState(102, 200)
	if len(r.followerSizes) != 3 {
		t.Errorf("not all state recorded")
	}
	tests := []struct {
		nodeID uint64
		v      uint64
		tick   uint64
	}{
		{100, 1, 0},
		{101, 4, 2},
		{102, 200, 2},
	}
	for idx, tt := range tests {
		rec, ok := r.followerSizes[tt.nodeID]
		if !ok {
			t.Errorf("%d, state not found", idx)
		}
		if rec.inMemLogSize != tt.v {
			t.Errorf("%d, v %d, want %d", idx, rec.inMemLogSize, tt.v)
		}
		if rec.tick != tt.tick {
			t.Errorf("%d, tick %d, want %d", idx, rec.tick, tt.tick)
		}
	}
}

func TestGCRemoveOutOfDateFollowerState(t *testing.T) {
	r := NewRateLimiter(100)
	r.SetFollowerState(101, 1)
	r.HeartbeatTick()
	r.SetFollowerState(102, 2)
	r.SetFollowerState(103, 3)
	r.gc()
	if len(r.followerSizes) != 3 {
		t.Errorf("count %d, want 3", len(r.followerSizes))
	}
	r.HeartbeatTick()
	r.HeartbeatTick()
	r.gc()
	if len(r.followerSizes) != 2 {
		t.Errorf("count %d, want 2", len(r.followerSizes))
	}
	_, ok := r.followerSizes[101]
	if ok {
		t.Errorf("old follower state not removed")
	}
	r.HeartbeatTick()
	r.gc()
	if len(r.followerSizes) != 0 {
		t.Errorf("count %d, want 0", len(r.followerSizes))
	}
}

func TestRateLimited(t *testing.T) {
	r := NewRateLimiter(100)
	r.Increase(100)
	if r.RateLimited() {
		t.Errorf("unexpectedly rate limited")
	}
	r.Increase(1)
	if !r.RateLimited() {
		t.Errorf("not rate limited")
	}
}

func TestRateLimitedWhenFollowerIsRateLimited(t *testing.T) {
	r := NewRateLimiter(100)
	r.Increase(100)
	if r.RateLimited() {
		t.Errorf("unexpectedly rate limited")
	}
	r.SetFollowerState(1, 100)
	r.SetFollowerState(2, 101)
	if !r.RateLimited() {
		t.Errorf("not rate limited")
	}
}

func TestRateNotLimitedWhenOutOfDateFollowerStateIsLimited(t *testing.T) {
	r := NewRateLimiter(100)
	r.Increase(100)
	if r.RateLimited() {
		t.Errorf("unexpectedly rate limited")
	}
	r.SetFollowerState(1, 100)
	r.SetFollowerState(2, 101)
	r.HeartbeatTick()
	r.HeartbeatTick()
	r.HeartbeatTick()
	r.HeartbeatTick()
	if r.RateLimited() {
		t.Errorf("unexpectedly rate limited")
	}
	if len(r.followerSizes) != 0 {
		t.Errorf("out of date follower state not GCed")
	}
}

func TestNotEnabledRateLimitNeverLimitRates(t *testing.T) {
	r := NewRateLimiter(0)
	for i := 0; i < 10000; i++ {
		r.Increase(math.MaxUint64 / 2)
		if r.RateLimited() {
			t.Errorf("unexpectedly rate limited")
		}
	}
}

func TestResetFollowerState(t *testing.T) {
	rl := NewRateLimiter(1024)
	rl.SetFollowerState(1, 1025)
	if !rl.RateLimited() {
		t.Errorf("not rate limited as expected")
	}
	rl.ResetFollowerState()
	if rl.RateLimited() {
		t.Errorf("unexpectedly rate limited")
	}
}
