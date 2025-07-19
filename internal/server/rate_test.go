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
	"testing"

	"github.com/stretchr/testify/require"
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
		r := NewInMemRateLimiter(tt.maxSize)
		require.Equal(t, tt.enabled, r.Enabled(),
			"%d, enabled %t, want %t", idx, r.Enabled(), tt.enabled)
	}
}

func TestInMemLogSizeIsAccessible(t *testing.T) {
	r := NewInMemRateLimiter(100)
	require.Equal(t, uint64(0), r.Get(), "sz %d, want 0", r.Get())
	r.Increase(100)
	require.Equal(t, uint64(100), r.Get(), "sz %d, want 100", r.Get())
	r.Decrease(10)
	require.Equal(t, uint64(90), r.Get(), "sz %d, want 90", r.Get())
	r.Set(243)
	require.Equal(t, uint64(243), r.Get(), "sz %d, want 243", r.Get())
}

func TestRateLimiterTick(t *testing.T) {
	r := NewInMemRateLimiter(100)
	for i := 0; i < 100; i++ {
		r.Tick()
		require.Equal(t, uint64(i+2), r.tick,
			"tick %d, want %d", r.tick, i+2)
	}
}

func TestFollowerStateCanBeSet(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.SetFollowerState(100, 1)
	r.SetFollowerState(101, 2)
	r.Tick()
	r.Tick()
	r.SetFollowerState(101, 4)
	r.SetFollowerState(102, 200)
	require.Equal(t, 3, len(r.followerSizes), "not all state recorded")
	tests := []struct {
		replicaID uint64
		v         uint64
		tick      uint64
	}{
		{100, 1, 0},
		{101, 4, 2},
		{102, 200, 2},
	}
	for idx, tt := range tests {
		rec, ok := r.followerSizes[tt.replicaID]
		require.True(t, ok, "%d, state not found", idx)
		require.Equal(t, tt.v, rec.inMemLogSize,
			"%d, v %d, want %d", idx, rec.inMemLogSize, tt.v)
		require.Equal(t, tt.tick+1, rec.tick,
			"%d, tick %d, want %d", idx, rec.tick, tt.tick+1)
	}
}

func TestGCRemoveOutOfDateFollowerState(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.SetFollowerState(101, 1)
	r.Tick()
	r.SetFollowerState(102, 2)
	r.SetFollowerState(103, 3)
	r.gc()
	require.Equal(t, 3, len(r.followerSizes),
		"count %d, want 3", len(r.followerSizes))
	for i := uint64(0); i < gcTick; i++ {
		r.Tick()
	}
	r.gc()
	require.Equal(t, 2, len(r.followerSizes),
		"count %d, want 2", len(r.followerSizes))
	_, ok := r.followerSizes[101]
	require.False(t, ok, "old follower state not removed")
	r.Tick()
	r.gc()
	require.Equal(t, 0, len(r.followerSizes),
		"count %d, want 0", len(r.followerSizes))
}

func TestRateLimited(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Increase(100)
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
	r.Increase(1)
	require.True(t, r.RateLimited(), "not rate limited")
}

func TestRateLimitedWhenFollowerIsRateLimited(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Increase(100)
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
	r.SetFollowerState(1, 100)
	r.SetFollowerState(2, 101)
	require.True(t, r.RateLimited(), "not rate limited")
}

func TestRateNotLimitedWhenOutOfDateFollowerStateIsLimited(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Increase(100)
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
	r.SetFollowerState(1, 100)
	r.SetFollowerState(2, 101)
	r.Tick()
	r.Tick()
	r.Tick()
	r.Tick()
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
	require.Equal(t, 0, len(r.followerSizes),
		"out of date follower state not GCed")
}

func TestNotEnabledRateLimitNeverLimitRates(t *testing.T) {
	r := NewInMemRateLimiter(0)
	for i := 0; i < 10000; i++ {
		r.Increase(math.MaxUint64 / 2)
		require.False(t, r.RateLimited(), "unexpectedly rate limited")
	}
}

func TestResetFollowerState(t *testing.T) {
	rl := NewInMemRateLimiter(1024)
	rl.SetFollowerState(1, 1025)
	require.True(t, rl.RateLimited(), "not rate limited as expected")
	rl.Reset()
	for i := uint64(0); i <= ChangeTickThreashold; i++ {
		rl.Tick()
	}
	require.False(t, rl.RateLimited(), "unexpectedly rate limited")
}

func TestUnlimitedThreshild(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Increase(101)
	require.True(t, r.RateLimited(), "unexpectedly not rate limited")
	for i := uint64(0); i <= ChangeTickThreashold; i++ {
		r.Tick()
	}
	r.Set(99)
	require.True(t, r.RateLimited(), "unexpectedly not rate limited")
	r.Set(70)
	require.True(t, r.RateLimited(), "unexpectedly not rate limited")
	r.Set(69)
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
}

func TestRateLimitChangeCantChangeVeryOften(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Increase(101)
	for i := uint64(0); i <= ChangeTickThreashold; i++ {
		r.Tick()
	}
	require.True(t, r.RateLimited(), "unexpectedly not rate limited")
	r.Set(69)
	require.True(t, r.RateLimited(), "unexpectedly not rate limited")
	for i := uint64(0); i <= ChangeTickThreashold; i++ {
		r.Tick()
	}
	require.False(t, r.RateLimited(), "unexpectedly rate limited")
}
