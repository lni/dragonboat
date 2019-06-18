package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

// RateLimiter is used to control write rate of flush and compaction.
type RateLimiter struct {
	c *C.rocksdb_ratelimiter_t
}

// NewRateLimiter creates a default RateLimiter object.
func NewRateLimiter(rate, refill int64, fairness int32) *RateLimiter {
	return NewNativeRateLimiter(C.rocksdb_ratelimiter_create(
		C.int64_t(rate),
		C.int64_t(refill),
		C.int32_t(fairness),
	))
}

// NewNativeRateLimiter creates a native RateLimiter object.
func NewNativeRateLimiter(c *C.rocksdb_ratelimiter_t) *RateLimiter {
	return &RateLimiter{c}
}

// Destroy deallocates the RateLimiter object.
func (rl *RateLimiter) Destroy() {
	C.rocksdb_ratelimiter_destroy(rl.c)
	rl.c = nil
}
