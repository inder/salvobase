package storage

import (
	"sync"
	"time"
)

// RateLimiter implements a per-tenant (per-database) token bucket rate limiter.
// This is an improvement over MongoDB Community which has no built-in rate limiting.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rps     int // requests per second (0 = unlimited)
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
	rps        float64
}

// NewRateLimiter creates a rate limiter with the given requests-per-second limit.
// rps=0 disables rate limiting.
func NewRateLimiter(rps int) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rps:     rps,
	}
}

// Allow returns true if the request for the given database should be allowed.
// Thread-safe.
func (r *RateLimiter) Allow(db string) bool {
	if r.rps == 0 {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	bucket, ok := r.buckets[db]
	if !ok {
		bucket = &tokenBucket{
			tokens:     float64(r.rps),
			lastRefill: time.Now(),
			rps:        float64(r.rps),
		}
		r.buckets[db] = bucket
	}

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * bucket.rps
	if bucket.tokens > bucket.rps {
		bucket.tokens = bucket.rps // cap at burst size = 1 second of tokens
	}
	bucket.lastRefill = now

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// SetLimit updates the rate limit for all tenants.
func (r *RateLimiter) SetLimit(rps int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rps = rps
	// Reset all buckets to pick up new limit
	r.buckets = make(map[string]*tokenBucket)
}
