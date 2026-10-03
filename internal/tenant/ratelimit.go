package tenant

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// RateLimiter provides per-tenant token bucket rate limiting.
// It uses a sync.RWMutex with a double-checked locking pattern so the common
// case (limiter already exists) only takes a read lock.
type RateLimiter struct {
	mu           sync.RWMutex
	limiters     map[string]*rate.Limiter
	defaultRate  rate.Limit
	defaultBurst int
}

// NewRateLimiter creates a rate limiter with the given default rate (events/sec)
// and burst size. Panics if defaultRate <= 0 or defaultBurst <= 0.
func NewRateLimiter(defaultRate float64, defaultBurst int) *RateLimiter {
	if defaultRate <= 0 {
		panic("tenant: NewRateLimiter: defaultRate must be > 0")
	}
	if defaultBurst <= 0 {
		panic("tenant: NewRateLimiter: defaultBurst must be > 0")
	}
	return &RateLimiter{
		limiters:     make(map[string]*rate.Limiter),
		defaultRate:  rate.Limit(defaultRate),
		defaultBurst: defaultBurst,
	}
}

// getOrCreate returns the limiter for the tenant, creating one with defaults
// if it doesn't exist. Uses RLock fast path, upgrades to Lock only on miss.
func (rl *RateLimiter) getOrCreate(tenantID string) *rate.Limiter {
	// Fast path: read lock
	rl.mu.RLock()
	lim, ok := rl.limiters[tenantID]
	rl.mu.RUnlock()
	if ok {
		return lim
	}

	// Slow path: write lock with double-check
	rl.mu.Lock()
	defer rl.mu.Unlock()

	lim, ok = rl.limiters[tenantID]
	if ok {
		return lim
	}

	lim = rate.NewLimiter(rl.defaultRate, rl.defaultBurst)
	rl.limiters[tenantID] = lim
	return lim
}

// Allow checks if one event is allowed for the given tenant.
// Returns true if the event is permitted under the rate limit.
// Thread-safe; fast path uses RLock when the limiter already exists.
func (rl *RateLimiter) Allow(tenantID string) bool {
	return rl.getOrCreate(tenantID).Allow()
}

// WaitN blocks until n tokens are available for the tenant or ctx is cancelled.
func (rl *RateLimiter) WaitN(ctx context.Context, tenantID string, n int) error {
	return rl.getOrCreate(tenantID).WaitN(ctx, n)
}

// SetTenantLimit overrides the rate and burst for a specific tenant.
// Creates a new limiter if the tenant doesn't exist yet.
func (rl *RateLimiter) SetTenantLimit(tenantID string, r float64, burst int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limiters[tenantID] = rate.NewLimiter(rate.Limit(r), burst)
}
