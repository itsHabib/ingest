package tenant

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAllow_BelowRate(t *testing.T) {
	rl := NewRateLimiter(1000, 5000)

	// First call should always be allowed (burst available).
	if !rl.Allow("tenant-a") {
		t.Fatal("expected Allow to return true for first request")
	}
}

func TestAllow_ExceedsRate(t *testing.T) {
	// Rate of 1 event/sec, burst of 1 -- second immediate call should be denied.
	rl := NewRateLimiter(1, 1)

	if !rl.Allow("tenant-a") {
		t.Fatal("first call should be allowed")
	}
	if rl.Allow("tenant-a") {
		t.Fatal("second immediate call should be denied (burst exhausted)")
	}
}

func TestAllow_TenantIsolation(t *testing.T) {
	rl := NewRateLimiter(1, 1)

	// Exhaust tenant-a's burst.
	if !rl.Allow("tenant-a") {
		t.Fatal("tenant-a first call should be allowed")
	}
	if rl.Allow("tenant-a") {
		t.Fatal("tenant-a should be rate limited")
	}

	// tenant-b should be unaffected.
	if !rl.Allow("tenant-b") {
		t.Fatal("tenant-b should not be affected by tenant-a's rate limit")
	}
}

func TestAllow_BurstBehavior(t *testing.T) {
	burst := 5
	rl := NewRateLimiter(1, burst) // 1 event/sec, burst of 5

	// Should allow exactly burst events immediately.
	for i := 0; i < burst; i++ {
		if !rl.Allow("tenant-a") {
			t.Fatalf("event %d should be allowed (within burst)", i)
		}
	}

	// Next one should be denied.
	if rl.Allow("tenant-a") {
		t.Fatal("event after burst should be denied")
	}
}

func TestAllow_Concurrent(t *testing.T) {
	rl := NewRateLimiter(10000, 50000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rl.Allow("tenant-a")
			}
		}()
	}
	wg.Wait()
	// If we get here without a race detector complaint, concurrency is safe.
}

func TestSetTenantLimit(t *testing.T) {
	rl := NewRateLimiter(1000, 5000)

	// Override tenant-a to a very restrictive limit.
	rl.SetTenantLimit("tenant-a", 1, 1)

	if !rl.Allow("tenant-a") {
		t.Fatal("first call should be allowed")
	}
	if rl.Allow("tenant-a") {
		t.Fatal("second immediate call should be denied (custom limit)")
	}

	// tenant-b should use defaults (very permissive).
	if !rl.Allow("tenant-b") {
		t.Fatal("tenant-b should use default (permissive) limits")
	}
}

func TestWaitN(t *testing.T) {
	rl := NewRateLimiter(1000, 5000)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Should not block since burst is large.
	if err := rl.WaitN(ctx, "tenant-a", 10); err != nil {
		t.Fatalf("WaitN should not error with available burst: %v", err)
	}
}

func TestWaitN_ContextCancellation(t *testing.T) {
	rl := NewRateLimiter(1, 1) // very slow rate

	// Exhaust the burst.
	rl.Allow("tenant-a")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := rl.WaitN(ctx, "tenant-a", 10)
	if err == nil {
		t.Fatal("WaitN should return error when context is cancelled")
	}
}

func TestNewRateLimiter_PanicsOnInvalidRate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for defaultRate <= 0")
		}
	}()
	NewRateLimiter(0, 100)
}

func TestAllow_ManyTenants(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(1000, 5000)

	// Create 1000 distinct tenants, verify all get limiters without panic.
	for i := 0; i < 1000; i++ {
		tenantID := fmt.Sprintf("tenant-%04d", i)
		if !rl.Allow(tenantID) {
			t.Fatalf("tenant %s first call should be allowed", tenantID)
		}
	}

	// Verify all limiters are independent -- re-check the first few.
	for i := 0; i < 10; i++ {
		tenantID := fmt.Sprintf("tenant-%04d", i)
		// Should still be allowed since burst is 5000.
		if !rl.Allow(tenantID) {
			t.Errorf("tenant %s should still have burst available", tenantID)
		}
	}
}

func TestNewRateLimiter_PanicsOnInvalidBurst(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for defaultBurst <= 0")
		}
	}()
	NewRateLimiter(100, 0)
}
