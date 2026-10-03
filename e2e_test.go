//go:build integration

package main

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	testharness "github.com/itsHabib/ingest/internal/testing/harness"
)

func TestEndToEnd_SingleTenant(t *testing.T) {
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(10),
		testharness.WithBatchAge(2*time.Second),
	)

	const (
		tenantID   = "tenant-a"
		numEvents  = 100
		flushWait  = 30 * time.Second
	)

	events := testharness.GenerateEvents(numEvents, tenantID)

	// Push events in batches of 25 to avoid overwhelming a single request.
	batchSize := 25
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		resp, err := testharness.PushEvents(h.GatewayURL(), tenantID, events[i:end])
		if err != nil {
			t.Fatalf("push events batch [%d:%d]: %v", i, end, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("push events batch [%d:%d]: status %d", i, end, resp.StatusCode)
		}
		testharness.DrainResponse(resp)
	}

	// Wait for all events to be flushed to storage (at-least-once).
	if err := testharness.WaitForFlush(h.StorageDir(), tenantID, numEvents, flushWait); err != nil {
		t.Fatalf("wait for flush: %v", err)
	}

	// Verify at-least-once: count >= expected.
	allEvents, err := testharness.ReadAllEvents(h.StorageDir())
	if err != nil {
		t.Fatalf("read all events: %v", err)
	}

	got := len(allEvents[tenantID])
	if got < numEvents {
		t.Errorf("at-least-once violation: got %d events, want >= %d", got, numEvents)
	}
	t.Logf("single-tenant: got %d events (expected >= %d)", got, numEvents)

	// Verify tenant ID is correct on all events.
	for i, ev := range allEvents[tenantID] {
		if ev.TenantID != tenantID {
			t.Errorf("event[%d].TenantID = %q, want %q", i, ev.TenantID, tenantID)
		}
	}
}

func TestEndToEnd_MultiTenant(t *testing.T) {
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(10),
		testharness.WithBatchAge(2*time.Second),
	)

	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}
	const (
		numEventsPerTenant = 100
		flushWait          = 30 * time.Second
	)

	// Push events for each tenant.
	for _, tid := range tenants {
		events := testharness.GenerateEvents(numEventsPerTenant, tid)
		batchSize := 25
		for i := 0; i < len(events); i += batchSize {
			end := i + batchSize
			if end > len(events) {
				end = len(events)
			}
			resp, err := testharness.PushEvents(h.GatewayURL(), tid, events[i:end])
			if err != nil {
				t.Fatalf("push events for %s [%d:%d]: %v", tid, i, end, err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("push events for %s [%d:%d]: status %d", tid, i, end, resp.StatusCode)
			}
			testharness.DrainResponse(resp)
		}
	}

	// Wait for all events to be flushed.
	totalExpected := len(tenants) * numEventsPerTenant
	if err := testharness.WaitForTotalFlush(h.StorageDir(), totalExpected, flushWait); err != nil {
		t.Fatalf("wait for total flush: %v", err)
	}

	allEvents, err := testharness.ReadAllEvents(h.StorageDir())
	if err != nil {
		t.Fatalf("read all events: %v", err)
	}

	// Verify each tenant has at-least-once delivery.
	for _, tid := range tenants {
		got := len(allEvents[tid])
		if got < numEventsPerTenant {
			t.Errorf("tenant %s: at-least-once violation: got %d events, want >= %d", tid, got, numEventsPerTenant)
		}
		t.Logf("tenant %s: got %d events (expected >= %d)", tid, got, numEventsPerTenant)
	}

	// Verify tenant isolation: no cross-tenant data leakage.
	for _, tid := range tenants {
		for _, ev := range allEvents[tid] {
			if ev.TenantID != tid {
				t.Errorf("tenant isolation violation: event in %s storage has TenantID=%q", tid, ev.TenantID)
			}
		}
	}
}

func TestEndToEnd_RateLimit(t *testing.T) {
	// Configure a very low rate limit: 5 events/sec, burst 5.
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(10),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithRateLimit(5, 5),
	)

	const (
		limitedTenant   = "tenant-limited"
		unlimitedTenant = "tenant-unlimited"
	)

	// Rapidly push many requests for the limited tenant to trigger rate limiting.
	var got429 atomic.Int32
	var got200 atomic.Int32

	events := testharness.GenerateEvents(5, limitedTenant)
	for i := 0; i < 30; i++ {
		resp, err := testharness.PushEvents(h.GatewayURL(), limitedTenant, events)
		if err != nil {
			t.Fatalf("push limited tenant: %v", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			got429.Add(1)
		} else if resp.StatusCode == http.StatusOK {
			got200.Add(1)
		}
		testharness.DrainResponse(resp)
	}

	if got429.Load() == 0 {
		t.Error("expected at least one 429 response for rate-limited tenant, got none")
	}
	t.Logf("rate limit: limited tenant got %d x 200, %d x 429", got200.Load(), got429.Load())

	// The unlimited tenant should get all 200s.
	unlimitedEvents := testharness.GenerateEvents(5, unlimitedTenant)
	for i := 0; i < 5; i++ {
		resp, err := testharness.PushEvents(h.GatewayURL(), unlimitedTenant, unlimitedEvents)
		if err != nil {
			t.Fatalf("push unlimited tenant: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("unlimited tenant request %d: got status %d, want 200", i, resp.StatusCode)
		}
		testharness.DrainResponse(resp)
	}
}

func TestEndToEnd_LargeVolume(t *testing.T) {
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(50),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithFlushWorkers(4),
	)

	tenants := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5"}
	const (
		eventsPerTenant = 2000
		flushWait       = 60 * time.Second
	)

	// Push events concurrently for all tenants.
	var wg sync.WaitGroup
	for _, tid := range tenants {
		wg.Add(1)
		go func(tenantID string) {
			defer wg.Done()
			events := testharness.GenerateEvents(eventsPerTenant, tenantID)
			batchSize := 50
			for i := 0; i < len(events); i += batchSize {
				end := i + batchSize
				if end > len(events) {
					end = len(events)
				}
				resp, err := testharness.PushEvents(h.GatewayURL(), tenantID, events[i:end])
				if err != nil {
					t.Errorf("push events for %s [%d:%d]: %v", tenantID, i, end, err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					t.Errorf("push events for %s [%d:%d]: status %d", tenantID, i, end, resp.StatusCode)
				}
				testharness.DrainResponse(resp)
			}
		}(tid)
	}
	wg.Wait()

	// Wait for all events to be flushed.
	totalExpected := len(tenants) * eventsPerTenant
	if err := testharness.WaitForTotalFlush(h.StorageDir(), totalExpected, flushWait); err != nil {
		t.Fatalf("wait for total flush: %v", err)
	}

	allEvents, err := testharness.ReadAllEvents(h.StorageDir())
	if err != nil {
		t.Fatalf("read all events: %v", err)
	}

	total := 0
	for _, tid := range tenants {
		got := len(allEvents[tid])
		if got < eventsPerTenant {
			t.Errorf("tenant %s: at-least-once violation: got %d events, want >= %d", tid, got, eventsPerTenant)
		}
		total += got
		t.Logf("tenant %s: got %d events", tid, got)
	}

	if total < totalExpected {
		t.Errorf("total at-least-once violation: got %d events, want >= %d", total, totalExpected)
	}
	t.Logf("large volume: total %d events (expected >= %d)", total, totalExpected)
}

func TestEndToEnd_GracefulShutdown(t *testing.T) {
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(10),
		testharness.WithBatchAge(2*time.Second),
	)

	const (
		tenantID  = "tenant-shutdown"
		numEvents = 50
	)

	events := testharness.GenerateEvents(numEvents, tenantID)

	// Push all events.
	batchSize := 10
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		resp, err := testharness.PushEvents(h.GatewayURL(), tenantID, events[i:end])
		if err != nil {
			t.Fatalf("push events [%d:%d]: %v", i, end, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("push events [%d:%d]: status %d", i, end, resp.StatusCode)
		}
		testharness.DrainResponse(resp)
	}

	// Wait until at least some events have been consumed and flushed,
	// confirming the consumer has joined and is processing.
	if err := testharness.WaitForFlush(h.StorageDir(), tenantID, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer never started consuming: %v", err)
	}

	// Trigger graceful shutdown — this should drain all in-flight batches.
	h.Close()

	// Verify all pre-shutdown events are flushed to storage.
	allEvents, err := testharness.ReadAllEvents(h.StorageDir())
	if err != nil {
		t.Fatalf("read all events: %v", err)
	}

	got := len(allEvents[tenantID])
	if got < numEvents {
		t.Errorf("graceful shutdown: at-least-once violation: got %d events, want >= %d", got, numEvents)
	}
	t.Logf("graceful shutdown: got %d events (expected >= %d)", got, numEvents)
}
