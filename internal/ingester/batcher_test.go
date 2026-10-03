package ingester

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
)

func makeEvent(tenantID string) event.Event {
	return event.NewEvent(tenantID, time.Now(), map[string]string{"k": "v"}, 1.0)
}

func TestBatcher_SizeTriggeredFlush(t *testing.T) {
	maxSize := 5
	b := NewBatcher(maxSize, 1*time.Hour) // long maxAge so only size triggers

	for i := 0; i < maxSize; i++ {
		b.Add(makeEvent("tenant-a"), 0, int64(i), "test-topic")
	}

	select {
	case job := <-b.FlushChan():
		if job.batch.Len() != maxSize {
			t.Errorf("batch len = %d, want %d", job.batch.Len(), maxSize)
		}
		if job.batch.TenantID != "tenant-a" {
			t.Errorf("tenant = %q, want %q", job.batch.TenantID, "tenant-a")
		}
		if job.offset != int64(maxSize-1) {
			t.Errorf("offset = %d, want %d", job.offset, maxSize-1)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for flush")
	}
}

func TestBatcher_TimeTriggeredFlush(t *testing.T) {
	b := NewBatcher(1000, 50*time.Millisecond) // small maxAge

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go b.Start(ctx)

	b.Add(makeEvent("tenant-a"), 0, 1, "test-topic")

	select {
	case job := <-b.FlushChan():
		if job.batch.Len() != 1 {
			t.Errorf("batch len = %d, want 1", job.batch.Len())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for time-triggered flush")
	}
}

func TestBatcher_FlushAll(t *testing.T) {
	b := NewBatcher(1000, 1*time.Hour)

	b.Add(makeEvent("tenant-a"), 0, 1, "test-topic")
	b.Add(makeEvent("tenant-b"), 1, 2, "test-topic")
	b.Add(makeEvent("tenant-c"), 2, 3, "test-topic")

	b.FlushAll()

	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		select {
		case job := <-b.FlushChan():
			seen[job.batch.TenantID] = true
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for flush job %d", i)
		}
	}

	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-c"} {
		if !seen[tenant] {
			t.Errorf("missing flush for %s", tenant)
		}
	}
}

func TestBatcher_MultiTenantIsolation(t *testing.T) {
	maxSize := 3
	b := NewBatcher(maxSize, 1*time.Hour)

	// Add events for 3 tenants, only filling one tenant's batch
	for i := 0; i < maxSize; i++ {
		b.Add(makeEvent("tenant-a"), 0, int64(i), "test-topic")
	}
	b.Add(makeEvent("tenant-b"), 0, 100, "test-topic")
	b.Add(makeEvent("tenant-c"), 1, 200, "test-topic")

	// Only tenant-a should have triggered a flush
	select {
	case job := <-b.FlushChan():
		if job.batch.TenantID != "tenant-a" {
			t.Errorf("expected tenant-a flush, got %s", job.batch.TenantID)
		}
		if job.batch.Len() != maxSize {
			t.Errorf("batch len = %d, want %d", job.batch.Len(), maxSize)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for tenant-a flush")
	}

	// No more flushes should be pending
	select {
	case job := <-b.FlushChan():
		t.Errorf("unexpected flush for %s", job.batch.TenantID)
	default:
		// expected
	}
}

func TestBatcher_BackpressureBlocking(t *testing.T) {
	// Create batcher with maxSize=1 so every Add triggers a flush
	b := NewBatcher(1, 1*time.Hour)

	// Fill the flush channel (buffer size 100)
	for i := 0; i < 100; i++ {
		b.Add(makeEvent("tenant-a"), 0, int64(i), "test-topic")
	}

	// The next Add should block because flushCh is full
	done := make(chan struct{})
	go func() {
		b.Add(makeEvent("tenant-a"), 0, 100, "test-topic")
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Add should have blocked on full flushCh")
	case <-time.After(100 * time.Millisecond):
		// expected: Add is blocked
	}

	// Drain one item to unblock
	<-b.FlushChan()

	select {
	case <-done:
		// unblocked as expected
	case <-time.After(1 * time.Second):
		t.Fatal("Add did not unblock after draining flushCh")
	}
}

func TestBatcher_ConcurrentAdd(t *testing.T) {
	maxSize := 10000 // large so flushes are rare
	b := NewBatcher(maxSize, 1*time.Hour)

	const goroutines = 10
	const eventsPerGoroutine = 100
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tenantID := fmt.Sprintf("tenant-%d", id%3) // 3 tenants, overlapping
			for i := 0; i < eventsPerGoroutine; i++ {
				b.Add(makeEvent(tenantID), int32(id%2), int64(id*eventsPerGoroutine+i), "test-topic")
			}
		}(g)
	}

	wg.Wait()

	// Flush remaining and count all events
	b.FlushAll()
	total := 0
	for {
		select {
		case job := <-b.FlushChan():
			total += job.batch.Len()
		default:
			goto done
		}
	}
done:
	expected := goroutines * eventsPerGoroutine
	if total != expected {
		t.Errorf("total events = %d, want %d", total, expected)
	}
}

func BenchmarkBatcher_Add(b *testing.B) {
	batcher := NewBatcher(1000000, 1*time.Hour) // huge maxSize so no flush triggers
	ev := makeEvent("tenant-a")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batcher.Add(ev, 0, int64(i), "test-topic")
	}
}
