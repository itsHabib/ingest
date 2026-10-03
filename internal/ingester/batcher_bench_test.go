package ingester

import (
	"fmt"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
)

func makeBenchEvent(tenantID string) event.Event {
	return event.NewEvent(tenantID, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), map[string]string{"service": "api", "env": "prod"}, 42.0)
}

// drainFlushChan starts a goroutine that drains the flush channel to prevent blocking.
// Returns a stop function that should be deferred.
func drainFlushChan(b *Batcher) func() {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-b.FlushChan():
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

func BenchmarkBatcherAdd(b *testing.B) {
	batcher := NewBatcher(1_000_000, 1*time.Hour)
	stop := drainFlushChan(batcher)
	defer stop()

	ev := makeBenchEvent("tenant-a")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Add(ev, 0, int64(i), "test-topic")
	}
}

func BenchmarkBatcherAdd_Parallel(b *testing.B) {
	batcher := NewBatcher(1_000_000, 1*time.Hour)
	stop := drainFlushChan(batcher)
	defer stop()

	ev := makeBenchEvent("tenant-a")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			batcher.Add(ev, 0, i, "test-topic")
			i++
		}
	})
}

func BenchmarkBatcherAdd_MultiTenant(b *testing.B) {
	batcher := NewBatcher(1_000_000, 1*time.Hour)
	stop := drainFlushChan(batcher)
	defer stop()

	// Pre-generate events for 100 tenants
	tenants := make([]event.Event, 100)
	for i := range tenants {
		tenants[i] = makeBenchEvent(fmt.Sprintf("tenant-%d", i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := tenants[i%len(tenants)]
		batcher.Add(ev, 0, int64(i), "test-topic")
	}
}
