package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/index"
)

// memStore is an in-memory ObjectStore for testing.
type memStore struct {
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) Put(_ context.Context, key string, data []byte) error {
	m.data[key] = data
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	d, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("memstore: key %q not found", key)
	}
	return d, nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range m.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *memStore) Exists(_ context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

// putBatch serializes and stores a batch at the given key.
func putBatch(t *testing.T, store *memStore, key string, batch event.Batch) {
	t.Helper()
	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	store.data[key] = data
}

func TestReadBlocks_BasicFilter(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()

	// 3 blocks with different events
	putBatch(t, store, "block1", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"service": "api", "env": "prod"}, Value: 1},
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"service": "worker", "env": "prod"}, Value: 2},
		},
	})
	putBatch(t, store, "block2", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"service": "api", "env": "staging"}, Value: 3},
		},
	})
	putBatch(t, store, "block3", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"service": "api", "env": "prod"}, Value: 4},
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"service": "db", "env": "prod"}, Value: 5},
		},
	})

	matchers := []index.LabelMatcher{
		{Name: "service", Value: "api", Type: index.MatchEqual},
		{Name: "env", Value: "prod", Type: index.MatchEqual},
	}

	reader := NewBlockReader(store, 4)
	eventCh, errCh := reader.ReadBlocks(context.Background(), []string{"block1", "block2", "block3"}, matchers)

	var events []event.Event
	for ev := range eventCh {
		events = append(events, ev)
	}

	for err := range errCh {
		t.Errorf("unexpected error: %v", err)
	}

	// Should match: block1 event1 (api+prod), block3 event1 (api+prod)
	if got := len(events); got != 2 {
		t.Fatalf("expected 2 events, got %d", got)
	}

	for _, ev := range events {
		if ev.Labels["service"] != "api" || ev.Labels["env"] != "prod" {
			t.Errorf("unexpected event labels: %v", ev.Labels)
		}
	}
}

func TestReadBlocks_EmptyPaths(t *testing.T) {
	store := newMemStore()
	reader := NewBlockReader(store, 4)
	eventCh, errCh := reader.ReadBlocks(context.Background(), nil, nil)

	var count int
	for range eventCh {
		count++
	}
	for range errCh {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 events/errors for empty paths, got %d", count)
	}
}

func TestReadBlocks_StorageError(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()

	putBatch(t, store, "good-block", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"a": "b"}, Value: 1},
		},
	})
	// "bad-block" is not in the store, so Get will fail

	reader := NewBlockReader(store, 2)
	eventCh, errCh := reader.ReadBlocks(context.Background(), []string{"good-block", "bad-block"}, nil)

	var events []event.Event
	for ev := range eventCh {
		events = append(events, ev)
	}

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
}

func TestReadBlocks_ContextCancel(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()

	// Put many blocks to ensure workers are busy
	for i := 0; i < 100; i++ {
		putBatch(t, store, fmt.Sprintf("block-%d", i), event.Batch{
			TenantID: "t1",
			Events: []event.Event{
				{TenantID: "t1", Timestamp: now, Labels: map[string]string{"i": fmt.Sprintf("%d", i)}, Value: float64(i)},
			},
		})
	}

	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("block-%d", i)
	}

	goroutinesBefore := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := NewBlockReader(store, 4)
	eventCh, errCh := reader.ReadBlocks(ctx, paths, nil)

	// Read a few events then cancel
	count := 0
	for range eventCh {
		count++
		if count >= 5 {
			cancel()
			break
		}
	}

	// Drain remaining
	for range eventCh {
	}
	for range errCh {
	}

	// Allow goroutines to exit
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	goroutinesAfter := runtime.NumGoroutine()
	// Allow some slack for runtime goroutines
	if goroutinesAfter > goroutinesBefore+5 {
		t.Errorf("potential goroutine leak: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
}

func TestReadBlocks_AllEventsMatch(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()

	putBatch(t, store, "block1", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"a": "1"}, Value: 1},
			{TenantID: "t1", Timestamp: now, Labels: map[string]string{"b": "2"}, Value: 2},
		},
	})

	reader := NewBlockReader(store, 2)
	eventCh, errCh := reader.ReadBlocks(context.Background(), []string{"block1"}, nil)

	var events []event.Event
	for ev := range eventCh {
		events = append(events, ev)
	}
	for err := range errCh {
		t.Errorf("unexpected error: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events (no matchers = all pass), got %d", len(events))
	}
}

func TestReadBlocks_EndToEnd(t *testing.T) {
	store := newMemStore()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create blocks with sorted events per block (simulating per-day partitions)
	putBatch(t, store, "day1-block", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: base.Add(1 * time.Second), Labels: map[string]string{"service": "api"}, Value: 1},
			{TenantID: "t1", Timestamp: base.Add(3 * time.Second), Labels: map[string]string{"service": "api"}, Value: 3},
			{TenantID: "t1", Timestamp: base.Add(5 * time.Second), Labels: map[string]string{"service": "worker"}, Value: 5},
		},
	})
	putBatch(t, store, "day2-block", event.Batch{
		TenantID: "t1",
		Events: []event.Event{
			{TenantID: "t1", Timestamp: base.Add(2 * time.Second), Labels: map[string]string{"service": "api"}, Value: 2},
			{TenantID: "t1", Timestamp: base.Add(4 * time.Second), Labels: map[string]string{"service": "api"}, Value: 4},
		},
	})

	matchers := []index.LabelMatcher{
		{Name: "service", Value: "api", Type: index.MatchEqual},
	}

	reader := NewBlockReader(store, 2)

	// Read each day separately to get sorted sub-streams
	stream1, errs1 := reader.ReadBlocks(context.Background(), []string{"day1-block"}, matchers)
	stream2, errs2 := reader.ReadBlocks(context.Background(), []string{"day2-block"}, matchers)

	// Merge the two streams
	merged := MergeStreams(stream1, stream2)

	var events []event.Event
	for ev := range merged {
		events = append(events, ev)
	}

	// Drain error channels
	for err := range errs1 {
		t.Errorf("unexpected error from stream1: %v", err)
	}
	for err := range errs2 {
		t.Errorf("unexpected error from stream2: %v", err)
	}

	// Should have 4 events (worker event filtered out)
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	// Verify sorted by timestamp
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("events not sorted: [%d]=%v > [%d]=%v",
				i-1, events[i-1].Timestamp, i, events[i].Timestamp)
		}
	}

	// Verify expected values in order: 1, 2, 3, 4
	for i, ev := range events {
		expected := float64(i + 1)
		if ev.Value != expected {
			t.Errorf("event[%d] value = %v, want %v", i, ev.Value, expected)
		}
	}
}

// --- Benchmarks ---

func BenchmarkReadBlocks(b *testing.B) {
	store := newMemStore()
	now := time.Now().UTC()

	// Create 100 blocks with 1000 events each, 50% match rate
	for i := 0; i < 100; i++ {
		batch := event.Batch{TenantID: "t1"}
		for j := 0; j < 1000; j++ {
			svc := "api"
			if j%2 == 0 {
				svc = "worker"
			}
			batch.Events = append(batch.Events, event.Event{
				TenantID:  "t1",
				Timestamp: now.Add(time.Duration(j) * time.Millisecond),
				Labels:    map[string]string{"service": svc},
				Value:     float64(j),
			})
		}
		data, _ := json.Marshal(batch)
		store.data[fmt.Sprintf("block-%d", i)] = data
	}

	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("block-%d", i)
	}

	matchers := []index.LabelMatcher{
		{Name: "service", Value: "api", Type: index.MatchEqual},
	}

	reader := NewBlockReader(store, 8)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		eventCh, errCh := reader.ReadBlocks(context.Background(), paths, matchers)
		for range eventCh {
		}
		for range errCh {
		}
	}
}
