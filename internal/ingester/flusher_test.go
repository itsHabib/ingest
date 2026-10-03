package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/index"
)

// mockStore implements storage.ObjectStore for testing.
type mockStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	putErr  error
	putFail int // number of Put calls that should fail before succeeding
	puts    int // total Put calls made
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string][]byte)}
}

func (m *mockStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	if m.putFail > 0 {
		m.putFail--
		return m.putErr
	}
	m.data[key] = data
	return nil
}

func (m *mockStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return d, nil
}

func (m *mockStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *mockStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *mockStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}

func makeFlushJob(tenantID string, partition int32, offset int64, numEvents int) *flushJob {
	batch := event.NewBatch(tenantID, partition)
	for i := 0; i < numEvents; i++ {
		batch.Add(makeEvent(tenantID))
	}
	return &flushJob{
		batch:  &batch,
		offset: offset,
		topic:  "test-topic",
		part:   partition,
	}
}

func TestFlusher_Success(t *testing.T) {
	store := newMockStore()
	f := NewFlusher(store, zap.NewNop())

	job := makeFlushJob("tenant-a", 3, 42, 10)
	if err := f.Flush(context.Background(), job); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify something was written
	if len(store.data) != 1 {
		t.Fatalf("expected 1 key in store, got %d", len(store.data))
	}

	// Verify key format contains expected components
	for key, data := range store.data {
		if !strings.HasPrefix(key, "tenant-a/") {
			t.Errorf("key %q does not start with tenant-a/", key)
		}
		if !strings.HasSuffix(key, ".json") {
			t.Errorf("key %q does not end with .json", key)
		}
		if !strings.Contains(key, "3-") {
			t.Errorf("key %q does not contain expected partition prefix", key)
		}
		if len(data) == 0 {
			t.Error("stored data is empty")
		}
	}
}

func TestFlusher_RetryOnError(t *testing.T) {
	store := newMockStore()
	store.putErr = fmt.Errorf("transient error")
	store.putFail = 2 // fail twice, then succeed

	f := NewFlusher(store, zap.NewNop())
	job := makeFlushJob("tenant-a", 0, 1, 5)

	err := f.FlushWithRetry(context.Background(), job, 3)
	if err != nil {
		t.Fatalf("FlushWithRetry() should have succeeded after retries, got %v", err)
	}

	store.mu.Lock()
	puts := store.puts
	store.mu.Unlock()

	if puts != 3 {
		t.Errorf("expected 3 Put calls (2 fails + 1 success), got %d", puts)
	}
}

func TestFlusher_RetryExhausted(t *testing.T) {
	store := newMockStore()
	store.putErr = fmt.Errorf("permanent error")
	store.putFail = 100 // always fail

	f := NewFlusher(store, zap.NewNop())
	job := makeFlushJob("tenant-a", 0, 1, 5)

	err := f.FlushWithRetry(context.Background(), job, 2)
	if err == nil {
		t.Fatal("FlushWithRetry() should have returned error after exhausting retries")
	}

	if !strings.Contains(err.Error(), "exhausted 2 retries") {
		t.Errorf("error message = %q, should contain 'exhausted 2 retries'", err.Error())
	}

	store.mu.Lock()
	puts := store.puts
	store.mu.Unlock()

	// 0, 1, 2 = 3 attempts total (initial + 2 retries)
	if puts != 3 {
		t.Errorf("expected 3 Put calls, got %d", puts)
	}
}

func TestFlusher_CorrectStorageKey(t *testing.T) {
	store := newMockStore()
	f := NewFlusher(store, zap.NewNop())

	job := makeFlushJob("my-tenant", 5, 123, 1)
	if err := f.Flush(context.Background(), job); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	today := time.Now().UTC().Format("2006-01-02")
	expectedPrefix := fmt.Sprintf("my-tenant/%s/5-", today)

	store.mu.Lock()
	var found bool
	for k := range store.data {
		if strings.HasPrefix(k, expectedPrefix) && strings.HasSuffix(k, ".json") {
			found = true
			break
		}
	}
	store.mu.Unlock()

	if !found {
		store.mu.Lock()
		var keys []string
		for k := range store.data {
			keys = append(keys, k)
		}
		store.mu.Unlock()
		t.Errorf("expected key with prefix %q not found, got keys: %v", expectedPrefix, keys)
	}
}

func TestFlusher_BatchDeserialization(t *testing.T) {
	store := newMockStore()
	f := NewFlusher(store, zap.NewNop())

	// Create a batch with specific events
	batch := event.NewBatch("tenant-a", 3)
	ev1 := event.NewEvent("tenant-a", time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), map[string]string{"service": "api"}, 42.0)
	ev2 := event.NewEvent("tenant-a", time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC), map[string]string{"service": "web"}, 99.5)
	batch.Add(ev1)
	batch.Add(ev2)

	job := &flushJob{
		batch:  &batch,
		offset: 100,
		topic:  "test-topic",
		part:   3,
	}

	if err := f.Flush(context.Background(), job); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read back from store and deserialize
	store.mu.Lock()
	var storedData []byte
	for _, v := range store.data {
		storedData = v
		break
	}
	store.mu.Unlock()

	var recovered event.Batch
	if err := json.Unmarshal(storedData, &recovered); err != nil {
		t.Fatalf("unmarshal stored batch: %v", err)
	}

	if recovered.TenantID != "tenant-a" {
		t.Errorf("tenant = %q, want %q", recovered.TenantID, "tenant-a")
	}
	if recovered.Partition != 3 {
		t.Errorf("partition = %d, want 3", recovered.Partition)
	}
	if len(recovered.Events) != 2 {
		t.Fatalf("events count = %d, want 2", len(recovered.Events))
	}
	if recovered.Events[0].Value != 42.0 {
		t.Errorf("event[0].Value = %f, want 42.0", recovered.Events[0].Value)
	}
	if recovered.Events[1].Labels["service"] != "web" {
		t.Errorf("event[1].Labels[service] = %q, want %q", recovered.Events[1].Labels["service"], "web")
	}
}

func TestFlusher_ContextCancellation(t *testing.T) {
	store := newMockStore()
	store.putErr = fmt.Errorf("storage error")
	store.putFail = 100 // always fail

	f := NewFlusher(store, zap.NewNop())
	job := makeFlushJob("tenant-a", 0, 1, 5)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay to interrupt retry
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := f.FlushWithRetry(ctx, job, 10) // many retries, but context will cancel first
	if err == nil {
		t.Fatal("expected error when context is cancelled during retry")
	}
	if !strings.Contains(err.Error(), "context cancelled") {
		t.Logf("error = %v (may contain context cancelled or exhausted retries depending on timing)", err)
	}
}

func TestFlusher_IndexIntegration(t *testing.T) {
	store := newMockStore()
	idxStore := newMockIndexStore()
	f := NewFlusher(store, zap.NewNop())
	f.SetIndexStore(idxStore)

	batch := event.NewBatch("tenant-a", 3)
	ev1 := event.NewEvent("tenant-a", time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), map[string]string{"service": "api"}, 42.0)
	ev2 := event.NewEvent("tenant-a", time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC), map[string]string{"service": "web"}, 99.5)
	batch.Add(ev1)
	batch.Add(ev2)

	job := &flushJob{
		batch:  &batch,
		offset: 100,
		topic:  "test-topic",
		part:   3,
	}

	if err := f.Flush(context.Background(), job); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Verify index was written
	if idxStore.writeCount() != 1 {
		t.Fatalf("expected 1 index write, got %d", idxStore.writeCount())
	}

	// Read back the index and verify the block
	today := time.Now().UTC().Format("2006-01-02")
	idx := idxStore.getIndex("tenant-a", today)
	if idx == nil {
		t.Fatal("index not found for tenant-a")
	}
	if len(idx.Blocks) != 1 {
		t.Fatalf("expected 1 block in index, got %d", len(idx.Blocks))
	}
	block := idx.Blocks[0]
	if block.TenantID != "tenant-a" {
		t.Errorf("block TenantID = %q", block.TenantID)
	}
	if block.EventCount != 2 {
		t.Errorf("block EventCount = %d, want 2", block.EventCount)
	}
	if block.Labels["service"] != "web" && block.Labels["service"] != "api" {
		t.Errorf("block Labels[service] = %q", block.Labels["service"])
	}
}

func TestFlusher_IndexMultipleFlushes(t *testing.T) {
	store := newMockStore()
	idxStore := newMockIndexStore()
	f := NewFlusher(store, zap.NewNop())
	f.SetIndexStore(idxStore)

	for i := 0; i < 3; i++ {
		job := makeFlushJob("tenant-a", 0, int64(i), 5)
		if err := f.Flush(context.Background(), job); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}

	today := time.Now().UTC().Format("2006-01-02")
	idx := idxStore.getIndex("tenant-a", today)
	if idx == nil {
		t.Fatal("index not found")
	}
	if len(idx.Blocks) != 3 {
		t.Errorf("expected 3 blocks, got %d", len(idx.Blocks))
	}
}

func TestFlusher_IndexFailureDoesNotFailFlush(t *testing.T) {
	store := newMockStore()
	idxStore := newMockIndexStore()
	idxStore.failWrites = true
	f := NewFlusher(store, zap.NewNop())
	f.SetIndexStore(idxStore)

	job := makeFlushJob("tenant-a", 0, 1, 5)
	err := f.Flush(context.Background(), job)
	if err != nil {
		t.Fatalf("Flush should succeed even when index write fails, got: %v", err)
	}

	// Batch should still be in the store
	if len(store.data) != 1 {
		t.Errorf("expected 1 key in store, got %d", len(store.data))
	}
}

func TestFlusher_NoIndexStore(t *testing.T) {
	store := newMockStore()
	f := NewFlusher(store, zap.NewNop())
	// No SetIndexStore call — should work fine without index

	job := makeFlushJob("tenant-a", 0, 1, 5)
	err := f.Flush(context.Background(), job)
	if err != nil {
		t.Fatalf("Flush without index store: %v", err)
	}
}

// mockIndexStore implements index.IndexStore for testing.
type mockIndexStore struct {
	mu         sync.Mutex
	indexes    map[string]*mockIndex
	writes     int
	failWrites bool
}

type mockIndex struct {
	idx *index.Index
}

func newMockIndexStore() *mockIndexStore {
	return &mockIndexStore{indexes: make(map[string]*mockIndex)}
}

func (m *mockIndexStore) WriteIndex(_ context.Context, idx *index.Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	if m.failWrites {
		return fmt.Errorf("mock index store: write failed")
	}
	key := idx.TenantID + "/" + idx.Date
	m.indexes[key] = &mockIndex{idx: idx}
	return nil
}

func (m *mockIndexStore) ReadIndex(_ context.Context, tenantID, date string) (*index.Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + "/" + date
	mi, ok := m.indexes[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mi.idx, nil
}

func (m *mockIndexStore) writeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

func (m *mockIndexStore) getIndex(tenantID, date string) *index.Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + "/" + date
	mi, ok := m.indexes[key]
	if !ok {
		return nil
	}
	return mi.idx
}

func BenchmarkFlusher_Flush(b *testing.B) {
	store := newMockStore()
	f := NewFlusher(store, zap.NewNop())
	job := makeFlushJob("tenant-a", 0, 1, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = f.Flush(context.Background(), job)
	}
}
