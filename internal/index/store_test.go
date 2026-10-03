package index

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// memStore is an in-memory ObjectStore for testing.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...) // copy
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return d, nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
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

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}

func TestStore_WriteReadRoundTrip(t *testing.T) {
	store := NewObjectStoreIndexStore(newMemStore())
	ctx := context.Background()

	idx := NewIndex("tenant-a", "2024-01-15")
	idx.AddBlock(BlockMeta{
		FilePath:   "tenant-a/2024-01-15/0-0000000001.json",
		TenantID:   "tenant-a",
		Partition:  0,
		MinTime:    time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		MaxTime:    time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
		EventCount: 100,
		Labels:     map[string]string{"service": "api"},
		SizeBytes:  2048,
	})

	if err := store.WriteIndex(ctx, idx); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	got, err := store.ReadIndex(ctx, "tenant-a", "2024-01-15")
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if got.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want tenant-a", got.TenantID)
	}
	if got.Date != "2024-01-15" {
		t.Errorf("Date = %q, want 2024-01-15", got.Date)
	}
	if len(got.Blocks) != 1 {
		t.Fatalf("Blocks count = %d, want 1", len(got.Blocks))
	}
	b := got.Blocks[0]
	if b.FilePath != "tenant-a/2024-01-15/0-0000000001.json" {
		t.Errorf("FilePath = %q", b.FilePath)
	}
	if b.EventCount != 100 {
		t.Errorf("EventCount = %d, want 100", b.EventCount)
	}
	if b.Labels["service"] != "api" {
		t.Errorf("Labels[service] = %q, want api", b.Labels["service"])
	}
}

func TestStore_ReadNonExistent(t *testing.T) {
	store := NewObjectStoreIndexStore(newMemStore())
	_, err := store.ReadIndex(context.Background(), "tenant-x", "2024-01-15")
	if err == nil {
		t.Error("expected error for non-existent index")
	}
}

func TestStore_CorruptJSON(t *testing.T) {
	ms := newMemStore()
	ms.data["tenant-a/2024-01-15/index.json"] = []byte("{invalid json")
	store := NewObjectStoreIndexStore(ms)

	_, err := store.ReadIndex(context.Background(), "tenant-a", "2024-01-15")
	if err == nil {
		t.Error("expected error for corrupt JSON")
	}
}

func TestStore_MultipleBlocks(t *testing.T) {
	store := NewObjectStoreIndexStore(newMemStore())
	ctx := context.Background()

	idx := NewIndex("tenant-a", "2024-01-15")
	for i := 0; i < 5; i++ {
		idx.AddBlock(BlockMeta{
			FilePath:   fmt.Sprintf("tenant-a/2024-01-15/0-%010d.json", i),
			TenantID:   "tenant-a",
			EventCount: 10 * (i + 1),
			MinTime:    time.Date(2024, 1, 15, i, 0, 0, 0, time.UTC),
			MaxTime:    time.Date(2024, 1, 15, i+1, 0, 0, 0, time.UTC),
			Labels:     map[string]string{"idx": fmt.Sprintf("%d", i)},
		})
	}

	if err := store.WriteIndex(ctx, idx); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	got, err := store.ReadIndex(ctx, "tenant-a", "2024-01-15")
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(got.Blocks) != 5 {
		t.Errorf("Blocks count = %d, want 5", len(got.Blocks))
	}
}
