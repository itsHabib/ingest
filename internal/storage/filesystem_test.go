package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *FilesystemStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFilesystemStore(dir)
	if err != nil {
		t.Fatalf("NewFilesystemStore: %v", err)
	}
	return store
}

func TestPutGetRoundtrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte(`{"events":[{"value":42}]}`)
	if err := store.Put(ctx, "tenant-a/2026-02-28/0-0000000001.json", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, "tenant-a/2026-02-28/0-0000000001.json")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("roundtrip mismatch: got %q, want %q", got, data)
	}
}

func TestGetMissing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Get(ctx, "no-such-key")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got: %v", err)
	}
}

func TestListWithPrefix(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	keys := []string{
		"tenant-a/2026-02-28/0-0000000001.json",
		"tenant-a/2026-02-28/0-0000000002.json",
		"tenant-b/2026-02-28/0-0000000001.json",
	}
	for _, k := range keys {
		if err := store.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	got, err := store.List(ctx, "tenant-a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(got), got)
	}
	for _, k := range got {
		if k != keys[0] && k != keys[1] {
			t.Errorf("unexpected key: %s", k)
		}
	}
}

func TestListEmpty(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	got, err := store.List(ctx, "no-match/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty list, got: %v", got)
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	key := "tenant-a/2026-02-28/0-0000000001.json"
	if err := store.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := store.Get(ctx, key)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist after delete, got: %v", err)
	}
}

func TestDeleteMissing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.Delete(ctx, "no-such-key"); err != nil {
		t.Errorf("Delete non-existent key should return nil, got: %v", err)
	}
}

func TestExists(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	key := "tenant-a/2026-02-28/0-0000000001.json"

	exists, err := store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected false before Put")
	}

	if err := store.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	exists, err = store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected true after Put")
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	exists, err = store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected false after Delete")
	}
}

func TestTenantIsolation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}
	for _, tenant := range tenants {
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%s/2026-02-28/0-%010d.json", tenant, i)
			if err := store.Put(ctx, key, []byte(tenant)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}

	for _, tenant := range tenants {
		keys, err := store.List(ctx, tenant+"/")
		if err != nil {
			t.Fatalf("List(%s): %v", tenant, err)
		}
		if len(keys) != 3 {
			t.Errorf("tenant %s: expected 3 keys, got %d", tenant, len(keys))
		}
		for _, k := range keys {
			if k[:len(tenant)] != tenant {
				t.Errorf("tenant %s: got key from another tenant: %s", tenant, k)
			}
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("tenant-%d/2026-02-28/0-%010d.json", id, id)
			data := []byte(fmt.Sprintf("data-%d", id))
			if err := store.Put(ctx, key, data); err != nil {
				errs <- fmt.Errorf("goroutine %d Put: %w", id, err)
				return
			}
			got, err := store.Get(ctx, key)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d Get: %w", id, err)
				return
			}
			if string(got) != string(data) {
				errs <- fmt.Errorf("goroutine %d: got %q, want %q", id, got, data)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestBatchKey(t *testing.T) {
	tests := []struct {
		name      string
		tenantID  string
		date      time.Time
		partition int32
		offset    int64
		want      string
	}{
		{
			name:      "basic",
			tenantID:  "tenant-a",
			date:      time.Date(2026, 2, 28, 15, 30, 0, 0, time.UTC),
			partition: 3,
			offset:    42,
			want:      "tenant-a/2026-02-28/3-0000000042.json",
		},
		{
			name:      "zero offset",
			tenantID:  "tenant-b",
			date:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			partition: 0,
			offset:    0,
			want:      "tenant-b/2026-01-01/0-0000000000.json",
		},
		{
			name:      "large offset",
			tenantID:  "tenant-c",
			date:      time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
			partition: 5,
			offset:    9999999999,
			want:      "tenant-c/2025-12-31/5-9999999999.json",
		},
		{
			name:      "max int64 offset extends beyond 10 digits",
			tenantID:  "tenant-d",
			date:      time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
			partition: 1,
			offset:    math.MaxInt64,
			want:      "tenant-d/2026-06-15/1-9223372036854775807.json",
		},
		{
			name:      "non-UTC date is converted",
			tenantID:  "tenant-e",
			date:      time.Date(2026, 3, 1, 2, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			partition: 2,
			offset:    100,
			want:      "tenant-e/2026-03-01/2-0000000100.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BatchKey(tt.tenantID, tt.date, tt.partition, tt.offset)
			if err != nil {
				t.Fatalf("BatchKey: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBatchKeyEmptyTenant(t *testing.T) {
	_, err := BatchKey("", time.Now(), 0, 0)
	if err == nil {
		t.Fatal("expected error for empty tenantID")
	}
}

func TestPutEmptyKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.Put(ctx, "", []byte("data"))
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestDeeplyNestedPaths(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	key := "a/b/c/d/e/f/g/file.json"
	data := []byte("deeply nested data")

	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}

	keys, err := store.List(ctx, "a/b/c/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Errorf("List = %v, want [%s]", keys, key)
	}
}

func TestSpecialCharactersInKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	tests := []struct {
		name string
		key  string
	}{
		{"hyphens", "tenant-a/2026-02-28/file-name.json"},
		{"underscores", "tenant_a/2026_02_28/file_name.json"},
		{"spaces", "tenant a/file name.json"},
		{"unicode", "tenant/\u00e9v\u00e9nement/donn\u00e9es.json"},
		{"dots", "tenant/2026.02.28/file.v2.json"},
		{"mixed", "my-tenant_1/2026.02/data-file_v2.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("content for " + tt.key)
			if err := store.Put(ctx, tt.key, data); err != nil {
				t.Fatalf("Put(%q): %v", tt.key, err)
			}
			got, err := store.Get(ctx, tt.key)
			if err != nil {
				t.Fatalf("Get(%q): %v", tt.key, err)
			}
			if string(got) != string(data) {
				t.Errorf("roundtrip mismatch for %q: got %q", tt.key, got)
			}
		})
	}
}

func TestConcurrentPutSameKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const goroutines = 10
	key := "shared/same-key.json"
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("data-from-%d", id))
			if err := store.Put(ctx, key, data); err != nil {
				t.Errorf("goroutine %d Put: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// The final value should be one of the written values (no corruption).
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	s := string(got)
	if !strings.HasPrefix(s, "data-from-") {
		t.Errorf("stored data appears corrupted: %q", s)
	}
}

func TestListOrdering(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Write keys in non-sorted order
	keys := []string{
		"tenant-a/2026-02-28/0-0000000003.json",
		"tenant-a/2026-02-28/0-0000000001.json",
		"tenant-a/2026-02-28/0-0000000002.json",
	}
	for _, k := range keys {
		if err := store.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	// List multiple times and verify consistency
	var prev []string
	for i := 0; i < 5; i++ {
		got, err := store.List(ctx, "tenant-a/")
		if err != nil {
			t.Fatalf("List (iteration %d): %v", i, err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 keys, got %d", len(got))
		}
		if prev != nil {
			for j := range got {
				if got[j] != prev[j] {
					t.Errorf("List ordering inconsistent: iteration %d got %v, previous %v", i, got, prev)
					break
				}
			}
		}
		prev = got
	}
}

func TestPutOverwrites(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	key := "tenant-a/file.json"

	if err := store.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := store.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("got %q, want %q", got, "v2")
	}
}

// --- Benchmarks ---

func BenchmarkPut(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			dir := b.TempDir()
			store, err := NewFilesystemStore(dir)
			if err != nil {
				b.Fatalf("NewFilesystemStore: %v", err)
			}
			ctx := context.Background()
			data := make([]byte, sz.size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("tenant/2026-02-28/0-%010d.json", i)
				if err := store.Put(ctx, key, data); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	store, err := NewFilesystemStore(dir)
	if err != nil {
		b.Fatalf("NewFilesystemStore: %v", err)
	}
	ctx := context.Background()
	data := make([]byte, 10*1024)

	const numFiles = 100
	keys := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		keys[i] = fmt.Sprintf("tenant/2026-02-28/0-%010d.json", i)
		if err := store.Put(ctx, keys[i], data); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(ctx, keys[i%numFiles]); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	dir := b.TempDir()
	store, err := NewFilesystemStore(dir)
	if err != nil {
		b.Fatalf("NewFilesystemStore: %v", err)
	}
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("tenant/2026-02-28/0-%010d.json", i)
		if err := store.Put(ctx, key, []byte("x")); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.List(ctx, "tenant/"); err != nil {
			b.Fatalf("List: %v", err)
		}
	}
}
