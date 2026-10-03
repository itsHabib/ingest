package testing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/storage"
)

// Compile-time interface check (also in fakes.go, but verifying in test too).
var _ storage.ObjectStore = (*FakeObjectStore)(nil)

func TestFakeObjectStore_PutGet(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	data := []byte("hello world")
	if err := store.Put(ctx, "key1", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestFakeObjectStore_PutOverwrites(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	if err := store.Put(ctx, "key1", []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := store.Put(ctx, "key1", []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	got, err := store.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("got %q, want %q", got, "v2")
	}
}

func TestFakeObjectStore_GetMissing(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	_, err := store.Get(ctx, "no-such-key")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestFakeObjectStore_SetError(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	injected := fmt.Errorf("injected error")
	store.SetError(injected)

	if err := store.Put(ctx, "key", []byte("data")); err != injected {
		t.Errorf("Put: got %v, want %v", err, injected)
	}
	if _, err := store.Get(ctx, "key"); err != injected {
		t.Errorf("Get: got %v, want %v", err, injected)
	}
	if _, err := store.List(ctx, ""); err != injected {
		t.Errorf("List: got %v, want %v", err, injected)
	}
	if err := store.Delete(ctx, "key"); err != injected {
		t.Errorf("Delete: got %v, want %v", err, injected)
	}
	if _, err := store.Exists(ctx, "key"); err != injected {
		t.Errorf("Exists: got %v, want %v", err, injected)
	}
}

func TestFakeObjectStore_ClearError(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	store.SetError(fmt.Errorf("fail"))
	store.ClearError()

	if err := store.Put(ctx, "key", []byte("data")); err != nil {
		t.Errorf("Put after ClearError: %v", err)
	}
	got, err := store.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get after ClearError: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("got %q, want %q", got, "data")
	}
}

func TestFakeObjectStore_SetDelay(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	store.SetDelay(50 * time.Millisecond)

	start := time.Now()
	_ = store.Put(ctx, "key", []byte("data"))
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Errorf("Put took %v, expected at least ~50ms delay", elapsed)
	}
}

func TestFakeObjectStore_CallTracking(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	if store.PutCalls() != 0 {
		t.Errorf("initial PutCalls = %d, want 0", store.PutCalls())
	}
	if store.GetCalls() != 0 {
		t.Errorf("initial GetCalls = %d, want 0", store.GetCalls())
	}

	_ = store.Put(ctx, "k1", []byte("d1"))
	_ = store.Put(ctx, "k2", []byte("d2"))
	_, _ = store.Get(ctx, "k1")

	if store.PutCalls() != 2 {
		t.Errorf("PutCalls = %d, want 2", store.PutCalls())
	}
	if store.GetCalls() != 1 {
		t.Errorf("GetCalls = %d, want 1", store.GetCalls())
	}
}

func TestFakeObjectStore_CallTrackingCountsOnError(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	store.SetError(fmt.Errorf("fail"))
	_ = store.Put(ctx, "k", []byte("d"))
	_, _ = store.Get(ctx, "k")

	if store.PutCalls() != 1 {
		t.Errorf("PutCalls = %d, want 1 (should count even on error)", store.PutCalls())
	}
	if store.GetCalls() != 1 {
		t.Errorf("GetCalls = %d, want 1 (should count even on error)", store.GetCalls())
	}
}

func TestFakeObjectStore_List(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	_ = store.Put(ctx, "tenant-a/file1", []byte("1"))
	_ = store.Put(ctx, "tenant-a/file2", []byte("2"))
	_ = store.Put(ctx, "tenant-b/file1", []byte("3"))

	keys, err := store.List(ctx, "tenant-a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestFakeObjectStore_ListSorted(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	_ = store.Put(ctx, "c", []byte("3"))
	_ = store.Put(ctx, "a", []byte("1"))
	_ = store.Put(ctx, "b", []byte("2"))

	keys, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	for i := 1; i < len(keys); i++ {
		if keys[i] < keys[i-1] {
			t.Errorf("keys not sorted: %v", keys)
			break
		}
	}
}

func TestFakeObjectStore_DeleteIdempotent(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	if err := store.Delete(ctx, "nonexistent"); err != nil {
		t.Errorf("Delete nonexistent should be nil, got: %v", err)
	}
}

func TestFakeObjectStore_Exists(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	exists, err := store.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected false before Put")
	}

	_ = store.Put(ctx, "key", []byte("data"))

	exists, err = store.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected true after Put")
	}
}

func TestFakeObjectStore_Concurrent(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			data := []byte(fmt.Sprintf("data-%d", id))

			_ = store.Put(ctx, key, data)
			_, _ = store.Get(ctx, key)
			_, _ = store.List(ctx, "key-")
			_, _ = store.Exists(ctx, key)
			_ = store.Delete(ctx, fmt.Sprintf("key-%d", id+goroutines))
		}(i)
	}

	wg.Wait()
}

func TestFakeObjectStore_DataIsolation(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	ctx := context.Background()

	original := []byte("original")
	if err := store.Put(ctx, "key", original); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Mutate the original slice -- should not affect stored data.
	original[0] = 'X'

	got, err := store.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "original" {
		t.Errorf("stored data was mutated: got %q", got)
	}

	// Mutate the returned slice -- should not affect stored data.
	got[0] = 'Y'

	got2, _ := store.Get(ctx, "key")
	if string(got2) != "original" {
		t.Errorf("stored data was mutated via returned slice: got %q", got2)
	}
}
