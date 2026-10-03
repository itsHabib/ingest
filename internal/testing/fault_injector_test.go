package testing

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestFaultInjector_BreakAndHeal(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	fi := NewFaultInjector(store)
	ctx := context.Background()

	// Normal operation.
	if err := store.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Put before break: %v", err)
	}

	// Break storage.
	fi.BreakStorage()
	if err := store.Put(ctx, "k2", []byte("v2")); err == nil {
		t.Fatal("expected error after BreakStorage, got nil")
	}
	if _, err := store.Get(ctx, "k1"); err == nil {
		t.Fatal("expected error on Get after BreakStorage, got nil")
	}

	// Heal storage.
	fi.HealStorage()
	if err := store.Put(ctx, "k3", []byte("v3")); err != nil {
		t.Fatalf("Put after HealStorage: %v", err)
	}
	got, err := store.Get(ctx, "k3")
	if err != nil {
		t.Fatalf("Get after HealStorage: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("expected v3, got %s", string(got))
	}
}

func TestFaultInjector_SlowAndReset(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	fi := NewFaultInjector(store)
	ctx := context.Background()

	// Inject 50ms delay.
	fi.SlowStorage(50 * time.Millisecond)

	start := time.Now()
	if err := store.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Put with slow storage: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected at least 40ms delay, got %v", elapsed)
	}

	// Reset latency.
	fi.ResetLatency()
	start = time.Now()
	if err := store.Put(ctx, "k2", []byte("v2")); err != nil {
		t.Fatalf("Put after ResetLatency: %v", err)
	}
	elapsed = time.Since(start)
	if elapsed > 30*time.Millisecond {
		t.Fatalf("expected fast Put after ResetLatency, got %v", elapsed)
	}
}

func TestFaultInjector_ConcurrentToggle(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	fi := NewFaultInjector(store)
	ctx := context.Background()

	var wg sync.WaitGroup
	const goroutines = 20
	const ops = 50

	// Half the goroutines toggle faults, half do operations.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				if id%2 == 0 {
					fi.BreakStorage()
					fi.HealStorage()
					fi.SlowStorage(time.Millisecond)
					fi.ResetLatency()
				} else {
					_ = store.Put(ctx, "key", []byte("val"))
					_, _ = store.Get(ctx, "key")
					_, _ = store.Exists(ctx, "key")
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestFaultInjector_StoreAccessor(t *testing.T) {
	t.Parallel()
	store := NewFakeObjectStore()
	fi := NewFaultInjector(store)
	if fi.Store() != store {
		t.Fatal("Store() did not return the underlying FakeObjectStore")
	}
}
