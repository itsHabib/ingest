package testing

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/itsHabib/ingest/internal/storage"
)

var _ storage.ObjectStore = (*FakeObjectStore)(nil)

// FakeObjectStore is a hand-rolled in-memory implementation of storage.ObjectStore.
// It supports runtime-togglable error injection and delay injection, making it
// suitable for chaos tests that need to toggle faults mid-test while the system
// is running.
type FakeObjectStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	injErr   error
	delay    time.Duration
	putCount int
	getCount int
}

// NewFakeObjectStore creates an empty in-memory store.
func NewFakeObjectStore() *FakeObjectStore {
	return &FakeObjectStore{
		data: make(map[string][]byte),
	}
}

// SetError configures all subsequent operations to return the given error.
func (f *FakeObjectStore) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.injErr = err
}

// ClearError resets the injected error so operations resume normal behavior.
func (f *FakeObjectStore) ClearError() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.injErr = nil
}

// SetDelay adds latency to every operation.
func (f *FakeObjectStore) SetDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// PutCalls returns the total number of Put invocations.
func (f *FakeObjectStore) PutCalls() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.putCount
}

// GetCalls returns the total number of Get invocations.
func (f *FakeObjectStore) GetCalls() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getCount
}

func (f *FakeObjectStore) applyDelay() {
	f.mu.RLock()
	d := f.delay
	f.mu.RUnlock()
	if d > 0 {
		time.Sleep(d)
	}
}

func (f *FakeObjectStore) checkError() error {
	f.mu.RLock()
	err := f.injErr
	f.mu.RUnlock()
	return err
}

// Put writes data to the given key. Overwrites if the key already exists.
func (f *FakeObjectStore) Put(_ context.Context, key string, data []byte) error {
	f.applyDelay()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCount++
	if f.injErr != nil {
		return f.injErr
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.data[key] = cp
	return nil
}

// Get reads data from the given key.
func (f *FakeObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	f.applyDelay()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCount++
	if f.injErr != nil {
		return nil, f.injErr
	}
	d, ok := f.data[key]
	if !ok {
		return nil, fmt.Errorf("fake store: key %q not found", key)
	}
	cp := make([]byte, len(d))
	copy(cp, d)
	return cp, nil
}

// List returns all keys matching the given prefix, sorted lexicographically.
func (f *FakeObjectStore) List(_ context.Context, prefix string) ([]string, error) {
	f.applyDelay()
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.injErr != nil {
		return nil, f.injErr
	}
	var keys []string
	for k := range f.data {
		if len(prefix) == 0 || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// Delete removes the key. Returns nil if the key does not exist (idempotent).
func (f *FakeObjectStore) Delete(_ context.Context, key string) error {
	f.applyDelay()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.injErr != nil {
		return f.injErr
	}
	delete(f.data, key)
	return nil
}

// Exists returns true if the key exists.
func (f *FakeObjectStore) Exists(_ context.Context, key string) (bool, error) {
	f.applyDelay()
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.injErr != nil {
		return false, f.injErr
	}
	_, ok := f.data[key]
	return ok, nil
}

// Keys returns a sorted list of all keys in the store.
func (f *FakeObjectStore) Keys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Len returns the number of keys in the store.
func (f *FakeObjectStore) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.data)
}
