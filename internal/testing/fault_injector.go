package testing

import (
	"fmt"
	"time"
)

// FaultInjector provides controlled failure injection for chaos tests.
// It wraps a FakeObjectStore and toggles error/delay state at runtime.
type FaultInjector struct {
	store *FakeObjectStore
}

// NewFaultInjector wraps a FakeObjectStore for fault injection.
func NewFaultInjector(store *FakeObjectStore) *FaultInjector {
	return &FaultInjector{store: store}
}

// BreakStorage makes all storage operations return an error.
func (fi *FaultInjector) BreakStorage() {
	fi.store.SetError(fmt.Errorf("injected: storage broken"))
}

// HealStorage restores normal storage operation.
func (fi *FaultInjector) HealStorage() {
	fi.store.SetError(nil)
}

// SlowStorage adds latency to every storage operation.
func (fi *FaultInjector) SlowStorage(delay time.Duration) {
	fi.store.SetDelay(delay)
}

// ResetLatency removes injected latency.
func (fi *FaultInjector) ResetLatency() {
	fi.store.SetDelay(0)
}

// Store returns the underlying FakeObjectStore.
func (fi *FaultInjector) Store() *FakeObjectStore {
	return fi.store
}
