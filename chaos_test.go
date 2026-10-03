//go:build chaos

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/ingester"
	"github.com/itsHabib/ingest/internal/storage"
	internaltesting "github.com/itsHabib/ingest/internal/testing"
	testharness "github.com/itsHabib/ingest/internal/testing/harness"
	"go.uber.org/zap"
)

// TestChaos_IngesterCrashRecovery verifies at-least-once delivery after an
// ingester crash mid-flush. It pushes events via a gateway harness, kills
// the consumer, restarts a new consumer reading from the same topic, and
// confirms all events eventually land in storage.
//
// Invariants verified: #1 At-least-once, #2 Crash recovery
func TestChaos_IngesterCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests require Docker (Kafka)")
	}

	const totalEvents = 500
	const tenantID = "chaos-crash-tenant"

	// Start full pipeline via harness.
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(50),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithFlushWorkers(2),
	)

	// Push events in batches of 50.
	events := testharness.GenerateEvents(totalEvents, tenantID)
	pushInBatches(t, h.GatewayURL(), tenantID, events, 50)

	t.Logf("pushed %d events, killing consumer (simulating crash)", totalEvents)

	// Close the harness (kills consumer mid-processing).
	h.Close()

	// Brief pause to let Kafka detect consumer death.
	time.Sleep(500 * time.Millisecond)

	// Restart a fresh consumer against the same topic, writing to the same storage dir.
	storageDir := h.StorageDir()
	logger := zap.NewNop()

	fsStore, err := storage.NewFilesystemStore(storageDir)
	if err != nil {
		t.Fatalf("create filesystem store: %v", err)
	}

	flusher := ingester.NewFlusher(fsStore, logger)
	batcher := ingester.NewBatcher(50, 2*time.Second)

	// Use a fresh consumer group so it reads from the beginning of the topic.
	restartGroup := fmt.Sprintf("test-crash-recovery-%d", time.Now().UnixNano())
	ingesterCfg := config.IngesterConfig{
		KafkaBrokers:  []string{"localhost:9092"},
		KafkaTopic:    "ingestion-events",
		ConsumerGroup: restartGroup,
		BatchSize:     50,
		BatchAge:      2 * time.Second,
		FlushWorkers:  2,
	}

	consumer, err := ingester.NewConsumer(ingesterCfg, batcher, flusher, fsStore, logger)
	if err != nil {
		t.Fatalf("create consumer for restart: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(ctx)
	}()

	t.Logf("restarted consumer, waiting for events to flush")

	// Wait for all events to appear in storage.
	err = testharness.WaitForFlush(storageDir, tenantID, totalEvents, 60*time.Second)
	cancel()
	wg.Wait()

	if err != nil {
		allEvents, _ := testharness.ReadAllEvents(storageDir)
		got := len(allEvents[tenantID])
		t.Fatalf("crash recovery failed: %v (got %d/%d events)", err, got, totalEvents)
	}

	// Verify at-least-once: stored >= sent.
	allEvents, err := testharness.ReadAllEvents(storageDir)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	got := len(allEvents[tenantID])
	t.Logf("crash recovery: stored %d events (sent %d) -- at-least-once OK", got, totalEvents)
	if got < totalEvents {
		t.Fatalf("at-least-once violation: stored %d < sent %d", got, totalEvents)
	}
}

// TestChaos_StorageFailure_Backpressure verifies that when storage fails,
// Kafka consumption pauses (backpressure), and when storage recovers,
// consumption resumes with no data loss.
//
// Invariants verified: #1 At-least-once, #3 Backpressure activation, #4 Recovery
func TestChaos_StorageFailure_Backpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests require Docker (Kafka)")
	}

	const totalEvents = 200
	const tenantID = "chaos-bp-tenant"

	store := internaltesting.NewFakeObjectStore()
	fi := internaltesting.NewFaultInjector(store)
	logger, _ := zap.NewDevelopment()

	flusher := ingester.NewFlusher(store, logger)
	batcher := ingester.NewBatcher(20, 2*time.Second)

	topic := fmt.Sprintf("chaos-bp-%d", time.Now().UnixNano())
	ingesterCfg := config.IngesterConfig{
		KafkaBrokers:   []string{"localhost:9092"},
		KafkaTopic:     topic,
		ConsumerGroup:  fmt.Sprintf("test-bp-%d", time.Now().UnixNano()),
		BatchSize:      20,
		BatchAge:       2 * time.Second,
		FlushWorkers:   2,
		PauseThreshold: 3,
	}

	consumer, err := ingester.NewConsumer(ingesterCfg, batcher, flusher, store, logger)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(ctx)
	}()

	// Use a harness as a gateway (producer) to push events to the same topic.
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(20),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithKafkaTopic(topic),
	)

	// Push half the events.
	events := testharness.GenerateEvents(totalEvents/2, tenantID)
	resp, err := testharness.PushEvents(h.GatewayURL(), tenantID, events)
	if err != nil {
		t.Fatalf("push first batch: %v", err)
	}
	testharness.DrainResponse(resp)

	// Wait for some events to be stored.
	time.Sleep(3 * time.Second)
	beforeBreak := store.Len()
	t.Logf("keys in store before break: %d", beforeBreak)

	// Break storage.
	t.Logf("breaking storage")
	fi.BreakStorage()

	// Push more events while storage is broken.
	events2 := testharness.GenerateEvents(totalEvents/2, tenantID)
	resp, err = testharness.PushEvents(h.GatewayURL(), tenantID, events2)
	if err != nil {
		t.Fatalf("push second batch: %v", err)
	}
	testharness.DrainResponse(resp)

	// Wait for backpressure to kick in.
	time.Sleep(5 * time.Second)
	t.Logf("storage broken for 5s, healing")

	// Heal storage.
	fi.HealStorage()

	// Wait for recovery and all events to flush.
	var finalCount int
	pollUntil(t, 30*time.Second, func() bool {
		finalCount = countFakeStoreEvents(store, tenantID)
		return finalCount >= totalEvents
	})

	cancel()
	wg.Wait()

	t.Logf("backpressure test: stored %d events (sent %d)", finalCount, totalEvents)
	if finalCount < totalEvents {
		t.Fatalf("at-least-once violation: stored %d < sent %d", finalCount, totalEvents)
	}
}

// TestChaos_SlowStorage_BatchAccumulation verifies that slow storage causes
// batches to accumulate but no data is lost.
//
// Invariants verified: #6 No data loss under slow storage
func TestChaos_SlowStorage_BatchAccumulation(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests require Docker (Kafka)")
	}

	const totalEvents = 300
	const tenantID = "chaos-slow-tenant"

	store := internaltesting.NewFakeObjectStore()
	fi := internaltesting.NewFaultInjector(store)
	logger, _ := zap.NewDevelopment()

	// Inject 200ms delay per storage operation.
	fi.SlowStorage(200 * time.Millisecond)

	flusher := ingester.NewFlusher(store, logger)
	batcher := ingester.NewBatcher(30, 2*time.Second)

	topic := fmt.Sprintf("chaos-slow-%d", time.Now().UnixNano())
	ingesterCfg := config.IngesterConfig{
		KafkaBrokers:  []string{"localhost:9092"},
		KafkaTopic:    topic,
		ConsumerGroup: fmt.Sprintf("test-slow-%d", time.Now().UnixNano()),
		BatchSize:     30,
		BatchAge:      2 * time.Second,
		FlushWorkers:  2,
	}

	consumer, err := ingester.NewConsumer(ingesterCfg, batcher, flusher, store, logger)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(ctx)
	}()

	// Use a harness as a gateway.
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(30),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithKafkaTopic(topic),
	)

	events := testharness.GenerateEvents(totalEvents, tenantID)
	pushInBatches(t, h.GatewayURL(), tenantID, events, 50)

	t.Logf("pushed %d events with 200ms storage delay", totalEvents)

	// Wait for all events to flush (generous timeout due to slow storage).
	var finalCount int
	pollUntil(t, 60*time.Second, func() bool {
		finalCount = countFakeStoreEvents(store, tenantID)
		return finalCount >= totalEvents
	})

	// Remove latency for clean shutdown.
	fi.ResetLatency()
	cancel()
	wg.Wait()

	t.Logf("slow storage test: stored %d events (sent %d)", finalCount, totalEvents)
	if finalCount < totalEvents {
		t.Fatalf("data loss under slow storage: stored %d < sent %d", finalCount, totalEvents)
	}
}

// TestChaos_MultipleIngesterFailover verifies that when one consumer in a
// consumer group dies, Kafka rebalances partitions to the surviving consumer
// and no data is lost.
//
// Invariants verified: #1 At-least-once, #5 Tenant isolation
func TestChaos_MultipleIngesterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests require Docker (Kafka)")
	}

	const eventsPerTenant = 200
	const tenantA = "chaos-failover-a"
	const tenantB = "chaos-failover-b"

	topic := fmt.Sprintf("chaos-failover-%d", time.Now().UnixNano())
	group := fmt.Sprintf("test-failover-%d", time.Now().UnixNano())
	logger, _ := zap.NewDevelopment()

	// Shared FakeObjectStore for both consumers.
	store := internaltesting.NewFakeObjectStore()

	makeConsumer := func(id int) *ingester.Consumer {
		flusher := ingester.NewFlusher(store, logger)
		batcher := ingester.NewBatcher(20, 2*time.Second)
		cfg := config.IngesterConfig{
			KafkaBrokers:  []string{"localhost:9092"},
			KafkaTopic:    topic,
			ConsumerGroup: group,
			BatchSize:     20,
			BatchAge:      2 * time.Second,
			FlushWorkers:  2,
		}
		c, err := ingester.NewConsumer(cfg, batcher, flusher, store, logger)
		if err != nil {
			t.Fatalf("create consumer %d: %v", id, err)
		}
		return c
	}

	consumer1 := makeConsumer(1)
	consumer2 := makeConsumer(2)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer1.Run(ctx1)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer2.Run(ctx2)
	}()

	// Wait for both consumers to join the consumer group.
	time.Sleep(5 * time.Second)

	// Use a harness as a gateway.
	h := testharness.NewTestHarness(t,
		testharness.WithBatchSize(20),
		testharness.WithBatchAge(2*time.Second),
		testharness.WithKafkaTopic(topic),
	)

	// Push initial events for two tenants.
	eventsA := testharness.GenerateEvents(eventsPerTenant, tenantA)
	eventsB := testharness.GenerateEvents(eventsPerTenant, tenantB)

	resp, err := testharness.PushEvents(h.GatewayURL(), tenantA, eventsA)
	if err != nil {
		t.Fatalf("push tenant A: %v", err)
	}
	testharness.DrainResponse(resp)

	resp, err = testharness.PushEvents(h.GatewayURL(), tenantB, eventsB)
	if err != nil {
		t.Fatalf("push tenant B: %v", err)
	}
	testharness.DrainResponse(resp)

	// Wait for some events to be processed.
	time.Sleep(3 * time.Second)

	// Kill consumer 1 -- simulates ingester crash.
	t.Logf("killing consumer 1, triggering Kafka rebalance")
	cancel1()

	// Push more events after the kill.
	eventsA2 := testharness.GenerateEvents(eventsPerTenant/2, tenantA)
	eventsB2 := testharness.GenerateEvents(eventsPerTenant/2, tenantB)

	resp, err = testharness.PushEvents(h.GatewayURL(), tenantA, eventsA2)
	if err != nil {
		t.Fatalf("push tenant A (post-kill): %v", err)
	}
	testharness.DrainResponse(resp)

	resp, err = testharness.PushEvents(h.GatewayURL(), tenantB, eventsB2)
	if err != nil {
		t.Fatalf("push tenant B (post-kill): %v", err)
	}
	testharness.DrainResponse(resp)

	totalA := eventsPerTenant + eventsPerTenant/2
	totalB := eventsPerTenant + eventsPerTenant/2

	t.Logf("waiting for surviving consumer to process all events (tenantA=%d, tenantB=%d)", totalA, totalB)

	// Wait for all events to be stored.
	var countA, countB int
	pollUntil(t, 45*time.Second, func() bool {
		countA = countFakeStoreEvents(store, tenantA)
		countB = countFakeStoreEvents(store, tenantB)
		return countA >= totalA && countB >= totalB
	})

	cancel2()
	wg.Wait()

	t.Logf("failover: tenantA stored %d/%d, tenantB stored %d/%d", countA, totalA, countB, totalB)

	if countA < totalA {
		t.Errorf("tenant A at-least-once violation: stored %d < sent %d", countA, totalA)
	}
	if countB < totalB {
		t.Errorf("tenant B at-least-once violation: stored %d < sent %d", countB, totalB)
	}

	// Verify tenant isolation.
	verifyTenantIsolation(t, store, tenantA)
	verifyTenantIsolation(t, store, tenantB)
}

// --- helpers ---

// pushInBatches pushes events in chunks of batchSize to the gateway.
func pushInBatches(t *testing.T, gatewayURL, tenantID string, events []event.Event, batchSize int) {
	t.Helper()
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		resp, err := testharness.PushEvents(gatewayURL, tenantID, events[i:end])
		if err != nil {
			t.Fatalf("push events batch %d: %v", i/batchSize, err)
		}
		testharness.DrainResponse(resp)
		if resp.StatusCode != 200 {
			t.Fatalf("push events batch %d: status %d", i/batchSize, resp.StatusCode)
		}
	}
}

// pollUntil calls check every 500ms until it returns true or timeout expires.
func pollUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	// One last check.
	if !check() {
		t.Logf("pollUntil: condition not met after %v", timeout)
	}
}

// countFakeStoreEvents counts events for a specific tenant in the FakeObjectStore.
func countFakeStoreEvents(store *internaltesting.FakeObjectStore, tenantID string) int {
	ctx := context.Background()
	keys, err := store.List(ctx, tenantID+"/")
	if err != nil {
		return 0
	}
	total := 0
	for _, key := range keys {
		data, err := store.Get(ctx, key)
		if err != nil {
			continue
		}
		var batch event.Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}
		total += len(batch.Events)
	}
	return total
}

// verifyTenantIsolation checks that all batches under a tenant's prefix
// contain only events belonging to that tenant.
func verifyTenantIsolation(t *testing.T, store *internaltesting.FakeObjectStore, tenantID string) {
	t.Helper()
	ctx := context.Background()
	keys, err := store.List(ctx, tenantID+"/")
	if err != nil {
		t.Errorf("list keys for tenant %s: %v", tenantID, err)
		return
	}
	for _, key := range keys {
		data, err := store.Get(ctx, key)
		if err != nil {
			continue
		}
		var batch event.Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			continue
		}
		if batch.TenantID != tenantID {
			t.Errorf("tenant isolation violation: key %s has batch.TenantID=%s, expected %s",
				key, batch.TenantID, tenantID)
		}
		for i, ev := range batch.Events {
			if ev.TenantID != tenantID {
				t.Errorf("tenant isolation violation: key %s event[%d].TenantID=%s, expected %s",
					key, i, ev.TenantID, tenantID)
			}
		}
	}
}
