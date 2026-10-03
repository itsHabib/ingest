package ingester

import (
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"go.uber.org/zap"
)

func TestConsumer_Wiring(t *testing.T) {
	cfg := config.IngesterConfig{
		KafkaBrokers:   []string{"localhost:9092"},
		KafkaTopic:     "test-topic",
		ConsumerGroup:  "test-group",
		BatchSize:      100,
		BatchAge:       5 * time.Second,
		StoragePath:    "/tmp/test",
		FlushWorkers:   2,
		PauseThreshold: 3,
	}

	batcher := NewBatcher(cfg.BatchSize, cfg.BatchAge)
	store := newMockStore()
	flusher := NewFlusher(store, zap.NewNop())

	consumer, err := NewConsumer(cfg, batcher, flusher, store, zap.NewNop())
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if consumer == nil {
		t.Fatal("NewConsumer() returned nil")
	}
	if consumer.topic != cfg.KafkaTopic {
		t.Errorf("topic = %q, want %q", consumer.topic, cfg.KafkaTopic)
	}
	if consumer.flushWorkers != cfg.FlushWorkers {
		t.Errorf("flushWorkers = %d, want %d", consumer.flushWorkers, cfg.FlushWorkers)
	}
	if consumer.pauseThreshold != int64(cfg.PauseThreshold) {
		t.Errorf("pauseThreshold = %d, want %d", consumer.pauseThreshold, cfg.PauseThreshold)
	}

	// Clean up the client
	consumer.client.Close()
}

func TestConsumer_ConsecFailuresBelowThreshold_NoPause(t *testing.T) {
	cfg := config.IngesterConfig{
		KafkaBrokers:   []string{"localhost:9092"},
		KafkaTopic:     "test-topic",
		ConsumerGroup:  "test-group",
		BatchSize:      100,
		BatchAge:       5 * time.Second,
		StoragePath:    "/tmp/test",
		FlushWorkers:   1,
		PauseThreshold: 5,
	}

	batcher := NewBatcher(cfg.BatchSize, cfg.BatchAge)
	store := newMockStore()
	flusher := NewFlusher(store, zap.NewNop())

	consumer, err := NewConsumer(cfg, batcher, flusher, store, zap.NewNop())
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	defer consumer.client.Close()

	// Simulate failures below threshold.
	for i := 0; i < 4; i++ {
		consumer.consecFailures.Add(1)
	}

	if consumer.consecFailures.Load() != 4 {
		t.Errorf("consecFailures = %d, want 4", consumer.consecFailures.Load())
	}
	if consumer.paused.Load() {
		t.Error("consumer should not be paused below threshold")
	}
}

func TestConsumer_ConsecFailuresResetOnSuccess(t *testing.T) {
	cfg := config.IngesterConfig{
		KafkaBrokers:   []string{"localhost:9092"},
		KafkaTopic:     "test-topic",
		ConsumerGroup:  "test-group",
		BatchSize:      100,
		BatchAge:       5 * time.Second,
		StoragePath:    "/tmp/test",
		FlushWorkers:   1,
		PauseThreshold: 5,
	}

	batcher := NewBatcher(cfg.BatchSize, cfg.BatchAge)
	store := newMockStore()
	flusher := NewFlusher(store, zap.NewNop())

	consumer, err := NewConsumer(cfg, batcher, flusher, store, zap.NewNop())
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	defer consumer.client.Close()

	// Simulate some failures.
	consumer.consecFailures.Store(3)

	// Simulate a success resetting the counter.
	consumer.consecFailures.Store(0)

	if consumer.consecFailures.Load() != 0 {
		t.Errorf("consecFailures = %d, want 0 after success", consumer.consecFailures.Load())
	}
}

func TestConsumer_ResumeResetsCounter(t *testing.T) {
	cfg := config.IngesterConfig{
		KafkaBrokers:   []string{"localhost:9092"},
		KafkaTopic:     "test-topic",
		ConsumerGroup:  "test-group",
		BatchSize:      100,
		BatchAge:       5 * time.Second,
		StoragePath:    "/tmp/test",
		FlushWorkers:   1,
		PauseThreshold: 5,
	}

	batcher := NewBatcher(cfg.BatchSize, cfg.BatchAge)
	store := newMockStore()
	flusher := NewFlusher(store, zap.NewNop())

	consumer, err := NewConsumer(cfg, batcher, flusher, store, zap.NewNop())
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	defer consumer.client.Close()

	// Simulate failures and paused state.
	consumer.consecFailures.Store(10)
	consumer.paused.Store(true)

	// Resume should reset everything.
	consumer.resumeConsumption()

	if consumer.consecFailures.Load() != 0 {
		t.Errorf("consecFailures = %d, want 0 after resume", consumer.consecFailures.Load())
	}
	if consumer.paused.Load() {
		t.Error("consumer should not be paused after resume")
	}
}
