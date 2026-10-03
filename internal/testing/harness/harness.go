// Package harness provides a TestHarness for integration and chaos tests.
// It wires up a full gateway + ingester pipeline in-process with real Kafka.
//
// This package is separate from internal/testing to avoid import cycles:
// internal/testing is imported by production package tests (e.g., gateway),
// while this package imports production packages (gateway, ingester).
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/gateway"
	"github.com/itsHabib/ingest/internal/ingester"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/itsHabib/ingest/internal/storage"
	"github.com/itsHabib/ingest/internal/tenant"
	"go.uber.org/zap"
)

// harnessConfig holds configurable options for the TestHarness.
type harnessConfig struct {
	batchSize    int
	batchAge     time.Duration
	flushWorkers int
	rateLimit    float64
	rateBurst    int
	kafkaBrokers []string
	kafkaTopic   string
}

// HarnessOption is a functional option for configuring the TestHarness.
type HarnessOption func(*harnessConfig)

// WithBatchSize sets the max events per batch before flush.
func WithBatchSize(n int) HarnessOption {
	return func(c *harnessConfig) {
		c.batchSize = n
	}
}

// WithBatchAge sets the max batch age before flush.
func WithBatchAge(d time.Duration) HarnessOption {
	return func(c *harnessConfig) {
		c.batchAge = d
	}
}

// WithFlushWorkers sets the number of concurrent flush goroutines.
func WithFlushWorkers(n int) HarnessOption {
	return func(c *harnessConfig) {
		c.flushWorkers = n
	}
}

// WithRateLimit sets per-tenant rate limit (events/sec) and burst.
func WithRateLimit(rate float64, burst int) HarnessOption {
	return func(c *harnessConfig) {
		c.rateLimit = rate
		c.rateBurst = burst
	}
}

// WithKafkaBrokers overrides the default Kafka broker addresses.
func WithKafkaBrokers(brokers []string) HarnessOption {
	return func(c *harnessConfig) {
		c.kafkaBrokers = brokers
	}
}

// WithKafkaTopic overrides the default Kafka topic.
func WithKafkaTopic(topic string) HarnessOption {
	return func(c *harnessConfig) {
		c.kafkaTopic = topic
	}
}

// TestHarness manages a full gateway + ingester lifecycle for integration tests.
// It starts both components in-process with random free ports, temp storage, and
// configurable options. Each harness uses its own Kafka consumer group to avoid
// interference between tests.
type TestHarness struct {
	t          *testing.T
	gatewayURL string
	storageDir string
	gatewaySrv *http.Server
	consumer   *ingester.Consumer
	cancelFn   context.CancelFunc
	doneCh     chan struct{} // closed when consumer.Run returns
}

// GatewayURL returns the base URL of the gateway HTTP server.
func (h *TestHarness) GatewayURL() string {
	return h.gatewayURL
}

// StorageDir returns the temporary directory used by the filesystem store.
func (h *TestHarness) StorageDir() string {
	return h.storageDir
}

// NewTestHarness creates and starts a gateway + ingester in-process with random
// free ports and temp storage. It registers t.Cleanup(h.Close) and blocks until
// the gateway /ready endpoint responds.
func NewTestHarness(t *testing.T, opts ...HarnessOption) *TestHarness {
	t.Helper()

	cfg := &harnessConfig{
		batchSize:    10,
		batchAge:     2 * time.Second,
		flushWorkers: 2,
		kafkaBrokers: []string{"localhost:9092"},
		kafkaTopic:   "ingestion-events",
	}
	for _, opt := range opts {
		opt(cfg)
	}

	storageDir := t.TempDir()
	logger := zap.NewNop()

	// Get a free port for the gateway.
	gatewayPort := getFreePort(t)

	// Build hash ring.
	hashRing := ring.NewRing(6, 64)

	// Create Kafka producer.
	producer, err := gateway.NewKafkaProducer(cfg.kafkaBrokers, cfg.kafkaTopic)
	if err != nil {
		t.Fatalf("harness: create producer: %v", err)
	}

	// Build gateway handler.
	gatewayCfg := config.GatewayConfig{
		Port:             gatewayPort,
		KafkaBrokers:     cfg.kafkaBrokers,
		KafkaTopic:       cfg.kafkaTopic,
		MaxBodySize:      5 * 1024 * 1024,
		RingPartitions:   6,
		RingVirtualNodes: 64,
		RateLimitRate:    1000,
		RateLimitBurst:   5000,
	}

	var handlerOpts []gateway.HandlerOption
	if cfg.rateLimit > 0 && cfg.rateBurst > 0 {
		rl := tenant.NewRateLimiter(cfg.rateLimit, cfg.rateBurst)
		handlerOpts = append(handlerOpts, gateway.WithRateLimiter(rl))
		gatewayCfg.RateLimitRate = cfg.rateLimit
		gatewayCfg.RateLimitBurst = cfg.rateBurst
	}

	handler := gateway.NewHandler(producer, hashRing, gatewayCfg, logger, handlerOpts...)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	gatewaySrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", gatewayPort),
		Handler: mux,
	}

	// Start gateway HTTP server.
	go func() {
		if err := gatewaySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Errorf("harness: gateway server error: %v", err)
		}
	}()

	gatewayURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	// Wait for gateway to be ready.
	if err := waitForReady(gatewayURL+"/ready", 10*time.Second); err != nil {
		t.Fatalf("harness: gateway not ready: %v", err)
	}

	// Create filesystem store.
	store, err := storage.NewFilesystemStore(storageDir)
	if err != nil {
		t.Fatalf("harness: create filesystem store: %v", err)
	}

	// Wire up ingester pipeline.
	flusher := ingester.NewFlusher(store, logger)
	batcher := ingester.NewBatcher(cfg.batchSize, cfg.batchAge)

	// Unique consumer group per test to avoid rebalance interference.
	consumerGroup := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())

	ingesterCfg := config.IngesterConfig{
		KafkaBrokers:  cfg.kafkaBrokers,
		KafkaTopic:    cfg.kafkaTopic,
		ConsumerGroup: consumerGroup,
		BatchSize:     cfg.batchSize,
		BatchAge:      cfg.batchAge,
		StoragePath:   storageDir,
		FlushWorkers:  cfg.flushWorkers,
	}

	consumer, err := ingester.NewConsumer(ingesterCfg, batcher, flusher, store, logger)
	if err != nil {
		t.Fatalf("harness: create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		if err := consumer.Run(ctx); err != nil {
			t.Errorf("harness: consumer error: %v", err)
		}
	}()

	h := &TestHarness{
		t:          t,
		gatewayURL: gatewayURL,
		storageDir: storageDir,
		gatewaySrv: gatewaySrv,
		consumer:   consumer,
		cancelFn:   cancel,
		doneCh:     doneCh,
	}

	t.Cleanup(func() {
		h.Close()
		if err := producer.Close(); err != nil {
			t.Errorf("harness: producer close error: %v", err)
		}
	})

	// Warm-up: push a canary event and wait for it to be flushed.
	// This ensures the consumer has fully joined Kafka before the test starts.
	canaryEvents := GenerateEvents(1, "__canary__")
	resp, err := PushEvents(gatewayURL, "__canary__", canaryEvents)
	if err != nil {
		t.Fatalf("harness: canary push: %v", err)
	}
	DrainResponse(resp)
	if err := WaitForFlush(storageDir, "__canary__", 1, 30*time.Second); err != nil {
		t.Fatalf("harness: consumer never joined Kafka: %v", err)
	}

	// Clear storage so canary data doesn't interfere with the test.
	entries, _ := os.ReadDir(storageDir)
	for _, e := range entries {
		os.RemoveAll(filepath.Join(storageDir, e.Name()))
	}

	return h
}

// Close performs graceful shutdown of the gateway and ingester.
func (h *TestHarness) Close() {
	h.t.Helper()

	// Shutdown gateway HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.gatewaySrv.Shutdown(ctx); err != nil {
		h.t.Errorf("harness: gateway shutdown error: %v", err)
	}

	// Cancel the consumer context to trigger graceful drain.
	h.cancelFn()

	// Wait for consumer to finish draining with a timeout.
	select {
	case <-h.doneCh:
	case <-time.After(30 * time.Second):
		h.t.Errorf("harness: consumer did not shut down within 30s")
	}
}

// getFreePort asks the OS for an available port.
func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("harness: get free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// waitForReady polls the given URL until it returns HTTP 200, or timeout.
func waitForReady(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s to become ready", url)
}

// PushEvents marshals events to JSON and POSTs them to the gateway /api/v1/push
// endpoint with the given tenant ID in the X-Scope-OrgID header.
func PushEvents(gatewayURL, tenantID string, events []event.Event) (*http.Response, error) {
	data, err := json.Marshal(events)
	if err != nil {
		return nil, fmt.Errorf("push: marshal events: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/api/v1/push", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("push: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	return client.Do(req)
}

// ReadAllEvents walks the storage directory, deserializes all JSON batch files,
// and returns events grouped by tenant ID.
func ReadAllEvents(storageDir string) (map[string][]event.Event, error) {
	result := make(map[string][]event.Event)

	err := filepath.WalkDir(storageDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".json") {
			return nil
		}
		// Skip temp files from atomic writes.
		if strings.HasPrefix(filepath.Base(path), ".tmp-") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		var batch event.Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			return fmt.Errorf("unmarshal %s: %w", path, err)
		}

		result[batch.TenantID] = append(result[batch.TenantID], batch.Events...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// WaitForFlush polls the storage directory until at least minEvents are found
// for the given tenant, or until timeout expires.
func WaitForFlush(storageDir, tenantID string, minEvents int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := ReadAllEvents(storageDir)
		if err == nil && len(events[tenantID]) >= minEvents {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	// One last check to give the final count in the error message.
	events, _ := ReadAllEvents(storageDir)
	return fmt.Errorf("timeout: tenant %q has %d events, wanted >= %d",
		tenantID, len(events[tenantID]), minEvents)
}

// WaitForTotalFlush polls the storage directory until the total event count
// across all tenants reaches at least minTotal, or until timeout expires.
func WaitForTotalFlush(storageDir string, minTotal int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := ReadAllEvents(storageDir)
		if err == nil {
			total := 0
			for _, evts := range events {
				total += len(evts)
			}
			if total >= minTotal {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	events, _ := ReadAllEvents(storageDir)
	total := 0
	for _, evts := range events {
		total += len(evts)
	}
	return fmt.Errorf("timeout: total events %d, wanted >= %d", total, minTotal)
}

// GenerateEvents creates n test events with unique labels and sequential values.
func GenerateEvents(n int, tenantID string) []event.Event {
	events := make([]event.Event, n)
	now := time.Now()
	for i := range events {
		events[i] = event.Event{
			TenantID:  tenantID,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Labels:    map[string]string{"index": fmt.Sprintf("%d", i), "source": "e2e-test"},
			Value:     float64(i),
		}
	}
	return events
}

// DrainResponse reads and closes the response body.
func DrainResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
