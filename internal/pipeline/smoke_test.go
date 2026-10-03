//go:build smoke

package pipeline_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/gateway"
	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/ingester"
	"github.com/itsHabib/ingest/internal/querier"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/itsHabib/ingest/internal/storage"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const smokeBroker = "localhost:9092"

// TestSmoke_FullPipeline verifies the complete data path:
//
//	HTTP POST → Gateway → Hash Ring → Kafka → Ingester → Batcher → Flusher → Filesystem
//
// It pushes events for two tenants, waits for them to flow through the pipeline,
// and asserts that correctly-formed batch files appear on disk with tenant isolation.
func TestSmoke_FullPipeline(t *testing.T) {
	ctx := context.Background()

	// Unique topic per run to avoid stale data.
	topic := fmt.Sprintf("smoke-pipeline-%d", time.Now().UnixNano())
	consumerGroup := fmt.Sprintf("smoke-cg-%d", time.Now().UnixNano())

	// Pre-create topic with 6 partitions (must match ring partition count,
	// otherwise ProduceToPartition will fail for partition >= topic partitions).
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(smokeBroker))
	if err != nil {
		t.Fatalf("create admin client: %v", err)
	}
	admin := kadm.NewClient(adminClient)
	_, err = admin.CreateTopic(ctx, 6, 1, nil, topic)
	adminClient.Close()
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	t.Cleanup(func() {
		cl, err := kgo.NewClient(kgo.SeedBrokers(smokeBroker))
		if err != nil {
			return
		}
		adm := kadm.NewClient(cl)
		adm.DeleteTopics(ctx, topic)
		cl.Close()
	})
	t.Logf("created topic %s with 6 partitions", topic)

	// --- Storage ---
	storageDir := t.TempDir()
	store, err := storage.NewFilesystemStore(storageDir)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// --- Gateway (producer + ring + HTTP server) ---
	producer, err := gateway.NewKafkaProducer([]string{smokeBroker}, topic)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	hashRing := ring.NewRing(6, 64)
	gatewayCfg := config.GatewayConfig{
		Port:         0,
		KafkaBrokers: []string{smokeBroker},
		KafkaTopic:   topic,
		MaxBodySize:  1024 * 1024,
	}
	handler := gateway.NewHandler(producer, hashRing, gatewayCfg, zap.NewNop())

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	defer srv.Shutdown(ctx)

	baseURL := fmt.Sprintf("http://%s", ln.Addr().String())
	t.Logf("gateway listening on %s", baseURL)

	// --- Ingester (consumer + batcher + flusher) ---
	// Small batch size (2) so we trigger a size flush without pushing 1000 events.
	// Short batch age (3s) so time-triggered flushes happen quickly.
	batchSize := 2
	batchAge := 3 * time.Second

	batcher := ingester.NewBatcher(batchSize, batchAge)
	flusher := ingester.NewFlusher(store, zap.NewNop())
	idxStore := index.NewObjectStoreIndexStore(store)
	flusher.SetIndexStore(idxStore)

	ingesterCfg := config.IngesterConfig{
		KafkaBrokers:  []string{smokeBroker},
		KafkaTopic:    topic,
		ConsumerGroup: consumerGroup,
		BatchSize:     batchSize,
		BatchAge:      batchAge,
		StoragePath:   storageDir,
		FlushWorkers:  2,
	}
	consumer, err := ingester.NewConsumer(ingesterCfg, batcher, flusher, store, zap.NewNop())
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	// Run consumer in background.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(consumerCtx)
	}()

	// Give consumer time to join the group and get partition assignments.
	time.Sleep(3 * time.Second)
	t.Log("consumer started, pushing events...")

	// --- Push events via HTTP ---
	// Send each event as a separate request so each gets a unique Kafka offset.
	// If multiple events share a Kafka offset, the batcher can produce batches
	// with the same offset → same storage key → overwrites. Separate requests
	// avoid this and better reflect real-world usage.
	//
	// tenant-a: 3 events (batch size=2 → 1 size-triggered flush, 1 age-triggered)
	// tenant-b: 1 event (age-triggered after 3s)
	pushEvents(t, baseURL, "tenant-a", `[{"labels":{"service":"api"},"value":1.0}]`, 1)
	pushEvents(t, baseURL, "tenant-a", `[{"labels":{"service":"api"},"value":2.0}]`, 1)
	pushEvents(t, baseURL, "tenant-a", `[{"labels":{"service":"api"},"value":3.0}]`, 1)

	pushEvents(t, baseURL, "tenant-b", `[{"labels":{"service":"web"},"value":99.0}]`, 1)

	// --- Wait for files to appear on disk ---
	// Size-triggered batch (tenant-a, 2 events) should flush almost immediately.
	// Age-triggered batches (tenant-a 1 event, tenant-b 1 event) flush after ~3s.
	// Total wait budget: 15s.
	t.Log("waiting for batches to flush to disk...")

	var tenantAKeys, tenantBKeys []string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		tenantAKeys, _ = store.List(ctx, "tenant-a/")
		tenantBKeys, _ = store.List(ctx, "tenant-b/")

		// We need all batches flushed: at least 2 files for tenant-a
		// (one size-triggered, one age-triggered) and 1 for tenant-b.
		if len(tenantAKeys) >= 2 && len(tenantBKeys) >= 1 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if len(tenantAKeys) == 0 {
		t.Fatal("no files found for tenant-a — pipeline did not flush to disk")
	}
	if len(tenantBKeys) == 0 {
		t.Fatal("no files found for tenant-b — pipeline did not flush to disk")
	}

	t.Logf("tenant-a: %d batch files", len(tenantAKeys))
	t.Logf("tenant-b: %d batch files", len(tenantBKeys))

	// --- Verify tenant-a batches ---
	totalTenantAEvents := 0
	for _, key := range tenantAKeys {
		data, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}

		var batch event.Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatalf("unmarshal %s: %v", key, err)
		}

		if batch.TenantID != "tenant-a" {
			t.Errorf("batch %s: tenant = %q, want tenant-a", key, batch.TenantID)
		}
		totalTenantAEvents += len(batch.Events)
		t.Logf("  %s: %d events, partition %d", key, len(batch.Events), batch.Partition)
	}

	if totalTenantAEvents != 3 {
		t.Errorf("tenant-a total events = %d, want 3", totalTenantAEvents)
	}

	// --- Verify tenant-b batches ---
	totalTenantBEvents := 0
	for _, key := range tenantBKeys {
		data, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}

		var batch event.Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatalf("unmarshal %s: %v", key, err)
		}

		if batch.TenantID != "tenant-b" {
			t.Errorf("batch %s: tenant = %q, want tenant-b", key, batch.TenantID)
		}
		totalTenantBEvents += len(batch.Events)
		t.Logf("  %s: %d events, partition %d", key, len(batch.Events), batch.Partition)
	}

	if totalTenantBEvents != 1 {
		t.Errorf("tenant-b total events = %d, want 1", totalTenantBEvents)
	}

	// --- Verify tenant isolation ---
	for _, key := range tenantAKeys {
		if strings.HasPrefix(key, "tenant-b/") {
			t.Errorf("tenant-a file list contains tenant-b key: %s", key)
		}
	}
	for _, key := range tenantBKeys {
		if strings.HasPrefix(key, "tenant-a/") {
			t.Errorf("tenant-b file list contains tenant-a key: %s", key)
		}
	}

	// --- Verify batch file format ---
	// Keys should match: <tenantID>/<YYYY-MM-DD>/<partition>-<offset>.json
	today := time.Now().UTC().Format("2006-01-02")
	for _, key := range append(tenantAKeys, tenantBKeys...) {
		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			t.Errorf("key %q: expected 3 path segments, got %d", key, len(parts))
			continue
		}
		if parts[1] != today {
			t.Errorf("key %q: date segment = %q, want %q", key, parts[1], today)
		}
		if !strings.HasSuffix(parts[2], ".json") {
			t.Errorf("key %q: missing .json suffix", key)
		}
	}

	// --- Query verification ---
	// Wire up the querier using the same store the flusher wrote to.
	// The index was built naturally by the flusher's SetIndexStore.
	t.Log("verifying query read path...")

	queryHandler := querier.NewHandler(
		&querier.IndexStoreAdapter{Store: idxStore, ObjectStore: store},
		&querier.EventReaderAdapter{Reader: querier.NewBlockReader(store, 4)},
		zap.NewNop(),
	)

	// Query all tenant-a events for today.
	t.Run("query_tenant_a_all", func(t *testing.T) {
		events := smokeQuery(t, queryHandler, "tenant-a", today)
		if len(events) != totalTenantAEvents {
			t.Errorf("query returned %d events, want %d", len(events), totalTenantAEvents)
		}
		for _, ev := range events {
			if ev.TenantID != "tenant-a" {
				t.Errorf("query returned event with tenant %q, want tenant-a", ev.TenantID)
			}
		}
		t.Logf("query returned %d events for tenant-a", len(events))
	})

	// Query all tenant-b events for today.
	t.Run("query_tenant_b_all", func(t *testing.T) {
		events := smokeQuery(t, queryHandler, "tenant-b", today)
		if len(events) != totalTenantBEvents {
			t.Errorf("query returned %d events, want %d", len(events), totalTenantBEvents)
		}
		for _, ev := range events {
			if ev.TenantID != "tenant-b" {
				t.Errorf("query returned event with tenant %q, want tenant-b", ev.TenantID)
			}
		}
		t.Logf("query returned %d events for tenant-b", len(events))
	})

	// Query tenant isolation: tenant-a query must not return tenant-b events.
	t.Run("query_tenant_isolation", func(t *testing.T) {
		events := smokeQuery(t, queryHandler, "tenant-a", today)
		for _, ev := range events {
			if ev.TenantID == "tenant-b" {
				t.Errorf("tenant-a query returned tenant-b event: %+v", ev)
			}
		}
	})

	// Query nonexistent tenant returns empty.
	t.Run("query_nonexistent_tenant", func(t *testing.T) {
		events := smokeQuery(t, queryHandler, "tenant-nonexistent", today)
		if len(events) != 0 {
			t.Errorf("query returned %d events for nonexistent tenant, want 0", len(events))
		}
	})

	// Query with label filter: all pushed events for tenant-a have service=api.
	t.Run("query_label_filter", func(t *testing.T) {
		events := smokeQueryWithLabels(t, queryHandler, "tenant-a", today, "service=api")
		if len(events) != totalTenantAEvents {
			t.Errorf("query service=api returned %d events, want %d", len(events), totalTenantAEvents)
		}
	})

	// Query with label filter that should exclude: tenant-a has no service=web events.
	t.Run("query_label_no_match", func(t *testing.T) {
		events := smokeQueryWithLabels(t, queryHandler, "tenant-a", today, "service=web")
		if len(events) != 0 {
			t.Errorf("query service=web returned %d events for tenant-a, want 0", len(events))
		}
	})

	// --- Graceful shutdown ---
	t.Log("shutting down consumer...")
	consumerCancel()
	select {
	case err := <-consumerDone:
		if err != nil {
			t.Errorf("consumer shutdown error: %v", err)
		}
		t.Log("consumer shut down gracefully")
	case <-time.After(10 * time.Second):
		t.Fatal("consumer shutdown timed out")
	}
}

// smokeQuery issues a full-day query to the handler and returns parsed events.
func smokeQuery(t *testing.T, handler http.Handler, tenantID, date string) []event.Event {
	t.Helper()
	return smokeQueryWithLabels(t, handler, tenantID, date, "")
}

// smokeQueryWithLabels issues a query with optional label filters.
func smokeQueryWithLabels(t *testing.T, handler http.Handler, tenantID, date, labels string) []event.Event {
	t.Helper()

	start, err := time.Parse("2006-01-02", date)
	if err != nil {
		t.Fatalf("parse date %q: %v", date, err)
	}
	end := start.AddDate(0, 0, 1)

	url := fmt.Sprintf("/api/v1/query?start=%s&end=%s",
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
	)
	if labels != "" {
		url += "&labels=" + labels
	}

	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Scope-OrgID", tenantID)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query returned status %d, want 200", resp.StatusCode)
	}

	var events []event.Event
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event line %q: %v", line, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan response: %v", err)
	}
	return events
}

// pushEvents sends a POST to /api/v1/push and asserts the response.
func pushEvents(t *testing.T, baseURL, tenantID, body string, expectedCount int) {
	t.Helper()

	req, _ := http.NewRequest("POST", baseURL+"/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", tenantID)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("push %s: %v", tenantID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("push %s: status = %d, want 200", tenantID, resp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if int(result["events_accepted"].(float64)) != expectedCount {
		t.Errorf("push %s: events_accepted = %v, want %d", tenantID, result["events_accepted"], expectedCount)
	}

	t.Logf("pushed %d events for %s", expectedCount, tenantID)
}
