package querier_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/querier"
	"github.com/itsHabib/ingest/internal/storage"
	itesting "github.com/itsHabib/ingest/internal/testing"
	"go.uber.org/zap"
)

// Fixed timestamps — no time.Now() anywhere.
var (
	day1 = time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	day2 = time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
)

// writeTestBatch serializes a batch to the fake store, computes BlockMeta, and
// updates the index.
func writeTestBatch(
	t *testing.T,
	store *itesting.FakeObjectStore,
	idxStore *index.ObjectStoreIndexStore,
	tenantID string,
	date time.Time,
	partition int32,
	seq int64,
	events []event.Event,
) {
	t.Helper()
	ctx := context.Background()

	batch := event.Batch{
		TenantID:  tenantID,
		Partition: partition,
		Events:    events,
		CreatedAt: date,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	key, err := storage.BatchKey(tenantID, date, partition, seq)
	if err != nil {
		t.Fatalf("batch key: %v", err)
	}

	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("put batch: %v", err)
	}

	// Compute min/max time and merged labels for block meta.
	var minT, maxT time.Time
	mergedLabels := make(map[string]string)
	for i, ev := range events {
		if i == 0 || ev.Timestamp.Before(minT) {
			minT = ev.Timestamp
		}
		if i == 0 || ev.Timestamp.After(maxT) {
			maxT = ev.Timestamp
		}
		for k, v := range ev.Labels {
			mergedLabels[k] = v
		}
	}

	meta := index.BlockMeta{
		FilePath:   key,
		TenantID:   tenantID,
		Partition:  partition,
		MinTime:    minT,
		MaxTime:    maxT,
		EventCount: len(events),
		Labels:     mergedLabels,
		SizeBytes:  len(data),
	}

	// Read or create the index for this tenant+date, add the block, write back.
	dateStr := date.UTC().Format("2006-01-02")
	idx, err := idxStore.ReadIndex(ctx, tenantID, dateStr)
	if err != nil {
		idx = index.NewIndex(tenantID, dateStr)
	}
	idx.AddBlock(meta)
	if err := idxStore.WriteIndex(ctx, idx); err != nil {
		t.Fatalf("write index: %v", err)
	}
}

// queryEvents issues a GET to /api/v1/query and parses the NDJSON response.
func queryEvents(t *testing.T, handler http.Handler, tenantID string, start, end time.Time, labels string) []event.Event {
	t.Helper()

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

// writeTestBatchNoIndex writes a batch to the store WITHOUT updating the index.
// Used to test the list-fallback path when no index exists.
func writeTestBatchNoIndex(
	t *testing.T,
	store *itesting.FakeObjectStore,
	tenantID string,
	date time.Time,
	partition int32,
	seq int64,
	events []event.Event,
) {
	t.Helper()
	ctx := context.Background()

	batch := event.Batch{
		TenantID:  tenantID,
		Partition: partition,
		Events:    events,
		CreatedAt: date,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	key, err := storage.BatchKey(tenantID, date, partition, seq)
	if err != nil {
		t.Fatalf("batch key: %v", err)
	}

	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("put batch: %v", err)
	}
}

func TestQueryIntegration_NoIndex(t *testing.T) {
	store := itesting.NewFakeObjectStore()
	idxStore := index.NewObjectStoreIndexStore(store)

	// Write batches WITHOUT any index.
	writeTestBatchNoIndex(t, store, "tenant-x", day1, 0, 1, []event.Event{
		{TenantID: "tenant-x", Timestamp: day1.Add(2 * time.Hour), Labels: map[string]string{"service": "api"}, Value: 1.0},
		{TenantID: "tenant-x", Timestamp: day1.Add(10 * time.Hour), Labels: map[string]string{"service": "web"}, Value: 2.0},
	})
	writeTestBatchNoIndex(t, store, "tenant-x", day1, 0, 2, []event.Event{
		{TenantID: "tenant-x", Timestamp: day1.Add(14 * time.Hour), Labels: map[string]string{"service": "api", "env": "staging"}, Value: 3.0},
	})

	// Wire adapters — idxStore has no index entries, so every lookup falls back to List.
	indexAdapter := &querier.IndexStoreAdapter{Store: idxStore, ObjectStore: store}
	readerAdapter := &querier.EventReaderAdapter{Reader: querier.NewBlockReader(store, 4)}
	handler := querier.NewHandler(indexAdapter, readerAdapter, zap.NewNop())

	t.Run("fallback_returns_all_events", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-x", day1, day1.AddDate(0, 0, 1), "")
		if got := len(events); got != 3 {
			t.Errorf("got %d events, want 3", got)
		}
	})

	t.Run("fallback_with_label_filter", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-x", day1, day1.AddDate(0, 0, 1), "service=api")
		if got := len(events); got != 2 {
			t.Errorf("got %d events, want 2", got)
		}
	})

	t.Run("fallback_with_time_filter", func(t *testing.T) {
		start := day1.Add(9 * time.Hour)
		end := day1.Add(15 * time.Hour)
		events := queryEvents(t, handler, "tenant-x", start, end, "")
		if got := len(events); got != 2 {
			t.Errorf("got %d events, want 2 (10:00 and 14:00)", got)
		}
	})

	t.Run("fallback_nonexistent_tenant", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-nope", day1, day1.AddDate(0, 0, 1), "")
		if got := len(events); got != 0 {
			t.Errorf("got %d events, want 0", got)
		}
	})
}

func TestQueryIntegration(t *testing.T) {
	store := itesting.NewFakeObjectStore()
	idxStore := index.NewObjectStoreIndexStore(store)

	// --- Tenant-A day1 batch: 3 events ---
	writeTestBatch(t, store, idxStore, "tenant-a", day1, 0, 1, []event.Event{
		{TenantID: "tenant-a", Timestamp: day1.Add(2 * time.Hour), Labels: map[string]string{"service": "api"}, Value: 1.0},
		{TenantID: "tenant-a", Timestamp: day1.Add(10 * time.Hour), Labels: map[string]string{"service": "web"}, Value: 2.0},
		{TenantID: "tenant-a", Timestamp: day1.Add(14 * time.Hour), Labels: map[string]string{"service": "api", "env": "staging"}, Value: 3.0},
	})

	// --- Tenant-A day2 batch: 1 event ---
	writeTestBatch(t, store, idxStore, "tenant-a", day2, 0, 1, []event.Event{
		{TenantID: "tenant-a", Timestamp: day2.Add(5 * time.Hour), Labels: map[string]string{"service": "api", "env": "prod"}, Value: 4.0},
	})

	// --- Tenant-B day1 batch: 2 events ---
	writeTestBatch(t, store, idxStore, "tenant-b", day1, 0, 1, []event.Event{
		{TenantID: "tenant-b", Timestamp: day1.Add(3 * time.Hour), Labels: map[string]string{"service": "api"}, Value: 10.0},
		{TenantID: "tenant-b", Timestamp: day1.Add(8 * time.Hour), Labels: map[string]string{"service": "db"}, Value: 11.0},
	})

	// Wire up real adapters.
	indexAdapter := &querier.IndexStoreAdapter{Store: idxStore, ObjectStore: store}
	readerAdapter := &querier.EventReaderAdapter{Reader: querier.NewBlockReader(store, 4)}
	handler := querier.NewHandler(indexAdapter, readerAdapter, zap.NewNop())

	t.Run("basic_single_day", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-a", day1, day1.AddDate(0, 0, 1), "")
		if got := len(events); got != 3 {
			t.Errorf("got %d events, want 3", got)
		}
	})

	t.Run("time_range_filter", func(t *testing.T) {
		start := day1.Add(9 * time.Hour)  // 09:00
		end := day1.Add(15 * time.Hour)   // 15:00
		events := queryEvents(t, handler, "tenant-a", start, end, "")
		if got := len(events); got != 2 {
			t.Errorf("got %d events, want 2 (10:00 and 14:00)", got)
		}
	})

	t.Run("label_exact_match", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-a", day1, day1.AddDate(0, 0, 1), "service=api")
		if got := len(events); got != 2 {
			t.Errorf("got %d events, want 2", got)
		}
	})

	t.Run("label_regex", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-a", day1, day1.AddDate(0, 0, 1), "service=~api|web")
		if got := len(events); got != 3 {
			t.Errorf("got %d events, want 3", got)
		}
	})

	t.Run("multi_label", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-a", day1, day1.AddDate(0, 0, 1), "service=api,env=staging")
		if got := len(events); got != 1 {
			t.Errorf("got %d events, want 1", got)
		}
	})

	t.Run("tenant_isolation", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-b", day1, day2.AddDate(0, 0, 1), "")
		if got := len(events); got != 2 {
			t.Errorf("got %d events, want 2", got)
		}
		for _, ev := range events {
			if ev.TenantID != "tenant-b" {
				t.Errorf("got event with tenant %q, want tenant-b", ev.TenantID)
			}
		}
	})

	t.Run("multi_day", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-a", day1, day2.AddDate(0, 0, 1), "")
		if got := len(events); got != 4 {
			t.Errorf("got %d events, want 4", got)
		}
	})

	t.Run("no_results", func(t *testing.T) {
		future := day2.AddDate(1, 0, 0)
		events := queryEvents(t, handler, "tenant-a", future, future.AddDate(0, 0, 1), "")
		if got := len(events); got != 0 {
			t.Errorf("got %d events, want 0", got)
		}
	})

	t.Run("nonexistent_tenant", func(t *testing.T) {
		events := queryEvents(t, handler, "tenant-c", day1, day2.AddDate(0, 0, 1), "")
		if got := len(events); got != 0 {
			t.Errorf("got %d events, want 0", got)
		}
	})
}
