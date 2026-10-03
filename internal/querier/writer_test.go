package querier

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
)

func TestStreamJSON_BasicOutput(t *testing.T) {
	events := make(chan event.Event, 3)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events <- event.Event{TenantID: "t1", Timestamp: base, Labels: map[string]string{"a": "1"}, Value: 1}
	events <- event.Event{TenantID: "t1", Timestamp: base.Add(time.Second), Labels: map[string]string{"a": "2"}, Value: 2}
	events <- event.Event{TenantID: "t1", Timestamp: base.Add(2 * time.Second), Labels: map[string]string{"a": "3"}, Value: 3}
	close(events)

	w := httptest.NewRecorder()
	err := StreamJSON(context.Background(), w, events)
	if err != nil {
		t.Fatalf("StreamJSON returned error: %v", err)
	}

	body := w.Body.String()
	scanner := bufio.NewScanner(strings.NewReader(body))
	var count int
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Errorf("invalid JSON on line %d: %v", count+1, err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("expected 3 NDJSON lines, got %d", count)
	}
}

func TestStreamJSON_EmptyChannel(t *testing.T) {
	events := make(chan event.Event)
	close(events)

	w := httptest.NewRecorder()
	err := StreamJSON(context.Background(), w, events)
	if err != nil {
		t.Fatalf("StreamJSON returned error: %v", err)
	}

	if body := w.Body.String(); body != "" {
		t.Errorf("expected empty body, got %q", body)
	}
}

func TestStreamJSON_ContentType(t *testing.T) {
	events := make(chan event.Event, 1)
	events <- event.Event{TenantID: "t1", Timestamp: time.Now(), Value: 1}
	close(events)

	w := httptest.NewRecorder()
	_ = StreamJSON(context.Background(), w, events)

	ct := w.Header().Get("Content-Type")
	if ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/x-ndjson")
	}
}

func TestStreamJSON_ContextCancel(t *testing.T) {
	events := make(chan event.Event) // unbuffered, will block

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	w := httptest.NewRecorder()
	go func() {
		done <- StreamJSON(ctx, w, events)
	}()

	// Cancel while StreamJSON is blocked waiting for events
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StreamJSON did not return after context cancellation")
	}
}

// --- Benchmarks ---

func BenchmarkStreamJSON(b *testing.B) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		events := make(chan event.Event, 1000)
		go func() {
			for j := 0; j < 100000; j++ {
				events <- event.Event{
					TenantID:  "t1",
					Timestamp: base.Add(time.Duration(j) * time.Millisecond),
					Labels:    map[string]string{"service": "api", "env": "prod"},
					Value:     float64(j),
				}
			}
			close(events)
		}()

		w := httptest.NewRecorder()
		_ = StreamJSON(context.Background(), w, events)
	}
}
