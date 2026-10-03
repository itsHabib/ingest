package querier

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
	"go.uber.org/zap"
)

// mockIndexStore implements IndexStore for testing.
type mockIndexStore struct {
	paths map[string][]string // key: "tenantID/date" -> paths
	err   error
}

func (m *mockIndexStore) Lookup(_ context.Context, tenantID, date string, _ []Matcher) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	key := tenantID + "/" + date
	return m.paths[key], nil
}

// mockEventReader implements EventReader for testing.
type mockEventReader struct {
	events []event.Event
	err    error
}

func (m *mockEventReader) Read(ctx context.Context, keys []string, matchers []Matcher, start, end time.Time) (<-chan event.Event, <-chan error) {
	eventCh := make(chan event.Event, len(m.events))
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)

		if m.err != nil {
			errCh <- m.err
			return
		}

		for _, ev := range m.events {
			select {
			case eventCh <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return eventCh, errCh
}

func newTestHandler(index IndexStore, reader EventReader) *Handler {
	return NewHandler(index, reader, zap.NewNop())
}

func TestHandler_MissingTenantID(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
	if !strings.Contains(w.Body.String(), "X-Scope-OrgID") {
		t.Errorf("expected error about X-Scope-OrgID, got: %s", w.Body.String())
	}
}

func TestHandler_InvalidTimeRange(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	// start >= end
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-02T00:00:00Z&end=2024-01-01T00:00:00Z", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
	if !strings.Contains(w.Body.String(), "start must be before end") {
		t.Errorf("expected time range error, got: %s", w.Body.String())
	}
}

func TestHandler_EqualStartEnd(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-01T00:00:00Z", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandler_MissingStartEnd(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	tests := []struct {
		name string
		url  string
	}{
		{name: "missing start", url: "/api/v1/query?end=2024-01-02T00:00:00Z"},
		{name: "missing end", url: "/api/v1/query?start=2024-01-01T00:00:00Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			req.Header.Set("X-Scope-OrgID", "tenant-a")
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestHandler_InvalidMatchers(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&labels=key%3D~%5Binvalid", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandler_Success(t *testing.T) {
	ts1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

	events := []event.Event{
		{TenantID: "tenant-a", Timestamp: ts1, Labels: map[string]string{"service": "api"}, Value: 1.0},
		{TenantID: "tenant-a", Timestamp: ts2, Labels: map[string]string{"service": "api"}, Value: 2.0},
	}

	index := &mockIndexStore{
		paths: map[string][]string{
			"tenant-a/2024-01-01": {"tenant-a/2024-01-01/0-0000000000.json"},
		},
	}
	reader := &mockEventReader{events: events}
	h := newTestHandler(index, reader)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&labels=service%3Dapi", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/x-ndjson")
	}

	// Parse NDJSON output.
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2, body:\n%s", len(lines), w.Body.String())
	}

	for i, line := range lines {
		var ev event.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("line %d: unmarshal error: %v", i, err)
		}
		if ev.TenantID != "tenant-a" {
			t.Errorf("line %d: tenant = %q, want %q", i, ev.TenantID, "tenant-a")
		}
	}
}

func TestHandler_NoResults(t *testing.T) {
	index := &mockIndexStore{paths: map[string][]string{}}
	reader := &mockEventReader{events: nil}
	h := newTestHandler(index, reader)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	body := strings.TrimSpace(w.Body.String())
	if body != "" {
		t.Errorf("expected empty body, got: %q", body)
	}
}

func TestHandler_Ready(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("status = %q, want %q", body["status"], "ready")
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/query", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got status %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandler_MultiDayQuery(t *testing.T) {
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)

	events := []event.Event{
		{TenantID: "tenant-a", Timestamp: ts1, Labels: map[string]string{"service": "api"}, Value: 1.0},
		{TenantID: "tenant-a", Timestamp: ts2, Labels: map[string]string{"service": "api"}, Value: 2.0},
	}

	index := &mockIndexStore{
		paths: map[string][]string{
			"tenant-a/2024-01-01": {"tenant-a/2024-01-01/0-0000000000.json"},
			"tenant-a/2024-01-02": {"tenant-a/2024-01-02/0-0000000000.json"},
		},
	}
	reader := &mockEventReader{events: events}
	h := newTestHandler(index, reader)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-03T00:00:00Z", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", w.Code, http.StatusOK)
	}

	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
}

func TestHandler_NotFound(t *testing.T) {
	h := newTestHandler(&mockIndexStore{}, &mockEventReader{})

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("got status %d, want %d", w.Code, http.StatusNotFound)
	}
}

