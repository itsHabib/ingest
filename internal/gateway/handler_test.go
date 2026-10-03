package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/itsHabib/ingest/internal/tenant"
)

// mockProducer implements Producer for testing.
type mockProducer struct {
	produceFunc            func(ctx context.Context, key []byte, value []byte) error
	produceToPartFunc      func(ctx context.Context, key []byte, value []byte, partition int32) error
	lastKey                []byte
	lastValue              []byte
	lastPartition          int32
	partitionProduceCalled bool
	closed                 bool
	callCount              int
}

func (m *mockProducer) Produce(ctx context.Context, key []byte, value []byte) error {
	m.lastKey = key
	m.lastValue = value
	m.callCount++
	if m.produceFunc != nil {
		return m.produceFunc(ctx, key, value)
	}
	return nil
}

func (m *mockProducer) ProduceAsync(ctx context.Context, key []byte, value []byte, callback func(error)) {
	err := m.Produce(ctx, key, value)
	if callback != nil {
		callback(err)
	}
}

func (m *mockProducer) ProduceToPartition(ctx context.Context, key []byte, value []byte, partition int32) error {
	m.lastKey = key
	m.lastValue = value
	m.lastPartition = partition
	m.partitionProduceCalled = true
	m.callCount++
	if m.produceToPartFunc != nil {
		return m.produceToPartFunc(ctx, key, value, partition)
	}
	// Fall back to produceFunc for backward-compatible tests that set produceErr via produceFunc.
	if m.produceFunc != nil {
		return m.produceFunc(ctx, key, value)
	}
	return nil
}

func (m *mockProducer) Close() error {
	m.closed = true
	return nil
}

func newTestHandler(mp *mockProducer) *Handler {
	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "test-topic",
		MaxBodySize:  1024 * 1024, // 1 MB for tests
	}
	r := ring.NewRing(6, 64)
	return NewHandler(mp, r, cfg, zap.NewNop())
}

func TestPushHandler(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		tenantID     string
		body         string
		produceErr   error
		wantStatus   int
		wantContains string
	}{
		{
			name:         "success",
			method:       "POST",
			tenantID:     "tenant-a",
			body:         `[{"labels":{"service":"api"},"value":42.0,"timestamp":"2024-01-15T10:30:00Z"}]`,
			wantStatus:   200,
			wantContains: `"events_accepted":1`,
		},
		{
			name:         "missing_tenant_id",
			method:       "POST",
			tenantID:     "",
			body:         `[{"value":1}]`,
			wantStatus:   400,
			wantContains: "X-Scope-OrgID",
		},
		{
			name:         "invalid_json",
			method:       "POST",
			tenantID:     "tenant-a",
			body:         `not json`,
			wantStatus:   400,
			wantContains: "invalid JSON",
		},
		{
			name:         "empty_events",
			method:       "POST",
			tenantID:     "tenant-a",
			body:         `[]`,
			wantStatus:   400,
			wantContains: "empty events array",
		},
		{
			name:         "kafka_error",
			method:       "POST",
			tenantID:     "tenant-a",
			body:         `[{"value":1}]`,
			produceErr:   fmt.Errorf("broker unavailable"),
			wantStatus:   503,
			wantContains: "service unavailable",
		},
		{
			name:         "method_not_allowed",
			method:       "GET",
			tenantID:     "tenant-a",
			body:         "",
			wantStatus:   405,
			wantContains: "method not allowed",
		},
		{
			name:         "multiple_events",
			method:       "POST",
			tenantID:     "tenant-b",
			body:         `[{"value":1,"timestamp":"2024-01-01T00:00:00Z"},{"value":2,"timestamp":"2024-01-01T00:00:00Z"}]`,
			wantStatus:   200,
			wantContains: `"events_accepted":2`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := &mockProducer{}
			if tt.produceErr != nil {
				mp.produceFunc = func(ctx context.Context, key, value []byte) error {
					return tt.produceErr
				}
			}

			h := newTestHandler(mp)
			req := httptest.NewRequest(tt.method, "/api/v1/push", strings.NewReader(tt.body))
			if tt.tenantID != "" {
				req.Header.Set("X-Scope-OrgID", tt.tenantID)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantContains != "" && !strings.Contains(rr.Body.String(), tt.wantContains) {
				t.Errorf("body = %q, want to contain %q", rr.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestPushHandler_TenantIDOverwrite(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	// Client sends tenant_id in body, but header should win
	body := `[{"tenant_id":"evil-tenant","value":42,"timestamp":"2024-01-15T10:30:00Z"}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "real-tenant")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}

	// Verify the key sent to Kafka is the header tenant, not the body tenant
	if string(mp.lastKey) != "real-tenant" {
		t.Errorf("kafka key = %q, want %q", string(mp.lastKey), "real-tenant")
	}

	// Unmarshal the value sent to Kafka and verify tenant_id was overwritten
	var events []struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(mp.lastValue, &events); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].TenantID != "real-tenant" {
		t.Errorf("event tenant_id = %q, want %q", events[0].TenantID, "real-tenant")
	}
}

func TestPushHandler_TimestampDefault(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	// Event with no timestamp — should be defaulted to approximately now
	body := `[{"value":42}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")

	before := time.Now()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	after := time.Now()

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}

	// Verify the timestamp in the produced value is approximately now
	var events []struct {
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(mp.lastValue, &events); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ts := events[0].Timestamp
	if ts.Before(before) || ts.After(after) {
		t.Errorf("timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestPushHandler_BodyTooLarge(t *testing.T) {
	mp := &mockProducer{}
	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "test-topic",
		MaxBodySize:  50, // very small limit
	}
	r := ring.NewRing(6, 64)
	h := NewHandler(mp, r, cfg, zap.NewNop())

	// Body larger than 50 bytes
	bigBody := `[{"labels":{"service":"api","env":"prod","region":"us-east"},"value":42.0,"timestamp":"2024-01-15T10:30:00Z"}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(bigBody))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 413 {
		t.Errorf("status = %d, want 413; body = %s", rr.Code, rr.Body.String())
	}
}

func TestReadyHandler(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Errorf("status = %d, want 200", rr.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "ready" {
		t.Errorf("status = %q, want %q", resp["status"], "ready")
	}
}

func TestNotFound(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	req := httptest.NewRequest("GET", "/unknown", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 404 {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, 200, map[string]string{"key": "value"})

	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	var data map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if data["key"] != "value" {
		t.Errorf("key = %q, want %q", data["key"], "value")
	}
}

// TestHandlerUsesRingPartition verifies the handler produces to the partition
// determined by the hash ring.
func TestHandlerUsesRingPartition(t *testing.T) {
	mp := &mockProducer{}
	r := ring.NewRing(6, 64)
	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "test-topic",
		MaxBodySize:  1024 * 1024,
	}
	h := NewHandler(mp, r, cfg, zap.NewNop())

	tenantID := "tenant-a"
	expectedPartition := r.GetPartitionForTenant(tenantID)

	body := `[{"labels":{"service":"api"},"value":42.0,"timestamp":"2024-01-15T10:30:00Z"}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", tenantID)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}

	if !mp.partitionProduceCalled {
		t.Fatal("expected ProduceToPartition to be called, but it was not")
	}
	if mp.lastPartition != expectedPartition {
		t.Errorf("partition = %d, want %d", mp.lastPartition, expectedPartition)
	}
}

// TestPushHandler_NoContentType ensures the handler still works even without
// Content-Type header (we don't enforce it since we parse as JSON regardless).
func TestPushHandler_NoContentType(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	body := `[{"value":1,"timestamp":"2024-01-01T00:00:00Z"}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	// No Content-Type header

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Errorf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}
}

func TestHandlePush_RateLimited(t *testing.T) {
	mp := &mockProducer{}
	rl := tenant.NewRateLimiter(1, 1) // 1 event/sec, burst 1

	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "test-topic",
		MaxBodySize:  1024 * 1024,
	}
	r := ring.NewRing(6, 64)
	h := NewHandler(mp, r, cfg, zap.NewNop(), WithRateLimiter(rl))

	body := `[{"value":1,"timestamp":"2024-01-01T00:00:00Z"}]`

	// First request should succeed (burst available).
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("first request: status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}

	// Second immediate request should be rate limited.
	req = httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 429 {
		t.Fatalf("second request: status = %d, want 429; body = %s", rr.Code, rr.Body.String())
	}

	if ra := rr.Header().Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want %q", ra, "1")
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["error"] != "rate limit exceeded" {
		t.Errorf("error = %q, want %q", resp["error"], "rate limit exceeded")
	}
	if resp["tenant"] != "tenant-a" {
		t.Errorf("tenant = %q, want %q", resp["tenant"], "tenant-a")
	}
}

func TestHandlePush_ProduceTimeout(t *testing.T) {
	mp := &mockProducer{
		produceToPartFunc: func(ctx context.Context, key, value []byte, partition int32) error {
			return fmt.Errorf("produce: context deadline exceeded")
		},
	}

	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "test-topic",
		MaxBodySize:  1024 * 1024,
	}
	r := ring.NewRing(6, 64)
	h := NewHandler(mp, r, cfg, zap.NewNop())

	body := `[{"value":1,"timestamp":"2024-01-01T00:00:00Z"}]`
	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 503 {
		t.Fatalf("status = %d, want 503; body = %s", rr.Code, rr.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["error"] != "service unavailable" {
		t.Errorf("error = %q, want %q", resp["error"], "service unavailable")
	}
}

// --- New tests ---

func TestPushHandler_LargeBatch(t *testing.T) {
	mp := &mockProducer{}
	h := newTestHandler(mp)

	// Build a batch of 150 events
	var events []map[string]interface{}
	for i := 0; i < 150; i++ {
		events = append(events, map[string]interface{}{
			"value":     float64(i),
			"timestamp": "2024-01-15T10:30:00Z",
		})
	}
	bodyBytes, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("marshal events: %v", err)
	}

	req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(string(bodyBytes)))
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if accepted, ok := resp["events_accepted"].(float64); !ok || int(accepted) != 150 {
		t.Errorf("events_accepted = %v, want 150", resp["events_accepted"])
	}

	// Verify all events were sent to Kafka in one ProduceToPartition call
	if mp.callCount != 1 {
		t.Errorf("expected 1 produce call, got %d", mp.callCount)
	}

	// Verify the Kafka value contains all 150 events
	var produced []json.RawMessage
	if err := json.Unmarshal(mp.lastValue, &produced); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if len(produced) != 150 {
		t.Errorf("kafka value contains %d events, want 150", len(produced))
	}
}

func TestPushHandler_ProducerNotCalledOnValidationError(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "invalid_json",
			body: `{not json`,
		},
		{
			name: "empty_events",
			body: `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := &mockProducer{}
			h := newTestHandler(mp)

			req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(tt.body))
			req.Header.Set("X-Scope-OrgID", "tenant-a")
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if mp.callCount != 0 {
				t.Errorf("expected 0 produce calls for invalid input, got %d", mp.callCount)
			}
		})
	}
}
