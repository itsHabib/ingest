package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestReportPercentiles(t *testing.T) {
	// Known latencies: 1ms through 100ms.
	latencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		latencies[i] = time.Duration(i+1) * time.Millisecond
	}

	p50 := percentile(latencies, 0.50)
	p99 := percentile(latencies, 0.99)

	if p50 < 49*time.Millisecond || p50 > 51*time.Millisecond {
		t.Errorf("P50 = %v, want ~50ms", p50)
	}

	if p99 < 98*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("P99 = %v, want ~99-100ms", p99)
	}
}

func TestPercentileEmpty(t *testing.T) {
	p := percentile(nil, 0.50)
	if p != 0 {
		t.Errorf("percentile(nil, 0.50) = %v, want 0", p)
	}
}

func TestGeneratorContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "ok",
			"events_accepted": 1,
		})
	}))
	defer server.Close()

	gen := NewGenerator(LoadGenConfig{
		TargetURL:   server.URL,
		Tenants:     2,
		Rate:        100,
		Duration:    5 * time.Second,
		PayloadSize: 0,
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_, _ = gen.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("generator did not shut down within 2s after context cancellation")
	}
}

func TestGeneratorRunReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "ok",
			"events_accepted": 1,
		})
	}))
	defer server.Close()

	gen := NewGenerator(LoadGenConfig{
		TargetURL:   server.URL,
		Tenants:     2,
		Rate:        50,
		Duration:    1 * time.Second,
		PayloadSize: 10,
	})

	report, err := gen.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.TotalEvents == 0 {
		t.Error("TotalEvents = 0, want > 0")
	}
	if report.Duration <= 0 {
		t.Error("Duration <= 0")
	}
	if report.EventsPerSec <= 0 {
		t.Error("EventsPerSec <= 0")
	}
	if report.TotalEvents != report.SuccessEvents+report.FailedEvents {
		t.Errorf("TotalEvents (%d) != SuccessEvents (%d) + FailedEvents (%d)",
			report.TotalEvents, report.SuccessEvents, report.FailedEvents)
	}
	if report.BytesSent <= 0 {
		t.Error("BytesSent <= 0")
	}
}

func TestGeneratorInvalidConfig(t *testing.T) {
	gen := NewGenerator(LoadGenConfig{
		TargetURL: "http://localhost:8080",
		Tenants:   0,
		Rate:      100,
		Duration:  1 * time.Second,
	})

	_, err := gen.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for zero tenants")
	}

	gen2 := NewGenerator(LoadGenConfig{
		TargetURL: "http://localhost:8080",
		Tenants:   1,
		Rate:      0,
		Duration:  1 * time.Second,
	})

	_, err = gen2.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for zero rate")
	}
}

func TestWarmupExcludesLatencies(t *testing.T) {
	var requestCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "ok",
			"events_accepted": 1,
		})
	}))
	defer server.Close()

	gen := NewGenerator(LoadGenConfig{
		TargetURL: server.URL,
		Tenants:   1,
		Rate:      100,
		Duration:  2 * time.Second,
		Warmup:    1 * time.Second,
	})

	report, err := gen.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All events should be counted in TotalEvents
	if report.TotalEvents == 0 {
		t.Fatal("TotalEvents = 0")
	}

	// Latency percentiles should be computed from post-warmup data only.
	// Since warmup is 1s out of 2s total, latency samples should be roughly
	// half of total events (the post-warmup half).
	// We can't directly check latency count, but we can verify latencies are non-zero
	// (meaning at least some post-warmup data was collected).
	if report.LatencyP50 == 0 {
		t.Error("LatencyP50 = 0, expected non-zero after warmup")
	}
}

func TestRampUpReachesTargetRate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "ok",
			"events_accepted": 1,
		})
	}))
	defer server.Close()

	gen := NewGenerator(LoadGenConfig{
		TargetURL: server.URL,
		Tenants:   1,
		Rate:      200,
		Duration:  3 * time.Second,
		RampUp:    true,
	})

	report, err := gen.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// With ramp-up, total events should be less than steady-state (200 * 3 = 600)
	// but still significant (ramp is only first 10% = 0.3s).
	if report.TotalEvents == 0 {
		t.Fatal("TotalEvents = 0")
	}

	// Throughput should be close to target rate since ramp-up is only 10% of duration.
	// Allow a generous margin since this is a timing-sensitive test.
	if report.EventsPerSec < 50 {
		t.Errorf("EventsPerSec = %.1f, expected closer to target rate of 200", report.EventsPerSec)
	}
}

func TestErrorBreakdown(t *testing.T) {
	var reqCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := reqCount.Add(1)
		if count%3 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if count%5 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "ok",
			"events_accepted": 1,
		})
	}))
	defer server.Close()

	gen := NewGenerator(LoadGenConfig{
		TargetURL: server.URL,
		Tenants:   1,
		Rate:      100,
		Duration:  1 * time.Second,
	})

	report, err := gen.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.FailedEvents == 0 {
		t.Fatal("FailedEvents = 0, expected some failures")
	}

	if len(report.ErrorBreakdown) == 0 {
		t.Fatal("ErrorBreakdown is empty, expected status code entries")
	}

	// We should see 429 errors at minimum
	if report.ErrorBreakdown[429] == 0 {
		t.Error("expected HTTP 429 in ErrorBreakdown")
	}
}
