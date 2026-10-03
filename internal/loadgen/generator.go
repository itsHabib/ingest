package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// LoadGenConfig holds load generator configuration.
type LoadGenConfig struct {
	TargetURL   string
	Tenants     int
	Rate        float64
	Duration    time.Duration
	PayloadSize int
	RampUp      bool
	Warmup      time.Duration
}

// Generator sends configurable multi-tenant traffic to the gateway.
type Generator struct {
	cfg LoadGenConfig
}

// NewGenerator creates a load generator with the given config.
func NewGenerator(cfg LoadGenConfig) *Generator {
	return &Generator{cfg: cfg}
}

// Run executes the load test and returns a report.
// It spawns one goroutine per tenant, all sharing a single rate limiter
// for aggregate rate control.
func (g *Generator) Run(ctx context.Context) (*Report, error) {
	if g.cfg.Tenants <= 0 {
		return nil, fmt.Errorf("loadgen: tenants must be > 0")
	}
	if g.cfg.Rate <= 0 {
		return nil, fmt.Errorf("loadgen: rate must be > 0")
	}

	initialRate := g.cfg.Rate
	if g.cfg.RampUp {
		initialRate = 1 // start near zero
	}

	burst := int(g.cfg.Rate)
	if burst < 1 {
		burst = 1
	}
	limiter := rate.NewLimiter(rate.Limit(initialRate), burst)

	ctx, cancel := context.WithTimeout(ctx, g.cfg.Duration)
	defer cancel()

	var (
		totalEvents    atomic.Int64
		successEvents  atomic.Int64
		failedEvents   atomic.Int64
		bytesSent      atomic.Int64
		mu             sync.Mutex
		latencies      []time.Duration
		statusCodes    sync.Map // map[int]*atomic.Int64
	)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}

	start := time.Now()
	warmupEnd := start.Add(g.cfg.Warmup)

	// Ramp-up goroutine: linearly increase rate over first 10% of duration.
	if g.cfg.RampUp {
		rampDuration := g.cfg.Duration / 10
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			rampStart := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					elapsed := time.Since(rampStart)
					if elapsed >= rampDuration {
						limiter.SetLimit(rate.Limit(g.cfg.Rate))
						return
					}
					progress := float64(elapsed) / float64(rampDuration)
					currentRate := g.cfg.Rate * progress
					if currentRate < 1 {
						currentRate = 1
					}
					limiter.SetLimit(rate.Limit(currentRate))
				}
			}
		}()
	}

	var wg sync.WaitGroup

	for i := 0; i < g.cfg.Tenants; i++ {
		wg.Add(1)
		tenantID := fmt.Sprintf("tenant-%d", i)

		go func(tenant string) {
			defer wg.Done()
			for {
				if err := limiter.Wait(ctx); err != nil {
					return
				}

				reqStart := time.Now()
				statusCode, n, err := g.sendEvent(ctx, client, tenant)
				lat := time.Since(reqStart)

				totalEvents.Add(1)
				bytesSent.Add(int64(n))

				if err != nil || (statusCode != http.StatusOK && statusCode != 0) {
					failedEvents.Add(1)
					if statusCode != 0 {
						val, _ := statusCodes.LoadOrStore(statusCode, &atomic.Int64{})
						val.(*atomic.Int64).Add(1)
					}
				} else {
					successEvents.Add(1)
				}

				// Only record latency after warmup period.
				if reqStart.After(warmupEnd) {
					mu.Lock()
					latencies = append(latencies, lat)
					mu.Unlock()
				}
			}
		}(tenantID)
	}

	wg.Wait()
	elapsed := time.Since(start)

	mu.Lock()
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	mu.Unlock()

	p50 := percentile(sorted, 0.50)
	p90 := percentile(sorted, 0.90)
	p99 := percentile(sorted, 0.99)
	maxLat := percentile(sorted, 1.0)

	total := totalEvents.Load()
	throughput := float64(total) / elapsed.Seconds()

	errorBreakdown := make(map[int]int)
	statusCodes.Range(func(key, value interface{}) bool {
		errorBreakdown[key.(int)] = int(value.(*atomic.Int64).Load())
		return true
	})

	return &Report{
		TotalEvents:    total,
		SuccessEvents:  successEvents.Load(),
		FailedEvents:   failedEvents.Load(),
		Duration:       elapsed,
		EventsPerSec:   throughput,
		LatencyP50:     p50,
		LatencyP90:     p90,
		LatencyP99:     p99,
		LatencyMax:     maxLat,
		ErrorBreakdown: errorBreakdown,
		BytesSent:      bytesSent.Load(),
	}, nil
}

// sendEvent sends a single batch of one event to the gateway.
// Returns the HTTP status code, number of bytes sent, and any error.
func (g *Generator) sendEvent(ctx context.Context, client *http.Client, tenantID string) (int, int, error) {
	payload := map[string]interface{}{
		"labels": map[string]string{
			"service": fmt.Sprintf("svc-%d", rand.Intn(10)),
			"method":  fmt.Sprintf("method-%d", rand.Intn(5)),
		},
		"value": rand.Float64() * 100,
	}

	if g.cfg.PayloadSize > 0 {
		p := make([]byte, g.cfg.PayloadSize)
		for i := range p {
			p[i] = byte('a' + rand.Intn(26))
		}
		payload["payload"] = p
	}

	events := []interface{}{payload}
	body, err := json.Marshal(events)
	if err != nil {
		return 0, 0, fmt.Errorf("loadgen: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.TargetURL+"/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return 0, 0, fmt.Errorf("loadgen: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenantID)

	resp, err := client.Do(req)
	if err != nil {
		return 0, len(body), fmt.Errorf("loadgen: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, len(body), fmt.Errorf("loadgen: unexpected status %d", resp.StatusCode)
	}
	return resp.StatusCode, len(body), nil
}

// percentile computes the p-th percentile from a slice of durations.
// The slice is sorted in place. Returns 0 if the slice is empty.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	idx := int(float64(len(latencies)-1) * p)
	return latencies[idx]
}
