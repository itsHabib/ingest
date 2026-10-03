//go:build smoke

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// These tests require Kafka on localhost:9092.
// Run: make smoke

const (
	smokeBroker = "localhost:9092"
	smokeTopic  = "smoke-test-events"
)

func TestSmoke_PushAndConsume(t *testing.T) {
	// Create a real producer
	producer, err := NewKafkaProducer([]string{smokeBroker}, smokeTopic)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	cfg := config.GatewayConfig{
		Port:         0,
		KafkaBrokers: []string{smokeBroker},
		KafkaTopic:   smokeTopic,
		MaxBodySize:  1024 * 1024,
	}
	hashRing := ring.NewRing(6, 64)
	handler := NewHandler(producer, hashRing, cfg, zap.NewNop())

	// Start a real HTTP server
	srv := &http.Server{Addr: ":0", Handler: handler}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())

	baseURL := fmt.Sprintf("http://%s", ln.Addr().String())

	t.Run("ready", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/ready")
		if err != nil {
			t.Fatalf("GET /ready: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("GET /ready status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("push_single_event", func(t *testing.T) {
		body := `[{"labels":{"service":"api"},"value":42.0}]`
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/push", strings.NewReader(body))
		req.Header.Set("X-Scope-OrgID", "smoke-tenant")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /api/v1/push: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)
		if result["events_accepted"] != float64(1) {
			t.Errorf("events_accepted = %v, want 1", result["events_accepted"])
		}
	})

	t.Run("push_multiple_events", func(t *testing.T) {
		body := `[{"labels":{"service":"web"},"value":1.5},{"labels":{"service":"db"},"value":99.9}]`
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/push", strings.NewReader(body))
		req.Header.Set("X-Scope-OrgID", "smoke-tenant")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /api/v1/push: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)
		if result["events_accepted"] != float64(2) {
			t.Errorf("events_accepted = %v, want 2", result["events_accepted"])
		}
	})

	t.Run("missing_tenant_returns_400", func(t *testing.T) {
		body := `[{"value":1}]`
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/push", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 400 {
			t.Errorf("status = %d, want 400", resp.StatusCode)
		}
	})

	t.Run("wrong_method_returns_405", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/api/v1/push")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 405 {
			t.Errorf("status = %d, want 405", resp.StatusCode)
		}
	})

	t.Run("verify_kafka_received", func(t *testing.T) {
		// Consume from Kafka to verify messages arrived
		client, err := kgo.NewClient(
			kgo.SeedBrokers(smokeBroker),
			kgo.ConsumeTopics(smokeTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatalf("create consumer: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var records []*kgo.Record
		for len(records) < 2 {
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				t.Fatalf("poll errors: %v", errs)
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for records, got %d", len(records))
			}
		}

		// Verify records have the right key
		for _, r := range records {
			if string(r.Key) != "smoke-tenant" {
				t.Errorf("record key = %q, want %q", string(r.Key), "smoke-tenant")
			}
		}

		t.Logf("received %d records from Kafka", len(records))
	})
}
