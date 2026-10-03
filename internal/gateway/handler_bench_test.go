package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/ring"
)

// noopProducer is a zero-allocation producer for benchmarks.
// Unlike mockProducer, it does not record state, making it safe for concurrent use.
type noopProducer struct{}

func (p *noopProducer) Produce(_ context.Context, _ []byte, _ []byte) error { return nil }
func (p *noopProducer) ProduceAsync(_ context.Context, _ []byte, _ []byte, cb func(error)) {
	if cb != nil {
		cb(nil)
	}
}
func (p *noopProducer) ProduceToPartition(_ context.Context, _ []byte, _ []byte, _ int32) error {
	return nil
}
func (p *noopProducer) Close() error { return nil }

func newBenchHandler() *Handler {
	cfg := config.GatewayConfig{
		Port:         8080,
		KafkaBrokers: []string{"localhost:9092"},
		KafkaTopic:   "bench-topic",
		MaxBodySize:  5 * 1024 * 1024,
	}
	r := ring.NewRing(6, 64)
	return NewHandler(&noopProducer{}, r, cfg, zap.NewNop())
}

const benchBody = `[{"labels":{"service":"api","env":"prod"},"value":42.0,"timestamp":"2024-01-15T10:30:00Z"}]`

func BenchmarkHandlerPush(b *testing.B) {
	h := newBenchHandler()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(benchBody))
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

func BenchmarkHandlerPush_Parallel(b *testing.B) {
	h := newBenchHandler()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest("POST", "/api/v1/push", strings.NewReader(benchBody))
			req.Header.Set("X-Scope-OrgID", "tenant-a")
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
		}
	})
}
