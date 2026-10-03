package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricVarsNonNil(t *testing.T) {
	if RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil")
	}
	if RequestDuration == nil {
		t.Fatal("RequestDuration is nil")
	}
	if EventsProducedTotal == nil {
		t.Fatal("EventsProducedTotal is nil")
	}
	if RateLimitedTotal == nil {
		t.Fatal("RateLimitedTotal is nil")
	}
	if EventsConsumedTotal == nil {
		t.Fatal("EventsConsumedTotal is nil")
	}
	if BatchFlushTotal == nil {
		t.Fatal("BatchFlushTotal is nil")
	}
	if BatchFlushDuration == nil {
		t.Fatal("BatchFlushDuration is nil")
	}
	if BatchSizeEvents == nil {
		t.Fatal("BatchSizeEvents is nil")
	}
	if KafkaLag == nil {
		t.Fatal("KafkaLag is nil")
	}
}

func TestWithLabelValuesNoPanic(t *testing.T) {
	// These should not panic with the expected label sets.
	RequestsTotal.WithLabelValues("tenant-a", "200")
	RequestDuration.WithLabelValues("tenant-a")
	EventsProducedTotal.WithLabelValues("tenant-a", "0")
	RateLimitedTotal.WithLabelValues("tenant-a")
	EventsConsumedTotal.WithLabelValues("0")
	BatchFlushTotal.WithLabelValues("tenant-a", "success")
	BatchFlushDuration.WithLabelValues("tenant-a")
	KafkaLag.WithLabelValues("0")
}

func TestCounterIncrement(t *testing.T) {
	RequestsTotal.WithLabelValues("test-tenant", "200").Inc()
	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("test-tenant", "200"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}
