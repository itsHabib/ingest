package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// durationBuckets covers sub-ms to 10s range for all duration histograms.
var durationBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// Gateway metrics.
var (
	// RequestsTotal counts all HTTP requests to the gateway.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_gateway_requests_total",
		Help: "Total HTTP requests to the gateway.",
	}, []string{"tenant", "status_code"})

	// RequestDuration observes request latency in seconds.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingest_gateway_request_duration_seconds",
		Help:    "Request latency in seconds.",
		Buckets: durationBuckets,
	}, []string{"tenant"})

	// EventsProducedTotal counts events successfully produced to Kafka.
	EventsProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_gateway_events_produced_total",
		Help: "Events sent to Kafka.",
	}, []string{"tenant", "partition"})

	// RateLimitedTotal counts requests rejected by the rate limiter.
	RateLimitedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_gateway_rate_limited_total",
		Help: "Requests rejected by rate limiter.",
	}, []string{"tenant"})
)

// Ingester metrics.
var (
	// EventsConsumedTotal counts events read from Kafka.
	EventsConsumedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_ingester_events_consumed_total",
		Help: "Events read from Kafka.",
	}, []string{"partition"})

	// BatchFlushTotal counts batches flushed to storage.
	BatchFlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_ingester_batch_flush_total",
		Help: "Batches flushed to storage.",
	}, []string{"tenant", "status"})

	// BatchFlushDuration observes flush latency in seconds.
	BatchFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingest_ingester_batch_flush_duration_seconds",
		Help:    "Flush latency in seconds.",
		Buckets: durationBuckets,
	}, []string{"tenant"})

	// BatchSizeEvents observes events per flushed batch.
	BatchSizeEvents = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingest_ingester_batch_size_events",
		Help:    "Events per flushed batch.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14), // 1, 2, 4, ..., 8192
	})

	// KafkaLag tracks consumer lag per partition.
	KafkaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingest_ingester_kafka_lag",
		Help: "Consumer lag per partition.",
	}, []string{"partition"})
)
