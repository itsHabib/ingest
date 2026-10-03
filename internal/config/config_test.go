package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.Gateway.Port != 8080 {
		t.Errorf("Gateway.Port = %d, want 8080", cfg.Gateway.Port)
	}
	if len(cfg.Gateway.KafkaBrokers) != 1 || cfg.Gateway.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("Gateway.KafkaBrokers = %v, want [localhost:9092]", cfg.Gateway.KafkaBrokers)
	}
	if cfg.Gateway.KafkaTopic != "ingestion-events" {
		t.Errorf("Gateway.KafkaTopic = %q, want %q", cfg.Gateway.KafkaTopic, "ingestion-events")
	}
	if cfg.Ingester.ConsumerGroup != "ingestion-ingesters" {
		t.Errorf("Ingester.ConsumerGroup = %q, want %q", cfg.Ingester.ConsumerGroup, "ingestion-ingesters")
	}
	if cfg.Ingester.BatchSize != 1000 {
		t.Errorf("Ingester.BatchSize = %d, want 1000", cfg.Ingester.BatchSize)
	}
	if cfg.Ingester.BatchAge != 10*time.Second {
		t.Errorf("Ingester.BatchAge = %s, want 10s", cfg.Ingester.BatchAge)
	}
	if cfg.Ingester.StoragePath != "./data" {
		t.Errorf("Ingester.StoragePath = %q, want %q", cfg.Ingester.StoragePath, "./data")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("INGEST_GATEWAY_PORT", "9090")
	t.Setenv("INGEST_KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("INGEST_KAFKA_TOPIC", "custom-topic")
	t.Setenv("INGEST_INGESTER_CONSUMER_GROUP", "custom-group")
	t.Setenv("INGEST_INGESTER_BATCH_SIZE", "500")
	t.Setenv("INGEST_INGESTER_BATCH_AGE", "5s")
	t.Setenv("INGEST_INGESTER_STORAGE_PATH", "/tmp/data")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.Gateway.Port != 9090 {
		t.Errorf("Gateway.Port = %d, want 9090", cfg.Gateway.Port)
	}
	if len(cfg.Gateway.KafkaBrokers) != 2 {
		t.Errorf("Gateway.KafkaBrokers = %v, want 2 brokers", cfg.Gateway.KafkaBrokers)
	}
	if cfg.Gateway.KafkaTopic != "custom-topic" {
		t.Errorf("Gateway.KafkaTopic = %q, want %q", cfg.Gateway.KafkaTopic, "custom-topic")
	}
	if cfg.Ingester.ConsumerGroup != "custom-group" {
		t.Errorf("Ingester.ConsumerGroup = %q, want %q", cfg.Ingester.ConsumerGroup, "custom-group")
	}
	if cfg.Ingester.BatchSize != 500 {
		t.Errorf("Ingester.BatchSize = %d, want 500", cfg.Ingester.BatchSize)
	}
	if cfg.Ingester.BatchAge != 5*time.Second {
		t.Errorf("Ingester.BatchAge = %s, want 5s", cfg.Ingester.BatchAge)
	}
	if cfg.Ingester.StoragePath != "/tmp/data" {
		t.Errorf("Ingester.StoragePath = %q, want %q", cfg.Ingester.StoragePath, "/tmp/data")
	}
}

func TestLoadInvalidPort(t *testing.T) {
	t.Setenv("INGEST_GATEWAY_PORT", "99999")
	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for invalid port, got nil")
	}
}

func TestLoadInvalidPortNonNumeric(t *testing.T) {
	t.Setenv("INGEST_GATEWAY_PORT", "abc")
	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for non-numeric port, got nil")
	}
}

func TestLoadInvalidBatchSize(t *testing.T) {
	t.Setenv("INGEST_INGESTER_BATCH_SIZE", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for zero batch size, got nil")
	}
}

func TestLoadInvalidBatchAge(t *testing.T) {
	t.Setenv("INGEST_INGESTER_BATCH_AGE", "notaduration")
	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for invalid batch age, got nil")
	}
}

func TestValidateEmptyBrokers(t *testing.T) {
	cfg := &Config{
		Gateway: GatewayConfig{
			Port:         8080,
			KafkaBrokers: []string{},
			KafkaTopic:   "test",
		},
		Ingester: IngesterConfig{
			KafkaBrokers:  []string{"localhost:9092"},
			KafkaTopic:    "test",
			ConsumerGroup: "test",
			BatchSize:     1,
			BatchAge:      time.Second,
			StoragePath:   "./data",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for empty gateway brokers, got nil")
	}
}

func TestValidateEmptyTopic(t *testing.T) {
	cfg := &Config{
		Gateway: GatewayConfig{
			Port:         8080,
			KafkaBrokers: []string{"localhost:9092"},
			KafkaTopic:   "",
		},
		Ingester: IngesterConfig{
			KafkaBrokers:  []string{"localhost:9092"},
			KafkaTopic:    "test",
			ConsumerGroup: "test",
			BatchSize:     1,
			BatchAge:      time.Second,
			StoragePath:   "./data",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for empty gateway topic, got nil")
	}
}

func TestValidateZeroPort(t *testing.T) {
	// Ensure we don't leak env vars from other tests
	_ = os.Unsetenv("INGEST_GATEWAY_PORT")

	cfg := &Config{
		Gateway: GatewayConfig{
			Port:         0,
			KafkaBrokers: []string{"localhost:9092"},
			KafkaTopic:   "test",
		},
		Ingester: IngesterConfig{
			KafkaBrokers:  []string{"localhost:9092"},
			KafkaTopic:    "test",
			ConsumerGroup: "test",
			BatchSize:     1,
			BatchAge:      time.Second,
			StoragePath:   "./data",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for zero port, got nil")
	}
}
