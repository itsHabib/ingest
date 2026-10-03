package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the ingestion engine.
type Config struct {
	Gateway  GatewayConfig
	Ingester IngesterConfig
	Storage  StorageConfig
	LogLevel string
}

// StorageConfig holds storage backend configuration.
type StorageConfig struct {
	Backend string // "filesystem" or "s3"
	S3      S3Config
}

// S3Config holds S3/MinIO-specific configuration.
type S3Config struct {
	Endpoint     string
	Bucket       string
	AccessKey    string
	SecretKey    string
	Region       string
	UsePathStyle bool
}

// LoadGenConfig holds load generator configuration.
type LoadGenConfig struct {
	TargetURL   string
	Tenants     int
	Rate        float64
	Duration    time.Duration
	PayloadSize int
}

// GatewayConfig holds gateway-specific configuration.
type GatewayConfig struct {
	Port             int
	AdminPort        int // metrics + pprof admin port (default 9100)
	KafkaBrokers     []string
	KafkaTopic       string
	MaxBodySize      int64
	RingPartitions   int
	RingVirtualNodes int
	RateLimitRate    float64 // events per second per tenant (default 1000)
	RateLimitBurst   int     // max burst per tenant (default 5000)
}

// IngesterConfig holds ingester-specific configuration.
type IngesterConfig struct {
	KafkaBrokers   []string
	KafkaTopic     string
	ConsumerGroup  string
	BatchSize      int
	BatchAge       time.Duration
	StoragePath    string
	FlushWorkers   int
	PauseThreshold int // consecutive flush failures before pausing consumption (default 5)
	AdminPort      int // metrics + pprof admin port (default 9101)
}

// Load returns a Config populated from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		Gateway: GatewayConfig{
			Port:             8080,
			AdminPort:        9100,
			KafkaBrokers:     []string{"localhost:9092"},
			KafkaTopic:       "ingestion-events",
			MaxBodySize:      5 * 1024 * 1024, // 5 MB
			RingPartitions:   6,
			RingVirtualNodes: 64,
			RateLimitRate:    1000,
			RateLimitBurst:   5000,
		},
		Ingester: IngesterConfig{
			KafkaBrokers:  []string{"localhost:9092"},
			KafkaTopic:    "ingestion-events",
			ConsumerGroup: "ingestion-ingesters",
			BatchSize:     1000,
			BatchAge:      10 * time.Second,
			StoragePath:   "./data",
			FlushWorkers:   4,
			PauseThreshold: 10,
			AdminPort:      9101,
		},
		Storage: StorageConfig{
			Backend: "filesystem",
			S3: S3Config{
				Endpoint:     "http://localhost:9000",
				Bucket:       "ingestion-engine",
				AccessKey:    "minioadmin",
				SecretKey:    "minioadmin",
				Region:       "us-east-1",
				UsePathStyle: true,
			},
		},
		LogLevel: "info",
	}

	if v := os.Getenv("INGEST_GATEWAY_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_GATEWAY_PORT %q: %w", v, err)
		}
		cfg.Gateway.Port = port
	}

	if v := os.Getenv("INGEST_KAFKA_BROKERS"); v != "" {
		brokers := strings.Split(v, ",")
		cfg.Gateway.KafkaBrokers = brokers
		cfg.Ingester.KafkaBrokers = brokers
	}

	if v := os.Getenv("INGEST_KAFKA_TOPIC"); v != "" {
		cfg.Gateway.KafkaTopic = v
		cfg.Ingester.KafkaTopic = v
	}

	if v := os.Getenv("INGEST_INGESTER_CONSUMER_GROUP"); v != "" {
		cfg.Ingester.ConsumerGroup = v
	}

	if v := os.Getenv("INGEST_INGESTER_BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_INGESTER_BATCH_SIZE %q: %w", v, err)
		}
		cfg.Ingester.BatchSize = size
	}

	if v := os.Getenv("INGEST_INGESTER_BATCH_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_INGESTER_BATCH_AGE %q: %w", v, err)
		}
		cfg.Ingester.BatchAge = d
	}

	if v := os.Getenv("INGEST_INGESTER_STORAGE_PATH"); v != "" {
		cfg.Ingester.StoragePath = v
	}

	if v := os.Getenv("INGEST_INGESTER_PAUSE_THRESHOLD"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_INGESTER_PAUSE_THRESHOLD %q: %w", v, err)
		}
		cfg.Ingester.PauseThreshold = n
	}

	if v := os.Getenv("INGEST_GATEWAY_ADMIN_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_GATEWAY_ADMIN_PORT %q: %w", v, err)
		}
		cfg.Gateway.AdminPort = port
	}

	if v := os.Getenv("INGEST_INGESTER_ADMIN_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_INGESTER_ADMIN_PORT %q: %w", v, err)
		}
		cfg.Ingester.AdminPort = port
	}

	if v := os.Getenv("INGEST_STORAGE_BACKEND"); v != "" {
		cfg.Storage.Backend = v
	}
	if v := os.Getenv("INGEST_S3_ENDPOINT"); v != "" {
		cfg.Storage.S3.Endpoint = v
	}
	if v := os.Getenv("INGEST_S3_BUCKET"); v != "" {
		cfg.Storage.S3.Bucket = v
	}
	if v := os.Getenv("INGEST_S3_ACCESS_KEY"); v != "" {
		cfg.Storage.S3.AccessKey = v
	}
	if v := os.Getenv("INGEST_S3_SECRET_KEY"); v != "" {
		cfg.Storage.S3.SecretKey = v
	}
	if v := os.Getenv("INGEST_S3_REGION"); v != "" {
		cfg.Storage.S3.Region = v
	}

	if v := os.Getenv("INGEST_GATEWAY_RATE_LIMIT_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_GATEWAY_RATE_LIMIT_RATE %q: %w", v, err)
		}
		cfg.Gateway.RateLimitRate = r
	}

	if v := os.Getenv("INGEST_GATEWAY_RATE_LIMIT_BURST"); v != "" {
		b, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid INGEST_GATEWAY_RATE_LIMIT_BURST %q: %w", v, err)
		}
		cfg.Gateway.RateLimitBurst = b
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that all config values are valid.
func (c *Config) Validate() error {
	if c.Gateway.Port < 1 || c.Gateway.Port > 65535 {
		return fmt.Errorf("config: gateway port must be 1-65535, got %d", c.Gateway.Port)
	}
	if len(c.Gateway.KafkaBrokers) == 0 {
		return fmt.Errorf("config: gateway kafka_brokers must not be empty")
	}
	if c.Gateway.KafkaTopic == "" {
		return fmt.Errorf("config: gateway kafka_topic must not be empty")
	}
	if len(c.Ingester.KafkaBrokers) == 0 {
		return fmt.Errorf("config: ingester kafka_brokers must not be empty")
	}
	if c.Ingester.KafkaTopic == "" {
		return fmt.Errorf("config: ingester kafka_topic must not be empty")
	}
	if c.Ingester.BatchSize <= 0 {
		return fmt.Errorf("config: ingester batch_size must be > 0, got %d", c.Ingester.BatchSize)
	}
	if c.Ingester.BatchAge <= 0 {
		return fmt.Errorf("config: ingester batch_age must be > 0, got %s", c.Ingester.BatchAge)
	}
	if c.Storage.Backend != "" && c.Storage.Backend != "filesystem" && c.Storage.Backend != "s3" {
		return fmt.Errorf("config: storage backend must be \"filesystem\" or \"s3\", got %q", c.Storage.Backend)
	}
	return nil
}
