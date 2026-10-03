package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/ingester"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	ingesterBrokers        string
	ingesterTopic          string
	ingesterGroup          string
	ingesterBatchSize      int
	ingesterBatchAge       string
	ingesterStoragePath    string
	ingesterFlushWorkers   int
	ingesterStorageBackend string
	ingesterS3Endpoint     string
	ingesterS3Bucket       string
	ingesterS3AccessKey    string
	ingesterS3SecretKey    string
	ingesterS3Region       string
	ingesterAdminPort      int
)

var ingesterCmd = &cobra.Command{
	Use:   "ingester",
	Short: "Start the Kafka ingester",
	Long:  "Starts the ingester that consumes events from Kafka, batches them, and flushes to object storage.",
	RunE:  runIngester,
}

func init() {
	ingesterCmd.Flags().StringVar(&ingesterBrokers, "kafka-brokers", "", "Comma-separated broker addresses (default \"localhost:9092\")")
	ingesterCmd.Flags().StringVar(&ingesterTopic, "kafka-topic", "", "Kafka topic to consume (default \"ingestion-events\")")
	ingesterCmd.Flags().StringVar(&ingesterGroup, "consumer-group", "", "Consumer group name (default \"ingestion-ingesters\")")
	ingesterCmd.Flags().IntVar(&ingesterBatchSize, "batch-size", 0, "Max events per batch before flush (default 1000)")
	ingesterCmd.Flags().StringVar(&ingesterBatchAge, "batch-age", "", "Max batch age before flush (default \"10s\")")
	ingesterCmd.Flags().StringVar(&ingesterStoragePath, "storage-path", "", "Base directory for filesystem store (default \"./data\")")
	ingesterCmd.Flags().IntVar(&ingesterFlushWorkers, "flush-workers", 0, "Number of concurrent flush goroutines (default 4)")
	ingesterCmd.Flags().StringVar(&ingesterStorageBackend, "storage-backend", "", "Storage backend: filesystem or s3 (default \"filesystem\")")
	ingesterCmd.Flags().StringVar(&ingesterS3Endpoint, "s3-endpoint", "", "S3/MinIO endpoint URL (default \"http://localhost:9000\")")
	ingesterCmd.Flags().StringVar(&ingesterS3Bucket, "s3-bucket", "", "S3 bucket name (default \"ingestion-engine\")")
	ingesterCmd.Flags().StringVar(&ingesterS3AccessKey, "s3-access-key", "", "S3 access key (default \"minioadmin\")")
	ingesterCmd.Flags().StringVar(&ingesterS3SecretKey, "s3-secret-key", "", "S3 secret key (default \"minioadmin\")")
	ingesterCmd.Flags().StringVar(&ingesterS3Region, "s3-region", "", "S3 region (default \"us-east-1\")")
	ingesterCmd.Flags().IntVar(&ingesterAdminPort, "admin-port", 0, "Admin port for /metrics and /debug/pprof (default 9101)")
	rootCmd.AddCommand(ingesterCmd)
}

func runIngester(cmd *cobra.Command, args []string) error {
	log := Logger()
	if log == nil {
		log = zap.NewNop()
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// CLI flags override config defaults.
	if ingesterBrokers != "" {
		cfg.Ingester.KafkaBrokers = strings.Split(ingesterBrokers, ",")
	}
	if ingesterTopic != "" {
		cfg.Ingester.KafkaTopic = ingesterTopic
	}
	if ingesterGroup != "" {
		cfg.Ingester.ConsumerGroup = ingesterGroup
	}
	if ingesterBatchSize > 0 {
		cfg.Ingester.BatchSize = ingesterBatchSize
	}
	if ingesterBatchAge != "" {
		d, err := time.ParseDuration(ingesterBatchAge)
		if err != nil {
			return fmt.Errorf("invalid --batch-age: %w", err)
		}
		cfg.Ingester.BatchAge = d
	}
	if ingesterStoragePath != "" {
		cfg.Ingester.StoragePath = ingesterStoragePath
	}
	if ingesterFlushWorkers > 0 {
		cfg.Ingester.FlushWorkers = ingesterFlushWorkers
	}
	if ingesterStorageBackend != "" {
		cfg.Storage.Backend = ingesterStorageBackend
	}
	if ingesterS3Endpoint != "" {
		cfg.Storage.S3.Endpoint = ingesterS3Endpoint
	}
	if ingesterS3Bucket != "" {
		cfg.Storage.S3.Bucket = ingesterS3Bucket
	}
	if ingesterS3AccessKey != "" {
		cfg.Storage.S3.AccessKey = ingesterS3AccessKey
	}
	if ingesterS3SecretKey != "" {
		cfg.Storage.S3.SecretKey = ingesterS3SecretKey
	}
	if ingesterS3Region != "" {
		cfg.Storage.S3.Region = ingesterS3Region
	}
	if ingesterAdminPort != 0 {
		cfg.Ingester.AdminPort = ingesterAdminPort
	}

	log.Info("starting ingester",
		zap.Strings("kafka_brokers", cfg.Ingester.KafkaBrokers),
		zap.String("kafka_topic", cfg.Ingester.KafkaTopic),
		zap.String("consumer_group", cfg.Ingester.ConsumerGroup),
		zap.Int("batch_size", cfg.Ingester.BatchSize),
		zap.Duration("batch_age", cfg.Ingester.BatchAge),
		zap.String("storage_path", cfg.Ingester.StoragePath),
		zap.Int("flush_workers", cfg.Ingester.FlushWorkers),
	)

	// Create storage backend.
	var store storage.ObjectStore
	switch cfg.Storage.Backend {
	case "s3":
		log.Info("using S3 storage backend",
			zap.String("endpoint", cfg.Storage.S3.Endpoint),
			zap.String("bucket", cfg.Storage.S3.Bucket),
		)
		store, err = storage.NewS3Store(
			cfg.Storage.S3.Endpoint,
			cfg.Storage.S3.Bucket,
			cfg.Storage.S3.AccessKey,
			cfg.Storage.S3.SecretKey,
			cfg.Storage.S3.Region,
			cfg.Storage.S3.UsePathStyle,
		)
		if err != nil {
			return fmt.Errorf("create S3 storage: %w", err)
		}
	default:
		log.Info("using filesystem storage backend", zap.String("path", cfg.Ingester.StoragePath))
		store, err = storage.NewFilesystemStore(cfg.Ingester.StoragePath)
		if err != nil {
			return fmt.Errorf("create filesystem storage: %w", err)
		}
	}

	// Wire up the pipeline: storage -> flusher -> batcher -> consumer.
	flusher := ingester.NewFlusher(store, log)
	flusher.SetIndexStore(index.NewObjectStoreIndexStore(store))
	batcher := ingester.NewBatcher(cfg.Ingester.BatchSize, cfg.Ingester.BatchAge)

	consumer, err := ingester.NewConsumer(cfg.Ingester, batcher, flusher, store, log)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	// Start admin server (metrics + pprof) on a separate port.
	adminPort := cfg.Ingester.AdminPort
	if adminPort == 0 {
		adminPort = 9101
	}
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.Handler())
	observability.RegisterPprof(adminMux)
	adminSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", adminPort),
		Handler: adminMux,
	}
	go func() {
		log.Info("admin server listening", zap.Int("port", adminPort))
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("admin server error", zap.Error(err))
		}
	}()

	// Run with signal handling for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}

	log.Info("ingester stopped")
	return nil
}
