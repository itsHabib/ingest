package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/querier"
	"github.com/itsHabib/ingest/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	querierPort           int
	querierAdminPort      int
	querierStoragePath    string
	querierStorageBackend string
	querierS3Endpoint     string
	querierS3Bucket       string
	querierS3AccessKey    string
	querierS3SecretKey    string
	querierS3Region       string
)

var querierCmd = &cobra.Command{
	Use:   "querier",
	Short: "Start the HTTP query server",
	Long:  "Starts an HTTP server that serves query requests over stored events with label matching and time range filtering.",
	RunE:  runQuerier,
}

func init() {
	querierCmd.Flags().IntVar(&querierPort, "port", 8082, "HTTP listen port")
	querierCmd.Flags().IntVar(&querierAdminPort, "admin-port", 9102, "Admin port for /metrics and /debug/pprof")
	querierCmd.Flags().StringVar(&querierStoragePath, "storage-path", "./data", "Base directory for filesystem store")
	querierCmd.Flags().StringVar(&querierStorageBackend, "storage-backend", "filesystem", "Storage backend: filesystem or s3")
	querierCmd.Flags().StringVar(&querierS3Endpoint, "s3-endpoint", "http://localhost:9000", "S3/MinIO endpoint URL")
	querierCmd.Flags().StringVar(&querierS3Bucket, "s3-bucket", "ingestion-engine", "S3 bucket name")
	querierCmd.Flags().StringVar(&querierS3AccessKey, "s3-access-key", "minioadmin", "S3 access key")
	querierCmd.Flags().StringVar(&querierS3SecretKey, "s3-secret-key", "minioadmin", "S3 secret key")
	querierCmd.Flags().StringVar(&querierS3Region, "s3-region", "us-east-1", "S3 region")
	rootCmd.AddCommand(querierCmd)
}

func runQuerier(cmd *cobra.Command, args []string) error {
	log := Logger()
	if log == nil {
		log = zap.NewNop()
	}

	// Create storage backend.
	var store storage.ObjectStore
	var err error
	switch querierStorageBackend {
	case "s3":
		log.Info("using S3 storage backend",
			zap.String("endpoint", querierS3Endpoint),
			zap.String("bucket", querierS3Bucket),
		)
		store, err = storage.NewS3Store(
			querierS3Endpoint,
			querierS3Bucket,
			querierS3AccessKey,
			querierS3SecretKey,
			querierS3Region,
			true,
		)
		if err != nil {
			return fmt.Errorf("create S3 storage: %w", err)
		}
	default:
		log.Info("using filesystem storage backend", zap.String("path", querierStoragePath))
		store, err = storage.NewFilesystemStore(querierStoragePath)
		if err != nil {
			return fmt.Errorf("create filesystem storage: %w", err)
		}
	}

	indexStore := &querier.IndexStoreAdapter{Store: index.NewObjectStoreIndexStore(store), ObjectStore: store}
	reader := &querier.EventReaderAdapter{Reader: querier.NewBlockReader(store, 8)}

	handler := querier.NewHandler(indexStore, reader, log)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", querierPort),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start admin server for /metrics and /debug/pprof.
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.Handler())
	observability.RegisterPprof(adminMux)
	adminSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", querierAdminPort),
		Handler: adminMux,
	}
	go func() {
		log.Info("admin server listening", zap.Int("port", querierAdminPort))
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("admin server error", zap.Error(err))
		}
	}()

	// Start main server.
	errCh := make(chan error, 1)
	go func() {
		log.Info("querier listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Graceful shutdown on SIGINT/SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := adminSrv.Shutdown(ctx); err != nil {
		log.Error("admin server shutdown error", zap.Error(err))
	}

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("server shutdown error", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}

	log.Info("querier stopped")
	return nil
}
