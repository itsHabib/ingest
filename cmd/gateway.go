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
	"github.com/itsHabib/ingest/internal/gateway"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/itsHabib/ingest/internal/tenant"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	gatewayPort      int
	gatewayAdminPort int
	kafkaBrokers     string
	kafkaTopic       string
	maxBodySize      int64
	ringPartitions   int
	ringVirtualNodes int
)

var gatewayCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Start the HTTP gateway server",
	Long:  "Starts an HTTP server that accepts multi-tenant push requests and produces events to Kafka.",
	RunE:  runGateway,
}

func init() {
	gatewayCmd.Flags().IntVar(&gatewayPort, "port", 0, "HTTP listen port (default 8080)")
	gatewayCmd.Flags().IntVar(&gatewayAdminPort, "admin-port", 0, "Admin port for /metrics and /debug/pprof (default 9100)")
	gatewayCmd.Flags().StringVar(&kafkaBrokers, "kafka-brokers", "", "Comma-separated broker addresses (default \"localhost:9092\")")
	gatewayCmd.Flags().StringVar(&kafkaTopic, "kafka-topic", "", "Target Kafka topic (default \"ingestion-events\")")
	gatewayCmd.Flags().Int64Var(&maxBodySize, "max-body-size", 0, "Max request body in bytes (default 5242880)")
	gatewayCmd.Flags().IntVar(&ringPartitions, "ring-partitions", 0, "Number of hash ring partitions (default 6)")
	gatewayCmd.Flags().IntVar(&ringVirtualNodes, "ring-vnodes", 0, "Virtual nodes per partition (default 64)")
	rootCmd.AddCommand(gatewayCmd)
}

func runGateway(cmd *cobra.Command, args []string) error {
	log := Logger()
	if log == nil {
		log = zap.NewNop()
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// CLI flags override config
	if gatewayPort != 0 {
		cfg.Gateway.Port = gatewayPort
	}
	if kafkaBrokers != "" {
		cfg.Gateway.KafkaBrokers = strings.Split(kafkaBrokers, ",")
	}
	if kafkaTopic != "" {
		cfg.Gateway.KafkaTopic = kafkaTopic
	}
	if maxBodySize != 0 {
		cfg.Gateway.MaxBodySize = maxBodySize
	}
	if ringPartitions != 0 {
		cfg.Gateway.RingPartitions = ringPartitions
	}
	if ringVirtualNodes != 0 {
		cfg.Gateway.RingVirtualNodes = ringVirtualNodes
	}
	if gatewayAdminPort != 0 {
		cfg.Gateway.AdminPort = gatewayAdminPort
	}

	// Construct the consistent hash ring
	hashRing := ring.NewRing(cfg.Gateway.RingPartitions, cfg.Gateway.RingVirtualNodes)

	log.Info("starting gateway",
		zap.Int("port", cfg.Gateway.Port),
		zap.Strings("kafka_brokers", cfg.Gateway.KafkaBrokers),
		zap.String("kafka_topic", cfg.Gateway.KafkaTopic),
		zap.Int("ring_partitions", cfg.Gateway.RingPartitions),
		zap.Int("ring_vnodes", cfg.Gateway.RingVirtualNodes),
	)

	producer, err := gateway.NewKafkaProducer(cfg.Gateway.KafkaBrokers, cfg.Gateway.KafkaTopic)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("producer close error", zap.Error(err))
		}
	}()

	// Create per-tenant rate limiter.
	rl := tenant.NewRateLimiter(cfg.Gateway.RateLimitRate, cfg.Gateway.RateLimitBurst)
	log.Info("rate limiter configured",
		zap.Float64("rate", cfg.Gateway.RateLimitRate),
		zap.Int("burst", cfg.Gateway.RateLimitBurst),
	)

	handler := gateway.NewHandler(producer, hashRing, cfg.Gateway, log, gateway.WithRateLimiter(rl))

	// Wrap with Prometheus instrumentation middleware.
	instrumented := observability.InstrumentHandler("gateway", handler)

	mux := http.NewServeMux()
	mux.Handle("/", instrumented)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Gateway.Port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start admin server for /metrics and /debug/pprof.
	adminPort := cfg.Gateway.AdminPort
	if adminPort == 0 {
		adminPort = 9100
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

	// Graceful shutdown on SIGINT/SIGTERM
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

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

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("server shutdown error", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}

	log.Info("gateway stopped")
	return nil
}
