package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itsHabib/ingest/internal/loadgen"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	loadgenTarget      string
	loadgenTenants     int
	loadgenRate        float64
	loadgenDuration    string
	loadgenPayloadSize int
	loadgenRampUp      bool
	loadgenWarmup      string
	loadgenOutput      string
	loadgenProfile     string
)

var loadgenCmd = &cobra.Command{
	Use:   "loadgen",
	Short: "Start the load generator",
	Long:  "Starts a configurable multi-tenant event generator for testing and benchmarking.",
	RunE:  runLoadgen,
}

func init() {
	loadgenCmd.Flags().StringVar(&loadgenTarget, "target", "http://localhost:8080", "Gateway URL to send events to")
	loadgenCmd.Flags().IntVar(&loadgenTenants, "tenants", 5, "Number of tenants to simulate")
	loadgenCmd.Flags().Float64Var(&loadgenRate, "rate", 500, "Target events per second (aggregate across all tenants)")
	loadgenCmd.Flags().StringVar(&loadgenDuration, "duration", "60s", "Test duration (e.g., 30s, 2m)")
	loadgenCmd.Flags().IntVar(&loadgenPayloadSize, "payload-size", 100, "Payload size in bytes per event (0 for no payload)")
	loadgenCmd.Flags().BoolVar(&loadgenRampUp, "ramp-up", false, "Gradually increase rate over first 10% of duration")
	loadgenCmd.Flags().StringVar(&loadgenWarmup, "warmup", "5s", "Exclude first N seconds from latency statistics")
	loadgenCmd.Flags().StringVar(&loadgenOutput, "output", "", "Path to write JSON report (empty = no file output)")
	loadgenCmd.Flags().StringVar(&loadgenProfile, "profile", "", "Path to YAML load profile (sets defaults; explicit flags override)")
	rootCmd.AddCommand(loadgenCmd)
}

func runLoadgen(cmd *cobra.Command, args []string) error {
	log := Logger()
	if log == nil {
		log = zap.NewNop()
	}

	// Load profile defaults if provided.
	var profileCfg *loadgen.LoadGenConfig
	if loadgenProfile != "" {
		p, err := loadgen.LoadProfile(loadgenProfile)
		if err != nil {
			return fmt.Errorf("load profile: %w", err)
		}
		profileCfg = p
	}

	// Start with profile defaults, then override with explicit flags.
	cfg := loadgen.LoadGenConfig{
		TargetURL:   loadgenTarget,
		Tenants:     loadgenTenants,
		Rate:        loadgenRate,
		PayloadSize: loadgenPayloadSize,
		RampUp:      loadgenRampUp,
	}

	// Apply profile values as defaults for flags that weren't explicitly set.
	if profileCfg != nil {
		if !cmd.Flags().Changed("target") {
			cfg.TargetURL = profileCfg.TargetURL
		}
		if !cmd.Flags().Changed("tenants") {
			cfg.Tenants = profileCfg.Tenants
		}
		if !cmd.Flags().Changed("rate") {
			cfg.Rate = profileCfg.Rate
		}
		if !cmd.Flags().Changed("duration") {
			cfg.Duration = profileCfg.Duration
		}
		if !cmd.Flags().Changed("payload-size") {
			cfg.PayloadSize = profileCfg.PayloadSize
		}
		if !cmd.Flags().Changed("ramp-up") {
			cfg.RampUp = profileCfg.RampUp
		}
		if !cmd.Flags().Changed("warmup") {
			cfg.Warmup = profileCfg.Warmup
		}
		if !cmd.Flags().Changed("output") && loadgenOutput == "" {
			loadgenOutput = "" // no override from profile for output path
		}
	}

	// Parse duration from string if not set by profile.
	if cfg.Duration == 0 {
		dur, err := time.ParseDuration(loadgenDuration)
		if err != nil {
			return fmt.Errorf("invalid --duration: %w", err)
		}
		cfg.Duration = dur
	}

	// Parse warmup from string if not set by profile.
	if cfg.Warmup == 0 || cmd.Flags().Changed("warmup") {
		warmup, err := time.ParseDuration(loadgenWarmup)
		if err != nil {
			return fmt.Errorf("invalid --warmup: %w", err)
		}
		cfg.Warmup = warmup
	}

	log.Info("starting load generator",
		zap.String("target", cfg.TargetURL),
		zap.Int("tenants", cfg.Tenants),
		zap.Float64("rate", cfg.Rate),
		zap.Duration("duration", cfg.Duration),
		zap.Int("payload_size", cfg.PayloadSize),
		zap.Bool("ramp_up", cfg.RampUp),
		zap.Duration("warmup", cfg.Warmup),
	)

	gen := loadgen.NewGenerator(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		log.Info("received signal, stopping load generator", zap.String("signal", sig.String()))
		cancel()
	}()

	report, err := gen.Run(ctx)
	if err != nil {
		return fmt.Errorf("loadgen run: %w", err)
	}

	report.PrintSummary()

	if loadgenOutput != "" {
		if err := report.WriteJSON(loadgenOutput); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		log.Info("report written", zap.String("path", loadgenOutput))
	}

	return nil
}
