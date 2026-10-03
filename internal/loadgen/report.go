package loadgen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Report holds the results of a load test run.
type Report struct {
	TotalEvents    int64            `json:"total_events"`
	SuccessEvents  int64            `json:"success_events"`
	FailedEvents   int64            `json:"failed_events"`
	Duration       time.Duration    `json:"duration_ns"`
	EventsPerSec   float64          `json:"events_per_sec"`
	LatencyP50     time.Duration    `json:"latency_p50_ns"`
	LatencyP90     time.Duration    `json:"latency_p90_ns"`
	LatencyP99     time.Duration    `json:"latency_p99_ns"`
	LatencyMax     time.Duration    `json:"latency_max_ns"`
	ErrorBreakdown map[int]int      `json:"error_breakdown"`
	BytesSent      int64            `json:"bytes_sent"`
}

// PrintSummary prints a human-readable summary table to stdout.
func (r *Report) PrintSummary() {
	fmt.Println()
	fmt.Println("=== Load Test Report ===")
	fmt.Printf("  Duration:       %s\n", r.Duration.Round(time.Millisecond))
	fmt.Printf("  Total Events:   %d\n", r.TotalEvents)
	fmt.Printf("  Success:        %d\n", r.SuccessEvents)
	fmt.Printf("  Failed:         %d\n", r.FailedEvents)
	errorPct := float64(0)
	if r.TotalEvents > 0 {
		errorPct = float64(r.FailedEvents) / float64(r.TotalEvents) * 100
	}
	fmt.Printf("  Error Rate:     %.2f%%\n", errorPct)
	fmt.Printf("  Throughput:     %.1f events/sec\n", r.EventsPerSec)
	fmt.Printf("  Bytes Sent:     %d\n", r.BytesSent)
	fmt.Println()
	fmt.Println("  Latency:")
	fmt.Printf("    P50:          %s\n", r.LatencyP50.Round(time.Microsecond))
	fmt.Printf("    P90:          %s\n", r.LatencyP90.Round(time.Microsecond))
	fmt.Printf("    P99:          %s\n", r.LatencyP99.Round(time.Microsecond))
	fmt.Printf("    Max:          %s\n", r.LatencyMax.Round(time.Microsecond))
	if len(r.ErrorBreakdown) > 0 {
		fmt.Println()
		fmt.Println("  Error Breakdown:")
		for code, count := range r.ErrorBreakdown {
			fmt.Printf("    HTTP %d:       %d\n", code, count)
		}
	}
	fmt.Println("========================")
}

// WriteJSON writes the report as JSON to the specified path.
// Parent directories are created if they do not exist.
func (r *Report) WriteJSON(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
