package loadgen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrintSummary(t *testing.T) {
	r := &Report{
		TotalEvents:   1000,
		SuccessEvents: 990,
		FailedEvents:  10,
		Duration:      30 * time.Second,
		EventsPerSec:  33.3,
		LatencyP50:    2 * time.Millisecond,
		LatencyP90:    5 * time.Millisecond,
		LatencyP99:    8 * time.Millisecond,
		LatencyMax:    15 * time.Millisecond,
		ErrorBreakdown: map[int]int{
			429: 7,
			503: 3,
		},
		BytesSent: 102400,
	}

	// Capture stdout
	old := os.Stdout
	rr, w, _ := os.Pipe()
	os.Stdout = w

	r.PrintSummary()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 4096)
	n, _ := rr.Read(buf)
	output := string(buf[:n])

	// Verify key fields appear in output
	checks := []string{
		"Load Test Report",
		"Total Events:   1000",
		"Success:        990",
		"Failed:         10",
		"33.3 events/sec",
		"P50:",
		"P90:",
		"P99:",
		"Max:",
		"HTTP 429",
		"HTTP 503",
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("PrintSummary output missing %q", check)
		}
	}
}

func TestWriteJSON(t *testing.T) {
	r := &Report{
		TotalEvents:   500,
		SuccessEvents: 495,
		FailedEvents:  5,
		Duration:      10 * time.Second,
		EventsPerSec:  50.0,
		LatencyP50:    1 * time.Millisecond,
		LatencyP90:    3 * time.Millisecond,
		LatencyP99:    7 * time.Millisecond,
		LatencyMax:    12 * time.Millisecond,
		ErrorBreakdown: map[int]int{
			429: 5,
		},
		BytesSent: 51200,
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "report.json")

	if err := r.WriteJSON(path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var loaded Report
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if loaded.TotalEvents != r.TotalEvents {
		t.Errorf("TotalEvents = %d, want %d", loaded.TotalEvents, r.TotalEvents)
	}
	if loaded.SuccessEvents != r.SuccessEvents {
		t.Errorf("SuccessEvents = %d, want %d", loaded.SuccessEvents, r.SuccessEvents)
	}
	if loaded.FailedEvents != r.FailedEvents {
		t.Errorf("FailedEvents = %d, want %d", loaded.FailedEvents, r.FailedEvents)
	}
	if loaded.BytesSent != r.BytesSent {
		t.Errorf("BytesSent = %d, want %d", loaded.BytesSent, r.BytesSent)
	}
}

func TestReportJSONRoundTrip(t *testing.T) {
	r := &Report{
		TotalEvents:   1234,
		SuccessEvents: 1200,
		FailedEvents:  34,
		Duration:      60 * time.Second,
		EventsPerSec:  20.5,
		LatencyP50:    2 * time.Millisecond,
		LatencyP90:    4 * time.Millisecond,
		LatencyP99:    9 * time.Millisecond,
		LatencyMax:    20 * time.Millisecond,
		ErrorBreakdown: map[int]int{
			429: 30,
			503: 4,
		},
		BytesSent: 123400,
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var loaded Report
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if loaded.TotalEvents != r.TotalEvents {
		t.Errorf("TotalEvents mismatch")
	}
	if loaded.SuccessEvents != r.SuccessEvents {
		t.Errorf("SuccessEvents mismatch")
	}
	if loaded.FailedEvents != r.FailedEvents {
		t.Errorf("FailedEvents mismatch")
	}
	if loaded.Duration != r.Duration {
		t.Errorf("Duration mismatch: %v vs %v", loaded.Duration, r.Duration)
	}
	if loaded.LatencyP50 != r.LatencyP50 {
		t.Errorf("LatencyP50 mismatch")
	}
	if loaded.LatencyP90 != r.LatencyP90 {
		t.Errorf("LatencyP90 mismatch")
	}
	if loaded.LatencyP99 != r.LatencyP99 {
		t.Errorf("LatencyP99 mismatch")
	}
	if loaded.LatencyMax != r.LatencyMax {
		t.Errorf("LatencyMax mismatch")
	}
	if loaded.BytesSent != r.BytesSent {
		t.Errorf("BytesSent mismatch")
	}
	if len(loaded.ErrorBreakdown) != len(r.ErrorBreakdown) {
		t.Errorf("ErrorBreakdown length mismatch")
	}
	for code, count := range r.ErrorBreakdown {
		if loaded.ErrorBreakdown[code] != count {
			t.Errorf("ErrorBreakdown[%d] = %d, want %d", code, loaded.ErrorBreakdown[code], count)
		}
	}
}
