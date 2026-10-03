package loadgen

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadProfile(t *testing.T) {
	yaml := `
name: test-profile
tenants: 5
rate: 500
duration: "60s"
payload_size: 100
ramp_up: true
warmup_secs: 5
`
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}

	if cfg.Tenants != 5 {
		t.Errorf("Tenants = %d, want 5", cfg.Tenants)
	}
	if cfg.Rate != 500 {
		t.Errorf("Rate = %f, want 500", cfg.Rate)
	}
	if cfg.Duration != 60*time.Second {
		t.Errorf("Duration = %v, want 60s", cfg.Duration)
	}
	if cfg.PayloadSize != 100 {
		t.Errorf("PayloadSize = %d, want 100", cfg.PayloadSize)
	}
	if !cfg.RampUp {
		t.Error("RampUp = false, want true")
	}
	if cfg.Warmup != 5*time.Second {
		t.Errorf("Warmup = %v, want 5s", cfg.Warmup)
	}
}

func TestLoadProfileInvalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "missing tenants",
			yaml: `
name: bad
rate: 100
duration: "10s"
`,
		},
		{
			name: "missing rate",
			yaml: `
name: bad
tenants: 5
duration: "10s"
`,
		},
		{
			name: "missing duration",
			yaml: `
name: bad
tenants: 5
rate: 100
`,
		},
		{
			name: "invalid duration",
			yaml: `
name: bad
tenants: 5
rate: 100
duration: "notaduration"
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "bad.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0o644); err != nil {
				t.Fatal(err)
			}

			_, err := LoadProfile(path)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestLoadProfileDefaults(t *testing.T) {
	// Minimal valid profile — optional fields should get zero values.
	yaml := `
name: minimal
tenants: 1
rate: 10
duration: "5s"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("LoadProfile: %v", err)
	}

	if cfg.PayloadSize != 0 {
		t.Errorf("PayloadSize = %d, want 0 (default)", cfg.PayloadSize)
	}
	if cfg.RampUp != false {
		t.Error("RampUp = true, want false (default)")
	}
	if cfg.Warmup != 0 {
		t.Errorf("Warmup = %v, want 0 (default)", cfg.Warmup)
	}
}

func TestLoadProfileFileNotFound(t *testing.T) {
	_, err := LoadProfile("/nonexistent/path/profile.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}
