package loadgen

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// loadProfile is the YAML representation of a load test profile.
type loadProfile struct {
	Name        string  `yaml:"name"`
	Tenants     int     `yaml:"tenants"`
	Rate        float64 `yaml:"rate"`
	Duration    string  `yaml:"duration"`
	PayloadSize int     `yaml:"payload_size"`
	RampUp      bool    `yaml:"ramp_up"`
	WarmupSecs  int     `yaml:"warmup_secs"`
}

// LoadProfile reads a YAML load profile from the given path and returns
// a LoadGenConfig populated with the profile's values.
// Unknown fields in the YAML cause an error (strict parsing).
func LoadProfile(path string) (*LoadGenConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profile %s: %w", path, err)
	}

	var p loadProfile
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parse profile %s: %w", path, err)
	}

	if p.Tenants <= 0 {
		return nil, fmt.Errorf("profile %s: tenants must be > 0", path)
	}
	if p.Rate <= 0 {
		return nil, fmt.Errorf("profile %s: rate must be > 0", path)
	}
	if p.Duration == "" {
		return nil, fmt.Errorf("profile %s: duration is required", path)
	}

	dur, err := time.ParseDuration(p.Duration)
	if err != nil {
		return nil, fmt.Errorf("profile %s: invalid duration %q: %w", path, p.Duration, err)
	}

	warmup := time.Duration(p.WarmupSecs) * time.Second
	if warmup < 0 {
		warmup = 0
	}

	cfg := &LoadGenConfig{
		TargetURL:   "http://localhost:8080",
		Tenants:     p.Tenants,
		Rate:        p.Rate,
		Duration:    dur,
		PayloadSize: p.PayloadSize,
		RampUp:      p.RampUp,
		Warmup:      warmup,
	}

	return cfg, nil
}
