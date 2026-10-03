package querier

import (
	"testing"
)

func TestParseMatchers_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantName []string
		wantVal  []string
		wantType []MatchType
	}{
		{
			name:     "single exact match",
			input:    "service=api",
			wantLen:  1,
			wantName: []string{"service"},
			wantVal:  []string{"api"},
			wantType: []MatchType{MatchEqual},
		},
		{
			name:     "two exact matchers",
			input:    "service=api,env=prod",
			wantLen:  2,
			wantName: []string{"service", "env"},
			wantVal:  []string{"api", "prod"},
			wantType: []MatchType{MatchEqual, MatchEqual},
		},
		{
			name:     "regex matcher",
			input:    "name=~foo.*",
			wantLen:  1,
			wantName: []string{"name"},
			wantVal:  []string{"foo.*"},
			wantType: []MatchType{MatchRegex},
		},
		{
			name:     "mixed exact and regex",
			input:    "service=api,name=~foo.*",
			wantLen:  2,
			wantName: []string{"service", "name"},
			wantVal:  []string{"api", "foo.*"},
			wantType: []MatchType{MatchEqual, MatchRegex},
		},
		{
			name:    "empty string",
			input:   "",
			wantLen: 0,
		},
		{
			name:     "value with equals sign",
			input:    "msg=a=b",
			wantLen:  1,
			wantName: []string{"msg"},
			wantVal:  []string{"a=b"},
			wantType: []MatchType{MatchEqual},
		},
		{
			name:     "empty value exact match",
			input:    "key=",
			wantLen:  1,
			wantName: []string{"key"},
			wantVal:  []string{""},
			wantType: []MatchType{MatchEqual},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers, err := ParseMatchers(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(matchers) != tt.wantLen {
				t.Fatalf("got %d matchers, want %d", len(matchers), tt.wantLen)
			}
			for i, m := range matchers {
				if m.Name != tt.wantName[i] {
					t.Errorf("matcher[%d].Name = %q, want %q", i, m.Name, tt.wantName[i])
				}
				if m.Value != tt.wantVal[i] {
					t.Errorf("matcher[%d].Value = %q, want %q", i, m.Value, tt.wantVal[i])
				}
				if m.Type != tt.wantType[i] {
					t.Errorf("matcher[%d].Type = %d, want %d", i, m.Type, tt.wantType[i])
				}
			}
		})
	}
}

func TestParseMatchers_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "equals only", input: "="},
		{name: "no label", input: "nolabel"},
		{name: "invalid regex", input: "key=~[invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMatchers(tt.input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestMatcherMatches(t *testing.T) {
	tests := []struct {
		name   string
		matcher Matcher
		labels map[string]string
		want   bool
	}{
		{
			name:    "exact match hit",
			matcher: Matcher{Name: "service", Value: "api", Type: MatchEqual},
			labels:  map[string]string{"service": "api", "env": "prod"},
			want:    true,
		},
		{
			name:    "exact match miss",
			matcher: Matcher{Name: "service", Value: "api", Type: MatchEqual},
			labels:  map[string]string{"service": "worker"},
			want:    false,
		},
		{
			name:    "exact match missing label",
			matcher: Matcher{Name: "service", Value: "api", Type: MatchEqual},
			labels:  map[string]string{"env": "prod"},
			want:    false,
		},
		{
			name: "regex match hit",
			matcher: func() Matcher {
				m, _ := ParseMatchers("service=~api.*")
				return m[0]
			}(),
			labels: map[string]string{"service": "api-gateway"},
			want:   true,
		},
		{
			name: "regex match miss - anchored",
			matcher: func() Matcher {
				m, _ := ParseMatchers("service=~api")
				return m[0]
			}(),
			labels: map[string]string{"service": "api-gateway"},
			want:   false,
		},
		{
			name: "regex match exact",
			matcher: func() Matcher {
				m, _ := ParseMatchers("service=~api")
				return m[0]
			}(),
			labels: map[string]string{"service": "api"},
			want:   true,
		},
		{
			name:    "empty labels",
			matcher: Matcher{Name: "service", Value: "api", Type: MatchEqual},
			labels:  map[string]string{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.matcher.Matches(tt.labels)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchAll(t *testing.T) {
	matchers, err := ParseMatchers("service=api,env=prod")
	if err != nil {
		t.Fatalf("ParseMatchers: %v", err)
	}

	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name:   "all match",
			labels: map[string]string{"service": "api", "env": "prod"},
			want:   true,
		},
		{
			name:   "partial match",
			labels: map[string]string{"service": "api", "env": "staging"},
			want:   false,
		},
		{
			name:   "no match",
			labels: map[string]string{"service": "worker", "env": "staging"},
			want:   false,
		},
		{
			name:   "empty matchers always match",
			labels: map[string]string{"anything": "goes"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := matchers
			if tt.name == "empty matchers always match" {
				ms = nil
			}
			got := MatchAll(ms, tt.labels)
			if got != tt.want {
				t.Errorf("MatchAll() = %v, want %v", got, tt.want)
			}
		})
	}
}
