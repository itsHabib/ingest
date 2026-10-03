package index

import (
	"testing"
)

func TestMatcher_MatchEqual(t *testing.T) {
	m := LabelMatcher{Name: "service", Value: "api", Type: MatchEqual}

	tests := []struct {
		value string
		want  bool
	}{
		{"api", true},
		{"web", false},
		{"", false},
		{"API", false},
	}

	for _, tt := range tests {
		if got := m.Matches(tt.value); got != tt.want {
			t.Errorf("MatchEqual(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestMatcher_MatchNotEqual(t *testing.T) {
	m := LabelMatcher{Name: "service", Value: "api", Type: MatchNotEqual}

	tests := []struct {
		value string
		want  bool
	}{
		{"api", false},
		{"web", true},
		{"", true},
	}

	for _, tt := range tests {
		if got := m.Matches(tt.value); got != tt.want {
			t.Errorf("MatchNotEqual(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestMatcher_MatchRegexp(t *testing.T) {
	re, _ := compileAnchored("prod.*")
	m := LabelMatcher{Name: "env", Value: "prod.*", Type: MatchRegexp, re: re}

	tests := []struct {
		value string
		want  bool
	}{
		{"prod", true},
		{"production", true},
		{"staging", false},
		{"", false},
	}

	for _, tt := range tests {
		if got := m.Matches(tt.value); got != tt.want {
			t.Errorf("MatchRegexp(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestMatcher_MatchNotRegexp(t *testing.T) {
	re, _ := compileAnchored("prod.*")
	m := LabelMatcher{Name: "env", Value: "prod.*", Type: MatchNotRegexp, re: re}

	tests := []struct {
		value string
		want  bool
	}{
		{"prod", false},
		{"production", false},
		{"staging", true},
		{"", true},
	}

	for _, tt := range tests {
		if got := m.Matches(tt.value); got != tt.want {
			t.Errorf("MatchNotRegexp(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestMatcher_RegexpAnchored(t *testing.T) {
	// "prod" should NOT match "production" because it's anchored
	re, _ := compileAnchored("prod")
	m := LabelMatcher{Name: "env", Value: "prod", Type: MatchRegexp, re: re}

	if m.Matches("production") {
		t.Error("anchored regex 'prod' should not match 'production'")
	}
	if !m.Matches("prod") {
		t.Error("anchored regex 'prod' should match 'prod'")
	}
}

func TestMatchAll(t *testing.T) {
	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
		{Name: "env", Value: "debug", Type: MatchNotEqual},
	}

	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			"both match",
			map[string]string{"service": "api", "env": "prod"},
			true,
		},
		{
			"first fails",
			map[string]string{"service": "web", "env": "prod"},
			false,
		},
		{
			"second fails",
			map[string]string{"service": "api", "env": "debug"},
			false,
		},
		{
			"empty labels - equal fails",
			map[string]string{},
			false,
		},
		{
			"extra labels ignored",
			map[string]string{"service": "api", "env": "prod", "version": "v2"},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchAll(matchers, tt.labels); got != tt.want {
				t.Errorf("MatchAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchAll_EmptyMatchers(t *testing.T) {
	labels := map[string]string{"service": "api"}
	if !MatchAll(nil, labels) {
		t.Error("MatchAll with nil matchers should return true")
	}
	if !MatchAll([]LabelMatcher{}, labels) {
		t.Error("MatchAll with empty matchers should return true")
	}
}

func TestParseMatchers_Valid(t *testing.T) {
	tests := []struct {
		input string
		count int
		name  string
		mtype MatchType
		value string
	}{
		{"service=api", 1, "service", MatchEqual, "api"},
		{"env!=debug", 1, "env", MatchNotEqual, "debug"},
		{"name=~prod.*", 1, "name", MatchRegexp, "prod.*"},
		{"name!~test.*", 1, "name", MatchNotRegexp, "test.*"},
	}

	for _, tt := range tests {
		matchers, err := ParseMatchers(tt.input)
		if err != nil {
			t.Errorf("ParseMatchers(%q) error = %v", tt.input, err)
			continue
		}
		if len(matchers) != tt.count {
			t.Errorf("ParseMatchers(%q) count = %d, want %d", tt.input, len(matchers), tt.count)
			continue
		}
		m := matchers[0]
		if m.Name != tt.name {
			t.Errorf("ParseMatchers(%q).Name = %q, want %q", tt.input, m.Name, tt.name)
		}
		if m.Type != tt.mtype {
			t.Errorf("ParseMatchers(%q).Type = %v, want %v", tt.input, m.Type, tt.mtype)
		}
		if m.Value != tt.value {
			t.Errorf("ParseMatchers(%q).Value = %q, want %q", tt.input, m.Value, tt.value)
		}
	}
}

func TestParseMatchers_Multiple(t *testing.T) {
	matchers, err := ParseMatchers("service=api,env!=debug,level=~warn.*")
	if err != nil {
		t.Fatalf("ParseMatchers error = %v", err)
	}
	if len(matchers) != 3 {
		t.Fatalf("expected 3 matchers, got %d", len(matchers))
	}
	if matchers[0].Type != MatchEqual || matchers[0].Name != "service" {
		t.Errorf("matcher[0] = %+v", matchers[0])
	}
	if matchers[1].Type != MatchNotEqual || matchers[1].Name != "env" {
		t.Errorf("matcher[1] = %+v", matchers[1])
	}
	if matchers[2].Type != MatchRegexp || matchers[2].Name != "level" {
		t.Errorf("matcher[2] = %+v", matchers[2])
	}
}

func TestParseMatchers_EmptyString(t *testing.T) {
	matchers, err := ParseMatchers("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matchers != nil {
		t.Errorf("expected nil, got %v", matchers)
	}
}

func TestParseMatchers_InvalidNoOperator(t *testing.T) {
	_, err := ParseMatchers("nolabel")
	if err == nil {
		t.Error("expected error for missing operator")
	}
}

func TestParseMatchers_InvalidRegex(t *testing.T) {
	_, err := ParseMatchers("name=~[invalid")
	if err == nil {
		t.Error("expected error for invalid regex")
	}
}

func BenchmarkMatchAll(b *testing.B) {
	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
		{Name: "env", Value: "debug", Type: MatchNotEqual},
	}
	labels := map[string]string{
		"service": "api",
		"env":     "prod",
		"version": "v2",
		"region":  "us-east-1",
		"cluster": "main",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MatchAll(matchers, labels)
	}
}

func BenchmarkParseMatchers(b *testing.B) {
	query := "service=api,env!=debug,level=~warn.*,region!~eu-.*"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseMatchers(query)
	}
}
