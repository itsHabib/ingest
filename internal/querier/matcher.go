package querier

import (
	"fmt"
	"regexp"
	"strings"
)

// MatchType represents the type of label match operation.
type MatchType int

const (
	// MatchEqual matches when the label value equals the matcher value exactly.
	MatchEqual MatchType = iota
	// MatchRegex matches when the label value matches the regex pattern.
	MatchRegex
)

// Matcher represents a single label matcher parsed from a query parameter.
type Matcher struct {
	Name  string
	Value string
	Type  MatchType
	re    *regexp.Regexp // compiled regex for MatchRegex type
}

// Matches returns true if this matcher matches the given label set.
// For MatchEqual, the label must exist and have the exact value.
// For MatchRegex, the label must exist and match the anchored regex.
func (m Matcher) Matches(labels map[string]string) bool {
	v, ok := labels[m.Name]
	if !ok {
		return false
	}
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchRegex:
		if m.re == nil {
			return false
		}
		return m.re.MatchString(v)
	default:
		return false
	}
}

// MatchAll returns true if all matchers match the given label set.
func MatchAll(matchers []Matcher, labels map[string]string) bool {
	for i := range matchers {
		if !matchers[i].Matches(labels) {
			return false
		}
	}
	return true
}

// ParseMatchers parses a comma-separated list of label matchers.
// Format: "key=value" for exact match, "key=~regex" for regex match.
// Returns an error if the input is malformed.
func ParseMatchers(s string) ([]Matcher, error) {
	if s == "" {
		return nil, nil
	}

	parts := strings.Split(s, ",")
	matchers := make([]Matcher, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		m, err := parseSingleMatcher(part)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}

	return matchers, nil
}

func parseSingleMatcher(s string) (Matcher, error) {
	// Check for regex match: key=~value
	if idx := strings.Index(s, "=~"); idx > 0 {
		name := s[:idx]
		value := s[idx+2:]
		if name == "" {
			return Matcher{}, fmt.Errorf("matcher: empty label name in %q", s)
		}
		// Anchor the regex
		anchored := "^(?:" + value + ")$"
		re, err := regexp.Compile(anchored)
		if err != nil {
			return Matcher{}, fmt.Errorf("matcher: invalid regex in %q: %w", s, err)
		}
		return Matcher{
			Name:  name,
			Value: value,
			Type:  MatchRegex,
			re:    re,
		}, nil
	}

	// Check for exact match: key=value
	if idx := strings.Index(s, "="); idx > 0 {
		name := s[:idx]
		value := s[idx+1:]
		return Matcher{
			Name:  name,
			Value: value,
			Type:  MatchEqual,
		}, nil
	}

	return Matcher{}, fmt.Errorf("matcher: invalid format %q, expected key=value or key=~regex", s)
}
