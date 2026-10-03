package index

import (
	"fmt"
	"regexp"
	"strings"
)

// MatchType defines how a label matcher compares values.
type MatchType int

const (
	MatchEqual     MatchType = iota // =
	MatchNotEqual                   // !=
	MatchRegexp                     // =~
	MatchNotRegexp                  // !~
)

// LabelMatcher matches a label name against a value using the specified match type.
type LabelMatcher struct {
	Name  string
	Value string
	Type  MatchType
	re    *regexp.Regexp // compiled regex for regex match types
}

// Matches returns true if the given value satisfies this matcher.
func (m LabelMatcher) Matches(value string) bool {
	switch m.Type {
	case MatchEqual:
		return value == m.Value
	case MatchNotEqual:
		return value != m.Value
	case MatchRegexp:
		if m.re == nil {
			return false
		}
		return m.re.MatchString(value)
	case MatchNotRegexp:
		if m.re == nil {
			return true
		}
		return !m.re.MatchString(value)
	default:
		return false
	}
}

// MatchAll returns true if all matchers match the given label set.
// A matcher for a label name that is not present in the label set behaves as follows:
//   - MatchEqual: does not match (empty string != expected value, unless value is "")
//   - MatchNotEqual: matches (absent label != value)
//   - MatchRegexp: matches only if regex matches empty string
//   - MatchNotRegexp: matches unless regex matches empty string
func MatchAll(matchers []LabelMatcher, labels map[string]string) bool {
	for _, m := range matchers {
		val := labels[m.Name]
		if !m.Matches(val) {
			return false
		}
	}
	return true
}

// ParseMatchers parses a comma-separated list of label matchers.
// Supported formats:
//
//	name=value    (exact match)
//	name!=value   (not equal)
//	name=~regex   (regex match, auto-anchored with ^...$)
//	name!~regex   (not regex match, auto-anchored with ^...$)
//
// Returns an error if the input is malformed or a regex fails to compile.
func ParseMatchers(query string) ([]LabelMatcher, error) {
	if query == "" {
		return nil, nil
	}

	parts := strings.Split(query, ",")
	matchers := make([]LabelMatcher, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		m, err := parseSingleMatcher(part)
		if err != nil {
			return nil, fmt.Errorf("parse matcher %q: %w", part, err)
		}
		matchers = append(matchers, m)
	}

	return matchers, nil
}

func parseSingleMatcher(s string) (LabelMatcher, error) {
	// Try two-character operators first: !=, =~, !~
	if idx := strings.Index(s, "!~"); idx > 0 {
		name := s[:idx]
		value := s[idx+2:]
		re, err := compileAnchored(value)
		if err != nil {
			return LabelMatcher{}, fmt.Errorf("invalid regex: %w", err)
		}
		return LabelMatcher{Name: name, Value: value, Type: MatchNotRegexp, re: re}, nil
	}
	if idx := strings.Index(s, "!="); idx > 0 {
		name := s[:idx]
		value := s[idx+2:]
		return LabelMatcher{Name: name, Value: value, Type: MatchNotEqual}, nil
	}
	if idx := strings.Index(s, "=~"); idx > 0 {
		name := s[:idx]
		value := s[idx+2:]
		re, err := compileAnchored(value)
		if err != nil {
			return LabelMatcher{}, fmt.Errorf("invalid regex: %w", err)
		}
		return LabelMatcher{Name: name, Value: value, Type: MatchRegexp, re: re}, nil
	}

	// Simple = (exact match)
	if idx := strings.Index(s, "="); idx > 0 {
		name := s[:idx]
		value := s[idx+1:]
		return LabelMatcher{Name: name, Value: value, Type: MatchEqual}, nil
	}

	return LabelMatcher{}, fmt.Errorf("no operator found in %q", s)
}

// compileAnchored wraps the pattern in ^...$ for full-match semantics.
func compileAnchored(pattern string) (*regexp.Regexp, error) {
	anchored := "^(?:" + pattern + ")$"
	return regexp.Compile(anchored)
}
