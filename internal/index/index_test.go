package index

import (
	"testing"
	"time"
)

var (
	t0 = time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	t1 = time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	t2 = time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	t3 = time.Date(2024, 1, 15, 18, 0, 0, 0, time.UTC)
	t4 = time.Date(2024, 1, 15, 23, 59, 59, 0, time.UTC)
)

func makeBlock(path string, minT, maxT time.Time, labels map[string]string) BlockMeta {
	return BlockMeta{
		FilePath:   path,
		TenantID:   "tenant-a",
		Partition:  0,
		MinTime:    minT,
		MaxTime:    maxT,
		EventCount: 100,
		Labels:     labels,
		SizeBytes:  1024,
	}
}

func TestIndex_NewIndex(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	if idx.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want tenant-a", idx.TenantID)
	}
	if idx.Date != "2024-01-15" {
		t.Errorf("Date = %q, want 2024-01-15", idx.Date)
	}
	if len(idx.Blocks) != 0 {
		t.Errorf("Blocks length = %d, want 0", len(idx.Blocks))
	}
}

func TestIndex_AddBlock(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	b := makeBlock("path/a", t0, t1, nil)
	idx.AddBlock(b)
	if len(idx.Blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(idx.Blocks))
	}
	if idx.Blocks[0].FilePath != "path/a" {
		t.Errorf("FilePath = %q", idx.Blocks[0].FilePath)
	}
}

func TestIndex_Lookup_TimeOverlap(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	idx.AddBlock(makeBlock("early", t0, t1, map[string]string{"service": "api"}))   // 00:00 - 06:00
	idx.AddBlock(makeBlock("mid", t1, t2, map[string]string{"service": "api"}))     // 06:00 - 12:00
	idx.AddBlock(makeBlock("late", t3, t4, map[string]string{"service": "api"}))    // 18:00 - 23:59

	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		want  int
	}{
		{"full day", t0, t4, 3},
		{"first half", t0, t2, 2},
		{"second half", t2, t4, 2}, // mid block maxTime == t2 == start, so it overlaps
		{"exact block range", t1, t2, 2}, // overlaps with early (edge) and mid
		{"no overlap", time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC), time.Date(2024, 1, 15, 17, 0, 0, 0, time.UTC), 0},
		{"edge: start equals block max", t1, t1, 2}, // early's max == t1, mid's min == t1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := idx.Lookup(tt.start, tt.end, nil)
			if len(results) != tt.want {
				t.Errorf("Lookup(%v, %v) returned %d blocks, want %d", tt.start, tt.end, len(results), tt.want)
			}
		})
	}
}

func TestIndex_Lookup_LabelFilter(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	idx.AddBlock(makeBlock("api-block", t0, t4, map[string]string{"service": "api", "env": "prod"}))
	idx.AddBlock(makeBlock("web-block", t0, t4, map[string]string{"service": "web", "env": "staging"}))

	// With no matchers, both blocks returned
	results := idx.Lookup(t0, t4, nil)
	if len(results) != 2 {
		t.Errorf("no matchers: got %d blocks, want 2", len(results))
	}

	// With matchers — label key must exist in block
	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
	}
	results = idx.Lookup(t0, t4, matchers)
	// Both blocks have "service" key, so both are returned (conservative filtering)
	if len(results) != 2 {
		t.Errorf("service=api: got %d blocks, want 2 (conservative)", len(results))
	}
}

func TestIndex_Lookup_LabelMissing(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	idx.AddBlock(makeBlock("no-labels", t0, t4, map[string]string{}))

	// Exact match for a label that doesn't exist — should not match (empty string != "api")
	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
	}
	results := idx.Lookup(t0, t4, matchers)
	if len(results) != 0 {
		t.Errorf("missing label with exact match: got %d blocks, want 0", len(results))
	}

	// NotEqual for a missing label — should match (absence != "api")
	matchers = []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchNotEqual},
	}
	results = idx.Lookup(t0, t4, matchers)
	if len(results) != 1 {
		t.Errorf("missing label with not-equal: got %d blocks, want 1", len(results))
	}
}

func TestIndex_Lookup_EmptyIndex(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	results := idx.Lookup(t0, t4, nil)
	if len(results) != 0 {
		t.Errorf("empty index: got %d blocks, want 0", len(results))
	}
}

func TestIndex_Lookup_CombinedTimeAndLabels(t *testing.T) {
	idx := NewIndex("tenant-a", "2024-01-15")
	idx.AddBlock(makeBlock("a", t0, t1, map[string]string{"service": "api"}))
	idx.AddBlock(makeBlock("b", t2, t3, map[string]string{"service": "web"}))
	idx.AddBlock(makeBlock("c", t3, t4, map[string]string{"service": "api"}))

	// Time range covers only first half (a, not b or c), no label filter
	results := idx.Lookup(t0, t1, nil)
	if len(results) != 1 {
		t.Errorf("time filter only: got %d, want 1", len(results))
	}

	// Time range covers all, label filter for "service" key present
	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
	}
	results = idx.Lookup(t0, t4, matchers)
	// All 3 blocks have "service" key, so all returned (conservative)
	if len(results) != 3 {
		t.Errorf("label filter conservative: got %d, want 3", len(results))
	}
}

func BenchmarkLookup_100(b *testing.B)   { benchmarkLookup(b, 100) }
func BenchmarkLookup_1000(b *testing.B)  { benchmarkLookup(b, 1000) }
func BenchmarkLookup_10000(b *testing.B) { benchmarkLookup(b, 10000) }

func benchmarkLookup(b *testing.B, numBlocks int) {
	idx := NewIndex("tenant-a", "2024-01-15")
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < numBlocks; i++ {
		minT := base.Add(time.Duration(i) * time.Second)
		maxT := minT.Add(time.Second)
		idx.AddBlock(BlockMeta{
			FilePath:   "block",
			TenantID:   "tenant-a",
			MinTime:    minT,
			MaxTime:    maxT,
			EventCount: 100,
			Labels:     map[string]string{"service": "api", "env": "prod"},
			SizeBytes:  1024,
		})
	}

	matchers := []LabelMatcher{
		{Name: "service", Value: "api", Type: MatchEqual},
	}
	// Query covering ~10% of the time range
	start := base.Add(time.Duration(numBlocks/2) * time.Second)
	end := start.Add(time.Duration(numBlocks/10) * time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Lookup(start, end, matchers)
	}
}
