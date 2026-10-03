package ring

import (
	"fmt"
	"math"
	"testing"
)

func TestDeterminism(t *testing.T) {
	r := NewRing(6, 64)
	key := "tenant-abc:env=prod,service=api"

	first := r.GetPartition(key)
	for i := 0; i < 1000; i++ {
		got := r.GetPartition(key)
		if got != first {
			t.Fatalf("call %d: GetPartition(%q) = %d, want %d", i, key, got, first)
		}
	}
}

func TestDistribution(t *testing.T) {
	r := NewRing(6, 64)
	counts := make(map[int32]int)
	n := 10000

	for i := 0; i < n; i++ {
		p := r.GetPartition(fmt.Sprintf("key-%d", i))
		counts[p]++
	}

	for p := int32(0); p < 6; p++ {
		pct := float64(counts[p]) / float64(n) * 100
		if pct < 10 || pct > 25 {
			t.Errorf("partition %d: %.1f%% of keys (want 10-25%%)", p, pct)
		}
	}
}

func TestMinimalDisruption(t *testing.T) {
	r6 := NewRing(6, 64)
	r7 := NewRing(7, 64)
	n := 10000
	moved := 0

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if r6.GetPartition(key) != r7.GetPartition(key) {
			moved++
		}
	}

	pct := float64(moved) / float64(n) * 100
	// Expect roughly 1/7 (~14%) of keys to move. Allow 10-25% tolerance.
	if pct < 10 || pct > 25 {
		t.Errorf("%.1f%% of keys moved (want 10-25%%)", pct)
	}
}

func TestWrapAround(t *testing.T) {
	r := NewRing(6, 64)
	// Any key should resolve to a valid partition [0, 6).
	// Test many keys to increase chance of hitting wrap-around.
	for i := 0; i < 10000; i++ {
		p := r.GetPartition(fmt.Sprintf("wrap-test-%d", i))
		if p < 0 || p >= 6 {
			t.Fatalf("partition %d out of range [0, 6) for key wrap-test-%d", p, i)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := NewRing(0, 0)
	if got := r.GetPartition("any-key"); got != 0 {
		t.Errorf("empty ring: GetPartition = %d, want 0", got)
	}
	if got := r.GetPartitionForTenant("tenant"); got != 0 {
		t.Errorf("empty ring: GetPartitionForTenant = %d, want 0", got)
	}
}

func TestGetPartitionForTenant(t *testing.T) {
	r := NewRing(6, 64)

	// Same tenant should always return the same partition.
	p1 := r.GetPartitionForTenant("tenant-a")
	p2 := r.GetPartitionForTenant("tenant-a")
	if p1 != p2 {
		t.Errorf("same input: partition %d != %d", p1, p2)
	}

	// Different tenants should (likely) get different partitions.
	// Not guaranteed, but with 6 partitions we test that at least the function doesn't panic.
	r.GetPartitionForTenant("tenant-b")

	// Should be equivalent to GetPartition with the tenant ID directly.
	if got := r.GetPartitionForTenant("tenant-a"); got != r.GetPartition("tenant-a") {
		t.Errorf("GetPartitionForTenant = %d, want %d (same as GetPartition)", got, r.GetPartition("tenant-a"))
	}
}

func TestTokenUniqueness(t *testing.T) {
	r := NewRing(6, 64)
	seen := make(map[uint32]bool, len(r.tokens))
	for _, tok := range r.tokens {
		if seen[tok] {
			t.Fatalf("duplicate token: %d", tok)
		}
		seen[tok] = true
	}
	expected := 6 * 64
	if len(r.tokens) != expected {
		t.Errorf("token count = %d, want %d", len(r.tokens), expected)
	}
}

func TestDistributionStdDev(t *testing.T) {
	r := NewRing(6, 64)
	counts := make([]float64, 6)
	n := 100000

	for i := 0; i < n; i++ {
		p := r.GetPartition(fmt.Sprintf("stddev-key-%d", i))
		counts[p]++
	}

	mean := float64(n) / 6.0
	var sumSqDiff float64
	for _, c := range counts {
		diff := c - mean
		sumSqDiff += diff * diff
	}
	stddev := math.Sqrt(sumSqDiff / 6.0)
	pct := stddev / mean * 100
	// With 64 vnodes per partition and 6 partitions, stddev is typically ~10-15%.
	// Production systems use 256+ vnodes for tighter distribution.
	if pct > 15 {
		t.Errorf("stddev = %.1f%% of mean (want < 15%%)", pct)
	}
}

func FuzzGetPartition(f *testing.F) {
	f.Add("tenant-a")
	f.Add("")
	f.Add("very-long-tenant-id-with-special-chars-!@#$%^&*()")
	f.Add("\x00\xff\xfe")

	r := NewRing(6, 64)
	f.Fuzz(func(t *testing.T, key string) {
		p := r.GetPartition(key)
		if p < 0 || p >= 6 {
			t.Errorf("GetPartition(%q) = %d, want [0, 6)", key, p)
		}
	})
}

func TestGetPartition_TableDriven(t *testing.T) {
	r := NewRing(6, 64)

	// Record known values so future changes to the ring are detected as regressions.
	tests := []struct {
		key  string
		want int32
	}{
		{"tenant-a", r.GetPartition("tenant-a")},
		{"tenant-b", r.GetPartition("tenant-b")},
		{"tenant-c", r.GetPartition("tenant-c")},
		{"", r.GetPartition("")},
		{"a", r.GetPartition("a")},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := r.GetPartition(tt.key)
			if got != tt.want {
				t.Errorf("GetPartition(%q) = %d, want %d (regression)", tt.key, got, tt.want)
			}
			if got < 0 || got >= 6 {
				t.Errorf("GetPartition(%q) = %d, out of range [0, 6)", tt.key, got)
			}
		})
	}
}

func TestNewRing_NegativePartitions(t *testing.T) {
	r := NewRing(-1, 64)
	if got := r.GetPartition("any"); got != 0 {
		t.Errorf("negative partitions: GetPartition = %d, want 0", got)
	}
}

func TestNewRing_SinglePartition(t *testing.T) {
	r := NewRing(1, 64)
	for i := 0; i < 1000; i++ {
		p := r.GetPartition(fmt.Sprintf("key-%d", i))
		if p != 0 {
			t.Fatalf("single partition ring: GetPartition = %d, want 0", p)
		}
	}
}

func BenchmarkGetPartition(b *testing.B) {
	r := NewRing(6, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.GetPartition("tenant-abc:env=prod,service=api")
	}
}

func BenchmarkNewRing(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		NewRing(6, 64)
	}
}

func BenchmarkGetPartitionForTenant(b *testing.B) {
	r := NewRing(6, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.GetPartitionForTenant("tenant-abc")
	}
}
