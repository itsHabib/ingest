package ring

import (
	"fmt"
	"testing"
)

func BenchmarkRingGetPartition(b *testing.B) {
	r := NewRing(6, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetPartition("tenant-a")
	}
}

func BenchmarkRingGetPartition_Parallel(b *testing.B) {
	r := NewRing(6, 64)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.GetPartition("tenant-a")
		}
	})
}

func BenchmarkRingGetPartition_VaryingKeys(b *testing.B) {
	r := NewRing(6, 64)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetPartition(keys[i%len(keys)])
	}
}

func BenchmarkRingConstruction(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		NewRing(6, 64)
	}
}
