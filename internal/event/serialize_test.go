package event

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestMarshalUnmarshalEvents_RoundTrip(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	events := []Event{
		NewEvent("tenant-a", ts, map[string]string{"service": "api"}, 42.0),
		NewEvent("tenant-b", ts.Add(time.Second), map[string]string{"env": "prod"}, 99.5),
	}

	data, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var got []Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if !reflect.DeepEqual(events, got) {
		t.Errorf("round-trip mismatch:\n  got  = %+v\n  want = %+v", got, events)
	}
}

func TestMarshalUnmarshalBatch_RoundTrip(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	b := NewBatch("tenant-a", 2)
	b.Add(NewEvent("tenant-a", ts, map[string]string{"k": "v"}, 1.0))
	b.Add(NewEvent("tenant-a", ts.Add(time.Second), map[string]string{"k2": "v2"}, 2.0))

	data, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var got Batch
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if got.TenantID != b.TenantID {
		t.Errorf("TenantID = %q, want %q", got.TenantID, b.TenantID)
	}
	if got.Partition != b.Partition {
		t.Errorf("Partition = %d, want %d", got.Partition, b.Partition)
	}
	if len(got.Events) != len(b.Events) {
		t.Fatalf("Events len = %d, want %d", len(got.Events), len(b.Events))
	}
	for i := range b.Events {
		if !reflect.DeepEqual(got.Events[i], b.Events[i]) {
			t.Errorf("Events[%d] mismatch:\n  got  = %+v\n  want = %+v", i, got.Events[i], b.Events[i])
		}
	}
}

func TestUnmarshalEvents_InvalidJSON(t *testing.T) {
	var events []Event
	err := json.Unmarshal([]byte(`not json`), &events)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestUnmarshalBatch_InvalidJSON(t *testing.T) {
	var batch Batch
	err := json.Unmarshal([]byte(`{broken`), &batch)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestMarshalEvents_Empty(t *testing.T) {
	data, err := json.Marshal([]Event{})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("json.Marshal([]) = %q, want %q", string(data), "[]")
	}
}

func TestMarshalEvents_WithPayload(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	payload := []byte("binary data here")
	e := NewEvent("tenant-a", ts, nil, 1.0)
	e.Payload = payload

	data, err := json.Marshal([]Event{e})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var got []Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if !reflect.DeepEqual(got[0].Payload, payload) {
		t.Errorf("Payload = %v, want %v", got[0].Payload, payload)
	}
}

func makeBenchEvents(n int) []Event {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	events := make([]Event, n)
	for i := range events {
		events[i] = NewEvent("tenant-bench", ts, map[string]string{"i": "v", "service": "api"}, float64(i))
	}
	return events
}

func BenchmarkMarshalEvents_10(b *testing.B) {
	events := makeBenchEvents(10)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(events)
	}
}

func BenchmarkMarshalEvents_1000(b *testing.B) {
	events := makeBenchEvents(1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(events)
	}
}

func BenchmarkUnmarshalEvents_10(b *testing.B) {
	data, _ := json.Marshal(makeBenchEvents(10))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var events []Event
		_ = json.Unmarshal(data, &events)
	}
}

func BenchmarkUnmarshalEvents_1000(b *testing.B) {
	data, _ := json.Marshal(makeBenchEvents(1000))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var events []Event
		_ = json.Unmarshal(data, &events)
	}
}

// Single-event benchmarks

func BenchmarkMarshalEvent(b *testing.B) {
	ev := makeBenchEvents(1)[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(ev)
	}
}

func BenchmarkUnmarshalEvent(b *testing.B) {
	data, _ := json.Marshal(makeBenchEvents(1)[0])
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev Event
		_ = json.Unmarshal(data, &ev)
	}
}

// Batch-level benchmarks (marshal full Batch struct with 100/1000 events)

func makeBenchBatch(n int) Batch {
	batch := NewBatch("tenant-bench", 0)
	batch.Events = makeBenchEvents(n)
	return batch
}

func BenchmarkMarshalBatch_100(b *testing.B) {
	batch := makeBenchBatch(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(batch)
	}
}

func BenchmarkMarshalBatch_1000(b *testing.B) {
	batch := makeBenchBatch(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(batch)
	}
}

func BenchmarkUnmarshalBatch_100(b *testing.B) {
	data, _ := json.Marshal(makeBenchBatch(100))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var batch Batch
		_ = json.Unmarshal(data, &batch)
	}
}

func BenchmarkUnmarshalBatch_1000(b *testing.B) {
	data, _ := json.Marshal(makeBenchBatch(1000))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var batch Batch
		_ = json.Unmarshal(data, &batch)
	}
}
