package querier

import (
	"testing"
	"time"

	"github.com/itsHabib/ingest/internal/event"
)

func makeEventChan(events ...event.Event) <-chan event.Event {
	ch := make(chan event.Event, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)
	return ch
}

func ts(sec int) time.Time {
	return time.Date(2024, 1, 1, 0, 0, sec, 0, time.UTC)
}

func TestMergeStreams_TwoStreams(t *testing.T) {
	s1 := makeEventChan(
		event.Event{Timestamp: ts(1), Value: 1},
		event.Event{Timestamp: ts(3), Value: 3},
		event.Event{Timestamp: ts(5), Value: 5},
	)
	s2 := makeEventChan(
		event.Event{Timestamp: ts(2), Value: 2},
		event.Event{Timestamp: ts(4), Value: 4},
		event.Event{Timestamp: ts(6), Value: 6},
	)

	out := MergeStreams(s1, s2)

	var events []event.Event
	for ev := range out {
		events = append(events, ev)
	}

	if len(events) != 6 {
		t.Fatalf("expected 6 events, got %d", len(events))
	}

	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("not sorted at index %d: %v > %v", i, events[i-1].Timestamp, events[i].Timestamp)
		}
	}

	for i, ev := range events {
		if ev.Value != float64(i+1) {
			t.Errorf("event[%d].Value = %v, want %v", i, ev.Value, float64(i+1))
		}
	}
}

func TestMergeStreams_EmptyStreams(t *testing.T) {
	s1 := makeEventChan()
	s2 := makeEventChan()

	out := MergeStreams(s1, s2)

	var count int
	for range out {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 events, got %d", count)
	}
}

func TestMergeStreams_SingleStream(t *testing.T) {
	s := makeEventChan(
		event.Event{Timestamp: ts(1), Value: 1},
		event.Event{Timestamp: ts(2), Value: 2},
	)

	out := MergeStreams(s)

	var events []event.Event
	for ev := range out {
		events = append(events, ev)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestMergeStreams_NoStreams(t *testing.T) {
	out := MergeStreams()

	var count int
	for range out {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 events, got %d", count)
	}
}

func TestMergeStreams_DuplicateTimestamps(t *testing.T) {
	s1 := makeEventChan(
		event.Event{Timestamp: ts(1), Value: 1},
		event.Event{Timestamp: ts(2), Value: 2},
	)
	s2 := makeEventChan(
		event.Event{Timestamp: ts(1), Value: 10},
		event.Event{Timestamp: ts(2), Value: 20},
	)

	out := MergeStreams(s1, s2)

	var events []event.Event
	for ev := range out {
		events = append(events, ev)
	}

	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	// Verify non-decreasing order
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("not sorted at index %d: %v > %v", i, events[i-1].Timestamp, events[i].Timestamp)
		}
	}
}

func TestMergeStreams_ThreeStreams(t *testing.T) {
	s1 := makeEventChan(
		event.Event{Timestamp: ts(1), Value: 1},
		event.Event{Timestamp: ts(4), Value: 4},
	)
	s2 := makeEventChan(
		event.Event{Timestamp: ts(2), Value: 2},
		event.Event{Timestamp: ts(5), Value: 5},
	)
	s3 := makeEventChan(
		event.Event{Timestamp: ts(3), Value: 3},
		event.Event{Timestamp: ts(6), Value: 6},
	)

	out := MergeStreams(s1, s2, s3)

	var events []event.Event
	for ev := range out {
		events = append(events, ev)
	}

	if len(events) != 6 {
		t.Fatalf("expected 6 events, got %d", len(events))
	}

	for i, ev := range events {
		if ev.Value != float64(i+1) {
			t.Errorf("event[%d].Value = %v, want %v", i, ev.Value, float64(i+1))
		}
	}
}

// --- Benchmarks ---

func BenchmarkMergeStreams(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		streams := make([]<-chan event.Event, 8)
		for s := 0; s < 8; s++ {
			ch := make(chan event.Event, 10000)
			base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < 10000; j++ {
				ch <- event.Event{
					Timestamp: base.Add(time.Duration(j*8+s) * time.Millisecond),
					Value:     float64(j),
				}
			}
			close(ch)
			streams[s] = ch
		}

		out := MergeStreams(streams...)
		for range out {
		}
	}
}
