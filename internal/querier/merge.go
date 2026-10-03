package querier

import (
	"container/heap"

	"github.com/itsHabib/ingest/internal/event"
)

// MergeStreams merges multiple sorted event channels into a single sorted channel.
// Uses a min-heap keyed on Event.Timestamp.
// The output channel is closed when all input channels are drained.
// If no streams are provided, returns a closed channel.
// If one stream is provided, returns it directly (no goroutine).
func MergeStreams(streams ...<-chan event.Event) <-chan event.Event {
	switch len(streams) {
	case 0:
		ch := make(chan event.Event)
		close(ch)
		return ch
	case 1:
		return streams[0]
	}

	out := make(chan event.Event, 256)

	go func() {
		defer close(out)

		h := &eventHeap{}
		heap.Init(h)

		// Seed the heap with the first event from each stream.
		for i, s := range streams {
			ev, ok := <-s
			if ok {
				heap.Push(h, heapItem{event: ev, streamIdx: i})
			}
		}

		for h.Len() > 0 {
			item := heap.Pop(h).(heapItem)
			out <- item.event

			// Pull the next event from the same stream.
			if ev, ok := <-streams[item.streamIdx]; ok {
				heap.Push(h, heapItem{event: ev, streamIdx: item.streamIdx})
			}
		}
	}()

	return out
}

// heapItem pairs an event with the index of the stream it came from.
type heapItem struct {
	event     event.Event
	streamIdx int
}

// eventHeap implements heap.Interface for min-heap by timestamp.
type eventHeap []heapItem

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].event.Timestamp.Before(h[j].event.Timestamp) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
