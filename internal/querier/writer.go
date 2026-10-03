package querier

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/itsHabib/ingest/internal/event"
)

// StreamJSON writes events as newline-delimited JSON to w.
// Sets Content-Type to application/x-ndjson before the first write.
// Flushes after each event if w implements http.Flusher.
// Returns nil on success, or a context error on cancellation.
func StreamJSON(ctx context.Context, w http.ResponseWriter, events <-chan event.Event) error {
	w.Header().Set("Content-Type", "application/x-ndjson")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := enc.Encode(ev); err != nil {
				return err
			}
			if canFlush {
				flusher.Flush()
			}
		}
	}
}
