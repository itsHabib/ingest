package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/itsHabib/ingest/internal/event"

	"go.uber.org/zap"
)

// IndexStore is the interface the handler uses to look up matching block paths.
// Implemented by index-engineer in internal/index.
type IndexStore interface {
	Lookup(ctx context.Context, tenantID string, date string, matchers []Matcher) ([]string, error)
}

// EventReader is the interface the handler uses to read and filter events from blocks.
// The concrete BlockReader type in reader.go can be adapted to satisfy this interface.
type EventReader interface {
	Read(ctx context.Context, keys []string, matchers []Matcher, start, end time.Time) (<-chan event.Event, <-chan error)
}

// Handler serves the query API.
type Handler struct {
	index  IndexStore
	reader EventReader
	logger *zap.Logger
}

// NewHandler creates a new query handler.
func NewHandler(index IndexStore, reader EventReader, logger *zap.Logger) *Handler {
	return &Handler{
		index:  index,
		reader: reader,
		logger: logger,
	}
}

// ServeHTTP routes requests to the appropriate handler method.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/query":
		h.handleQuery(w, r)
	case "/ready":
		h.handleReady(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantID := r.Header.Get("X-Scope-OrgID")
	if tenantID == "" {
		http.Error(w, "missing X-Scope-OrgID header", http.StatusBadRequest)
		return
	}

	startStr := r.URL.Query().Get("start")
	if startStr == "" {
		http.Error(w, "missing required query parameter: start", http.StatusBadRequest)
		return
	}
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid start time: %v", err), http.StatusBadRequest)
		return
	}

	endStr := r.URL.Query().Get("end")
	if endStr == "" {
		http.Error(w, "missing required query parameter: end", http.StatusBadRequest)
		return
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid end time: %v", err), http.StatusBadRequest)
		return
	}

	if !start.Before(end) {
		http.Error(w, "start must be before end", http.StatusBadRequest)
		return
	}

	labelsStr := r.URL.Query().Get("labels")
	matchers, err := ParseMatchers(labelsStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid labels: %v", err), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Enumerate dates in [start, end) and look up blocks for each date.
	var allPaths []string
	for d := start.UTC().Truncate(24 * time.Hour); d.Before(end.UTC()); d = d.AddDate(0, 0, 1) {
		dateStr := d.Format("2006-01-02")
		paths, err := h.index.Lookup(ctx, tenantID, dateStr, matchers)
		if err != nil {
			h.logger.Warn("index lookup failed",
				zap.String("tenant", tenantID),
				zap.String("date", dateStr),
				zap.Error(err),
			)
			continue
		}
		allPaths = append(allPaths, paths...)
	}

	// Read blocks and stream results as NDJSON.
	events, errs := h.reader.Read(ctx, allPaths, matchers, start, end)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)

	for ev := range events {
		if err := enc.Encode(ev); err != nil {
			h.logger.Warn("failed to write event", zap.Error(err))
			return
		}
		if canFlush {
			flusher.Flush()
		}
	}

	// Drain error channel.
	for err := range errs {
		h.logger.Warn("block read error", zap.Error(err))
	}
}
