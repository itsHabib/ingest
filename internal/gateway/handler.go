package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/ring"
	"github.com/itsHabib/ingest/internal/tenant"

	"go.uber.org/zap"
)

// Handler serves the gateway HTTP endpoints.
type Handler struct {
	producer    Producer
	ring        *ring.Ring
	cfg         config.GatewayConfig
	logger      *zap.Logger
	rateLimiter *tenant.RateLimiter // optional; nil disables rate limiting
}

// HandlerOption is a functional option for configuring the Handler.
type HandlerOption func(*Handler)

// WithRateLimiter sets a per-tenant rate limiter on the handler.
func WithRateLimiter(rl *tenant.RateLimiter) HandlerOption {
	return func(h *Handler) {
		h.rateLimiter = rl
	}
}

// NewHandler creates a gateway handler with the given producer, ring, and config.
func NewHandler(producer Producer, r *ring.Ring, cfg config.GatewayConfig, logger *zap.Logger, opts ...HandlerOption) *Handler {
	h := &Handler{
		producer: producer,
		ring:     r,
		cfg:      cfg,
		logger:   logger,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// ServeHTTP routes requests to the appropriate handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/push":
		h.handlePush(w, r)
	case "/ready":
		h.handleReady(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handlePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract tenant ID from header
	tenantID, err := tenant.ExtractTenantID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Per-tenant rate limiting (fail fast before reading body)
	if h.rateLimiter != nil && !h.rateLimiter.Allow(tenantID) {
		observability.RateLimitedTotal.WithLabelValues(tenantID).Inc()
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error":  "rate limit exceeded",
			"tenant": tenantID,
		})
		return
	}

	// Enforce max body size
	maxBody := h.cfg.MaxBodySize
	if maxBody <= 0 {
		maxBody = 5 * 1024 * 1024 // 5 MB default
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Check if this is a max bytes error
		if err.Error() == "http: request body too large" {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to read body: %s", err))
		return
	}

	// Unmarshal events
	var events []event.Event
	if err := json.Unmarshal(body, &events); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err))
		return
	}

	if len(events) == 0 {
		writeError(w, http.StatusBadRequest, "empty events array")
		return
	}

	// Stamp tenant ID and default timestamp on each event
	now := time.Now()
	for i := range events {
		events[i].TenantID = tenantID
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = now
		}
	}

	// Validate each event
	for i, ev := range events {
		if err := ev.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("event[%d]: %s", i, err))
			return
		}
	}

	// Marshal for Kafka
	data, err := json.Marshal(events)
	if err != nil {
		h.logger.Error("failed to marshal events", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Route to partition via hash ring
	partition := h.ring.GetPartitionForTenant(tenantID)

	// Produce to specific Kafka partition with a 5s timeout for backpressure.
	produceCtx, produceCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer produceCancel()

	if err := h.producer.ProduceToPartition(produceCtx, []byte(tenantID), data, partition); err != nil {
		h.logger.Error("failed to produce to Kafka", zap.Error(err), zap.String("tenant_id", tenantID))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "service unavailable"})
		return
	}

	observability.EventsProducedTotal.WithLabelValues(tenantID, strconv.FormatInt(int64(partition), 10)).Add(float64(len(events)))

	h.logger.Info("events accepted",
		zap.String("tenant_id", tenantID),
		zap.Int("count", len(events)),
		zap.Int32("partition", partition),
	)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "ok",
		"events_accepted": len(events),
	})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
