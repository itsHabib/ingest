package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/itsHabib/ingest/internal/tenant"
)

// InstrumentHandler wraps an http.Handler to record request count and duration.
// The tenant label is extracted from the X-Scope-OrgID header; if missing, "unknown" is used.
func InstrumentHandler(name string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		tenantID, err := tenant.ExtractTenantID(r)
		if err != nil {
			tenantID = "unknown"
		}

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(rw.statusCode)

		RequestsTotal.WithLabelValues(tenantID, statusStr).Inc()
		RequestDuration.WithLabelValues(tenantID).Observe(duration)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.wroteHeader = true
	}
	return rw.ResponseWriter.Write(b)
}
