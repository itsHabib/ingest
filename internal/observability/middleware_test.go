package observability

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInstrumentHandler_StatusCode200(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push", nil)
	req.Header.Set("X-Scope-OrgID", "middleware-test-200")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("middleware-test-200", "200"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}

func TestInstrumentHandler_StatusCode400(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push", nil)
	req.Header.Set("X-Scope-OrgID", "middleware-test-400")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("middleware-test-400", "400"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}

func TestInstrumentHandler_StatusCode500(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push", nil)
	req.Header.Set("X-Scope-OrgID", "middleware-test-500")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("middleware-test-500", "500"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}

func TestInstrumentHandler_DefaultStatus200(t *testing.T) {
	// Handler that writes body without calling WriteHeader -- Go defaults to 200.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req.Header.Set("X-Scope-OrgID", "middleware-test-default")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("middleware-test-default", "200"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}

func TestInstrumentHandler_DurationObserved(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req.Header.Set("X-Scope-OrgID", "middleware-test-duration")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Verify the histogram was observed by checking the overall collector has data.
	// We just confirm no panic occurred during observation.
	count := testutil.CollectAndCount(RequestDuration)
	if count == 0 {
		t.Fatal("expected histogram to have observations")
	}
}

func TestInstrumentHandler_MissingTenantUsesUnknown(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})

	handler := InstrumentHandler("test", inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/push", nil)
	// No X-Scope-OrgID header.
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	val := testutil.ToFloat64(RequestsTotal.WithLabelValues("unknown", "400"))
	if val < 1 {
		t.Fatalf("expected counter >= 1, got %f", val)
	}
}

func TestInstrumentHandler_ConcurrentRequests(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := InstrumentHandler("test", inner)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/push", nil)
			req.Header.Set("X-Scope-OrgID", "concurrent-test")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		}()
	}
	wg.Wait()
}
