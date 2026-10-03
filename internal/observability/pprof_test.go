package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterPprof(t *testing.T) {
	mux := http.NewServeMux()
	RegisterPprof(mux)

	paths := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
	}

	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("GET %s returned %d, want 200", path, rec.Code)
		}
	}
}
