package observability

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync"
)

var enableProfilingOnce sync.Once

// EnableProfilingRates enables mutex and block profiling at rates suitable for
// profiling sessions. Must be called before capturing mutex or block profiles.
// Safe to call multiple times; only the first call takes effect.
//
// These settings add minimal overhead but should not be enabled unconditionally
// in production. They are automatically called by RegisterPprof.
func EnableProfilingRates() {
	enableProfilingOnce.Do(func() {
		// Sample 1 in 5 mutex contention events.
		runtime.SetMutexProfileFraction(5)
		// Record all blocking events (every blocking event is captured).
		runtime.SetBlockProfileRate(1)
	})
}

// RegisterPprof registers all standard Go pprof handlers on the given mux
// and enables mutex/block profiling rates.
func RegisterPprof(mux *http.ServeMux) {
	EnableProfilingRates()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
