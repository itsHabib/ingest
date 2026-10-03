# Observability Engineer Plan

## You are in PLAN MODE.

### Project
I want to do a **production-readiness enhancement for the ingestion pipeline**.

**Goal:** build **Prometheus metrics, HTTP instrumentation middleware, pprof endpoints, and a Grafana dashboard** so that all components of the ingestion pipeline are observable in production.

### Role + Scope (fill in)
- **Role:** Observability Engineer
- **Scope:** I own all Prometheus metric definitions, HTTP instrumentation middleware, pprof registration, metrics exposition endpoints (admin ports), Prometheus scrape configuration, Grafana datasource provisioning, and the ingestion dashboard. I also own instrumenting existing gateway and ingester code with metric increments/observations. I do NOT own rate limiting logic (tenancy-engineer), S3/MinIO storage backend (integration-engineer), or the load generator.
- **File you will write:** `/docs/production/plans/observability-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: Define all Prometheus metrics (counters, histograms, gauges) for gateway and ingester using `promauto` for auto-registration.
- FR2: HTTP middleware that wraps handlers to record request count and duration, capturing status codes via a `responseWriter` wrapper.
- FR3: pprof handler registration on a separate admin mux for both gateway and ingester.
- FR4: Instrument existing gateway handler (RequestsTotal, RequestDuration, EventsProducedTotal, RateLimitedTotal) and ingester components (EventsConsumedTotal, BatchFlushTotal, BatchFlushDuration, BatchSizeEvents, KafkaLag).
- FR5: Expose `/metrics` on separate admin HTTP servers (gateway :9100, ingester :9101).
- FR6: Prometheus scrape config targeting gateway and ingester admin ports.
- FR7: Grafana auto-provisioned datasource + dashboard with key panels.
- Tests required: unit tests for middleware (status code capture, metric increments) and metric registration (no panics on duplicate registration).
- Metrics required: this IS the metrics work.

## Non-Functional Requirements
- Language/runtime: Go (matches existing codebase)
- Local dev: docker-compose brings up Prometheus + Grafana alongside Kafka
- Observability: `/metrics` + `/debug/pprof/*` on admin ports; Grafana dashboard
- Safety: admin port is separate from data port so metrics scraping cannot interfere with ingestion traffic
- Documentation: CLAUDE.md + EXPLAIN.md contributions proposed below
- Performance: `promauto` + package-level vars means zero-alloc metric operations on the hot path; histogram buckets chosen to cover sub-ms to 10s range

---

## Assumptions / System Model
- Deployment environment: local docker-compose; Prometheus and Grafana run as containers alongside Kafka
- Failure modes: if Prometheus is down, metrics are still collected in-process (no impact on pipeline); if admin port fails to bind, the data path still works (fail-open)
- Delivery guarantees: N/A for observability (best-effort scraping)
- Multi-tenancy: tenant label on gateway metrics allows per-tenant cardinality; ingester metrics use partition label. Cardinality is bounded by number of tenants and partitions (both small in MVP).

---

## Data Model (as relevant to your role)

### Metric Definitions

**Gateway metrics** (prefix `ingest_gateway_`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `requests_total` | CounterVec | tenant, status_code | Total HTTP requests |
| `request_duration_seconds` | HistogramVec | tenant | Request latency |
| `events_produced_total` | CounterVec | tenant, partition | Events sent to Kafka |
| `rate_limited_total` | CounterVec | tenant | Requests rejected by rate limiter |

**Ingester metrics** (prefix `ingest_ingester_`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `events_consumed_total` | CounterVec | partition | Events read from Kafka |
| `batch_flush_total` | CounterVec | tenant, status | Batches flushed (success/error) |
| `batch_flush_duration_seconds` | HistogramVec | tenant | Flush latency |
| `batch_size_events` | Histogram | (none) | Events per flushed batch |
| `kafka_lag` | GaugeVec | partition | Consumer lag per partition |

**Histogram buckets:** `.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10`

No persistence required -- metrics are in-process and scraped by Prometheus.

---

## APIs (as relevant to your role)

### Admin HTTP Endpoints

**Gateway admin server** (default `:9100`):
- `GET /metrics` -- Prometheus exposition format (via `promhttp.Handler()`)
- `GET /debug/pprof/*` -- standard Go pprof handlers

**Ingester admin server** (default `:9101`):
- `GET /metrics` -- Prometheus exposition format
- `GET /debug/pprof/*` -- standard Go pprof handlers

### Go API Surface

```
package observability

// Metric vars (package-level, auto-registered)
var RequestsTotal       *prometheus.CounterVec
var RequestDuration     *prometheus.HistogramVec
var EventsProducedTotal *prometheus.CounterVec
var RateLimitedTotal    *prometheus.CounterVec
var EventsConsumedTotal *prometheus.CounterVec
var BatchFlushTotal     *prometheus.CounterVec
var BatchFlushDuration  *prometheus.HistogramVec
var BatchSizeEvents     prometheus.Histogram
var KafkaLag            *prometheus.GaugeVec

// Middleware
func InstrumentHandler(name string, next http.Handler) http.Handler

// pprof
func RegisterPprof(mux *http.ServeMux)
```

No request/response schemas -- these are all Go function signatures and Prometheus endpoints.

---

## Architecture / Component Boundaries (as relevant)

### Components I Touch

1. **`internal/observability/`** (NEW package, 3 files):
   - `metrics.go` -- metric definitions as package-level vars
   - `middleware.go` -- `InstrumentHandler` + `responseWriter` wrapper
   - `pprof.go` -- `RegisterPprof` helper

2. **`internal/gateway/handler.go`** (MODIFY):
   - After Kafka produce: `observability.EventsProducedTotal.WithLabelValues(tenantID, partitionStr).Inc()`
   - Rate limit path (when tenancy-engineer adds it): `observability.RateLimitedTotal.WithLabelValues(tenantID).Inc()`
   - Wrap handler with `observability.InstrumentHandler("gateway", handler)` in `cmd/gateway.go`

3. **`internal/ingester/consumer.go`** (MODIFY):
   - In `EachRecord`: `observability.EventsConsumedTotal.WithLabelValues(partitionStr).Inc()`

4. **`internal/ingester/flusher.go`** (MODIFY):
   - On success: `observability.BatchFlushTotal.WithLabelValues(tenantID, "success").Inc()`
   - On error: `observability.BatchFlushTotal.WithLabelValues(tenantID, "error").Inc()`
   - Observe duration: `observability.BatchFlushDuration.WithLabelValues(tenantID).Observe(elapsed.Seconds())`
   - Observe batch size: `observability.BatchSizeEvents.Observe(float64(batch.Len()))`

5. **`cmd/gateway.go`** (MODIFY):
   - Start admin HTTP server on :9100 with `/metrics` + pprof
   - Wrap data handler with instrumentation middleware

6. **`cmd/ingester.go`** (MODIFY):
   - Start admin HTTP server on :9101 with `/metrics` + pprof

7. **`docker-compose.yml`** (MODIFY):
   - Add `prometheus` service (port 9090)
   - Add `grafana` service (port 3000)

8. **`configs/`** (NEW files):
   - `prometheus.yml` -- scrape config
   - `grafana/provisioning/datasources/prometheus.yml`
   - `grafana/provisioning/dashboards/dashboard.yml`
   - `grafana/provisioning/dashboards/ingestion.json`

### Concurrency Model
- Metric vars are thread-safe by design (Prometheus client_golang uses atomic operations internally)
- `responseWriter` wrapper is per-request, no shared state
- Admin HTTP server runs in its own goroutine, independent of data path

### Config Propagation
- Admin port is added to `GatewayConfig` and `IngesterConfig` as new fields with defaults
- No dynamic config -- admin port is set at startup

---

## Correctness Invariants (must be explicit)

1. **No duplicate metric registration:** using `promauto` with package-level `var` init ensures single registration. Tests verify no panic on import.
2. **Status code capture is accurate:** `responseWriter` wrapper intercepts `WriteHeader` call; default to 200 if `WriteHeader` was never called (Go's implicit 200).
3. **Tenant label consistency:** gateway metrics use the same tenant ID extracted by `tenant.ExtractTenantID` -- no second extraction.
4. **Admin port isolation:** metrics and pprof MUST be on a separate port from the data path. A crash or slow scrape on the admin port must not affect ingestion.
5. **Histogram buckets are consistent:** all duration histograms use the same bucket set.

---

## Tests

### Unit Tests

- **`internal/observability/middleware_test.go`:**
  - Test that `InstrumentHandler` records status code correctly (200, 400, 500)
  - Test that request duration is observed (histogram count > 0 after request)
  - Test that `WriteHeader` not called defaults to 200
  - Test concurrent requests do not race (run with `-race`)

- **`internal/observability/metrics_test.go`:**
  - Test that all metric vars are non-nil after package init
  - Test that WithLabelValues does not panic for expected label sets
  - Test incrementing a counter and reading it back via `testutil.ToFloat64`

- **`internal/observability/pprof_test.go`:**
  - Test that `RegisterPprof` adds handlers at `/debug/pprof/` (httptest request returns 200)

### Integration Tests

- Start gateway + admin server, send requests, scrape `/metrics`, verify counters incremented
- This can be done in `internal/gateway/smoke_test.go` or a new `observability_test.go`

### Commands

```bash
go test ./internal/observability/... -v -race
go test ./internal/observability/... -count=1
go vet ./internal/observability/...
```

---

## Benchmarks + "Success"

N/A -- Observability code is on the hot path but consists of atomic counter increments and histogram observations, which are O(1) operations. The Prometheus client_golang library is already benchmarked upstream. If performance concerns arise during the benchmark-engineer phase, they can profile the instrumented hot path with pprof. No custom benchmarks needed for this role.

---

# Please produce (no code yet):

## 1) Recommended API Surface

### Package `internal/observability`

**metrics.go:**
- 9 package-level metric vars (see Data Model table above)
- All initialized via `promauto.NewCounterVec` / `promauto.NewHistogramVec` / `promauto.NewGaugeVec` in `init()` or var block
- Single shared histogram bucket slice: `var durationBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}`

**middleware.go:**
- `func InstrumentHandler(name string, next http.Handler) http.Handler` -- returns handler that records `RequestsTotal` and `RequestDuration`
- `type responseWriter struct` (unexported) -- wraps `http.ResponseWriter`, captures status code

**pprof.go:**
- `func RegisterPprof(mux *http.ServeMux)` -- registers `/debug/pprof/`, `/debug/pprof/cmdline`, `/debug/pprof/profile`, `/debug/pprof/symbol`, `/debug/pprof/trace`

### Instrumentation Points (modifications to existing code)

- `gateway/handler.go`: 2 metric calls (EventsProducedTotal, RateLimitedTotal)
- `ingester/consumer.go`: 1 metric call (EventsConsumedTotal)
- `ingester/flusher.go`: 3 metric calls (BatchFlushTotal, BatchFlushDuration, BatchSizeEvents)
- `cmd/gateway.go`: start admin server, wrap handler with middleware
- `cmd/ingester.go`: start admin server

## 2) Folder Structure

```
internal/
  observability/
    metrics.go              # metric definitions (NEW)
    metrics_test.go         # metric registration tests (NEW)
    middleware.go           # HTTP instrumentation middleware (NEW)
    middleware_test.go      # middleware tests (NEW)
    pprof.go                # pprof handler registration (NEW)
    pprof_test.go           # pprof tests (NEW)

configs/
  prometheus.yml                                    # scrape config (NEW)
  grafana/
    provisioning/
      datasources/
        prometheus.yml                              # datasource (NEW)
      dashboards/
        dashboard.yml                               # provisioning config (NEW)
        ingestion.json                              # dashboard JSON (NEW)
```

**Ownership:** all files above are owned by observability-engineer. Modifications to `gateway/handler.go`, `ingester/consumer.go`, `ingester/flusher.go`, `cmd/gateway.go`, `cmd/ingester.go`, and `docker-compose.yml` are shared with other roles but scoped to adding metric calls and admin server startup.

## 3) Step-by-Step Task Plan

See "Tighten the plan" section below.

## 4) Benchmark Plan

N/A -- see Benchmarks section above.

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Define all Prometheus metrics
- **Outcome:** `internal/observability/metrics.go` with all 9 metric vars defined and auto-registered.
- **Files to create/modify:** `internal/observability/metrics.go`, `internal/observability/metrics_test.go`
- **Verification:**
  ```bash
  go test ./internal/observability/... -v -race
  go vet ./internal/observability/...
  ```
- **Commit:** `feat(observability): define Prometheus metrics for gateway and ingester`

### Task 2: Build HTTP instrumentation middleware
- **Outcome:** `InstrumentHandler` wraps any `http.Handler` and records request count + duration with tenant and status_code labels. responseWriter wrapper captures status code.
- **Files to create/modify:** `internal/observability/middleware.go`, `internal/observability/middleware_test.go`
- **Verification:**
  ```bash
  go test ./internal/observability/... -v -race
  ```
- **Commit:** `feat(observability): add HTTP instrumentation middleware`

### Task 3: Add pprof registration helper
- **Outcome:** `RegisterPprof` registers all standard pprof handlers on a given mux.
- **Files to create/modify:** `internal/observability/pprof.go`, `internal/observability/pprof_test.go`
- **Verification:**
  ```bash
  go test ./internal/observability/... -v -race
  ```
- **Commit:** `feat(observability): add pprof registration helper`

### Task 4: Instrument gateway and ingester code
- **Outcome:** Gateway handler increments EventsProducedTotal and RateLimitedTotal. Ingester consumer increments EventsConsumedTotal. Flusher records BatchFlushTotal, BatchFlushDuration, BatchSizeEvents. Gateway and ingester cmd files start admin HTTP servers on :9100/:9101 with /metrics and /debug/pprof/*.
- **Files to create/modify:** `internal/gateway/handler.go`, `internal/ingester/consumer.go`, `internal/ingester/flusher.go`, `cmd/gateway.go`, `cmd/ingester.go`, `internal/config/config.go` (add AdminPort fields)
- **Verification:**
  ```bash
  go build ./...
  go test ./... -v -race
  go vet ./...
  ```
- **Commit:** `feat(observability): instrument gateway and ingester with Prometheus metrics`

### Task 5: Add Prometheus and Grafana to docker-compose
- **Outcome:** `docker compose up` starts Prometheus (scraping gateway + ingester) and Grafana (with auto-provisioned datasource and dashboard).
- **Files to create/modify:** `docker-compose.yml`, `configs/prometheus.yml`, `configs/grafana/provisioning/datasources/prometheus.yml`, `configs/grafana/provisioning/dashboards/dashboard.yml`, `configs/grafana/provisioning/dashboards/ingestion.json`
- **Verification:**
  ```bash
  docker compose config   # validates compose file
  docker compose up -d
  curl -s http://localhost:9090/-/ready  # Prometheus
  curl -s http://localhost:3000/api/health  # Grafana
  ```
- **Commit:** `feat(observability): add Prometheus and Grafana to docker-compose with dashboard`

---

## Top 3 Risks/Unknowns

1. **High-cardinality tenant labels:** If many tenants exist, `tenant` label on histograms can cause memory pressure. **Validation (<15 min):** Check Prometheus client_golang docs for recommended cardinality limits. For MVP with 5-10 tenants this is fine. Add a comment noting the risk for future work.

2. **Coordination with tenancy-engineer on rate limit instrumentation:** The `RateLimitedTotal` counter needs to be incremented in the rate limiting code path, which is owned by tenancy-engineer. **Validation (<15 min):** Review the rate limiter integration point in `handler.go`. The observability-engineer adds the metric definition; the tenancy-engineer increments it where rate limiting occurs, or we add it after their code lands.

3. **Admin port conflict on host:** Gateway (:9100) and ingester (:9101) default admin ports might conflict with node_exporter (:9100) if running locally. **Validation (<15 min):** Make the ports configurable via config/flags. Use 9100/9101 as defaults, document the flag to change them.

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From Observability Engineer

### Coding style rules
- All Prometheus metrics are defined in `internal/observability/metrics.go` as package-level vars -- never define metrics inline in handlers
- Use `promauto` for metric registration (auto-registers with default registry)
- Metric naming follows Grafana convention: `ingest_<component>_<metric_name>` with `_total` suffix for counters, `_seconds` suffix for duration histograms
- Always use label values from the same extraction point (e.g., tenant from `tenant.ExtractTenantID`, not from event body)

### Dev commands
```bash
# Run observability tests
go test ./internal/observability/... -v -race

# Check metrics endpoint (gateway)
curl -s http://localhost:9100/metrics | grep ingest_

# Check metrics endpoint (ingester)
curl -s http://localhost:9101/metrics | grep ingest_

# Access Grafana dashboard
open http://localhost:3000  # anonymous auth, no login needed

# Access Prometheus
open http://localhost:9090

# pprof (gateway)
go tool pprof http://localhost:9100/debug/pprof/profile?seconds=10
```

### Before you commit checklist
- [ ] `go test ./internal/observability/... -v -race` passes
- [ ] `go vet ./...` clean
- [ ] No duplicate metric names (will panic at startup)
- [ ] All new metrics have tests verifying non-nil and correct label sets
- [ ] Admin port does not conflict with data port

### Guardrails
- Never expose `/metrics` or `/debug/pprof` on the data port (8080/8081) -- always use separate admin port
- Keep tenant label cardinality bounded -- do not add unbounded labels (like request IDs) to metrics
- Histogram buckets must cover the expected range -- do not use default buckets for sub-millisecond operations

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

## Observability

### Flow / Architecture
- Both gateway and ingester expose a separate admin HTTP server on a dedicated port
- Admin server serves `/metrics` (Prometheus exposition format) and `/debug/pprof/*` (Go profiling)
- Prometheus scrapes both admin servers every 5 seconds
- Grafana auto-provisions a datasource pointing to Prometheus and loads the ingestion dashboard

### Key Engineering Decisions + Tradeoffs
- **Separate admin port:** isolates observability traffic from data traffic; a slow Prometheus scrape cannot block ingestion requests
- **`promauto` for registration:** eliminates boilerplate but means metrics are registered at package init time (acceptable for a service, not a library)
- **Tenant label on gateway metrics:** enables per-tenant visibility but introduces cardinality proportional to tenant count; acceptable for MVP scale
- **No custom Prometheus registry:** uses the default global registry for simplicity; `promhttp.Handler()` works out of the box

### Limits of MVP + Next Steps
- No alerting rules yet (Prometheus alertmanager integration)
- No exemplars or trace correlation
- Dashboard is static JSON -- no Grafana-as-code tooling (grafonnet)
- Kafka consumer lag is set manually from franz-go metadata; a more accurate approach would use Kafka admin API to fetch committed vs. latest offsets

### How to Run Locally + How to Validate
- `docker compose up -d` starts Prometheus (`:9090`) and Grafana (`:3000`)
- Start gateway: `go run . gateway` -- admin on `:9100`
- Start ingester: `go run . ingester` -- admin on `:9101`
- Verify metrics: `curl localhost:9100/metrics | grep ingest_gateway`
- Open Grafana at `http://localhost:3000`, navigate to "Ingestion Pipeline" dashboard
- Send traffic with loadgen or curl, observe metrics update in real time

---

## READY FOR APPROVAL
