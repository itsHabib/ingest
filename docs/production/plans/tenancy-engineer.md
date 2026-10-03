# Tenancy Engineer Plan

## You are in PLAN MODE.

### Project
I want to add **per-tenant rate limiting and backpressure propagation** to the ingestion pipeline.

**Goal:** build a **rate limiting and backpressure layer** in which we **protect the pipeline from tenant overload and propagate storage failures back through the system** so that no single tenant can starve others and storage outages are handled gracefully.

### Role + Scope (fill in)
- **Role:** Tenancy Engineer
- **Scope:** Per-tenant rate limiting (`internal/tenant/ratelimit.go`), gateway integration for 429 responses, ingester backpressure (pause/resume Kafka consumption on storage failure), gateway 503 on Kafka produce timeout. I do NOT own metrics instrumentation (observability-engineer), S3 storage backend (integration-engineer), or load generator.
- **File you will write:** `/docs/production/plans/tenancy-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- **FR1:** Per-tenant token bucket rate limiter using `golang.org/x/time/rate` with configurable default rate (1000 events/sec) and burst (5000 events).
- **FR2:** Lazy limiter creation -- allocate a `rate.Limiter` on first request for a given tenant; concurrent-safe via `sync.RWMutex` with read-lock fast path.
- **FR3:** Per-tenant override -- `SetTenantLimit(tenantID, rate, burst)` to configure individual tenant limits without restarting.
- **FR4:** Gateway returns HTTP 429 with JSON body `{"error":"rate limit exceeded","tenant":"..."}` and `Retry-After: 1` header when a tenant exceeds its rate.
- **FR5:** Ingester pauses Kafka consumption (`PauseFetchTopics`) when flusher fails after all retries; background probe tests storage every 5s and resumes when healthy.
- **FR6:** Gateway returns HTTP 503 with JSON body `{"error":"service unavailable"}` when Kafka produce times out (5s deadline).
- **Tests required:** Unit tests for rate limiter (boundary, isolation, burst, concurrency, overrides). Unit test for gateway 429 path. Unit test for gateway 503 path.
- **Metrics required:** N/A for this role (observability-engineer owns metrics); however, the rate limiter and backpressure code will expose hooks/fields that the observability-engineer can instrument.

## Non-Functional Requirements
- Language/runtime: Go 1.25+, uses `golang.org/x/time/rate`
- Local dev: no new infra dependencies; rate limiter is in-process
- Observability: rate limiter exposes `Allow()` bool that can be counted by metrics layer; pause/resume events are logged via `zap`
- Safety: rate limiter never panics; backpressure probe is non-blocking and stops on context cancellation
- Documentation: CLAUDE.md + EXPLAIN.md contributions proposed below
- Performance: rate limiter is on the hot path -- must be lock-contention-free for the common case (RLock fast path when limiter already exists)

---

## Assumptions / System Model
- Deployment environment: local docker-compose; rate limiter is in-process (no distributed rate limiting needed for MVP)
- Failure modes:
  - Storage down: flusher returns errors after retries -> ingester pauses consumption -> Kafka lag grows -> if Kafka queue fills, produce times out -> gateway returns 503
  - Tenant burst: rate limiter rejects with 429; other tenants unaffected
  - Process crash: rate limiter state is lost (acceptable -- limiters re-create lazily)
- Delivery guarantees: at-least-once unchanged; rate limiting happens before Kafka produce so rejected events are never produced
- Multi-tenancy: per-tenant isolation is the core concern of this role

---

## Data Model (as relevant to your role)

- **RateLimiter**
  - `mu sync.RWMutex` -- protects `limiters` map
  - `limiters map[string]*rate.Limiter` -- keyed by tenant ID
  - `defaultRate float64` -- default events/sec (1000)
  - `defaultBurst int` -- default burst size (5000)
  - No persistence: in-memory only, re-created on startup

- **Validation rules:** tenant ID must be non-empty (already enforced by `tenant.ExtractTenantID`). Rate and burst must be positive.

- **Versioning:** N/A -- no persisted config for rate limits in MVP. Per-tenant overrides are set programmatically.

---

## APIs (as relevant to your role)

### Rate Limiter (internal Go API)

```go
// NewRateLimiter creates a rate limiter with the given defaults.
func NewRateLimiter(defaultRate float64, defaultBurst int) *RateLimiter

// Allow checks if one event is allowed for the tenant. Returns true if allowed.
// Thread-safe. Fast path uses RLock when limiter already exists.
func (rl *RateLimiter) Allow(tenantID string) bool

// WaitN blocks until n tokens are available or ctx is cancelled.
// For batch requests.
func (rl *RateLimiter) WaitN(ctx context.Context, tenantID string, n int) error

// SetTenantLimit overrides the rate and burst for a specific tenant.
func (rl *RateLimiter) SetTenantLimit(tenantID string, r float64, burst int)
```

### Gateway HTTP changes

- `POST /api/v1/push` -- existing endpoint gains two new error responses:
  - **429 Too Many Requests**: `{"error":"rate limit exceeded","tenant":"tenant-a"}` + `Retry-After: 1` header. Returned when `rateLimiter.Allow(tenantID)` returns false.
  - **503 Service Unavailable**: `{"error":"service unavailable"}`. Returned when Kafka produce times out or fails.

### Ingester backpressure (internal behavior, no new API surface)

- On flush failure after retries: `client.PauseFetchTopics("ingestion-events")`
- Background probe every 5s: test write to storage with a sentinel key
- On probe success: `client.ResumeFetchTopics("ingestion-events")`
- Logged via zap at `Warn` level for pause and `Info` level for resume

---

## Architecture / Component Boundaries (as relevant)

### Components I touch:

1. **`internal/tenant/ratelimit.go`** (NEW) -- standalone rate limiter, no external dependencies beyond `golang.org/x/time/rate`. Pure library code, testable in isolation.

2. **`internal/gateway/handler.go`** (MODIFY) -- add `RateLimiter` field to `Handler`, check `Allow()` after tenant extraction but before body parsing (fail fast). Add produce timeout context.

3. **`internal/ingester/consumer.go`** (MODIFY) -- add backpressure logic to `flushWorker`: on flush failure, pause consumption and start probe goroutine.

### Concurrency model:
- **Rate limiter:** `sync.RWMutex` with double-checked locking pattern. RLock for the fast path (limiter exists), upgrade to Lock only on first request for a new tenant. `rate.Limiter` itself is safe for concurrent use.
- **Backpressure:** atomic boolean (`paused`) guards pause/resume to prevent duplicate calls. Probe goroutine is spawned once and exits after resume.

### Config propagation:
- Rate limiter defaults come from `config.GatewayConfig` (new fields: `RateLimitRate`, `RateLimitBurst`).
- Per-tenant overrides are set via `SetTenantLimit()` (programmatic API for now; could be driven by config file or admin API in the future).

### Backpressure strategy:
```
Storage failure
  -> Flusher.FlushWithRetry exhausts retries
  -> flushWorker calls client.PauseFetchTopics
  -> Background probe: every 5s, store.Put(ctx, "__probe/health", []byte("ok"))
  -> On success: client.ResumeFetchTopics
  -> Meanwhile: Kafka consumer lag grows (observable via metrics)
  -> If lag causes Kafka produce backpressure / timeout -> gateway returns 503
```

---

## Correctness Invariants (must be explicit)

1. **Tenant isolation:** Rate limiting tenant A must not affect tenant B's throughput. Tested by running two tenants concurrently -- one at limit, one below.
2. **No race conditions:** Concurrent `Allow()` calls for the same tenant must not corrupt state. Tested with `-race` and concurrent goroutines.
3. **Burst behavior:** A fresh tenant can send up to `burst` events in a single burst before being rate limited. Tested by sending `burst+1` events immediately.
4. **At-least-once preserved:** Rate-limited requests are rejected BEFORE producing to Kafka, so no events are lost or duplicated by rate limiting.
5. **Backpressure pause/resume idempotency:** Multiple flush failures do not call PauseFetchTopics multiple times. Resume is only called after a pause.
6. **Produce timeout:** Gateway context deadline of 5s on Kafka produce ensures the handler does not hang indefinitely.

---

## Tests

### Unit tests:

**`internal/tenant/ratelimit_test.go`:**
- `TestAllow_BelowRate` -- allows events below the rate
- `TestAllow_ExceedsRate` -- denies events when rate exceeded
- `TestAllow_TenantIsolation` -- tenant A limited, tenant B unaffected
- `TestAllow_BurstBehavior` -- burst-size events allowed immediately, next one denied
- `TestAllow_Concurrent` -- 100 goroutines calling Allow for same tenant with `-race`
- `TestSetTenantLimit` -- override for specific tenant takes effect
- `TestWaitN` -- blocks until tokens available, respects context cancellation

**`internal/gateway/handler_test.go` (additions):**
- `TestHandlePush_RateLimited` -- mock rate limiter returns false, expect 429 + Retry-After header
- `TestHandlePush_ProduceTimeout` -- mock producer returns error, expect 503

**`internal/ingester/consumer_test.go` (additions):**
- `TestFlushWorker_BackpressurePause` -- mock flusher fails, verify PauseFetchTopics called
- `TestFlushWorker_BackpressureResume` -- mock storage recovers, verify ResumeFetchTopics called

### Integration tests:
- Covered by existing smoke test (`internal/pipeline/smoke_test.go`) -- rate limiting and backpressure will be exercised by the load-test-engineer and chaos-engineer in later phases.

### Property/fuzz tests:
- N/A for this scope.

### Failure injection tests:
- Covered by chaos-engineer role in Quality phase.

### Exact commands:
```bash
go test ./internal/tenant/... -v -race -count=1
go test ./internal/gateway/... -v -race -count=1
go test ./internal/ingester/... -v -race -count=1
go test ./... -race -count=1
go vet ./...
```

---

## Benchmarks + "Success"

N/A -- the rate limiter is a thin wrapper around `golang.org/x/time/rate` which is already well-benchmarked in the Go standard library. The benchmark-engineer role will benchmark the full gateway hot path including rate limiting in the Performance phase.

That said, the rate limiter's RLock fast path should ensure it adds negligible overhead (<100ns per call) to the gateway request path.

---

# Please produce (no code yet):

## 1) Recommended API surface

### `internal/tenant/ratelimit.go`
| Function | Behavior |
|---|---|
| `NewRateLimiter(defaultRate float64, defaultBurst int) *RateLimiter` | Creates limiter with defaults. Panics if rate <= 0 or burst <= 0. |
| `Allow(tenantID string) bool` | RLock fast path -> Lock slow path (create limiter) -> `limiter.Allow()` |
| `WaitN(ctx context.Context, tenantID string, n int) error` | Same lookup pattern, calls `limiter.WaitN(ctx, n)` |
| `SetTenantLimit(tenantID string, r float64, burst int)` | Write-locks, creates or replaces limiter with custom params |

### Gateway handler changes
| Change | Behavior |
|---|---|
| Add `rateLimiter *tenant.RateLimiter` field to `Handler` | Optional -- if nil, rate limiting is skipped (backwards compat) |
| Check `Allow()` after tenant extraction | If false, return 429 + `Retry-After: 1` |
| Add 5s context deadline on `ProduceToPartition` | If deadline exceeded, return 503 |

### Ingester consumer changes
| Change | Behavior |
|---|---|
| Add `paused atomic.Bool` to `Consumer` | Guards idempotent pause/resume |
| On flush failure in `flushWorker` | If `!paused.Load()`: set paused, call `PauseFetchTopics`, spawn probe goroutine |
| Probe goroutine | Every 5s: `store.Put(ctx, "__probe/health", []byte("ok"))` then `store.Delete(ctx, "__probe/health")`. On success: `ResumeFetchTopics`, clear paused, exit. |

## 2) Folder structure

```
internal/
  tenant/
    tenant.go           -- existing (unchanged)
    tenant_test.go      -- existing (unchanged)
    ratelimit.go        -- NEW: RateLimiter struct + methods
    ratelimit_test.go   -- NEW: comprehensive unit tests
  gateway/
    handler.go          -- MODIFY: add rate limiter + produce timeout
    handler_test.go     -- MODIFY: add 429 and 503 test cases
    producer.go         -- unchanged
  ingester/
    consumer.go         -- MODIFY: add backpressure pause/resume logic
    consumer_test.go    -- MODIFY: add backpressure tests
    batcher.go          -- unchanged
    flusher.go          -- unchanged
  config/
    config.go           -- MODIFY: add RateLimitRate + RateLimitBurst to GatewayConfig
```

## 3) Step-by-step task plan (see Tighten section below)

## 4) Benchmark plan
N/A -- rate limiter wraps stdlib `rate.Limiter`. Full gateway benchmarks owned by benchmark-engineer.

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Rate Limiter Core
- **Outcome:** `RateLimiter` struct with `Allow`, `WaitN`, `SetTenantLimit` methods, fully tested.
- **Files to create/modify:**
  - CREATE `internal/tenant/ratelimit.go`
  - CREATE `internal/tenant/ratelimit_test.go`
- **Exact verification command(s):**
  ```bash
  go test ./internal/tenant/... -v -race -count=1
  go vet ./internal/tenant/...
  ```
- **Suggested commit message:** `feat: add per-tenant token bucket rate limiter`

### Task 2: Add `golang.org/x/time/rate` dependency + config fields
- **Outcome:** `go.mod` includes `golang.org/x/time/rate`; `GatewayConfig` has `RateLimitRate` and `RateLimitBurst` fields with defaults (1000, 5000).
- **Files to create/modify:**
  - MODIFY `go.mod` / `go.sum` (via `go get golang.org/x/time`)
  - MODIFY `internal/config/config.go` -- add two fields + env var loading + validation
- **Exact verification command(s):**
  ```bash
  go build ./...
  go test ./internal/config/... -v -race -count=1
  ```
- **Suggested commit message:** `feat: add rate limit config fields and x/time/rate dependency`

### Task 3: Gateway rate limiting (429 response)
- **Outcome:** `Handler` accepts optional `*tenant.RateLimiter`; calls `Allow()` before producing; returns 429 with JSON body and `Retry-After` header when denied.
- **Files to create/modify:**
  - MODIFY `internal/gateway/handler.go` -- add `rateLimiter` field, check in `handlePush`
  - MODIFY `internal/gateway/handler_test.go` -- add `TestHandlePush_RateLimited`
- **Exact verification command(s):**
  ```bash
  go test ./internal/gateway/... -v -race -count=1
  go vet ./internal/gateway/...
  ```
- **Suggested commit message:** `feat: integrate per-tenant rate limiting into gateway (HTTP 429)`

### Task 4: Gateway produce timeout (503 response)
- **Outcome:** `handlePush` wraps the Kafka produce call with a 5s context deadline. On timeout or produce error, returns 503 with JSON body.
- **Files to create/modify:**
  - MODIFY `internal/gateway/handler.go` -- add `context.WithTimeout` around `ProduceToPartition`
  - MODIFY `internal/gateway/handler_test.go` -- add `TestHandlePush_ProduceTimeout`
- **Exact verification command(s):**
  ```bash
  go test ./internal/gateway/... -v -race -count=1
  ```
- **Suggested commit message:** `feat: add produce timeout and HTTP 503 backpressure to gateway`

### Task 5: Ingester backpressure (pause/resume Kafka consumption)
- **Outcome:** When `flushWorker` exhausts retries, it pauses Kafka consumption via `PauseFetchTopics`. A background probe tests storage every 5s and calls `ResumeFetchTopics` when storage recovers.
- **Files to create/modify:**
  - MODIFY `internal/ingester/consumer.go` -- add `paused atomic.Bool`, `storageProbe` method, update `flushWorker`
  - MODIFY `internal/ingester/consumer_test.go` -- add backpressure unit tests
- **Exact verification command(s):**
  ```bash
  go test ./internal/ingester/... -v -race -count=1
  go vet ./internal/ingester/...
  ```
- **Suggested commit message:** `feat: add backpressure pause/resume on storage failure in ingester`

### Task 6: Wire everything together + full test pass
- **Outcome:** `cmd/gateway.go` creates `RateLimiter` from config and passes it to `Handler`. `cmd/ingester.go` passes the `ObjectStore` reference to `Consumer` for the probe. All tests pass.
- **Files to create/modify:**
  - MODIFY `cmd/gateway.go` -- create and inject `RateLimiter`
  - MODIFY `cmd/ingester.go` -- pass `ObjectStore` to `Consumer` for probe
- **Exact verification command(s):**
  ```bash
  go build ./...
  go test ./... -v -race -count=1
  go vet ./...
  ```
- **Suggested commit message:** `feat: wire rate limiter and backpressure into CLI commands`

---

### Top 3 Risks/Unknowns

1. **`franz-go` PauseFetchTopics/ResumeFetchTopics API correctness** -- Need to verify these methods exist on the `*kgo.Client` and behave as expected (pause consumption for specific topics without affecting others).
   - **Validation (<15 min):** Read franz-go godoc for `PauseFetchTopics` and `ResumeFetchTopics`. Write a minimal test that creates a client and calls both methods to confirm they compile and don't panic.

2. **RLock-to-Lock upgrade pattern (double-checked locking)** -- The standard Go RWMutex does not support lock upgrade. We must release RLock, acquire Lock, and re-check.
   - **Validation (<15 min):** Write the pattern and run `go test -race` with 100 concurrent goroutines. The double-check pattern is well-established in Go and will work correctly.

3. **Storage probe sentinel key conflicts** -- The probe writes `__probe/health` to the same ObjectStore as real data. If the probe key format collides with real batch keys, it could cause issues.
   - **Validation (<15 min):** Verify that `storage.BatchKey()` always generates keys starting with `<tenantID>/` which will never match `__probe/`. The `__` prefix convention ensures no collision.

---

## CLAUDE.md contributions (do NOT write the file; propose content)

### From Tenancy Engineer
**Coding style rules:**
- Rate limiter uses `sync.RWMutex` with double-checked locking. Do not simplify to a plain `sync.Mutex` -- the RLock fast path is critical for hot-path performance.
- Backpressure uses `sync/atomic.Bool` to guard idempotent pause/resume. Do not replace with a mutex-guarded bool.
- All new code must pass `go test -race`.

**Dev commands:**
```bash
# Run tenant package tests (rate limiter)
go test ./internal/tenant/... -v -race -count=1

# Run all tests including gateway + ingester changes
go test ./... -v -race -count=1

# Quick smoke test: rate limiting
curl -X POST localhost:8080/api/v1/push \
  -H "X-Scope-OrgID: tenant-a" \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"service":"api"},"value":42.0}]'
# Repeat rapidly to trigger 429
```

**Before you commit checklist:**
- [ ] `go test ./... -race` passes
- [ ] `go vet ./...` passes
- [ ] Rate limiter tests cover: boundary, isolation, burst, concurrency, overrides
- [ ] Gateway returns 429 with `Retry-After: 1` header (not just JSON body)
- [ ] Backpressure probe cleans up its sentinel key after successful health check
- [ ] No import cycles between `tenant` and `gateway` packages

**Guardrails:**
- Do not add distributed rate limiting (Redis, etc.) in this phase -- in-process only
- Do not change the `ObjectStore` interface -- backpressure probe uses the existing `Put`/`Delete` methods
- Rate limiter must remain optional in the Handler (nil check) to avoid breaking existing tests

---

## EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

### Flow / Architecture explanation
- Per-tenant rate limiting sits in the gateway between tenant ID extraction and Kafka produce. It uses a token bucket algorithm (`golang.org/x/time/rate`) with lazy per-tenant initialization.
- Backpressure propagates from storage failures through the ingester (pause Kafka consumption) to the gateway (Kafka produce timeout -> 503).

### Key engineering decisions + tradeoffs
- **In-process rate limiting vs. distributed:** Chose in-process for simplicity. This means each gateway instance has independent limits -- total cluster rate is `N * limit`. Acceptable for single-instance MVP.
- **RWMutex with double-checked locking:** Optimizes for the common case (limiter already exists) at the cost of slightly more complex code on the create path.
- **Backpressure via Kafka lag:** No explicit signal from ingester to gateway. Instead, storage failures cause consumer lag, which naturally slows produces when Kafka buffers fill. This is the same pattern Grafana uses.
- **Probe-based resume:** Polling storage every 5s is simple and reliable. Alternative (exponential backoff) risks long recovery times. 5s is a reasonable balance.

### Limits of MVP + next steps
- Rate limits are not persisted -- process restart resets all limiters (but they re-create lazily, so this is transparent).
- No admin API for per-tenant overrides at runtime (would need an HTTP endpoint or config reload).
- No distributed rate limiting across multiple gateway instances.
- Backpressure probe writes a sentinel key -- in production, a lightweight `HeadBucket` or `ListObjects(limit=1)` would be less intrusive.

### How to run locally + how to validate
- Start Kafka: `docker compose up -d`
- Start gateway with rate limiting: `go run . gateway --port 8080`
- Test rate limiting: send >1000 events/sec for a single tenant, observe 429 responses
- Test backpressure: stop or fill the storage backend, observe ingester pausing and gateway 503s

---

## READY FOR APPROVAL
