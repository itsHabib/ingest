# Master Plan: Integration Test Engineer

## You are in PLAN MODE.

### Project
I want to build a **comprehensive end-to-end integration test suite** for the ingestion engine.

**Goal:** build a **test harness and integration tests** in which we **verify the full pipeline (Gateway -> Kafka -> Ingester -> ObjectStore) works correctly under realistic conditions with real infrastructure**.

### Role + Scope
- **Role:** Integration Test Engineer
- **Scope:** End-to-end integration tests that exercise the full pipeline with real Kafka and filesystem storage. Owns `internal/testing/harness.go`, `e2e_test.go`, and `scripts/e2e.sh`. Does NOT own unit tests, chaos/failure tests, benchmarks, or any production code modifications.
- **File you will write:** `/docs/quality/plans/integration-test-engineer.md`
- **No-touch zones:** do not edit any production code; do not write unit tests or chaos tests.

---

## Functional Requirements
- FR1: `TestHarness` manages full gateway + ingester lifecycle with random free ports, temp storage dirs, and configurable options (batch size, batch age, flush workers, rate limit).
- FR2: Single-tenant end-to-end test: push 100 events, verify all 100 appear in storage with correct tenant ID (at-least-once: count >= 100).
- FR3: Multi-tenant end-to-end test: push events for 3 tenants, verify tenant isolation -- no cross-tenant data leakage.
- FR4: Rate limiting end-to-end test: configure low rate limit, verify 429 responses for one tenant while another tenant is unaffected.
- FR5: Large volume test: push 10,000 events across 5 tenants, verify total event count matches.
- FR6: Graceful shutdown test: push events, trigger shutdown, verify all pre-shutdown events are flushed.
- Tests required: integration tests (build tag `integration`) requiring Docker (Kafka).
- Metrics required: N/A -- metrics are tested by observability-engineer's unit tests; integration tests focus on data correctness.

## Non-Functional Requirements
- Language/runtime: Go 1.22+
- Local dev: `docker compose up -d` brings up Kafka (required for integration tests)
- Observability: Tests verify data correctness, not metrics endpoints
- Safety: Tests use `t.TempDir()` for storage isolation; random free ports avoid collisions; cleanup via `t.Cleanup`
- Documentation: CLAUDE.md + EXPLAIN.md contributions (proposed below)
- Performance: N/A for integration tests (benchmarks are a separate role)

---

## Assumptions / System Model
- Deployment environment: Tests run against local Docker Compose (Kafka KRaft on port 9092)
- Failure modes: Not in scope (chaos-engineer's domain). Integration tests assume infrastructure is healthy.
- Delivery guarantees: At-least-once -- tests verify `count >= expected`, never `count == expected` (duplicates are acceptable)
- Multi-tenancy: Fully tested -- tenant isolation is a key invariant

---

## Data Model (as relevant to your role)

The harness works with existing types:
- `event.Event` -- pushed via HTTP to gateway, serialized to JSON
- `event.Batch` -- flushed to storage by ingester, deserialized from storage files for verification
- Storage key format: `<tenantID>/<YYYY-MM-DD>/<partition>-<offset>.json`

Verification helper types (internal to test code):
- `StorageReader` -- walks the temp storage dir, deserializes all batches, groups events by tenant
- `EventCounter` -- aggregates event counts per tenant for at-least-once assertions

---

## APIs (as relevant to your role)

### TestHarness API

```go
// internal/testing/harness.go
type TestHarness struct { /* ... */ }

type HarnessOption func(*harnessConfig)

func WithBatchSize(n int) HarnessOption
func WithBatchAge(d time.Duration) HarnessOption
func WithFlushWorkers(n int) HarnessOption
func WithRateLimit(rate float64, burst int) HarnessOption

func NewTestHarness(t *testing.T, opts ...HarnessOption) *TestHarness
func (h *TestHarness) GatewayURL() string       // e.g. "http://localhost:54321"
func (h *TestHarness) StorageDir() string        // temp dir for filesystem store
func (h *TestHarness) Close()                    // graceful shutdown of gateway + ingester
```

### HTTP API under test

- `POST /api/v1/push` with `X-Scope-OrgID` header -- pushes JSON array of events
- `GET /ready` -- health check (harness polls this during startup)

### Storage verification

- Walk `StorageDir()/<tenantID>/` recursively
- Each `.json` file is a serialized `event.Batch`
- Deserialize and count events per tenant

---

## Architecture / Component Boundaries

```
TestHarness
  |-- starts Gateway (HTTP server + Kafka producer) on random port
  |-- starts Ingester (Kafka consumer + batcher + flusher) with filesystem store in t.TempDir()
  |-- both connect to real Kafka on localhost:9092
  |-- test code pushes events via HTTP to GatewayURL()
  |-- test code reads flushed batches from StorageDir()
  |-- Close() sends shutdown signal, waits for drain
```

Components the harness wires up (from existing code):
1. `ring.NewRing` -- hash ring for partition routing
2. `gateway.NewKafkaProducer` -- real Kafka producer
3. `gateway.NewHandler` -- HTTP handler with optional rate limiter
4. `storage.NewFilesystemStore` -- filesystem storage in temp dir
5. `ingester.NewFlusher` -- writes batches to storage
6. `ingester.NewBatcher` -- accumulates events, flushes on size/age
7. `ingester.NewConsumer` -- Kafka consumer group, drives batcher + flusher

The harness creates and starts these components in-process (no subprocess spawning). It mirrors the wiring in `cmd/gateway.go` and `cmd/ingester.go` but with test-friendly config (random ports, small batches, short timeouts).

---

## Correctness Invariants (must be explicit)

1. **At-least-once delivery**: Every event pushed to the gateway MUST appear in storage. Count >= expected. Duplicates are acceptable.
2. **Tenant isolation**: Tenant-A's events MUST NOT appear in Tenant-B's storage path. Each batch's `tenant_id` field MUST match the directory it's stored under.
3. **Rate limiting correctness**: When rate limit is exceeded, gateway returns 429. Other tenants remain unaffected (return 200).
4. **Graceful shutdown completeness**: All events produced before shutdown signal MUST be flushed to storage before the process exits.
5. **Event integrity**: Deserialized events from storage MUST match the events that were pushed (labels, value, tenant_id).

---

## Tests

### Integration tests (`//go:build integration`)

All in `e2e_test.go` (root of project or `internal/testing/`):

1. **TestEndToEnd_SingleTenant** -- push 100 events for tenant-a, wait for flush, verify all 100 present with correct tenant_id
2. **TestEndToEnd_MultiTenant** -- push 100 events each for tenant-a/b/c, verify each tenant's events are isolated
3. **TestEndToEnd_RateLimit** -- configure 10 events/sec rate limit, push 50 rapidly, verify some 429s; push for another tenant simultaneously, verify all 200
4. **TestEndToEnd_LargeVolume** -- push 10,000 events across 5 tenants, verify total count
5. **TestEndToEnd_GracefulShutdown** -- push events, trigger shutdown, verify all events flushed

### Test helpers (`internal/testing/harness.go`)
- `NewTestHarness` -- lifecycle management
- `waitForReady(url string, timeout time.Duration)` -- polls /ready until 200
- `pushEvents(gatewayURL, tenantID string, events []event.Event) (*http.Response, error)` -- HTTP push helper
- `readAllEvents(storageDir string) (map[string][]event.Event, error)` -- walks storage, deserializes batches, groups by tenant

### Commands
- `go test -tags integration -v -count=1 -timeout 120s ./...` -- run all integration tests
- `go test -tags integration -v -run TestEndToEnd_SingleTenant ./...` -- run a single test
- `./scripts/e2e.sh` -- full lifecycle: docker compose up, run tests, docker compose down

### Property/fuzz tests
N/A -- not in scope for integration tests (unit-test-engineer handles fuzz tests).

### Failure injection tests
N/A -- chaos-engineer's domain.

---

## Benchmarks + "Success"

N/A -- benchmarks are the benchmark-engineer's scope. Integration tests measure correctness, not performance.

Success criteria for integration tests:
- All 5 tests pass with `go test -tags integration -v -count=1 -timeout 120s ./...`
- Tests are reliable (no flakes) -- use polling with timeout, not fixed sleeps
- Tests complete within 60 seconds total (with small batch sizes: 10 events, 2s max age)
- Tests are fully isolated -- each test uses its own harness with its own temp dir and consumer group

---

# Recommended API surface

1. **`NewTestHarness(t *testing.T, opts ...HarnessOption) *TestHarness`** -- creates and starts gateway + ingester with random ports and temp storage. Registers `t.Cleanup(h.Close)`. Blocks until both `/ready` endpoints respond.

2. **`pushEvents(gatewayURL, tenantID string, events []event.Event) (*http.Response, error)`** -- marshals events to JSON, POSTs to `/api/v1/push` with `X-Scope-OrgID` header. Returns raw response for status code checks.

3. **`readAllEvents(storageDir string) (map[string][]event.Event, error)`** -- walks storage directory tree, reads all `.json` files, unmarshals `event.Batch`, returns events grouped by tenant ID.

4. **`waitForFlush(storageDir, tenantID string, minEvents int, timeout time.Duration) error`** -- polls storage until at least `minEvents` are found for the given tenant, or timeout expires.

5. **`generateEvents(n int, tenantID string) []event.Event`** -- creates N test events with unique labels and sequential values.

---

# Folder structure

```
ingest/
  internal/
    testing/
      harness.go          -- TestHarness + HarnessOption + helpers (NEW)
  e2e_test.go             -- integration tests with //go:build integration (NEW)
  scripts/
    e2e.sh                -- docker lifecycle + test runner (NEW)
```

Ownership:
- `internal/testing/harness.go` -- integration-test-engineer (shared with chaos-engineer who may extend it)
- `e2e_test.go` -- integration-test-engineer
- `scripts/e2e.sh` -- integration-test-engineer

---

# Step-by-step task plan

## Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Build TestHarness with lifecycle management
- **Outcome**: `TestHarness` starts gateway + ingester in-process with random free ports, temp storage, and configurable options. `Close()` performs graceful shutdown.
- **Files to create/modify**:
  - `internal/testing/harness.go` (create)
- **Exact verification command(s)**:
  - `go build ./internal/testing/...` (compiles without error)
  - `go vet ./internal/testing/...`
- **Suggested commit message**: `test: add TestHarness for integration test lifecycle management`

### Task 2: Add test helper functions (push, read, wait, generate)
- **Outcome**: Helper functions for pushing events via HTTP, reading flushed batches from storage, polling for flush completion, and generating test events.
- **Files to create/modify**:
  - `internal/testing/harness.go` (extend with helpers)
- **Exact verification command(s)**:
  - `go build ./internal/testing/...`
  - `go vet ./internal/testing/...`
- **Suggested commit message**: `test: add integration test helper functions for push, read, wait`

### Task 3: Write single-tenant and multi-tenant end-to-end tests
- **Outcome**: `TestEndToEnd_SingleTenant` and `TestEndToEnd_MultiTenant` pass against real Kafka.
- **Files to create/modify**:
  - `e2e_test.go` (create, build tag `integration`)
- **Exact verification command(s)**:
  - `docker compose up -d && sleep 15` (Kafka startup)
  - `go test -tags integration -v -count=1 -run "TestEndToEnd_SingleTenant|TestEndToEnd_MultiTenant" -timeout 120s ./...`
- **Suggested commit message**: `test: add single-tenant and multi-tenant end-to-end integration tests`

### Task 4: Write rate limit and large volume end-to-end tests
- **Outcome**: `TestEndToEnd_RateLimit` and `TestEndToEnd_LargeVolume` pass against real Kafka.
- **Files to create/modify**:
  - `e2e_test.go` (extend)
- **Exact verification command(s)**:
  - `go test -tags integration -v -count=1 -run "TestEndToEnd_RateLimit|TestEndToEnd_LargeVolume" -timeout 120s ./...`
- **Suggested commit message**: `test: add rate limit and large volume end-to-end integration tests`

### Task 5: Write graceful shutdown end-to-end test
- **Outcome**: `TestEndToEnd_GracefulShutdown` verifies no data loss on clean shutdown.
- **Files to create/modify**:
  - `e2e_test.go` (extend)
- **Exact verification command(s)**:
  - `go test -tags integration -v -count=1 -run TestEndToEnd_GracefulShutdown -timeout 120s ./...`
- **Suggested commit message**: `test: add graceful shutdown end-to-end integration test`

### Task 6: Write e2e.sh script for CI/local automation
- **Outcome**: `scripts/e2e.sh` handles full lifecycle: docker compose up, wait for Kafka health, run integration tests, docker compose down. Propagates exit code.
- **Files to create/modify**:
  - `scripts/e2e.sh` (create)
- **Exact verification command(s)**:
  - `bash scripts/e2e.sh` (runs full suite, exits 0 on success)
  - `shellcheck scripts/e2e.sh` (if available)
- **Suggested commit message**: `test: add e2e.sh script for integration test automation`

---

### Top 3 risks/unknowns and how to validate each in <15 minutes

1. **Risk: Kafka consumer group rebalancing delays cause flaky tests**
   - Each test needs a unique consumer group to avoid interference between tests running in parallel or sequentially.
   - **Validation**: In `NewTestHarness`, generate a unique consumer group name using `t.Name()` + random suffix. Run two tests back-to-back and verify no interference. (<5 min)

2. **Risk: Polling for flush completion is unreliable (timing-dependent)**
   - If batch age or size thresholds are too large, tests wait too long. If too small, the ingester thrashes.
   - **Validation**: Default harness config uses batch size=10, batch age=2s. Write a quick test that pushes 10 events and polls with 30s timeout. Measure actual flush latency. Adjust thresholds. (<10 min)

3. **Risk: Port conflicts when multiple tests run concurrently**
   - If tests use `t.Parallel()`, two harnesses might grab the same port.
   - **Validation**: Use `net.Listen(":0")` to get OS-assigned free ports, then close the listener and pass the port to the server. Verify by running 3 tests in parallel. (<5 min)

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From Integration Test Engineer
- **Coding style rules**:
  - Integration tests use build tag `//go:build integration` -- never run without Docker
  - Each integration test creates its own `TestHarness` with unique consumer group (no shared state between tests)
  - Use polling with timeout for async assertions, never `time.Sleep`
  - Use `t.TempDir()` for all storage paths -- automatic cleanup
  - Helper functions in `internal/testing/harness.go` are shared with chaos tests
- **Dev commands**:
  - `docker compose up -d` -- start Kafka (required for integration tests)
  - `go test -tags integration -v -count=1 -timeout 120s ./...` -- run integration tests
  - `./scripts/e2e.sh` -- full lifecycle (start Docker, run tests, stop Docker)
- **Before you commit checklist**:
  - [ ] `go vet ./...` passes
  - [ ] `go test ./...` passes (unit tests, no build tags)
  - [ ] `go test -tags integration -count=1 -timeout 120s ./...` passes (with Docker running)
  - [ ] No `time.Sleep` in test assertions (use polling)
- **Guardrails**:
  - Never run integration tests in CI without Docker services
  - Always use `-count=1` to disable test caching (stateful tests)
  - Set `-timeout 120s` to prevent hung tests from blocking CI

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

- **Flow / architecture explanation**:
  - Integration tests exercise the full pipeline in-process: HTTP push -> Kafka -> consumer -> batcher -> flusher -> filesystem storage
  - `TestHarness` mirrors production wiring (`cmd/gateway.go` + `cmd/ingester.go`) but with test-friendly config
  - Tests verify data by reading flushed batch files from the filesystem store

- **Key engineering decisions + tradeoffs**:
  - In-process harness vs. subprocess: chose in-process for speed and deterministic shutdown, tradeoff is tighter coupling to internal APIs
  - Unique consumer group per test: avoids Kafka rebalancing interference, tradeoff is slightly slower startup (new group each time)
  - Polling with timeout instead of channels/callbacks: simpler, works with filesystem storage, tradeoff is slower feedback (up to polling interval)
  - At-least-once assertion (`count >= expected`): matches system guarantees, avoids flakes from Kafka replay

- **Limits of MVP + next steps**:
  - Tests use filesystem storage only (no S3/MinIO integration tests yet)
  - No concurrent test execution (`t.Parallel()` is safe but not yet enabled due to consumer group coordination)
  - Chaos/failure scenarios are in a separate test suite (chaos-engineer)
  - Future: add S3 integration tests with MinIO, add metrics verification

- **How to run locally + how to validate**:
  - `docker compose up -d` to start Kafka
  - `go test -tags integration -v -count=1 -timeout 120s ./...`
  - Or use `./scripts/e2e.sh` for full lifecycle
  - Validate: all 5 tests pass, no flakes over 3 consecutive runs

---

## READY FOR APPROVAL
