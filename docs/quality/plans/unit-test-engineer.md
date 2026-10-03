# Unit Test Engineer Plan

## You are in PLAN MODE.

### Project
I want to build a **comprehensive unit test suite** for the ingestion engine.

**Goal:** build a **thorough set of unit tests, gomock-generated mocks, and a hand-rolled FakeObjectStore** in which we **verify every package works correctly in isolation, catch regressions early, and provide reusable test infrastructure for integration and chaos tests**.

### Role + Scope
- **Role:** Unit Test Engineer
- **Scope:** All unit tests (no external dependencies), shared test infrastructure in `internal/testing/` (hand-rolled FakeObjectStore + gomock-generated mocks). I own test correctness for: gateway handler, hash ring, batcher, flusher, rate limiter, filesystem storage. I do NOT own integration tests, chaos tests, benchmarks, or Docker infrastructure.
- **File I will write:** `/docs/quality/plans/unit-test-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: Hand-rolled `FakeObjectStore` with in-memory state, runtime-togglable error/delay injection (needed by chaos tests), and call tracking. Gomock-generated `MockProducer` for the `gateway.Producer` interface (simple call capture + configurable returns -- gomock handles this perfectly). Add `github.com/uber-go/mock` as a dependency.
- FR2: Expand existing unit tests for all packages to cover edge cases, error paths, concurrency safety, and validation rules.
- FR3: Add fuzz tests for the hash ring to verify no panics on arbitrary input.
- FR4: Add a batcher concurrent-Add stress test to exercise the race detector.
- FR5: Add flusher batch-deserialization round-trip test to verify data integrity end-to-end through serialization.
- FR6: Add filesystem store tests for deeply nested paths, special characters, and concurrent writes to the same key.
- FR7: Add rate limiter many-tenant test to verify memory behavior under tenant churn.
- Tests required: all unit tests, run with `go test -race ./...`
- Metrics required: N/A for unit tests (metrics are tested in `observability/metrics_test.go`)

## Non-Functional Requirements
- Language/runtime: Go
- Local dev: `go test -race -count=1 ./...` (no Docker needed)
- Observability: N/A for unit tests
- Safety: All tests use `t.TempDir()` for filesystem isolation; no shared mutable state across tests
- Mocking: Use `github.com/uber-go/mock/mockgen` (gomock) for simple interface stubs; hand-roll fakes only when runtime fault toggling is needed (FakeObjectStore for chaos tests)
- Documentation: CLAUDE.md contributions for test conventions
- Performance: Existing benchmarks already cover hot paths; fuzz test runs for a bounded duration

---

## Assumptions / System Model
- Deployment environment: local test runner, CI (no Docker required for unit tests)
- Failure modes: storage errors, producer errors, context cancellation, malformed input, concurrent access
- Delivery guarantees: unit tests verify individual component contracts, not end-to-end guarantees
- Multi-tenancy: verified via tenant isolation tests in batcher, handler, and filesystem store

---

## Data Model (as relevant to role)

Key types under test:

- **Event** (`internal/event/event.go`)
  - Fields: TenantID, Timestamp, Labels (map[string]string), Value (float64), Payload ([]byte optional)
  - Validation: TenantID non-empty, Timestamp non-zero
  - Serialization: JSON round-trip

- **Batch** (`internal/event/event.go`)
  - Fields: TenantID, Partition, Events []Event, CreatedAt
  - Key format: `<tenantID>/<YYYY-MM-DD>/<partition>-<offset padded 10>.json`

- **ObjectStore** interface (`internal/storage/storage.go`)
  - Methods: Put, Get, List, Delete, Exists
  - Contract: concurrent-safe, Put overwrites, Delete is idempotent, List by prefix

- **Producer** interface (`internal/gateway/producer.go`)
  - Methods: Produce, ProduceAsync, ProduceToPartition, Close

Versioning: N/A (no config changes in test scope).
Persistence: in-memory fakes only.

---

## APIs (as relevant to role)

### FakeObjectStore API (hand-rolled -- runtime fault toggling needed by chaos tests)
- `NewFakeObjectStore() *FakeObjectStore` -- creates empty in-memory store
- `SetError(err error)` -- all subsequent ops return this error (togglable at runtime mid-test)
- `ClearError()` -- resume normal operation
- `SetDelay(d time.Duration)` -- add latency to every op (for chaos slow-storage tests)
- `PutCalls() int` -- returns total Put invocations
- `GetCalls() int` -- returns total Get invocations
- All `storage.ObjectStore` methods (Put, Get, List, Delete, Exists)
- Thread-safe with `sync.RWMutex`

Error semantics: SetError makes every call return the configured error; ClearError resets to nil. SetDelay adds a `time.Sleep` before each op.

**Why hand-rolled:** Chaos tests need to toggle errors/delays at runtime while the system is running. gomock expectations are set upfront and cannot handle this pattern cleanly.

### MockProducer (gomock-generated)
- Generated from `gateway.Producer` interface via `mockgen`
- Generation command: `mockgen -source=internal/gateway/producer.go -destination=internal/testing/mock_producer.go -package=testing Producer`
- Usage: `ctrl := gomock.NewController(t); mp := mock_testing.NewMockProducer(ctrl)`
- Test expectations via standard gomock API: `mp.EXPECT().ProduceToPartition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)`
- Argument capture via `gomock.Do()` when tests need to inspect produced messages

**Why gomock:** The Producer mock just captures calls and returns configurable errors. gomock handles this perfectly with `EXPECT().Return()` and argument captors. No need to hand-roll.

---

## Architecture / Component Boundaries (as relevant)

Components I test (each in its own package):

- **`internal/testing/`** (NEW): Shared test doubles used by unit, integration, and chaos tests.
  - `fakes.go`: FakeObjectStore (hand-rolled -- runtime fault toggling for chaos tests)
  - `fakes_test.go`: tests for FakeObjectStore
  - `mock_producer.go`: gomock-generated MockProducer from `gateway.Producer` interface
  - Dependencies: `github.com/uber-go/mock` (gomock)
  - FakeObjectStore is thread-safe with `sync.RWMutex`

- **`internal/gateway/handler_test.go`** (EXPAND): Tests for the HTTP push handler.
  - Currently has: basic success, missing tenant, invalid JSON, empty events, kafka error, rate limiting, body too large, tenant ID overwrite, timestamp default, ring partition routing, content-type, produce timeout
  - Currently uses: ad-hoc `mockProducer` struct (hand-rolled in test file)
  - Migration: replace ad-hoc `mockProducer` with gomock `MockProducer` from `internal/testing/mock_producer.go`
  - To add: event validation failure (missing labels after tenant stamp), large batch (100+ events)
  - **Uses: gomock MockProducer**

- **`internal/ring/hashring_test.go`** (EXPAND): Tests for the consistent hash ring.
  - Currently has: determinism, distribution, minimal disruption, wrap-around, empty ring, tenant routing, token uniqueness, stddev, benchmarks
  - To add: fuzz test (`FuzzGetPartition`), table-driven partition assignment with known expected values

- **`internal/ingester/batcher_test.go`** (EXPAND): Tests for the batch accumulator.
  - Currently has: size-triggered, time-triggered, FlushAll, multi-tenant isolation, backpressure, benchmark
  - To add: concurrent Add from multiple goroutines (race detector stress test)

- **`internal/ingester/flusher_test.go`** (EXPAND): Tests for the storage flusher.
  - Currently has: success, retry, retry exhausted, correct storage key, benchmark
  - Currently uses: ad-hoc `mockStore` struct (hand-rolled in test file)
  - Migration: replace ad-hoc `mockStore` with shared `FakeObjectStore` from `internal/testing/fakes.go`
  - To add: batch deserialization round-trip (verify events survive marshal/unmarshal through flush), context cancellation during retry
  - **Uses: hand-rolled FakeObjectStore** (needed for runtime SetError toggling in retry tests)

- **`internal/tenant/ratelimit_test.go`** (EXPAND): Tests for per-tenant rate limiting.
  - Currently has: below rate, exceeds rate, tenant isolation, burst behavior, concurrent, set tenant limit, WaitN, panic on invalid
  - To add: many-tenant churn test (create 1000 tenants, verify no panic/leak pattern)

- **`internal/storage/filesystem_test.go`** (EXPAND): Tests for filesystem object store.
  - Currently has: put/get round-trip, get missing, list prefix, list empty, delete, delete missing, exists, tenant isolation, concurrent writes, batch key, empty key, benchmarks
  - To add: deeply nested paths, special characters in key, concurrent Put to same key (verify no corruption), list ordering consistency

Concurrency model: FakeObjectStore uses `sync.RWMutex` for thread safety. gomock-generated MockProducer is not inherently thread-safe (gomock controllers are per-test). All tests that touch concurrency run with `-race`.

---

## Correctness Invariants (must be explicit)
1. FakeObjectStore faithfully implements the ObjectStore interface contract (Put overwrites, Delete is idempotent, List returns matching prefixes, Get returns error for missing keys). Compile-time interface check enforced.
2. gomock MockProducer satisfies the `gateway.Producer` interface (enforced by code generation). Expectations are verified at end of each test via `ctrl.Finish()` (automatic with `gomock.NewController(t)`).
3. Gateway handler always sets TenantID from the header, never from the request body.
4. Gateway handler returns 400 for all invalid inputs before touching the producer.
5. Hash ring returns a partition in [0, N) for any input string, never panics.
6. Batcher routes events for different tenant:partition keys into separate batches.
7. Batcher size-triggered flush fires exactly when batch reaches maxSize.
8. Flusher writes data that can be deserialized back to the original batch.
9. Flusher retry exhaustion returns an error wrapping the last failure.
10. Rate limiter for tenant-a does not affect tenant-b.
11. FilesystemStore Put is atomic (no partial files visible).
12. All tests pass with `-race` flag enabled.

---

## Tests

Specific tests to add/expand:

### `internal/testing/fakes.go` (NEW -- hand-rolled FakeObjectStore)
- Unit tests in `internal/testing/fakes_test.go`:
  - `TestFakeObjectStore_PutGet` -- basic round-trip
  - `TestFakeObjectStore_SetError` -- all ops fail after SetError
  - `TestFakeObjectStore_ClearError` -- ops resume after ClearError
  - `TestFakeObjectStore_SetDelay` -- ops sleep for configured duration
  - `TestFakeObjectStore_CallTracking` -- PutCalls/GetCalls count correctly
  - `TestFakeObjectStore_Concurrent` -- 50 goroutines read/write simultaneously
  - `TestFakeObjectStore_InterfaceCompliance` -- compile-time `var _ storage.ObjectStore = (*FakeObjectStore)(nil)`

### `internal/testing/mock_producer.go` (NEW -- gomock-generated)
- Generated via: `mockgen -source=internal/gateway/producer.go -destination=internal/testing/mock_producer.go -package=testing Producer`
- No hand-written tests needed -- gomock generation is correct by construction
- Verified implicitly by all gateway handler tests that use it

### `internal/gateway/handler_test.go` (EXPAND -- migrate to gomock MockProducer)
- Remove ad-hoc `mockProducer` struct; replace with gomock `MockProducer`
- Existing tests rewritten to use `mp.EXPECT().ProduceToPartition(...).Return(nil)` pattern
- `TestPushHandler_ValidationFailure` -- event missing required labels after tenant stamp (uses gomock)
- `TestPushHandler_LargeBatch` -- 100+ events, verify count in response and kafka payload (uses gomock with `gomock.Do()` to capture args)

### `internal/ring/hashring_test.go` (EXPAND)
- `FuzzGetPartition` -- fuzz test: arbitrary strings, verify partition in [0, N)
- `TestGetPartition_TableDriven` -- known input/output pairs for regression

### `internal/ingester/batcher_test.go` (EXPAND)
- `TestBatcher_ConcurrentAdd` -- 10 goroutines adding 100 events each with race detector

### `internal/ingester/flusher_test.go` (EXPAND)
- `TestFlusher_BatchDeserialization` -- flush, read back from store, unmarshal, verify events match
- `TestFlusher_ContextCancellation` -- cancel context during retry, verify early return

### `internal/tenant/ratelimit_test.go` (EXPAND)
- `TestAllow_ManyTenants` -- create 1000 distinct tenants, verify all get limiters, no panic

### `internal/storage/filesystem_test.go` (EXPAND)
- `TestDeeplyNestedPaths` -- key like `a/b/c/d/e/f/g/file.json`
- `TestSpecialCharactersInKey` -- keys with spaces, unicode, hyphens, underscores
- `TestConcurrentPutSameKey` -- 10 goroutines writing to same key, verify final read is one of them (no corruption)
- `TestListOrdering` -- verify List returns consistent (sorted or not) results across calls

### Commands
- `go test -race -count=1 ./internal/testing/...`
- `go test -race -count=1 ./internal/gateway/...`
- `go test -race -count=1 ./internal/ring/...`
- `go test -race -count=1 ./internal/ingester/...`
- `go test -race -count=1 ./internal/tenant/...`
- `go test -race -count=1 ./internal/storage/...`
- `go test -race -count=1 ./...` (all at once)
- `go test -fuzz=FuzzGetPartition -fuzztime=10s ./internal/ring/...`
- `go test -bench=. -benchmem ./internal/ring/... ./internal/ingester/... ./internal/storage/...`

---

## Benchmarks + "Success"
N/A for the unit-test-engineer role. Benchmarks already exist in the codebase (hash ring, batcher, flusher, filesystem, serialization) and are owned by the benchmark-engineer. I will not duplicate them, but my fuzz test for the hash ring implicitly validates performance since it must complete within `fuzztime`.

---

# Recommended API surface

### `internal/testing/fakes.go` (hand-rolled)

```
FakeObjectStore (hand-rolled -- runtime fault toggling for chaos tests)
  - NewFakeObjectStore() *FakeObjectStore
  - Put(ctx, key, data) error          // storage.ObjectStore
  - Get(ctx, key) ([]byte, error)      // storage.ObjectStore
  - List(ctx, prefix) ([]string, error) // storage.ObjectStore
  - Delete(ctx, key) error             // storage.ObjectStore
  - Exists(ctx, key) (bool, error)     // storage.ObjectStore
  - SetError(err error)                // runtime fault injection
  - ClearError()                       // runtime fault clearing
  - SetDelay(d time.Duration)          // runtime latency injection
  - PutCalls() int                     // call tracking
  - GetCalls() int                     // call tracking

Compile-time check: var _ storage.ObjectStore = (*FakeObjectStore)(nil)
```

### `internal/testing/mock_producer.go` (gomock-generated)

```
Generated from gateway.Producer interface via mockgen.
Command: mockgen -source=internal/gateway/producer.go \
  -destination=internal/testing/mock_producer.go \
  -package=testing Producer

MockProducer (gomock-generated -- standard call capture + configurable returns)
  - All gateway.Producer methods auto-generated
  - Usage: ctrl := gomock.NewController(t)
           mp := NewMockProducer(ctrl)
           mp.EXPECT().ProduceToPartition(gomock.Any(), ...).Return(nil)
           mp.EXPECT().Close().Return(nil)
```

### Mock vs Fake decision matrix

| Interface       | Approach    | Justification                                        |
|-----------------|-------------|------------------------------------------------------|
| Producer        | gomock      | Simple call capture + configurable errors            |
| ObjectStore     | Hand-rolled | Chaos tests need runtime SetError/SetDelay toggling  |
| Any future simple interface | gomock | Default to gomock unless runtime toggling needed |

---

# Folder structure

```
internal/
  testing/
    fakes.go          -- FakeObjectStore hand-rolled fake (NEW)
    fakes_test.go     -- tests for FakeObjectStore (NEW)
    mock_producer.go  -- gomock-generated MockProducer (NEW, generated)
  gateway/
    handler_test.go   -- EXPAND: migrate to gomock MockProducer + 2 new tests
  ring/
    hashring_test.go  -- EXPAND with fuzz test + table-driven regression
  ingester/
    batcher_test.go   -- EXPAND with concurrent-add stress test
    flusher_test.go   -- EXPAND with deserialization + context cancellation tests
  tenant/
    ratelimit_test.go -- EXPAND with many-tenant test
  storage/
    filesystem_test.go -- EXPAND with nested paths, special chars, concurrent same-key, list ordering
```

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Add gomock dependency + generate MockProducer + create FakeObjectStore
- **Outcome:** `github.com/uber-go/mock` added to go.mod. `internal/testing/mock_producer.go` generated via mockgen. `internal/testing/fakes.go` with hand-rolled FakeObjectStore (with SetError, ClearError, SetDelay, PutCalls, GetCalls). `internal/testing/fakes_test.go` with full coverage of FakeObjectStore.
- **Files to create:** `internal/testing/fakes.go`, `internal/testing/fakes_test.go`, `internal/testing/mock_producer.go` (generated)
- **Files to modify:** `go.mod`, `go.sum`
- **Verification:** `go generate ./internal/testing/...` (if using go:generate directive) and `go test -race -count=1 ./internal/testing/...`
- **Commit message:** `test: add gomock MockProducer and hand-rolled FakeObjectStore`

### Task 2: Migrate gateway handler tests to gomock + expand
- **Outcome:** Remove ad-hoc `mockProducer` struct from `handler_test.go`. Replace with gomock `MockProducer`. Add new tests for event validation failure and large batch handling.
- **Files to modify:** `internal/gateway/handler_test.go`
- **Verification:** `go test -race -count=1 ./internal/gateway/...`
- **Commit message:** `test: migrate gateway handler to gomock MockProducer and expand tests`

### Task 3: Expand hash ring tests with fuzz + regression
- **Outcome:** Fuzz test (`FuzzGetPartition`) that verifies no panics and valid partition range; table-driven regression test with known values.
- **Files to modify:** `internal/ring/hashring_test.go`
- **Verification:** `go test -race -count=1 ./internal/ring/...` and `go test -fuzz=FuzzGetPartition -fuzztime=10s ./internal/ring/...`
- **Commit message:** `test: add hash ring fuzz test and table-driven regression`

### Task 4: Expand batcher and flusher tests (migrate flusher to FakeObjectStore)
- **Outcome:** Batcher concurrent-add stress test; flusher migrated from ad-hoc `mockStore` to shared `FakeObjectStore`; flusher batch-deserialization round-trip test and context-cancellation test.
- **Files to modify:** `internal/ingester/batcher_test.go`, `internal/ingester/flusher_test.go`
- **Verification:** `go test -race -count=1 ./internal/ingester/...`
- **Commit message:** `test: migrate flusher to FakeObjectStore, add batcher concurrency and flusher deserialization tests`

### Task 5: Expand rate limiter and filesystem storage tests
- **Outcome:** Rate limiter many-tenant test; filesystem tests for nested paths, special characters, concurrent same-key writes, and list ordering.
- **Files to modify:** `internal/tenant/ratelimit_test.go`, `internal/storage/filesystem_test.go`
- **Verification:** `go test -race -count=1 ./internal/tenant/... ./internal/storage/...`
- **Commit message:** `test: expand rate limiter and filesystem storage edge case tests`

### Task 6: Final verification -- full test suite pass
- **Outcome:** All existing + new tests pass together with race detector.
- **Files to modify:** none (verification only)
- **Verification:** `go test -race -count=1 -v ./...`
- **Commit message:** N/A (no code changes, verification only)

---

## Top 3 risks/unknowns and how to validate each in <15 minutes

1. **Risk: Circular import between `internal/testing/` and packages it depends on.**
   - `internal/testing/` imports `storage.ObjectStore` (for FakeObjectStore) and `gateway.Producer` (for mockgen source). Since `internal/testing/` is a leaf package, this works as long as neither `storage` nor `gateway` imports `internal/testing/`. Validate: run `go build ./internal/testing/...` after creating the files.

2. **Risk: gomock-generated MockProducer may become stale if `gateway.Producer` interface changes.**
   - Validate: add a `//go:generate mockgen ...` directive in `internal/testing/mock_producer.go` so `go generate ./...` regenerates it. CI should run `go generate` and fail if the generated file is out of date (diff check). For FakeObjectStore, use compile-time check: `var _ storage.ObjectStore = (*FakeObjectStore)(nil)`.

3. **Risk: Fuzz test for hash ring may find a genuine bug (e.g., index out of bounds for edge-case token values).**
   - Validate: run `go test -fuzz=FuzzGetPartition -fuzztime=30s` on the existing code before writing any new ring code. If it panics, the current code has a bug that needs fixing first.

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From unit-test-engineer
- **Coding style rules:**
  - All new tests must use `t.Run` subtests for grouping.
  - Table-driven tests for any function with more than 3 test cases.
  - Use `t.Helper()` in all test helper functions.
  - Use `t.Parallel()` for tests that don't share mutable state.
  - Prefer `t.TempDir()` over manual temp directory creation.
  - Use gomock (`github.com/uber-go/mock`) for simple interface stubs. Only hand-roll fakes when runtime fault toggling is needed.
  - Prefer shared test doubles from `internal/testing/` over ad-hoc mocks in test files.
  - Run `go generate ./internal/testing/...` after changing any mocked interface.

- **Dev commands:**
  - `go test -race -count=1 ./...` -- run all unit tests with race detector
  - `go test -fuzz=FuzzGetPartition -fuzztime=10s ./internal/ring/...` -- fuzz the hash ring
  - `go test -v -run TestName ./internal/package/...` -- run a specific test

- **Before you commit checklist:**
  - [ ] `go test -race -count=1 ./...` passes
  - [ ] `go vet ./...` passes
  - [ ] No test relies on external services (Kafka, MinIO, etc.)
  - [ ] New hand-rolled fakes have compile-time interface checks
  - [ ] gomock-generated mocks are up to date (`go generate ./internal/testing/...`)

- **Guardrails:**
  - Do not add build tags to unit tests (build tags are for integration/chaos only).
  - Do not import `net/http` in test fakes -- keep fakes pure data structures.
  - Any change to a production interface must: (a) regenerate gomock mocks via `go generate`, (b) update hand-rolled fakes + compile-time checks.
  - Default to gomock for new interface stubs. Only hand-roll when runtime fault toggling is required.

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

- **Testing architecture:** Three tiers -- unit (fast, no deps), integration (Docker), chaos (fault injection). Unit tests provide the foundation.
- **Test double strategy:** gomock for simple interface stubs (Producer), hand-rolled fakes for runtime fault toggling (FakeObjectStore). Decision matrix in plan doc.
- **Shared test infrastructure:** `internal/testing/` provides `FakeObjectStore` (hand-rolled, runtime-togglable errors/delays for chaos tests) and `MockProducer` (gomock-generated, standard call capture).
- **Key decisions:**
  - gomock for Producer: simple expectations, no runtime toggling needed. Eliminates hand-rolled boilerplate.
  - Hand-rolled FakeObjectStore: chaos tests need `SetError`/`SetDelay` toggled mid-test while the system is running. gomock expectations are set upfront and can't handle this.
  - Compile-time interface checks on hand-rolled fakes prevent drift. gomock mocks are regenerated from source.
  - Fuzz testing on the hash ring catches edge cases that table-driven tests miss.
  - Race detector (`-race`) is mandatory on all test runs to catch concurrency bugs early.
- **Limits of MVP:** Unit tests verify component contracts in isolation; they cannot verify end-to-end delivery guarantees (that is the integration test engineer's scope).
- **How to run:** `go test -race -count=1 ./...` runs all unit tests in ~5 seconds with no external dependencies.
- **How to validate:** Green test suite + zero race detector warnings = passing.

---

## READY FOR APPROVAL
