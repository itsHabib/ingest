# Master Plan Template (Agent Teams)

## You are in PLAN MODE.

### Project
I want to build a **comprehensive chaos/failure-mode test suite** for the ingestion engine.

**Goal:** build a **fault injection framework and chaos tests** in which we **deliberately break storage, kill ingesters mid-flush, and inject latency -- then verify the system recovers correctly with zero data loss**.

### Role + Scope (fill in)
- **Role:** Chaos Engineer
- **Scope:** Fault injection utilities (`internal/testing/fault_injector.go`), chaos tests (`chaos_test.go` with `//go:build chaos` tag), and the `scripts/chaos.sh` runner script. I own failure-mode testing only. I do NOT own unit tests, integration tests, fakes, the test harness, benchmarks, or any production code.
- **File you will write:** `/docs/quality/plans/chaos-engineer.md`
- **No-touch zones:** do not edit production code, unit tests, integration tests, test harness (`harness.go`), or fakes (`fakes.go`). Those belong to unit-test-engineer and integration-test-engineer.

---

## Functional Requirements
- **FR1:** `FaultInjector` utility that can break, heal, and slow down a `FakeObjectStore` on demand, with thread-safe toggling.
- **FR2:** Chaos test verifying ingester crash recovery — kill mid-flush, restart, confirm all events replayed from Kafka (at-least-once).
- **FR3:** Chaos test verifying storage failure triggers backpressure — break storage, confirm consumption pauses, heal storage, confirm resume and no data loss.
- **FR4:** Chaos test verifying slow storage causes batch accumulation without data loss.
- **FR5:** Chaos test verifying multi-ingester failover — kill one member of a consumer group, confirm Kafka rebalances and no data loss.
- **Tests required:** All deliverables ARE tests (chaos tier). Build tag `//go:build chaos`.
- **Metrics required:** N/A — chaos tests verify existing metrics behavior, do not add new ones.

## Non-Functional Requirements
- Language/runtime: Go (standard `testing` package)
- Local dev: `scripts/chaos.sh` brings up Docker Compose (Kafka, MinIO), runs chaos tests, tears down
- Observability: Tests assert on existing Prometheus counter values and log output where relevant
- Safety: All tests use `t.TempDir()` for filesystem isolation; Docker containers cleaned up via trap handler
- Documentation: CLAUDE.md + EXPLAIN.md contributions (proposed below)
- Performance: N/A — chaos tests are correctness-focused, not speed-focused

---

## Assumptions / System Model
- **Deployment environment:** Docker Compose (Kafka KRaft single-broker, MinIO for S3)
- **Failure modes tested:**
  1. Ingester process crash (kill mid-flush, offsets uncommitted, Kafka replays)
  2. Storage total failure (all Put/Get/Delete/List/Exists return error)
  3. Storage latency injection (configurable delay on every operation)
  4. Consumer group member loss (Kafka partition rebalance)
- **Delivery guarantees:** At-least-once. Chaos tests verify `count(stored) >= count(sent)`, never `<`.
- **Multi-tenancy:** Tests use multiple tenants to verify isolation is preserved under failure.

---

## Data Model (as relevant to your role)
The `FaultInjector` wraps the existing `FakeObjectStore` (from unit-test-engineer) and controls its error/delay state:

| Entity | Fields | Notes |
|--------|--------|-------|
| `FaultInjector` | `store *FakeObjectStore` | Reference to the fake being controlled |
| Broken state | `store.SetError(err)` | Makes all ops return the injected error |
| Healed state | `store.SetError(nil)` | Restores normal operation |
| Slow state | `delay time.Duration` on `FakeObjectStore` | Added via `SetDelay(d)` method — requires FakeObjectStore to support it |

The `FakeObjectStore` from unit-test-engineer already has `SetError(err error)`. The chaos engineer needs it to also support `SetDelay(d time.Duration)`. This will be coordinated with unit-test-engineer (they add the field; we use it).

---

## APIs (as relevant to your role)

### `internal/testing/fault_injector.go`

```go
// FaultInjector provides controlled failure injection for chaos tests.
type FaultInjector struct {
    store *FakeObjectStore
}

// NewFaultInjector wraps a FakeObjectStore for fault injection.
func NewFaultInjector(store *FakeObjectStore) *FaultInjector

// BreakStorage makes all storage operations return an error.
func (fi *FaultInjector) BreakStorage()

// HealStorage restores normal storage operation.
func (fi *FaultInjector) HealStorage()

// SlowStorage adds latency to every storage operation.
func (fi *FaultInjector) SlowStorage(delay time.Duration)

// ResetLatency removes injected latency.
func (fi *FaultInjector) ResetLatency()
```

### `chaos_test.go` (build tag: `//go:build chaos`)

| Test Function | What It Proves |
|---------------|----------------|
| `TestChaos_IngesterCrashRecovery` | At-least-once: kill ingester mid-flush, restart, all 500 events eventually in storage |
| `TestChaos_StorageFailure_Backpressure` | Backpressure: break storage -> consumption pauses -> heal -> resumes -> no data lost |
| `TestChaos_SlowStorage_BatchAccumulation` | Slow storage causes batches to grow but no data is lost |
| `TestChaos_MultipleIngesterFailover` | Kafka rebalance after consumer death, no data lost |

### `scripts/chaos.sh`

```bash
#!/usr/bin/env bash
# Bring up Docker, run chaos tests, tear down with trap cleanup.
# Usage: ./scripts/chaos.sh
```

---

## Architecture / Component Boundaries (as relevant)

```
                          +-----------------------+
                          |  FaultInjector        |
                          |  (controls the fake)  |
                          +-----------+-----------+
                                      |
                                      v
+----------+    +-------+    +--------+--------+    +-----------+
| Gateway  |--->| Kafka |--->| Ingester        |--->| FakeObj   |
| (HTTP)   |    | (real)|    | (Consumer+      |    | Store     |
+----------+    +-------+    | Batcher+Flusher)|    | (in-mem)  |
                             +-----------------+    +-----------+

For crash recovery test:
  - TestHarness.StopIngester() kills the consumer
  - TestHarness.StartIngester() restarts it
  - Kafka replays uncommitted offsets

For storage failure test:
  - FaultInjector.BreakStorage() -> FakeObjectStore.SetError(errInjected)
  - Consumer detects flush failures -> pauses Kafka consumption
  - FaultInjector.HealStorage() -> FakeObjectStore.SetError(nil)
  - Storage probe detects recovery -> resumes consumption

For multi-ingester failover:
  - Two Consumers in the same consumer group
  - Kill one -> Kafka rebalances partitions to the survivor
```

The chaos tests depend on:
- `internal/testing/fakes.go` (from unit-test-engineer) — `FakeObjectStore`
- `internal/testing/harness.go` (from integration-test-engineer) — `TestHarness` with `StopIngester()`/`StartIngester()` lifecycle methods
- Real Kafka (Docker Compose) for rebalance and replay tests

---

## Correctness Invariants (must be explicit)

1. **At-least-once delivery:** `count(events in storage) >= count(events sent to gateway)`. Never less.
2. **Crash recovery:** After ingester kill + restart, Kafka replays from last committed offset. All uncommitted batches are re-consumed and flushed.
3. **Backpressure activation:** When consecutive flush failures >= `pauseThreshold`, Kafka consumption is paused (verified via `consumer.paused` state or log output).
4. **Backpressure recovery:** When storage heals, the storage probe detects it within 10 seconds and resumes consumption.
5. **Tenant isolation under failure:** Even during chaos, tenant-A's events never appear in tenant-B's storage path.
6. **No data loss under slow storage:** Latency injection causes batches to accumulate (larger sizes) but total event count is preserved.

---

## Tests
All deliverables are tests. Specifics:

### Chaos tests (`chaos_test.go`, build tag `//go:build chaos`)

| Test | Modules Exercised | Invariant Verified |
|------|-------------------|--------------------|
| `TestChaos_IngesterCrashRecovery` | Consumer, Batcher, Flusher, Kafka consumer group | #1 At-least-once, #2 Crash recovery |
| `TestChaos_StorageFailure_Backpressure` | Consumer (backpressure), Flusher, FaultInjector | #3 Backpressure activation, #4 Recovery, #1 At-least-once |
| `TestChaos_SlowStorage_BatchAccumulation` | Batcher, Flusher, FaultInjector | #6 No data loss under latency |
| `TestChaos_MultipleIngesterFailover` | Consumer group, Kafka rebalance | #1 At-least-once, #5 Tenant isolation |

### Fault injection unit test (in `fault_injector.go` or `fault_injector_test.go`)
- Verify `BreakStorage` / `HealStorage` / `SlowStorage` toggle correctly
- Verify thread safety under concurrent access

### Exact commands
```bash
# Run chaos tests only (requires Docker)
go test -tags chaos -v -timeout 120s ./...

# Run via script (manages Docker lifecycle)
./scripts/chaos.sh

# Run a single chaos test
go test -tags chaos -v -run TestChaos_IngesterCrashRecovery -timeout 60s ./...
```

---

## Benchmarks + "Success"

N/A -- Chaos tests are correctness tests, not performance tests. Success is defined by:

1. All 4 chaos tests pass consistently (no flakes over 5 consecutive runs)
2. At-least-once invariant holds in every test (stored events >= sent events)
3. Backpressure activates and deactivates correctly (no stuck consumers)
4. Tests complete within the 120s timeout
5. `scripts/chaos.sh` exits cleanly (0 on success, non-zero on failure, always cleans up Docker)

---

# Please produce (no code yet):

## 1) Recommended API surface

### `internal/testing/fault_injector.go`
- `NewFaultInjector(store *FakeObjectStore) *FaultInjector`
- `(*FaultInjector) BreakStorage()` — calls `store.SetError(fmt.Errorf("injected: storage broken"))`
- `(*FaultInjector) HealStorage()` — calls `store.SetError(nil)`
- `(*FaultInjector) SlowStorage(delay time.Duration)` — calls `store.SetDelay(delay)`
- `(*FaultInjector) ResetLatency()` — calls `store.SetDelay(0)`

### `chaos_test.go`
- 4 test functions as described in the Tests section above
- Each uses `TestHarness` from integration-test-engineer
- Each uses `FaultInjector` to control failure injection
- Each verifies at-least-once by counting events in storage vs events sent

### `scripts/chaos.sh`
- Manages `docker compose up -d` / `docker compose down`
- Waits for Kafka readiness (poll topic creation)
- Runs `go test -tags chaos -v -timeout 120s ./...`
- Trap handler ensures cleanup on failure/interrupt

## 2) Folder structure

```
ingest/
  internal/
    testing/
      fault_injector.go       # NEW — FaultInjector struct + methods
      fault_injector_test.go   # NEW — unit tests for fault injector
      fakes.go                 # EXISTS (unit-test-engineer) — FakeObjectStore
      harness.go               # EXISTS (integration-test-engineer) — TestHarness
  chaos_test.go                # NEW — //go:build chaos — 4 chaos test functions
  scripts/
    chaos.sh                   # NEW — Docker lifecycle + chaos test runner
```

Ownership:
- `fault_injector.go`, `fault_injector_test.go`, `chaos_test.go`, `scripts/chaos.sh` — chaos-engineer (me)
- `fakes.go` — unit-test-engineer (dependency)
- `harness.go` — integration-test-engineer (dependency)

## 3) Step-by-step task plan

See "Tighten the plan" section below.

## 4) Benchmark plan

N/A — chaos tests are correctness-focused. Success is all tests pass, at-least-once holds, no flakes.

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Implement FaultInjector

- **Outcome:** `internal/testing/fault_injector.go` with `NewFaultInjector`, `BreakStorage`, `HealStorage`, `SlowStorage`, `ResetLatency`. Thin wrapper over `FakeObjectStore.SetError()` and `FakeObjectStore.SetDelay()`.
- **Files to create/modify:**
  - `internal/testing/fault_injector.go` (create)
  - `internal/testing/fault_injector_test.go` (create)
- **Exact verification command(s):**
  ```bash
  go test -v ./internal/testing/ -run TestFaultInjector
  go vet ./internal/testing/
  ```
- **Suggested commit message:** `test: add FaultInjector for controlled storage failure injection`

### Task 2: Implement crash recovery chaos test

- **Outcome:** `TestChaos_IngesterCrashRecovery` in `chaos_test.go`. Pushes 500 events, kills ingester mid-flush via `harness.StopIngester()`, restarts via `harness.StartIngester()`, waits for flush, verifies all 500 events in storage.
- **Files to create/modify:**
  - `chaos_test.go` (create with build tag `//go:build chaos`)
- **Exact verification command(s):**
  ```bash
  # Requires Docker running
  go test -tags chaos -v -run TestChaos_IngesterCrashRecovery -timeout 60s ./...
  ```
- **Suggested commit message:** `test: add chaos test for ingester crash recovery`

### Task 3: Implement storage failure + backpressure chaos test

- **Outcome:** `TestChaos_StorageFailure_Backpressure` in `chaos_test.go`. Pushes events, breaks storage via FaultInjector, verifies consumption pauses, heals storage, verifies resume and no data loss.
- **Files to create/modify:**
  - `chaos_test.go` (append)
- **Exact verification command(s):**
  ```bash
  go test -tags chaos -v -run TestChaos_StorageFailure_Backpressure -timeout 60s ./...
  ```
- **Suggested commit message:** `test: add chaos test for storage failure backpressure`

### Task 4: Implement slow storage + multi-ingester failover chaos tests

- **Outcome:** `TestChaos_SlowStorage_BatchAccumulation` and `TestChaos_MultipleIngesterFailover` in `chaos_test.go`. Slow storage test injects 500ms latency and verifies no data loss. Multi-ingester test starts 2 consumers, kills one, verifies rebalance and no data loss.
- **Files to create/modify:**
  - `chaos_test.go` (append)
- **Exact verification command(s):**
  ```bash
  go test -tags chaos -v -run "TestChaos_SlowStorage|TestChaos_MultipleIngester" -timeout 90s ./...
  ```
- **Suggested commit message:** `test: add slow storage and multi-ingester failover chaos tests`

### Task 5: Create chaos test runner script

- **Outcome:** `scripts/chaos.sh` that manages Docker Compose lifecycle and runs all chaos tests. Includes trap handler for cleanup on failure/interrupt.
- **Files to create/modify:**
  - `scripts/chaos.sh` (create, `chmod +x`)
- **Exact verification command(s):**
  ```bash
  bash -n scripts/chaos.sh   # syntax check
  ./scripts/chaos.sh          # full run (requires Docker)
  ```
- **Suggested commit message:** `test: add chaos test runner script with Docker lifecycle`

---

## Top 3 risks/unknowns and how to validate each in <15 minutes

### Risk 1: FakeObjectStore may not support `SetDelay()` yet
- **Validation:** Read `internal/testing/fakes.go` once unit-test-engineer delivers it. If `SetDelay` is missing, coordinate with them to add a `delay` field with `sync.RWMutex` protection. The change is ~10 lines.
- **Mitigation:** If coordination is slow, the FaultInjector can implement delay wrapping externally by wrapping the `ObjectStore` interface with a delay-injecting decorator instead.

### Risk 2: TestHarness may not expose `StopIngester()` / `StartIngester()` lifecycle methods
- **Validation:** Read `internal/testing/harness.go` once integration-test-engineer delivers it. The crash recovery test requires the ability to kill and restart the ingester independently.
- **Mitigation:** If harness doesn't support it, the chaos test can manage the ingester lifecycle directly (create Consumer, cancel its context, create a new one). More verbose but no external dependency.

### Risk 3: Kafka rebalance timing makes multi-ingester test flaky
- **Validation:** Run `TestChaos_MultipleIngesterFailover` 5 times in a row. Kafka rebalance can take 10-30 seconds depending on `session.timeout.ms`.
- **Mitigation:** Set `session.timeout.ms=6000` (6s) and `heartbeat.interval.ms=2000` (2s) in test config to speed up rebalance detection. Add a generous poll loop (up to 45s) waiting for the surviving ingester to pick up all partitions.

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From Chaos Engineer
### Coding style rules
- Chaos tests use build tag `//go:build chaos` — never mix with unit or integration tests
- Use `t.Helper()` in all chaos test helper functions
- All chaos tests must verify at-least-once: `stored >= sent`, never `==` (duplicates are expected)
- Use `t.Logf` liberally — chaos test failures are hard to debug without timing info
- Timeouts: individual test timeout 60s, full suite 120s

### Dev commands
```bash
# Run all chaos tests (requires Docker)
./scripts/chaos.sh

# Run a specific chaos test
go test -tags chaos -v -run TestChaos_IngesterCrashRecovery -timeout 60s ./...

# Run with race detector (slower but catches concurrency bugs)
go test -tags chaos -race -v -timeout 180s ./...
```

### Before you commit checklist
- [ ] All chaos tests pass: `go test -tags chaos -v -timeout 120s ./...`
- [ ] Race detector clean: `go test -tags chaos -race -timeout 180s ./...`
- [ ] No flakes: run the full suite 3 times consecutively
- [ ] `go vet ./...` passes
- [ ] `scripts/chaos.sh` exits 0 and cleans up Docker containers

### Guardrails
- Never commit a chaos test that sleeps for a fixed duration to "wait for something" — always poll with a timeout
- Never hardcode Kafka broker addresses — use config from test harness
- Chaos tests must clean up after themselves (temp dirs, Docker containers, goroutines)

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

### Flow / architecture explanation
- Chaos tests exercise the full pipeline (gateway -> Kafka -> ingester -> storage) with real Kafka and injected faults
- The `FaultInjector` wraps `FakeObjectStore` to toggle failure/latency at runtime
- Tests use `TestHarness` from integration-test-engineer with added lifecycle control (stop/start ingester)

### Key engineering decisions + tradeoffs
- **Build tags (`//go:build chaos`):** Keeps chaos tests separate from fast unit tests; CI can run them in a dedicated stage with Docker
- **FaultInjector as thin wrapper:** Rather than a complex fault injection framework, we delegate to `FakeObjectStore.SetError()`/`SetDelay()`. Simple, testable, no magic.
- **At-least-once verification (`>=` not `==`):** Kafka replay after crash may produce duplicates. Tests intentionally allow this — deduplication is a higher-level concern.
- **Polling over sleeping:** All "wait for X" loops use deadline-based polling (check condition, sleep 100ms, repeat) rather than fixed `time.Sleep`. This makes tests faster on fast machines and less flaky on slow ones.

### Limits of MVP + next steps
- Single Kafka broker — does not test broker failure or partition leader election
- `FakeObjectStore` for storage failure tests — does not test real S3/MinIO failure modes (network timeout, partial write)
- No network partition simulation (would need `iptables` or a proxy like Toxiproxy)
- Future: add Toxiproxy for realistic network fault injection between components

### How to run locally + how to validate
- `./scripts/chaos.sh` — one command, manages Docker, runs tests, cleans up
- Verify: all 4 chaos tests pass, exit code 0, `docker compose ps` shows no leftover containers

---

## READY FOR APPROVAL
