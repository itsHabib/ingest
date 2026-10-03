# Benchmark Engineer Plan

## You are in PLAN MODE.

### Project
I want to do a **performance benchmarking suite** for the ingestion engine.

**Goal:** build a **comprehensive Go benchmark suite** in which we **measure every hot path's latency, throughput, and allocation profile to establish baselines and identify optimization targets**.

### Role + Scope
- **Role:** Benchmark Engineer
- **Scope:** Write Go `_bench_test.go` files for hash ring, event serialization, batcher, and gateway handler. Capture baseline results. I do NOT own load testing (load-test-engineer), pprof profiling (profiling-engineer), or optimization implementation.
- **File I will write:** `/docs/performance/plans/benchmark-engineer.md`
- **No-touch zones:** do not edit any production code; do not modify existing test files; do not write load test scripts or profiling scripts.

---

## Functional Requirements
- FR1: Benchmarks for hash ring `GetPartition` (serial + parallel) and `NewRing` construction
- FR2: Benchmarks for event JSON marshal/unmarshal at 1, 100, and 1000 event batch sizes
- FR3: Benchmarks for `Batcher.Add` (serial, parallel, multi-tenant map growth)
- FR4: Benchmarks for gateway `handlePush` full HTTP request cycle (serial + parallel) using `httptest` and `mockProducer`
- FR5: All benchmarks report allocations via `b.ReportAllocs()`
- FR6: Baseline results captured to `benchmarks/baseline.txt` with `-count=5` for statistical validity
- Tests required: benchmarks ARE the tests; each benchmark verifies the hot path functions correctly under load
- Metrics required: N/A (benchmarks measure ns/op, B/op, allocs/op natively)

## Non-Functional Requirements
- Language/runtime: Go (standard `testing.B` framework)
- Local dev: `go test -bench=. -benchmem ./...` runs all benchmarks
- Observability: N/A for benchmarks
- Safety: benchmarks must not mutate shared state or require external services (Kafka, MinIO)
- Documentation: CLAUDE.md contributions for benchmark commands
- Performance: benchmark results ARE the performance data

---

## Assumptions / System Model
- Deployment environment: local machine, `go test` only (no Docker required)
- Failure modes: N/A (benchmarks are isolated, no I/O)
- Delivery guarantees: N/A
- Multi-tenancy: batcher benchmarks test multi-tenant scenarios (100 tenants) to measure map growth and lock contention

---

## Data Model (as relevant to this role)

Benchmarks operate on existing data types from `internal/event` and `internal/ingester`:

- **Event** (`internal/event/event.go`)
  - TenantID (string), Timestamp (time.Time), Labels (map[string]string), Value (float64), Payload ([]byte)
  - Benchmarks use events with 2 labels, no payload (typical hot path)

- **Batch** (`internal/event/event.go`)
  - TenantID, Partition, Events ([]Event), CreatedAt
  - Benchmarks test batches of 100 and 1000 events

- **Ring** (`internal/ring/hashring.go`)
  - Immutable after construction. 6 partitions, 64 vnodes (matches production config)
  - Lookup is binary search over sorted uint32 tokens

No validation rules or persistence relevant to benchmarks.

---

## APIs (as relevant to this role)

N/A -- benchmarks do not expose APIs. They exercise existing internal APIs:

| Function | Package | Benchmark |
|----------|---------|-----------|
| `Ring.GetPartition(key)` | `internal/ring` | BenchmarkRingGetPartition, BenchmarkRingGetPartition_Parallel |
| `ring.NewRing(p, v)` | `internal/ring` | BenchmarkRingConstruction |
| `json.Marshal(events)` | `internal/event` | BenchmarkMarshalEvent, BenchmarkMarshalBatch_100, BenchmarkMarshalBatch_1000 |
| `json.Unmarshal(data, &events)` | `internal/event` | BenchmarkUnmarshalEvent, BenchmarkUnmarshalBatch_100, BenchmarkUnmarshalBatch_1000 |
| `Batcher.Add(ev, partition, offset, topic)` | `internal/ingester` | BenchmarkBatcherAdd, BenchmarkBatcherAdd_Parallel, BenchmarkBatcherAdd_MultiTenant |
| `Handler.ServeHTTP(w, r)` | `internal/gateway` | BenchmarkHandlerPush, BenchmarkHandlerPush_Parallel |

---

## Architecture / Component Boundaries

Benchmarks are isolated test files that import only their target package. No cross-package dependencies beyond what the target already imports.

- **Hash ring benchmarks** (`internal/ring/hashring_bench_test.go`): Tests the `Ring` struct directly. No mocks needed -- ring is a pure data structure with no I/O.
- **Serialization benchmarks** (`internal/event/serialize_bench_test.go`): Note -- some benchmarks already exist here (BenchmarkMarshalEvents_10, _1000, BenchmarkUnmarshalEvents_10, _1000). I will ADD single-event and batch-level benchmarks alongside the existing ones.
- **Batcher benchmarks** (`internal/ingester/batcher_bench_test.go`): Uses `NewBatcher` with a large `maxSize` (1M) so batches never flush during benchmark. Drains `FlushChan` in a background goroutine to prevent blocking.
- **Handler benchmarks** (`internal/gateway/handler_bench_test.go`): Uses the existing `mockProducer` and `newTestHandler` from `handler_test.go`. Constructs `httptest.NewRequest` per iteration.

Concurrency model for parallel benchmarks:
- `b.RunParallel` uses `testing.PB` to distribute iterations across GOMAXPROCS goroutines
- Hash ring: safe for concurrent reads (immutable after construction)
- Batcher: `sync.Mutex` contention is what we're measuring
- Handler: `httptest.ResponseRecorder` per goroutine, shared handler (thread-safe)

---

## Correctness Invariants

1. **Ring lookup is deterministic**: same key always returns same partition (tested by existing `TestDeterminism`)
2. **JSON round-trip preserves data**: marshal then unmarshal yields identical events (tested by existing `TestMarshalUnmarshalEvents_RoundTrip`)
3. **Batcher does not lose events**: every `Add` call either stores the event in pending or sends a full batch to flushCh
4. **Handler returns 200 for valid requests**: benchmark setup must produce 200 status codes (validated in benchmark setup, not per-iteration)
5. **Parallel benchmarks do not data-race**: all parallel benchmarks must pass `go test -race`

---

## Tests

Benchmarks ARE the tests for this role. Specific files:

- **Unit benchmarks:**
  - `internal/ring/hashring_bench_test.go` -- ring lookup and construction
  - `internal/event/serialize_bench_test.go` -- JSON marshal/unmarshal (extends existing file)
  - `internal/ingester/batcher_bench_test.go` -- batcher add operations
  - `internal/gateway/handler_bench_test.go` -- full HTTP handler cycle

- **Race detection:**
  - `go test -race -bench=Parallel ./internal/ring/ ./internal/ingester/ ./internal/gateway/`

- **Exact commands:**
  - Run all benchmarks: `go test -bench=. -benchmem -count=5 ./... 2>&1 | tee benchmarks/baseline.txt`
  - Run ring benchmarks only: `go test -bench=BenchmarkRing -benchmem ./internal/ring/`
  - Run serialization benchmarks only: `go test -bench=BenchmarkMarshal -benchmem ./internal/event/`
  - Run batcher benchmarks only: `go test -bench=BenchmarkBatcher -benchmem ./internal/ingester/`
  - Run handler benchmarks only: `go test -bench=BenchmarkHandler -benchmem ./internal/gateway/`
  - Race check: `go test -race -bench=. ./...`

---

## Benchmarks + "Success"

This is the core of my role. Success criteria:

| Benchmark | Target ns/op | Target allocs/op | Rationale |
|-----------|-------------|-----------------|-----------|
| RingGetPartition | < 500 | 0 | Binary search + xxhash on uint32 slice. No allocations expected. |
| RingGetPartition_Parallel | < 500 | 0 | Read-only, no contention. Should scale linearly. |
| RingConstruction (6p, 64v) | < 100,000 | moderate | One-time cost. Allocations acceptable. |
| MarshalEvent (single) | < 1,000 | low | Single small struct to JSON. |
| UnmarshalEvent (single) | < 2,000 | low | JSON to single struct. |
| MarshalBatch_100 | < 100,000 | moderate | 100 events. Watch for per-event allocs. |
| MarshalBatch_1000 | < 1,000,000 | moderate | 1000 events. Linear scaling expected. |
| BatcherAdd | < 1,000 | < 3 | Key via `fmt.Sprintf`, map lookup, append. |
| BatcherAdd_Parallel | < 2,000 | < 3 | Mutex contention. 2x serial is acceptable. >5x is a red flag. |
| BatcherAdd_MultiTenant (100) | < 2,000 | < 3 | Map with 100 keys. Measure map growth cost. |
| HandlerPush | < 50,000 | moderate | Full HTTP cycle: decode, validate, marshal, mock produce. |
| HandlerPush_Parallel | < 50,000 | moderate | Should scale; no shared mutable state besides mock. |

**Benchmark commands:**
```bash
# Full suite with statistical significance
go test -bench=. -benchmem -count=5 ./... 2>&1 | tee benchmarks/baseline.txt

# Compare after optimization (profiling-engineer will use this)
benchstat benchmarks/baseline.txt benchmarks/optimized.txt
```

---

## Engineering Decisions & Tradeoffs

### Decision 1: Separate `_bench_test.go` files vs. adding to existing `_test.go`
- **Decision:** Create separate `_bench_test.go` files for ring, batcher, and handler. Extend the existing `serialize_test.go` for event serialization (since it already has benchmarks).
- **Alternatives considered:** Put all benchmarks in existing test files.
- **Why:** Separate files make benchmarks easy to find, run selectively, and review independently. The serialization file already has benchmarks, so extending it maintains consistency.
- **Tradeoff acknowledged:** Slightly more files in the codebase. Benchmark helpers (e.g., `makeBenchEvents`) in the serialization file are already defined there, so extending is natural.

### Decision 2: Use `mockProducer` from handler_test.go vs. a dedicated benchmark mock
- **Decision:** Reuse the existing `mockProducer` in `handler_test.go` for handler benchmarks.
- **Alternatives considered:** Create a zero-allocation `noopProducer` specifically for benchmarks to eliminate mock overhead from measurements.
- **Why:** The `mockProducer` has near-zero overhead (no allocations in the happy path, just sets fields). Using it keeps the benchmark close to the real test setup. If profiling later shows mock overhead is significant, a noop producer can be swapped in.
- **Tradeoff acknowledged:** Mock field assignments add a tiny constant cost per iteration, but this is negligible compared to JSON decode/encode in the handler.

### Decision 3: `-count=5` for baseline capture
- **Decision:** Run benchmarks 5 times for `benchstat` compatibility.
- **Alternatives considered:** `-count=10` for higher statistical confidence, or `-count=1` for speed.
- **Why:** 5 is the standard `benchstat` recommendation. Enough to detect variance without making the run too long.
- **Tradeoff acknowledged:** 5 runs take ~2-3 minutes total. Acceptable for baseline capture.

---

## Risks & Mitigations

### Risk 1: Batcher benchmark blocks on full flushCh
- **Risk:** If `Batcher.Add` fills the flush channel (cap 100), the benchmark goroutine blocks, giving misleading results.
- **Impact:** Benchmark hangs or measures channel contention instead of Add performance.
- **Mitigation:** Use a very large `maxSize` (1,000,000) so batches never trigger a flush during the benchmark. Alternatively, drain `flushCh` in a background goroutine.
- **Validation time:** < 5 minutes (run the benchmark, verify it completes without hanging).

### Risk 2: Existing benchmarks in serialize_test.go conflict with new ones
- **Risk:** Adding new benchmarks to `serialize_test.go` may clash with existing `BenchmarkMarshalEvents_10/1000` naming.
- **Impact:** Confusing output, duplicate benchmark names.
- **Mitigation:** Use distinct names: `BenchmarkMarshalEvent` (single), `BenchmarkMarshalBatch_100`, `BenchmarkMarshalBatch_1000` vs. existing `BenchmarkMarshalEvents_10`, `BenchmarkMarshalEvents_1000`. Clear naming differentiation.
- **Validation time:** < 2 minutes (check `go test -list Benchmark ./internal/event/` for conflicts).

### Risk 3: Handler benchmark allocations dominated by httptest overhead
- **Risk:** `httptest.NewRequest` and `httptest.NewRecorder` allocate significantly per call, drowning out handler-specific allocations.
- **Impact:** Benchmark measures httptest overhead, not handler efficiency.
- **Mitigation:** This is expected and acceptable. The benchmark measures the full HTTP request cycle (which is the real hot path). Document that allocations include httptest overhead. If needed, the profiling-engineer can isolate handler internals via pprof.
- **Validation time:** < 5 minutes (run benchmark, note allocs/op).

### Risk 4: Parallel benchmarks may data-race on mockProducer fields
- **Risk:** `mockProducer` sets `lastKey`, `lastValue`, `callCount` without synchronization.
- **Impact:** Race detector failure; invalid benchmark results.
- **Mitigation:** For parallel handler benchmarks, create a dedicated noop producer that does not record state, or use separate mockProducer instances per goroutine via `b.RunParallel`.
- **Validation time:** < 5 minutes (run `go test -race -bench=Parallel ./internal/gateway/`).

---

## Recommended API Surface

No new APIs. Benchmarks exercise existing internal APIs (see APIs section above).

## Folder Structure

```
internal/
  ring/
    hashring.go              (existing, read-only)
    hashring_test.go          (existing, read-only)
    hashring_bench_test.go    (NEW — ring benchmarks)
  event/
    event.go                  (existing, read-only)
    serialize_test.go         (MODIFY — add single-event and batch benchmarks)
  ingester/
    batcher.go                (existing, read-only)
    batcher_bench_test.go     (NEW — batcher benchmarks)
  gateway/
    handler.go                (existing, read-only)
    handler_test.go           (existing, read-only)
    handler_bench_test.go     (NEW — handler benchmarks)
benchmarks/
    baseline.txt              (NEW — captured baseline results)
```

---

# Tighten the plan into 4-7 small tasks

### Task 1: Hash ring benchmarks
- **Outcome:** `internal/ring/hashring_bench_test.go` with `BenchmarkRingGetPartition`, `BenchmarkRingGetPartition_Parallel`, `BenchmarkRingConstruction`
- **Files to create/modify:** `internal/ring/hashring_bench_test.go` (new)
- **Verification:**
  ```bash
  go test -bench=BenchmarkRing -benchmem ./internal/ring/
  go test -race -bench=BenchmarkRing ./internal/ring/
  ```
- **Commit message:** `bench: add hash ring benchmarks for lookup and construction`

### Task 2: Event serialization benchmarks
- **Outcome:** Add `BenchmarkMarshalEvent`, `BenchmarkUnmarshalEvent`, `BenchmarkMarshalBatch_100`, `BenchmarkMarshalBatch_1000`, `BenchmarkUnmarshalBatch_100`, `BenchmarkUnmarshalBatch_1000` to existing file
- **Files to create/modify:** `internal/event/serialize_test.go` (modify — append new benchmarks)
- **Verification:**
  ```bash
  go test -bench=Benchmark -benchmem ./internal/event/
  ```
- **Commit message:** `bench: add single-event and batch serialization benchmarks`

### Task 3: Batcher benchmarks
- **Outcome:** `internal/ingester/batcher_bench_test.go` with `BenchmarkBatcherAdd`, `BenchmarkBatcherAdd_Parallel`, `BenchmarkBatcherAdd_MultiTenant`
- **Files to create/modify:** `internal/ingester/batcher_bench_test.go` (new)
- **Verification:**
  ```bash
  go test -bench=BenchmarkBatcher -benchmem ./internal/ingester/
  go test -race -bench=BenchmarkBatcher ./internal/ingester/
  ```
- **Commit message:** `bench: add batcher benchmarks for serial, parallel, and multi-tenant add`

### Task 4: Gateway handler benchmarks
- **Outcome:** `internal/gateway/handler_bench_test.go` with `BenchmarkHandlerPush`, `BenchmarkHandlerPush_Parallel`
- **Files to create/modify:** `internal/gateway/handler_bench_test.go` (new)
- **Verification:**
  ```bash
  go test -bench=BenchmarkHandler -benchmem ./internal/gateway/
  go test -race -bench=BenchmarkHandler ./internal/gateway/
  ```
- **Commit message:** `bench: add gateway handler benchmarks for full HTTP request cycle`

### Task 5: Capture baseline results
- **Outcome:** `benchmarks/baseline.txt` with 5 runs of all benchmarks
- **Files to create/modify:** `benchmarks/baseline.txt` (new directory + file)
- **Verification:**
  ```bash
  go test -bench=. -benchmem -count=5 ./... 2>&1 | tee benchmarks/baseline.txt
  # Verify file is non-empty and contains all benchmark names
  grep -c "Benchmark" benchmarks/baseline.txt
  ```
- **Commit message:** `bench: capture baseline benchmark results`

---

## CLAUDE.md contributions (do NOT write the file; propose content)

### From benchmark-engineer

**Coding style rules:**
- Benchmark functions follow `Benchmark<Component><Operation>` naming (e.g., `BenchmarkRingGetPartition`)
- Parallel benchmarks use `_Parallel` suffix and `b.RunParallel`
- All benchmarks call `b.ReportAllocs()`
- Benchmark test data is constructed once outside the `b.N` loop (use `b.ResetTimer()` after setup)
- Benchmark files use `_bench_test.go` suffix for easy filtering

**Dev commands:**
```bash
# Run all benchmarks
go test -bench=. -benchmem ./...

# Run benchmarks for a specific package
go test -bench=. -benchmem ./internal/ring/

# Run with race detector (parallel benchmarks)
go test -race -bench=Parallel ./...

# Capture results for comparison
go test -bench=. -benchmem -count=5 ./... 2>&1 | tee benchmarks/results.txt

# Compare two runs (requires golang.org/x/perf/cmd/benchstat)
benchstat benchmarks/baseline.txt benchmarks/optimized.txt
```

**Before you commit checklist:**
- [ ] All benchmarks pass: `go test -bench=. ./...`
- [ ] No data races: `go test -race -bench=. ./...`
- [ ] Benchmarks report allocations (`b.ReportAllocs()` present)
- [ ] Benchmark names are descriptive and follow naming convention

**Guardrails:**
- Never modify production code in benchmark files
- Never use `b.N` as a parameter value in benchmark logic (it controls iteration count only)
- Never call `b.Fatal` inside `b.RunParallel` (use `b.Error` instead)

---

## EXPLAIN.md contributions (propose outline bullets)

### Benchmarking
- **Flow:** Go `testing.B` framework runs each benchmark function, increasing `b.N` until timing stabilizes. Results report ns/op, B/op (bytes allocated), and allocs/op.
- **Hot paths measured:** hash ring lookup (O(log N) binary search), JSON serialization (encoding/json), batcher add (mutex + map + append), gateway handler (full HTTP cycle).
- **Key engineering decisions:**
  - Used `b.RunParallel` to measure lock contention in batcher and scalability of the handler under concurrent load.
  - Kept benchmarks isolated (no Kafka, no Docker) so they run in CI without infrastructure.
  - Extended existing `serialize_test.go` rather than creating a new file, maintaining the existing benchmark pattern.
- **Limits of MVP:**
  - Benchmarks use `encoding/json` (stdlib). A future optimization could switch to a faster JSON library (e.g., `github.com/goccy/go-json` or `github.com/bytedance/sonic`).
  - Handler benchmarks include `httptest` overhead. For micro-benchmarking handler internals, use pprof.
  - No protobuf benchmarks yet (protobuf serialization is defined in `proto/event.proto` but not implemented).
- **How to run locally:** `go test -bench=. -benchmem ./...`
- **How to validate:** Compare ns/op and allocs/op against targets in the plan. Use `benchstat` to compare before/after optimization runs.

---

## READY FOR APPROVAL
