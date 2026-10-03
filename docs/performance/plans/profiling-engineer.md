# Profiling Engineer Plan

## You are in PLAN MODE.

### Project
I want to do a **performance profiling and optimization pass** on the ingestion engine.

**Goal:** Profile the running system under load with pprof, identify CPU/memory/contention hotspots, apply targeted optimizations, and document the before/after narrative.

### Role + Scope (fill in)
- **Role:** Profiling Engineer
- **Scope:** pprof profiling infrastructure, runtime profile capture under load, profile analysis, targeted code optimizations based on findings, before/after benchstat comparison, performance report documentation.
- **Explicitly NOT owned:** Benchmark scaffolding (benchmark-engineer), load test scenario design (load-test-engineer), Docker Compose infrastructure, Grafana dashboards.
- **File you will write:** `/docs/performance/plans/profiling-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: Enable mutex and block profiling in the running system so pprof captures contention data.
- FR2: Provide a `scripts/profile.sh` script that starts load, captures CPU/heap/mutex/block profiles from gateway and ingester, and saves them to `profiles/`.
- FR3: Analyze profiles and document top CPU consumers, allocation sites, mutex contention points, and blocking points.
- FR4: Apply targeted optimizations to the top 2-3 hotspots found in profiles.
- FR5: Re-benchmark after optimizations and produce benchstat comparison showing statistically significant improvements.
- FR6: Document the full profiling narrative in `docs/performance/report.md`.
- Tests required: Re-run `go test -bench=. -benchmem ./...` before and after optimizations to produce baseline and optimized benchmark files.
- Metrics required: N/A (Prometheus metrics already exist; this role focuses on pprof, not dashboards).

## Non-Functional Requirements
- Language/runtime: Go (pprof is stdlib)
- Local dev: docker-compose brings up Kafka, MinIO, Prometheus, Grafana; gateway and ingester run natively for profiling (avoids container overhead skewing profiles)
- Observability: pprof endpoints on admin ports (gateway :9100, ingester :9101)
- Safety: `runtime.SetMutexProfileFraction(5)` and `runtime.SetBlockProfileRate(1)` are low-overhead settings safe for profiling runs; they should NOT be enabled unconditionally in production.
- Documentation: `docs/performance/report.md` with flame graph observations, hotspot analysis, optimization narrative
- Performance: optimizations must show measurable improvement via benchstat (p < 0.05)

---

## Assumptions / System Model
- Deployment environment: local docker-compose for Kafka/MinIO; gateway and ingester run as native Go processes (not containerized) during profiling to get accurate CPU profiles without container scheduling noise.
- Failure modes: not directly relevant to profiling; we profile the happy path under sustained load.
- Delivery guarantees: unchanged (at-least-once); optimizations must not alter correctness.
- Multi-tenancy: loadgen generates events for 5 tenants concurrently, which exercises the hash ring and per-tenant batcher paths.

---

## Data Model (as relevant to your role)
N/A -- not in scope for this role. The profiling engineer does not change data models. We analyze how existing data models (Event, Batch) are serialized/deserialized and where allocations occur, but the schema is unchanged.

---

## APIs (as relevant to your role)

### pprof endpoints (already registered, need enhancement)

All registered on the admin mux at `/debug/pprof/*`:

| Endpoint | Description |
|----------|-------------|
| `GET /debug/pprof/profile?seconds=30` | CPU profile (30s sampling) |
| `GET /debug/pprof/heap` | Heap allocations snapshot |
| `GET /debug/pprof/mutex` | Mutex contention profile |
| `GET /debug/pprof/block` | Goroutine blocking profile |
| `GET /debug/pprof/goroutine` | Goroutine stack dump |
| `GET /debug/pprof/allocs` | Allocation sampling profile |

Enhancement needed in `internal/observability/pprof.go`:
- Add `runtime.SetMutexProfileFraction(5)` -- sample 1 in 5 mutex contention events
- Add `runtime.SetBlockProfileRate(1)` -- record all blocking events

These calls must happen once at init time, before any profiling endpoint is hit.

---

## Architecture / Component Boundaries (as relevant)

### Components touched by profiling

1. **`internal/observability/pprof.go`** -- Enable mutex/block profiling rates. Currently only registers handlers; needs runtime configuration.

2. **`scripts/profile.sh`** -- New script that orchestrates a profiling session:
   - Ensures `profiles/` directory exists
   - Starts loadgen at 5000 events/sec, 5 tenants, 60s duration
   - While load runs, captures profiles from gateway (:9100) and ingester (:9101) via curl
   - Saves `.pb.gz` files to `profiles/`

3. **Hot-path code that may be optimized** (read-only analysis, then targeted edits):
   - `internal/gateway/handler.go` -- double JSON marshal (unmarshal from body, re-marshal for Kafka), `io.ReadAll` allocations
   - `internal/ingester/batcher.go` -- `fmt.Sprintf` for key generation on every `Add()` call, single mutex for all partitions
   - `internal/ingester/flusher.go` -- `json.Marshal` per batch (allocates new buffer each time)
   - `internal/ring/hashring.go` -- `xxhash.Sum64String` on every lookup (already fast, but verify)
   - `internal/storage/storage.go` -- `fmt.Sprintf` in `BatchKey()` on every flush

4. **`docs/performance/report.md`** -- New document with profiling findings and optimization narrative.

### Concurrency model awareness
- Gateway: one goroutine per HTTP request; contention risk at Kafka producer
- Ingester: N flush workers sharing one `Batcher.mu` mutex; this is the primary contention risk
- Batcher: single `sync.Mutex` guards the `pending` map; every `Add()` call acquires it

### Profiling does NOT require:
- Changes to Docker Compose
- Changes to Kafka configuration
- Changes to Prometheus metrics
- Changes to the load generator

---

## Correctness Invariants (must be explicit)

1. **Optimizations must not change external behavior.** All existing unit tests, integration tests, and chaos tests must continue to pass after optimizations.
2. **At-least-once delivery is preserved.** No optimization may cause events to be silently dropped.
3. **Tenant isolation is preserved.** Optimizations to the batcher (e.g., sharding) must maintain the invariant that tenant-A events never appear in tenant-B storage paths.
4. **Thread safety is preserved.** Any lock restructuring (e.g., batcher sharding) must pass the race detector: `go test -race ./...`.
5. **Benchmark reproducibility.** Benchmarks run with `-count=5` and compared with benchstat for statistical significance.

---

## Tests

### Unit tests (which modules)
- Re-run all existing unit tests after each optimization to verify no regressions:
  ```bash
  go test ./...
  ```
- Run with race detector to verify thread safety of any lock changes:
  ```bash
  go test -race ./...
  ```

### Benchmark tests (critical for this role)
- Capture baseline before any optimization:
  ```bash
  go test -bench=. -benchmem -count=5 ./... | tee benchmarks/baseline.txt
  ```
- Capture optimized after each round of changes:
  ```bash
  go test -bench=. -benchmem -count=5 ./... | tee benchmarks/optimized.txt
  ```
- Compare with benchstat:
  ```bash
  benchstat benchmarks/baseline.txt benchmarks/optimized.txt
  ```

### Integration tests
- After all optimizations, run integration tests to verify end-to-end correctness:
  ```bash
  go test -tags integration -v -count=1 ./...
  ```

### Fuzz/property tests
- N/A for this role (already covered by quality phase).

### Failure injection tests
- N/A directly, but run chaos tests to ensure optimizations don't break recovery:
  ```bash
  go test -tags chaos -v -timeout 120s ./...
  ```

---

## Benchmarks + "Success"

### What to measure
1. **CPU time** -- top functions in gateway and ingester CPU profiles
2. **Heap allocations** -- bytes allocated per operation, allocation sites
3. **Mutex contention** -- time goroutines spend waiting for locks (especially `Batcher.mu`)
4. **Block profile** -- time goroutines spend blocked on channels/I/O

### Target success criteria
- Hash ring lookup: < 500ns/op (already likely fast, verify)
- Event JSON marshal: < 1us/op
- `Batcher.Add`: < 500ns/op (excluding channel send)
- Batch flush marshal: measurable improvement with buffer pooling
- Overall: at least one optimization shows >= 20% improvement in ns/op or allocs/op via benchstat

### Benchmark commands
```bash
# Individual hot paths
go test -bench=BenchmarkGetPartition -benchmem -count=5 ./internal/ring/
go test -bench=BenchmarkMarshal -benchmem -count=5 ./internal/event/
go test -bench=BenchmarkBatcherAdd -benchmem -count=5 ./internal/ingester/
go test -bench=BenchmarkFlush -benchmem -count=5 ./internal/ingester/

# All benchmarks
go test -bench=. -benchmem -count=5 ./... | tee benchmarks/baseline.txt
```

---

## Engineering Decisions & Tradeoffs (REQUIRED)

### Decision 1: Profile native processes, not containerized
- **Decision:** Run gateway and ingester as native Go processes during profiling, not inside Docker containers.
- **Alternatives considered:** Profile inside containers using `docker exec curl` to hit pprof endpoints.
- **Why:** Container scheduling, cgroup CPU limits, and network namespacing add noise to CPU profiles. Native profiling gives the truest picture of Go-level hotspots. Production profiling (in containers) is a separate concern.
- **Tradeoff acknowledged:** Profiles may not capture container-specific overhead (cgroup throttling, overlay filesystem latency). This is acceptable because we are optimizing Go code, not container configuration.

### Decision 2: Use `sync.Pool` for buffer reuse rather than custom slab allocator
- **Decision:** If JSON marshal allocations are a hotspot, use `sync.Pool` of `bytes.Buffer` for reuse.
- **Alternatives considered:** Pre-allocated ring buffer, custom slab allocator, switching to a zero-allocation serialization format (flatbuffers/cap'n'proto).
- **Why:** `sync.Pool` is idiomatic Go, well-understood, zero external dependencies, and directly addresses the "allocate-marshal-discard" pattern. It integrates with the GC (pools are drained on GC). The codebase already uses `encoding/json`, so staying with JSON + pool is the minimal change.
- **Tradeoff acknowledged:** `sync.Pool` doesn't guarantee buffer reuse (GC can drain it), so improvement is probabilistic under low load. For sustained high-throughput (our profiling scenario), pool hit rate will be high.

### Decision 3: Shard batcher by partition key rather than global lock
- **Decision:** If mutex contention in `Batcher.Add` is a hotspot, shard the pending map so each partition key maps to a dedicated lock (e.g., stripe-lock pattern with N shards).
- **Alternatives considered:** (a) Lock-free concurrent map (e.g., `sync.Map`), (b) channel-per-partition with dedicated goroutine, (c) keep single lock but reduce critical section.
- **Why:** `sync.Map` is optimized for read-heavy workloads, but batcher is write-heavy (every event calls `Add`). Channel-per-partition adds goroutine overhead and complicates FlushAll. Stripe-locking is a well-known pattern that reduces contention proportional to the number of stripes while keeping the code structurally similar.
- **Tradeoff acknowledged:** Stripe-locking adds code complexity (shard selection, FlushAll must iterate all shards). Only apply if mutex profile shows >= 10% of CPU time in lock contention.

---

## Risks & Mitigations (REQUIRED)

### Risk 1: Profiles show no significant hotspot
- **Risk:** The system is already fast enough that no single hotspot dominates, making optimization difficult.
- **Impact:** The profiling narrative becomes "I profiled and confirmed the system is well-optimized" -- less actionable.
- **Mitigation:** Focus on allocation profiles (heap) which almost always reveal pooling opportunities in JSON-heavy Go services. Also try higher load rates (10k-20k events/sec) to amplify any contention.
- **Validation time:** < 10 minutes (capture profile, check top10 functions).

### Risk 2: Optimization introduces a correctness regression
- **Risk:** Buffer pooling or lock sharding introduces subtle race conditions or data corruption.
- **Impact:** Tests fail; worse, silent data corruption in integration tests.
- **Mitigation:** Run `go test -race ./...` after every optimization. Run integration tests before declaring success. Keep each optimization in a separate commit for easy revert.
- **Validation time:** < 5 minutes (race detector + unit tests).

### Risk 3: benchstat shows no statistically significant improvement
- **Risk:** Optimization reduces allocations but benchstat p-value is > 0.05 due to variance.
- **Impact:** Cannot claim a measurable improvement.
- **Mitigation:** Use `-count=10` instead of `-count=5` for more samples. Run benchmarks on an otherwise idle machine. Use `perflock` or `cpufreq` pinning if available.
- **Validation time:** < 10 minutes (re-run benchmarks with more samples).

### Risk 4: pprof endpoints not accessible during load
- **Risk:** Admin server not starting, port conflict, or pprof not registered on the admin mux.
- **Impact:** Cannot capture profiles; blocked until fixed.
- **Mitigation:** Before running the full profiling session, verify `curl -s http://localhost:9100/debug/pprof/ | head` returns the pprof index. The existing `pprof_test.go` already tests handler registration.
- **Validation time:** < 2 minutes (start gateway, curl pprof index).

### Risk 5: `fmt.Sprintf` in Batcher.Add is not actually a hotspot
- **Risk:** We assume `fmt.Sprintf("%s:%d", tenantID, partition)` is expensive per-call, but it may be negligible compared to actual batch operations.
- **Impact:** Optimizing it (e.g., pre-computed keys, `strings.Builder`) adds complexity for no measurable gain.
- **Mitigation:** Only optimize if the CPU profile shows `fmt.Sprintf` or `runtime.convT` in the top 10 callers within `Batcher.Add`. Profile first, optimize second.
- **Validation time:** < 5 minutes (check pprof top output).

---

# Please produce (no code yet):

## 1) Recommended API surface

No new APIs. The profiling work enhances existing pprof endpoints and produces a profiling script.

**Functions/endpoints to add or modify:**

| Item | Location | Behavior |
|------|----------|----------|
| `EnableProfilingRates()` | `internal/observability/pprof.go` | Calls `runtime.SetMutexProfileFraction(5)` and `runtime.SetBlockProfileRate(1)`. Called once from `RegisterPprof`. |
| `scripts/profile.sh` | `scripts/profile.sh` | Bash script: creates `profiles/` dir, runs loadgen, captures CPU/heap/mutex/block profiles from both gateway and ingester via curl, saves as `.pb.gz` |
| Buffer pool helpers | `internal/ingester/flusher.go` | `sync.Pool` of `bytes.Buffer` for batch JSON marshaling (if heap profile confirms this hotspot) |
| Optimized batcher key | `internal/ingester/batcher.go` | Replace `fmt.Sprintf` with `strconv`-based key construction (if CPU profile confirms) |

## 2) Folder structure

```
ingest/
  profiles/                          # NEW: captured pprof profiles (.pb.gz)
    cpu-gateway.pb.gz
    cpu-ingester.pb.gz
    heap-gateway.pb.gz
    heap-ingester.pb.gz
    mutex-gateway.pb.gz
    mutex-ingester.pb.gz
    block-gateway.pb.gz
    block-ingester.pb.gz
  benchmarks/                        # NEW (or shared with benchmark-engineer)
    baseline.txt
    optimized.txt
  scripts/
    profile.sh                       # NEW: profiling session orchestrator
  internal/
    observability/
      pprof.go                       # MODIFIED: add mutex/block profiling rates
    ingester/
      batcher.go                     # POTENTIALLY MODIFIED: key generation optimization
      flusher.go                     # POTENTIALLY MODIFIED: sync.Pool for marshal buffers
    gateway/
      handler.go                     # POTENTIALLY MODIFIED: buffer pooling for body read / re-marshal
  docs/
    performance/
      report.md                      # NEW: profiling findings and optimization narrative
```

Ownership:
- `scripts/profile.sh` -- profiling-engineer owns
- `profiles/` -- profiling-engineer owns (gitignored, binary data)
- `docs/performance/report.md` -- profiling-engineer owns
- `internal/observability/pprof.go` -- profiling-engineer modifies (small enhancement)
- `internal/ingester/batcher.go`, `flusher.go`, `gateway/handler.go` -- profiling-engineer modifies (targeted optimizations)
- `benchmarks/` -- shared with benchmark-engineer

## 3) Step-by-step task plan (see "Tighten the plan" below)

## 4) Benchmark plan + what "success" looks like

**Benchmark plan:**
1. Capture baseline: `go test -bench=. -benchmem -count=5 ./... > benchmarks/baseline.txt`
2. Run profiling session via `scripts/profile.sh`
3. Analyze profiles: `go tool pprof -top profiles/cpu-gateway.pb.gz`, `go tool pprof -top profiles/heap-ingester.pb.gz`, etc.
4. Apply optimizations based on findings
5. Capture optimized: `go test -bench=. -benchmem -count=5 ./... > benchmarks/optimized.txt`
6. Compare: `benchstat benchmarks/baseline.txt benchmarks/optimized.txt`

**Success looks like:**
- At least one optimization shows >= 20% reduction in ns/op or allocs/op with p < 0.05
- All existing tests pass (unit, integration, chaos)
- Race detector passes: `go test -race ./...`
- `docs/performance/report.md` contains a clear narrative: "profiled with pprof, found X, applied Y, measured Z improvement"

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Enable mutex/block profiling and create profile capture script
- **Outcome:** pprof endpoints return mutex and block profiles; `scripts/profile.sh` captures all profile types from gateway and ingester under load.
- **Files to create/modify:**
  - Modify: `internal/observability/pprof.go` (add `runtime.SetMutexProfileFraction(5)`, `runtime.SetBlockProfileRate(1)`)
  - Create: `scripts/profile.sh`
  - Create: `profiles/.gitkeep` (and add `profiles/*.pb.gz` to `.gitignore`)
- **Exact verification commands:**
  ```bash
  go test ./internal/observability/...
  go vet ./internal/observability/...
  # Manual: start gateway, curl http://localhost:9100/debug/pprof/mutex → non-empty response
  ```
- **Suggested commit message:** `feat: enable mutex/block profiling and add profile capture script`

### Task 2: Capture baseline benchmarks and run profiling session
- **Outcome:** `benchmarks/baseline.txt` contains benchmark results for all hot paths. `profiles/` contains CPU, heap, mutex, and block profiles from both gateway and ingester captured under 5000 events/sec load.
- **Files to create/modify:**
  - Create: `benchmarks/baseline.txt` (output of `go test -bench=. -benchmem -count=5 ./...`)
  - Populate: `profiles/*.pb.gz` (via `scripts/profile.sh`)
- **Exact verification commands:**
  ```bash
  test -f benchmarks/baseline.txt && echo "baseline exists"
  ls profiles/*.pb.gz | wc -l  # should be >= 6 files
  go tool pprof -top profiles/cpu-gateway.pb.gz | head -20
  ```
- **Suggested commit message:** `perf: capture baseline benchmarks and pprof profiles under load`

### Task 3: Analyze profiles and document findings
- **Outcome:** `docs/performance/report.md` contains analysis of all profiles -- top CPU functions, top allocation sites, mutex contention points, blocking points, with specific function names, line numbers, and percentage of total.
- **Files to create/modify:**
  - Create: `docs/performance/report.md`
- **Exact verification commands:**
  ```bash
  test -f docs/performance/report.md && echo "report exists"
  # Manual review: report should list specific function names and percentages
  ```
- **Suggested commit message:** `docs: add pprof analysis findings to performance report`

### Task 4: Apply targeted optimizations based on profile findings
- **Outcome:** Top 2-3 hotspots identified in Task 3 are optimized. Likely candidates:
  - `sync.Pool` for `bytes.Buffer` in flusher JSON marshaling
  - `strconv`-based key in `Batcher.Add` instead of `fmt.Sprintf`
  - Buffer pooling for `io.ReadAll` + re-marshal in gateway handler
- **Files to create/modify:**
  - Modify: `internal/ingester/flusher.go` (buffer pool for marshal)
  - Modify: `internal/ingester/batcher.go` (optimized key generation)
  - Modify: `internal/gateway/handler.go` (reduce double-marshal allocations)
- **Exact verification commands:**
  ```bash
  go test ./...
  go test -race ./...
  go vet ./...
  ```
- **Suggested commit message:** `perf: optimize hot paths based on pprof findings (pool buffers, reduce allocs)`

### Task 5: Re-benchmark, compare with benchstat, and complete report
- **Outcome:** `benchmarks/optimized.txt` captured. benchstat comparison shows statistically significant improvements. `docs/performance/report.md` updated with before/after numbers and the optimization narrative.
- **Files to create/modify:**
  - Create: `benchmarks/optimized.txt`
  - Modify: `docs/performance/report.md` (add before/after comparison, optimization narrative)
- **Exact verification commands:**
  ```bash
  benchstat benchmarks/baseline.txt benchmarks/optimized.txt
  go test ./...
  go test -race ./...
  ```
- **Suggested commit message:** `perf: document optimization results with benchstat comparison`

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From Profiling Engineer

### Coding style rules
- Never use `json.Marshal` in hot paths without checking if a `sync.Pool` buffer would be appropriate.
- Prefer `strconv.AppendInt` / `strconv.Itoa` over `fmt.Sprintf` for simple numeric-to-string conversions in hot paths.
- When adding `sync.Pool`, always reset the pooled object before returning it to the pool (e.g., `buf.Reset()` for `bytes.Buffer`).

### Dev commands
```bash
# Capture pprof profiles under load
./scripts/profile.sh

# Analyze a CPU profile
go tool pprof -top profiles/cpu-gateway.pb.gz
go tool pprof -http=:8080 profiles/cpu-gateway.pb.gz  # web UI with flame graph

# Run benchmarks
go test -bench=. -benchmem -count=5 ./... | tee benchmarks/optimized.txt

# Compare benchmarks
benchstat benchmarks/baseline.txt benchmarks/optimized.txt

# Install benchstat
go install golang.org/x/perf/cmd/benchstat@latest
```

### Before you commit checklist
- [ ] `go test ./...` passes
- [ ] `go test -race ./...` passes (especially after any lock changes)
- [ ] `go vet ./...` clean
- [ ] If you changed a hot path, re-run the relevant benchmark and verify no regression
- [ ] If you added a `sync.Pool`, verify the pooled object is Reset before Put

### Guardrails
- Do NOT enable `runtime.SetMutexProfileFraction` or `runtime.SetBlockProfileRate` in production builds (small but non-zero overhead). Gate behind a flag or only enable when admin server starts.
- Profile binary data (`profiles/*.pb.gz`) should be gitignored -- they are large and machine-specific.
- Always profile THEN optimize. Never optimize based on assumptions about what is slow.

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

### Flow / architecture explanation
- pprof endpoints are registered on the admin mux alongside Prometheus metrics
- Mutex and block profiling are enabled at startup to capture contention data
- Profiling sessions are run with the system under sustained load (5000+ events/sec, 5 tenants)

### Key engineering decisions + tradeoffs
- Profiled native Go processes (not containerized) for accurate CPU profiles; tradeoff is missing container-specific overhead
- Used `sync.Pool` for buffer reuse in JSON marshaling -- idiomatic, GC-friendly, no external deps
- Applied stripe-locking to batcher only if mutex profile showed significant contention (data-driven, not speculative)
- Every optimization was validated with benchstat (statistical significance, not anecdotal)

### Limits of MVP + next steps
- Profiles are captured manually via script; a future step could integrate with continuous profiling
- Optimizations target single-broker, single-node setup; multi-broker contention patterns may differ
- JSON serialization is still used; switching to protobuf would be the next major optimization
- No flamegraph diffing automation yet (could add `pprof -diff_base`)

### How to run locally + how to validate
- Start infrastructure: `docker compose up -d`
- Start gateway and ingester natively (not in containers)
- Run `./scripts/profile.sh` to capture profiles
- View flame graphs: `go tool pprof -http=:8080 profiles/cpu-gateway.pb.gz`
- Run benchmarks: `go test -bench=. -benchmem -count=5 ./...`
- Compare before/after: `benchstat benchmarks/baseline.txt benchmarks/optimized.txt`

---

## READY FOR APPROVAL
