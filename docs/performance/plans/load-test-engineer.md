# Load Test Engineer Plan

## You are in PLAN MODE.

### Project
I want to do a **systematic load testing framework** for the ingestion engine.

**Goal:** build a **load testing toolkit** in which we **measure throughput ceilings, latency percentiles, and failure thresholds across multiple load profiles, producing machine-readable results for comparison**.

### Role + Scope
- **Role:** Load Test Engineer
- **Scope:** Own the load generator enhancements (latency tracking, warmup, ramp-up, JSON output), the `Report` struct and output formatting, load profile configs, and the orchestration script. Do NOT own Go benchmarks (benchmark-engineer), pprof profiling (profiling-engineer), or any core pipeline code.
- **File you will write:** `/docs/performance/plans/load-test-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: Load generator tracks per-request latency and computes P50, P90, P99, and Max percentiles, excluding a configurable warmup period from statistics.
- FR2: Load generator supports ramp-up mode that gradually increases the send rate over the first 10% of the test duration.
- FR3: A `Report` struct captures total/success/failed events, throughput, latency percentiles, error breakdown by HTTP status code, and bytes sent. Reports are printable as human-readable tables and serializable to JSON.
- FR4: YAML-based load profiles define test parameters (tenants, rate, duration, payload size, ramp-up, warmup) and can be loaded via `--profile` flag.
- FR5: An orchestration script runs all profiles sequentially, captures JSON results, and prints a summary comparison table.
- Tests required: unit tests for Report formatting, warmup filtering, ramp-up rate calculation, and profile YAML loading.
- Metrics required: N/A (loadgen is a client tool; metrics are on the server side).

## Non-Functional Requirements
- Language/runtime: Go (loadgen is part of the existing Go module)
- Local dev: docker-compose brings up Kafka + gateway + ingester; loadgen runs against the gateway
- Observability: loadgen itself does not expose `/metrics`; it produces JSON reports consumed by humans and scripts
- Safety: graceful shutdown on SIGINT preserves partial results; invalid profiles fail fast with clear errors
- Documentation: CLAUDE.md + EXPLAIN.md contributions (proposed below)
- Performance: the loadgen itself must not be the bottleneck; HTTP client connection pooling and efficient latency tracking are required

---

## Assumptions / System Model
- Deployment environment: local docker-compose; loadgen runs on the host against `localhost:8080`
- Failure modes: gateway unreachable (connection refused), gateway rate-limiting (429), gateway overloaded (503), request timeout
- Delivery guarantees: loadgen is fire-and-forget; it does not verify end-to-end delivery (that is the integration test engineer's concern)
- Multi-tenancy: loadgen distributes events across N configurable tenants; each tenant gets roughly `rate/tenants` events/sec

---

## Data Model (as relevant to your role)

### Report struct (enhanced)
```go
type Report struct {
    TotalEvents   int64
    SuccessEvents int64
    FailedEvents  int64
    Duration      time.Duration
    EventsPerSec  float64
    LatencyP50    time.Duration
    LatencyP90    time.Duration
    LatencyP99    time.Duration
    LatencyMax    time.Duration
    ErrorBreakdown map[int]int  // HTTP status code -> count
    BytesSent     int64
}
```
- Validation: TotalEvents == SuccessEvents + FailedEvents
- No persistence needed; reports are ephemeral JSON files in `benchmarks/`

### LoadProfile struct (new)
```go
type LoadProfile struct {
    Name        string        `yaml:"name"`
    Tenants     int           `yaml:"tenants"`
    Rate        float64       `yaml:"rate"`
    Duration    string        `yaml:"duration"`  // parsed to time.Duration
    PayloadSize int           `yaml:"payload_size"`
    RampUp      bool          `yaml:"ramp_up"`
    WarmupSecs  int           `yaml:"warmup_secs"`
}
```

---

## APIs (as relevant to your role)

### CLI interface (enhanced `loadgen` command)
```
go run . loadgen \
  --target http://localhost:8080 \
  --tenants 5 \
  --rate 500 \
  --duration 60s \
  --payload-size 100 \
  --ramp-up \
  --warmup 5s \
  --output benchmarks/result.json \
  --profile configs/load-profiles/baseline.yaml
```

New flags:
- `--ramp-up` (bool): gradually increase rate over first 10% of duration
- `--warmup` (duration): exclude first N seconds from latency statistics (default 5s)
- `--output` (string): path to write JSON report (empty = no file output)
- `--profile` (string): path to YAML profile that sets all other flags

When `--profile` is provided, YAML values are used as defaults; explicit flags override them.

### Report output methods
- `PrintSummary()` — writes formatted table to stdout
- `WriteJSON(path string) error` — writes JSON to file

---

## Architecture / Component Boundaries

### Components touched
1. **`internal/loadgen/generator.go`** — enhanced Generator with warmup, ramp-up, status code tracking, bytes counting
2. **`internal/loadgen/report.go`** — new file; Report struct, PrintSummary, WriteJSON
3. **`internal/loadgen/profile.go`** — new file; LoadProfile struct, YAML loading
4. **`cmd/loadgen.go`** — enhanced cobra command with new flags, profile loading
5. **`configs/load-profiles/*.yaml`** — 4 load profile configs
6. **`scripts/load-test.sh`** — orchestration script

### Concurrency model
- One goroutine per tenant (unchanged)
- Shared rate limiter for aggregate rate control (unchanged)
- For ramp-up: the rate limiter's limit is adjusted periodically (every 100ms) during the ramp-up window using `limiter.SetLimit()`
- Latency tracking: each worker appends to a shared slice under mutex (existing pattern). This is acceptable because the append is O(1) amortized and the lock is held for nanoseconds. At very high rates (>50k/s), we could switch to per-worker slices merged at the end, but this is unlikely to be the bottleneck.

### Warmup implementation
- Record the start time of the test
- During the warmup period, still send events (and count them in TotalEvents), but do not append latencies to the tracking slice
- This means latency percentiles reflect steady-state behavior, not cold-start

### Ramp-up implementation
- Compute `rampDuration = totalDuration * 0.10`
- Start a goroutine that, every 100ms during the ramp period, calls `limiter.SetLimit(rate.Limit(currentRate))` where `currentRate` linearly interpolates from 0 to `targetRate`
- After ramp-up completes, set the final rate and stop adjusting

---

## Correctness Invariants
1. **TotalEvents == SuccessEvents + FailedEvents** — enforced by counting logic
2. **Warmup events are counted but excluded from latency stats** — verified by unit test
3. **Ramp-up reaches target rate by 10% of duration** — verified by unit test with mock time
4. **Profile YAML loading produces identical config to equivalent CLI flags** — verified by unit test
5. **JSON report round-trips correctly** — write then read back and compare
6. **Error breakdown sums to FailedEvents** — enforced by counting logic

---

## Tests
### Unit tests
- `internal/loadgen/report_test.go`:
  - `TestPrintSummary` — captures stdout, verifies table format
  - `TestWriteJSON` — writes to temp file, reads back, verifies fields
  - `TestReportJSONRoundTrip` — marshal/unmarshal equality
- `internal/loadgen/profile_test.go`:
  - `TestLoadProfile` — loads a YAML file, verifies all fields parsed correctly
  - `TestLoadProfileInvalid` — bad YAML returns clear error
  - `TestLoadProfileDefaults` — missing optional fields get sensible defaults
- `internal/loadgen/generator_test.go` (expand existing):
  - `TestWarmupExcludesLatencies` — run with 1s warmup, 2s total; verify latency count < total event count
  - `TestRampUpReachesTargetRate` — run with ramp-up, verify final rate approximates target
  - `TestErrorBreakdown` — use test server that returns mixed 200/429/503; verify breakdown map

### Commands
```bash
go test ./internal/loadgen/... -v -count=1
go test ./internal/loadgen/... -race -count=1
```

### Integration / load tests (manual, require docker-compose)
- Run `scripts/load-test.sh` with full stack up
- Verify JSON output files are created in `benchmarks/`
- Verify human-readable summary is printed

---

## Benchmarks + "Success"

### What to measure
- Throughput ceiling: maximum events/sec before p99 > 10ms
- Rate-limit threshold: rate at which 429s start appearing
- Ingester lag threshold: rate at which Kafka consumer lag grows unbounded
- Latency distribution under each load profile

### Target success criteria
- Baseline (500 events/sec): p99 < 5ms, 0 errors
- High throughput (5000 events/sec): p99 < 10ms, < 1% errors
- Many tenants (50 tenants, 1000 events/sec): p99 < 10ms, no tenant starvation
- Burst (ramp to 10000 events/sec): system recovers after burst, no data loss

### Commands
```bash
./scripts/load-test.sh                      # run all profiles
cat benchmarks/load-test-baseline-*.json    # inspect individual results
```

---

## Engineering Decisions & Tradeoffs

### Decision 1: Simple sorted-slice percentiles vs HDR histogram library
- **Decision:** Use simple sorted-slice percentile calculation (existing pattern in codebase)
- **Alternatives considered:** HDR histogram library (`github.com/HdrHistogram/hdrhistogram-go`) — provides O(1) percentile queries, bounded memory, and merge support
- **Why:** The existing codebase already uses sorted-slice percentiles. For test durations of 30-60s at rates up to 10k/s, we accumulate at most ~600k latency samples (~5MB). This fits comfortably in memory and sorting is fast. Adding an external dependency for marginal benefit is not worth the complexity. If we scale to millions of samples, we can switch later.
- **Tradeoff acknowledged:** O(n log n) sort at the end of each test vs O(1) HDR lookup. Acceptable for our scale.

### Decision 2: Separate Report file vs inline in generator.go
- **Decision:** Extract Report into its own file (`report.go`) with PrintSummary and WriteJSON methods
- **Alternatives considered:** Keep Report inline in generator.go (fewer files, simpler)
- **Why:** The Report struct is growing significantly (new fields, two output methods). Separating it follows the existing codebase pattern where each concern gets its own file (e.g., `handler.go` vs `producer.go`). It also makes the report logic independently testable.
- **Tradeoff acknowledged:** One more file to navigate. Acceptable given the clear separation of concerns.

### Decision 3: Rate limiter SetLimit for ramp-up vs custom ramp logic
- **Decision:** Use `golang.org/x/time/rate` Limiter's `SetLimit()` method to dynamically adjust the rate during ramp-up
- **Alternatives considered:** Custom token bucket implementation; separate ramp-up goroutine pool that grows over time
- **Why:** `SetLimit()` is designed for exactly this use case — dynamically changing the rate. It is thread-safe and requires no additional synchronization. The existing codebase already uses this rate limiter.
- **Tradeoff acknowledged:** Rate transitions are not perfectly smooth (depends on 100ms tick interval), but this is acceptable for load testing purposes.

---

## Risks & Mitigations

### Risk 1: Loadgen itself is the bottleneck at high rates
- **Risk:** At 10,000+ events/sec, the loadgen's HTTP client, JSON marshaling, or latency tracking mutex could become the bottleneck, making throughput measurements inaccurate.
- **Impact:** Measured throughput ceiling reflects loadgen limits, not gateway limits.
- **Mitigation:** Use `http.Client` with connection pooling (`MaxIdleConnsPerHost: 100`). If mutex contention appears, switch to per-worker latency slices merged at end. Profile loadgen itself with `-cpuprofile` flag if suspicious.
- **Validation time:** ~10 minutes (run at 10k/s, check loadgen CPU usage)

### Risk 2: Warmup period too short or too long
- **Risk:** Default 5s warmup may not be enough for JIT/GC warmup, or may be too long for short tests (wasting test time).
- **Impact:** Latency stats include cold-start outliers, or too few samples for percentiles.
- **Mitigation:** Make warmup configurable via `--warmup` flag and YAML profile. Document recommended warmup (5s for 60s tests, 2s for 30s tests). Validate that warmup < 50% of total duration.
- **Validation time:** ~5 minutes (run with different warmup values, compare percentiles)

### Risk 3: YAML profile schema drift
- **Risk:** As loadgen gains more flags, YAML profiles may become out of sync with CLI flags, leading to confusing behavior.
- **Impact:** Users specify a profile that silently ignores fields, producing unexpected results.
- **Mitigation:** LoadProfile struct uses `yaml:"field"` tags with strict unmarshaling (`yaml.Decoder` with `KnownFields(true)`). Unit test validates that every LoadGenConfig field has a corresponding profile field.
- **Validation time:** ~5 minutes (unit test)

### Risk 4: Docker-compose flakiness in load test script
- **Risk:** The load-test.sh script depends on docker-compose services being healthy. Kafka or the gateway may not be ready when the loadgen starts.
- **Impact:** First profile run fails with connection refused; script exits early.
- **Mitigation:** Script includes a health-check loop (poll `/ready` endpoint with retries, max 30s wait). On timeout, exit with clear error message.
- **Validation time:** ~5 minutes (run script, verify health check works)

---

# Recommended API surface

### Functions/methods
1. `Report.PrintSummary()` — prints formatted table to stdout
2. `Report.WriteJSON(path string) error` — writes JSON report to file
3. `LoadProfile(path string) (*LoadGenConfig, error)` — parses YAML profile into config
4. Enhanced `Generator.Run(ctx)` — now tracks status codes, bytes sent, respects warmup/ramp-up
5. Updated `cmd/loadgen.go` `runLoadgen()` — new flags, profile loading, JSON output

### Exact behavior
- `PrintSummary()` outputs a fixed-width table with labels and values, suitable for terminal display
- `WriteJSON()` marshals with `json.MarshalIndent` for readability, creates parent directories if needed
- `LoadProfile()` reads YAML, validates required fields, returns error on unknown fields
- `Generator.Run()` returns enhanced Report with all new fields populated

---

# Folder structure

```
internal/loadgen/
  generator.go          # existing — enhanced with warmup, ramp-up, status tracking
  generator_test.go     # existing — expanded with new test cases
  report.go             # NEW — Report struct, PrintSummary, WriteJSON
  report_test.go        # NEW — unit tests for report formatting and JSON
  profile.go            # NEW — LoadProfile struct, YAML loading
  profile_test.go       # NEW — unit tests for profile loading

cmd/
  loadgen.go            # existing — enhanced with --ramp-up, --warmup, --output, --profile flags

configs/load-profiles/
  baseline.yaml         # NEW — 5 tenants, 500 events/sec, 60s
  high-throughput.yaml  # NEW — 5 tenants, 5000 events/sec, 60s
  many-tenants.yaml     # NEW — 50 tenants, 1000 events/sec, 60s
  burst.yaml            # NEW — 5 tenants, ramp 0-10000, 30s

scripts/
  load-test.sh          # NEW — orchestration script

benchmarks/             # NEW directory — JSON results stored here (gitignored)
```

---

# Step-by-step task plan

## Task 1: Extract and enhance Report struct
- **Outcome:** `Report` struct lives in its own file with PrintSummary and WriteJSON methods. Fields expanded to include SuccessEvents, FailedEvents, LatencyP90, LatencyMax, ErrorBreakdown, BytesSent.
- **Files to create/modify:**
  - Create `internal/loadgen/report.go`
  - Create `internal/loadgen/report_test.go`
  - Modify `internal/loadgen/generator.go` (remove old Report struct, import new one)
- **Verification:**
  ```bash
  go test ./internal/loadgen/... -v -run TestReport
  go vet ./internal/loadgen/...
  ```
- **Commit message:** `feat(loadgen): extract Report struct with PrintSummary and WriteJSON`

## Task 2: Add warmup and ramp-up to Generator
- **Outcome:** Generator supports warmup period (latencies excluded during warmup) and ramp-up mode (rate linearly increases over first 10% of duration). Status codes tracked in ErrorBreakdown. BytesSent counted.
- **Files to create/modify:**
  - Modify `internal/loadgen/generator.go`
  - Expand `internal/loadgen/generator_test.go`
- **Verification:**
  ```bash
  go test ./internal/loadgen/... -v -run TestWarmup
  go test ./internal/loadgen/... -v -run TestRampUp
  go test ./internal/loadgen/... -v -run TestErrorBreakdown
  go test ./internal/loadgen/... -race -count=1
  ```
- **Commit message:** `feat(loadgen): add warmup period, ramp-up mode, and status code tracking`

## Task 3: Add YAML profile loading
- **Outcome:** LoadProfile struct and loading function. Profiles define all loadgen parameters in YAML. Strict parsing rejects unknown fields.
- **Files to create/modify:**
  - Create `internal/loadgen/profile.go`
  - Create `internal/loadgen/profile_test.go`
- **Verification:**
  ```bash
  go test ./internal/loadgen/... -v -run TestLoadProfile
  go vet ./internal/loadgen/...
  ```
- **Commit message:** `feat(loadgen): add YAML load profile support`

## Task 4: Enhance CLI with new flags and profile support
- **Outcome:** `loadgen` command supports `--ramp-up`, `--warmup`, `--output`, and `--profile` flags. Profile values are used as defaults; explicit flags override.
- **Files to create/modify:**
  - Modify `cmd/loadgen.go`
- **Verification:**
  ```bash
  go build . && ./ingestion-engine loadgen --help
  go vet ./cmd/...
  ```
- **Commit message:** `feat(cmd): add --ramp-up, --warmup, --output, --profile flags to loadgen`

## Task 5: Create load profile configs
- **Outcome:** Four YAML load profiles covering baseline, high-throughput, many-tenants, and burst scenarios.
- **Files to create/modify:**
  - Create `configs/load-profiles/baseline.yaml`
  - Create `configs/load-profiles/high-throughput.yaml`
  - Create `configs/load-profiles/many-tenants.yaml`
  - Create `configs/load-profiles/burst.yaml`
- **Verification:**
  ```bash
  go test ./internal/loadgen/... -v -run TestLoadProfile
  ```
- **Commit message:** `feat(loadgen): add baseline, high-throughput, many-tenants, and burst profiles`

## Task 6: Create orchestration script and benchmarks directory
- **Outcome:** `scripts/load-test.sh` runs all profiles sequentially, stores JSON results in `benchmarks/`, prints summary table. Includes health-check loop for docker-compose readiness.
- **Files to create/modify:**
  - Create `scripts/load-test.sh`
  - Create `benchmarks/.gitkeep`
  - Add `benchmarks/*.json` to `.gitignore`
- **Verification:**
  ```bash
  bash -n scripts/load-test.sh   # syntax check
  shellcheck scripts/load-test.sh  # lint (if available)
  ```
- **Commit message:** `feat(scripts): add load-test.sh orchestration script`

---

# CLAUDE.md contributions (proposed content, do NOT write the file)

## From Load Test Engineer
### Coding style
- Load profiles are YAML files in `configs/load-profiles/`. Use strict YAML parsing (`KnownFields(true)`) to catch typos.
- Report JSON files go in `benchmarks/` (gitignored except `.gitkeep`).
- Loadgen flags follow the pattern `--kebab-case`. YAML profile keys use `snake_case`.

### Dev commands
```bash
# Run load generator manually
go run . loadgen --target http://localhost:8080 --tenants 5 --rate 500 --duration 30s

# Run with a profile
go run . loadgen --profile configs/load-profiles/baseline.yaml --output benchmarks/result.json

# Run all load profiles (requires docker-compose stack running)
./scripts/load-test.sh

# Run loadgen unit tests
go test ./internal/loadgen/... -v -count=1 -race
```

### Before you commit checklist
- [ ] `go test ./internal/loadgen/... -race` passes
- [ ] `go vet ./internal/loadgen/...` clean
- [ ] New flags have help text in cobra command
- [ ] YAML profiles parse without error: `go test ./internal/loadgen/... -run TestLoadProfile`
- [ ] Report JSON is valid: check with `jq . < benchmarks/result.json`

### Guardrails
- Do not add external histogram libraries; use the existing sorted-slice approach.
- The loadgen must not import any internal packages beyond `internal/loadgen/`. It is a standalone client tool.
- Always use `context.Context` for cancellation in the generator.

---

# EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- The load generator is a multi-tenant HTTP client that sends synthetic events to the gateway at a configurable rate.
- One goroutine per tenant shares a global rate limiter for aggregate rate control.
- Ramp-up mode linearly increases the rate limiter's limit over the first 10% of the test duration.
- Warmup period (default 5s) excludes early latency samples from percentile calculations.

### Key Engineering Decisions + Tradeoffs
- Sorted-slice percentiles over HDR histogram: simpler, no dependency, acceptable at our scale (~600k samples max).
- Rate limiter `SetLimit()` for ramp-up: leverages existing `golang.org/x/time/rate` API, no custom implementation needed.
- YAML profiles with strict parsing: prevents silent misconfiguration, catches typos early.

### Limits of MVP + Next Steps
- Loadgen sends one event per HTTP request; batching multiple events per request would better simulate real workloads.
- No distributed load generation; single-host loadgen may bottleneck above 50k events/sec.
- No automated regression detection; future work could compare JSON results across runs with `benchstat`-like tooling.

### How to Run Locally + Validate
```bash
docker compose up -d                          # start infrastructure
go run . gateway &                            # start gateway
go run . ingester &                           # start ingester
./scripts/load-test.sh                        # run all load profiles
cat benchmarks/load-test-baseline-*.json      # inspect results
```

---

## READY FOR APPROVAL
