# Master Plan: Integration Engineer

## You are in PLAN MODE.

### Project
I want to build **production-readiness features** for the ingestion pipeline.

**Goal:** build the **S3/MinIO storage backend** and **load generator** so the pipeline can store to S3-compatible storage and be stress-tested with realistic multi-tenant traffic.

### Role + Scope
- **Role:** Integration Engineer
- **Scope:** S3Store implementation (ObjectStore interface for MinIO/S3), load generator CLI + library, MinIO in docker-compose, S3-related config fields. I explicitly do NOT own: rate limiting, backpressure, Prometheus metrics, Grafana dashboards, or observability middleware.
- **File I will write:** `/docs/production/plans/integration-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: `S3Store` implements the existing `ObjectStore` interface (`Put`, `Get`, `List`, `Delete`, `Exists`) using `aws-sdk-go-v2` against MinIO/S3.
- FR2: MinIO runs in docker-compose alongside Kafka, accessible on ports 9000 (API) and 9001 (console).
- FR3: Config supports `--storage-backend` flag (`filesystem` or `s3`) and S3-specific fields (endpoint, bucket, access key, secret key, region).
- FR4: Load generator sends configurable multi-tenant traffic to the gateway and reports latency/throughput stats.
- FR5: Tests required: unit tests for S3Store (against MinIO in docker-compose), unit tests for load generator report computation, integration smoke test for loadgen against the gateway.
- FR6: Metrics required: N/A -- observability-engineer owns Prometheus/Grafana instrumentation.

## Non-Functional Requirements
- Language/runtime: Go 1.25+
- Local dev: `docker compose up -d` brings up MinIO alongside Kafka
- Observability: N/A for this role (observability-engineer scope)
- Safety: S3Store must handle missing buckets (auto-create), network errors (return wrapped errors), and context cancellation. Load generator must respect context cancellation and graceful shutdown.
- Documentation: CLAUDE.md + EXPLAIN.md contributions proposed below.
- Performance: Load generator must sustain 500+ events/sec with sub-100ms p99 to the local gateway.

---

## Assumptions / System Model
- Deployment environment: local docker-compose; MinIO runs as a single container.
- Failure modes: MinIO unavailable (S3Store returns error, flusher retries), network timeout (context-based cancellation), invalid credentials (fail at startup on bucket check).
- Delivery guarantees: S3Store is a simple put/get layer; at-least-once semantics are owned by the ingester (commit after flush).
- Multi-tenancy: S3Store is tenant-agnostic; tenant isolation is enforced by the key format (`<tenantID>/...`) produced by `storage.BatchKey()`.

---

## Data Model (as relevant to this role)

### S3Store
- Wraps `s3.Client` from `aws-sdk-go-v2`. Keys map directly to S3 object keys.
- No versioning needed; the existing `BatchKey()` function produces unique keys (tenant/date/partition-offset).
- Bucket auto-created on startup if it does not exist.

### Load Generator
- `LoadGenConfig` struct: TargetURL, Tenants (count or list), Rate (events/sec), Duration, PayloadSize (bytes).
- `Report` struct: TotalEvents, Errors, Duration, Throughput (events/sec), P50Latency, P99Latency.
- Events are generated with random labels and values, distributed round-robin across tenants.

---

## APIs (as relevant to this role)

### S3Store (implements `storage.ObjectStore`)
```
Put(ctx, key, data) error        -- s3.PutObject, Content-Type: application/json
Get(ctx, key) ([]byte, error)    -- s3.GetObject, read body, close
List(ctx, prefix) ([]string, error) -- s3.ListObjectsV2, paginate
Delete(ctx, key) error           -- s3.DeleteObject
Exists(ctx, key) (bool, error)   -- s3.HeadObject, check NoSuchKey
```

Constructor:
```
NewS3Store(endpoint, bucket, accessKey, secretKey, region string, usePathStyle bool) (*S3Store, error)
```

### Load Generator CLI
```
go run . loadgen --target http://localhost:8080 --tenants 5 --rate 500 --duration 60s --payload-size 100
```

### Config Additions
New fields on `Config`:
```go
type StorageConfig struct {
    Backend   string // "filesystem" or "s3"
    S3        S3Config
}

type S3Config struct {
    Endpoint     string
    Bucket       string
    AccessKey    string
    SecretKey    string
    Region       string
    UsePathStyle bool
}
```

---

## Architecture / Component Boundaries

### S3Store
- Lives in `internal/storage/s3.go` alongside `filesystem.go`.
- Implements the same `ObjectStore` interface -- the ingester/flusher is unaware of which backend is in use.
- Config selects the backend at startup; the `cmd/ingester.go` wiring creates either `FilesystemStore` or `S3Store`.

### Load Generator
- `internal/loadgen/generator.go` contains the core `Generator` struct and `Run()` method.
- `cmd/loadgen.go` provides the Cobra command with flags.
- Generator uses `golang.org/x/time/rate.Limiter` for rate control.
- One goroutine per tenant, all sharing a single rate limiter for aggregate rate control.
- Each goroutine POSTs JSON arrays of events to `POST /api/v1/push` with `X-Scope-OrgID` header.
- Latencies collected in a slice, sorted for percentile computation at the end.

### Concurrency Model
- S3Store: no internal concurrency; relies on `s3.Client` being thread-safe.
- Load generator: one goroutine per tenant, coordinated via `context.Context` and `sync.WaitGroup`.

### Config Propagation
- New fields added to `config.Config` struct.
- Env vars: `INGEST_STORAGE_BACKEND`, `INGEST_S3_ENDPOINT`, `INGEST_S3_BUCKET`, `INGEST_S3_ACCESS_KEY`, `INGEST_S3_SECRET_KEY`, `INGEST_S3_REGION`.
- CLI flags on `ingester` command: `--storage-backend`, `--s3-endpoint`, `--s3-bucket`, `--s3-access-key`, `--s3-secret-key`, `--s3-region`.

---

## Correctness Invariants
1. S3Store must satisfy the same contract as FilesystemStore: `Put` then `Get` returns identical bytes.
2. S3Store `List` with prefix must return only matching keys (no false positives).
3. S3Store `Delete` is idempotent (deleting a non-existent key returns nil).
4. S3Store `Exists` returns false for non-existent keys without error.
5. Load generator total events sent equals sum of successes + errors.
6. Load generator respects `--duration` and shuts down within 1 second of the deadline.
7. Storage backend selection: `--storage-backend=filesystem` creates `FilesystemStore`, `--storage-backend=s3` creates `S3Store`.

---

## Tests

### Unit Tests
- `internal/storage/s3_test.go`:
  - `TestS3PutGetRoundtrip` -- put data, get it back, compare bytes.
  - `TestS3ListPrefix` -- put multiple keys, list with prefix, verify filtering.
  - `TestS3Delete` -- put, delete, verify Exists returns false.
  - `TestS3ExistsNonExistent` -- Exists on missing key returns (false, nil).
  - `TestS3PutContentType` -- verify Content-Type header is set (via GetObject metadata).
  - These tests require MinIO running (`docker compose up -d minio`). Use build tag `//go:build integration` or skip with `testing.Short()`.

- `internal/loadgen/generator_test.go`:
  - `TestReportPercentiles` -- given known latencies, verify p50/p99 computation.
  - `TestGeneratorContextCancellation` -- cancel context, verify shutdown within 1s.

### Integration Tests
- `TestLoadgenSmokeE2E` (in `internal/loadgen/` or `internal/pipeline/`):
  - Requires full stack (Kafka + gateway).
  - Sends 50 events at low rate, verifies non-zero success count.
  - Use `testing.Short()` skip for CI without infra.

### Commands
- `go test ./internal/storage/ -run TestS3 -v` (requires MinIO)
- `go test ./internal/loadgen/ -v`
- `go test ./... -short` (skips integration tests)

---

## Benchmarks + "Success"

### S3Store
- `BenchmarkS3Put` -- measure put latency for 1KB and 100KB payloads.
- Target: p99 < 50ms for 1KB puts to local MinIO.
- Command: `go test ./internal/storage/ -bench BenchmarkS3 -benchmem`

### Load Generator
- Success criteria: sustain 500 events/sec for 60s against local gateway with < 1% error rate.
- Verification: `go run . loadgen --rate 500 --duration 60s --tenants 5`
- Check output report: Throughput >= 450 events/sec, Errors < 1%, P99 < 200ms.

---

# Recommended API Surface

## S3Store (1 constructor + 5 interface methods)
1. `NewS3Store(endpoint, bucket, accessKey, secretKey, region string, usePathStyle bool) (*S3Store, error)` -- creates client, ensures bucket exists.
2. `Put(ctx, key, data) error` -- `s3.PutObject` with `Content-Type: application/json`.
3. `Get(ctx, key) ([]byte, error)` -- `s3.GetObject`, read + close body.
4. `List(ctx, prefix) ([]string, error)` -- `s3.ListObjectsV2`, paginate, return keys.
5. `Delete(ctx, key) error` -- `s3.DeleteObject`, ignore NoSuchKey.
6. `Exists(ctx, key) (bool, error)` -- `s3.HeadObject`, check NoSuchKey error.

## Load Generator (1 constructor + 1 method + 1 type)
1. `NewGenerator(cfg LoadGenConfig) *Generator`
2. `Run(ctx context.Context) (*Report, error)` -- runs load test, returns report.
3. `Report{TotalEvents, Errors, Duration, Throughput, P50Latency, P99Latency}`

## Config Additions
1. `StorageConfig{Backend, S3}` added to top-level `Config`.
2. `S3Config{Endpoint, Bucket, AccessKey, SecretKey, Region, UsePathStyle}`.
3. `LoadGenConfig{TargetURL, Tenants, Rate, Duration, PayloadSize}`.

---

# Folder Structure

```
internal/
  storage/
    storage.go         -- ObjectStore interface (exists)
    filesystem.go      -- FilesystemStore (exists)
    filesystem_test.go -- (exists)
    s3.go              -- NEW: S3Store implementation
    s3_test.go         -- NEW: S3Store tests (integration, needs MinIO)
  config/
    config.go          -- MODIFY: add StorageConfig, S3Config, LoadGenConfig
    config_test.go     -- MODIFY: add tests for new config fields
  loadgen/
    generator.go       -- NEW: Generator, Report, Run()
    generator_test.go  -- NEW: unit tests for report computation, cancellation
cmd/
  loadgen.go           -- MODIFY: replace stub with real flags + wiring
  ingester.go          -- MODIFY: add --storage-backend flag, S3Store creation
docker-compose.yml     -- MODIFY: add minio service
```

Ownership:
- `internal/storage/s3.go` -- integration-engineer
- `internal/loadgen/` -- integration-engineer
- `cmd/loadgen.go` -- integration-engineer
- Config changes -- integration-engineer (additive fields only)
- docker-compose MinIO -- integration-engineer

---

# Step-by-Step Task Plan (small commits)

See "Tighten the plan" section below for the final 4-7 tasks.

---

# Tighten the plan into 4-7 small tasks (STRICT)

## Task 1: Add MinIO to docker-compose and S3 config fields
- **Outcome:** MinIO container runs alongside Kafka; config struct supports S3 fields with env var parsing.
- **Files to create/modify:**
  - `docker-compose.yml` (add `minio` service)
  - `internal/config/config.go` (add `StorageConfig`, `S3Config`, `LoadGenConfig` structs + env var parsing)
  - `internal/config/config_test.go` (test new config fields)
- **Verification:**
  - `docker compose up -d minio && docker compose ps` (MinIO healthy)
  - `go test ./internal/config/ -v`
  - `go vet ./...`
- **Commit:** `feat: add MinIO to docker-compose and S3/loadgen config fields`

## Task 2: Implement S3Store
- **Outcome:** `S3Store` implements full `ObjectStore` interface using `aws-sdk-go-v2`.
- **Files to create/modify:**
  - `internal/storage/s3.go` (new file)
  - `go.mod` / `go.sum` (add `aws-sdk-go-v2` dependencies)
- **Verification:**
  - `go build ./...`
  - `go vet ./...`
- **Commit:** `feat: implement S3Store for MinIO/S3 object storage`

## Task 3: S3Store integration tests
- **Outcome:** Full test coverage for S3Store against local MinIO.
- **Files to create/modify:**
  - `internal/storage/s3_test.go` (new file)
- **Verification:**
  - `docker compose up -d minio`
  - `go test ./internal/storage/ -run TestS3 -v` (all pass)
- **Commit:** `test: add S3Store integration tests against MinIO`

## Task 4: Wire S3Store into ingester command
- **Outcome:** `go run . ingester --storage-backend=s3 --s3-endpoint=...` uses S3Store instead of FilesystemStore.
- **Files to create/modify:**
  - `cmd/ingester.go` (add flags, backend selection logic)
- **Verification:**
  - `go build ./...`
  - `go run . ingester --help` (shows new flags)
  - `go vet ./...`
- **Commit:** `feat: wire S3Store selection into ingester CLI`

## Task 5: Implement load generator library
- **Outcome:** `internal/loadgen/generator.go` with Generator, Report, and Run() method.
- **Files to create/modify:**
  - `internal/loadgen/generator.go` (new file)
  - `internal/loadgen/generator_test.go` (new file)
- **Verification:**
  - `go test ./internal/loadgen/ -v`
  - `go vet ./...`
- **Commit:** `feat: implement load generator with rate limiting and percentile reporting`

## Task 6: Wire load generator into CLI
- **Outcome:** `go run . loadgen --target ... --tenants 5 --rate 500 --duration 30s` sends traffic and prints report.
- **Files to create/modify:**
  - `cmd/loadgen.go` (replace stub with flags + Generator wiring)
- **Verification:**
  - `go build ./...`
  - `go run . loadgen --help` (shows all flags)
  - Manual: `docker compose up -d && go run . gateway & go run . loadgen --duration 5s` (prints report)
- **Commit:** `feat: wire load generator into loadgen CLI command`

---

# Top 3 Risks/Unknowns

1. **aws-sdk-go-v2 custom endpoint configuration for MinIO** -- The SDK's endpoint resolver API has changed across versions. Risk: wrong API call leads to connection errors.
   - Validate in <15 min: write a 20-line Go program that creates an S3 client pointing to `localhost:9000` with path-style, calls `ListBuckets`, and prints results. Run against `docker compose up -d minio`.

2. **MinIO bucket auto-creation timing** -- MinIO might not be ready when `NewS3Store` tries to create the bucket on startup.
   - Validate in <15 min: `docker compose up -d minio && sleep 2 && curl -s http://localhost:9000/minio/health/live` to verify health endpoint. Add a startup readiness check or retry in `NewS3Store`.

3. **Load generator rate accuracy under high concurrency** -- `rate.Limiter` shared across goroutines might have contention, causing actual rate to deviate from target.
   - Validate in <15 min: write a bench test that creates 5 goroutines sharing a `rate.NewLimiter(500, 50)`, count events over 5 seconds, verify within 5% of 2500 expected.

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From Integration Engineer
- **Coding style rules:**
  - S3Store must wrap all AWS errors with `fmt.Errorf("s3: <operation>: %w", err)` for consistent error context.
  - Load generator must use `context.Context` for cancellation; never use bare `time.Sleep` for shutdown.
  - Use `testing.Short()` to skip tests that require external services (MinIO, Kafka, gateway).

- **Dev commands:**
  - `docker compose up -d minio` -- start MinIO for S3 tests
  - `go test ./internal/storage/ -run TestS3 -v` -- run S3 integration tests
  - `go test ./internal/loadgen/ -v` -- run load generator unit tests
  - `go run . loadgen --target http://localhost:8080 --tenants 5 --rate 500 --duration 30s` -- run load test
  - MinIO console: `http://localhost:9001` (minioadmin/minioadmin)

- **Before you commit checklist:**
  - [ ] `go vet ./...` passes
  - [ ] `go test ./... -short` passes (unit tests without external deps)
  - [ ] `go build ./...` succeeds
  - [ ] If touching S3Store: `go test ./internal/storage/ -run TestS3 -v` with MinIO running

- **Guardrails:**
  - S3Store must use `UsePathStyle: true` for MinIO compatibility.
  - Load generator must not create unbounded goroutines; cap at one per tenant.
  - Never hardcode MinIO credentials; always read from config/flags.

---

# EXPLAIN.md contributions (propose outline bullets)

## Flow / Architecture
- S3Store is a drop-in replacement for FilesystemStore, selected at startup via `--storage-backend` flag.
- Both backends implement the same `ObjectStore` interface; the ingester/flusher is backend-agnostic.
- MinIO runs in docker-compose as an S3-compatible endpoint on port 9000.

## Key Engineering Decisions + Tradeoffs
- **aws-sdk-go-v2 with custom endpoint:** allows pointing at MinIO locally and real S3 in production, same code path.
- **Path-style addressing:** required for MinIO; virtual-hosted style does not work with localhost endpoints.
- **Bucket auto-creation:** convenience for dev; in production, buckets should be pre-provisioned.
- **Load generator uses shared rate.Limiter:** simpler than per-tenant limiters; gives accurate aggregate rate control.
- **One goroutine per tenant:** keeps concurrency bounded and predictable; sufficient for 5-50 tenants.

## Limits of MVP + Next Steps
- S3Store does not implement multipart upload (fine for batch sizes under 100MB).
- No retry logic in S3Store itself (the flusher's `FlushWithRetry` handles retries).
- Load generator does not support weighted tenant distribution (round-robin only).
- No TLS/auth for MinIO in docker-compose (dev only).
- Future: add S3Store benchmarks, multipart upload for large batches, configurable tenant weights in loadgen.

## How to Run Locally + Validate
- `docker compose up -d` -- starts Kafka + MinIO
- `go run . ingester --storage-backend=s3 --s3-endpoint=http://localhost:9000 --s3-bucket=ingestion-engine --s3-access-key=minioadmin --s3-secret-key=minioadmin` -- ingester with S3 backend
- `go run . gateway` -- start gateway
- `go run . loadgen --target http://localhost:8080 --tenants 5 --rate 100 --duration 10s` -- send test traffic
- Check MinIO console at `http://localhost:9001` to verify objects are stored

---

## READY FOR APPROVAL
