# Storage Engineer Plan

## You are in PLAN MODE.

### Project
I want to build a **multi-tenant distributed ingestion engine** using Kafka as the durability layer instead of a traditional WAL.

**Goal:** build an **ObjectStore abstraction with filesystem backend** so the ingester can flush batched events to tenant-isolated, crash-safe persistent storage.

### Role + Scope
- **Role:** Storage Engineer
- **Scope:** `internal/storage/` package — ObjectStore interface, filesystem implementation, key format helper. Does NOT own the ingester, batcher, flusher, hash ring, or gateway.
- **File you will write:** `/docs/core-engine/plans/storage-engineer.md`
- **No-touch zones:** All files outside `internal/storage/`; no code in this document.

---

## Functional Requirements
- **FR1:** `ObjectStore` interface with `Put`, `Get`, `List`, `Delete`, `Exists` operations, all accepting `context.Context`.
- **FR2:** `FilesystemStore` implementing `ObjectStore` with atomic writes (write-to-temp + rename) to prevent partial files on crash.
- **FR3:** `BatchKey(tenantID, date, partition, offset)` helper producing the canonical key format `<tenantID>/<YYYY-MM-DD>/<partition>-<offset padded to 10 digits>.json`.
- **FR4:** Tenant isolation — each tenant's data lives under its own prefix; `List("tenant-a/")` never returns tenant-b keys.
- **Tests required:** Unit tests covering Put/Get roundtrip, List with prefix filtering, Delete, Exists, tenant isolation, concurrent writes, and BatchKey formatting.
- **Metrics required:** N/A for this phase (observability-engineer owns metrics). The interface is designed to allow wrapping with instrumented middleware later.

## Non-Functional Requirements
- **Language/runtime:** Go 1.22+
- **Local dev:** No additional docker-compose services needed; filesystem store uses local disk.
- **Observability:** Interface methods accept `context.Context` so spans/metrics can be layered via decorator pattern.
- **Safety:** Atomic writes via temp file + `os.Rename` prevent partial/corrupt files. Errors wrapped with `fmt.Errorf("filesystem: %w", err)` for stack context.
- **Documentation:** CLAUDE.md and EXPLAIN.md contributions proposed below.
- **Performance:** Benchmarks for Put and Get operations (sequential and concurrent).

---

## Assumptions / System Model
- **Deployment environment:** Local filesystem for dev/test; S3/MinIO backend is a future phase (integration-engineer).
- **Failure modes:** Process crash during write (atomic rename protects against partial files), disk full (surfaces as `Put` error), permission errors (surfaced as wrapped os errors).
- **Delivery guarantees:** The ingester commits Kafka offsets after a successful `Put`. If `Put` fails, the offset is not committed, and the batch is re-consumed (at-least-once). Duplicate writes to the same key are idempotent (overwrite).
- **Multi-tenancy:** Tenant isolation is enforced by key prefix. No cross-tenant access is possible through the `List(prefix)` API as long as callers scope prefix to their tenant ID.

---

## Data Model
- **Key format:** `<tenantID>/<YYYY-MM-DD>/<partition>-<offset>.json`
  - `tenantID`: non-empty string, extracted from Kafka record header
  - `date`: UTC date of the batch flush, formatted `2006-01-02`
  - `partition`: Kafka partition (int32), zero-padded to 1 digit minimum (natural formatting)
  - `offset`: Kafka offset (int64), zero-padded to 10 digits for lexicographic ordering
  - Example: `tenant-a/2026-02-28/3-0000000042.json`
- **Data:** Opaque `[]byte` — the storage layer does not interpret contents. The ingester/flusher owns serialization (JSON-encoded `Batch`).
- **Validation:** `Put` rejects empty keys. `BatchKey` rejects empty tenantID.
- **Versioning:** N/A — keys are immutable once written. Overwrites are idempotent.

---

## APIs

### ObjectStore Interface (`internal/storage/storage.go`)

```go
type ObjectStore interface {
    Put(ctx context.Context, key string, data []byte) error
    Get(ctx context.Context, key string) ([]byte, error)
    List(ctx context.Context, prefix string) ([]string, error)
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
}
```

### Key Helper (`internal/storage/storage.go`)

```go
func BatchKey(tenantID string, date time.Time, partition int32, offset int64) string
// Returns: "<tenantID>/2006-01-02/<partition>-<offset padded 10>.json"
```

### FilesystemStore Constructor (`internal/storage/filesystem.go`)

```go
func NewFilesystemStore(baseDir string) (*FilesystemStore, error)
// Creates baseDir if it doesn't exist (os.MkdirAll with 0o755).
// Returns error if baseDir cannot be created.
```

### Error Semantics
- `Get` on missing key: returns error wrapping `os.ErrNotExist`
- `Delete` on missing key: returns nil (idempotent)
- `Exists` on missing key: returns `(false, nil)`
- `List` with no matches: returns `(nil, nil)` (empty slice)
- All errors wrapped with `fmt.Errorf("filesystem: <op>: %w", err)` pattern

---

## Architecture / Component Boundaries

### Components Owned
1. **`ObjectStore` interface** — contract consumed by the ingester's flusher component.
2. **`FilesystemStore`** — concrete implementation for local dev/test.
3. **`BatchKey`** — shared key format helper used by flusher and potentially query paths.

### Component Interaction
```
Ingester (flusher) --calls--> ObjectStore.Put(key, data)
                   --uses---> BatchKey(tenantID, date, partition, offset)
```

### Concurrency Model
- `FilesystemStore` is stateless (no shared mutable state). All state lives on the filesystem.
- Multiple goroutines can call `Put` concurrently for different keys safely (different file paths).
- Concurrent `Put` to the same key is safe due to atomic rename (last writer wins).
- `List` uses `filepath.WalkDir` which is read-only and concurrent-safe with writes.

### Backpressure
- N/A for the storage layer directly. If disk I/O is slow, `Put` blocks, which naturally back-pressures the flusher/batcher upstream.

---

## Correctness Invariants
1. **Atomic writes:** A crash at any point during `Put` must not leave a partial file at the final key path. Enforced by write-to-temp + `os.Rename`.
2. **Tenant isolation:** `List("tenant-a/")` never returns keys with prefix `tenant-b/`. Enforced by `strings.HasPrefix` filtering on walked paths.
3. **Roundtrip fidelity:** `Get(key)` after `Put(key, data)` returns byte-identical data. Tested explicitly.
4. **Idempotent delete:** `Delete` on a non-existent key returns nil. Tested explicitly.
5. **Key format consistency:** `BatchKey` always produces deterministic output for the same inputs. Tested with table-driven cases.

---

## Tests

### Unit Tests (`internal/storage/filesystem_test.go`)

| Test | What it verifies |
|------|-----------------|
| `TestPutGetRoundtrip` | Write data, read it back, assert byte equality |
| `TestGetMissing` | Get non-existent key returns `os.ErrNotExist` |
| `TestListWithPrefix` | Put keys under multiple prefixes, List with one prefix returns only matching keys |
| `TestListEmpty` | List on empty store returns nil/empty slice |
| `TestDelete` | Put then Delete then Get returns not-found |
| `TestDeleteMissing` | Delete non-existent key returns nil |
| `TestExists` | Exists returns true after Put, false after Delete |
| `TestTenantIsolation` | Put keys for tenant-a and tenant-b, List for each returns only their own keys |
| `TestConcurrentWrites` | 10 goroutines writing different keys concurrently, all succeed, all readable |
| `TestBatchKey` | Table-driven: verify key format for various inputs including zero offset, large offset, different dates |
| `TestBatchKeyEmptyTenant` | BatchKey with empty tenantID returns error or panics (design decision: return error) |

### Commands
```bash
go test ./internal/storage/... -v -race -count=1
go test ./internal/storage/... -run TestConcurrentWrites -race -count=10
```

### Integration Tests
- N/A for this role. Integration with the ingester/flusher is tested by the ingester-engineer and integration-test-engineer.

### Property/Fuzz Tests (optional, stretch goal)
- Fuzz `BatchKey` inputs to ensure no panics on arbitrary strings/values.

---

## Benchmarks + "Success"

### What to measure
- `Put` throughput: sequential writes of 1KB, 10KB, 100KB payloads
- `Get` latency: random reads from a pre-populated store
- `List` performance: listing 1000 keys under a prefix

### Target success criteria
- `Put` 10KB payload: < 1ms p50 on SSD
- `Get` 10KB payload: < 500us p50 on SSD
- `List` 1000 keys: < 50ms

### Benchmark commands
```bash
go test ./internal/storage/... -bench . -benchmem -benchtime=5s
```

---

## Recommended API Surface

| Function/Method | Package | Behavior |
|----------------|---------|----------|
| `ObjectStore` interface | `storage` | 5-method CRUD interface for blob storage |
| `BatchKey(tenantID, date, partition, offset) string` | `storage` | Deterministic key generation |
| `NewFilesystemStore(baseDir) (*FilesystemStore, error)` | `storage` | Constructor; creates baseDir |
| `FilesystemStore.Put(ctx, key, data) error` | `storage` | Atomic write via temp+rename |
| `FilesystemStore.Get(ctx, key) ([]byte, error)` | `storage` | Read file; os.ErrNotExist if missing |
| `FilesystemStore.List(ctx, prefix) ([]string, error)` | `storage` | Walk tree, return matching keys |
| `FilesystemStore.Delete(ctx, key) error` | `storage` | Remove file; idempotent |
| `FilesystemStore.Exists(ctx, key) (bool, error)` | `storage` | Stat check |

---

## Folder Structure

```
internal/storage/
  storage.go           -- ObjectStore interface + BatchKey helper
  filesystem.go        -- FilesystemStore implementation
  filesystem_test.go   -- Unit tests + benchmarks
```

Ownership:
- **storage-engineer** owns all three files.
- **ingester-engineer** consumes the `ObjectStore` interface from the flusher.
- **integration-engineer** owns the future `s3.go` backend.

---

## Step-by-step Task Plan

### Task 1: ObjectStore interface + BatchKey helper
- **Outcome:** `internal/storage/storage.go` with the `ObjectStore` interface and `BatchKey` function.
- **Files:** `internal/storage/storage.go`
- **Verify:** `go vet ./internal/storage/...` compiles cleanly.
- **Commit:** `feat(storage): add ObjectStore interface and BatchKey helper`

### Task 2: FilesystemStore — constructor + Put + Get
- **Outcome:** `FilesystemStore` struct with `NewFilesystemStore`, `Put` (atomic write), and `Get`.
- **Files:** `internal/storage/filesystem.go`
- **Verify:** `go build ./internal/storage/...`
- **Commit:** `feat(storage): implement FilesystemStore with atomic Put and Get`

### Task 3: FilesystemStore — List, Delete, Exists
- **Outcome:** Complete the `ObjectStore` interface implementation.
- **Files:** `internal/storage/filesystem.go`
- **Verify:** `go vet ./internal/storage/...`
- **Commit:** `feat(storage): add List, Delete, Exists to FilesystemStore`

### Task 4: Unit tests — core operations
- **Outcome:** Tests for Put/Get roundtrip, Get missing, List with prefix, Delete, Exists, BatchKey.
- **Files:** `internal/storage/filesystem_test.go`
- **Verify:** `go test ./internal/storage/... -v -race -count=1`
- **Commit:** `test(storage): add unit tests for FilesystemStore and BatchKey`

### Task 5: Unit tests — tenant isolation + concurrency
- **Outcome:** Tests for tenant isolation and concurrent writes under `-race`.
- **Files:** `internal/storage/filesystem_test.go`
- **Verify:** `go test ./internal/storage/... -v -race -count=1 -run "TestTenantIsolation|TestConcurrentWrites"`
- **Commit:** `test(storage): add tenant isolation and concurrent write tests`

### Task 6: Benchmarks
- **Outcome:** Benchmarks for Put, Get, List operations with various payload sizes.
- **Files:** `internal/storage/filesystem_test.go`
- **Verify:** `go test ./internal/storage/... -bench . -benchmem -benchtime=3s`
- **Commit:** `bench(storage): add benchmarks for FilesystemStore operations`

---

## Top 3 Risks / Unknowns

1. **Atomic rename across filesystems:** `os.Rename` is not atomic if temp file and target are on different filesystems. **Mitigation:** Write temp file in the same directory as the target (use `os.CreateTemp` in the target's parent directory). Validate in < 5 min by writing a test that checks file content after simulated crash (process kill between write and rename).

2. **`BatchKey` offset padding width:** The spec says 10 digits, but Kafka offsets can theoretically exceed 10 digits (max int64 = 19 digits). **Mitigation:** Use `%010d` formatting which naturally extends beyond 10 digits for large values. Validate with a table-driven test including offset `math.MaxInt64`.

3. **`List` performance at scale:** `filepath.WalkDir` scans the entire subtree. With millions of files per tenant, this could be slow. **Mitigation:** For MVP, this is acceptable (filesystem store is for dev only; S3 backend handles production scale). Validate by benchmarking List with 1000 files — should complete in < 50ms.

---

## CLAUDE.md contributions (proposed, do NOT write the file)

## From Storage Engineer
- **Coding style:**
  - `internal/storage/` uses the `ObjectStore` interface for all blob operations — never use `os.ReadFile`/`os.WriteFile` directly outside this package
  - Atomic writes: always write to temp file in the target directory, then `os.Rename`
  - Errors wrap with `fmt.Errorf("filesystem: <op>: %w", err)` for grep-friendly prefixes
  - Keys use forward slashes as separators (even on Windows); `filepath.FromSlash`/`filepath.ToSlash` for OS conversion

- **Dev commands:**
  - `go test ./internal/storage/... -v -race` — run storage tests with race detector
  - `go test ./internal/storage/... -bench . -benchmem` — run storage benchmarks

- **Before you commit checklist:**
  - [ ] `go test ./internal/storage/... -race -count=1` passes
  - [ ] `go vet ./internal/storage/...` is clean
  - [ ] No hardcoded file paths — always use `BatchKey` helper for key generation
  - [ ] New operations implement `context.Context` cancellation

- **Guardrails:**
  - Never bypass the `ObjectStore` interface — all storage access goes through it
  - Temp files must be created in the same directory as the target to ensure atomic rename
  - `List` returns keys with forward-slash separators regardless of OS

---

## EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- The `ObjectStore` interface provides a 5-method CRUD contract for blob storage
- `FilesystemStore` maps keys to file paths under a base directory (key = relative path)
- Key format: `<tenantID>/<date>/<partition>-<offset>.json` — lexicographically sortable within a partition
- Atomic writes prevent partial files: data goes to a temp file first, then `os.Rename` moves it into place

### Key Engineering Decisions + Tradeoffs
- **Interface-first design:** `ObjectStore` is consumed by the ingester flusher; swapping filesystem for S3 requires zero ingester changes
- **Atomic rename pattern:** Trades an extra syscall per write for crash safety — worth it for data integrity
- **Offset zero-padding (10 digits):** Enables lexicographic listing of batches in offset order within `ls` or `List`
- **Stateless struct:** `FilesystemStore` holds only `baseDir` — no mutexes, no caches, no lifecycle management

### Limits of MVP + Next Steps
- Filesystem store is for local dev only; not suitable for multi-node deployment
- No retention/cleanup — old files accumulate; add TTL-based cleanup in a later phase
- No compression — raw JSON; consider gzip or snappy for production
- S3/MinIO backend (`internal/storage/s3.go`) is planned for the Production phase

### How to Run Locally + Validate
- Tests: `go test ./internal/storage/... -v -race`
- Benchmarks: `go test ./internal/storage/... -bench . -benchmem`
- Manual validation: create a `FilesystemStore` with baseDir `/tmp/ingest-test`, Put some data, inspect the directory tree

---

## READY FOR APPROVAL
