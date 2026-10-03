# Model Engineer Plan — Foundation Phase

---

## You are in PLAN MODE.

### Project
I want to build a **multi-tenant Kafka-backed distributed ingestion service**.

**Goal:** build the **core data types, serialization, and tenant extraction** layer that the gateway and ingester will depend on.

### Role + Scope
- **Role:** Model Engineer
- **Scope:** `internal/model/` (Event, Batch, serialization) and `internal/tenant/` (tenant ID extraction, context helpers). I do NOT own the gateway, ingester, Kafka producer/consumer, config, CLI, or infrastructure.
- **File you will write:** `docs/foundation/plans/model-engineer.md`
- **No-touch zones:** `cmd/`, `internal/gateway/`, `internal/config/`, `docker-compose.yml`, `Makefile`, `main.go`

---

## Functional Requirements
- FR1: `Event` struct holds tenant ID, timestamp, labels, value, and optional payload bytes. Matches the canonical schema from PROJECT.md.
- FR2: `Batch` struct groups events by tenant+partition with metadata (CreatedAt, SizeBytes). `Add()` method appends events and tracks cumulative size.
- FR3: `Event.Validate()` enforces: non-empty TenantID, non-zero timestamp. Labels are optional metadata — empty/nil labels are valid.
- FR4: JSON serialization/deserialization for both `[]Event` and `Batch` via dedicated functions (not raw `json.Marshal` — centralizes format decisions).
- FR5: `ExtractTenantID(r *http.Request)` reads `X-Scope-OrgID` header, returns error if missing/empty.
- FR6: Context helpers (`ContextWithTenantID`, `TenantIDFromContext`) use a private unexported key type to avoid collisions.
- Tests required: unit tests for every exported function and method.
- Metrics required: N/A for this role — no runtime metrics; observability-engineer owns that.

## Non-Functional Requirements
- Language/runtime: Go 1.22+
- Local dev: no infrastructure needed for these packages — pure Go, no external deps
- Observability: N/A — pure data types, no runtime component
- Safety: `Validate()` catches invalid events at the boundary before Kafka production
- Documentation: CLAUDE.md + EXPLAIN.md contributions (see below)
- Performance: serialization benchmarks to establish baseline allocation counts

---

## Assumptions / System Model
- Deployment environment: these packages are imported by gateway and ingester; no standalone process
- Failure modes: invalid input (missing tenant, bad JSON, zero timestamp) -- handled by returning errors, never panicking
- Delivery guarantees: N/A at this layer — model is data-in, data-out
- Multi-tenancy: TenantID is a first-class field on every Event; tenant extraction is a standalone helper

---

## Data Model

### Event
| Field | Type | JSON tag | Validation |
|-------|------|----------|------------|
| TenantID | `string` | `tenant_id` | Non-empty |
| Timestamp | `time.Time` | `timestamp` | Non-zero |
| Labels | `map[string]string` | `labels` | None (optional metadata) |
| Value | `float64` | `value` | None (zero is valid) |
| Payload | `[]byte` | `payload,omitempty` | None (optional) |

### Batch
| Field | Type | JSON tag | Notes |
|-------|------|----------|-------|
| TenantID | `string` | `tenant_id` | All events in batch share this |
| Partition | `int32` | `partition` | Kafka partition assignment |
| Events | `[]Event` | `events` | Ordered slice |
| CreatedAt | `time.Time` | `created_at` | Set by `NewBatch()` |
| SizeBytes | `int` | `size_bytes` | Updated by `Add()` |

**Validation rules:**
- `Event.Validate()` returns a descriptive error on first failing check (short-circuit).
- `Batch.Add()` does NOT re-validate events — the gateway validates before batching. This avoids double-validation overhead.

**Versioning strategy:** Not needed for MVP. JSON field tags are the contract. If the schema changes, we add fields with `omitempty` for backward compatibility.

**Size estimation for `SizeBytes`:** Use `len(json.Marshal(event))` as a rough byte count. This is an approximation — good enough for batch-size thresholds; exact accounting is not needed for MVP.

---

## APIs

### `internal/model` package

```go
// Constructors
func NewEvent(tenantID string, ts time.Time, labels map[string]string, value float64) Event
func NewBatch(tenantID string, partition int32) Batch

// Validation
func (e Event) Validate() error

// Batch operations
func (b *Batch) Add(event Event)  // appends + updates SizeBytes
func (b *Batch) Len() int

// Serialization (internal/model/serialize.go)
func MarshalEvents(events []Event) ([]byte, error)
func UnmarshalEvents(data []byte) ([]Event, error)
func MarshalBatch(batch Batch) ([]byte, error)
func UnmarshalBatch(data []byte) (Batch, error)
```

### `internal/tenant` package

```go
func ExtractTenantID(r *http.Request) (string, error)
func ContextWithTenantID(ctx context.Context, tenantID string) context.Context
func TenantIDFromContext(ctx context.Context) (string, bool)
```

**Error semantics:**
- `ExtractTenantID` returns `ErrMissingTenantID` (exported sentinel) when header is missing or empty.
- `Event.Validate()` returns descriptive wrapped errors: `fmt.Errorf("event: %w", ...)`.
- Serialization functions pass through `json.Marshal`/`json.Unmarshal` errors unchanged.

---

## Architecture / Component Boundaries

### Components I own
- **model package** (`internal/model/`): Pure data types + serialization. Zero external dependencies beyond stdlib. Imported by gateway (to parse requests), ingester (to deserialize from Kafka), and storage (to serialize for flush).
- **tenant package** (`internal/tenant/`): HTTP tenant extraction + context propagation. Depends only on `net/http` and `context` from stdlib. Imported by gateway handler.

### Components I do NOT touch
- Gateway handler (gateway-engineer) — imports my types
- Kafka producer/consumer (gateway-engineer / ingester-engineer) — uses my serialization
- Config (infra-engineer) — no config needed for model/tenant
- Infrastructure (infra-engineer) — docker-compose, Makefile

### How config changes propagate
N/A — these packages have no runtime config. They are pure functions and data types.

### Concurrency model
All functions are safe for concurrent use. `Event` and `Batch` are value types (or pointer receivers with no shared mutable state across goroutines). Callers are responsible for synchronization if they share a `*Batch` across goroutines.

### Backpressure strategy
N/A at this layer.

---

## Correctness Invariants

1. `NewEvent()` always sets TenantID and Timestamp from arguments — never leaves them zero.
2. `Event.Validate()` rejects events with empty TenantID or zero Timestamp. Empty/nil labels are valid.
3. `MarshalEvents` then `UnmarshalEvents` is a round-trip identity (no data loss).
4. `MarshalBatch` then `UnmarshalBatch` is a round-trip identity.
5. `Batch.Add()` increments `SizeBytes` by the approximate serialized size of the added event.
6. `Batch.Len()` always equals `len(b.Events)`.
7. `ExtractTenantID` returns `ErrMissingTenantID` when header is absent or empty string.
8. `TenantIDFromContext` returns `("", false)` when no tenant was set on the context.
9. Context round-trip: `TenantIDFromContext(ContextWithTenantID(ctx, "x"))` returns `("x", true)`.

---

## Tests

### Unit tests

**`internal/model/event_test.go`:**
- `TestNewEvent` — constructor sets all fields correctly
- `TestEventValidate` — table-driven test with subtests:
  - `TestEventValidate/valid` — valid event passes
  - `TestEventValidate/empty_tenant_id` — returns error
  - `TestEventValidate/zero_timestamp` — returns error
  - `TestEventValidate/no_labels` — valid (labels are optional)
- `TestNewBatch` — constructor sets TenantID, Partition, CreatedAt
- `TestBatchAdd` — adds event, updates SizeBytes, increments Len()
- `TestBatchAdd_Multiple` — multiple adds accumulate correctly

**`internal/model/serialize_test.go`:**
- `TestMarshalUnmarshalEvents_RoundTrip` — marshal then unmarshal recovers original data
- `TestMarshalUnmarshalBatch_RoundTrip` — same for Batch
- `TestUnmarshalEvents_InvalidJSON` — returns error on garbage input
- `TestUnmarshalBatch_InvalidJSON` — returns error on garbage input
- `TestMarshalEvents_Empty` — empty slice marshals to `[]`
- `TestMarshalEvents_WithPayload` — payload bytes survive round-trip (base64 in JSON)

**`internal/tenant/tenant_test.go`:**
- `TestExtractTenantID_Present` — returns header value
- `TestExtractTenantID_Missing` — returns ErrMissingTenantID
- `TestExtractTenantID_Empty` — returns ErrMissingTenantID
- `TestContextRoundTrip` — set then get returns same value
- `TestTenantIDFromContext_NotSet` — returns ("", false)

### Integration tests
N/A — pure library code, no external dependencies.

### Property/fuzz tests (stretch goal)
- Fuzz `UnmarshalEvents` with random bytes — must never panic, always return error or valid data.

### Failure injection tests
N/A — no I/O, no network, no external systems.

### Commands
```bash
go test ./internal/model/... -v
go test ./internal/tenant/... -v
go test ./internal/model/... -run TestMarshalUnmarshal
go test ./internal/tenant/... -run TestExtract
```

---

## Benchmarks + "Success"

**What to measure:**
- `MarshalEvents` throughput and allocations (events/sec, bytes/op, allocs/op)
- `UnmarshalEvents` throughput and allocations
- `Event.Validate()` — should be nearly zero-alloc

**Benchmark functions** (in `serialize_test.go` and `event_test.go`):
- `BenchmarkMarshalEvents_10` — 10 events
- `BenchmarkMarshalEvents_1000` — 1000 events
- `BenchmarkUnmarshalEvents_10`
- `BenchmarkUnmarshalEvents_1000`
- `BenchmarkEventValidate`

**Success criteria:**
- `MarshalEvents(1000)` completes in < 1ms on local hardware
- `Event.Validate()` is zero-alloc (0 allocs/op)
- Round-trip (marshal + unmarshal) for 1000 events < 2ms

**Command:**
```bash
go test ./internal/model/... -bench . -benchmem
```

---

## Recommended API Surface

Summarized above in the APIs section. Key design choices:

1. **Separate `MarshalEvents` / `MarshalBatch`** rather than implementing `json.Marshaler` on the types — keeps serialization explicit and avoids surprises when types are embedded.
2. **`Validate()` on Event only** — Batch does not validate its events (caller's responsibility). This avoids re-validation in the hot path.
3. **Sentinel error `ErrMissingTenantID`** — allows gateway to check with `errors.Is()` and return appropriate HTTP 400.
4. **Private context key type** in tenant package — prevents any other package from accidentally overwriting the tenant ID in context.

---

## Folder Structure

```
internal/
  model/
    event.go           — Event, Batch types, constructors, Validate(), Add()
    event_test.go      — unit tests + benchmarks for Event/Batch
    serialize.go       — MarshalEvents, UnmarshalEvents, MarshalBatch, UnmarshalBatch
    serialize_test.go  — round-trip tests, error cases, benchmarks
  tenant/
    tenant.go          — ExtractTenantID, ContextWithTenantID, TenantIDFromContext, ErrMissingTenantID
    tenant_test.go     — extraction tests, context round-trip tests
```

**Ownership:** model-engineer owns all 6 files above. No other engineer should modify these files without coordination.

---

## Tighten the plan into 4-7 small tasks

### Task 1: Event and Batch types with constructors
- **Outcome:** `Event` and `Batch` structs defined with JSON tags, `NewEvent()` and `NewBatch()` constructors, `Batch.Add()` and `Batch.Len()` methods.
- **Files:** `internal/model/event.go`
- **Verification:** `go vet ./internal/model/...` passes (compilation check)
- **Commit:** `feat(model): add Event and Batch types with constructors`

### Task 2: Event validation
- **Outcome:** `Event.Validate()` method that rejects empty tenant and zero timestamp. Labels are optional.
- **Files:** `internal/model/event.go` (append to existing)
- **Verification:** `go test ./internal/model/... -run TestEventValidate -v`
- **Commit:** `feat(model): add Event.Validate with boundary checks`

### Task 3: Serialization functions
- **Outcome:** `MarshalEvents`, `UnmarshalEvents`, `MarshalBatch`, `UnmarshalBatch` — all using `encoding/json`.
- **Files:** `internal/model/serialize.go`
- **Verification:** `go test ./internal/model/... -run TestMarshalUnmarshal -v`
- **Commit:** `feat(model): add JSON serialization for Events and Batch`

### Task 4: Tenant extraction and context helpers
- **Outcome:** `ExtractTenantID`, `ContextWithTenantID`, `TenantIDFromContext`, `ErrMissingTenantID`.
- **Files:** `internal/tenant/tenant.go`
- **Verification:** `go test ./internal/tenant/... -v`
- **Commit:** `feat(tenant): add tenant ID extraction and context helpers`

### Task 5: Full unit test suite
- **Outcome:** Comprehensive tests for all exported functions in both packages. Table-driven where appropriate.
- **Files:** `internal/model/event_test.go`, `internal/model/serialize_test.go`, `internal/tenant/tenant_test.go`
- **Verification:** `go test ./internal/model/... ./internal/tenant/... -v -count=1`
- **Commit:** `test(model,tenant): add comprehensive unit tests`

### Task 6: Benchmarks
- **Outcome:** Benchmark functions for marshal/unmarshal (10 and 1000 events) and Validate.
- **Files:** `internal/model/event_test.go` (append), `internal/model/serialize_test.go` (append)
- **Verification:** `go test ./internal/model/... -bench . -benchmem`
- **Commit:** `bench(model): add serialization and validation benchmarks`

---

### Top 3 Risks/Unknowns

1. **SizeBytes estimation accuracy:** Using `len(json.Marshal(event))` per-add is simple but allocates on every `Add()`. **Mitigation:** benchmark the Add path; if allocations are a problem, switch to a fixed estimate (e.g., 200 bytes per event) and document the tradeoff. Validate in < 5 min with `BenchmarkBatchAdd`.

2. **Payload base64 overhead:** `[]byte` fields are base64-encoded in JSON, roughly 33% overhead. If batch size thresholds use SizeBytes from JSON marshaling, actual Kafka message sizes will differ from raw byte counts. **Mitigation:** document that SizeBytes is an approximation. For Phase 1, this is acceptable. Validate by writing a test that marshals an event with a 1KB payload and checking the output size.

3. **Timestamp format compatibility:** `time.Time` marshals to RFC3339Nano by default in Go's `encoding/json`. If other consumers expect Unix epoch or a different format, this could break. **Mitigation:** confirm RFC3339Nano is acceptable for the gateway's JSON API contract. If not, add custom marshaling. Validate in < 5 min by checking what the gateway-engineer expects.

---

## CLAUDE.md contributions (proposed content, NOT written to file)

## From Model Engineer
- **Coding style:** All types in `internal/model/` use value receivers except `Batch.Add()` which uses pointer receiver (mutates state). JSON tags use `snake_case`.
- **Dev commands:**
  - `go test ./internal/model/... -v` — run model tests
  - `go test ./internal/tenant/... -v` — run tenant tests
  - `go test ./internal/model/... -bench . -benchmem` — run benchmarks
- **Before you commit:**
  - `go vet ./internal/model/... ./internal/tenant/...` must pass
  - All tests pass: `go test ./internal/model/... ./internal/tenant/... -count=1`
  - No new exported functions without corresponding test coverage
- **Guardrails:**
  - Do NOT add external dependencies to `internal/model/` — it must stay stdlib-only
  - Do NOT implement `json.Marshaler`/`json.Unmarshaler` on Event or Batch — use the explicit functions in `serialize.go`
  - `ErrMissingTenantID` is a sentinel — check with `errors.Is()`, never string comparison

---

## EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- Events enter the system as JSON arrays via HTTP POST. The `model` package defines the canonical `Event` struct that all components use.
- Tenant ID is extracted from the `X-Scope-OrgID` HTTP header by the `tenant` package and propagated via `context.Context`.
- Serialization is centralized in `serialize.go` to ensure consistent JSON format across gateway (marshal for Kafka) and ingester (unmarshal from Kafka).

### Key Engineering Decisions
- **Value types over interfaces:** `Event` and `Batch` are concrete structs, not interfaces. There is only one event format — no need for polymorphism.
- **Explicit serialization functions** rather than `json.Marshaler`: keeps the format visible and testable without magic.
- **Private context key:** `type ctxKey struct{}` prevents external packages from accidentally overwriting tenant context values.
- **SizeBytes is approximate:** uses JSON-marshaled size, not raw struct size. Good enough for batch threshold decisions; exact accounting adds complexity with no Phase 1 benefit.

### Limits of MVP
- No protobuf serialization (JSON only). Protobuf can be added later alongside JSON.
- No event schema versioning. Field additions use `omitempty` for backward compatibility.
- SizeBytes is approximate — not suitable for exact quota enforcement.

### How to Run / Validate
```bash
go test ./internal/model/... ./internal/tenant/... -v        # all tests
go test ./internal/model/... -bench . -benchmem               # benchmarks
go vet ./internal/model/... ./internal/tenant/...             # static analysis
```

---

## READY FOR APPROVAL
