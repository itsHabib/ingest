# Index Engineer Plan

---

## You are in PLAN MODE.

### Project
I want to build a **query foundation for the ingestion engine**.

**Goal:** build a **block index** in which we **enable the querier to skip blocks that cannot contain matching events, reducing query cost from O(all data) to O(matching blocks)**.

### Role + Scope
- **Role:** Index Engineer
- **Scope:** I own the `internal/index/` package — block metadata, index read/write, label matchers, and the ingester integration that writes index entries at flush time. I do NOT own the querier HTTP API, the parallel block reader, or the compactor.
- **File I will write:** `/docs/query-foundation/plans/index-engineer.md`
- **No-touch zones:** do not edit `cmd/`, `internal/gateway/`, `internal/ring/`, `internal/observability/`, or any docker-compose/infra files.

---

## Functional Requirements
- FR1: `BlockMeta` struct captures file path, tenant, partition, time range, event count, label key set, and size for each flushed batch.
- FR2: `Index` struct aggregates `BlockMeta` entries per tenant per day. `Lookup(start, end, matchers)` returns only blocks whose time range overlaps and whose labels could match.
- FR3: `LabelMatcher` supports four match types — exact (`=`), not-equal (`!=`), regex (`=~`), not-regex (`!~`) — parsed from Prometheus-style query strings.
- FR4: `IndexStore` interface with an `ObjectStoreIndexStore` implementation that reads/writes index JSON files at `<tenantID>/<date>/index.json`.
- FR5: The ingester flusher writes block metadata to the index after every successful batch flush.
- Tests required: unit tests for matchers, index lookup, index store serialization, and flusher integration.
- Metrics required: N/A for this role (observability-engineer scope); however the flusher integration will log index writes via existing zap logger.

## Non-Functional Requirements
- Language/runtime: Go (same as rest of codebase)
- Local dev: no new infra dependencies; index files are written to the same ObjectStore as batch data
- Observability: index write failures logged at WARN level; success at INFO
- Safety: index write failure must NOT block or fail the batch flush — log and continue (index is advisory, not authoritative)
- Documentation: CLAUDE.md + EXPLAIN.md contribution sections below
- Performance: index lookup must be sub-millisecond for typical tenant-day indexes (< 10k blocks)

---

## Assumptions / System Model
- Deployment environment: local docker-compose (same as existing pipeline)
- Failure modes: ObjectStore write failure for index (handled: log + skip), corrupt index JSON on read (handled: return error, querier falls back to listing all blocks)
- Delivery guarantees: index is best-effort. If an index write fails, the batch data is still safely stored. The querier can fall back to listing files by prefix.
- Multi-tenancy: indexes are per-tenant per-day, naturally isolated by key path.
- Concurrency: multiple ingester goroutines may flush batches for the same tenant-day concurrently. The index store must handle read-modify-write with optimistic concurrency (retry on conflict) or append-only semantics.

---

## Data Model

### BlockMeta
```go
type BlockMeta struct {
    FilePath   string            // storage key of the batch file
    TenantID   string            // tenant that owns this block
    Partition  int32             // Kafka partition
    MinTime    time.Time         // earliest event timestamp in block
    MaxTime    time.Time         // latest event timestamp in block
    EventCount int               // number of events
    Labels     map[string]string // union of all label keys present (key -> "" for presence-only)
    SizeBytes  int               // serialized batch size
}
```

- Validation: FilePath and TenantID must be non-empty; MinTime <= MaxTime; EventCount > 0.
- Labels stores the union of all distinct label key-value pairs across events in the block. For lookup, we check if the matcher could match any event in the block (label key must be present).

### Index
```go
type Index struct {
    TenantID string      `json:"tenant_id"`
    Date     string      `json:"date"` // YYYY-MM-DD
    Blocks   []BlockMeta `json:"blocks"`
}
```

- One index file per tenant per day.
- Key format: `<tenantID>/<YYYY-MM-DD>/index.json`
- Versioning: not needed for MVP. The index is always overwritten atomically.

### LabelMatcher
```go
type MatchType int

const (
    MatchEqual    MatchType = iota // =
    MatchNotEqual                  // !=
    MatchRegexp                    // =~
    MatchNotRegexp                 // !~
)

type LabelMatcher struct {
    Name  string
    Value string
    Type  MatchType
    re    *regexp.Regexp // compiled for regex types
}
```

- Parse format: `service=api,env=~prod.*,level!=debug`
- Regex matchers are anchored (full match: `^value$`).

---

## APIs

### Package `internal/index` — Public Functions

**index.go**
- `NewIndex(tenantID, date string) *Index` — create empty index
- `(*Index) AddBlock(meta BlockMeta)` — append block
- `(*Index) Lookup(start, end time.Time, matchers []LabelMatcher) []BlockMeta` — filter blocks by time overlap and label match

**store.go**
- `IndexStore` interface:
  - `WriteIndex(ctx context.Context, idx *Index) error`
  - `ReadIndex(ctx context.Context, tenantID, date string) (*Index, error)`
- `NewObjectStoreIndexStore(store storage.ObjectStore) *ObjectStoreIndexStore`

**matcher.go**
- `ParseMatchers(query string) ([]LabelMatcher, error)` — parse query string
- `(LabelMatcher) Matches(value string) bool` — single matcher against single value
- `MatchAll(matchers []LabelMatcher, labels map[string]string) bool` — all matchers must pass

### Ingester Integration
- Modify `Flusher.Flush()` to compute `BlockMeta` from the batch and call `IndexStore.WriteIndex()` (read-modify-write: read existing index, append block, write back).
- Index write failure is logged but does not fail the flush.

---

## Architecture / Component Boundaries

### Components I own
- **`internal/index/index.go`** — BlockMeta, Index structs, Lookup logic
- **`internal/index/store.go`** — IndexStore interface, ObjectStoreIndexStore (reads/writes JSON via existing ObjectStore)
- **`internal/index/matcher.go`** — LabelMatcher, parsing, matching logic

### Integration point (shared with ingester)
- **`internal/ingester/flusher.go`** — after a successful `Put()`, compute BlockMeta from the batch and update the index. This is the only file outside `internal/index/` that I modify.

### How it works
1. Flusher writes batch to object storage (existing behavior, unchanged).
2. Flusher computes BlockMeta: scans batch events for min/max time and label union.
3. Flusher calls `IndexStore.ReadIndex()` to load existing index for that tenant-day (or creates new one if not found).
4. Flusher calls `Index.AddBlock(meta)` and then `IndexStore.WriteIndex()`.
5. If step 3 or 4 fails, log warning and return nil (batch is already stored).

### Concurrency model
- Read-modify-write on index files is NOT atomic with the current ObjectStore interface.
- For MVP: accept that concurrent flushes to the same tenant-day may cause a lost update (one block entry dropped). This is acceptable because the querier can fall back to file listing.
- Future: add `PutIfNotModified` (conditional write) to ObjectStore, or switch to append-only index segments that get merged.

### Backpressure
- N/A — index writes are fire-and-forget from the flusher's perspective.

---

## Correctness Invariants
1. **Every successfully flushed batch has a corresponding BlockMeta in the index** (best-effort; concurrent writes may lose entries).
2. **Lookup never returns false negatives for time range** — if a block's [MinTime, MaxTime] overlaps [start, end], it is returned.
3. **Lookup never returns false negatives for labels** — if any event in a block could match the matchers, the block is returned. (It may return false positives — that is acceptable; the block reader does the precise filtering.)
4. **LabelMatcher parsing rejects malformed input** — returns a clear error for unparseable matcher strings.
5. **Regex matchers are anchored** — `=~foo` matches `^foo$`, not a substring.
6. **Index write failure never causes batch flush failure** — the batch data is the source of truth.

---

## Tests

### Unit tests
- `internal/index/matcher_test.go`:
  - Table-driven tests for all four match types (exact, not-equal, regex, not-regex)
  - Edge cases: empty value, empty label name, invalid regex, unanchored patterns
  - `ParseMatchers` round-trip tests
- `internal/index/index_test.go`:
  - `Lookup` with time range filtering (overlap, no overlap, edge alignment)
  - `Lookup` with label matchers (match, no match, partial match)
  - `Lookup` with combined time + label filters
  - Empty index returns no results
- `internal/index/store_test.go`:
  - Write then read round-trip with in-memory ObjectStore
  - Read non-existent index returns appropriate error or empty index
  - Corrupt JSON returns error

### Integration tests
- `internal/ingester/flusher_test.go` (addition):
  - Flush a batch and verify that BlockMeta appears in the index
  - Flush multiple batches and verify all appear
  - Index store failure does not fail the flush

### Commands
- `go test ./internal/index/... -v -count=1`
- `go test ./internal/ingester/... -v -count=1 -run TestFlush`
- `go test ./... -count=1` (full suite, verify no regressions)

---

## Benchmarks + "Success"

- **What to measure:** `Lookup` performance on indexes of varying size (100, 1000, 10000 blocks).
- **Target:** Lookup on a 10k-block index completes in < 1ms. AddBlock is O(1) amortized.
- **Benchmark command:** `go test ./internal/index/... -bench . -benchmem`
- **What to measure:** `ParseMatchers` and `MatchAll` throughput.
- **Target:** > 1M match operations/sec for typical label sets (5-10 labels).

---

## Engineering Decisions & Tradeoffs

### 1. Index as single JSON file per tenant-day vs. append-only log segments
- **Decision:** Single JSON file, read-modify-write on each flush.
- **Alternatives considered:** (a) Append-only log segments (one small file per flush, merged at read time). (b) Embedded database (bbolt/sqlite).
- **Why:** Single JSON file is simplest to implement and matches the existing ObjectStore interface (Put/Get). The querier reads one file per tenant-day instead of scanning many small segments. Aligns with the project's "explainable and whiteboardable" constraint.
- **Tradeoff acknowledged:** Concurrent flushes to the same tenant-day can cause lost updates (last writer wins, dropping a block entry). Acceptable for MVP because (a) the querier can fall back to file listing, and (b) the compactor (Phase 7) will rebuild indexes anyway.

### 2. Label storage in BlockMeta: full key-value pairs vs. key-only set
- **Decision:** Store full `map[string]string` (union of all label key-value pairs in the block).
- **Alternatives considered:** Store only label key names (set of strings). This would reduce index size but only allow key-presence checks, not value filtering.
- **Why:** Storing full key-value pairs lets `Lookup` do value-level filtering (e.g., `service=api` can skip blocks where all events have `service=worker`). This significantly reduces false positives, meaning fewer blocks to read.
- **Tradeoff acknowledged:** Index files are larger because they store full label values. For blocks with high label cardinality, this could be significant. Mitigated by the fact that typical blocks have < 100 unique label pairs.

### 3. Regex anchoring strategy
- **Decision:** Automatically anchor regex matchers with `^...$`, matching Prometheus/Loki convention.
- **Alternatives considered:** Let users provide their own anchors (raw regex).
- **Why:** Consistency with the wider observability ecosystem. Users familiar with Prometheus will expect `=~prod` to NOT match `production`.
- **Tradeoff acknowledged:** Users who want substring matching must explicitly use `.*substring.*`.

---

## Risks & Mitigations

### 1. Concurrent index writes cause lost block entries
- **Risk:** Two goroutines flush batches for the same tenant-day simultaneously, both read the same index, both append their block, and the second write overwrites the first's addition.
- **Impact:** One block entry is silently dropped from the index. The querier might miss that block unless it falls back to file listing.
- **Mitigation:** (a) Document this as a known limitation. (b) Querier should union index results with file listing for correctness in Phase 6. (c) Compactor in Phase 7 rebuilds indexes from actual files.
- **Validation time:** < 5 minutes — write a test with two concurrent flushes and verify behavior.

### 2. Index file grows unbounded for high-volume tenants
- **Risk:** A tenant with thousands of flushes per day produces a very large index.json.
- **Impact:** Slow index reads, high memory usage when parsing.
- **Mitigation:** (a) Benchmark with 10k blocks to confirm sub-millisecond lookup. (b) Compactor (Phase 7) will merge small blocks, reducing entry count. (c) Could add index sharding in the future (e.g., per-hour indexes).
- **Validation time:** < 10 minutes — generate synthetic index with 10k entries and benchmark.

### 3. Modifying flusher.go introduces regressions in the ingestion hot path
- **Risk:** Adding index write logic to the flusher could introduce bugs or slow down the critical flush path.
- **Impact:** Ingestion pipeline failures or performance degradation.
- **Mitigation:** (a) Index write is fire-and-forget — wrapped in a goroutine or deferred, never blocks the main flush. (b) Existing flusher tests must continue to pass. (c) Add explicit test for index failure not propagating.
- **Validation time:** < 5 minutes — run `go test ./internal/ingester/... -v`.

### 4. Label matcher regex compilation is expensive
- **Risk:** Compiling regex on every query could add latency.
- **Impact:** Slow query parsing for complex regex matchers.
- **Mitigation:** Compile regex once in `ParseMatchers` and cache the compiled `*regexp.Regexp` in the `LabelMatcher` struct. Benchmark to verify.
- **Validation time:** < 5 minutes — benchmark `ParseMatchers` with regex matchers.

---

## Recommended API Surface

### `internal/index/index.go`
```go
func NewIndex(tenantID, date string) *Index
func (idx *Index) AddBlock(meta BlockMeta)
func (idx *Index) Lookup(start, end time.Time, matchers []LabelMatcher) []BlockMeta
```

### `internal/index/store.go`
```go
type IndexStore interface {
    WriteIndex(ctx context.Context, idx *Index) error
    ReadIndex(ctx context.Context, tenantID, date string) (*Index, error)
}
func NewObjectStoreIndexStore(store storage.ObjectStore) *ObjectStoreIndexStore
```

### `internal/index/matcher.go`
```go
func ParseMatchers(query string) ([]LabelMatcher, error)
func (m LabelMatcher) Matches(value string) bool
func MatchAll(matchers []LabelMatcher, labels map[string]string) bool
```

---

## Folder Structure

```
internal/index/
    index.go           — BlockMeta, Index, Lookup
    index_test.go      — unit tests for Index + Lookup
    store.go           — IndexStore interface, ObjectStoreIndexStore
    store_test.go      — round-trip tests for store
    matcher.go         — LabelMatcher, ParseMatchers, MatchAll
    matcher_test.go    — table-driven matcher tests
```

Modified (not owned):
```
internal/ingester/
    flusher.go         — add index write after successful batch Put
    flusher_test.go    — add test for index integration
```

---

## Tighten the plan into 4-7 small tasks

### Task 1: Label matchers
- **Outcome:** `LabelMatcher` struct, `ParseMatchers`, `Matches`, `MatchAll` all working with full test coverage.
- **Files to create:** `internal/index/matcher.go`, `internal/index/matcher_test.go`
- **Verification:** `go test ./internal/index/... -v -run TestMatcher -count=1`
- **Commit message:** `feat(index): add label matcher with exact, not-equal, regex, not-regex support`

### Task 2: BlockMeta and Index with Lookup
- **Outcome:** `BlockMeta` struct, `Index` struct, `NewIndex`, `AddBlock`, `Lookup` with time and label filtering. Full test coverage.
- **Files to create:** `internal/index/index.go`, `internal/index/index_test.go`
- **Verification:** `go test ./internal/index/... -v -run TestIndex -count=1`
- **Commit message:** `feat(index): add block index with time-range and label-based lookup`

### Task 3: IndexStore with ObjectStore backend
- **Outcome:** `IndexStore` interface and `ObjectStoreIndexStore` that serializes/deserializes index JSON via the existing `ObjectStore`. Round-trip tests.
- **Files to create:** `internal/index/store.go`, `internal/index/store_test.go`
- **Verification:** `go test ./internal/index/... -v -run TestStore -count=1`
- **Commit message:** `feat(index): add index store backed by ObjectStore`

### Task 4: Ingester integration
- **Outcome:** `Flusher.Flush` computes `BlockMeta` from the batch and writes to the index after a successful batch Put. Index write failure does not fail the flush. Existing tests still pass.
- **Files to modify:** `internal/ingester/flusher.go`, `internal/ingester/flusher_test.go`
- **Verification:** `go test ./internal/ingester/... -v -count=1`
- **Commit message:** `feat(index): integrate block index writes into ingester flusher`

### Task 5: Benchmarks
- **Outcome:** Benchmarks for `Lookup` (100, 1k, 10k blocks), `ParseMatchers`, and `MatchAll`. Verify < 1ms for 10k-block lookup.
- **Files to modify:** `internal/index/index_test.go`, `internal/index/matcher_test.go`
- **Verification:** `go test ./internal/index/... -bench . -benchmem`
- **Commit message:** `bench(index): add benchmarks for lookup and matcher operations`

---

## CLAUDE.md contributions (proposed, do NOT write the file)

## From Index Engineer
- **Coding style:** All structs in `internal/index/` use JSON tags for serialization. Label matchers follow Prometheus conventions (`=`, `!=`, `=~`, `!~`). Regex matchers are auto-anchored with `^...$`.
- **Dev commands:**
  - `go test ./internal/index/... -v -count=1` — run index unit tests
  - `go test ./internal/index/... -bench . -benchmem` — run index benchmarks
  - `go test ./internal/ingester/... -v -count=1 -run TestFlush` — verify flusher integration
- **Before you commit:**
  - All existing tests still pass: `go test ./... -count=1`
  - No data races: `go test ./internal/index/... -race -count=1`
  - `go vet ./internal/index/...` clean
- **Guardrails:**
  - Index write failure must NEVER fail a batch flush — this is a hard invariant.
  - Do not add external dependencies to `internal/index/`; use only stdlib + existing project deps.
  - `Lookup` must never produce false negatives (missing blocks that match). False positives are acceptable.

---

## EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- Block index is a lightweight per-tenant-per-day JSON file mapping time ranges and label sets to block file paths.
- Written by the ingester at flush time, read by the querier at query time.
- Key format: `<tenantID>/<YYYY-MM-DD>/index.json`.

### Key Engineering Decisions + Tradeoffs
- Single JSON file per tenant-day (simple, one read per day-range query) vs. append-only segments (no lost updates but more reads). Chose simplicity; compactor rebuilds indexes later.
- Full label key-value pairs in BlockMeta (enables value-level filtering, fewer false positives) vs. key-only set (smaller index). Chose precision over size.
- Regex matchers auto-anchored to match Prometheus/Loki conventions.

### Limits of MVP + Next Steps
- Concurrent index writes can lose entries (last-writer-wins). Querier should union index results with file listing for correctness.
- No index sharding — a single file per tenant-day could grow large for high-volume tenants. Future: per-hour sharding or compactor-driven merge.
- No conditional writes (PutIfNotModified). Future: add to ObjectStore interface.

### How to Run + Validate
- `go test ./internal/index/... -v -count=1` — all index tests
- `go test ./internal/index/... -bench . -benchmem` — performance benchmarks
- `go test ./internal/ingester/... -v -run TestFlush -count=1` — flusher integration

---

## READY FOR APPROVAL
