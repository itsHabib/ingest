# Query API Engineer Plan

## You are in PLAN MODE.

### Project
I want to build the **read path** for the ingestion engine.

**Goal:** build an **HTTP query API and querier CLI command** that allows clients to query stored events by time range, tenant, and label matchers without scanning every block.

### Role + Scope
- **Role:** Query API Engineer
- **Scope:** HTTP query handler (`internal/querier/handler.go`), querier CLI command (`cmd/querier.go`), and unit tests. I own the HTTP layer that accepts query requests, coordinates with the IndexStore and BlockReader, and streams results. I do NOT own the block index format/writer (index-engineer) or the parallel block reader internals (block-reader-engineer).
- **File I will write:** `/docs/query-foundation/plans/query-api-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- FR1: `GET /api/v1/query` endpoint that accepts `start`, `end` (RFC3339), tenant ID (`X-Scope-OrgID` header), and `labels` (comma-separated key=value matchers) query parameters.
- FR2: Validate inputs: tenant not empty, start < end, matchers parse correctly. Return 400 on invalid input.
- FR3: Enumerate all dates in [start, end] range, read block index for each date, filter to matching blocks, pass to the block reader, and stream results as newline-delimited JSON (NDJSON).
- FR4: `GET /ready` health endpoint returning 200.
- FR5: Querier CLI command (`go run . querier --port 8082`) with graceful shutdown on SIGINT/SIGTERM.
- Tests required: unit tests using `httptest` with mock IndexStore and mock ObjectStore.
- Metrics required: query request count, query latency histogram, blocks scanned counter (Prometheus, consistent with existing `internal/observability` patterns).

## Non-Functional Requirements
- Language/runtime: Go (consistent with all other components)
- Local dev: `go run . querier --port 8082 --storage-path ./data`
- Observability: `/metrics` on admin port (default 9102), pprof endpoints
- Safety: context.Context for cancellation; graceful shutdown with 10s timeout
- Documentation: CLAUDE.md + EXPLAIN.md contributions (proposed below)
- Performance: streaming NDJSON avoids buffering entire result set in memory

---

## Assumptions / System Model
- Deployment environment: local binary; later docker-compose and k8s
- Failure modes: missing index file (return empty results, not error), object store read failure (return 500), context cancellation (stop streaming)
- Delivery guarantees: reads are best-effort; no caching in this phase
- Multi-tenancy: tenant ID is mandatory on every query (extracted from `X-Scope-OrgID` header, same as gateway)

---

## Data Model (as relevant to this role)

### IndexStore interface (consumed, not owned)
The query handler depends on an `IndexStore` interface that the index-engineer will implement. From the handler's perspective:

```
type IndexStore interface {
    Lookup(ctx context.Context, tenantID string, date string, matchers []Matcher) ([]string, error)
}
```

Returns a list of block file paths (object store keys) that may contain matching events.

### Matcher (shared type)
```
type Matcher struct {
    Name  string
    Value string
    Type  MatchType  // MatchEqual or MatchRegex
}
```

Parsed from the `labels` query parameter. Format: `key=value` (exact) or `key=~regex` (regex).

### BlockReader interface (consumed, not owned)
```
type BlockReader interface {
    Read(ctx context.Context, keys []string, matchers []Matcher, start, end time.Time) (<-chan event.Event, <-chan error)
}
```

Returns channels for streamed events and errors. Owned by block-reader-engineer.

### Query response format
Newline-delimited JSON (NDJSON). Each line is one `event.Event` serialized as JSON. This allows streaming without buffering.

---

## APIs

### Query API

#### `GET /api/v1/query`
**Request:**
- Header: `X-Scope-OrgID: <tenant-id>` (required)
- Query params:
  - `start` (required): RFC3339 timestamp, e.g. `2024-01-01T00:00:00Z`
  - `end` (required): RFC3339 timestamp, must be after start
  - `labels` (optional): comma-separated matchers, e.g. `service=api,env=prod`

**Response (200):**
- Content-Type: `application/x-ndjson`
- Body: one JSON-encoded `event.Event` per line

**Error responses:**
- 400: missing/invalid tenant ID, missing/invalid start/end, malformed matchers, start >= end
- 404: no matching data found (zero events after filtering)
- 500: internal error (storage failure)

#### `GET /ready`
**Response (200):**
```json
{"status": "ready"}
```

### Admin endpoints (on separate port)
- `GET /metrics` — Prometheus metrics
- `GET /debug/pprof/*` — pprof endpoints

---

## Architecture / Component Boundaries

### Components I own
- **`internal/querier/handler.go`** — HTTP handler implementing `ServeHTTP`. Routes `/api/v1/query` and `/ready`. Parses request, validates inputs, calls IndexStore + BlockReader, streams NDJSON response.
- **`internal/querier/handler_test.go`** — Unit tests with mocks.
- **`internal/querier/matcher.go`** — `Matcher` type and `ParseMatchers(s string) ([]Matcher, error)` function for parsing the `labels` query parameter.
- **`cmd/querier.go`** — Cobra subcommand that wires config, storage, index store, block reader, handler, and HTTP server with graceful shutdown.

### Components I consume (interfaces only)
- **IndexStore** — provided by index-engineer. I define the interface I need; they implement it.
- **BlockReader** — provided by block-reader-engineer. I define the interface I need; they implement it.
- **ObjectStore** — `internal/storage/storage.go` (already exists). Used to construct the index store and block reader.

### Concurrency model
- The handler itself is stateless and safe for concurrent use.
- Each request gets its own goroutine (standard `net/http`).
- The handler delegates concurrency to the BlockReader (worker pool is internal to block-reader-engineer).
- NDJSON streaming uses `json.NewEncoder(w).Encode()` per event, flushing after each line via `http.Flusher`.

### Backpressure strategy
- Context cancellation: if the client disconnects, `r.Context()` is cancelled, propagating through IndexStore and BlockReader.
- The BlockReader's channel-based API naturally provides backpressure — the handler consumes events at the rate it can write to the HTTP response.

---

## Correctness Invariants
1. A query with `start >= end` is always rejected with 400.
2. A query without `X-Scope-OrgID` is always rejected with 400.
3. Every event in the response has a timestamp within [start, end).
4. Every event in the response matches all provided label matchers.
5. Events are streamed in time order (ascending).
6. If no events match, the response is 200 with an empty body (or 404 — see Engineering Decisions below).
7. Context cancellation stops all downstream work (index lookup + block reads).

---

## Tests

### Unit tests (`internal/querier/handler_test.go`)
- `TestHandler_MissingTenantID` — 400 response
- `TestHandler_InvalidTimeRange` — start >= end returns 400
- `TestHandler_MissingStartEnd` — 400 response
- `TestHandler_InvalidMatchers` — malformed labels param returns 400
- `TestHandler_Success` — mock IndexStore returns block paths, mock BlockReader returns events, verify NDJSON output
- `TestHandler_NoResults` — mock returns zero events, verify 200 empty body
- `TestHandler_Ready` — GET /ready returns 200
- `TestHandler_MethodNotAllowed` — POST to /api/v1/query returns 405

### Unit tests (`internal/querier/matcher_test.go`)
- `TestParseMatchers_Valid` — table-driven: `"service=api"`, `"service=api,env=prod"`, `"name=~foo.*"`
- `TestParseMatchers_Invalid` — table-driven: `"="`, `"nolabel"`, empty string
- `TestMatcherMatch` — exact and regex matching against label maps

### Verification commands
- `go test ./internal/querier/... -v`
- `go test ./cmd/... -v` (if cmd tests exist)
- `go vet ./...`

---

## Benchmarks + "Success"
N/A — The query handler is a thin HTTP layer that delegates to IndexStore and BlockReader. The hot path is in the block reader (owned by block-reader-engineer). Benchmarking the handler itself would mostly measure `net/http` and JSON encoding overhead, which is not where performance matters. Read-path benchmarks are scoped to query-bench-engineer in Phase 8.

---

## Engineering Decisions & Tradeoffs

### Decision 1: NDJSON streaming vs. JSON array response
- **Decision:** Use newline-delimited JSON (NDJSON) for query responses.
- **Alternatives considered:** JSON array (`[{...}, {...}]`) which buffers the entire result set.
- **Why:** NDJSON allows streaming results as they arrive from the BlockReader without buffering in memory. This is critical for large result sets. It is also the standard format used by Loki, Prometheus remote-read, and other observability systems.
- **Tradeoff acknowledged:** Clients must parse line-by-line rather than a single JSON parse. Most HTTP client libraries handle this easily, but it is slightly less convenient for simple `curl | jq` usage (though `jq` supports NDJSON with `--slurp`).

### Decision 2: Interface-driven design with mock-based testing
- **Decision:** Define `IndexStore` and `BlockReader` as Go interfaces in the querier package. Test the handler with hand-rolled mock implementations.
- **Alternatives considered:** Using `gomock` or `testify/mock` for generated mocks.
- **Why:** Hand-rolled mocks are simpler, require no code generation, and are consistent with the existing test patterns in this codebase (e.g., `internal/testing/fakes.go`). The interfaces are small (1-2 methods each), so hand-rolled mocks are trivial.
- **Tradeoff acknowledged:** If interfaces grow significantly, hand-rolled mocks become tedious. At that point, switching to generated mocks would be warranted.

### Decision 3: 200 with empty body for no results (not 404)
- **Decision:** Return HTTP 200 with an empty NDJSON body when no events match, rather than 404.
- **Alternatives considered:** 404 ("no matching data").
- **Why:** 404 semantically means "resource not found" — the query endpoint exists, it just returned zero results. Returning 200 with empty body is consistent with how Prometheus, Loki, and other query APIs behave. It also simplifies client logic (no need to distinguish "no data" from "endpoint not found").
- **Tradeoff acknowledged:** Clients cannot distinguish "no matching data" from "empty time range with no data ingested" without additional metadata. This is acceptable for MVP.

### Decision 4: Matcher parsing in handler package vs. shared package
- **Decision:** Define `Matcher` type and `ParseMatchers` in the `internal/querier` package.
- **Alternatives considered:** A shared `internal/matcher` package used by both querier and index.
- **Why:** Keep it local until there is a proven need for sharing. The index-engineer may need to import this type, at which point we can extract it. Starting local avoids premature abstraction.
- **Tradeoff acknowledged:** If the index package needs the same Matcher type, we will need a small refactor to extract it to a shared location.

---

## Risks & Mitigations

### Risk 1: IndexStore interface mismatch with index-engineer implementation
- **Risk:** The IndexStore interface I define may not match what the index-engineer builds.
- **Impact:** Integration failure; handler cannot call the index store.
- **Mitigation:** Define a minimal interface (`Lookup(ctx, tenantID, date, matchers) ([]string, error)`) and coordinate with index-engineer on the contract before implementation begins.
- **Validation time:** 5 minutes (review index-engineer plan doc).

### Risk 2: BlockReader interface mismatch with block-reader-engineer
- **Risk:** The channel-based BlockReader API may not align with how the block-reader-engineer implements parallel reading.
- **Impact:** Integration failure or awkward adapter code.
- **Mitigation:** Define a simple interface (`Read(ctx, keys, matchers, start, end) (<-chan event.Event, <-chan error)`) and share it early. The channel pattern is idiomatic Go for streaming.
- **Validation time:** 5 minutes (review block-reader-engineer plan doc).

### Risk 3: NDJSON streaming breaks on large responses
- **Risk:** Without proper flushing, the HTTP response may buffer in memory or time out.
- **Impact:** OOM or client timeout on large queries.
- **Mitigation:** Use `http.Flusher` to flush after each event line. Set appropriate write deadlines. Test with a mock that returns 10k+ events.
- **Validation time:** 10 minutes (unit test with large mock dataset).

### Risk 4: Date enumeration edge cases
- **Risk:** Enumerating dates between start and end may miss or double-count dates across timezone boundaries.
- **Impact:** Missing events or duplicate events in query results.
- **Mitigation:** Always work in UTC. Use `time.Date(y, m, d, 0, 0, 0, 0, time.UTC)` for date iteration. Unit test with cross-midnight queries.
- **Validation time:** 5 minutes (table-driven test).

---

## Recommended API Surface

### Functions / Endpoints

1. **`querier.NewHandler(indexStore IndexStore, reader BlockReader, logger *zap.Logger) *Handler`** — constructor
2. **`Handler.ServeHTTP(w, r)`** — routes to `/api/v1/query` and `/ready`
3. **`querier.ParseMatchers(s string) ([]Matcher, error)`** — parses label matchers from query param
4. **`Matcher.Matches(labels map[string]string) bool`** — checks if a single matcher matches a label set
5. **`GET /api/v1/query`** — query endpoint (spec above)
6. **`GET /ready`** — health check

## Folder Structure

```
internal/querier/
    handler.go          -- Handler struct, ServeHTTP, query logic, NDJSON streaming
    handler_test.go     -- httptest-based unit tests with mock IndexStore/BlockReader
    matcher.go          -- Matcher type, ParseMatchers, MatchType constants
    matcher_test.go     -- table-driven matcher parsing and matching tests
cmd/
    querier.go          -- cobra subcommand, flag parsing, wiring, graceful shutdown
```

Ownership:
- `internal/querier/handler.go` + `handler_test.go` — query-api-engineer
- `internal/querier/matcher.go` + `matcher_test.go` — query-api-engineer
- `cmd/querier.go` — query-api-engineer
- `internal/querier/index.go` — index-engineer (IndexStore interface + implementation)
- `internal/querier/reader.go` — block-reader-engineer (BlockReader implementation)

---

## Step-by-Step Task Plan

### Task 1: Matcher type and parser
- **Outcome:** `Matcher` struct, `MatchType` enum, `ParseMatchers()` function, `Matches()` method, and comprehensive table-driven tests.
- **Files to create:** `internal/querier/matcher.go`, `internal/querier/matcher_test.go`
- **Verification:** `go test ./internal/querier/... -v -run TestParseMatcher`
- **Commit message:** `feat(querier): add label matcher type and parser`

### Task 2: Handler with request validation
- **Outcome:** `Handler` struct with `NewHandler()`, `ServeHTTP` routing `/api/v1/query` and `/ready`. Input validation: tenant ID extraction, start/end parsing, matcher parsing. Returns proper 400/405 errors. No query execution yet (returns empty 200).
- **Files to create:** `internal/querier/handler.go`, `internal/querier/handler_test.go`
- **Verification:** `go test ./internal/querier/... -v -run TestHandler`
- **Commit message:** `feat(querier): add HTTP handler with request validation`

### Task 3: Query execution and NDJSON streaming
- **Outcome:** Handler calls IndexStore.Lookup for each date in range, passes matching block paths to BlockReader.Read, streams events as NDJSON with `http.Flusher`. Mock-based tests verify the full flow.
- **Files to modify:** `internal/querier/handler.go`, `internal/querier/handler_test.go`
- **Verification:** `go test ./internal/querier/... -v -run TestHandler_Success`
- **Commit message:** `feat(querier): implement query execution with NDJSON streaming`

### Task 4: Querier CLI command
- **Outcome:** Cobra `querier` subcommand with flags: `--port` (8082), `--admin-port` (9102), `--storage-backend`, `--storage-path`, `--s3-*`. Wires config -> storage -> index store -> block reader -> handler -> http.Server. Admin server with /metrics and pprof. Graceful shutdown on SIGINT/SIGTERM.
- **Files to create:** `cmd/querier.go`
- **Verification:** `go build . && ./ingest querier --help` (verify flags and usage)
- **Commit message:** `feat(querier): add querier CLI command with graceful shutdown`

### Task 5: Integration wiring and vet
- **Outcome:** Ensure all packages compile together, `go vet` passes, and the querier subcommand starts and stops cleanly.
- **Files to modify:** (possibly minor fixes across querier files)
- **Verification:** `go vet ./... && go test ./internal/querier/... -v && go build .`
- **Commit message:** `chore(querier): fix lint and ensure clean build`

---

## Benchmark Plan + "Success"
N/A for this role. The handler is a thin HTTP layer. Read-path benchmarks are owned by query-bench-engineer (Phase 8). Success for this role is defined by:
- All unit tests pass.
- `go vet ./...` is clean.
- The querier starts, accepts queries, and shuts down gracefully.
- NDJSON streaming works correctly with mocked backends.

---

## CLAUDE.md contributions (proposed, do NOT write the file)

## From query-api-engineer
- The querier is started with `go run . querier --port 8082`
- Query endpoint: `GET /api/v1/query?start=<RFC3339>&end=<RFC3339>&labels=<matchers>` with `X-Scope-OrgID` header
- Labels format: comma-separated `key=value` (exact match) or `key=~regex` (regex match)
- Response format: NDJSON (one JSON event per line), Content-Type: `application/x-ndjson`
- Admin metrics on port 9102 by default
- Run tests: `go test ./internal/querier/... -v`
- Before you commit: `go vet ./... && go test ./internal/querier/... -v`
- Do not change the NDJSON response format without updating all handler tests

---

## EXPLAIN.md contributions (proposed outline bullets)

- **Query flow:** Client sends GET /api/v1/query with tenant, time range, and label matchers -> Handler validates inputs -> Enumerates dates in range -> For each date, calls IndexStore.Lookup to get matching block paths -> Passes all block paths to BlockReader.Read -> Streams events as NDJSON
- **Key decisions:** NDJSON over JSON array for memory-efficient streaming; interface-driven design for testability; hand-rolled mocks matching codebase conventions
- **Matcher syntax:** `key=value` for exact match, `key=~regex` for regex match, comma-separated for multiple matchers (AND semantics)
- **Limits of MVP:** No query caching, no result pagination, no query timeout configuration (uses context cancellation only), no aggregation (raw events only)
- **How to run locally:** `go run . querier --port 8082 --storage-path ./data`
- **How to validate:** `curl -H "X-Scope-OrgID: tenant-a" "http://localhost:8082/api/v1/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&labels=service=api"`

---

## READY FOR APPROVAL
