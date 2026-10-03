# Block Reader Engineer Plan

## You are in PLAN MODE.

### Project
I want to build a **parallel block reader** for the query read path.

**Goal:** build a **concurrent block reader, k-way merge, and NDJSON streaming writer** in which we **read matching blocks from object storage in parallel, filter events by label matchers, merge results in timestamp order, and stream them to HTTP clients as newline-delimited JSON**.

### Role + Scope
- **Role:** Block Reader Engineer
- **Scope:** I own `internal/querier/reader.go`, `internal/querier/merge.go`, `internal/querier/writer.go`, and their corresponding test files. I do NOT own the index package, the query API handler, the CLI command, or the ingester flush path.
- **File you will write:** `/docs/query-foundation/plans/block-reader-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- **FR1:** `BlockReader` reads blocks from `ObjectStore` in parallel using a bounded worker pool (default 8 workers), deserializes JSON batches, applies label matchers to filter events, and sends matching events to an output channel.
- **FR2:** `MergeStreams` performs k-way merge of multiple event channels ordered by timestamp using a min-heap. Output is a single channel of time-ordered events.
- **FR3:** `StreamJSON` writes events from a channel to an `http.ResponseWriter` as newline-delimited JSON (one JSON object per line), flushing periodically. Sets `Content-Type: application/x-ndjson`. Handles context cancellation for client disconnect.
- **FR4:** All functions accept `context.Context` and honor cancellation — workers stop, channels close cleanly, no goroutine leaks.
- **Tests required:** unit tests for reader (mock storage), merge ordering (multiple streams, edge cases), streaming writer (httptest recorder).

## Non-Functional Requirements
- Language/runtime: Go (matches existing codebase)
- Local dev: `go test ./internal/querier/...`
- Observability: N/A for MVP (metrics added by query-api-engineer in handler layer)
- Safety: context cancellation propagates to all goroutines; bounded concurrency prevents resource exhaustion; channels are always closed by producers
- Documentation: CLAUDE.md + EXPLAIN.md contributions proposed below
- Performance: benchmarks for block reading and merge throughput

---

## Assumptions / System Model
- **Deployment environment:** same as existing services; querier runs as a standalone process
- **Failure modes:** storage read failure (network timeout, missing key), context cancellation (client disconnect, server shutdown), malformed JSON in stored blocks
- **Delivery guarantees:** read-only path; no ordering guarantees beyond the merge sort by timestamp
- **Multi-tenancy:** tenant filtering is handled upstream by the query handler (only passes relevant block paths); the reader itself is tenant-agnostic

---

## Data Model

### Input: Stored Batch (JSON in Object Storage)
Each block file is a JSON-encoded `event.Batch`:
```go
type Batch struct {
    TenantID  string    `json:"tenant_id"`
    Partition int32     `json:"partition"`
    Events    []Event   `json:"events"`
    CreatedAt time.Time `json:"created_at"`
}
```

### Input: Event (from `internal/event/event.go`)
```go
type Event struct {
    TenantID  string            `json:"tenant_id"`
    Timestamp time.Time         `json:"timestamp"`
    Labels    map[string]string `json:"labels"`
    Value     float64           `json:"value"`
    Payload   []byte            `json:"payload,omitempty"`
}
```

### Input: LabelMatcher (from index package, provided by index-engineer)
```go
type LabelMatcher struct {
    Name  string
    Value string
    Type  MatchType // Exact, Regex, NotEqual, NotRegex
}
```
The block reader will import `LabelMatcher` and its `Match(labels)` method from `internal/index/matcher.go`.

### Output: Filtered, time-ordered events streamed as NDJSON

- Validation rules: events with zero timestamps are included in output (filtering is by labels only; time range filtering is done at the index level)
- Versioning: N/A for MVP
- Persistence: read-only; no writes

---

## APIs

### BlockReader
```go
// NewBlockReader creates a reader with the given concurrency.
func NewBlockReader(store storage.ObjectStore, workers int) *BlockReader

// ReadBlocks reads blocks in parallel, filters by matchers, returns event and error channels.
// The event channel is closed when all blocks are processed.
// The error channel receives non-fatal per-block errors and is closed when done.
func (r *BlockReader) ReadBlocks(ctx context.Context, paths []string, matchers []LabelMatcher) (<-chan event.Event, <-chan error)
```

### MergeStreams
```go
// MergeStreams merges multiple sorted event channels into a single sorted channel.
// Uses a min-heap keyed on Event.Timestamp.
// The output channel is closed when all input channels are drained.
func MergeStreams(streams ...<-chan event.Event) <-chan event.Event
```

### StreamJSON
```go
// StreamJSON writes events as newline-delimited JSON to w.
// Flushes after each event (if w implements http.Flusher).
// Returns nil on success, context error on cancellation.
func StreamJSON(ctx context.Context, w http.ResponseWriter, events <-chan event.Event) error
```

---

## Architecture / Component Boundaries

### Components I own
- **`internal/querier/reader.go`** — `BlockReader` struct. Owns the worker pool lifecycle. Each worker: `store.Get(path)` -> `json.Unmarshal` into `event.Batch` -> iterate events -> apply matchers -> send to output channel.
- **`internal/querier/merge.go`** — `MergeStreams` function. Stateless. Creates a goroutine that reads from N input channels using `container/heap`, pushes to one output channel.
- **`internal/querier/writer.go`** — `StreamJSON` function. Stateless. Reads from event channel, encodes each event as JSON line, writes + flushes to ResponseWriter.

### How these connect to the query-api-engineer's handler
The handler (owned by query-api-engineer) will:
1. Look up matching block paths from the index
2. Call `BlockReader.ReadBlocks(ctx, paths, matchers)` to get an event channel
3. Call `StreamJSON(ctx, w, events)` to stream to the client

`MergeStreams` is available if the handler wants to merge results from multiple date ranges, but the simpler approach is to pass all paths to a single `ReadBlocks` call (events are merge-sorted internally within the reader).

### Concurrency model
- Worker pool: `workers` goroutines (default 8) read from a shared `paths` channel. A `sync.WaitGroup` tracks completion. When all workers finish, the output event channel is closed.
- Backpressure: the event channel is unbuffered (or small buffer, e.g., 256). If the consumer (StreamJSON / handler) is slow, workers block on channel send, naturally throttling reads.
- Cancellation: all workers check `ctx.Done()` before each block read and before each channel send.

### Design note: MergeStreams as separate utility
`MergeStreams` is intentionally separate from `BlockReader` so the handler can merge across multiple `ReadBlocks` calls (e.g., across day boundaries). Within a single `ReadBlocks` call, events arrive unordered (parallel workers), so the handler should either:
- Use a single `ReadBlocks` and collect + sort, or
- Split paths by day, call `ReadBlocks` per day, and `MergeStreams` the results

The recommended pattern for the handler is: split by day, call `ReadBlocks` per day (each day's events are unordered), then `MergeStreams`. But to keep it simple for MVP, we also support a `ReadBlocksSorted` convenience wrapper that internally collects, sorts, and streams.

**Revised decision:** For simplicity in MVP, `ReadBlocks` returns events in arbitrary order. The handler can either sort them or use `MergeStreams` if it partitions by day. This keeps the reader simple and composable.

---

## Correctness Invariants
1. **No goroutine leaks:** every goroutine started by `ReadBlocks` and `MergeStreams` must exit when the context is cancelled or all inputs are consumed. Verified by `goleak` or counting goroutines before/after in tests.
2. **Channel close guarantees:** the event channel returned by `ReadBlocks` is always closed exactly once, regardless of errors or cancellation.
3. **Error isolation:** a single block read failure does not abort the entire read. The error is sent to the error channel; other workers continue. The caller decides how to handle partial failures.
4. **Merge ordering:** `MergeStreams` output is strictly non-decreasing by `Event.Timestamp`. Verified by test assertion on output ordering.
5. **Bounded concurrency:** at most `workers` goroutines perform storage reads concurrently. No unbounded goroutine creation.
6. **NDJSON format:** `StreamJSON` output is valid NDJSON (one JSON object per line, newline terminated). No trailing comma, no array wrapper.

---

## Tests

### Unit tests
- **`internal/querier/reader_test.go`:**
  - `TestReadBlocks_BasicFilter` — 3 blocks, 2 matchers, verify only matching events are returned
  - `TestReadBlocks_EmptyPaths` — no paths, channel closes immediately
  - `TestReadBlocks_StorageError` — one block fails, others succeed, error channel receives error
  - `TestReadBlocks_ContextCancel` — cancel context mid-read, verify workers exit, no goroutine leak
  - `TestReadBlocks_AllEventsMatch` — no matchers, all events pass through
  - Mock: in-memory `ObjectStore` implementation returning pre-encoded JSON batches

- **`internal/querier/merge_test.go`:**
  - `TestMergeStreams_TwoStreams` — two sorted channels, verify merged output is sorted
  - `TestMergeStreams_EmptyStreams` — all empty, output closes immediately
  - `TestMergeStreams_SingleStream` — passthrough
  - `TestMergeStreams_DuplicateTimestamps` — equal timestamps, both events present in output

- **`internal/querier/writer_test.go`:**
  - `TestStreamJSON_BasicOutput` — 3 events, verify NDJSON format in httptest.ResponseRecorder
  - `TestStreamJSON_EmptyChannel` — no events, empty body, no error
  - `TestStreamJSON_ContentType` — verify `application/x-ndjson` header
  - `TestStreamJSON_ContextCancel` — cancel context, verify function returns context error

### Commands
```bash
go test ./internal/querier/... -v
go test ./internal/querier/... -race
go test ./internal/querier/... -bench . -benchmem
```

---

## Benchmarks + "Success"
- **BenchmarkReadBlocks:** 100 blocks, 1000 events each, 50% match rate. Measure events/sec and allocs/op.
  - Target: >= 500k events/sec with 8 workers on local filesystem storage
- **BenchmarkMergeStreams:** 8 streams, 10k events each. Measure throughput and allocs.
  - Target: >= 1M events/sec merge throughput (heap operations are O(log K) where K=8)
- **BenchmarkStreamJSON:** 100k events. Measure MB/sec and allocs.
  - Target: >= 50 MB/sec NDJSON encoding throughput

Commands:
```bash
go test ./internal/querier/... -bench . -benchmem -benchtime=5s
```

---

## Engineering Decisions & Tradeoffs

### Decision 1: Channel-based streaming vs. callback/iterator pattern
- **Decision:** Use Go channels (`<-chan event.Event`) as the streaming primitive between reader, merge, and writer.
- **Alternatives considered:** (1) Iterator interface with `Next() (Event, bool)` — simpler, no goroutine management. (2) Callback `func(Event) error` — push-based, simple but harder to compose.
- **Why:** Channels compose naturally with `select` for cancellation, fan-in for merge, and the Go concurrency model. The handler can easily pass the channel from reader to writer. Channels also provide natural backpressure.
- **Tradeoff acknowledged:** Channels add goroutine overhead and make debugging harder (stack traces show channel operations). The iterator pattern would be simpler to debug but harder to parallelize. Since parallel reads are the core value of this component, channels are the right choice.

### Decision 2: Worker pool with shared path channel vs. goroutine-per-block
- **Decision:** Fixed-size worker pool (N goroutines reading from a shared paths channel) rather than spawning one goroutine per block path.
- **Alternatives considered:** (1) Goroutine per block with a semaphore for concurrency control — simpler code, same concurrency bound. (2) `errgroup` with `SetLimit` — well-known pattern.
- **Why:** The worker pool pattern (N long-lived goroutines pulling from a work channel) is more efficient for large path lists — no goroutine creation overhead per block. It also makes cancellation simpler: close the paths channel to signal "no more work."
- **Tradeoff acknowledged:** Slightly more code than `errgroup.SetLimit`, but more explicit control over lifecycle and more consistent with distributed systems patterns the codebase follows.

### Decision 3: Min-heap merge vs. simple sort-after-collect
- **Decision:** Use `container/heap` for k-way merge of sorted streams.
- **Alternatives considered:** (1) Collect all events into a slice and `sort.Slice` by timestamp — simpler, works fine for small result sets. (2) Tournament tree — optimal but overkill.
- **Why:** The heap-based merge is streaming (O(1) memory per stream, O(N log K) total) and starts producing output immediately. Sort-after-collect requires O(N) memory and cannot stream results until all blocks are read.
- **Tradeoff acknowledged:** The heap merge only works if input streams are individually sorted. Since blocks from parallel workers may not be sorted, the handler must partition reads (e.g., by day) to get sorted sub-streams. For MVP, if the handler passes all paths to one `ReadBlocks` call, it may need to sort the output. The merge utility is available for the optimized path.

### Decision 4: Flush per event vs. periodic flush in StreamJSON
- **Decision:** Flush after every event write (if `http.Flusher` is available).
- **Alternatives considered:** (1) Flush every N events or every T milliseconds — better throughput, higher latency to first byte. (2) Buffer everything, flush at end — not streaming.
- **Why:** Streaming queries benefit from low time-to-first-byte. The client can start processing events immediately. Per-event flush maximizes responsiveness.
- **Tradeoff acknowledged:** Per-event flush has higher syscall overhead than batched flushing. For very large result sets, this could reduce throughput by 10-20%. Can be optimized later with a configurable flush interval if needed.

---

## Risks & Mitigations

### Risk 1: LabelMatcher type not available yet
- **Risk:** The `LabelMatcher` type and `Match` method are built by index-engineer. If the interface differs from what this plan assumes, the reader code will need rework.
- **Impact:** Blocked on compilation; tests won't pass until matcher types exist.
- **Mitigation:** Define a local `Matcher` interface in `reader.go` that the index package's `LabelMatcher` must satisfy. This decouples compilation. If index-engineer's API differs, only the interface adapter needs changing.
- **Validation time:** 5 minutes — check if `internal/index/matcher.go` exists; if not, define the local interface.

### Risk 2: Batch JSON format assumptions
- **Risk:** The reader assumes stored blocks are JSON-encoded `event.Batch` structs (based on `flusher.go`). If the format changes (e.g., compactor introduces a new format), the reader will fail.
- **Impact:** Deserialization errors at read time; no events returned.
- **Mitigation:** Validate assumption by reading `flusher.go` (confirmed: `json.NewEncoder(buf).Encode(job.batch)` writes `event.Batch`). Add a clear error message on deserialize failure so format mismatches are obvious.
- **Validation time:** 2 minutes — already confirmed by reading flusher.go.

### Risk 3: Goroutine leak under partial failure
- **Risk:** If a worker panics or a channel send blocks forever (e.g., no consumer), goroutines leak.
- **Impact:** Memory leak, test failures, production resource exhaustion.
- **Mitigation:** (1) Use `defer` + recover in each worker. (2) All channel sends are wrapped in `select` with `ctx.Done()`. (3) Add `TestReadBlocks_ContextCancel` that verifies goroutine count returns to baseline. (4) Run tests with `-race`.
- **Validation time:** 5 minutes — run tests with `-race` flag and check for goroutine leaks.

### Risk 4: Large blocks causing memory spikes
- **Risk:** A single block could contain a very large batch (e.g., 100k events). Deserializing the entire block into memory before filtering could spike memory usage.
- **Impact:** OOM under high query load with large blocks.
- **Mitigation:** For MVP, accept this risk — batch sizes are bounded by the ingester's flush threshold (1000 events per batch). Document the assumption. Future optimization: streaming JSON decoder that yields events one at a time.
- **Validation time:** 3 minutes — check ingester batch size config to confirm 1000-event limit.

---

## Recommended API Surface

### Functions/Types

| Package | Type/Function | Behavior |
|---------|--------------|----------|
| `internal/querier` | `BlockReader` struct | Holds `ObjectStore` reference and worker count |
| `internal/querier` | `NewBlockReader(store, workers)` | Constructor; defaults workers to 8 if <= 0 |
| `internal/querier` | `ReadBlocks(ctx, paths, matchers)` | Returns `(<-chan Event, <-chan error)`; spawns worker pool |
| `internal/querier` | `MergeStreams(streams...)` | Returns `<-chan Event`; heap-based k-way merge |
| `internal/querier` | `StreamJSON(ctx, w, events)` | Writes NDJSON to ResponseWriter; returns error on ctx cancel |

### Exact behavior spec
- `ReadBlocks`: If `paths` is empty, both channels close immediately. If `matchers` is nil/empty, all events pass. Each worker reads one block at a time from the path queue. Storage errors are sent to the error channel (not fatal). The event channel has a buffer of 256 to reduce contention.
- `MergeStreams`: If no streams provided, returns a closed channel. If one stream, returns it directly (no goroutine). For 2+ streams, spawns one goroutine with a min-heap.
- `StreamJSON`: Sets `Content-Type: application/x-ndjson` header before first write. Uses `json.NewEncoder` for each event. Calls `Flusher.Flush()` after each event if available.

---

## Folder Structure

```
internal/querier/
  reader.go          — BlockReader, NewBlockReader, ReadBlocks
  reader_test.go     — unit tests + benchmark for reader
  merge.go           — MergeStreams, eventHeap (min-heap)
  merge_test.go      — unit tests + benchmark for merge
  writer.go          — StreamJSON
  writer_test.go     — unit tests + benchmark for writer
```

Owned by: **block-reader-engineer**

Not owned (but depended on):
- `internal/event/event.go` — Event, Batch types (read-only dependency)
- `internal/storage/storage.go` — ObjectStore interface (read-only dependency)
- `internal/index/matcher.go` — LabelMatcher type (dependency on index-engineer)

---

## Step-by-Step Task Plan

### Task 1: Scaffold querier package with reader and mock storage
- **Outcome:** `BlockReader` struct, `NewBlockReader` constructor, `ReadBlocks` method with worker pool, label filtering. In-memory mock `ObjectStore` for tests. Tests pass.
- **Files to create:** `internal/querier/reader.go`, `internal/querier/reader_test.go`
- **Verification:**
  ```bash
  go test ./internal/querier/... -v -run TestReadBlocks
  go test ./internal/querier/... -race -run TestReadBlocks
  ```
- **Commit message:** `feat(querier): add parallel block reader with worker pool and label filtering`

### Task 2: Implement k-way merge with min-heap
- **Outcome:** `MergeStreams` function using `container/heap`. Merges N sorted event channels into one sorted output channel. Tests verify ordering with multiple streams, edge cases.
- **Files to create:** `internal/querier/merge.go`, `internal/querier/merge_test.go`
- **Verification:**
  ```bash
  go test ./internal/querier/... -v -run TestMergeStreams
  go test ./internal/querier/... -race -run TestMergeStreams
  ```
- **Commit message:** `feat(querier): add k-way merge for sorted event streams using min-heap`

### Task 3: Implement NDJSON streaming writer
- **Outcome:** `StreamJSON` function that writes events as newline-delimited JSON, sets correct Content-Type, flushes per event, handles context cancellation. Tests with httptest recorder.
- **Files to create:** `internal/querier/writer.go`, `internal/querier/writer_test.go`
- **Verification:**
  ```bash
  go test ./internal/querier/... -v -run TestStreamJSON
  go test ./internal/querier/... -race -run TestStreamJSON
  ```
- **Commit message:** `feat(querier): add NDJSON streaming writer for HTTP responses`

### Task 4: Add benchmarks for reader, merge, and writer
- **Outcome:** Benchmarks in each test file. Reader: 100 blocks x 1000 events. Merge: 8 streams x 10k events. Writer: 100k events. All pass with reasonable throughput.
- **Files to modify:** `internal/querier/reader_test.go`, `internal/querier/merge_test.go`, `internal/querier/writer_test.go`
- **Verification:**
  ```bash
  go test ./internal/querier/... -bench . -benchmem -benchtime=5s
  ```
- **Commit message:** `bench(querier): add throughput benchmarks for reader, merge, and writer`

### Task 5: Integration wiring and cross-component test
- **Outcome:** One integration-style test that wires reader -> merge -> writer end-to-end: create mock blocks, read + filter, merge streams, write NDJSON, verify output is valid sorted NDJSON with only matching events.
- **Files to create/modify:** `internal/querier/reader_test.go` (add `TestReadBlocks_EndToEnd`)
- **Verification:**
  ```bash
  go test ./internal/querier/... -v -run TestReadBlocks_EndToEnd
  go vet ./internal/querier/...
  ```
- **Commit message:** `test(querier): add end-to-end integration test for read-filter-merge-stream pipeline`

---

## CLAUDE.md contributions (proposed content, do NOT write the file)

## From block-reader-engineer
### Coding style
- Channel producers always close the channel; consumers never close
- All goroutines must be context-aware: wrap channel sends in `select { case ch <- v: case <-ctx.Done(): return }`
- Worker pools use `sync.WaitGroup` + deferred `close(ch)` after `wg.Wait()`
- Error channels carry `error` values for non-fatal per-block failures; callers decide policy

### Dev commands
```bash
# Run querier tests
go test ./internal/querier/... -v

# Race detection
go test ./internal/querier/... -race

# Benchmarks
go test ./internal/querier/... -bench . -benchmem -benchtime=5s
```

### Before you commit
- [ ] All tests pass: `go test ./internal/querier/... -race`
- [ ] No goroutine leaks: context cancellation tests pass
- [ ] `go vet ./internal/querier/...` clean
- [ ] Benchmark numbers recorded in commit message for performance changes

---

## EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- Query handler receives request -> looks up matching blocks from index -> passes block paths to `BlockReader.ReadBlocks()`
- `ReadBlocks` spawns N worker goroutines (default 8) that pull paths from a shared channel, read blocks from storage, deserialize, filter by label matchers, and push matching events to an output channel
- If results span multiple days, handler calls `ReadBlocks` per day and `MergeStreams` combines them in timestamp order using a min-heap
- `StreamJSON` drains the event channel and writes each event as a JSON line to the HTTP response, flushing after each write for low latency

### Key Engineering Decisions + Tradeoffs
- Channels over iterators: natural Go concurrency composition, but harder to debug
- Fixed worker pool over goroutine-per-block: lower overhead for large block counts
- Min-heap merge: streaming O(N log K) vs. collect-and-sort O(N log N) — starts producing results immediately
- Per-event flush: optimizes time-to-first-byte at the cost of throughput for large result sets

### Limits of MVP + Next Steps
- Events within a single `ReadBlocks` call are unordered (parallel workers); handler must sort or partition by day for sorted output
- No streaming JSON decoder — entire block deserialized into memory (safe because ingester limits batch size to 1000 events)
- No connection pooling or block caching (future: LRU block cache in Phase 7)
- No result size limits or pagination (future: add `limit` query parameter)

### How to Run Locally + Validate
```bash
go test ./internal/querier/... -v -race
go test ./internal/querier/... -bench . -benchmem
```

---

## READY FOR APPROVAL
