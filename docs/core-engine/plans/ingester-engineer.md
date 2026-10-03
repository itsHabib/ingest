# Ingester Engineer Plan

## You are in PLAN MODE.

### Project
I want to build a **stateless Kafka ingester** for the multi-tenant ingestion engine.

**Goal:** build a **Kafka consumer + in-memory batcher + storage flusher** in which we **consume events from Kafka, batch them per tenant+partition, and flush to object storage with at-least-once semantics**.

### Role + Scope (fill in)
- **Role:** Ingester Engineer (Kafka consumer, batcher, flusher)
- **Scope:** I own `internal/ingester/` (batcher, flusher, consumer) and `cmd/ingester.go`. I do NOT own the storage interface (`internal/storage/`), the hash ring (`internal/ring/`), the event model (`internal/event/`), or the gateway (`internal/gateway/`).
- **File I will write:** `/docs/core-engine/plans/ingester-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- **FR1:** In-memory batcher accumulates events keyed by `tenantID:partition`, flushing when batch size >= 1000 events OR batch age >= 10 seconds.
- **FR2:** Flusher serializes batches to JSON and writes to ObjectStore using the key format `<tenantID>/<YYYY-MM-DD>/<partition>-<offset>.json`. Supports retry with exponential backoff.
- **FR3:** Consumer runs a franz-go consumer group (`ingestion-consumers`) with manual offset commit — commits ONLY after successful flush (at-least-once semantics).
- **FR4:** Graceful shutdown: stop consuming, flush all pending batches, commit final offsets, exit.
- **FR5:** Cobra `ingester` subcommand wires config -> storage -> flusher -> batcher -> consumer with signal handling.
- **Tests required:** unit tests for batcher (size/time triggers, FlushAll), flusher (success, retry with mock store), consumer (wiring test).
- **Metrics required:** N/A for this phase (observability-engineer scope in Production phase).

## Non-Functional Requirements
- Language/runtime: Go 1.25+, franz-go for Kafka, cobra for CLI
- Local dev: `docker compose up -d` for Kafka, then `go run . ingester`
- Observability: structured logging via `go.uber.org/zap` (matching existing codebase)
- Safety: at-least-once delivery; exponential backoff on flush failures; no data loss on graceful shutdown
- Documentation: CLAUDE.md + EXPLAIN.md contributions below
- Performance: batcher and flusher are hot paths — minimize allocations, avoid holding locks during I/O

---

## Assumptions / System Model
- **Deployment environment:** local docker-compose; single Kafka broker, 6 partitions
- **Failure modes:**
  - Process crash mid-batch: uncommitted offsets, Kafka replays on restart (at-least-once, duplicates possible)
  - ObjectStore write failure: retry with backoff (100ms, 200ms, 400ms), drop batch after max retries and log error
  - Kafka unavailable: consumer poll returns error, loop retries naturally
- **Delivery guarantees:** at-least-once. Duplicates possible on crash/restart. Ordering within a partition is preserved.
- **Multi-tenancy:** events are keyed by `tenantID:partition` for batching; storage keys are tenant-isolated by path prefix.

---

## Data Model (as relevant to your role)

The ingester consumes events produced by the gateway. The gateway marshals `[]event.Event` as JSON and produces with tenant ID as the Kafka record key.

**Batcher key:** `fmt.Sprintf("%s:%d", tenantID, partition)` — uniquely identifies a batch in the pending map.

**Batch struct** (from `internal/event/event.go`):
- `TenantID string`, `Partition int32`, `Events []Event`, `CreatedAt time.Time`, `SizeBytes int`
- Already has `Add(event)`, `Len()` methods

**Storage key format** (from storage-engineer's `BatchKey` helper):
- `<tenantID>/<YYYY-MM-DD>/<partition>-<offset padded 10 digits>.json`

**Offset tracking:** The flusher needs the Kafka offset of the last event in the batch to generate the storage key and to know what to commit. The `Batch` struct does not currently carry an `Offset` field. Two options:
1. Add an `Offset int64` field to the `event.Batch` struct (cleanest, but touches model-engineer's code)
2. Track offset externally in the consumer via a wrapper struct

**Decision:** Use a wrapper struct in the ingester package to avoid modifying the shared model. This keeps the ingester self-contained:
```go
type flushJob struct {
    batch  *event.Batch
    offset int64       // last Kafka offset in this batch
    topic  string
    part   int32
}
```

- **Validation rules:** Events are already validated by the gateway before Kafka. The consumer should handle malformed records gracefully (log and skip).
- **Versioning:** N/A for MVP.

---

## APIs (as relevant to your role)

### Batcher API
```go
func NewBatcher(maxSize int, maxAge time.Duration) *Batcher
func (b *Batcher) Add(ev event.Event, partition int32, offset int64)
func (b *Batcher) Start(ctx context.Context)    // background age-check goroutine
func (b *Batcher) FlushAll()                     // drain all pending batches
func (b *Batcher) FlushChan() <-chan *flushJob   // read-only channel for flush workers
```

### Flusher API
```go
func NewFlusher(store storage.ObjectStore, logger *zap.Logger) *Flusher
func (f *Flusher) Flush(ctx context.Context, job *flushJob) error
func (f *Flusher) FlushWithRetry(ctx context.Context, job *flushJob, maxRetries int) error
```

### Consumer API
```go
func NewConsumer(cfg config.IngesterConfig, batcher *Batcher, flusher *Flusher, logger *zap.Logger) (*Consumer, error)
func (c *Consumer) Run(ctx context.Context) error
```

### CLI
```
ingest ingester [flags]
  --kafka-brokers    Comma-separated broker addresses (default "localhost:9092")
  --kafka-topic      Kafka topic to consume (default "ingestion-events")
  --consumer-group   Consumer group name (default "ingestion-ingesters")
  --batch-size       Max events per batch before flush (default 1000)
  --batch-age        Max batch age before flush (default "10s")
  --storage-path     Base directory for filesystem store (default "./data")
  --flush-workers    Number of concurrent flush goroutines (default 4)
```

---

## Architecture / Component Boundaries (as relevant)

```
                    Kafka
                      |
                      v
              +---------------+
              |   Consumer    |  - franz-go consumer group, manual commit
              |   (Run loop)  |  - polls records, deserializes events
              +-------+-------+
                      |
                      v
              +---------------+
              |    Batcher    |  - pending map[string]*flushJob (tenantID:partition)
              |               |  - size trigger (>= maxSize) and age trigger (>= maxAge)
              +-------+-------+
                      |
                flushCh (buffered chan, size 100)
                      |
          +-----------+-----------+
          v           v           v
     +--------+  +--------+  +--------+
     | Worker | | Worker | | Worker |   (N=4 flush workers)
     +---+----+  +---+----+  +---+----+
         |           |           |
         v           v           v
     +-------------------------------+
     |           Flusher             |  - JSON serialize, generate key, store.Put
     |                               |  - retry with exponential backoff
     +-------------------------------+
                      |
                      v
              +---------------+
              |  ObjectStore  |  (storage-engineer's interface)
              +---------------+
                      |
              commit offset to Kafka (after successful flush)
```

**Concurrency model:**
- 1 consumer goroutine: polls Kafka, routes events to batcher
- 1 batcher background goroutine: ticks every 1s, checks age-triggered flushes
- N flush worker goroutines (default 4): read from flushCh, call flusher, then commit offset
- Batcher lock (`sync.Mutex`) only held during Add/flush-check (no I/O under lock)

**Backpressure strategy:**
- `flushCh` is buffered (100). If full, `Add()` blocks, which blocks the consumer poll loop, which applies natural Kafka-level backpressure (consumer stops fetching).
- This is intentional: if ObjectStore is slow, the pipeline slows down rather than OOMing.

---

## Correctness Invariants (must be explicit)

1. **Never commit before flush.** Kafka offset is committed ONLY after `store.Put` succeeds. Violation = data loss.
2. **Batch isolation.** A batch for `tenantA:partition-3` never contains events from `tenantB` or `partition-5`.
3. **FlushAll drains everything.** After `FlushAll()` returns, `pending` map is empty and all jobs are on `flushCh`.
4. **No duplicate sends to flushCh.** Once a batch is sent to `flushCh`, it is removed from `pending`. The reference is handed off, not shared.
5. **Graceful shutdown ordering.** Stop consuming -> FlushAll -> drain flushCh -> commit remaining -> return. No goroutine leaks.
6. **Backpressure propagation.** If flushCh is full, Add blocks, consumer stops polling, Kafka applies consumer-level backpressure.

---

## Tests

### Unit Tests

**`internal/ingester/batcher_test.go`:**
- `TestBatcher_SizeTriggeredFlush` — add maxSize events, verify batch appears on flushCh
- `TestBatcher_TimeTriggeredFlush` — add 1 event, wait > maxAge, verify flush
- `TestBatcher_FlushAll` — add events to multiple keys, call FlushAll, verify all appear on flushCh
- `TestBatcher_MultiTenantIsolation` — add events for 3 tenants, verify separate batches
- `TestBatcher_BackpressureBlocking` — fill flushCh, verify Add blocks (goroutine with timeout)

**`internal/ingester/flusher_test.go`:**
- `TestFlusher_Success` — mock ObjectStore, flush a batch, verify key and data written
- `TestFlusher_RetryOnError` — mock ObjectStore that fails twice then succeeds, verify 3 calls
- `TestFlusher_RetryExhausted` — mock ObjectStore that always fails, verify error returned after maxRetries
- `TestFlusher_CorrectStorageKey` — verify key matches `<tenantID>/<date>/<partition>-<offset>.json`

**`internal/ingester/consumer_test.go`:**
- `TestConsumer_Wiring` — verify NewConsumer returns non-nil with valid config (no Kafka needed)
- Integration testing with real Kafka deferred to integration-test-engineer

### Commands
```bash
go test ./internal/ingester/... -v -count=1
go test ./internal/ingester/... -run TestBatcher
go test ./internal/ingester/... -run TestFlusher
go vet ./internal/ingester/...
```

---

## Benchmarks + "Success"

**What to measure:**
- `BenchmarkBatcher_Add` — throughput of adding events to batcher (no I/O)
- `BenchmarkFlusher_Flush` — serialize + write overhead per batch (using in-memory mock store)

**Target success criteria:**
- Batcher Add: >= 500,000 events/sec (it's just a map append + size check)
- Flusher Flush of 1000 events: < 5ms (JSON marshal + mock store Put)

**Benchmark commands:**
```bash
go test ./internal/ingester/... -bench . -benchmem -count=3
```

---

# Please produce (no code yet):

## 1) Recommended API Surface

| Package | Function/Method | Behavior |
|---------|----------------|----------|
| `ingester` | `NewBatcher(maxSize, maxAge)` | Creates batcher with empty pending map and buffered flushCh(100) |
| `ingester` | `Batcher.Add(event, partition, offset)` | Lock, get/create batch for key, append, size-check flush, unlock |
| `ingester` | `Batcher.Start(ctx)` | Background goroutine: tick 1s, flush age-expired batches. Returns on ctx.Done |
| `ingester` | `Batcher.FlushAll()` | Lock, move all pending batches to flushCh, clear map, unlock |
| `ingester` | `Batcher.FlushChan()` | Returns `<-chan *flushJob` for workers to read |
| `ingester` | `NewFlusher(store, logger)` | Creates flusher with ObjectStore dependency |
| `ingester` | `Flusher.Flush(ctx, job)` | Serialize batch JSON, build key, store.Put, log metrics |
| `ingester` | `Flusher.FlushWithRetry(ctx, job, maxRetries)` | Retry Flush with exponential backoff (100ms base, 2x) |
| `ingester` | `NewConsumer(cfg, batcher, flusher, logger)` | Create franz-go consumer group client with manual commit |
| `ingester` | `Consumer.Run(ctx)` | Start batcher, start flush workers, poll loop, graceful shutdown |
| `cmd` | `ingester` subcommand | Wire config -> filesystem store -> flusher -> batcher -> consumer, signal handling |

## 2) Folder Structure

```
internal/ingester/
  batcher.go          # Batcher struct, Add, Start, FlushAll, FlushChan
  batcher_test.go     # Size/time/FlushAll/isolation/backpressure tests
  flusher.go          # Flusher struct, Flush, FlushWithRetry
  flusher_test.go     # Success/retry/exhausted/key-format tests
  consumer.go         # Consumer struct, NewConsumer, Run
  consumer_test.go    # Wiring test
cmd/
  ingester.go         # Cobra subcommand, flags, wiring, signal handling
```

**Owned by ingester-engineer:** all files above.
**Dependencies (read-only):** `internal/event/`, `internal/storage/`, `internal/config/`, `cmd/root.go`.

## 3) Step-by-Step Task Plan

See "Tighten the plan" section below.

## 4) Benchmark Plan

- **Batcher.Add:** benchmark adding 1/10/1000/10000 events to measure throughput and allocs
- **Flusher.Flush:** benchmark serializing+writing a 1000-event batch to an in-memory mock store
- **Success:** Batcher Add >= 500k events/sec, Flusher < 5ms per 1000-event batch
- **Command:** `go test ./internal/ingester/... -bench . -benchmem -count=3`

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Batcher core — Add + size-triggered flush
- **Outcome:** `Batcher` struct with `NewBatcher`, `Add`, `FlushChan`, and size-triggered flushing work correctly.
- **Files to create:** `internal/ingester/batcher.go`, `internal/ingester/batcher_test.go`
- **Verification:**
  ```bash
  go test ./internal/ingester/... -run TestBatcher_SizeTriggeredFlush -v
  go test ./internal/ingester/... -run TestBatcher_MultiTenantIsolation -v
  go vet ./internal/ingester/...
  ```
- **Commit:** `feat(ingester): add batcher with size-triggered flush`

### Task 2: Batcher time-trigger + FlushAll + backpressure
- **Outcome:** `Start()` background goroutine flushes age-expired batches. `FlushAll()` drains pending map. Backpressure works when flushCh is full.
- **Files to modify:** `internal/ingester/batcher.go`, `internal/ingester/batcher_test.go`
- **Verification:**
  ```bash
  go test ./internal/ingester/... -run TestBatcher_TimeTriggeredFlush -v
  go test ./internal/ingester/... -run TestBatcher_FlushAll -v
  go test ./internal/ingester/... -run TestBatcher_Backpressure -v
  go vet ./internal/ingester/...
  ```
- **Commit:** `feat(ingester): add time-triggered flush, FlushAll, and backpressure`

### Task 3: Flusher with retry
- **Outcome:** `Flusher` serializes batch to JSON, generates storage key, writes to ObjectStore. Retry with exponential backoff works.
- **Files to create:** `internal/ingester/flusher.go`, `internal/ingester/flusher_test.go`
- **Verification:**
  ```bash
  go test ./internal/ingester/... -run TestFlusher -v
  go vet ./internal/ingester/...
  ```
- **Commit:** `feat(ingester): add flusher with retry and exponential backoff`

### Task 4: Consumer — Kafka consumer group + flush workers
- **Outcome:** `Consumer` connects to Kafka via franz-go consumer group, polls records, routes to batcher, flush workers commit offsets after successful flush. Graceful shutdown works.
- **Files to create:** `internal/ingester/consumer.go`, `internal/ingester/consumer_test.go`
- **Verification:**
  ```bash
  go test ./internal/ingester/... -run TestConsumer -v
  go build ./...
  go vet ./internal/ingester/...
  ```
- **Commit:** `feat(ingester): add Kafka consumer with flush workers and graceful shutdown`

### Task 5: CLI ingester subcommand
- **Outcome:** `ingest ingester` cobra command with all flags, wires dependencies, handles SIGINT/SIGTERM.
- **Files to create:** `cmd/ingester.go`
- **Verification:**
  ```bash
  go build ./...
  go vet ./cmd/...
  ./ingest ingester --help  # verify flags shown
  ```
- **Commit:** `feat(cmd): add ingester subcommand`

### Task 6: Benchmarks
- **Outcome:** Benchmarks for Batcher.Add and Flusher.Flush confirm performance targets.
- **Files to modify:** `internal/ingester/batcher_test.go`, `internal/ingester/flusher_test.go`
- **Verification:**
  ```bash
  go test ./internal/ingester/... -bench . -benchmem -count=3
  ```
- **Commit:** `bench(ingester): add batcher and flusher benchmarks`

---

## Top 3 Risks/Unknowns

### Risk 1: Offset tracking in flushJob
**Risk:** The `event.Batch` struct has no `Offset` field. We need to carry the Kafka offset through to commit after flush.
**Validation:** Define a `flushJob` wrapper struct in the ingester package that carries `batch + offset + topic + partition`. Verify in Task 1 that the batcher correctly populates and passes this through flushCh. (< 5 min to validate)

### Risk 2: Franz-go consumer group manual commit API
**Risk:** Franz-go's manual offset commit API may have subtleties (e.g., needing to commit per-partition, or needing specific options to disable auto-commit).
**Validation:** Read franz-go's `CommitOffsets` and `DisableAutoCommit` docs. Write a minimal test that creates a consumer client with manual commit disabled. (< 10 min to validate)

### Risk 3: Storage-engineer interface not ready
**Risk:** The `storage.ObjectStore` interface and `storage.BatchKey` helper may not exist yet when ingester implementation begins.
**Validation:** Define a minimal mock `ObjectStore` in the test files. If the real interface isn't ready, code against the mock and adapt later. The interface is simple (5 methods) so the risk is low. (< 5 min to validate)

---

# CLAUDE.md contributions (do NOT write the file; propose content)

## From ingester-engineer

### Coding Style
- Batcher lock scope: acquire lock, do map operations, release BEFORE sending to channel. Never hold lock during I/O.
- Use `flushJob` wrapper to carry batch + offset metadata through the pipeline.
- All errors wrap with package prefix: `fmt.Errorf("batcher: %w", err)`, `fmt.Errorf("flusher: %w", err)`, `fmt.Errorf("consumer: %w", err)`.

### Dev Commands
```bash
# Run ingester (requires Kafka + storage dir)
go run . ingester --storage-path ./data

# Run ingester tests
go test ./internal/ingester/... -v -count=1

# Run benchmarks
go test ./internal/ingester/... -bench . -benchmem

# Full pipeline smoke test (gateway + ingester)
docker compose up -d
go run . gateway &
go run . ingester --storage-path ./data &
curl -X POST localhost:8080/api/v1/push \
  -H "X-Scope-OrgID: tenant-a" \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"service":"api"},"value":42.0}]'
ls ./data/tenant-a/  # verify file appeared
```

### Before You Commit Checklist
- [ ] `go test ./internal/ingester/... -v` passes
- [ ] `go vet ./internal/ingester/...` clean
- [ ] `go build ./...` succeeds
- [ ] No goroutine leaks (batcher Start returns on ctx cancel, FlushAll drains)
- [ ] At-least-once invariant: grep for any commit-before-flush patterns

### Guardrails
- NEVER commit Kafka offset before ObjectStore write succeeds
- NEVER hold batcher mutex during I/O (channel send, store write)
- flushCh buffer size is 100 — this is the backpressure valve. Do not make it unbounded.

---

# EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

### Flow / Architecture
- Consumer polls Kafka via franz-go consumer group, deserializes JSON event arrays
- Events are routed to Batcher keyed by `tenantID:partition`
- Batcher flushes on size threshold (1000) or age threshold (10s)
- Flush jobs go through a buffered channel to N worker goroutines
- Workers call Flusher which serializes to JSON, writes to ObjectStore, then commits Kafka offset
- Buffered channel provides natural backpressure: if storage is slow, consumer slows down

### Key Engineering Decisions + Tradeoffs
- **Stateless ingester:** Kafka owns durability, not the ingester. No WAL, no local state to recover. Crash = Kafka replay from last committed offset.
- **At-least-once semantics:** Commit after flush means crashes can produce duplicates, but never data loss. Exactly-once would require transactional writes (overkill for MVP).
- **flushJob wrapper instead of modifying Batch:** Keeps ingester self-contained, doesn't force offset tracking into the shared model.
- **Exponential backoff on flush:** Simple retry (3 attempts, 100ms/200ms/400ms) handles transient storage errors without complex circuit breaker logic.
- **Backpressure via buffered channel:** flushCh(100) blocks batcher, which blocks consumer, which applies Kafka consumer lag. Simple, effective, no explicit rate limiting needed.

### Limits of MVP + Next Steps
- No deduplication (at-least-once means duplicates on crash recovery)
- No per-tenant flush priority or fairness
- Retry is limited to 3 attempts — no dead letter queue
- No metrics (added in Production phase by observability-engineer)
- Consumer group rebalancing relies on franz-go defaults (cooperative sticky)

### How to Run Locally + Validate
```bash
docker compose up -d               # start Kafka
go run . ingester --storage-path ./data
# In another terminal:
go run . gateway
curl -X POST localhost:8080/api/v1/push \
  -H "X-Scope-OrgID: tenant-a" \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"service":"api"},"value":42.0}]'
# Wait ~10s for time-triggered flush, then:
ls ./data/tenant-a/
# Should see a file like: 2026-02-28/0-0000000001.json
```

---

## READY FOR APPROVAL
