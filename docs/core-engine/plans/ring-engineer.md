# Ring Engineer Plan

## You are in PLAN MODE.

### Project
I want to build a **multi-tenant Kafka-backed distributed ingestion service**.

**Goal:** build a **consistent hash ring** that deterministically routes tenant data to Kafka partitions, integrated into the existing gateway.

### Role + Scope
- **Role:** Ring Engineer
- **Scope:** Own `internal/ring/` package (hash ring from scratch), integrate into `internal/gateway/handler.go` and `internal/gateway/producer.go`. Do NOT own storage, ingester, batcher, or flusher.
- **File you will write:** `/docs/core-engine/plans/ring-engineer.md`
- **No-touch zones:** `internal/storage/`, `internal/ingester/`, `cmd/ingester.go`, `docker-compose.yml`, observability, rate limiting.

---

## Functional Requirements
- FR1: Consistent hash ring maps arbitrary string keys to partition IDs (int32) using xxhash + virtual nodes.
- FR2: `GetPartitionForTenant(tenantID, labels)` builds a composite key from tenant ID and sorted labels, then resolves to a partition.
- FR3: Ring wraps around (keys hashing above the max token resolve to partition 0's region).
- FR4: Gateway uses the ring to set an explicit Kafka partition on each produced record instead of relying on Kafka's default partitioner.
- Tests required: unit tests for determinism, distribution, minimal disruption, wrap-around, edge cases.
- Metrics required: N/A for this phase (observability phase later).

## Non-Functional Requirements
- Language/runtime: Go 1.22+
- Local dev: existing docker-compose (Kafka KRaft) unchanged
- Observability: N/A for this role
- Safety: Ring is read-only after construction; no concurrency issues. Zero-partition ring returns 0 gracefully.
- Documentation: CLAUDE.md + EXPLAIN.md contributions proposed below.
- Performance: ring lookup must be O(log N) where N = total virtual nodes. Benchmark included.

---

## Assumptions / System Model
- Deployment environment: local docker-compose; ring is constructed once at gateway startup.
- Failure modes: ring construction cannot fail (deterministic, no I/O). Lookup is pure computation.
- Delivery guarantees: ring does not affect delivery guarantees; those are owned by producer + ingester.
- Multi-tenancy: ring routes tenants deterministically to partitions; same tenant+labels always land on the same partition.

---

## Data Model

- **Ring** (internal struct, not persisted)
  - `partitions int` — number of Kafka partitions (default 6)
  - `tokens []uint32` — sorted array of virtual node token values
  - `owners []int32` — parallel array; `owners[i]` is the partition that owns `tokens[i]`
  - Constructed once, immutable after creation. No persistence needed.

- **Token generation**
  - For partition `p` in `[0, partitions)` and vnode `v` in `[0, vnodes)`:
    `token = uint32(xxhash.Sum64String(fmt.Sprintf("partition-%d-vnode-%d", p, v)))`
  - Tokens are sorted ascending. Owners array is reordered in parallel.

- **Key construction for tenant routing**
  - `tenantID + ":" + sorted(label_key=label_value, ...)` joined by `,`
  - Example: `tenant-a:env=prod,service=api`

- Validation rules: partitions must be > 0, virtualNodesPerPartition must be > 0.
- Versioning: N/A (no config changes; ring is rebuilt on restart if partition count changes).

---

## APIs

### Package `internal/ring`

```go
// NewRing creates a consistent hash ring with the given number of partitions
// and virtual nodes per partition. Tokens are generated using xxhash.
func NewRing(partitions int, virtualNodesPerPartition int) *Ring

// GetPartition returns the partition ID for the given key.
// Uses binary search over sorted tokens (O(log N) lookup).
func (r *Ring) GetPartition(key string) int32

// GetPartitionForTenant builds a routing key from tenantID and sorted labels,
// then returns the owning partition.
func (r *Ring) GetPartitionForTenant(tenantID string, labels map[string]string) int32
```

### Gateway integration (modified interfaces)

The `Producer` interface gains a partition-aware produce method:

```go
// ProduceToPartition produces a record to a specific Kafka partition.
func (p *KafkaProducer) ProduceToPartition(ctx context.Context, key []byte, value []byte, partition int32) error
```

The handler calls `ring.GetPartitionForTenant()` and passes the result to `ProduceToPartition()`.

### Error semantics
- `NewRing` with `partitions <= 0` or `vnodes <= 0`: returns a ring that maps everything to partition 0 (defensive, no panic).
- `GetPartition` on an empty ring (0 tokens): returns 0.
- All errors in the producer path are already handled by the existing gateway error responses (503).

---

## Architecture / Component Boundaries

- **Ring package** (`internal/ring/`): Pure computation, no I/O, no dependencies beyond `xxhash`. Constructed once at startup, immutable, safe for concurrent reads without locks.
- **Gateway handler** (`internal/gateway/handler.go`): Gains a `*ring.Ring` field. On each push request, calls `ring.GetPartitionForTenant(tenantID, events[0].Labels)` to determine the target partition.
- **Gateway producer** (`internal/gateway/producer.go`): Gains `ProduceToPartition` method that sets `kgo.Record.Partition` to the explicit partition value. The `Producer` interface is extended.
- **Gateway command** (`cmd/gateway.go`): Constructs the ring with configured partition count and vnodes, passes to handler.

Config changes: `GatewayConfig` gains `RingPartitions int` (default 6) and `RingVirtualNodes int` (default 64).

Concurrency model: Ring is immutable after construction. No locks needed. Multiple goroutines (HTTP handlers) can call `GetPartition` concurrently.

---

## Correctness Invariants

1. **Determinism:** `GetPartition(key)` always returns the same partition for the same key and ring configuration.
2. **Coverage:** Every possible uint32 hash value maps to exactly one partition (no gaps in the ring).
3. **Wrap-around:** Keys hashing above the maximum token wrap to the first token (ring topology).
4. **Minimal disruption:** Adding one partition to an N-partition ring reassigns roughly 1/(N+1) of keys, not all keys.
5. **Bounded distribution:** With 64 vnodes per partition, no partition should own more than 25% of 10,000 random keys across 6 partitions.
6. **Label ordering:** `GetPartitionForTenant("t", {"b":"2","a":"1"})` equals `GetPartitionForTenant("t", {"a":"1","b":"2"})` (sorted labels).
7. **Gateway partition match:** The partition written to Kafka by the gateway equals `ring.GetPartitionForTenant(tenantID, labels)` for the same input.

---

## Tests

### Unit tests (`internal/ring/hashring_test.go`)

| Test | What it verifies |
|------|-----------------|
| `TestDeterminism` | Same key returns same partition across 1000 calls |
| `TestDistribution` | 10,000 random keys across 6 partitions: each gets 10-25% |
| `TestMinimalDisruption` | 6 -> 7 partitions moves ~1/7 of 10,000 keys (tolerance: 10-20%) |
| `TestWrapAround` | A key known to hash above max token still resolves correctly |
| `TestEmptyRing` | `NewRing(0, 0)` returns partition 0 for any key |
| `TestLabelOrdering` | Swapped label order produces same partition |
| `TestGetPartitionForTenant` | Composite key routing works correctly |

### Integration (gateway handler test modifications)

| Test | What it verifies |
|------|-----------------|
| `TestHandlerUsesRingPartition` | Handler produces to the partition determined by ring |

### Benchmark (`internal/ring/hashring_test.go`)

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkGetPartition` | Lookup latency and allocations for a single key |
| `BenchmarkNewRing` | Ring construction time for 6 partitions x 64 vnodes |

### Commands
- `go test ./internal/ring/ -v` — run ring unit tests
- `go test ./internal/ring/ -bench . -benchmem` — run benchmarks
- `go test ./internal/gateway/ -v -run TestHandlerUsesRingPartition` — integration
- `go test ./... -v` — full suite

---

## Benchmarks + "Success"

- **Lookup latency:** `BenchmarkGetPartition` target: < 200ns/op, 0 allocs/op (binary search over sorted slice, no heap allocation).
- **Ring construction:** `BenchmarkNewRing` target: < 500us for 6 partitions x 64 vnodes (384 tokens). One-time cost at startup, not latency-critical.
- **Distribution quality:** standard deviation of partition counts across 100,000 random keys should be < 5% of mean.

Benchmark command:
```bash
go test ./internal/ring/ -bench . -benchmem -benchtime 5s
```

---

# Recommended API Surface

1. `ring.NewRing(partitions, vnodes) *Ring` — constructor
2. `ring.GetPartition(key) int32` — core lookup
3. `ring.GetPartitionForTenant(tenantID, labels) int32` — tenant-aware lookup
4. `Producer.ProduceToPartition(ctx, key, value, partition) error` — partition-explicit produce (added to existing interface)

# Folder Structure

```
internal/ring/
  hashring.go          — Ring struct, NewRing, GetPartition, GetPartitionForTenant (~80-100 lines)
  hashring_test.go     — unit tests + benchmarks (~150-200 lines)
```

Modified files:
```
internal/gateway/handler.go    — add Ring field, use GetPartitionForTenant in handlePush
internal/gateway/producer.go   — add ProduceToPartition method, extend Producer interface
internal/gateway/handler_test.go — update mock producer, add ring partition test
internal/config/config.go      — add RingPartitions, RingVirtualNodes to GatewayConfig
cmd/gateway.go                 — construct ring, pass to handler
```

---

# Tighten the plan into 4-7 small tasks

### Task 1: Implement the consistent hash ring
- **Outcome:** `internal/ring/hashring.go` with `Ring`, `NewRing`, `GetPartition`, `GetPartitionForTenant`.
- **Files to create:** `internal/ring/hashring.go`
- **Verification:** `go build ./internal/ring/` compiles; `go vet ./internal/ring/`
- **Commit message:** `feat(ring): implement consistent hash ring with xxhash and virtual nodes`

### Task 2: Unit tests for the hash ring
- **Outcome:** Full test coverage: determinism, distribution, minimal disruption, wrap-around, empty ring, label ordering.
- **Files to create:** `internal/ring/hashring_test.go`
- **Verification:** `go test ./internal/ring/ -v` — all pass; `go test ./internal/ring/ -bench . -benchmem` — <200ns/op lookup
- **Commit message:** `test(ring): add unit tests and benchmarks for hash ring`

### Task 3: Extend producer interface for partition-aware producing
- **Outcome:** `Producer` interface gains `ProduceToPartition`; `KafkaProducer` implements it using `kgo.Record.Partition`.
- **Files to modify:** `internal/gateway/producer.go`
- **Verification:** `go build ./internal/gateway/`; existing tests still pass: `go test ./internal/gateway/ -v`
- **Commit message:** `feat(gateway): add partition-aware produce method to producer`

### Task 4: Add ring config fields
- **Outcome:** `GatewayConfig` gains `RingPartitions` (default 6) and `RingVirtualNodes` (default 64).
- **Files to modify:** `internal/config/config.go`
- **Verification:** `go test ./internal/config/ -v`
- **Commit message:** `feat(config): add ring partition and vnode count to gateway config`

### Task 5: Integrate ring into gateway handler
- **Outcome:** Handler uses `ring.GetPartitionForTenant()` to determine partition, calls `ProduceToPartition`. Handler struct gains `ring *ring.Ring` field.
- **Files to modify:** `internal/gateway/handler.go`, `internal/gateway/handler_test.go`
- **Verification:** `go test ./internal/gateway/ -v` — all tests pass including new `TestHandlerUsesRingPartition`
- **Commit message:** `feat(gateway): integrate hash ring for deterministic partition routing`

### Task 6: Wire ring into gateway command
- **Outcome:** `cmd/gateway.go` constructs ring from config, passes to handler. Adds `--ring-partitions` and `--ring-vnodes` flags.
- **Files to modify:** `cmd/gateway.go`
- **Verification:** `go build .` succeeds; `go run . gateway --help` shows ring flags
- **Commit message:** `feat(cmd): wire hash ring into gateway startup`

---

## Top 3 Risks/Unknowns

### 1. Token collisions in virtual node generation
- **Risk:** Two different (partition, vnode) pairs produce the same uint32 token, causing one to shadow the other.
- **Validation:** Add a test that checks `len(unique(tokens)) == partitions * vnodes`. With 384 tokens in a uint32 space (4B values), collision probability is negligible (~0.00002%) but worth asserting.
- **Time:** < 5 minutes.

### 2. franz-go explicit partition semantics
- **Risk:** Setting `kgo.Record.Partition` might not work as expected, or might conflict with the default partitioner.
- **Validation:** Read franz-go docs on `Record.Partition` field. Confirm that setting it bypasses the default partitioner. Write a small test with a mock or integration test against local Kafka.
- **Time:** < 10 minutes.

### 3. Distribution quality with 64 vnodes
- **Risk:** 64 vnodes per partition might not provide adequate distribution across 6 partitions for realistic tenant ID patterns.
- **Validation:** Run `TestDistribution` with 10,000 random keys. If any partition gets <10% or >25%, increase vnodes to 128 or 256. Benchmark the tradeoff (more vnodes = larger token slice = slower binary search, but still O(log N)).
- **Time:** < 5 minutes.

---

## CLAUDE.md contributions (proposed content, do NOT write the file)

### From Ring Engineer

**Coding style rules:**
- `internal/ring/` is a pure-computation package: no I/O, no logging, no context.Context needed.
- Ring is immutable after construction. Never add mutation methods.
- Use `uint32` for tokens (truncated from xxhash's uint64) to keep memory compact.
- Token format string is `"partition-%d-vnode-%d"` — do not change without updating all tests.

**Dev commands:**
```bash
go test ./internal/ring/ -v                      # unit tests
go test ./internal/ring/ -bench . -benchmem      # benchmarks
go test ./internal/gateway/ -v                   # gateway integration
```

**Before you commit checklist:**
- [ ] `go test ./internal/ring/ -v` passes
- [ ] `go test ./internal/gateway/ -v` passes
- [ ] `go vet ./internal/ring/ ./internal/gateway/`
- [ ] Benchmark shows < 200ns/op for GetPartition

**Guardrails:**
- Do not import third-party consistent hashing libraries. The ring is hand-built for deep understanding.
- Do not add mutex to Ring — it is immutable and safe for concurrent reads.
- Changing the token format string breaks partition assignment for all tenants. Treat it as a versioned contract.

---

## EXPLAIN.md contributions (proposed outline bullets)

### Flow / Architecture
- The consistent hash ring maps tenant routing keys to Kafka partition IDs.
- Ring is constructed at gateway startup with N partitions and M virtual nodes per partition.
- Each partition gets M tokens generated by `xxhash("partition-P-vnode-V")`, truncated to uint32.
- All tokens are sorted. Lookup hashes the key, binary searches for the first token >= hash, and returns the owning partition.
- The ring wraps: if the hash is above all tokens, it maps to the partition owning the first (smallest) token.

### Key Engineering Decisions + Tradeoffs
- **xxhash over murmur3/fnv:** xxhash is fast, well-tested, and widely used in distributed systems for consistent hashing.
- **uint32 truncation:** xxhash produces uint64; we truncate to uint32 for memory efficiency (4B token space is more than sufficient for hundreds of vnodes).
- **Virtual nodes (64 default):** Balance between distribution quality and lookup speed. 64 vnodes x 6 partitions = 384 tokens. Binary search over 384 items is ~9 comparisons.
- **Immutable ring:** No dynamic membership changes. If partition count changes, rebuild the ring. This matches Kafka's static partition model.
- **Sorted slice + binary search over map:** O(log N) lookup with cache-friendly memory layout. A map would be O(1) amortized but wastes memory and has worse cache behavior for small N.

### Limits of MVP + Next Steps
- Ring does not support dynamic partition changes at runtime (restart required).
- No replication or ownership transfer (Kafka handles this at the consumer group level).
- Future: ring could be extended for zone-aware routing or weighted partitions.

### How to Run Locally + How to Validate
```bash
# Run ring tests
go test ./internal/ring/ -v

# Check distribution quality
go test ./internal/ring/ -v -run TestDistribution

# Benchmark lookup performance
go test ./internal/ring/ -bench . -benchmem

# Full pipeline test (gateway with ring)
docker compose up -d
go run . gateway --port 8080
curl -X POST localhost:8080/api/v1/push \
  -H "X-Scope-OrgID: tenant-a" \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"service":"api"},"value":42.0}]'
# Observe in Redpanda Console that the message lands on the expected partition
```

---

## READY FOR APPROVAL
