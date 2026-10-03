# Gateway Engineer Plan — Foundation Phase

## You are in PLAN MODE.

### Project
I want to build a **multi-tenant Kafka-backed ingestion service**.

**Goal:** build an **HTTP gateway** that accepts multi-tenant push requests, validates events, and produces them to Kafka using franz-go.

### Role + Scope
- **Role:** Gateway Engineer
- **Scope:** HTTP push handler, Kafka producer wrapper, cobra `gateway` subcommand, and their unit tests. I own `internal/gateway/` and `cmd/gateway.go`.
- **No-touch zones:** `internal/config/`, `internal/model/`, `internal/tenant/`, `docker-compose.yml`, `Makefile`, `go.mod` — those belong to infra-engineer and model-engineer.
- **File you will write:** `/docs/foundation/plans/gateway-engineer.md`
- **No code in this document.**

---

## Functional Requirements
- **FR1:** `POST /api/v1/push` accepts a JSON array of events, validates them, stamps tenant ID from `X-Scope-OrgID` header, and produces to Kafka.
- **FR2:** Kafka producer wraps franz-go with sync and async produce methods; tenant ID is set as both the record key and a Kafka header.
- **FR3:** `GET /ready` health endpoint returns 200 when the gateway is ready to serve.
- **FR4:** Cobra `gateway` subcommand wires config, producer, handler, and HTTP server with graceful shutdown on SIGINT/SIGTERM.
- **Tests required:** Unit tests for handler (httptest + mock producer), unit tests for producer construction and close.
- **Metrics required:** N/A for Foundation phase (observability-engineer owns this in Production phase).

## Non-Functional Requirements
- Language/runtime: Go 1.22+
- Local dev: `docker compose up -d` for Kafka, then `go run . gateway`
- Observability: Structured logging via `go.uber.org/zap`; Prometheus metrics deferred to Production phase
- Safety: Max request body size enforced (default 5 MB). Graceful shutdown drains in-flight requests and flushes producer.
- Documentation: CLAUDE.md + EXPLAIN.md contributions below
- Performance: N/A for Foundation (benchmark-engineer handles in Performance phase)

---

## Assumptions / System Model
- **Deployment:** Local docker-compose; Kafka KRaft single broker on port 9092
- **Failure modes:**
  - Kafka unavailable: producer returns error, handler returns 503 to client
  - Bad input (missing tenant, malformed JSON): handler returns 400
  - Request body too large: handler returns 413
- **Delivery guarantees:** At-least-once. Producer uses `RequiredAcks: AllISRAcks` (acks=all). Duplicates possible on retry.
- **Multi-tenancy:** Tenant ID from `X-Scope-OrgID` header. Gateway stamps it on every event — client-provided `tenant_id` is overwritten.

---

## Data Model (as relevant to gateway)

The gateway consumes and produces `model.Event`:

```
Event {
    TenantID  string            — set from X-Scope-OrgID (overwrite any client value)
    Timestamp time.Time         — if zero, gateway sets to time.Now()
    Labels    map[string]string — optional (may be empty or nil)
    Value     float64           — required
    Payload   []byte            — optional
}
```

**Validation rules (handler enforces):**
- Tenant ID must be non-empty (extracted from header, not body)
- Timestamp defaults to `time.Now()` if zero-value
- Request body <= 5 MB (configurable via `GatewayConfig.MaxBodySize`)

**Kafka record layout:**
- Key: `[]byte(tenantID)` — ensures same tenant goes to same partition
- Value: JSON-marshaled `[]Event` (the full batch from the request)
- Headers: `[{"tenant_id": tenantID}]` — for consumer-side filtering without deserialization

---

## APIs

### Push Endpoint
```
POST /api/v1/push
Headers:
  X-Scope-OrgID: <tenant-id>    (required)
  Content-Type: application/json (required)

Body: JSON array of events
[
  {
    "labels": {"service": "api", "env": "prod"},
    "value": 42.0,
    "timestamp": "2024-01-15T10:30:00Z",   // optional, defaults to now
    "payload": "<base64>"                    // optional
  }
]

Success Response: 200 OK
{
  "status": "ok",
  "events_accepted": 3
}

Error Responses:
  400 — missing X-Scope-OrgID, malformed JSON, validation failure
  413 — request body exceeds max size
  405 — non-POST method
  503 — Kafka unavailable (produce failed)
```

### Health Endpoint
```
GET /ready
200 OK  — gateway is serving
```

### CLI
```
ingest gateway [flags]
  --port             int      HTTP listen port (default 8080)
  --kafka-brokers    string   Comma-separated broker addresses (default "localhost:9092")
  --kafka-topic      string   Target topic (default "ingestion-events")
  --max-body-size    int      Max request body in bytes (default 5242880)
```

---

## Architecture / Component Boundaries

### Components I own:

1. **`internal/gateway/producer.go` — Kafka Producer**
   - Wraps `franz-go` (`kgo.Client`)
   - `NewProducer(brokers []string, topic string) (*Producer, error)` — creates client with `RequiredAcks: AllISRAcks`, `DefaultProduceTopic`
   - `Produce(ctx, key, value) error` — synchronous produce (blocks until ack or error)
   - `ProduceAsync(ctx, key, value, callback)` — fire-and-forget with error callback
   - `Close() error` — calls `client.Flush()` then `client.Close()`
   - Producer is an interface-backed struct so handler tests can mock it

2. **`internal/gateway/handler.go` — HTTP Handler**
   - `Handler` struct holds a `ProducerInterface` and `GatewayConfig`
   - Implements `http.Handler` via `ServeHTTP`
   - Routes: POST `/api/v1/push` -> push handler; GET `/ready` -> health check
   - Push flow: extract tenant -> read body (limited reader) -> unmarshal -> validate -> stamp tenant+timestamp -> marshal -> produce -> respond
   - Uses `http.MaxBytesReader` for body size limit

3. **`cmd/gateway.go` — Cobra Subcommand**
   - Registers `gateway` under root command
   - Binds flags to config struct
   - `RunE`: create producer -> create handler -> create `http.ServeMux` -> start `http.Server` -> block on signal -> graceful shutdown (context with 10s timeout)

### Components I depend on (owned by others):
- `internal/model/event.go` — `Event` struct, `Validate()`, `NewEvent()`
- `internal/model/serialize.go` — `MarshalEvents()`, `UnmarshalEvents()`
- `internal/tenant/tenant.go` — `ExtractTenantID(r)`
- `internal/config/config.go` — `GatewayConfig` struct
- `cmd/root.go` — root cobra command to attach subcommand

### Concurrency model:
- Standard `http.Server` — one goroutine per connection (Go net/http default)
- Sync produce per request in Foundation; async produce is available but not wired as default until benchmarked
- Graceful shutdown: server.Shutdown() drains connections, then producer.Close() flushes pending records

### Backpressure:
- Foundation phase: slow Kafka = slow HTTP response (sync produce)
- Future: async produce with bounded in-flight count + 503 when full

---

## Correctness Invariants
1. Every accepted event has `TenantID` set from the HTTP header, never from client JSON.
2. Every accepted event has a non-zero `Timestamp` (defaulted if omitted).
3. A request missing `X-Scope-OrgID` always returns 400 — never reaches Kafka.
4. A request with invalid JSON or events failing validation returns 400 — nothing produced.
5. If Kafka produce fails, the handler returns 503 — no false 200s.
6. Request body exceeding `MaxBodySize` returns 413 before parsing.
7. `GET /ready` returns 200 regardless of Kafka state (it is a liveness check, not a deep health check).
8. Graceful shutdown: in-flight requests complete, producer flushes, then process exits.

---

## Tests

### Unit tests (`internal/gateway/handler_test.go`)
- `TestPushHandler` — table-driven test with subtests:
  - `/success` — valid request, mock producer returns nil, expect 200 + correct JSON response
  - `/missing_tenant_id` — no X-Scope-OrgID header, expect 400
  - `/invalid_json` — malformed body, expect 400
  - `/tenant_id_overwrite` — client sends tenant_id in JSON, verify it's overwritten with header value
  - `/timestamp_default` — event with zero timestamp, verify it's set to approximately now
  - `/body_too_large` — body exceeding max size, expect 413
  - `/kafka_error` — mock producer returns error, expect 503
  - `/method_not_allowed` — GET to /api/v1/push, expect 405
- `TestReadyHandler` — GET /ready returns 200

### Unit tests (`internal/gateway/producer_test.go`)
- `TestNewProducer_InvalidBrokers` — empty brokers list, expect error
- `TestProducerClose` — verify Close calls flush then close on underlying client

### Mock strategy
- Define `ProducerInterface` with `Produce`, `ProduceAsync`, `Close`
- Handler depends on interface, not concrete struct
- Test file defines `mockProducer` implementing the interface

### Commands
```bash
go test ./internal/gateway/... -v
go test ./cmd/... -v
go test ./... -count=1
```

---

## Benchmarks + "Success"

N/A for Foundation phase. The gateway hot path (handler + produce) will be benchmarked in the Performance phase by benchmark-engineer. Foundation success is functional correctness:

- `docker compose up -d` starts Kafka
- `go run . gateway --port 8080` starts without error
- `curl` push request returns 200 with `events_accepted` count
- Message appears in Kafka topic (verified via kcat or Redpanda Console)

---

## Recommended API Surface

| Function / Method | Package | Behavior |
|---|---|---|
| `NewProducer(brokers, topic)` | `internal/gateway` | Creates franz-go client, returns `*Producer` |
| `Producer.Produce(ctx, key, value)` | `internal/gateway` | Sync produce, blocks until ack |
| `Producer.ProduceAsync(ctx, key, value, cb)` | `internal/gateway` | Async produce with error callback |
| `Producer.Close()` | `internal/gateway` | Flush + close |
| `NewHandler(producer, cfg)` | `internal/gateway` | Creates handler with deps |
| `Handler.ServeHTTP(w, r)` | `internal/gateway` | Routes to push or ready handler |
| `gateway` cobra command | `cmd` | Wires everything, runs server |

---

## Folder Structure

```
internal/gateway/
  producer.go          — Producer struct, NewProducer, Produce, ProduceAsync, Close
  handler.go           — Handler struct, NewHandler, ServeHTTP, pushHandler, readyHandler
  handler_test.go      — httptest-based unit tests with mock producer
  producer_test.go     — producer construction and close tests
cmd/
  gateway.go           — cobra gateway subcommand
```

---

## Tighten the plan into 4-7 small tasks

### Task 1: Producer interface and franz-go wrapper
- **Outcome:** `ProducerInterface` and concrete `Producer` struct wrapping franz-go
- **Files:** `internal/gateway/producer.go`, `internal/gateway/producer_test.go`
- **Verification:** `go build ./internal/gateway/...` compiles; `go test ./internal/gateway/... -run TestProducer`
- **Commit:** `feat(gateway): add Kafka producer wrapper using franz-go`

### Task 2: HTTP push handler with routing
- **Outcome:** Handler that extracts tenant, validates, produces to Kafka, and responds
- **Files:** `internal/gateway/handler.go`
- **Verification:** `go build ./internal/gateway/...`
- **Commit:** `feat(gateway): add HTTP push handler with tenant validation`

### Task 3: Handler unit tests
- **Outcome:** Full test coverage for handler: success, missing tenant, bad JSON, body too large, Kafka error, method not allowed, ready endpoint, tenant overwrite, timestamp default
- **Files:** `internal/gateway/handler_test.go`
- **Verification:** `go test ./internal/gateway/... -v -count=1` — all pass
- **Commit:** `test(gateway): add comprehensive handler unit tests`

### Task 4: Cobra gateway subcommand
- **Outcome:** `gateway` subcommand with flags, wires producer+handler+server, graceful shutdown
- **Files:** `cmd/gateway.go`
- **Verification:** `go build .` succeeds; `go run . gateway --help` shows flags; `go run . gateway --port 8080` starts (with Kafka running) and responds to curl
- **Commit:** `feat(gateway): add cobra gateway subcommand with graceful shutdown`

### Task 5: End-to-end smoke test
- **Outcome:** Verify full flow: docker compose up, start gateway, curl push, check Kafka topic
- **Files:** No new files — manual verification
- **Verification:**
  ```bash
  docker compose up -d
  go run . gateway --port 8080 &
  curl -s -X POST localhost:8080/api/v1/push \
    -H "X-Scope-OrgID: tenant-a" \
    -H "Content-Type: application/json" \
    -d '[{"labels":{"service":"api"},"value":42.0}]'
  # expect {"status":"ok","events_accepted":1}
  ```
- **Commit:** N/A (manual verification step)

### Top 3 Risks/Unknowns

1. **franz-go API surface unfamiliarity**
   - Risk: Misconfigure producer options (acks, batching, timeouts)
   - Validation (<15 min): Write a minimal main.go that produces one record to local Kafka and reads it back. Confirm `kgo.RequiredAcks` and `kgo.DefaultProduceTopic` options work as expected.

2. **Dependency on model-engineer and infra-engineer**
   - Risk: `model.Event`, `tenant.ExtractTenantID`, `config.GatewayConfig`, and `cmd/root.go` may not exist yet when gateway code is written.
   - Validation (<15 min): Check if the files exist. If not, define minimal local interfaces/types to unblock, then swap to real imports once available.

3. **`http.MaxBytesReader` behavior on oversized body**
   - Risk: MaxBytesReader may return a generic error that is hard to distinguish from other read errors.
   - Validation (<15 min): Write a test with httptest that sends a body larger than the limit. Confirm the error type is `*http.MaxBytesError` (Go 1.19+) so we can return 413 specifically.

---

## CLAUDE.md contributions (proposed, do NOT write the file)

### From Gateway Engineer
- **Coding style:**
  - Handler dependencies injected via constructor (`NewHandler`), never as globals
  - Use `ProducerInterface` for testability — handler never depends on concrete producer
  - HTTP errors return JSON `{"error": "message"}` with appropriate status codes
  - Use `http.MaxBytesReader` for body size limits, not manual length checks
- **Dev commands:**
  - `go test ./internal/gateway/... -v` — run gateway unit tests
  - `go run . gateway --port 8080` — start gateway locally (requires Kafka on :9092)
  - `curl -X POST localhost:8080/api/v1/push -H "X-Scope-OrgID: test" -H "Content-Type: application/json" -d '[{"labels":{"svc":"a"},"value":1}]'` — test push
- **Before you commit:**
  - `go test ./internal/gateway/... -count=1` passes
  - `go vet ./internal/gateway/...` clean
  - Handler returns correct status codes for all error paths
  - No hardcoded broker addresses or topic names (use config)
- **Guardrails:**
  - Never trust client-provided `tenant_id` in JSON body — always overwrite from header
  - Never return 200 if Kafka produce failed
  - Always enforce body size limit

---

## EXPLAIN.md contributions (proposed outline bullets)

### Gateway Flow
- Client sends `POST /api/v1/push` with `X-Scope-OrgID` header and JSON event array
- Handler extracts tenant ID from header (returns 400 if missing)
- Body is read through `http.MaxBytesReader` (returns 413 if too large)
- JSON is unmarshaled into `[]model.Event` (returns 400 if malformed)
- Each event is stamped with tenant ID + timestamp
- Events are marshaled back to JSON and produced to Kafka with tenant ID as key
- Key-based partitioning ensures all events for a tenant land on the same partition
- Handler returns 200 with accepted count, or 503 if Kafka produce fails

### Key Engineering Decisions
- **Sync produce in Foundation:** Simpler correctness model — client knows immediately if their data was durably written. Async produce available for Performance phase optimization.
- **Interface-based producer:** Enables handler unit tests without Kafka. Mock producer in tests, real producer in production.
- **Tenant ID as Kafka key:** Ensures partition affinity per tenant. When hash ring is added in Core Engine phase, the key will change to tenant+labels hash.
- **Overwrite tenant_id from header:** Defense in depth — a misbehaving client cannot inject events into another tenant's partition.

### Limits of MVP
- No rate limiting (Phase 3 — Production)
- No metrics/observability (Phase 3 — Production)
- No async produce batching optimization (Phase 5 — Performance)
- No request authentication beyond tenant header
- Single Kafka broker, no replication

### How to Run + Validate
```bash
docker compose up -d
go run . gateway --port 8080
curl -X POST localhost:8080/api/v1/push \
  -H "X-Scope-OrgID: tenant-a" \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"service":"api"},"value":42.0}]'
# Expect: {"status":"ok","events_accepted":1}
```

---

## READY FOR APPROVAL
