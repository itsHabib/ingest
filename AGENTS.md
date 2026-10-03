# Agent Development Process

This project was built entirely using Claude Code's multi-agent team workflow (`/team-kickoff` and `/team-code-execute` skills). Each phase spawned a team of 3 specialized AI engineers who first planned their work in detail, then executed it as code — with self-verification at every step.

---

## Workflow

Each phase followed the same pattern:

```
1. /team-kickoff <phase>/kickoff.yaml
   → Spawns 3 teammates in PLAN MODE
   → Each writes a detailed plan doc (requirements, API surface, tests, risks)
   → Plans reviewed and approved by team lead

2. /team-code-execute <phase>/kickoff.yaml
   → Spawns 3 teammates with their approved plans
   → Each writes actual Go code matching their plan
   → Self-verifies with go build, go test, go vet
   → Team lead collects results and resolves conflicts

3. Manual review between phases
   → Human reviews output, fixes issues, runs full test suite
   → Next phase builds on the verified output
```

---

## Phases

### Phase 1: Foundation

**Goal:** Runnable gateway that accepts HTTP events and produces to Kafka.

| Role | Focus | Key Deliverables |
|------|-------|-------------------|
| infra-engineer | Infrastructure scaffolding | Docker Compose (Kafka KRaft), go.mod, Cobra CLI, Makefile |
| model-engineer | Core data types | Event/Batch structs, JSON serialization, tenant extraction |
| gateway-engineer | HTTP ingestion | `/api/v1/push` handler, Kafka producer (franz-go), `/ready` health check |

**Outcome:** Gateway accepts `POST /api/v1/push` with `X-Scope-OrgID` header, validates events, produces to Kafka with at-least-once semantics.

---

### Phase 2: Core Engine

**Goal:** Hash ring routing, stateless ingester with batching, flush to storage.

| Role | Focus | Key Deliverables |
|------|-------|-------------------|
| ring-engineer | Consistent hashing | Hash ring from scratch (xxhash + 64 vnodes), O(log N) lookup |
| storage-engineer | Persistence layer | ObjectStore interface, filesystem backend with atomic writes |
| ingester-engineer | Event processing pipeline | Kafka consumer group, in-memory batcher (size + time triggers), flusher with retry, graceful shutdown |

**Outcome:** Full pipeline works end-to-end — events flow from HTTP push through Kafka, get batched by tenant+partition, and flush to storage as JSON files.

---

### Phase 3: Production

**Goal:** Rate limiting, metrics, observability, S3 storage, load generator.

| Role | Focus | Key Deliverables |
|------|-------|-------------------|
| tenancy-engineer | Tenant isolation | Per-tenant token bucket rate limiter (RWMutex + double-checked locking), backpressure propagation (pause/resume Kafka) |
| observability-engineer | Monitoring | Prometheus counters/histograms on all components, Grafana dashboard, pprof endpoints, HTTP middleware |
| integration-engineer | Production readiness | S3/MinIO storage backend, multi-tenant load generator with YAML profiles, latency reporting (p50/p95/p99) |

**Outcome:** Production-grade system with per-tenant rate limiting, full Prometheus metrics, pre-built Grafana dashboard, and configurable load testing.

---

### Phase 4: Quality Engineering

**Goal:** Comprehensive testing — unit, integration, and chaos tests.

| Role | Focus | Key Deliverables |
|------|-------|-------------------|
| unit-test-engineer | Unit coverage | Tests for all packages, hand-rolled fakes (FakeObjectStore, FakeProducer), race detector validation |
| integration-test-engineer | End-to-end testing | TestHarness for full pipeline tests, multi-tenant scenarios, graceful shutdown verification |
| chaos-engineer | Failure modes | Crash recovery tests, storage failure + backpressure, slow storage accumulation, multi-ingester failover |

**Key invariants tested:**
1. At-least-once: every event sent appears in storage (possibly duplicated, never lost)
2. Tenant isolation: tenant-a data never in tenant-b's storage path
3. Rate limiting: exceeding rate returns 429, other tenants unaffected
4. Crash recovery: kill ingester, restart, no data lost
5. Backpressure: storage broken → ingester pauses → gateway returns 503

---

### Phase 5: Performance

**Goal:** Benchmarks, load testing, profiling, and targeted optimizations.

| Role | Focus | Key Deliverables |
|------|-------|-------------------|
| benchmark-engineer | Micro-benchmarks | Go benchmarks for hot paths (hash ring < 500ns/op, serialization, batcher, handler) |
| load-test-engineer | System-level load testing | Load profiles (baseline, high-throughput, many-tenants, burst), throughput ceiling identification |
| profiling-engineer | Bottleneck analysis | pprof profiling (CPU, heap, mutex, block), optimization (buffer pools, encoder reuse, string builder) |

**Performance targets:**
- Gateway: > 50,000 events/sec
- Ingester: > 30,000 events/sec
- Hash ring lookup: < 500ns/op
- P99 gateway latency: < 10ms under load

---

## Query Path

### Phase 6: Query Foundation

**Goal:** HTTP query API, block index for fast lookups, parallel block reading with filtering.

| Role | Focus |
|------|-------|
| index-engineer | Block index for fast query lookups, label matchers |
| query-api-engineer | HTTP query endpoints (`GET /api/v1/query`), querier CLI command |
| block-reader-engineer | Parallel block reading, filtering, streaming merge |

---

### Phase 7: Compaction & Caching

**Goal:** Background compactor to merge small blocks, block cache for read performance, structured block format.

| Role | Focus |
|------|-------|
| compactor-engineer | Background compaction loop, merge small blocks |
| block-format-engineer | Structured block format with headers for partial reads |
| cache-engineer | LRU block cache, cached object store wrapper |

---

### Phase 8: Query Performance

**Goal:** Query planner, parallel fan-out execution, read-path benchmarks and load testing.

| Role | Focus |
|------|-------|
| query-planner-engineer | Query planning, time/label pruning, cost estimation |
| fanout-engineer | Parallel fan-out execution engine, streaming merge |
| query-bench-engineer | Read-path benchmarks, query load testing, profiling |

---

## Kubernetes & Operations

### Phase 9: Kubernetes Foundation

**Goal:** K8s manifests, 3-broker Kafka with replication, Kustomize overlays for dev/staging/prod.

| Role | Focus |
|------|-------|
| k8s-manifest-engineer | Kubernetes Deployments, Services, ConfigMaps for all services |
| kafka-cluster-engineer | Strimzi 3-broker Kafka cluster with replication |
| config-engineer | Kustomize base + overlays (dev/staging/prod), MinIO, monitoring |

---

### Phase 10: Scaling & Observability

**Goal:** HPA autoscaling, OpenTelemetry distributed tracing, resilience hardening.

| Role | Focus |
|------|-------|
| autoscaling-engineer | HPA for gateway/querier, KEDA for ingester (Kafka lag) |
| tracing-engineer | OpenTelemetry distributed tracing across full pipeline |
| resilience-engineer | PDBs, pod anti-affinity, network policies |

---

### Phase 11: CI/CD & Operations

**Goal:** CI/CD pipeline, GitOps deployment, alerting rules, runbooks, SLOs.

| Role | Focus |
|------|-------|
| cicd-engineer | GitHub Actions CI/CD pipeline, Docker image publishing |
| gitops-engineer | ArgoCD GitOps deployment, image updater, secrets management |
| operations-engineer | Prometheus alerting rules, operational runbooks, SLO definitions |

---

## By the Numbers

| Metric | Value |
|--------|-------|
| Total phases | 11 |
| Engineers per phase | 3 |
| Total engineer roles | 33 |
| Plan docs produced | 15 (phases 1-5 complete) |
| Phase config files | 11 kickoff YAMLs |

---

## Plan Document Structure

Each engineer's plan followed a consistent template:

1. **Functional Requirements** — numbered (FR1, FR2, ...)
2. **Non-Functional Requirements** — performance, safety, concurrency
3. **Data Model** — structs, types, serialization
4. **API Surface** — Go function signatures and interfaces
5. **Architecture / Component Boundaries** — what lives where
6. **Correctness Invariants** — properties that must always hold
7. **Tests** — unit, integration, property-based, failure injection
8. **Benchmarks + Success Criteria** — measurable targets
9. **Task Breakdown** — 4-7 small tasks with verification steps
10. **Top 3 Risks** — with mitigation strategies

---

## File Layout

```
docs/
├── foundation/
│   ├── kickoff.yaml                    # Phase config
│   └── plans/
│       ├── infra-engineer.md           # Infrastructure plan
│       ├── model-engineer.md           # Data model plan
│       └── gateway-engineer.md         # HTTP handler plan
├── core-engine/
│   ├── kickoff.yaml
│   └── plans/
│       ├── ring-engineer.md            # Hash ring plan
│       ├── storage-engineer.md         # Storage layer plan
│       └── ingester-engineer.md        # Consumer pipeline plan
├── production/
│   ├── kickoff.yaml
│   └── plans/
│       ├── tenancy-engineer.md         # Rate limiting plan
│       ├── observability-engineer.md   # Metrics/monitoring plan
│       └── integration-engineer.md     # S3 + load gen plan
├── quality/
│   ├── kickoff.yaml
│   └── plans/
│       ├── unit-test-engineer.md       # Unit test plan
│       ├── integration-test-engineer.md # E2E test plan
│       └── chaos-engineer.md           # Chaos test plan
├── performance/
│   ├── kickoff.yaml
│   ├── report.md                       # Performance analysis
│   └── plans/
│       ├── benchmark-engineer.md       # Benchmark plan
│       ├── load-test-engineer.md       # Load test plan
│       └── profiling-engineer.md       # Profiling plan
├── query-foundation/
│   ├── kickoff.yaml
│   └── plans/
├── compaction/
│   ├── kickoff.yaml
│   └── plans/
├── query-performance/
│   ├── kickoff.yaml
│   └── plans/
├── k8s-foundation/
│   ├── kickoff.yaml
│   └── plans/
├── k8s-scaling/
│   ├── kickoff.yaml
│   └── plans/
└── k8s-operations/
    ├── kickoff.yaml
    └── plans/
```

---

## Tools Used

- **Claude Code** — AI coding assistant (CLI)
- **`/team-kickoff`** — Spawns planning teams from kickoff YAML configs
- **`/team-code-execute`** — Spawns coding teams that implement approved plans
- **`PROJECT.md`** — Source of truth for all phases (listed as dependency in every kickoff)

Each phase was executed iteratively with human review between phases, ensuring each layer was solid before building the next.
