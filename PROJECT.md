# Ingestion Engine

> Multi-tenant Kafka-backed distributed ingestion service in Go, using Kafka as the durability layer instead of a traditional WAL.

---

## Problem & Motivation

Modern observability stacks share a common ingestion skeleton: gateway → distributor → ingester (stateful, WAL-backed) → object storage. The ingester's statefulness (local WAL) is the biggest operational pain point — crash recovery, handoffs, data loss risk.

The industry is moving toward Kafka as the durability layer, which makes ingesters stateless. This project builds that architecture from scratch to deeply understand the design decisions, trade-offs, and failure modes.

---

## Definition of Done

- Gateway accepts multi-tenant HTTP push requests and produces to Kafka
- Consistent hash ring routes tenant data to specific Kafka partitions
- Stateless ingesters consume from Kafka, batch in memory, flush to object storage
- Per-tenant rate limiting and backpressure propagation through the full pipeline
- Prometheus metrics + Grafana dashboard for observability
- Local filesystem and S3/MinIO storage backends
- Comprehensive test suite (unit, integration, chaos, benchmarks)
- Performance profiled with pprof
- HTTP query API with block index, parallel block reading, and label filtering
- Background compactor, structured block format, and LRU block cache
- Query planner with parallel fan-out execution and read-path benchmarks
- Kubernetes manifests with Kustomize overlays for dev/staging/prod
- 3-broker Kafka cluster with replication, HPA autoscaling, distributed tracing
- CI/CD pipeline, GitOps deployment, alerting rules, runbooks, and SLOs

---

## Key Components

- **Gateway** — Stateless HTTP server. Validates requests, extracts tenant ID (`X-Scope-OrgID`), rate limits per tenant, routes via hash ring, produces to Kafka.
- **Hash Ring** — Consistent hash ring (built from scratch) using xxhash + virtual nodes. Maps tenant+labels to Kafka partitions.
- **Kafka** — Durability layer replacing the traditional WAL. KRaft mode (no Zookeeper). Decouples write rate from processing rate.
- **Ingester** — Stateless Kafka consumer. Batches events in memory (size + time triggers), flushes to object storage, commits offsets after flush (at-least-once).
- **Batcher** — In-memory accumulator per tenant+partition. Flushes on size threshold (1000 events) or time threshold (10s).
- **Object Storage** — Pluggable backend (interface). Filesystem for dev, S3/MinIO for production. Key format: `<tenantID>/<date>/<partition>-<offset>.batch`.
- **Rate Limiter** — Per-tenant token bucket rate limiter. Returns HTTP 429 when exceeded.
- **Load Generator** — Configurable multi-tenant event generator for testing and benchmarking.
- **Observability** — Prometheus metrics on all components, Grafana dashboard, pprof endpoints.
- **Querier** — Stateless HTTP server for query API. Reads block indexes, plans queries, executes parallel block reads with filtering, streams results.
- **Block Index** — Lightweight per-tenant-per-day index mapping time ranges and label sets to block file paths. Enables fast block pruning.
- **Compactor** — Background process that merges small blocks into larger ones, writes structured block format with headers, and updates indexes.

---

## Tech Stack

- **Language**: Go
- **Kafka client**: `github.com/twmb/franz-go` (pure Go, consumer groups)
- **Hashing**: `github.com/cespare/xxhash/v2`
- **CLI**: `github.com/spf13/cobra`
- **Rate limiting**: `golang.org/x/time/rate`
- **Metrics**: `github.com/prometheus/client_golang`
- **S3**: `github.com/aws/aws-sdk-go-v2`
- **Logging**: `log/slog` (stdlib)
- **Infra**: Docker Compose (Kafka KRaft, MinIO, Prometheus, Grafana)

---

## Non-Goals

- No pprof profile parsing (generic events first; pprof is a future extension)
- No authentication/authorization beyond tenant ID header

---

## Constraints

- Follow standard distributed ingestion patterns (gateway → distributor → ingester → storage)
- Hash ring must be built from scratch (deep understanding of consistent hashing)
- At-least-once semantics (commit after flush)
- All code must be explainable and whiteboardable

---

## Project Structure

```
ingest/
  go.mod / main.go
  cmd/
    root.go              — cobra setup
    gateway.go           — run gateway command
    ingester.go          — run ingester command
    loadgen.go           — run loadgen command
  internal/
    config/
      config.go          — centralized config, env/flag parsing
    model/
      event.go           — Event, Batch types
      serialize.go       — JSON + protobuf serialization
    tenant/
      tenant.go          — tenant ID extraction, context helpers
      ratelimit.go       — per-tenant token bucket rate limiter
    ring/
      hashring.go        — consistent hash ring (from scratch)
    gateway/
      handler.go         — HTTP push handler
      producer.go        — Kafka producer wrapper
    ingester/
      consumer.go        — Kafka consumer loop
      batcher.go         — in-memory batch accumulator
      flusher.go         — flush to object storage
    storage/
      storage.go         — ObjectStore interface
      filesystem.go      — local filesystem backend
      s3.go              — S3/MinIO backend
    observability/
      metrics.go         — Prometheus metrics
      middleware.go       — HTTP middleware
      pprof.go           — pprof endpoint setup
    loadgen/             — load generator internals
    testing/             — test harness, fault injection
  proto/
    event.proto          — protobuf definitions
  configs/
    prometheus.yml
    grafana/             — dashboard provisioning
    load-profiles/       — load test configurations
  scripts/
    e2e.sh
    chaos.sh
    profile.sh
  docker-compose.yml
  Makefile
```

---

## Team

| Role | Focus |
|------|-------|
| infra-engineer | Docker Compose, Makefile, Go module, cobra CLI, config |
| model-engineer | Core data types, serialization, tenant extraction |
| gateway-engineer | HTTP handler, Kafka producer, gateway command |
| ring-engineer | Consistent hash ring from scratch |
| storage-engineer | ObjectStore interface, filesystem implementation |
| ingester-engineer | Kafka consumer, batcher, flusher |
| tenancy-engineer | Per-tenant rate limiter, backpressure |
| observability-engineer | Prometheus metrics, Grafana dashboard |
| integration-engineer | Load generator, S3/MinIO, integration tests |
| unit-test-engineer | Comprehensive unit tests, test helpers |
| integration-test-engineer | End-to-end pipeline tests |
| chaos-engineer | Failure mode and recovery tests |
| benchmark-engineer | Go benchmarks for hot paths |
| load-test-engineer | Large-scale load testing |
| profiling-engineer | pprof profiling and optimization |
| index-engineer | Block index for fast query lookups |
| query-api-engineer | HTTP query endpoints, querier CLI command |
| block-reader-engineer | Parallel block reading, filtering, merging |
| compactor-engineer | Background compaction loop |
| block-format-engineer | Structured block format with headers |
| cache-engineer | LRU block cache for read performance |
| query-planner-engineer | Query planning and optimization |
| fanout-engineer | Parallel query execution engine |
| query-bench-engineer | Read-path benchmarks and load testing |
| k8s-manifest-engineer | Kubernetes manifests for all services |
| kafka-cluster-engineer | Multi-broker Kafka with replication |
| config-engineer | Kustomize overlays, secrets, CI scaffolding |
| autoscaling-engineer | HPA for gateway and ingester |
| tracing-engineer | OpenTelemetry distributed tracing |
| resilience-engineer | PDBs, anti-affinity, network policies |
| cicd-engineer | GitHub Actions CI/CD pipeline |
| gitops-engineer | ArgoCD/Flux GitOps deployment |
| operations-engineer | Alerting rules, runbooks, SLO dashboards |

---

## Phases

### Ingestion Pipeline

| Phase | Config | Goal |
|-------|--------|------|
| 1 — Foundation | `docs/foundation/kickoff.yaml` | Runnable gateway that accepts HTTP events and produces to Kafka |
| 2 — Core Engine | `docs/core-engine/kickoff.yaml` | Hash ring routing, stateless ingester with batching, flush to storage |
| 3 — Production | `docs/production/kickoff.yaml` | Rate limiting, metrics, load generator, S3/MinIO |
| 4 — Quality | `docs/quality/kickoff.yaml` | Comprehensive testing — unit, integration, chaos |
| 5 — Performance | `docs/performance/kickoff.yaml` | Benchmarks, load testing, pprof profiling, optimization |

### Query Path

| Phase | Config | Goal |
|-------|--------|------|
| 6 — Query Foundation | `docs/query-foundation/kickoff.yaml` | HTTP query API, block index for fast lookups, parallel block reading with filtering |
| 7 — Compaction & Caching | `docs/compaction/kickoff.yaml` | Background compactor to merge small blocks, block cache for read performance, structured block format |
| 8 — Query Performance | `docs/query-performance/kickoff.yaml` | Query planner, parallel fan-out execution, read-path benchmarks and load testing |

### Kubernetes & Operations

| Phase | Config | Goal |
|-------|--------|------|
| 9 — Kubernetes Foundation | `docs/k8s-foundation/kickoff.yaml` | K8s manifests, 3-broker Kafka with replication, Kustomize overlays for dev/staging/prod |
| 10 — Scaling & Observability | `docs/k8s-scaling/kickoff.yaml` | HPA autoscaling, OpenTelemetry distributed tracing, resilience hardening |
| 11 — CI/CD & Operations | `docs/k8s-operations/kickoff.yaml` | CI/CD pipeline, GitOps deployment, alerting rules, runbooks, SLOs |

---

## Usage in Phase Planning

This file is the source of truth for all planning phases.
List it as the first dependency in every phase config:

```yaml
dependencies:
  - PROJECT.md
```
