# ingest

**Follow an event from an HTTP request to a Kafka partition to a query result.**

A Go systems lab for multi-tenant event ingestion: consistent hashing, batching,
object storage, backpressure, and a bounded parallel query reader. Small enough
to trace through the code; complete enough to run a write and read path locally.

```text
                 WRITE                              READ
HTTP → gateway → Kafka → ingester → object storage → querier → NDJSON
         │                  │              ↑            ↑
   tenant + rate limit   batch + flush     └── index ────┘
```

[Run it](#run-it-locally) · [Trace an event](#read-the-code-in-order) · [Experiments](#three-experiments) · [Limits](#what-this-demonstrates)

## Run it locally

Requires **Docker with Compose**. Go **1.25.1 or newer** is needed for local
tests and the load generator. No cloud account or API key is required.

```sh
git clone https://github.com/itsHabib/ingest.git
cd ingest
docker compose up --build -d --wait
```

This starts Kafka, MinIO, the gateway, ingester, querier, and dashboards. Published
ports bind to **127.0.0.1**. The stack uses demo credentials and an anonymous
Grafana admin session; keep it on a trusted local machine. The tenant header is
routing information, not authentication.

Check the HTTP services:

```sh
curl --fail http://localhost:8082/ready
curl --fail http://localhost:8083/ready
```

Send one event. The gateway supplies its timestamp and tenant ID:

```sh
curl --fail http://localhost:8082/api/v1/push   -H 'X-Scope-OrgID: demo'   -H 'Content-Type: application/json'   -d '[{"labels":{"service":"arcade"},"value":42}]'
```

Expected acknowledgement:

```json
{"events_accepted":1,"status":"ok"}
```

Acceptance means Kafka accepted the batch; it does **not** mean the querier can
already see it. The ingester flushes by size or age (10 seconds by default).
After a flush, query the current UTC day:

```sh
curl --fail --get http://localhost:8083/api/v1/query   -H 'X-Scope-OrgID: demo'   --data-urlencode "start=$(date -u +%Y-%m-%d)T00:00:00Z"   --data-urlencode "end=$(date -u +%Y-%m-%d)T23:59:59Z"   --data-urlencode 'labels=service=arcade'
```

The response is newline-delimited JSON with the event’s tenant, timestamp,
labels, and value. If it is empty, allow the first flush to finish and retry;
inspect `docker compose logs ingester` if it stays empty. If midnight passed
between push and query, use the event’s UTC date instead.

```sh
docker compose down
```

This stops the demo and retains its named data volumes. Avoid running the
integration/chaos scripts alongside a demo you want to keep running: those
scripts manage the same Compose stack.

## What is running

| Component | Local URL / purpose |
|---|---|
| Gateway | `http://localhost:8082` — push events, validate input, limit requests per tenant |
| Querier | `http://localhost:8083` — time ranges and label matchers, NDJSON results |
| Kafka console | `http://localhost:8888` — inspect partitions and records |
| MinIO console | `http://localhost:9001` — inspect objects; demo login `minioadmin` / `minioadmin` |
| Grafana | `http://localhost:3000` — provisioned dashboards |
| Prometheus | `http://localhost:9090` — metrics |

These are **Compose host ports**. Native commands use different defaults:
`gateway` listens on 8080 and `querier` on 8082. See
[configuration defaults](internal/config/config.go) and `go run . --help`.

## Read the code in order

| Stop | Mechanism to study |
|---|---|
| [gateway/handler.go](internal/gateway/handler.go) | Stamp the tenant, validate a batch, pick a partition, acknowledge Kafka |
| [ring/hashring.go](internal/ring/hashring.go) | Sorted virtual nodes and binary-search lookup using xxhash |
| [ingester/batcher.go](internal/ingester/batcher.go) | Per-tenant/partition batches, size/age triggers, bounded channels |
| [ingester/consumer.go](internal/ingester/consumer.go) | Flush workers, offset commits, pause/resume and storage probes |
| [ingester/flusher.go](internal/ingester/flusher.go) | Buffer reuse, object writes, retries, and index publication |
| [index/](internal/index/) | Per-tenant/day block metadata and label matching |
| [querier/reader.go](internal/querier/reader.go) | Bounded parallel reads, filtering, and cancellation |
| [storage/](internal/storage/) | One interface with filesystem and S3 implementations |

## Three experiments

**1. Watch batching change latency.** Send one event and observe the delay before
it appears in a query. Compare with a larger batch. Follow the size and age
triggers in `batcher.go`; change `INGEST_INGESTER_BATCH_SIZE` or
`INGEST_INGESTER_BATCH_AGE` in the Compose ingester environment and recreate that
service to compare behavior.

**2. Put pressure on the write path.** With the demo running:

```sh
go run . loadgen --target http://localhost:8082 --tenants 5 --rate 500 --duration 30s
```

Watch throughput, response codes, latency, and batch sizes. Change one variable
at a time. The gateway’s token bucket currently charges per request, so event
batch size affects how many events one permitted request can carry.

**3. Compare the mechanisms in isolation.** No Kafka needed:

```sh
go test -run '^$' -bench . -benchmem ./internal/ring ./internal/ingester
```

Measure on your machine. The repository’s [older performance report](docs/performance/report.md)
is historical evidence, not a promised throughput figure.

## Test and inspect

```sh
go test -race ./...
go vet ./...
go build ./...
```

Those commands run the default suite using local test fixtures and fakes. For
Kafka-backed integration and failure tests, use `make test-integration` and
`make test-chaos`; both scripts start and tear down local infrastructure.
See [the Makefile](Makefile) for the individual targets.

## What this demonstrates

Kafka separates HTTP acceptance from batch persistence. Ingester memory is
transient; recovery is intended to use Kafka offsets rather than a local WAL.
The consumer requests offset commits after successful object flushes. That is
the intended at-least-once design, not a blanket no-data-loss guarantee.

There are deliberate limits to treat as study material:

- **Index publication is best-effort.** An object can be stored without becoming
  queryable. Index updates use read/modify/write without compare-and-swap.
- **A successful query can be incomplete.** Index and block-read failures are
  logged; the HTTP response does not reliably tell the caller results are partial.
- **Isolation is not authorization.** Clients supply `X-Scope-OrgID`; there is
  no authentication layer. Do not expose this demo to untrusted clients.
- **Recovery needs more than a happy-path test.** Duplicate delivery, flush
  ordering, offset commits, and index recovery must be evaluated together before
  relying on this for durable data.
- Compaction, cache layers, Kubernetes deployment, and distributed tracing are
  plans, not shipped features. [PROJECT.md](PROJECT.md) records the design plan;
  the current code is the implementation.

The useful result is a system you can inspect, break, and measure. It is an
experimental implementation, not a production database.

## License

[MIT](LICENSE).
