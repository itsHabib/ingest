# Infra Engineer Plan -- Foundation Phase

## You are in PLAN MODE.

### Project
I want to do a **multi-tenant distributed ingestion service**.

**Goal:** build a **Kafka-backed ingestion engine in Go** in which we **accept multi-tenant HTTP push requests, validate them, and produce events to Kafka -- using Kafka as the durability layer instead of a traditional WAL**.

### Role + Scope
- **Role:** Infra Engineer
- **Scope:** Docker Compose (Kafka KRaft), Makefile, Go module (`go.mod`), `main.go` entry point, cobra CLI skeleton (`cmd/root.go`), centralized config (`internal/config/config.go`). I do NOT own HTTP handlers, data models, Kafka producer logic, or any business logic packages.
- **File you will write:** `/docs/foundation/plans/infra-engineer.md`
- **No-touch zones:** do not edit any other files; do not write code.

---

## Functional Requirements
- **FR1:** `go.mod` initializes module `github.com/itsHabib/ingest` with Go 1.22+ and declares dependencies: `franz-go`, `cobra`, `prometheus/client_golang`, `xxhash/v2`, `golang.org/x/time`, `go.uber.org/zap`.
- **FR2:** `main.go` is a thin entry point that calls `cmd.Execute()`.
- **FR3:** `cmd/root.go` provides a cobra root command with global flags `--log-level` (default `info`) and `--config` (optional config file path). Sets up `go.uber.org/zap` with the chosen level.
- **FR4:** `internal/config/config.go` defines a `Config` struct covering Gateway and Ingester settings, parsed from flags and environment variables (`INGEST_GATEWAY_PORT`, `INGEST_KAFKA_BROKERS`, `INGEST_KAFKA_TOPIC`, `INGEST_INGESTER_CONSUMER_GROUP`, `INGEST_INGESTER_BATCH_SIZE`, `INGEST_INGESTER_BATCH_AGE`, `INGEST_INGESTER_STORAGE_PATH`).
- **FR5:** `docker-compose.yml` runs Kafka in KRaft mode (bitnami/kafka:3.7, single broker, 6 default partitions, port 9092) and Redpanda Console on port 8888.
- **FR6:** `Makefile` provides targets: `build`, `test`, `vet`, `lint`, `docker-up`, `docker-down`, `run-gateway`, `run-ingester`.
- **Tests required:** Unit test for config parsing (defaults, env override). Integration test that `docker compose up` starts Kafka and the broker is reachable.
- **Metrics required:** N/A for infra scaffolding; metrics package is owned by observability-engineer.

## Non-Functional Requirements
- Language/runtime: Go 1.22+
- Local dev: `docker compose up -d` brings up Kafka + Redpanda Console
- Observability: N/A for this role (owned by observability-engineer)
- Safety: Kafka health-check in docker-compose ensures broker is ready before dependent services start
- Documentation: CLAUDE.md + EXPLAIN.md contributions (see below)
- Performance: N/A for scaffolding

---

## Assumptions / System Model
- Deployment environment: local docker-compose; single Kafka broker for dev
- Failure modes: Kafka container fails to start (health-check + restart policy), bad config (validation at startup with clear error messages)
- Delivery guarantees: N/A for infra layer; at-least-once is an ingester concern
- Multi-tenancy: config supports multi-tenant fields but infra layer is tenant-agnostic

---

## Data Model (as relevant to your role)
N/A -- not in scope for this role. The Event and Batch types are owned by model-engineer.

The Config struct is the primary data model for this role:

```
Config
  Gateway
    Port         int      (default: 8080, env: INGEST_GATEWAY_PORT)
    KafkaBrokers []string (default: ["localhost:9092"], env: INGEST_KAFKA_BROKERS)
    KafkaTopic   string   (default: "ingestion-events", env: INGEST_KAFKA_TOPIC)
  Ingester
    KafkaBrokers  []string (default: ["localhost:9092"], env: INGEST_KAFKA_BROKERS)
    KafkaTopic    string   (default: "ingestion-events", env: INGEST_KAFKA_TOPIC)
    ConsumerGroup string   (default: "ingestion-ingesters", env: INGEST_INGESTER_CONSUMER_GROUP)
    BatchSize     int      (default: 1000, env: INGEST_INGESTER_BATCH_SIZE)
    BatchAge      duration (default: 10s, env: INGEST_INGESTER_BATCH_AGE)
    StoragePath   string   (default: "./data", env: INGEST_INGESTER_STORAGE_PATH)
  LogLevel string (default: "info")
```

Validation rules:
- Port must be 1-65535
- KafkaBrokers must be non-empty
- KafkaTopic must be non-empty
- BatchSize must be > 0
- BatchAge must be > 0

---

## APIs (as relevant to your role)
N/A -- not in scope for this role. HTTP endpoints are owned by gateway-engineer.

The CLI surface is:

```
ingest                          # root command, prints help
ingest gateway --port 8080      # starts HTTP gateway (implemented by gateway-engineer)
ingest ingester                 # starts ingester (future phase)
ingest loadgen                  # starts load generator (future phase)

Global flags:
  --log-level string   Log level: debug, info, warn, error (default "info")
  --config string      Path to config file (optional)
```

---

## Architecture / Component Boundaries (as relevant)

Components I own and their responsibilities:

- **`main.go`**: Single-line entry point. Calls `cmd.Execute()`. No logic.
- **`cmd/root.go`**: Cobra root command. Binds global flags (`--log-level`, `--config`). Initializes `go.uber.org/zap` in `PersistentPreRunE`. Provides `func Execute()` called from main.
- **`internal/config/config.go`**: Centralized config struct. Loads from environment variables with sensible defaults. Exported `Load()` function returns a validated `Config`. Subcommands (gateway, ingester) call `config.Load()` in their `RunE`.
- **`docker-compose.yml`**: Kafka (KRaft, bitnami/kafka:3.7) + Redpanda Console. Kafka has health-check on port 9092. Topic auto-creation with 6 default partitions.
- **`Makefile`**: Standard targets wrapping `go build`, `go test`, `go vet`, `golangci-lint`, `docker compose up/down`, `go run . gateway/ingester`.

How config propagates: environment variables and flags are read at startup. No hot-reload. Restart to change config. Subcommands access config via `config.Load()` which merges defaults -> env vars -> flags (flags win).

Concurrency model: N/A for infra scaffolding.

---

## Correctness Invariants (must be explicit)
- Config `Load()` always returns valid config or a non-nil error -- never a half-initialized struct.
- Default values are usable out-of-the-box for local development (localhost:9092, port 8080, etc.).
- `docker compose up -d` starts Kafka broker that is reachable on localhost:9092 within 30 seconds.
- Cobra subcommands registered but empty (return nil) until their owning engineers implement them.
- Environment variable names follow the pattern `INGEST_<SECTION>_<FIELD>` consistently.

---

## Tests
- **Unit tests:**
  - `internal/config/config_test.go`: Test default values, env var overrides, validation errors (bad port, empty brokers, zero batch size).
- **Integration tests:**
  - Manual: `docker compose up -d && docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` confirms Kafka is alive.
  - `make test` runs all Go tests.
- **Property/fuzz tests:** N/A for config scaffolding.
- **Failure injection:** N/A for this phase.

Exact commands:
- `go test ./internal/config/...`
- `go test ./...`
- `go vet ./...`
- `make test`

---

## Benchmarks + "Success"
N/A -- infra scaffolding has no hot path to benchmark. Success criteria:
- `go build .` produces a binary
- `go test ./...` passes
- `go vet ./...` clean
- `docker compose up -d` starts Kafka (healthy) + Redpanda Console
- `go run . --help` prints cobra help text
- `go run . gateway --help` prints gateway subcommand help

---

# Recommended API surface

**Config package (`internal/config`):**
```
func Load() (*Config, error)            // load config from env + defaults
func (c *Config) Validate() error       // validate all fields
```

**CLI (`cmd/`):**
```
func Execute()                          // entry point, called from main.go
root command: global flags --log-level, --config
gateway subcommand: --port flag (overrides config)
ingester subcommand: stub (returns nil)
loadgen subcommand: stub (returns nil)
```

# Folder structure

```
ingest/
  go.mod                          # [infra-engineer] module declaration + deps
  main.go                         # [infra-engineer] thin entry point
  cmd/
    root.go                       # [infra-engineer] cobra root + global flags + zap init
    gateway.go                    # [infra-engineer] stub; [gateway-engineer] implements RunE
    ingester.go                   # [infra-engineer] stub
    loadgen.go                    # [infra-engineer] stub
  internal/
    config/
      config.go                   # [infra-engineer] Config struct + Load() + Validate()
      config_test.go              # [infra-engineer] unit tests
    model/                        # [model-engineer]
    tenant/                       # [model-engineer / tenancy-engineer]
    gateway/                      # [gateway-engineer]
    ring/                         # [ring-engineer]
    ingester/                     # [ingester-engineer]
    storage/                      # [storage-engineer]
    observability/                # [observability-engineer]
  docker-compose.yml              # [infra-engineer]
  Makefile                        # [infra-engineer]
```

# Step-by-step task plan (small commits)

See "Tighten the plan" section below.

# Benchmark plan

N/A for infra scaffolding. No hot-path code. Success is "it compiles, tests pass, Kafka starts."

---

# Tighten the plan into 4-7 small tasks (STRICT)

### Task 1: Go module + entry point + cobra root
- **Outcome:** `go build .` compiles; `go run . --help` prints help with `--log-level` and `--config` flags; zap is initialized.
- **Files to create:** `go.mod`, `main.go`, `cmd/root.go`
- **Verification:**
  - `go build .` -- exits 0
  - `go run . --help` -- shows root help
  - `go run . --log-level debug` -- no error (zap set to debug)
- **Commit message:** `feat: init go module with cobra CLI skeleton and zap`

### Task 2: Config package with defaults, env parsing, validation
- **Outcome:** `config.Load()` returns a valid Config from env vars with sensible defaults. Validation rejects invalid values.
- **Files to create:** `internal/config/config.go`, `internal/config/config_test.go`
- **Verification:**
  - `go test ./internal/config/... -v` -- all pass
  - Tests cover: defaults, env override, validation errors
- **Commit message:** `feat: add config package with env parsing and validation`

### Task 3: Cobra subcommand stubs (gateway, ingester, loadgen)
- **Outcome:** `go run . gateway --help`, `go run . ingester --help`, `go run . loadgen --help` all work. Gateway accepts `--port` flag. RunE functions are placeholders that load config and log startup.
- **Files to create:** `cmd/gateway.go`, `cmd/ingester.go`, `cmd/loadgen.go`
- **Verification:**
  - `go run . gateway --help` -- shows gateway flags
  - `go run . gateway --port 9090` -- logs "starting gateway on :9090" then exits (stub)
  - `go build .` -- still compiles
- **Commit message:** `feat: add cobra subcommand stubs for gateway, ingester, loadgen`

### Task 4: Docker Compose -- Kafka KRaft + Redpanda Console
- **Outcome:** `docker compose up -d` starts Kafka in KRaft mode on port 9092 with 6 default partitions, and Redpanda Console on port 8888.
- **Files to create:** `docker-compose.yml`
- **Verification:**
  - `docker compose up -d` -- both containers start
  - `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` -- responds (Kafka alive)
  - `curl -s localhost:8888/api/topics` -- Redpanda Console responds
  - `docker compose down` -- clean shutdown
- **Commit message:** `feat: add docker-compose with Kafka KRaft and Redpanda Console`

### Task 5: Makefile with all targets
- **Outcome:** Standard Makefile with targets: `build`, `test`, `vet`, `lint`, `docker-up`, `docker-down`, `run-gateway`, `run-ingester`.
- **Files to create:** `Makefile`
- **Verification:**
  - `make build` -- produces binary
  - `make test` -- runs tests
  - `make vet` -- clean
  - `make docker-up` / `make docker-down` -- starts/stops compose
- **Commit message:** `feat: add Makefile with build, test, and docker targets`

### Top 3 risks/unknowns

1. **Bitnami Kafka KRaft config drift** -- Bitnami env var names change between versions. Validate by pulling `bitnami/kafka:3.7` and checking `docker compose up -d` succeeds with our config. < 5 min.

2. **`franz-go` module version compatibility with Go 1.22** -- Validate by running `go mod tidy` after adding the dependency and confirming `go build` succeeds. < 2 min.

3. **Redpanda Console image compatibility with KRaft-only Kafka** -- Some console versions expect Schema Registry or specific Kafka APIs. Validate by starting compose and hitting `curl localhost:8888/api/topics`. < 5 min.

---

## CLAUDE.md contributions (do NOT write the file; propose content)

### From infra-engineer

**Coding style rules:**
- Use `go.uber.org/zap` for all logging -- never `fmt.Println`, `log.Printf`, or `log/slog`
- Config values from env vars follow `INGEST_<SECTION>_<FIELD>` naming
- All cobra commands go in `cmd/` package, one file per subcommand
- `internal/` packages are not importable externally -- this is intentional

**Dev commands:**
```bash
make build          # compile binary
make test           # go test ./...
make vet            # go vet ./...
make lint           # golangci-lint run (install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
make docker-up      # docker compose up -d
make docker-down    # docker compose down
make run-gateway    # go run . gateway
make run-ingester   # go run . ingester
```

**Before you commit checklist:**
- [ ] `make vet` passes
- [ ] `make test` passes
- [ ] `go mod tidy` leaves no diff
- [ ] Docker Compose still starts cleanly if you changed `docker-compose.yml`

**Guardrails:**
- Do not add dependencies without justification -- prefer stdlib
- Do not change `go.mod` module path
- Kafka topic name `ingestion-events` is a project constant; do not rename without updating all references

---

## EXPLAIN.md contributions (do NOT write the file; propose outline bullets)

### From infra-engineer

**Flow / architecture:**
- `main.go` -> `cmd.Execute()` -> cobra dispatches to subcommand -> subcommand calls `config.Load()` -> runs service
- Config precedence: defaults < environment variables < CLI flags

**Key engineering decisions + tradeoffs:**
- Chose `bitnami/kafka` in KRaft mode to avoid Zookeeper dependency (simpler local setup, matches Kafka's future direction)
- Single broker with 6 partitions: enough for local dev parallelism without complexity
- Redpanda Console included for visual debugging during development
- Config via env vars + flags (no YAML/TOML config file for MVP) to keep it simple; `--config` flag reserved for future use
- `cobra` for CLI because it's the Go standard and consistent with the team's other tools

**Limits of MVP + next steps:**
- Single Kafka broker -- not representative of production partition rebalancing
- No TLS on Kafka -- fine for local dev, would need TLS + SASL for production
- No config hot-reload -- restart required for config changes
- `--config` file support is stubbed but not implemented

**How to run locally + how to validate:**
- `make docker-up` starts Kafka + Redpanda Console
- `make run-gateway` starts the gateway (once implemented by gateway-engineer)
- Verify Kafka: `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`
- Verify Console: open `http://localhost:8888` in browser

---

## READY FOR APPROVAL
