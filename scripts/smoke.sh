#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}==>${NC} $*"; }
warn()  { echo -e "${YELLOW}==>${NC} $*"; }
fail()  { echo -e "${RED}==>${NC} $*"; exit 1; }

# Start Kafka if not already running
if ! docker compose ps --status running 2>/dev/null | grep -q ingest-kafka; then
    info "Starting Kafka..."
    docker compose up -d
else
    info "Kafka already running"
fi

# Wait for Kafka to be healthy
info "Waiting for Kafka to be ready..."
timeout=60
elapsed=0
while ! docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "$elapsed" -ge "$timeout" ]; then
        fail "Kafka not ready after ${timeout}s"
    fi
done
info "Kafka ready (${elapsed}s)"

# Run gateway smoke tests
info "Running gateway smoke tests..."
if go test -tags smoke -v -count=1 ./internal/gateway/ -run TestSmoke; then
    echo ""
    info "Gateway smoke tests passed"
else
    echo ""
    fail "Gateway smoke tests failed"
fi

# Run full pipeline smoke test (gateway → Kafka → ingester → storage)
info "Running full pipeline smoke test..."
if go test -tags smoke -v -count=1 -timeout=60s ./internal/pipeline/ -run TestSmoke; then
    echo ""
    info "Pipeline smoke tests passed"
else
    echo ""
    fail "Pipeline smoke tests failed"
fi
