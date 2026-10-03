#!/usr/bin/env bash
# Chaos test runner: brings up Docker Compose, runs chaos tests, tears down.
# Usage: ./scripts/chaos.sh
# Exit code: 0 on success, non-zero on failure. Always cleans up Docker.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# Cleanup function -- always runs on exit.
cleanup() {
    echo "==> Tearing down Docker Compose..."
    docker compose down --remove-orphans --timeout 10 2>/dev/null || true
}

trap cleanup EXIT INT TERM

echo "==> Starting Docker Compose (Kafka)..."
docker compose up -d kafka

echo "==> Waiting for Kafka to be ready..."
MAX_WAIT=60
ELAPSED=0
until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo "ERROR: Kafka not ready after ${MAX_WAIT}s"
        exit 1
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    echo "    waiting... (${ELAPSED}s)"
done
echo "==> Kafka is ready (took ${ELAPSED}s)"

echo "==> Running chaos tests..."
go test -tags chaos -v -count=1 -timeout 120s ./...
TEST_EXIT=$?

if [ "$TEST_EXIT" -eq 0 ]; then
    echo "==> All chaos tests PASSED"
else
    echo "==> Chaos tests FAILED (exit code: $TEST_EXIT)"
fi

exit $TEST_EXIT
