#!/usr/bin/env bash
set -euo pipefail

# e2e.sh — Full lifecycle for integration tests:
#   1. Start Docker Compose (Kafka)
#   2. Wait for Kafka to be healthy
#   3. Run integration tests
#   4. Tear down Docker Compose
#
# Usage: ./scripts/e2e.sh [go test flags...]
# Example: ./scripts/e2e.sh -run TestEndToEnd_SingleTenant -v

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# Ensure cleanup on exit, regardless of success or failure.
cleanup() {
    echo "--- Tearing down Docker Compose ---"
    docker compose down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "--- Starting Docker Compose (Kafka) ---"
docker compose up -d kafka

echo "--- Waiting for Kafka to be healthy ---"
MAX_WAIT=60
WAITED=0
until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        echo "ERROR: Kafka did not become healthy within ${MAX_WAIT}s"
        exit 1
    fi
    echo "  Waiting for Kafka... (${WAITED}s/${MAX_WAIT}s)"
    sleep 5
    WAITED=$((WAITED + 5))
done
echo "Kafka is healthy."

echo "--- Running integration tests ---"
go test -tags integration -v -count=1 -timeout 120s "$@" ./...
TEST_EXIT=$?

echo "--- Integration tests finished (exit code: $TEST_EXIT) ---"
exit "$TEST_EXIT"
