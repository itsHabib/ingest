#!/usr/bin/env bash
set -euo pipefail

# Load test orchestration script.
# Runs each load profile sequentially, captures JSON results to benchmarks/,
# and prints a summary comparison table at the end.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$ROOT_DIR/ingestion-engine"
PROFILE_DIR="$ROOT_DIR/configs/load-profiles"
BENCHMARK_DIR="$ROOT_DIR/benchmarks"
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

PROFILES=(
    baseline
    high-throughput
    many-tenants
    burst
)

# Build the binary.
echo "==> Building ingestion-engine..."
(cd "$ROOT_DIR" && go build -o "$BINARY" .)

# Create benchmarks directory.
mkdir -p "$BENCHMARK_DIR"

# Health check: wait for gateway to be ready.
echo "==> Waiting for gateway at $GATEWAY_URL..."
MAX_RETRIES=30
RETRY=0
while ! curl -sf "$GATEWAY_URL/ready" > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: Gateway not ready after ${MAX_RETRIES}s. Is docker-compose running?"
        exit 1
    fi
    sleep 1
done
echo "    Gateway is ready."

# Run each profile.
RESULTS=()
for PROFILE in "${PROFILES[@]}"; do
    PROFILE_FILE="$PROFILE_DIR/$PROFILE.yaml"
    OUTPUT_FILE="$BENCHMARK_DIR/load-test-${PROFILE}-${TIMESTAMP}.json"

    if [ ! -f "$PROFILE_FILE" ]; then
        echo "WARNING: Profile $PROFILE_FILE not found, skipping."
        continue
    fi

    echo ""
    echo "==> Running profile: $PROFILE"
    echo "    Profile: $PROFILE_FILE"
    echo "    Output:  $OUTPUT_FILE"

    "$BINARY" loadgen \
        --profile "$PROFILE_FILE" \
        --target "$GATEWAY_URL" \
        --output "$OUTPUT_FILE" || true

    if [ -f "$OUTPUT_FILE" ]; then
        RESULTS+=("$OUTPUT_FILE")
        echo "    Result saved to $OUTPUT_FILE"
    else
        echo "    WARNING: No output file generated for $PROFILE"
    fi
done

# Print summary table.
echo ""
echo "=============================================="
echo "  Load Test Summary ($TIMESTAMP)"
echo "=============================================="
printf "%-18s %10s %10s %10s %10s %10s\n" \
    "Profile" "Events" "Err%" "Tput" "P50(ms)" "P99(ms)"
echo "----------------------------------------------------------------------"

for RESULT_FILE in "${RESULTS[@]}"; do
    PROFILE_NAME=$(basename "$RESULT_FILE" | sed "s/load-test-//;s/-${TIMESTAMP}.json//")

    # Parse JSON fields using Go's json output format (durations are in nanoseconds).
    TOTAL=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['total_events'])" 2>/dev/null || echo "?")
    FAILED=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(d['failed_events'])" 2>/dev/null || echo "0")
    TPUT=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(f\"{d['events_per_sec']:.0f}\")" 2>/dev/null || echo "?")
    P50=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(f\"{d['latency_p50_ns']/1e6:.2f}\")" 2>/dev/null || echo "?")
    P99=$(python3 -c "import json; d=json.load(open('$RESULT_FILE')); print(f\"{d['latency_p99_ns']/1e6:.2f}\")" 2>/dev/null || echo "?")

    ERR_PCT="0.00"
    if [ "$TOTAL" != "?" ] && [ "$TOTAL" != "0" ]; then
        ERR_PCT=$(python3 -c "print(f'{$FAILED/$TOTAL*100:.2f}')" 2>/dev/null || echo "?")
    fi

    printf "%-18s %10s %9s%% %10s %10s %10s\n" \
        "$PROFILE_NAME" "$TOTAL" "$ERR_PCT" "$TPUT" "$P50" "$P99"
done

echo "=============================================="
echo ""
echo "Results stored in: $BENCHMARK_DIR/"
