#!/usr/bin/env bash
#
# profile.sh - Capture pprof profiles from gateway and ingester under load.
#
# Prerequisites:
#   - Gateway running on :8080 (admin on :9100)
#   - Ingester running on :9101 (admin port)
#   - Kafka + MinIO running via docker-compose
#   - go run . loadgen available
#
# Usage:
#   ./scripts/profile.sh
#
# Output:
#   profiles/*.pb.gz - CPU, heap, mutex, and block profiles from both services

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GATEWAY_ADMIN="http://localhost:9100"
INGESTER_ADMIN="http://localhost:9101"
PROFILE_DIR="$PROJECT_DIR/profiles"
PROFILE_DURATION=30  # seconds for CPU profile
LOADGEN_RATE=5000
LOADGEN_TENANTS=5
LOADGEN_DURATION=60s

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# Create profiles directory
mkdir -p "$PROFILE_DIR"

# Verify pprof endpoints are accessible
check_pprof() {
    local name=$1
    local url=$2
    if curl -sf "${url}/debug/pprof/" > /dev/null 2>&1; then
        log_info "$name pprof endpoint is accessible at $url"
    else
        log_error "$name pprof endpoint not accessible at $url"
        log_error "Make sure $name is running with admin server enabled."
        exit 1
    fi
}

log_info "Checking pprof endpoints..."
check_pprof "Gateway"  "$GATEWAY_ADMIN"
check_pprof "Ingester" "$INGESTER_ADMIN"

# Start load generator in background
log_info "Starting load generator: ${LOADGEN_RATE} events/sec, ${LOADGEN_TENANTS} tenants, ${LOADGEN_DURATION}"
cd "$PROJECT_DIR"
go run . loadgen \
    --target http://localhost:8080 \
    --rate "$LOADGEN_RATE" \
    --tenants "$LOADGEN_TENANTS" \
    --duration "$LOADGEN_DURATION" &
LOADGEN_PID=$!
trap 'kill $LOADGEN_PID 2>/dev/null; exit' INT TERM

# Wait a few seconds for load to stabilize
log_info "Waiting 5s for load to stabilize..."
sleep 5

# Capture CPU profiles (these block for $PROFILE_DURATION seconds)
log_info "Capturing CPU profiles (${PROFILE_DURATION}s)..."
curl -sf -o "$PROFILE_DIR/cpu-gateway.pb.gz" \
    "${GATEWAY_ADMIN}/debug/pprof/profile?seconds=${PROFILE_DURATION}" &
CPU_GW_PID=$!

curl -sf -o "$PROFILE_DIR/cpu-ingester.pb.gz" \
    "${INGESTER_ADMIN}/debug/pprof/profile?seconds=${PROFILE_DURATION}" &
CPU_ING_PID=$!

wait $CPU_GW_PID && log_info "Gateway CPU profile captured" || log_warn "Gateway CPU profile failed"
wait $CPU_ING_PID && log_info "Ingester CPU profile captured" || log_warn "Ingester CPU profile failed"

# Capture heap profiles (instant snapshots)
log_info "Capturing heap profiles..."
curl -sf -o "$PROFILE_DIR/heap-gateway.pb.gz" \
    "${GATEWAY_ADMIN}/debug/pprof/heap" && log_info "Gateway heap profile captured" || log_warn "Gateway heap profile failed"

curl -sf -o "$PROFILE_DIR/heap-ingester.pb.gz" \
    "${INGESTER_ADMIN}/debug/pprof/heap" && log_info "Ingester heap profile captured" || log_warn "Ingester heap profile failed"

# Capture mutex contention profiles
log_info "Capturing mutex profiles..."
curl -sf -o "$PROFILE_DIR/mutex-gateway.pb.gz" \
    "${GATEWAY_ADMIN}/debug/pprof/mutex" && log_info "Gateway mutex profile captured" || log_warn "Gateway mutex profile failed"

curl -sf -o "$PROFILE_DIR/mutex-ingester.pb.gz" \
    "${INGESTER_ADMIN}/debug/pprof/mutex" && log_info "Ingester mutex profile captured" || log_warn "Ingester mutex profile failed"

# Capture block profiles
log_info "Capturing block profiles..."
curl -sf -o "$PROFILE_DIR/block-gateway.pb.gz" \
    "${GATEWAY_ADMIN}/debug/pprof/block" && log_info "Gateway block profile captured" || log_warn "Gateway block profile failed"

curl -sf -o "$PROFILE_DIR/block-ingester.pb.gz" \
    "${INGESTER_ADMIN}/debug/pprof/block" && log_info "Ingester block profile captured" || log_warn "Ingester block profile failed"

# Capture allocs profiles
log_info "Capturing allocs profiles..."
curl -sf -o "$PROFILE_DIR/allocs-gateway.pb.gz" \
    "${GATEWAY_ADMIN}/debug/pprof/allocs" && log_info "Gateway allocs profile captured" || log_warn "Gateway allocs profile failed"

curl -sf -o "$PROFILE_DIR/allocs-ingester.pb.gz" \
    "${INGESTER_ADMIN}/debug/pprof/allocs" && log_info "Ingester allocs profile captured" || log_warn "Ingester allocs profile failed"

# Wait for loadgen to finish
log_info "Waiting for load generator to finish..."
wait $LOADGEN_PID 2>/dev/null || true

log_info ""
log_info "Profiling complete. Profiles saved to: $PROFILE_DIR/"
log_info ""
log_info "Analyze profiles with:"
log_info "  go tool pprof -top $PROFILE_DIR/cpu-gateway.pb.gz"
log_info "  go tool pprof -http=:8080 $PROFILE_DIR/cpu-gateway.pb.gz"
log_info ""

# List captured profiles
log_info "Captured profiles:"
ls -lh "$PROFILE_DIR"/*.pb.gz 2>/dev/null || log_warn "No profiles captured"
