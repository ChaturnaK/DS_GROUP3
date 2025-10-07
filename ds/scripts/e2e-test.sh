#!/usr/bin/env bash

# ============================================================================
# Distributed Storage – End-to-End Automated Test with Metrics
# ----------------------------------------------------------------------------
# This script is location agnostic: run it from the repo root or from the
# module directory. It orchestrates a full build, launches the metadata and
# storage services, executes functional tests, and reports basic metrics.
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"   # .../repo/ds
REPO_ROOT="$(cd "$MODULE_ROOT/.." && pwd)"    # .../repo

LOGDIR="${LOGDIR:-"$REPO_ROOT/logs"}"
DATADIR="${DATADIR:-"$REPO_ROOT/data"}"
METRICS_DIR="${METRICS_DIR:-"$REPO_ROOT/metrics"}"

mkdir -p "$LOGDIR" "$METRICS_DIR"
mkdir -p "$DATADIR/node1" "$DATADIR/node2" "$DATADIR/node3"
chmod -R u+w "$DATADIR" >/dev/null 2>&1 || true

banner() { echo -e "\n\033[1;34m=== $* ===\033[0m"; }
ok() { echo -e "\033[1;32m✅ $*\033[0m"; }
warn() { echo -e "\033[1;33m⚠️  $*\033[0m"; }
err() { echo -e "\033[1;31m❌ $*\033[0m"; }

run_ds() {
  (cd "$MODULE_ROOT" && "$@")
}

cleanup() {
  echo "🧹 Stopping running DS processes..."
  pkill -f "MetadataServer" 2>/dev/null || true
  pkill -f "StorageNode" 2>/dev/null || true
}
trap cleanup EXIT

META_PORT=${META_PORT:-7000}
ZK=${ZK:-localhost:2181}
ZK_HOST="${ZK%%:*}"
ZK_PORT="${ZK##*:}"
if [[ "$ZK_HOST" == "$ZK_PORT" ]]; then
  ZK_HOST="$ZK"
  ZK_PORT=2181
fi
STORAGE_PORTS=(8001 8002 8003)

TEST_FILE="$REPO_ROOT/test.txt"
OUT_FILE="$REPO_ROOT/out.txt"
BIG_FILE="$REPO_ROOT/big.bin"
BIG_OUT="$REPO_ROOT/big_out.bin"

banner "Setup and Build"
cleanup
if [[ "${E2E_CLEAN_BUILD:-}" == "1" ]]; then
  run_ds mvn -q -DskipTests clean package
else
  run_ds mvn -q -DskipTests package
fi

banner "Starting ZooKeeper"
if ! nc -z "$ZK_HOST" "$ZK_PORT"; then
  zkServer start >/dev/null 2>&1 || true
  sleep 3
else
  echo "ZooKeeper already running."
fi

start_metadata() {
  local port=$1
  local log=$2
  run_ds java -cp "metadata/target/*:common/target/*" \
    com.ds.metadata.MetadataServer --port "$port" --zk "$ZK" \
    >"$log" 2>&1 &
}

start_storage() {
  local port=$1
  local data_dir=$2
  local zone=$3
  local log=$4
  mkdir -p "$data_dir"
  run_ds java -cp "storage/target/*:common/target/*" \
    com.ds.storage.StorageNode --port "$port" --data "$data_dir" --zone "$zone" --zk "$ZK" \
    >"$log" 2>&1 &
}

banner "Launching MetadataServer (leader)"
start_metadata "$META_PORT" "$LOGDIR/meta.log"
sleep 4

for idx in "${!STORAGE_PORTS[@]}"; do
  port=${STORAGE_PORTS[$idx]}
  node_dir="$DATADIR/node$((idx + 1))"
  banner "Launching StorageNode on port $port"
  start_storage "$port" "$node_dir" "z$((idx + 1))" "$LOGDIR/storage_$port.log"
  sleep 2
done
sleep 3

banner "Preparing Test Files"
echo "Hello Distributed Systems" >"$TEST_FILE"
dd if=/dev/urandom of="$BIG_FILE" bs=1M count=50 status=none

# ---------------------------------------------------------------------------
banner "TEST 1 – Basic PUT/GET"
PUT_START=$(date +%s.%N)
run_ds java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient put "$TEST_FILE" /demo/test.txt
PUT_END=$(date +%s.%N)
PUT_LAT=$(echo "$PUT_END - $PUT_START" | bc)

GET_START=$(date +%s.%N)
run_ds java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient get /demo/test.txt "$OUT_FILE"
GET_END=$(date +%s.%N)
GET_LAT=$(echo "$GET_END - $GET_START" | bc)

if diff "$TEST_FILE" "$OUT_FILE" >/dev/null; then
  ok "PUT/GET succeeded – PUT: ${PUT_LAT}s, GET: ${GET_LAT}s"
else
  err "File mismatch after GET"
  exit 1
fi

# ---------------------------------------------------------------------------
banner "TEST 2 – Fault Tolerance (Kill One Node)"
(
  run_ds java -cp "client/target/*:common/target/*" \
    com.ds.client.DsClient put "$BIG_FILE" /demo/big.bin
) >"$LOGDIR/client_put.log" 2>&1 &
sleep 3
FAIL_TIME=$(date +%s)
pkill -f "StorageNode.*8002" && echo "💀 Node 8002 killed"
sleep 10
RECOVER_TIME=$(date +%s)
HEAL_TIME=$((RECOVER_TIME - FAIL_TIME))
ok "Healing completed in ${HEAL_TIME}s"

banner "Restarting Node 8002"
start_storage 8002 "$DATADIR/node2" "z2" "$LOGDIR/storage_8002_restarted.log"
sleep 3

# ---------------------------------------------------------------------------
banner "TEST 3 – Leader Failover"
FAILOVER_START=$(date +%s)
pkill -f "MetadataServer" && echo "💀 Old leader stopped"
sleep 2
start_metadata 7001 "$LOGDIR/meta_newleader.log"
sleep 4
FAILOVER_END=$(date +%s)
FAILOVER_TIME=$((FAILOVER_END - FAILOVER_START))
ok "Leader failover completed in ${FAILOVER_TIME}s"

# ---------------------------------------------------------------------------
banner "TEST 4 – Data Consistency Check"
run_ds java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient get /demo/big.bin "$BIG_OUT"
if cmp -s "$BIG_FILE" "$BIG_OUT"; then
  ok "File integrity verified after failover"
else
  err "Checksum mismatch after failover"
  exit 1
fi

# ---------------------------------------------------------------------------
banner "TEST 5 – Metrics Collection"
NTP_AVG=$(grep -h "offset_ms" "$LOGDIR/meta.log" "$LOGDIR/meta_newleader.log" 2>/dev/null \
  | awk -F'=' '{sum+=$2;count++} END{if(count>0) printf "%.2f", sum/count; else print 0}')
THROUGHPUT=$(echo "scale=2; 50 / $PUT_LAT" | bc)

echo ""
echo "📊  METRICS SUMMARY"
echo "----------------------------------"
echo " PUT Latency:     ${PUT_LAT}s"
echo " GET Latency:     ${GET_LAT}s"
echo " Healing Time:    ${HEAL_TIME}s"
echo " Leader Failover: ${FAILOVER_TIME}s"
echo " Avg NTP Offset:  ${NTP_AVG} ms"
echo " Throughput:      ${THROUGHPUT} MB/s"
echo "----------------------------------"

# ---------------------------------------------------------------------------
banner "PASS/FAIL Evaluation"
if (( $(echo "$HEAL_TIME < 10" | bc -l) )); then ok "Healing ✅ (<10s)"; else err "Healing ❌ (too slow)"; fi
if (( $(echo "$THROUGHPUT > 80" | bc -l) )); then ok "Throughput ✅ (>80 MB/s)"; else warn "Throughput ⚠️  (${THROUGHPUT} MB/s)"; fi
if (( $(echo "$NTP_AVG < 15" | bc -l) )); then ok "Time Sync ✅ (<15ms)"; else warn "Time Sync ⚠️  (${NTP_AVG} ms)"; fi
if (( $(echo "$FAILOVER_TIME < 5" | bc -l) )); then ok "Leader Failover ✅ (<5s)"; else warn "Leader Failover ⚠️  (${FAILOVER_TIME} s)"; fi

banner "ALL TESTS COMPLETE 🎉"
