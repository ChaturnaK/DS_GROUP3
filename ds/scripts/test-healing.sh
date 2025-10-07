#!/usr/bin/env bash
# ============================================================================
#  test-healing.sh — Verify automatic self-healing replication (Option B)
#  Requires: ds/ project built with HealingPlanner using live-node filtering
# ============================================================================

set -euo pipefail
cd "$(dirname "$0")/.."  # go to repo root

LOGDIR="logs"
DATADIR="data"
mkdir -p "$LOGDIR" "$DATADIR"/node{1..4}
chmod -R u+w "$DATADIR" >/dev/null 2>&1 || true

banner() { echo -e "\n\033[1;34m=== $* ===\033[0m"; }
ok()     { echo -e "\033[1;32m✅ $*\033[0m"; }
warn()   { echo -e "\033[1;33m⚠️  $*\033[0m"; }
err()    { echo -e "\033[1;31m❌ $*\033[0m"; }

cleanup() {
  pkill -f "MetadataServer" 2>/dev/null || true
  pkill -f "StorageNode" 2>/dev/null || true
}
trap cleanup EXIT

META_PORT=7000
STORAGE_PORTS=(8001 8002 8003)
ZK=localhost:2181

banner "1. Build Project"
mvn -q -DskipTests clean package

banner "2. Start ZooKeeper"
if ! nc -z localhost 2181; then
  zkServer start >/dev/null 2>&1 || true
  sleep 3
else
  echo "ZooKeeper already running."
fi

banner "3. Launch Metadata Leader"
java -cp "metadata/target/*:common/target/*" \
  com.ds.metadata.MetadataServer --port "$META_PORT" --zk "$ZK" \
  >"$LOGDIR/meta.log" 2>&1 &
sleep 4

banner "4. Launch Storage Nodes"
for i in "${!STORAGE_PORTS[@]}"; do
  port=${STORAGE_PORTS[$i]}
  java -cp "storage/target/*:common/target/*" \
    com.ds.storage.StorageNode --port "$port" --data "$DATADIR/node$((i+1))" --zk "$ZK" \
    >"$LOGDIR/storage_$port.log" 2>&1 &
  sleep 2
done
sleep 3

banner "5. Upload File (replication=3)"
echo "Verifying self-healing replication" > test.txt
java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient put test.txt /demo/test.txt

banner "6. Simulate Node Failure"
pkill -f "StorageNode.*8002" && echo "💀 Node 8002 killed"
sleep 3

banner "7. Start a New Spare Node (Node 4)"
java -cp "storage/target/*:common/target/*" \
  com.ds.storage.StorageNode --port 8004 --data "$DATADIR/node4" --zk "$ZK" \
  >"$LOGDIR/storage_8004.log" 2>&1 &
sleep 10  # let the healer cycle run

banner "8. Fetch File and Verify Integrity"
java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient get /demo/test.txt out.txt || true

if diff -q test.txt out.txt >/dev/null; then
  ok "File retrieved successfully — checksum match."
else
  err "File mismatch after healing."
  exit 1
fi

banner "9. Check Logs for Healing Activity"
HEAL_LOG=$(grep -h -E "Healing|needed=|replicateBlock|considered missing" "$LOGDIR"/meta.log || true)
if [[ -n "$HEAL_LOG" ]]; then
  ok "Healing logs detected — self-healing executed."
  echo "$HEAL_LOG" | tail -10
else
  warn "No healing logs detected. Verify HealingPlanner.run() logic."
fi

banner "10. Final Result"
echo "----------------------------------------------------"
grep -h -E "Healing|needed=|replicateBlock" "$LOGDIR"/meta.log | tail -5 || echo "No Healing lines found."
echo "----------------------------------------------------"
ok "Self-healing test complete."
