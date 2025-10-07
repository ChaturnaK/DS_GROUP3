#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  set +e
  [[ -n "${UPID:-}" ]] && kill "$UPID" 2>/dev/null || true
  [[ -n "${SN1:-}" ]] && kill "$SN1" 2>/dev/null || true
  [[ -n "${SN2:-}" ]] && kill "$SN2" 2>/dev/null || true
  [[ -n "${SN3:-}" ]] && kill "$SN3" 2>/dev/null || true
  [[ -n "${MS1:-}" ]] && kill "$MS1" 2>/dev/null || true
  [[ -n "${MS2:-}" ]] && kill "$MS2" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

mvn -q -DskipTests package

if command -v zkServer >/dev/null 2>&1; then
  zkServer start || true
  sleep 2
fi

CLIENT_CP="client/target/classes:client/target/*:common/target/classes:common/target/*"
METADATA_CP="metadata/target/classes:metadata/target/*:common/target/classes:common/target/*"
STORAGE_CP="storage/target/classes:storage/target/*:common/target/classes:common/target/*"

java -cp "$METADATA_CP" com.ds.metadata.MetadataServer \
  --port 7000 --zk localhost:2181 --replication 3 > logs-ms1.txt 2>&1 &
MS1=$!
java -cp "$METADATA_CP" com.ds.metadata.MetadataServer \
  --port 7001 --zk localhost:2181 --replication 3 > logs-ms2.txt 2>&1 &
MS2=$!

export CHAOS_DROP_PCT=${CHAOS_DROP_PCT:-5}
export CHAOS_DELAY_MS=${CHAOS_DELAY_MS:-10}
java -cp "$STORAGE_CP" com.ds.storage.StorageNode \
  --port 8001 --data ./data/node1 --zone z1 --zk localhost:2181 > logs-sn1.txt 2>&1 &
SN1=$!
java -cp "$STORAGE_CP" com.ds.storage.StorageNode \
  --port 8002 --data ./data/node2 --zone z2 --zk localhost:2181 > logs-sn2.txt 2>&1 &
SN2=$!
java -cp "$STORAGE_CP" com.ds.storage.StorageNode \
  --port 8003 --data ./data/node3 --zone z3 --zk localhost:2181 > logs-sn3.txt 2>&1 &
SN3=$!

sleep 5

echo "==== Step 1: Upload 512MB with chaos W=2 ===="
dd if=/dev/urandom of=/tmp/demo512.bin bs=1m count=512 2>/dev/null
java -Dds.zk=localhost:2181 -cp "$CLIENT_CP" \
  com.ds.client.DsClient put /tmp/demo512.bin /demo/file512.bin
echo "OK upload quorum"

echo "==== Step 2: Kill one storage node during upload retry path ===="
dd if=/dev/urandom of=/tmp/demo512b.bin bs=1m count=512 2>/dev/null
(
  java -Dds.zk=localhost:2181 -cp "$CLIENT_CP" \
    com.ds.client.DsClient put /tmp/demo512b.bin /demo/file512b.bin && \
    echo "OK upload with one node killed"
) &
UPID=$!
sleep 2
kill -9 "$SN2" 2>/dev/null || true
wait "$UPID"

java -cp "$STORAGE_CP" com.ds.storage.StorageNode \
  --port 8002 --data ./data/node2 --zone z2 --zk localhost:2181 > logs-sn2-restart.txt 2>&1 &
SN2=$!

echo "==== Step 3: Healing should restore N=3 replicas ===="
sleep 12
echo "ZK replicas for a sample block (if zkCli available):"
if command -v zkCli.sh >/dev/null 2>&1; then
  zkCli.sh -server localhost:2181 ls /ds/meta/blocks 2>/dev/null | head -n 5
fi

echo "==== Step 4: Quorum read with one node down (R=2) ===="
java -Dds.zk=localhost:2181 -cp "$CLIENT_CP" \
  com.ds.client.DsClient get /demo/file512.bin ./download.bin
cmp /tmp/demo512.bin ./download.bin && echo "OK download quorum"

echo "==== Step 5: Kill metadata leader mid-PlanPut; client should retry to new leader ===="
LEADER=$(java -Dds.zk=localhost:2181 -cp "$CLIENT_CP" com.ds.client.DsClient leader 2>/dev/null || echo "")
if [[ "$LEADER" == *":7001" ]]; then
  kill -9 "$MS2" 2>/dev/null || true
else
  kill -9 "$MS1" 2>/dev/null || true
fi
dd if=/dev/urandom of=/tmp/demo64.bin bs=1m count=64 2>/dev/null
java -Dds.zk=localhost:2181 -cp "$CLIENT_CP" \
  com.ds.client.DsClient put /tmp/demo64.bin /demo/leaderfail.bin
echo "OK leader failover handled by client"

echo "==== Step 6: Metrics snapshot directory ===="
ls -l metrics || true

echo "Done. Logs: logs-ms1.txt logs-ms2.txt logs-sn1.txt logs-sn2.txt logs-sn2-restart.txt logs-sn3.txt"
