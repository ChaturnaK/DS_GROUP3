#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Start ZooKeeper if available and wait until it's up
echo "Starting ZooKeeper (if available)..."
if [[ -x "$ROOT_DIR/scripts/start-zk.sh" ]]; then
  "$ROOT_DIR/scripts/start-zk.sh" || true
else
  echo "Hint: ZooKeeper not auto-started. Ensure localhost:2181 is up."
fi

echo -n "Waiting for ZooKeeper on localhost:2181"
for i in {1..30}; do
  if nc -z localhost 2181 >/dev/null 2>&1; then
    echo " - ready"
    break
  fi
  echo -n "."
  sleep 1
  if [[ "$i" -eq 30 ]]; then
    echo "\nZooKeeper did not become ready. Proceeding anyway..."
  fi
done

# Build all modules
mvn -q -DskipTests clean package

echo "Launching Stage 2 services..."
COMMON_CP="common/target/classes:common/target/*"

# Helper: find next free port starting from a given number
find_free_port() {
  local p="$1"
  while nc -z localhost "$p" >/dev/null 2>&1; do
    p=$((p+1))
  done
  echo "$p"
}

# Pick metadata port: prefer 7000, fallback to next free if busy
META_PORT="$(find_free_port 7000)"
echo "Using MetadataServer port: $META_PORT"

java -cp "metadata/target/classes:metadata/target/*:${COMMON_CP}" \
  com.ds.metadata.MetadataServer --port "$META_PORT" --zk localhost:2181 --replication 3 &

# Pick three free storage ports starting from 8001
PORT1="$(find_free_port 8001)"
PORT2="$(find_free_port $((PORT1+1)))"
PORT3="$(find_free_port $((PORT2+1)))"
echo "Using StorageNode ports: $PORT1, $PORT2, $PORT3"

java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port "$PORT1" --data ./data/node1 --zone z1 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port "$PORT2" --data ./data/node2 --zone z2 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port "$PORT3" --data ./data/node3 --zone z3 --zk localhost:2181 &

wait
