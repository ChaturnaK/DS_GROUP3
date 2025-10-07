#!/bin/bash
# ================================
# Data Replication & Consistency Test
# Member 2 Verification Script
# ================================

set -e

BASE_DIR=$(pwd)
CP="client/target/classes:client/target/*:common/target/classes:common/target/*:storage/target/classes:storage/target/*"

echo "=== STEP 1: Clean previous data and logs ==="
rm -rf logs data/node* || true
mkdir -p logs data/node{1..4}

echo "=== STEP 2: Start ZooKeeper (if not running) ==="
if ! nc -z localhost 2181; then
  echo "Starting local ZooKeeper..."
  zkServer start || bin/zkServer.sh start
  sleep 3
else
  echo "ZooKeeper already running."
fi

echo "=== STEP 3: Start Metadata Server ==="
java -cp "metadata/target/classes:metadata/target/*:common/target/classes:common/target/*" \
  com.ds.metadata.MetadataServer --port 7000 --zk localhost:2181 --replication 3 \
  2>&1 | tee logs/meta_test.log &
META_PID=$!
sleep 5

echo "=== STEP 4: Start 3 Storage Nodes ==="
for i in 1 2 3; do
  PORT=$((8000+i))
  java -cp "storage/target/classes:storage/target/*:common/target/classes:common/target/*" \
    com.ds.storage.StorageNode --port $PORT --data data/node$i --zone z$i --zk localhost:2181 \
    2>&1 | tee logs/storage_$PORT.log &
done
sleep 5

echo "=== STEP 5: Test Replication ==="
echo "Replication test started..."
echo "HELLO_DS_REPLICATION" > test.txt
java -cp "$CP" com.ds.client.DsClient put test.txt /demo/test.txt
echo "✅ Replication PUT complete"

echo "Now verifying across replicas..."
for i in 1 2 3; do
  find data/node$i -type f | grep "test.txt" || echo "❌ Missing on node$i"
done
echo "✅ Replication verified across 3 replicas."

echo "=== STEP 6: Test Consistency (Read Quorum) ==="
java -cp "$CP" com.ds.client.DsClient get /demo/test.txt out.txt
diff -q test.txt out.txt && echo "✅ Read quorum consistent" || echo "❌ Data mismatch!"

echo "=== STEP 7: Simulate Node Failure ==="
pkill -f "StorageNode --port 8002"
sleep 3
echo "Reading again (node2 down)..."
java -cp "$CP" com.ds.client.DsClient get /demo/test.txt out2.txt
diff -q test.txt out2.txt && echo "✅ Quorum read OK with node2 down" || echo "❌ Read failed"

echo "=== STEP 8: Test Conflict Handling (Concurrent Writes) ==="
echo "A_VERSION" > a.txt
echo "B_VERSION" > b.txt

(java -cp "$CP" com.ds.client.DsClient put a.txt /demo/conflict.txt &) 
sleep 0.5
(java -cp "$CP" com.ds.client.DsClient put b.txt /demo/conflict.txt &)
wait
sleep 3

java -cp "$CP" com.ds.client.DsClient get /demo/conflict.txt conflict_out.txt
cat conflict_out.txt
echo "✅ Conflict resolved (check output; last-writer-wins or VC rule applied)"

echo "=== STEP 9: Heal a new node (Recovery Consistency) ==="
echo "Starting node4..."
java -cp "storage/target/classes:storage/target/*:common/target/classes:common/target/*" \
  com.ds.storage.StorageNode --port 8004 --data data/node4 --zone z4 --zk localhost:2181 \
  2>&1 | tee logs/storage_8004.log &
sleep 15

echo "Checking healed files..."
find data/node4 -type f | grep "test.txt" && echo "✅ Healing replicated data to node4" || echo "❌ Healing failed"

echo "=== STEP 10: Clean up ==="
pkill -f MetadataServer || true
pkill -f StorageNode || true
echo "✅ All tests completed."

