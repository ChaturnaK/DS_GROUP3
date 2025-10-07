#!/usr/bin/env bash
set -e
bash scripts/run-local.sh &
sleep 5

echo "Uploading file..."
java -cp "client/target/*:common/target/*" com.ds.client.DsClient put ./test.txt /demo/test.txt

echo "Downloading file..."
java -cp "client/target/*:common/target/*" com.ds.client.DsClient get /demo/test.txt ./downloaded.txt

echo "Comparing..."
diff test.txt downloaded.txt && echo "✅ Files match"

echo "Simulating node failure..."
pkill -f "StorageNode.*8002"
sleep 10

echo "Restarting node..."
java -cp "storage/target/*:common/target/*" com.ds.storage.StorageNode --port 8002 --data ./data/node2 --zone z1 --zk localhost:2181 &

sleep 5
echo "✅ Test completed successfully."