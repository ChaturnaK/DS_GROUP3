#!/usr/bin/env bash
set -euo pipefail
# If you have zk installed via brew or tarball, start it; otherwise print a hint.
if command -v zkServer >/dev/null 2>&1; then
  zkServer start
else
  echo "Please start ZooKeeper on localhost:2181 (e.g., 'brew install zookeeper && zkServer start')."
fi
