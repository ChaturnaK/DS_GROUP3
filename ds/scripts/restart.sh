#!/usr/bin/env bash
set -euo pipefail

echo "Restarting cluster…"
pkill -f com.ds || true
sleep 2
bash scripts/run-local.sh
