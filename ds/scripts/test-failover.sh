#!/usr/bin/env bash
# ============================================================================
#  test-failover.sh — Verify metadata leader failover and fencing epochs
# ============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOGDIR="logs/failover"
DATADIR="data/failover"
mkdir -p "$LOGDIR" "$DATADIR"/node{1..2}

ZK_ADDR="${ZK_ADDR:-localhost:2181}"
META_PORTS=(7000 7001)
STORAGE_PORTS=(8001 8002)

declare -A META_PIDS=()
declare -a STORAGE_PIDS=()

TMP_JAVA="$(mktemp "${TMPDIR:-/tmp}/leader-infoXXXX.java")"
cat <<'EOF' > "$TMP_JAVA"
import com.ds.client.LeaderDiscovery;

public class LeaderInfoPrinter {
  public static void main(String[] args) throws Exception {
    String zk = args.length > 0 ? args[0] : "localhost:2181";
    try (LeaderDiscovery ld = new LeaderDiscovery(zk)) {
      System.out.println(ld.hostPort() + "|" + ld.epoch());
    }
  }
}
EOF

cleanup() {
  for pid in "${META_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${STORAGE_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  rm -f "$TMP_JAVA"
}
trap cleanup EXIT

ensure_zk() {
  if nc -z "$(echo "$ZK_ADDR" | cut -d: -f1)" "$(echo "$ZK_ADDR" | cut -d: -f2)" >/dev/null 2>&1; then
    return 0
  fi
  echo "ZooKeeper not reachable at $ZK_ADDR. Please start it before running this script." >&2
  exit 1
}

leader_info() {
  java --class-path "client/target/*:common/target/*" "$TMP_JAVA" "$ZK_ADDR"
}

start_metadata() {
  local port=$1
  local log="$LOGDIR/meta_${port}.log"
  java -cp "metadata/target/*:common/target/*:time/target/*" \
    com.ds.metadata.MetadataServer \
    --port "$port" \
    --zk "$ZK_ADDR" \
    >"$log" 2>&1 &
  META_PIDS["$port"]=$!
}

start_storage() {
  local port=$1
  local idx=$2
  local log="$LOGDIR/storage_${port}.log"
  java -cp "storage/target/*:common/target/*" \
    com.ds.storage.StorageNode \
    --port "$port" \
    --data "$DATADIR/node$idx" \
    --zk "$ZK_ADDR" \
    >"$log" 2>&1 &
  STORAGE_PIDS+=("$!")
}

wait_for_new_leader() {
  local previous_hp=$1
  local previous_epoch=$2
  for _ in {1..20}; do
    sleep 2
    local info
    if ! info="$(leader_info 2>/dev/null)"; then
      continue
    fi
    local hp="${info%%|*}"
    local epoch="${info##*|}"
    if [[ "$hp" != "$previous_hp" || "$epoch" -gt "$previous_epoch" ]]; then
      echo "$info"
      return 0
    fi
  done
  echo "Timed out waiting for new leader" >&2
  return 1
}

echo "== Build modules =="
mvn -q -DskipTests package

ensure_zk

echo "== Launch metadata servers =="
for port in "${META_PORTS[@]}"; do
  start_metadata "$port"
done

echo "== Launch storage nodes =="
idx=1
for port in "${STORAGE_PORTS[@]}"; do
  start_storage "$port" "$idx"
  idx=$((idx + 1))
done

sleep 6

info_before="$(leader_info)"
leader_hp_before="${info_before%%|*}"
epoch_before="${info_before##*|}"
echo "Leader before failover: $leader_hp_before (epoch $epoch_before)"

leader_port="${leader_hp_before##*:}"
follower_port="${META_PORTS[0]}"
if [[ "$follower_port" == "$leader_port" ]]; then
  follower_port="${META_PORTS[1]}"
fi

echo "== Kill leader on port $leader_port =="
leader_pid="${META_PIDS[$leader_port]}"
if [[ -n "${leader_pid:-}" ]]; then
  kill "$leader_pid"
  wait "$leader_pid" >/dev/null 2>&1 || true
  unset 'META_PIDS[$leader_port]'
else
  echo "No PID recorded for leader port $leader_port" >&2
fi

info_after="$(wait_for_new_leader "$leader_hp_before" "$epoch_before")"
leader_hp_after="${info_after%%|*}"
epoch_after="${info_after##*|}"
echo "Leader after failover: $leader_hp_after (epoch $epoch_after)"

echo "== Verify client write on new leader =="
echo "failover-check" > "$DATADIR/failover.txt"
java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient put "$DATADIR/failover.txt" /failover/test.txt
java -cp "client/target/*:common/target/*" \
  com.ds.client.DsClient get /failover/test.txt "$DATADIR/failover.out"
if diff -q "$DATADIR/failover.txt" "$DATADIR/failover.out" >/dev/null; then
  echo "Write/read succeeded on new leader."
else
  echo "Mismatch reading data after failover!" >&2
  exit 1
fi

echo "Done. Logs under $LOGDIR"
