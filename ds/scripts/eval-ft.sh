#!/usr/bin/env bash
set -euo pipefail

# ---------- CONFIG ----------
RF=3
META_PORT=7000
ZK="localhost:2181"
HEAL_INITIAL_DELAY=2
HEAL_INTERVAL=3

NODE1_PORT=8001
NODE2_PORT=8002
NODE3_PORT=8003
NODE4_PORT=8004

DATA_DIR_ROOT="data"
LOG_DIR="logs"
JARS_CLIENT='client/target/classes:client/target/*:common/target/classes:common/target/*'
JARS_META='metadata/target/classes:metadata/target/*:common/target/classes:common/target/*'
JARS_STORAGE='storage/target/classes:storage/target/*:common/target/classes:common/target/*'

# Workload: total MiB to write/read; tweak as needed
WORKLOAD_SIZE_MIB=64
WORKLOAD_NS="demo"    # namespace/prefix used by client tools
RESULTS_TSV="${LOG_DIR}/eval-results.tsv"

# PerfRunner command (adjust if your CLI differs)
PERFRUNNER_CMD=(java -cp "${JARS_CLIENT}" com.ds.client.PerfRunner "${WORKLOAD_SIZE_MIB}" "${WORKLOAD_NS}")

# Alternative manual workload (if PerfRunner CLI differs):
#   WORKLOAD_CMD=(bash -c 'for i in $(seq 1 32); do
#     dd if=/dev/urandom of=tmp.$$ bs=1m count=2 status=none
#     java -cp "'"${JARS_CLIENT}"'" com.ds.client.DsClient put tmp.$$ /demo/ft_$i.bin
#     rm -f tmp.$$
#   done')

# ---------- helpers ----------
log() { printf "%s %s\n" "[$(date +%H:%M:%S)]" "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

wait_port() {
  local port=$1; local tries=50
  for _ in $(seq 1 $tries); do
    if lsof -i :"$port" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  return 1
}

start_metadata() {
  log "Starting MetadataServer :${META_PORT}"
  mkdir -p "${LOG_DIR}"
  nohup java -cp "${JARS_META}" \
    com.ds.metadata.MetadataServer \
      --port "${META_PORT}" \
      --zk "${ZK}" \
      --replication "${RF}" \
      --heal-initial-delay "${HEAL_INITIAL_DELAY}" \
      --heal-interval "${HEAL_INTERVAL}" \
      > "${LOG_DIR}/meta_eval.log" 2>&1 &
  sleep 0.5
  wait_port "${META_PORT}" || die "MetadataServer not listening on ${META_PORT}"
}

start_storage() {
  local port=$1 data=$2 zone=$3
  log "Starting StorageNode :${port} data=${data} zone=${zone}"
  mkdir -p "${data}"
  nohup java -cp "${JARS_STORAGE}" \
    com.ds.storage.StorageNode \
      --port "${port}" \
      --data "${data}" \
      --zone "${zone}" \
      --zk "${ZK}" \
      > "${LOG_DIR}/storage_${port}.log" 2>&1 &
  sleep 0.5
  wait_port "${port}" || die "StorageNode ${port} not listening"
}

stop_storage() {
  local port=$1
  log "Stopping StorageNode :${port}"
  pkill -f "com.ds.storage.StorageNode --port ${port}" || true
}

stamp_rows_since() {
  # prints rows in file newer than a given timestamp (epoch ms); safe if file missing
  local file=$1 since_ms=$2
  [[ -f "$file" ]] || return 0
  awk -F, -v S="$since_ms" 'NR==1{next} {print $0}' "$file" \
    | awk -F, -v S="$since_ms" '{
         # assume col1 is millis since epoch; adjust if your CSV differs
         t=$1; gsub(/[^0-9]/,"",t); if(t>S) print $0
       }'
}

measure_workload() {
  local label=$1
  local start_ms
  start_ms=$(python3 - <<'PY'
import time
print(int(time.time()*1000))
PY
)
  log "Running workload: ${label} (${WORKLOAD_SIZE_MIB} MiB) ..."
  local start_s end_s
  start_s=$(date +%s)
  # Run PerfRunner (or switch to WORKLOAD_CMD if needed)
  "${PERFRUNNER_CMD[@]}" || true
  end_s=$(date +%s)

  local elapsed=$(( end_s - start_s ))
  if (( elapsed == 0 )); then elapsed=1; fi
  local mib=${WORKLOAD_SIZE_MIB}
  local thr=$(( mib * 1 / elapsed ))
  # floating point MiB/s using bc if available
  local thrf
  if command -v bc >/dev/null; then
    thrf=$(echo "scale=2; ${mib}/${elapsed}" | bc)
  else
    thrf="${thr}"
  fi

  # healing backlog snapshot (last value)
  local backlog_csv="metrics/replication_backlog.csv"
  local backlog="NA"
  if [[ -f "${backlog_csv}" ]]; then
    backlog=$(tail -n 1 "${backlog_csv}" | awk -F, 'NF>1{print $2}')
  fi

  echo -e "${label}\t${elapsed}\t${mib}\t${thrf}\t${backlog}" >> "${RESULTS_TSV}"

  # Also stash perf_runs since start_ms (if generated)
  local perf_csv="metrics/perf_runs.csv"
  if [[ -f "${perf_csv}" ]]; then
    mkdir -p "${LOG_DIR}/eval-details"
    stamp_rows_since "${perf_csv}" "${start_ms}" \
      > "${LOG_DIR}/eval-details/perf_${label// /_}.csv" || true
  fi

  log "Done: ${label}  elapsed=${elapsed}s  throughput≈${thrf} MiB/s  backlog=${backlog}"
}

# ---------- main ----------
main() {
  command -v mvn >/dev/null || die "mvn not found"
  [[ -d ".git" || -d "client" ]] || die "Run from project root (where pom.xml lives)"

  log "Build project"
  mvn -q -DskipTests clean package

  mkdir -p "${LOG_DIR}" "${DATA_DIR_ROOT}/node1" "${DATA_DIR_ROOT}/node2" "${DATA_DIR_ROOT}/node3" "${DATA_DIR_ROOT}/node4"

  log "Start cluster (metadata + 3 storage)"
  start_metadata
  start_storage "${NODE1_PORT}" "${DATA_DIR_ROOT}/node1" "z1"
  start_storage "${NODE2_PORT}" "${DATA_DIR_ROOT}/node2" "z2"
  start_storage "${NODE3_PORT}" "${DATA_DIR_ROOT}/node3" "z3"

  # seed a file so healing has something to do later
  echo "Verify self-healing replication" > test.txt
  java -cp "${JARS_CLIENT}" com.ds.client.DsClient put test.txt /demo/test.txt || true

  printf "scenario\tseconds\tMiB\tMiB_per_s\trepl_backlog\n" > "${RESULTS_TSV}"

  # 1) Baseline (all nodes healthy)
  measure_workload "baseline_all_healthy"

  # 2) One node down (no healing yet)
  stop_storage "${NODE2_PORT}"
  sleep 1
  measure_workload "one_down_no_healing"

  # 3) Healing active (bring a fresh empty node)
  start_storage "${NODE4_PORT}" "${DATA_DIR_ROOT}/node4" "z4"
  log "Give healer a moment to notice under-replication..."
  sleep 10
  measure_workload "during_healing"

  log "Final status:"
  column -t -s $'\t' "${RESULTS_TSV}" || cat "${RESULTS_TSV}"

  log "Hints:"
  echo " - Check healing logs: rg \"Under-replicated|Scheduling replication|Replication result\" logs/meta_eval.log"
  echo " - Check node4 received blocks: find ${DATA_DIR_ROOT}/node4 -maxdepth 3 -type f | head"
  echo " - Detailed perf slices (if PerfRunner emits CSV): ls ${LOG_DIR}/eval-details/"
}

main "$@"
