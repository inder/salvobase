#!/usr/bin/env bash
# run_ycsb.sh — Load and run a go-ycsb workload against a target, emit raw stdout.
#
# Environment variables:
#   TARGET_URL      MongoDB URI to bench (default: mongodb://localhost:27017)
#   WORKLOAD        YCSB workload letter A-F (default: A)
#   THREADS         Concurrent client threads (default: 4)
#   RECORD_COUNT    Number of records to load (default: 100000)
#   OPERATION_COUNT Number of operations to execute (default: 100000)
#
# NOTE on go-ycsb versioning: pingcap/go-ycsb does not publish proper semantic
# version tags (v1.x.y), so we use @latest here. Pin this to a specific commit
# SHA once the upstream project adopts proper release tags, e.g.:
#   go install github.com/pingcap/go-ycsb/cmd/go-ycsb@<commit-sha>

set -euo pipefail

TARGET_URL="${TARGET_URL:-mongodb://localhost:27017}"
WORKLOAD="${WORKLOAD:-A}"
THREADS="${THREADS:-4}"
RECORD_COUNT="${RECORD_COUNT:-10000}"
OPERATION_COUNT="${OPERATION_COUNT:-10000}"

DB_NAME="ycsb_bench_$(date +%s)"

# Ensure go-ycsb is installed. If it's already on PATH, skip.
if ! command -v go-ycsb &>/dev/null; then
  echo "[run_ycsb] Installing go-ycsb@latest ..." >&2
  go install github.com/pingcap/go-ycsb/cmd/go-ycsb@latest >&2
fi

WORKLOAD_LOWER=$(echo "$WORKLOAD" | tr '[:upper:]' '[:lower:]')
WORKLOAD_FILE="workloads/workload${WORKLOAD_LOWER}"

# Build the workload path relative to go-ycsb source, falling back to a temp file
# if the workloads directory isn't available (i.e., running from $GOPATH/bin only).
GOPATH_BIN="$(go env GOPATH)/bin"
GOPATH_PKG="$(go env GOPATH)/pkg/mod/github.com/pingcap/go-ycsb@$(go list -m -json github.com/pingcap/go-ycsb 2>/dev/null | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("Version",""))' 2>/dev/null || true)"

if [ -d "$GOPATH_PKG/workloads" ]; then
  WORKLOAD_PATH="$GOPATH_PKG/${WORKLOAD_FILE}"
else
  # Generate a minimal workload properties file inline
  WORKLOAD_PATH="/tmp/ycsb_workload_${WORKLOAD}.properties"
  cat > "$WORKLOAD_PATH" <<PROPS
workload=core
readallfields=true
fieldcount=10
fieldlength=100
readproportion=$(python3 -c "
props = {
  'A': ('0.5','0.5','0','0','0','0'),
  'B': ('0.95','0.05','0','0','0','0'),
  'C': ('1.0','0','0','0','0','0'),
  'D': ('0.95','0','0','0.05','0','0'),
  'E': ('0','0','0','0','0.95','0.05'),
  'F': ('0.5','0','0.5','0','0','0'),
}
r,u,rmw,i,s,d = props.get('${WORKLOAD}', ('0.5','0.5','0','0','0','0'))
print(r)" 2>/dev/null || echo "0.5")
updateproportion=$(python3 -c "
props = {
  'A': ('0.5','0.5','0','0','0','0'),
  'B': ('0.95','0.05','0','0','0','0'),
  'C': ('1.0','0','0','0','0','0'),
  'D': ('0.95','0','0','0.05','0','0'),
  'E': ('0','0','0','0','0.95','0.05'),
  'F': ('0.5','0','0.5','0','0','0'),
}
r,u,rmw,i,s,d = props.get('${WORKLOAD}', ('0.5','0.5','0','0','0','0'))
print(u)" 2>/dev/null || echo "0.5")
scanproportion=0
insertproportion=0
requestdistribution=zipfian
PROPS
fi

echo "[run_ycsb] Loading ${RECORD_COUNT} records into ${TARGET_URL}/${DB_NAME} ..." >&2
go-ycsb load mongodb -P "$WORKLOAD_PATH" \
  -p mongodb.url="${TARGET_URL}" \
  -p mongodb.namespace="${DB_NAME}.usertable" \
  -p recordcount="${RECORD_COUNT}" \
  -p threadcount="${THREADS}" \
  2>&1 | grep -v "^$" >&2 || true

echo "[run_ycsb] Running workload ${WORKLOAD} with ${THREADS} threads ..." >&2
go-ycsb run mongodb -P "$WORKLOAD_PATH" \
  -p mongodb.url="${TARGET_URL}" \
  -p mongodb.namespace="${DB_NAME}.usertable" \
  -p recordcount="${RECORD_COUNT}" \
  -p operationcount="${OPERATION_COUNT}" \
  -p threadcount="${THREADS}"
