#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/remote_env.sh"

VAR_ROOT="${MILVUS_RS_VAR_ROOT:-${MILVUS_RS_INTEG_ROOT:-/data/work/milvus-rs-integ}/milvus-var}"
LOG_PATH="${MILVUS_RS_STANDALONE_LOG:-${VAR_ROOT}/logs/standalone-stage1.log}"
WAIT_SECONDS="${MILVUS_RS_STANDALONE_WAIT_SECONDS:-90}"

mkdir -p \
    "${VAR_ROOT}/logs" \
    "${VAR_ROOT}/data" \
    "${VAR_ROOT}/etcd" \
    "${VAR_ROOT}/rdb_data" \
    "${VAR_ROOT}/analyzer" \
    "${VAR_ROOT}/pprof"

pkill -f "bin/milvus run standalone" || true
sleep 2
rm -f "${LOG_PATH}"

export ETCD_USE_EMBED="${ETCD_USE_EMBED:-true}"
unset ETCD_CONFIG_PATH || true
unset MILVUS_CONF_ETCD_CONFIG_PATH || true
export COMMON_STORAGETYPE="${COMMON_STORAGETYPE:-local}"
export DEPLOY_MODE="${DEPLOY_MODE:-STANDALONE}"
export MILVUSCONF="${MILVUSCONF:-${ROOT_DIR}/configs}"

nohup "${ROOT_DIR}/bin/milvus" run standalone >"${LOG_PATH}" 2>&1 &
PID=$!
echo "PID=${PID}"
echo "LOG=${LOG_PATH}"

for _ in $(seq 1 "${WAIT_SECONDS}"); do
    if ! kill -0 "${PID}" 2>/dev/null; then
        echo "PROCESS_EXITED"
        tail -n 120 "${LOG_PATH}" || true
        exit 1
    fi
    if curl -fsS http://127.0.0.1:9091/healthz >/dev/null 2>&1; then
        echo "HEALTHY"
        exit 0
    fi
    sleep 1
done

echo "HEALTH_TIMEOUT"
tail -n 120 "${LOG_PATH}" || true
exit 1
