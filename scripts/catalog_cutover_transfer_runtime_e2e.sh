#!/usr/bin/env bash
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${RUN_ID:=milvus-catalog-runtime-e2e}"
: "${RUN_DIR:=/tmp/${RUN_ID}}"
: "${BUILD_MILVUS:=0}"
: "${START_INFRA:=1}"
: "${ETCD_PORT:=23790}"
: "${PD_PORT:=23890}"
: "${PD_PEER_PORT:=23891}"
: "${TIKV_PORT:=20161}"
: "${ETCD_ENDPOINTS:=127.0.0.1:${ETCD_PORT}}"
: "${TIKV_PD:=127.0.0.1:${PD_PORT}}"
: "${CATALOG_ADDR:=127.0.0.1:19540}"
: "${CATALOG_ROOT_PREFIX:=by-dev/catalog-runtime-e2e}"
: "${CATALOG_JOB_PREFIX:=by-dev/catalog-runtime-e2e-jobs}"
: "${NAMESPACE_META_SUBPATH:=meta}"
: "${SRC_NAMESPACE:=milvus1}"
: "${DST_NAMESPACE:=milvus2}"
: "${SRC_PROXY_PORT:=19530}"
: "${DST_PROXY_PORT:=19630}"
: "${SRC_ROOTCOORD_PORT:=22125}"
: "${DST_ROOTCOORD_PORT:=22225}"
: "${SRC_METRICS_PORT:=29191}"
: "${DST_METRICS_PORT:=29192}"
: "${SRC_ROOTCOORD_ADDR:=127.0.0.1:${SRC_ROOTCOORD_PORT}}"
: "${DST_ROOTCOORD_ADDR:=127.0.0.1:${DST_ROOTCOORD_PORT}}"
: "${DB_NAME:=catalog_runtime_e2e_db}"
: "${COLLECTION_NAME:=catalog_cutover_transfer_runtime_e2e}"
: "${ALIAS_NAME:=catalog_cutover_transfer_runtime_e2e_alias}"
: "${TRANSFER_ID:=runtime-e2e-$(date +%s)}"
: "${DRAIN_TIMEOUT_MS:=30000}"
: "${MILVUS_BIN:=${ROOT_DIR}/bin/milvus}"
: "${MILVUS_READY_TIMEOUT:=240}"
: "${CATALOG_READY_TIMEOUT:=90}"
: "${INFRA_READY_TIMEOUT:=90}"
: "${KEEP_RUNTIME:=0}"
: "${DUMP_ETCD_KEYS:=0}"

SRC_URI="http://127.0.0.1:${SRC_PROXY_PORT}"
DST_URI="http://127.0.0.1:${DST_PROXY_PORT}"
WRITER_RUN_FILE="${RUN_DIR}/writer.run"
WRITER_READY_FILE="${RUN_DIR}/writer.ready"
WRITER_ERROR_FILE="${RUN_DIR}/writer.error"
ETCD_CONTAINER="${RUN_ID}-etcd"
PD_CONTAINER="${RUN_ID}-pd"
TIKV_CONTAINER="${RUN_ID}-tikv"

cleanup() {
  if [[ "${KEEP_RUNTIME}" == "1" ]]; then
    echo "KEEP_RUNTIME=1, leaving runtime processes and containers alive under ${RUN_DIR}"
    return
  fi
  rm -f "${WRITER_RUN_FILE}" >/dev/null 2>&1 || true
  if [[ -n "${WRITER_PID:-}" ]]; then
    wait "${WRITER_PID}" >/dev/null 2>&1 || true
  fi
  for pid in ${SRC_PID:-} ${DST_PID:-} ${CATALOG_PID:-}; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    fi
  done
  if [[ "${START_INFRA}" == "1" ]]; then
    docker rm -f "${TIKV_CONTAINER}" "${PD_CONTAINER}" "${ETCD_CONTAINER}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

wait_tcp() {
  local host="$1"
  local port="$2"
  local timeout="$3"
  local deadline=$((SECONDS + timeout))
  while [[ ${SECONDS} -lt ${deadline} ]]; do
    if bash -c ">/dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${host}:${port}" >&2
  return 1
}

require_port_free() {
  local port="$1"
  local label="$2"
  if bash -c ">/dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1; then
    echo "port ${port} for ${label} is already in use; choose a different port via environment variables" >&2
    exit 1
  fi
}

wait_file() {
  local file="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while [[ ! -f "${file}" && ${SECONDS} -lt ${deadline} ]]; do
    sleep 1
  done
  [[ -f "${file}" ]]
}

dump_etcd_keys() {
  local prefix="$1"
  if [[ "${DUMP_ETCD_KEYS}" != "1" ]]; then
    return
  fi
  echo "Dumping etcd keys under ${prefix}"
  docker exec "${ETCD_CONTAINER}" etcdctl \
    --endpoints="http://127.0.0.1:${ETCD_PORT}" \
    get --prefix "${prefix}" --keys-only || true
}

start_infra() {
  docker run -d --name "${ETCD_CONTAINER}" --network host quay.io/coreos/etcd:v3.5.25 \
    etcd \
    --advertise-client-urls=http://127.0.0.1:${ETCD_PORT} \
    --listen-client-urls=http://0.0.0.0:${ETCD_PORT} \
    --data-dir=/etcd >/dev/null

  docker run -d --name "${PD_CONTAINER}" --network host pingcap/pd:v8.5.2 \
    --name=pd \
    --data-dir=/data/pd \
    --client-urls=http://0.0.0.0:${PD_PORT} \
    --advertise-client-urls=http://127.0.0.1:${PD_PORT} \
    --peer-urls=http://0.0.0.0:${PD_PEER_PORT} \
    --advertise-peer-urls=http://127.0.0.1:${PD_PEER_PORT} \
    --initial-cluster=pd=http://127.0.0.1:${PD_PEER_PORT} >/dev/null

  docker run -d --name "${TIKV_CONTAINER}" --network host pingcap/tikv:v8.5.2 \
    --addr=0.0.0.0:${TIKV_PORT} \
    --advertise-addr=127.0.0.1:${TIKV_PORT} \
    --pd=127.0.0.1:${PD_PORT} \
    --data-dir=/data/tikv >/dev/null

  wait_tcp 127.0.0.1 "${ETCD_PORT}" "${INFRA_READY_TIMEOUT}"
  wait_tcp 127.0.0.1 "${PD_PORT}" "${INFRA_READY_TIMEOUT}"
  wait_tcp 127.0.0.1 "${TIKV_PORT}" "${INFRA_READY_TIMEOUT}"
}

start_catalog_service() {
  echo "Starting Catalog Service ${CATALOG_ADDR} backend=TiKV ${TIKV_PD}"
  (
    cd "${ROOT_DIR}"
    go run ./cmd/catalogservice \
      --listen "${CATALOG_ADDR}" \
      --metastore tikv \
      --etcd "${ETCD_ENDPOINTS}" \
      --tikv-pd "${TIKV_PD}" \
      --root-prefix "${CATALOG_ROOT_PREFIX}" \
      --namespace-meta-subpath "${NAMESPACE_META_SUBPATH}" \
      --job-prefix "${CATALOG_JOB_PREFIX}" \
      --rootcoord-routes "${SRC_NAMESPACE}=${SRC_ROOTCOORD_ADDR},${DST_NAMESPACE}=${DST_ROOTCOORD_ADDR}"
  ) >"${RUN_DIR}/catalogservice.log" 2>&1 &
  CATALOG_PID=$!
  wait_tcp 127.0.0.1 "${CATALOG_ADDR##*:}" "${CATALOG_READY_TIMEOUT}"
}

start_milvus() {
  local name="$1"
  local namespace="$2"
  local metastore_type="$3"
  local proxy_port="$4"
  local rootcoord_port="$5"
  local port_base="$6"
  local metrics_port="$7"
  local instance_dir="${RUN_DIR}/${name}"
  local namespace_root="${CATALOG_ROOT_PREFIX}/${namespace}"
  local etcd_root="${namespace_root}"
  if [[ "${metastore_type}" == "catalogservice" ]]; then
    etcd_root="${namespace_root}/discovery"
  fi

  mkdir -p "${instance_dir}/local" "${instance_dir}/rdb" "${instance_dir}/logs"
  echo "Starting ${name} namespace=${namespace} metastore=${metastore_type} proxy=${proxy_port} rootcoord=${rootcoord_port}"
  (
    cd "${ROOT_DIR}"
    env \
      LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-${ROOT_DIR}/internal/core/output/lib:${ROOT_DIR}/cmake_build/lib}" \
      MILVUS_CONF_COMMON_CLUSTERNAME="${namespace}" \
      MILVUS_CONF_METASTORE_TYPE="${metastore_type}" \
      MILVUS_CONF_CATALOGSERVICE_ADDRESS="${CATALOG_ADDR}" \
      MILVUS_CONF_CATALOGSERVICE_NAMESPACE="${namespace}" \
      MILVUS_CONF_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
      MILVUS_CONF_ETCD_AUTH_ENABLED=false \
      MILVUS_CONF_ETCD_ROOTPATH="${etcd_root}" \
      MILVUS_CONF_ETCD_METASUBPATH="${NAMESPACE_META_SUBPATH}" \
      MILVUS_CONF_TIKV_ENDPOINTS="${TIKV_PD}" \
      MILVUS_CONF_TIKV_ROOTPATH="${namespace_root}" \
      MILVUS_CONF_TIKV_METASUBPATH="${NAMESPACE_META_SUBPATH}" \
      MILVUS_CONF_COMMON_STORAGETYPE=local \
      MILVUS_CONF_LOCALSTORAGE_PATH="${instance_dir}/local" \
      MILVUS_CONF_ROCKSMQ_PATH="${instance_dir}/rdb" \
      MILVUS_CONF_MSGCHANNEL_CHANNAMEPREFIX_CLUSTER="${namespace}" \
      MILVUS_CONF_PROXY_PORT="${proxy_port}" \
      MILVUS_CONF_PROXY_INTERNALPORT="$((proxy_port - 1))" \
      MILVUS_CONF_ROOTCOORD_PORT="${rootcoord_port}" \
      MILVUS_CONF_COMMON_METRICSPORT="${metrics_port}" \
      MILVUS_CONF_QUERYCOORD_PORT="$((port_base + 1))" \
      MILVUS_CONF_QUERYNODE_PORT="$((port_base + 2))" \
      MILVUS_CONF_DATACOORD_PORT="$((port_base + 3))" \
      MILVUS_CONF_DATANODE_PORT="$((port_base + 4))" \
      MILVUS_CONF_STREAMINGNODE_PORT="$((port_base + 5))" \
      MILVUS_CONF_LOG_FILE_ROOTPATH="${instance_dir}/logs" \
      "${MILVUS_BIN}" run standalone --run-with-subprocess
  ) >"${instance_dir}/milvus.log" 2>&1 &
  STARTED_PID=$!
}

stop_pid() {
  local name="$1"
  local pid="$2"
  echo "Stopping ${name} pid=${pid}"
  kill "${pid}" >/dev/null 2>&1 || true
  wait "${pid}" >/dev/null 2>&1 || true
  sleep 3
}

require_cmd go
require_cmd python3
require_cmd docker

require_port_free "${CATALOG_ADDR##*:}" "Catalog Service"
require_port_free "${SRC_PROXY_PORT}" "source Milvus proxy"
require_port_free "$((SRC_PROXY_PORT - 1))" "source Milvus proxy internal"
require_port_free "${SRC_ROOTCOORD_PORT}" "source RootCoord"
require_port_free "${SRC_METRICS_PORT}" "source Milvus metrics"
require_port_free "${DST_PROXY_PORT}" "target Milvus proxy"
require_port_free "$((DST_PROXY_PORT - 1))" "target Milvus proxy internal"
require_port_free "${DST_ROOTCOORD_PORT}" "target RootCoord"
require_port_free "${DST_METRICS_PORT}" "target Milvus metrics"
for port in 23101 23102 23103 23104 23105 24101 24102 24103 24104 24105; do
  require_port_free "${port}" "Milvus subprocess"
done
if [[ "${START_INFRA}" == "1" ]]; then
  require_port_free "${ETCD_PORT}" "etcd"
  require_port_free "${PD_PORT}" "PD"
  require_port_free "${PD_PEER_PORT}" "PD peer"
  require_port_free "${TIKV_PORT}" "TiKV"
fi

rm -rf "${RUN_DIR}"
mkdir -p "${RUN_DIR}"

if [[ "${BUILD_MILVUS}" == "1" ]]; then
  (cd "${ROOT_DIR}" && make build-go)
fi

if [[ ! -x "${MILVUS_BIN}" ]]; then
  echo "Milvus binary not found or not executable: ${MILVUS_BIN}" >&2
  echo "Build Milvus first or run with BUILD_MILVUS=1." >&2
  exit 1
fi

if [[ "${START_INFRA}" == "1" ]]; then
  start_infra
fi

start_catalog_service

start_milvus "milvus1-legacy-etcd" "${SRC_NAMESPACE}" "etcd" "${SRC_PROXY_PORT}" "${SRC_ROOTCOORD_PORT}" 23100 "${SRC_METRICS_PORT}"
SRC_PID="${STARTED_PID}"
start_milvus "milvus2-catalogservice" "${DST_NAMESPACE}" "catalogservice" "${DST_PROXY_PORT}" "${DST_ROOTCOORD_PORT}" 24100 "${DST_METRICS_PORT}"
DST_PID="${STARTED_PID}"

python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" wait --uri "${SRC_URI}" --alias src-legacy --timeout "${MILVUS_READY_TIMEOUT}"
python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" wait --uri "${DST_URI}" --alias dst-catalog --timeout "${MILVUS_READY_TIMEOUT}"
python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" seed \
  --source-uri "${SRC_URI}" \
  --target-uri "${DST_URI}" \
  --db-name "${DB_NAME}" \
  --collection "${COLLECTION_NAME}" \
  --alias-name "${ALIAS_NAME}" \
  --skip-target-db

dump_etcd_keys "${CATALOG_ROOT_PREFIX}/${SRC_NAMESPACE}"

echo "Online cutting over ${SRC_NAMESPACE} RootCoord metadata without stopping Milvus1: etcd -> Catalog Service/TiKV"
(
  cd "${ROOT_DIR}"
  go run ./cmd/catalogcutoverctl \
    --rootcoord-address "${SRC_ROOTCOORD_ADDR}" \
    --catalog-address "${CATALOG_ADDR}" \
    --target-namespace "${SRC_NAMESPACE}" \
    --drain-timeout-ms "${DRAIN_TIMEOUT_MS}"
)
python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" wait --uri "${SRC_URI}" --alias src-online-cutover --timeout "${MILVUS_READY_TIMEOUT}"

rm -f "${WRITER_READY_FILE}" "${WRITER_ERROR_FILE}"
touch "${WRITER_RUN_FILE}"
python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" writer \
  --source-uri "${SRC_URI}" \
  --target-uri "${DST_URI}" \
  --db-name "${DB_NAME}" \
  --collection "${COLLECTION_NAME}" \
  --alias-name "${ALIAS_NAME}" \
  --run-file "${WRITER_RUN_FILE}" \
  --ready-file "${WRITER_READY_FILE}" \
  --error-file "${WRITER_ERROR_FILE}" &
WRITER_PID=$!
wait_file "${WRITER_READY_FILE}" 30 || {
  echo "writer did not become ready" >&2
  exit 1
}

echo "Starting live namespace transfer ${TRANSFER_ID}: ${SRC_NAMESPACE}/${DB_NAME}/${COLLECTION_NAME} -> ${DST_NAMESPACE}"
(
  cd "${ROOT_DIR}"
  go run ./cmd/catalogtransferctl \
    --address "${CATALOG_ADDR}" \
    --transfer-id "${TRANSFER_ID}" \
    --source-namespace "${SRC_NAMESPACE}" \
    --target-namespace "${DST_NAMESPACE}" \
    --db "${DB_NAME}" \
    --collection "${COLLECTION_NAME}" \
    --drain-timeout-ms "${DRAIN_TIMEOUT_MS}"
)

wait_file "${WRITER_ERROR_FILE}" 30 || {
  echo "writer did not observe source write rejection after transfer" >&2
  exit 1
}
rm -f "${WRITER_RUN_FILE}"
wait "${WRITER_PID}"
WRITER_PID=""

python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" verify \
  --source-uri "${SRC_URI}" \
  --target-uri "${DST_URI}" \
  --db-name "${DB_NAME}" \
  --collection "${COLLECTION_NAME}" \
  --alias-name "${ALIAS_NAME}"

echo
echo "Runtime E2E passed:"
echo "  1. Milvus1 wrote RootCoord metadata to etcd."
echo "  2. RootCoord online cutover drained metadata writes and switched its live MetaTable to Catalog Service backed by TiKV without stopping Milvus1."
echo "  3. The same Milvus1 process stayed serving after cutover."
echo "  4. Catalog Service live-transferred the collection from ${SRC_NAMESPACE} to ${DST_NAMESPACE} through RootCoord RPC drain/invalidate/apply."
