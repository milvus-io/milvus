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

: "${CATALOG_ADDR:=127.0.0.1:19540}"
: "${METASTORE_TYPE:=tikv}"
: "${ETCD_ENDPOINTS:=127.0.0.1:2379}"
: "${TIKV_PD:=127.0.0.1:2389}"
: "${CATALOG_ROOT_PREFIX:=by-dev/catalog-demo}"
: "${CATALOG_JOB_PREFIX:=by-dev/catalog-demo-jobs}"
: "${NAMESPACE_META_SUBPATH:=meta}"
: "${SRC_NAMESPACE:=milvus1}"
: "${DST_NAMESPACE:=milvus2}"
: "${SRC_PROXY_PORT:=19530}"
: "${DST_PROXY_PORT:=19630}"
: "${SRC_ROOTCOORD_PORT:=22125}"
: "${DST_ROOTCOORD_PORT:=22225}"
: "${SRC_ROOTCOORD_ADDR:=127.0.0.1:${SRC_ROOTCOORD_PORT}}"
: "${DST_ROOTCOORD_ADDR:=127.0.0.1:${DST_ROOTCOORD_PORT}}"
: "${DB_NAME:=default}"
: "${COLLECTION_NAME:=catalog_transfer_demo}"
: "${ALIAS_NAME:=catalog_transfer_demo_alias}"
: "${TRANSFER_ID:=demo-$(date +%s)}"
: "${DRAIN_TIMEOUT_MS:=30000}"
: "${RUN_DIR:=/tmp/milvus-catalog-transfer-demo}"
: "${START_MILVUS:=1}"
: "${RUN_CLIENT_CHECKS:=1}"
: "${MILVUS_BIN:=${ROOT_DIR}/bin/milvus}"
: "${MILVUS_READY_TIMEOUT:=180}"

SRC_URI="http://127.0.0.1:${SRC_PROXY_PORT}"
DST_URI="http://127.0.0.1:${DST_PROXY_PORT}"
WRITER_RUN_FILE="${RUN_DIR}/writer.run"
WRITER_READY_FILE="${RUN_DIR}/writer.ready"
WRITER_ERROR_FILE="${RUN_DIR}/writer.error"

cleanup() {
  rm -f "${WRITER_RUN_FILE}" >/dev/null 2>&1 || true
  if [[ -n "${WRITER_PID:-}" ]]; then
    wait "${WRITER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${CATALOG_PID:-}" ]]; then
    kill "${CATALOG_PID}" >/dev/null 2>&1 || true
    wait "${CATALOG_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${START_MILVUS}" == "1" ]]; then
    for pid in ${MILVUS_PIDS:-}; do
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
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

start_milvus() {
  local name="$1"
  local namespace="$2"
  local proxy_port="$3"
  local rootcoord_port="$4"
  local port_base="$5"
  local namespace_root="${CATALOG_ROOT_PREFIX}/${namespace}"
  local instance_dir="${RUN_DIR}/${name}"

  mkdir -p "${instance_dir}/local" "${instance_dir}/rdb" "${instance_dir}/logs"
  echo "Starting ${name} namespace=${namespace} proxy=${proxy_port} rootcoord=${rootcoord_port} metastore=tikv root=${namespace_root}/${NAMESPACE_META_SUBPATH}"
  (
    cd "${ROOT_DIR}"
    env \
      MILVUS_CONF_METASTORE_TYPE=tikv \
      MILVUS_CONF_TIKV_ENDPOINTS="${TIKV_PD}" \
      MILVUS_CONF_TIKV_ROOTPATH="${namespace_root}" \
      MILVUS_CONF_TIKV_METASUBPATH="${NAMESPACE_META_SUBPATH}" \
      MILVUS_CONF_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
      MILVUS_CONF_ETCD_ROOTPATH="${namespace_root}/discovery" \
      MILVUS_CONF_COMMON_STORAGETYPE=local \
      MILVUS_CONF_LOCALSTORAGE_PATH="${instance_dir}/local" \
      MILVUS_CONF_ROCKSMQ_PATH="${instance_dir}/rdb" \
      MILVUS_CONF_MSGCHANNEL_CHANNAMEPREFIX_CLUSTER="${namespace}" \
      MILVUS_CONF_PROXY_PORT="${proxy_port}" \
      MILVUS_CONF_PROXY_INTERNALPORT="$((proxy_port - 1))" \
      MILVUS_CONF_ROOTCOORD_PORT="${rootcoord_port}" \
      MILVUS_CONF_QUERYCOORD_PORT="$((port_base + 1))" \
      MILVUS_CONF_QUERYNODE_PORT="$((port_base + 2))" \
      MILVUS_CONF_DATACOORD_PORT="$((port_base + 3))" \
      MILVUS_CONF_DATANODE_PORT="$((port_base + 4))" \
      MILVUS_CONF_STREAMINGNODE_PORT="$((port_base + 5))" \
      MILVUS_CONF_LOG_FILE_ROOTPATH="${instance_dir}/logs" \
      "${MILVUS_BIN}" run standalone --run-with-subprocess
  ) >"${instance_dir}/milvus.log" 2>&1 &
  MILVUS_PIDS="${MILVUS_PIDS:-} $!"
}

require_cmd go
require_cmd python3
mkdir -p "${RUN_DIR}"

if [[ "${START_MILVUS}" == "1" ]]; then
  if [[ ! -x "${MILVUS_BIN}" ]]; then
    echo "Milvus binary not found or not executable: ${MILVUS_BIN}" >&2
    echo "Build Milvus first, or set START_MILVUS=0 and provide SRC/DST proxy/rootcoord addresses." >&2
    exit 1
  fi
  start_milvus "milvus1" "${SRC_NAMESPACE}" "${SRC_PROXY_PORT}" "${SRC_ROOTCOORD_PORT}" 23100
  start_milvus "milvus2" "${DST_NAMESPACE}" "${DST_PROXY_PORT}" "${DST_ROOTCOORD_PORT}" 24100
fi

if [[ "${RUN_CLIENT_CHECKS}" == "1" ]]; then
  python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" wait --uri "${SRC_URI}" --alias src --timeout "${MILVUS_READY_TIMEOUT}"
  python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" wait --uri "${DST_URI}" --alias dst --timeout "${MILVUS_READY_TIMEOUT}"
  python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" seed \
    --source-uri "${SRC_URI}" \
    --target-uri "${DST_URI}" \
    --db-name "${DB_NAME}" \
    --collection "${COLLECTION_NAME}" \
    --alias-name "${ALIAS_NAME}"

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
fi

echo "Starting Catalog Service on ${CATALOG_ADDR}"
(
  cd "${ROOT_DIR}"
  go run ./cmd/catalogservice \
    --listen "${CATALOG_ADDR}" \
    --metastore "${METASTORE_TYPE}" \
    --etcd "${ETCD_ENDPOINTS}" \
    --tikv-pd "${TIKV_PD}" \
    --root-prefix "${CATALOG_ROOT_PREFIX}" \
    --namespace-meta-subpath "${NAMESPACE_META_SUBPATH}" \
    --job-prefix "${CATALOG_JOB_PREFIX}" \
    --rootcoord-routes "${SRC_NAMESPACE}=${SRC_ROOTCOORD_ADDR},${DST_NAMESPACE}=${DST_ROOTCOORD_ADDR}"
) &
CATALOG_PID=$!

sleep 3

echo "Starting collection transfer ${TRANSFER_ID}: ${SRC_NAMESPACE}/${DB_NAME}/${COLLECTION_NAME} -> ${DST_NAMESPACE}"
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

if [[ "${RUN_CLIENT_CHECKS}" == "1" ]]; then
  wait_file "${WRITER_ERROR_FILE}" 30 || {
    echo "writer did not observe source write rejection after transfer" >&2
    exit 1
  }
  rm -f "${WRITER_RUN_FILE}"
  wait "${WRITER_PID}"
  python3 "${ROOT_DIR}/scripts/catalog_transfer_e2e.py" verify \
    --source-uri "${SRC_URI}" \
    --target-uri "${DST_URI}" \
    --db-name "${DB_NAME}" \
    --collection "${COLLECTION_NAME}" \
    --alias-name "${ALIAS_NAME}"
fi

echo
echo "Transfer finished through Catalog Service backed by TiKV."
echo "  source ${SRC_NAMESPACE}: public describe/insert is rejected after RootCoord drain + cache invalidation"
echo "  target ${DST_NAMESPACE}: public describe and alias resolution hit transferred RootCoord live metadata/cache"
