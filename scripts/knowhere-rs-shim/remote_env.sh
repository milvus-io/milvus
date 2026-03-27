#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

INTEG_ROOT="${MILVUS_RS_INTEG_ROOT:-/data/work/milvus-rs-integ}"
KNOWHERE_RS_ROOT_DEFAULT="${MILVUS_RS_KNOWHERE_RS_ROOT:-/data/work/knowhere-rs}"

export PATH="${INTEG_ROOT}/bin:/root/.cargo/bin:${PATH}"
export HOME="${INTEG_ROOT}/home"
export TMPDIR="${INTEG_ROOT}/tmp"
export KNOWHERE_RS_LIB_DIR="${KNOWHERE_RS_LIB_DIR:-${INTEG_ROOT}/knowhere-rs-target/release}"

mkdir -p "${HOME}" "${TMPDIR}"

if [[ -f /root/.cargo/env ]]; then
    # shellcheck disable=SC1091
    source /root/.cargo/env
fi

shim_cmake_args="-DMILVUS_USE_KNOWHERE_RS_SHIM=ON -DKNOWHERE_RS_ROOT=${KNOWHERE_RS_ROOT_DEFAULT}"
if [[ -n "${CMAKE_EXTRA_ARGS:-}" ]]; then
    export CMAKE_EXTRA_ARGS="${shim_cmake_args} ${CMAKE_EXTRA_ARGS}"
else
    export CMAKE_EXTRA_ARGS="${shim_cmake_args}"
fi

# GCC 13 on the reference host needs a fixed-width integer header visible
# before RocksDB's hash-index header is parsed.
if command -v gcc >/dev/null 2>&1; then
    gcc_major="$(gcc -dumpversion | cut -d. -f1)"
    if [[ "${gcc_major}" =~ ^[0-9]+$ ]] && (( gcc_major >= 13 )); then
        export MILVUS_EXTRA_CXXFLAGS="${MILVUS_EXTRA_CXXFLAGS:-} -include cstdint"
    fi
fi

export MILVUS_RS_REMOTE_ROOT="${ROOT_DIR}"
