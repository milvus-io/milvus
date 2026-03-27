#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"
INTEG_ROOT="${MILVUS_RS_INTEG_ROOT:-/data/work/milvus-rs-integ}"
INSTALL_PATH="${MILVUS_RS_BIN_DIR:-${INTEG_ROOT}/bin}"
GO_ROOT="${MILVUS_RS_GO_ROOT:-${INTEG_ROOT}/go}"
GO_VERSION="${MILVUS_RS_GO_VERSION:-1.24.11}"
GO_ARCH="${MILVUS_RS_GO_ARCH:-amd64}"
OPENBLAS_ROOT="${MILVUS_RS_OPENBLAS_ROOT:-${INTEG_ROOT}/openblas}"
OPENBLAS_VERSION="${MILVUS_RS_OPENBLAS_VERSION:-0.3.9}"

mode="${1:-thirdparty}"
if [[ "${mode}" != "thirdparty" && "${mode}" != "proto" && "${mode}" != "core" ]]; then
    echo "Usage: $0 [thirdparty|proto|core]"
    exit 1
fi

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/remote_env.sh"

cd "${ROOT_DIR}"

ensure_go() {
    if command -v go >/dev/null 2>&1; then
        go version
        return
    fi

    local tarball="go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
    local download_url="https://go.dev/dl/${tarball}"
    local archive_path="${TMPDIR}/${tarball}"

    mkdir -p "${TMPDIR}"
    rm -rf "${GO_ROOT}"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "${download_url}" -o "${archive_path}"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "${archive_path}" "${download_url}"
    else
        echo "Neither curl nor wget is available to install Go"
        exit 1
    fi

    mkdir -p "${GO_ROOT}"
    tar -xzf "${archive_path}" --strip-components=1 -C "${GO_ROOT}"
    export GOROOT="${GO_ROOT}"
    export PATH="${GO_ROOT}/bin:${PATH}"
    go version
}

ensure_openblas() {
    if [[ -f "${OPENBLAS_ROOT}/include/cblas.h" && -f "${OPENBLAS_ROOT}/lib/libopenblas.so" ]]; then
        return
    fi

    local tarball="v${OPENBLAS_VERSION}.tar.gz"
    local source_parent="${TMPDIR}/openblas-src"
    local source_dir="${source_parent}/OpenBLAS-${OPENBLAS_VERSION}"
    local archive_path="${source_parent}/${tarball}"
    local jobs=8

    if command -v nproc >/dev/null 2>&1; then
        jobs="$(nproc)"
    fi

    mkdir -p "${source_parent}"
    if [[ ! -f "${archive_path}" ]]; then
        if command -v curl >/dev/null 2>&1; then
            curl -fsSL "https://github.com/xianyi/OpenBLAS/archive/${tarball}" -o "${archive_path}"
        elif command -v wget >/dev/null 2>&1; then
            wget -qO "${archive_path}" "https://github.com/xianyi/OpenBLAS/archive/${tarball}"
        else
            echo "Neither curl nor wget is available to install OpenBLAS"
            exit 1
        fi
    fi

    rm -rf "${source_dir}" "${OPENBLAS_ROOT}"
    tar -xzf "${archive_path}" -C "${source_parent}"

    pushd "${source_dir}" >/dev/null
    make clean >/dev/null 2>&1 || true
    make -j"${jobs}" \
        TARGET=CORE2 \
        DYNAMIC_ARCH=0 \
        DYNAMIC_OLDER=0 \
        USE_THREAD=0 \
        USE_OPENMP=0 \
        ONLY_CBLAS=1 \
        CC=gcc \
        COMMON_OPT="-O3 -g -fPIC" \
        LIBPREFIX="libopenblas" \
        INTERFACE64=0 \
        NO_STATIC=1
    make PREFIX="${OPENBLAS_ROOT}" install
    popd >/dev/null
}

run_proto_generation() {
    ensure_go
    bash scripts/download_milvus_proto.sh
    make get-proto-deps INSTALL_PATH="${INSTALL_PATH}"
    bash scripts/generate_proto.sh "${INSTALL_PATH}"
}

if [[ "${mode}" == "thirdparty" || "${mode}" == "core" ]]; then
    bash scripts/3rdparty_build.sh -o OFF -t Release
fi

if [[ "${mode}" == "proto" || "${mode}" == "core" ]]; then
    run_proto_generation
fi

if [[ "${mode}" == "core" ]]; then
    ensure_openblas

    export CPATH="${OPENBLAS_ROOT}/include${CPATH:+:${CPATH}}"
    export C_INCLUDE_PATH="${OPENBLAS_ROOT}/include${C_INCLUDE_PATH:+:${C_INCLUDE_PATH}}"
    export CPLUS_INCLUDE_PATH="${OPENBLAS_ROOT}/include${CPLUS_INCLUDE_PATH:+:${CPLUS_INCLUDE_PATH}}"
    export LIBRARY_PATH="${OPENBLAS_ROOT}/lib${LIBRARY_PATH:+:${LIBRARY_PATH}}"
    export LD_LIBRARY_PATH="${OPENBLAS_ROOT}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    export CMAKE_PREFIX_PATH="${OPENBLAS_ROOT}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}"
    export CMAKE_INCLUDE_PATH="${OPENBLAS_ROOT}/include${CMAKE_INCLUDE_PATH:+:${CMAKE_INCLUDE_PATH}}"
    export CMAKE_LIBRARY_PATH="${OPENBLAS_ROOT}/lib${CMAKE_LIBRARY_PATH:+:${CMAKE_LIBRARY_PATH}}"

    bash scripts/core_build.sh -t Release
fi
