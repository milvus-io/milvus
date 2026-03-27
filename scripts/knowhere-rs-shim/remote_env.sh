#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

INTEG_ROOT="${MILVUS_RS_INTEG_ROOT:-/data/work/milvus-rs-integ}"
KNOWHERE_RS_ROOT_DEFAULT="${MILVUS_RS_KNOWHERE_RS_ROOT:-/data/work/knowhere-rs}"
GO_ROOT_DEFAULT="${MILVUS_RS_GO_ROOT:-${INTEG_ROOT}/go}"
GO_WORK_ROOT="${MILVUS_RS_GO_WORK_ROOT:-${INTEG_ROOT}/go-work}"
OPENBLAS_ROOT_DEFAULT="${MILVUS_RS_OPENBLAS_ROOT:-${INTEG_ROOT}/openblas}"
MILVUS_RS_VAR_ROOT_DEFAULT="${MILVUS_RS_VAR_ROOT:-${INTEG_ROOT}/milvus-var}"
MILVUS_RS_PYTHON_SITE_DEFAULT="${MILVUS_RS_PYTHON_SITE:-${INTEG_ROOT}/python-site}"

export PATH="${INTEG_ROOT}/bin:/root/.cargo/bin:${PATH}"
export HOME="${INTEG_ROOT}/home"
export TMPDIR="${INTEG_ROOT}/tmp"
export KNOWHERE_RS_LIB_DIR="${KNOWHERE_RS_LIB_DIR:-${INTEG_ROOT}/knowhere-rs-target/release}"
export GOPATH="${GOPATH:-${GO_WORK_ROOT}}"
export GOCACHE="${GOCACHE:-${GO_WORK_ROOT}/cache}"
export GOMODCACHE="${GOMODCACHE:-${GO_WORK_ROOT}/pkg/mod}"

prepend_path_var() {
    local var_name="$1"
    local dir_path="$2"
    local current_value

    [[ -d "${dir_path}" ]] || return 0
    current_value="${!var_name:-}"
    if [[ -n "${current_value}" ]]; then
        printf -v "${var_name}" '%s:%s' "${dir_path}" "${current_value}"
    else
        printf -v "${var_name}" '%s' "${dir_path}"
    fi
    export "${var_name}"
}

prepend_flag_var() {
    local var_name="$1"
    local flag_value="$2"
    local current_value

    current_value="${!var_name:-}"
    if [[ -n "${current_value}" ]]; then
        printf -v "${var_name}" '%s %s' "${flag_value}" "${current_value}"
    else
        printf -v "${var_name}" '%s' "${flag_value}"
    fi
    export "${var_name}"
}

prepend_include_dir() {
    local include_dir="$1"

    prepend_path_var CPATH "${include_dir}"
    prepend_path_var C_INCLUDE_PATH "${include_dir}"
    prepend_path_var CPLUS_INCLUDE_PATH "${include_dir}"
    prepend_path_var CMAKE_INCLUDE_PATH "${include_dir}"
    prepend_flag_var CGO_CFLAGS "-I${include_dir}"
    prepend_flag_var CGO_CPPFLAGS "-I${include_dir}"
    prepend_flag_var CGO_CXXFLAGS "-I${include_dir}"
}

prepend_library_dir() {
    local library_dir="$1"

    prepend_path_var LIBRARY_PATH "${library_dir}"
    prepend_path_var LD_LIBRARY_PATH "${library_dir}"
    prepend_path_var CMAKE_LIBRARY_PATH "${library_dir}"
    prepend_flag_var CGO_LDFLAGS "-L${library_dir}"
}

if [[ -x "${GO_ROOT_DEFAULT}/bin/go" ]]; then
    export GOROOT="${GOROOT:-${GO_ROOT_DEFAULT}}"
    export PATH="${GO_ROOT_DEFAULT}/bin:${PATH}"
fi

if [[ -f "${OPENBLAS_ROOT_DEFAULT}/include/cblas.h" ]]; then
    prepend_include_dir "${OPENBLAS_ROOT_DEFAULT}/include"
    prepend_library_dir "${OPENBLAS_ROOT_DEFAULT}/lib"
    prepend_flag_var CGO_LDFLAGS "-Wl,--no-as-needed -lopenblas -Wl,--as-needed"
    export PKG_CONFIG_PATH="${OPENBLAS_ROOT_DEFAULT}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
    export CMAKE_PREFIX_PATH="${OPENBLAS_ROOT_DEFAULT}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}"
fi

prepend_include_dir "${ROOT_DIR}/cmake_build/thirdparty/milvus-common/milvus-common-src/include"
prepend_include_dir "${ROOT_DIR}/cmake_build/thirdparty/milvus-storage/milvus-storage-src/cpp/include"
prepend_library_dir "${ROOT_DIR}/cmake_build/lib"

for conan_include_dir in \
    "${INTEG_ROOT}/conan-home/.conan/data/rocksdb"/*/*/*/package/*/include \
    "${INTEG_ROOT}/conan-home/.conan/data/librdkafka"/*/*/*/package/*/include; do
    [[ -d "${conan_include_dir}" ]] || continue
    prepend_include_dir "${conan_include_dir}"
done

for pkg_dir in \
    "${ROOT_DIR}/cmake_build/thirdparty/rdkafka" \
    "${ROOT_DIR}/cmake_build/thirdparty/rocksdb" \
    "${ROOT_DIR}/cmake_build/thirdparty/milvus-storage" \
    "${ROOT_DIR}/cmake_build/thirdparty/milvus-storage/milvus-storage-build"; do
    if [[ -d "${pkg_dir}" ]]; then
        export PKG_CONFIG_PATH="${pkg_dir}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
    fi
done

mkdir -p "${HOME}" "${HOME}/.cargo/bin" "${TMPDIR}" "${GOPATH}" "${GOCACHE}" "${GOMODCACHE}"
mkdir -p "${MILVUS_RS_PYTHON_SITE_DEFAULT}"

prepend_path_var PYTHONPATH "${MILVUS_RS_PYTHON_SITE_DEFAULT}"

if [[ -f /root/.cargo/env ]]; then
    # shellcheck disable=SC1091
    source /root/.cargo/env
fi

if [[ -d /root/.cargo/bin ]]; then
    for tool in /root/.cargo/bin/*; do
        [[ -e "${tool}" ]] || continue
        ln -snf "${tool}" "${HOME}/.cargo/bin/$(basename "${tool}")"
    done
fi

export PATH="${HOME}/.cargo/bin:${PATH}"

# Keep all writable Milvus runtime state off the full rootfs.
export ETCD_DATA_DIR="${ETCD_DATA_DIR:-${MILVUS_RS_VAR_ROOT_DEFAULT}/etcd}"
export MILVUS_CONF_LOCALSTORAGE_PATH="${MILVUS_CONF_LOCALSTORAGE_PATH:-${MILVUS_RS_VAR_ROOT_DEFAULT}/data}"
export MILVUS_CONF_ROCKSMQ_PATH="${MILVUS_CONF_ROCKSMQ_PATH:-${MILVUS_RS_VAR_ROOT_DEFAULT}/rdb_data}"
export MILVUS_CONF_FUNCTION_ANALYZER_LOCAL_RESOURCE_PATH="${MILVUS_CONF_FUNCTION_ANALYZER_LOCAL_RESOURCE_PATH:-${MILVUS_RS_VAR_ROOT_DEFAULT}/analyzer}"
export MILVUS_CONF_PROFILE_PPROF_PATH="${MILVUS_CONF_PROFILE_PPROF_PATH:-${MILVUS_RS_VAR_ROOT_DEFAULT}/pprof}"

# Stage-1 only implements the explicit float/HNSW path. Override Milvus'
# unsupported no-CUDA auto-index default so standalone can start cleanly.
export MILVUS_CONF_AUTOINDEX_PARAMS_BUILD="${MILVUS_CONF_AUTOINDEX_PARAMS_BUILD:-{\"M\":18,\"efConstruction\":240,\"index_type\":\"HNSW\",\"metric_type\":\"COSINE\"}}"

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
