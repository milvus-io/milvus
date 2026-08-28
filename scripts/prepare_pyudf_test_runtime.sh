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
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
    DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
ROOT_DIR="$(cd -P "$(dirname "$SOURCE")/.." && pwd)"
CMAKE_CACHE="${ROOT_DIR}/cmake_build/CMakeCache.txt"

if [[ ! -f "${CMAKE_CACHE}" ]]; then
    echo "ERROR: CMake cache not found: ${CMAKE_CACHE}" >&2
    exit 1
fi

PYUDF_ENABLED="$(grep -m1 '^MILVUS_ENABLE_PY_UDF:BOOL=' "${CMAKE_CACHE}" | cut -d= -f2- || true)"
if [[ "${PYUDF_ENABLED}" == "OFF" ]]; then
    echo "PyUDF native backend is disabled; skipping trusted runtime setup"
    exit 0
fi
if [[ "${PYUDF_ENABLED}" != "ON" ]]; then
    echo "ERROR: invalid MILVUS_ENABLE_PY_UDF value in ${CMAKE_CACHE}: ${PYUDF_ENABLED}" >&2
    exit 1
fi

PYTHON_EXECUTABLE="$(grep -m1 -E '^MILVUS_PYTHON_EXECUTABLE(:[^=]*)?=' "${CMAKE_CACHE}" | cut -d= -f2- || true)"
if [[ -z "${PYTHON_EXECUTABLE}" ]]; then
    PYTHON_EXECUTABLE="$(grep -m1 -E '^Python3_EXECUTABLE(:[^=]*)?=' "${CMAKE_CACHE}" | cut -d= -f2- || true)"
fi
if [[ -z "${PYTHON_EXECUTABLE}" ]]; then
    PYTHON_EXECUTABLE="$(grep -m1 -E '^_Python3_EXECUTABLE(:[^=]*)?=' "${CMAKE_CACHE}" | cut -d= -f2- || true)"
fi
if [[ -z "${PYTHON_EXECUTABLE}" || ! -x "${PYTHON_EXECUTABLE}" ]]; then
    echo "ERROR: invalid Python executable in ${CMAKE_CACHE}: ${PYTHON_EXECUTABLE}" >&2
    exit 1
fi

make -C "${ROOT_DIR}" \
    PYTHON="${PYTHON_EXECUTABLE}" \
    install-pyudf-runtime-wheel
