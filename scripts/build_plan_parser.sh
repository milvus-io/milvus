#!/usr/bin/env bash

set -euo pipefail

PWD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "${PWD_DIR}/scripts/setenv.sh"

OUTPUT_LIB="${PWD_DIR}/internal/core/output/lib"
OUTPUT_INCLUDE="${PWD_DIR}/internal/core/output/include"
CWRAPPER_DIR="${PWD_DIR}/internal/parser/planparserv2/cwrapper"

mkdir -p "${OUTPUT_LIB}" "${OUTPUT_INCLUDE}"

OS="$(uname -s)"
if [[ "${OS}" == "Darwin" ]]; then
    EXT="dylib"
    RPATH_FLAG="-Wl,-rpath,@loader_path"
else
    EXT="so"
    RPATH_FLAG="-Wl,-rpath,\$ORIGIN"
fi

echo "Building plan parser shared library (${OS}, .${EXT}) ..."

go env -w CGO_ENABLED="1"

# Build Go c-shared library using package path (not file path) to ensure
# proper module dependency resolution in all build environments.
GO111MODULE=on go build -buildmode=c-shared \
    -o "${OUTPUT_LIB}/libmilvus-planparser.${EXT}" \
    ./internal/parser/planparserv2/cwrapper

# Fix install name on macOS so dependent libraries can find it via @loader_path
if [[ "${OS}" == "Darwin" ]]; then
    install_name_tool -id "@loader_path/libmilvus-planparser.dylib" \
        "${OUTPUT_LIB}/libmilvus-planparser.dylib"
fi

# Move generated header
mv "${OUTPUT_LIB}/libmilvus-planparser.h" "${OUTPUT_INCLUDE}/libmilvus-planparser.h"
cp "${CWRAPPER_DIR}/milvus_plan_parser.h" "${OUTPUT_INCLUDE}/"

# Build C++ wrapper
${CXX:-g++} -std=c++17 -shared -fPIC \
    -o "${OUTPUT_LIB}/libmilvus-planparser-cpp.${EXT}" \
    "${CWRAPPER_DIR}/milvus_plan_parser.cpp" \
    -I"${OUTPUT_INCLUDE}" \
    -L"${OUTPUT_LIB}" -lmilvus-planparser \
    ${RPATH_FLAG}

echo "Plan parser shared library built successfully."
