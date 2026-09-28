#!/usr/bin/env bash

set -euo pipefail

PWD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "${PWD_DIR}/scripts/setenv.sh" || exit 1
set -euo pipefail

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

PARSER_SANITIZER_FLAGS=()
PARSER_SANITIZER_LINK_FLAGS=()
if [[ "${USE_TSAN:-OFF}" == "ON" ]]; then
    if [[ "${OS}" != "Linux" || "${USE_ASAN:-OFF}" == "ON" ]]; then
        echo "ERROR: USE_TSAN requires Linux and cannot be combined with USE_ASAN" >&2
        exit 1
    fi
    case " $(go env GOFLAGS) " in
        *" -race "*|*" -race=true "*)
            echo "ERROR: ThreadSanitizer cannot be combined with Go -race" >&2
            exit 1
            ;;
    esac
    PARSER_SANITIZER_FLAGS=(-fsanitize=thread -g -fno-omit-frame-pointer)
    PARSER_SANITIZER_LINK_FLAGS=(-shared-libsan)
    export CGO_CFLAGS="$(go env CGO_CFLAGS) ${PARSER_SANITIZER_FLAGS[*]}"
    export CGO_CXXFLAGS="$(go env CGO_CXXFLAGS) ${PARSER_SANITIZER_FLAGS[*]}"
    export CGO_LDFLAGS="$(go env CGO_LDFLAGS) -fsanitize=thread -shared-libsan"
fi

go env -w CGO_ENABLED="1"

# Build Go c-shared library using package path (not file path) and keep the
# local Milvus module at (devel) instead of stamping a misleading pseudo-version.
GO111MODULE=on go build -buildvcs=false -buildmode=c-shared \
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
${CXX:-g++} -std=c++17 -shared -fPIC ${PARSER_SANITIZER_FLAGS[@]+"${PARSER_SANITIZER_FLAGS[@]}"} \
    -o "${OUTPUT_LIB}/libmilvus-planparser-cpp.${EXT}" \
    "${CWRAPPER_DIR}/milvus_plan_parser.cpp" \
    -I"${OUTPUT_INCLUDE}" \
    -L"${OUTPUT_LIB}" -lmilvus-planparser \
    ${RPATH_FLAG} ${PARSER_SANITIZER_LINK_FLAGS[@]+"${PARSER_SANITIZER_LINK_FLAGS[@]}"}

echo "Plan parser shared library built successfully."
