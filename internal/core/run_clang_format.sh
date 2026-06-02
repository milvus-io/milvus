#!/usr/bin/env bash

set -euo pipefail

if [ -z "${1:-}" ]; then
    echo "usage: $0 <path_to_core>"
    exit 1
else
    echo start formating
fi
CorePath=$1

CLANG_FORMAT=${CLANG_FORMAT:-clang-format-15}

if ! command -v "$CLANG_FORMAT" >/dev/null 2>&1; then
    echo "ERROR: $CLANG_FORMAT is required for cpp format check but was not found in PATH" >&2
    exit 1
fi

formatThis() {
    find "$1" \
        -type f \
        \( -name "*.cpp" -o -name "*.c" -o -name "*.h" -o -name "*.cc" \) \
        ! -path "*/gen_tools/templates/*" \
        ! -name "*.pb.*" \
        ! -name "tantivy-binding.h" \
        ! -path "*/CMakeFiles/*" \
        -exec "$CLANG_FORMAT" -i {} +
}

formatThis "${CorePath}/src"
formatThis "${CorePath}/unittest"
formatThis "${CorePath}/thirdparty/tantivy"

${CorePath}/build-support/add_cpp_license.sh ${CorePath}/build-support/cpp_license.txt ${CorePath}
${CorePath}/build-support/add_cmake_license.sh ${CorePath}/build-support/cmake_license.txt ${CorePath}
