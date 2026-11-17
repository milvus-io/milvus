#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

[[ "$(uname -s)" == Linux && "${USE_TSAN:-OFF}" != ON ]] || exit 0
case "${WITH_GPU:-OFF}" in ON|on|TRUE|True|true|1) exit 0 ;; esac
# Only the final CMake configuration can identify a compiler selected by a
# custom profile/toolchain. Do not reject an unrelated compiler on PATH.
if [[ -n "${CONAN_HOST_PROFILE:-}${CMAKE_TOOLCHAIN_FILE:-}" ||
      "${CMAKE_EXTRA_ARGS:-}" =~ CMAKE_(CXX_COMPILER|TOOLCHAIN_FILE) ]]; then
    echo 'Custom compiler selection: GCC version policy will be checked during core CMake configuration.'
    exit 0
fi

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat > "$work/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.18)
project(milvus_compiler_check LANGUAGES CXX)
include("${CHECK_MODULE}")
EOF
cmake -S "$work" -B "$work/build" \
    "-DCHECK_MODULE=$root/internal/core/cmake/CheckCompiler.cmake"
