#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d)
trap 'result=$?; if [[ $result != 0 ]]; then cat "$work/log" >&2; fi; rm -rf "$work"' EXIT
export MILVUS_ALLOW_UNSUPPORTED_GCC=0
export USE_TSAN=OFF WITH_GPU=OFF CONAN_HOST_PROFILE= CMAKE_TOOLCHAIN_FILE= CMAKE_EXTRA_ARGS=
printf 'include("%s/internal/core/cmake/CheckCompiler.cmake" OPTIONAL)\n' "$root" > "$work/check.cmake"

check() {
    local expected=$1
    shift
    local result=0
    cmake -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_CXX_COMPILER_ID=GNU \
        -DCMAKE_CXX_COMPILER_VERSION=12.3.0 -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
        "$@" -P "$work/check.cmake" > "$work/log" 2>&1 || result=$?
    if [[ "$expected" == reject ]]; then
        if [[ "$result" == 0 ]]; then
            echo 'Expected old GCC to be rejected' >&2
            exit 1
        fi
        grep -q 'MILVUS_ALLOW_UNSUPPORTED_GCC=1' "$work/log"
        grep -q '15.3' "$work/log"
    else
        if [[ "$result" != 0 ]]; then
            cat "$work/log" >&2
            exit 1
        fi
        if [[ "$expected" == force ]]; then
            tr '\n' ' ' < "$work/log" | tr -s ' ' | \
                grep -q 'Build success and runtime correctness are not guaranteed'
        elif grep -q 'not guaranteed' "$work/log"; then
            echo 'Unexpected GCC warning for a supported or unrelated compiler' >&2
            exit 1
        fi
    fi
}

check reject
check reject -DCMAKE_CXX_COMPILER_VERSION=15.2.0
for version in 15.3.0 15.10.0 16.1.0; do
    check pass "-DCMAKE_CXX_COMPILER_VERSION=$version"
done
MILVUS_ALLOW_UNSUPPORTED_GCC=1 check force
MILVUS_ALLOW_UNSUPPORTED_GCC=true check reject
check pass -DCMAKE_CXX_COMPILER_ID=Clang
check pass -DCMAKE_CXX_COMPILER_ID=AppleClang -DCMAKE_SYSTEM_NAME=Darwin
check pass -DWITH_GPU=ON
check pass -DCMAKE_SYSTEM_NAME=Darwin

# Verify the real entry points on hosts with the old GCC used by existing
# builders. The dependency script must stop before touching Conan or its cache.
if [[ "$(uname -s)" == Linux ]] && command -v g++ >/dev/null; then
    gcc_version=$(g++ -dumpfullversion)
    cmake -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_CXX_COMPILER_ID=GNU \
        "-DCMAKE_CXX_COMPILER_VERSION=$gcc_version" -P "$work/check.cmake" \
        > "$work/log" 2>&1 && old_gcc=0 || old_gcc=1
    if [[ "$old_gcc" == 1 ]]; then
        if CXX=$(command -v g++) SKIP_3RDPARTY=0 CONAN_CMD="$work/no-conan" \
            bash "$root/scripts/3rdparty_build.sh" > "$work/log" 2>&1; then
            echo 'Expected dependency build to reject old GCC' >&2
            exit 1
        fi
        grep -q 'MILVUS_ALLOW_UNSUPPORTED_GCC=1' "$work/log"
        if cmake -S "$root/internal/core" -B "$work/core" \
            -DWITH_GPU=OFF "-DCMAKE_CXX_COMPILER=$(command -v g++)" \
            > "$work/log" 2>&1; then
            echo 'Expected core CMake configuration to reject old GCC' >&2
            exit 1
        fi
        grep -q 'MILVUS_ALLOW_UNSUPPORTED_GCC=1' "$work/log"
        # A changed CXX environment must not hide the compiler in an old cache.
        if CXX=/missing/compiler cmake -S "$root/internal/core" -B "$work/core" \
            > "$work/log" 2>&1; then
            echo 'Expected cached old GCC to remain rejected' >&2
            exit 1
        fi
        grep -q 'MILVUS_ALLOW_UNSUPPORTED_GCC=1' "$work/log"
        MILVUS_ALLOW_UNSUPPORTED_GCC=1 CXX=$(command -v g++) \
            bash "$root/scripts/check_cpp_compiler.sh" > "$work/log" 2>&1
        tr '\n' ' ' < "$work/log" | tr -s ' ' | \
            grep -q 'Continuing because MILVUS_ALLOW_UNSUPPORTED_GCC=1'
    fi
fi

# The preflight must inspect CXX, not an unrelated gcc on PATH. No dependencies
# are downloaded or built by this small CMake compiler-identification project.
if command -v clang++ >/dev/null; then
    CXX=$(command -v clang++) bash "$root/scripts/check_cpp_compiler.sh" > "$work/log" 2>&1
fi
USE_TSAN=ON CXX=/missing/compiler bash "$root/scripts/check_cpp_compiler.sh"
WITH_GPU=ON CXX=/missing/compiler bash "$root/scripts/check_cpp_compiler.sh"
CONAN_HOST_PROFILE=custom CXX=/missing/compiler bash "$root/scripts/check_cpp_compiler.sh"

echo 'C++ compiler version policy tests passed'
