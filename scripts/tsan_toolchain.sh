#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
# Source this file before configuring CMake, Conan, or cgo in a TSan build.
milvus_tsan_toolchain() {
    [[ "${USE_TSAN:-OFF}" == "ON" ]] || return 0
    export MILVUS_LLVM_ROOT="${MILVUS_LLVM_ROOT:-/usr/lib/llvm-20}"
    export CC="${MILVUS_LLVM_ROOT}/bin/clang"
    export CXX="${MILVUS_LLVM_ROOT}/bin/clang++"
    if [[ ! -x "$CC" || ! -x "$CXX" ]]; then
        echo "ERROR: TSan requires LLVM 20 with compiler-rt, libomp and Archer; set MILVUS_LLVM_ROOT" >&2
        return 1
    fi
    if [[ "$("$CC" -dumpversion | cut -d. -f1)" != 20 ]]; then
        echo "ERROR: TSan requires the validated LLVM 20 toolchain" >&2
        return 1
    fi
    local runtime
    runtime="$("$CC" -print-file-name="libclang_rt.tsan-$(uname -m).so")"
    if [[ ! -f "$runtime" || ! -f "${MILVUS_LLVM_ROOT}/lib/libarcher.so" ||
          ! -f "${MILVUS_LLVM_ROOT}/lib/libomp.so" ]]; then
        echo "ERROR: LLVM TSan runtime, libomp or Archer is missing" >&2
        return 1
    fi
    export MILVUS_TSAN_RUNTIME="$runtime"
    # pkg-config exports this Clang driver option through #cgo LDFLAGS.
    export CGO_LDFLAGS_ALLOW="${CGO_LDFLAGS_ALLOW:+(${CGO_LDFLAGS_ALLOW})|}^-shared-libsan$"
    export PATH="${MILVUS_LLVM_ROOT}/bin:${PATH}"
    export LIBRARY_PATH="${MILVUS_LLVM_ROOT}/lib:$(dirname "$runtime")${LIBRARY_PATH:+:${LIBRARY_PATH}}"
    export LD_LIBRARY_PATH="${MILVUS_LLVM_ROOT}/lib:$(dirname "$runtime")${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
}
milvus_tsan_toolchain
