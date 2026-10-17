#!/usr/bin/env bash
# Build the isolated syscall fault library and run its registered regression.
# Run this only in the final verification phase, after building all_tests.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 /absolute/path/to/all_tests /absolute/output/directory" >&2
    exit 2
fi

artifact_test_binary="$1"
artifact_fault_output="$2"
artifact_fault_source="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rtk proxy mkdir -p -- "$artifact_fault_output"
rtk proxy cc -std=c11 -shared -fPIC -O2 \
    "$artifact_fault_source/publication_faults.c" -ldl \
    -o "$artifact_fault_output/publication_faults.so"

# Put the same sanitizer runtime used for all_tests before the fault library
# when MILVUS_TEST_PRELOAD is set by the verification environment.
artifact_fault_preload="${MILVUS_TEST_PRELOAD:-${LD_PRELOAD:-}}"
if [[ -n "$artifact_fault_preload" ]]; then
    artifact_fault_preload+=:
fi
artifact_fault_preload+="$artifact_fault_output/publication_faults.so"
rtk proxy env LD_PRELOAD="$artifact_fault_preload" \
    "$artifact_test_binary" \
    --gtest_filter=V1FileSourceTest.RollbackFailurePreservesCodeAndPath
