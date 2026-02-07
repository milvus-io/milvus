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

# C++ Unit Test Runner (WITHOUT coverage collection)
# =====================================================
# This script is for FAST parallel execution of C++ unit tests.
# It does NOT collect coverage data - use run_cpp_codecov.sh for coverage.
#
# Purpose:
#   - Fast feedback during development (parallel execution)
#   - CI pipelines that don't require coverage (UT-CPP job)
#
# For coverage collection, use run_cpp_codecov.sh instead.
#
# Environment variables:
#   CPP_UT_SHARDS:   Number of parallel shards for all_tests (default: 3)
#   CPP_UT_PARALLEL: Run tests in parallel (default: true)
#   TEST_TIMEOUT:    Timeout per test in seconds (default: 600)
#   MILVUS_TEST_LOCAL_PATH: Override temp directory for test files

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
    DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
ROOT_DIR="$(cd -P "$(dirname "$SOURCE")/.." && pwd)"
source ${ROOT_DIR}/scripts/setenv.sh

MILVUS_CORE_DIR="${ROOT_DIR}/internal/core"
MILVUS_CORE_UNITTEST_DIR="${MILVUS_CORE_DIR}/output/unittest"

echo "=============================================="
echo "=== C++ Unit Tests (No Coverage) ==="
echo "=============================================="
echo "ROOT_DIR = ${ROOT_DIR}"
echo "MILVUS_CORE_DIR = ${MILVUS_CORE_DIR}"
echo "MILVUS_CORE_UNITTEST_DIR = ${MILVUS_CORE_UNITTEST_DIR}"

# Parallel execution settings
CPP_UT_SHARDS=${CPP_UT_SHARDS:-3}
CPP_UT_PARALLEL=${CPP_UT_PARALLEL:-true}

echo "CPP_UT_SHARDS=${CPP_UT_SHARDS}, CPP_UT_PARALLEL=${CPP_UT_PARALLEL}"

# Check if unittest directory exists
if [ ! -d "${MILVUS_CORE_UNITTEST_DIR}" ]; then
    echo "ERROR: Unittest directory not found: ${MILVUS_CORE_UNITTEST_DIR}"
    echo "Please build the tests first with: make build-cpp-ut"
    exit 1
fi

# Starting the timer
beginTime=$(date +%s)

# Function to run a single test
run_test() {
    local test=$1
    local shard_index=$2
    local total_shards=$3

    echo "[$(date +%H:%M:%S)] Running: ${test} (shard ${shard_index}/${total_shards})"

    local env_vars=""
    if [ "$total_shards" -gt 1 ]; then
        env_vars="GTEST_TOTAL_SHARDS=${total_shards} GTEST_SHARD_INDEX=${shard_index}"
        # Use separate temp directory for each shard to avoid file conflicts
        local shard_tmp_dir="/tmp/milvus/local_data_shard${shard_index}"
        mkdir -p "$shard_tmp_dir"
        env_vars="$env_vars MILVUS_TEST_LOCAL_PATH=${shard_tmp_dir}"
    fi

    if [ -n "$MILVUS_ENABLE_ASAN_LIB" ]; then
        echo "ASAN is enabled with env MILVUS_ENABLE_ASAN_LIB"
        env $env_vars LD_PRELOAD="$MILVUS_ENABLE_ASAN_LIB:$LD_PRELOAD" ${MILVUS_CORE_UNITTEST_DIR}/${test}
    else
        env $env_vars ${MILVUS_CORE_UNITTEST_DIR}/${test}
    fi
}

# Run unittest
if [ "$CPP_UT_PARALLEL" = "true" ] && [ "$CPP_UT_SHARDS" -gt 1 ]; then
    # Parallel mode with sharding
    echo "=== Running C++ unit tests in PARALLEL mode (shards=${CPP_UT_SHARDS}) ==="
    pids=()
    test_names=()
    log_files=()

    # Create log directory
    LOG_DIR="${ROOT_DIR}/cpp_ut_logs"
    rm -rf "${LOG_DIR}"
    mkdir -p "${LOG_DIR}"

    for test in $(ls ${MILVUS_CORE_UNITTEST_DIR}); do
        if [ "$test" = "all_tests" ]; then
            # Shard all_tests across multiple processes
            for i in $(seq 0 $((CPP_UT_SHARDS-1))); do
                log_file="${LOG_DIR}/${test}_shard${i}.log"
                run_test "$test" "$i" "$CPP_UT_SHARDS" > "$log_file" 2>&1 &
                pids+=($!)
                test_names+=("${test}[shard${i}]")
                log_files+=("$log_file")
            done
        else
            # Run other tests directly (no sharding needed for small tests)
            log_file="${LOG_DIR}/${test}.log"
            run_test "$test" "0" "1" > "$log_file" 2>&1 &
            pids+=($!)
            test_names+=("${test}")
            log_files+=("$log_file")
        fi
    done

    echo "Started ${#pids[@]} test processes, waiting for completion..."

    # Wait for all tests with timeout (10 minutes per test max for faster debugging)
    TEST_TIMEOUT=${TEST_TIMEOUT:-600}  # 10 minutes in seconds
    failed=0
    failed_tests=()

    for i in "${!pids[@]}"; do
        pid=${pids[$i]}
        start_wait=$(date +%s)

        while kill -0 $pid 2>/dev/null; do
            elapsed=$(($(date +%s) - start_wait))
            if [ $elapsed -gt $TEST_TIMEOUT ]; then
                echo "TIMEOUT: ${test_names[$i]} (exceeded ${TEST_TIMEOUT}s)"
                echo "=== Log tail for ${test_names[$i]} (before kill) ==="
                tail -100 "${log_files[$i]}" 2>/dev/null || echo "(no log file)"
                echo "=== End log tail ==="
                kill -9 $pid 2>/dev/null || true
                failed_tests+=("${test_names[$i]} (TIMEOUT)")
                failed=1
                break
            fi
            # Print progress every 60 seconds
            if [ $((elapsed % 60)) -eq 0 ] && [ $elapsed -gt 0 ]; then
                echo "[$(date +%H:%M:%S)] Waiting for ${test_names[$i]} (${elapsed}s elapsed)..."
            fi
            sleep 5
        done

        # If not timed out, check exit status
        if [ $elapsed -le $TEST_TIMEOUT ]; then
            if ! wait $pid 2>/dev/null; then
                echo "FAILED: ${test_names[$i]}"
                failed_tests+=("${test_names[$i]}")
                failed=1
            else
                echo "PASSED: ${test_names[$i]}"
            fi
        fi
    done

    # Print summary
    echo ""
    echo "=============================================="
    echo "=== Test Results Summary ==="
    echo "=============================================="
    echo "Total tests: ${#pids[@]}"
    echo "Failed: ${#failed_tests[@]}"

    if [ $failed -ne 0 ]; then
        echo ""
        echo "Failed tests:"
        for ft in "${failed_tests[@]}"; do
            echo "  - $ft"
        done
        echo ""
        echo "Check logs in: ${LOG_DIR}"

        # Print last 50 lines of failed test logs
        for i in "${!test_names[@]}"; do
            for ft in "${failed_tests[@]}"; do
                if [ "${test_names[$i]}" = "$ft" ]; then
                    echo ""
                    echo "=== Log tail for $ft ==="
                    tail -50 "${log_files[$i]}" 2>/dev/null || true
                fi
            done
        done

        exit 1
    fi
else
    # Sequential mode
    echo "=== Running C++ unit tests in SEQUENTIAL mode ==="
    for test in $(ls ${MILVUS_CORE_UNITTEST_DIR}); do
        run_test "$test" "0" "1"
        if [ $? -ne 0 ]; then
            echo "${MILVUS_CORE_UNITTEST_DIR}/${test} run failed"
            exit 1
        fi
    done
fi

endTime=$(date +%s)
duration=$((endTime - beginTime))

echo ""
echo "=============================================="
echo "=== C++ Unit Tests Completed ==="
echo "=============================================="
echo "Total time: ${duration}s ($(echo "scale=1; ${duration}/60" | bc)m)"
echo "Mode: $([ "$CPP_UT_PARALLEL" = "true" ] && echo "Parallel (${CPP_UT_SHARDS} shards)" || echo "Sequential")"
