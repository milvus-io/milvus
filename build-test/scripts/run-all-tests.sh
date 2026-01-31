#!/bin/bash

# Milvus Build Test Runner
# Tests compilation on multiple platforms using Docker
#
# Usage:
#   ./scripts/run-all-tests.sh [--parallel] [--platforms <list>]
#
# Options:
#   --parallel     Run all builds in parallel (requires more resources)
#   --platforms    Comma-separated list of platforms to test
#                  Default: ubuntu2004,ubuntu2204,ubuntu2404,rocky8,rocky9,amazonlinux2023
#
# Examples:
#   ./scripts/run-all-tests.sh
#   ./scripts/run-all-tests.sh --parallel
#   ./scripts/run-all-tests.sh --platforms ubuntu2204,rocky9

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_TEST_DIR="$(dirname "$SCRIPT_DIR")"
MILVUS_ROOT="$(dirname "$BUILD_TEST_DIR")"
RESULTS_DIR="${BUILD_TEST_DIR}/results"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default platforms
DEFAULT_PLATFORMS="ubuntu2004,ubuntu2204,ubuntu2404,rocky8,rocky9,amazonlinux2023"
PARALLEL=false
PLATFORMS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --parallel)
            PARALLEL=true
            shift
            ;;
        --platforms)
            PLATFORMS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

PLATFORMS="${PLATFORMS:-$DEFAULT_PLATFORMS}"

print_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

print_result() {
    local platform=$1
    local exit_file="${RESULTS_DIR}/${platform}.exit"
    local log_file="${RESULTS_DIR}/${platform}.log"

    if [ -f "$exit_file" ]; then
        local exit_code=$(cat "$exit_file")
        if [ "$exit_code" -eq 0 ]; then
            echo -e "${GREEN}[PASS]${NC} ${platform}"
        else
            echo -e "${RED}[FAIL]${NC} ${platform} (exit code: ${exit_code})"
            echo -e "       Log: ${log_file}"
        fi
    else
        echo -e "${YELLOW}[SKIP]${NC} ${platform} (no result)"
    fi
}

# Ensure results directory exists
mkdir -p "${RESULTS_DIR}"

# Clean old results
rm -f "${RESULTS_DIR}"/*.log "${RESULTS_DIR}"/*.exit

print_header "Milvus Build Test"

echo "Platforms: ${PLATFORMS}"
echo "Parallel: ${PARALLEL}"
echo "Results: ${RESULTS_DIR}"
echo ""

cd "${BUILD_TEST_DIR}"

# Convert comma-separated to space-separated for docker compose
PLATFORM_LIST=$(echo "${PLATFORMS}" | tr ',' ' ')

if [ "$PARALLEL" = true ]; then
    print_header "Running builds in parallel"
    docker compose up --build ${PLATFORM_LIST}
else
    print_header "Running builds sequentially"
    for platform in ${PLATFORM_LIST}; do
        echo -e "${YELLOW}Building ${platform}...${NC}"
        docker compose up --build "${platform}" || true
        echo ""
    done
fi

print_header "Build Results"

for platform in ${PLATFORM_LIST}; do
    print_result "${platform}"
done

echo ""

# Count results
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

for platform in ${PLATFORM_LIST}; do
    exit_file="${RESULTS_DIR}/${platform}.exit"
    if [ -f "$exit_file" ]; then
        exit_code=$(cat "$exit_file")
        if [ "$exit_code" -eq 0 ]; then
            ((PASS_COUNT++))
        else
            ((FAIL_COUNT++))
        fi
    else
        ((SKIP_COUNT++))
    fi
done

echo -e "${GREEN}Passed: ${PASS_COUNT}${NC}  ${RED}Failed: ${FAIL_COUNT}${NC}  ${YELLOW}Skipped: ${SKIP_COUNT}${NC}"
echo ""

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
