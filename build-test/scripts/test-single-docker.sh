#!/bin/bash

# Milvus Single Platform Docker Build Test
#
# Usage:
#   ./scripts/test-single-docker.sh <platform> [--arm64]
#
# Platforms:
#   ubuntu2004, ubuntu2204, ubuntu2404, rocky8, rocky9, amazonlinux2023
#
# Options:
#   --arm64    Build for ARM64 architecture (for Apple Silicon testing)
#
# Examples:
#   ./scripts/test-single-docker.sh ubuntu2204
#   ./scripts/test-single-docker.sh rocky9 --arm64

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

if [ $# -lt 1 ]; then
    echo "Usage: $0 <platform> [--arm64]"
    echo "Platforms: ubuntu2004, ubuntu2204, ubuntu2404, rocky8, rocky9, amazonlinux2023"
    exit 1
fi

PLATFORM=$1
ARM64=false

if [ "$2" = "--arm64" ]; then
    ARM64=true
fi

# Validate platform
DOCKERFILE="${BUILD_TEST_DIR}/docker/Dockerfile.${PLATFORM}"
if [ ! -f "$DOCKERFILE" ]; then
    echo -e "${RED}Error: Unknown platform '${PLATFORM}'${NC}"
    echo "Available platforms:"
    ls -1 "${BUILD_TEST_DIR}/docker/" | sed 's/Dockerfile./  /'
    exit 1
fi

print_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Ensure results directory exists
mkdir -p "${RESULTS_DIR}"

# Determine result file name
if [ "$ARM64" = true ]; then
    RESULT_NAME="${PLATFORM}-arm64"
    DOCKER_PLATFORM="linux/arm64"
else
    RESULT_NAME="${PLATFORM}"
    DOCKER_PLATFORM="linux/amd64"
fi

LOG_FILE="${RESULTS_DIR}/${RESULT_NAME}.log"
EXIT_FILE="${RESULTS_DIR}/${RESULT_NAME}.exit"
IMAGE_NAME="milvus-build-test:${RESULT_NAME}"

print_header "Building ${PLATFORM} (${DOCKER_PLATFORM})"

echo "Platform: ${PLATFORM}"
echo "Architecture: ${DOCKER_PLATFORM}"
echo "Dockerfile: ${DOCKERFILE}"
echo "Log file: ${LOG_FILE}"
echo ""

cd "${MILVUS_ROOT}"

# Build Docker image
print_header "Building Docker Image"
docker build \
    --platform "${DOCKER_PLATFORM}" \
    -f "${DOCKERFILE}" \
    -t "${IMAGE_NAME}" \
    . 2>&1 | tee "${LOG_FILE}.build"

# Run build test
print_header "Running Build Test"
docker run --rm \
    --platform "${DOCKER_PLATFORM}" \
    -v "${RESULTS_DIR}:/results" \
    "${IMAGE_NAME}" 2>&1 | tee -a "${LOG_FILE}"

# Check result
if [ -f "${EXIT_FILE}" ]; then
    EXIT_CODE=$(cat "${EXIT_FILE}")
else
    EXIT_CODE=1
fi

print_header "Build Result"

if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}[PASS]${NC} ${RESULT_NAME}"
else
    echo -e "${RED}[FAIL]${NC} ${RESULT_NAME} (exit code: ${EXIT_CODE})"
    echo "Log file: ${LOG_FILE}"
fi

echo ""
exit $EXIT_CODE
