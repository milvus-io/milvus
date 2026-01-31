#!/bin/bash

# Milvus macOS Build Test
#
# This script tests the build on macOS (run on Mac mini or local Mac)
#
# Usage:
#   ./scripts/test-macos.sh [--clean]
#
# Options:
#   --clean    Clean build (remove cmake_build and output directories)

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

CLEAN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

print_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Check if running on macOS
if [[ "$(uname -s)" != "Darwin" ]]; then
    echo -e "${RED}Error: This script must be run on macOS${NC}"
    exit 1
fi

# Detect macOS version
MACOS_VERSION=$(sw_vers -productVersion)
MACOS_MAJOR=$(echo "$MACOS_VERSION" | cut -d. -f1)
ARCH=$(uname -m)

print_header "macOS Build Test"

echo "macOS Version: ${MACOS_VERSION}"
echo "Architecture: ${ARCH}"
echo "Clean Build: ${CLEAN}"
echo ""

# Ensure results directory exists
mkdir -p "${RESULTS_DIR}"

# Determine result file name
RESULT_NAME="macos${MACOS_MAJOR}-${ARCH}"
LOG_FILE="${RESULTS_DIR}/${RESULT_NAME}.log"
EXIT_FILE="${RESULTS_DIR}/${RESULT_NAME}.exit"

cd "${MILVUS_ROOT}"

if [ "$CLEAN" = true ]; then
    print_header "Cleaning previous build"
    rm -rf cmake_build internal/core/output
fi

# Run build and capture output
print_header "Installing Dependencies"
{
    echo "=== Install Dependencies ==="
    echo "Started: $(date)"
    ./scripts/install_deps.sh
    echo "Finished: $(date)"
    echo ""
    echo "=== Build Milvus ==="
    echo "Started: $(date)"
    make
    echo "Finished: $(date)"
} 2>&1 | tee "${LOG_FILE}"

EXIT_CODE=${PIPESTATUS[0]}
echo "$EXIT_CODE" > "${EXIT_FILE}"

print_header "Build Result"

if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}[PASS]${NC} ${RESULT_NAME}"

    # Verify binary exists
    if [ -f "bin/milvus" ]; then
        echo -e "${GREEN}Binary created: bin/milvus${NC}"
        ls -lh bin/milvus
    else
        echo -e "${YELLOW}Warning: bin/milvus not found${NC}"
    fi
else
    echo -e "${RED}[FAIL]${NC} ${RESULT_NAME} (exit code: ${EXIT_CODE})"
    echo "Log file: ${LOG_FILE}"
fi

echo ""
exit $EXIT_CODE
