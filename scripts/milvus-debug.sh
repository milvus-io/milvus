#!/bin/bash
#
# Debug a coredump produced by the stripped (production) Milvus image
# using the matching unstripped debug image.
#
# Starting from v2.6.15, each Milvus release publishes two image variants:
#   milvusdb/milvus:<tag>         - stripped, production (default, ~1/3 size)
#   milvusdb/milvus:<tag>-debug   - unstripped, full debug symbols (for GDB)
#
# The stripped and debug images share byte-identical code sections, so GDB
# can use the debug image's symbols to resolve addresses in a coredump
# produced by the stripped image.
#
# Usage:
#   ./milvus-debug.sh <coredump> <stripped-image> <debug-image>
#
# Examples:
#   ./milvus-debug.sh ./core.12345 milvusdb/milvus:v2.6.15 milvusdb/milvus:v2.6.15-debug
#   ./milvus-debug.sh ./core.12345 harbor.example.com/milvus:v2.6.15 harbor.example.com/milvus:v2.6.15-debug
#
# Typical workflow when a Milvus pod crashes in production:
#   # 1. Copy the coredump out of the crashed pod
#   kubectl cp <ns>/<pod>:/tmp/cores/core.<pid> ./core.<pid>
#
#   # 2. Run this script (GDB starts automatically with backtrace)
#   ./milvus-debug.sh ./core.<pid> milvusdb/milvus:v2.6.15 milvusdb/milvus:v2.6.15-debug
#
set -euo pipefail

COREDUMP="${1:?Usage: $0 <coredump> <stripped-image> <debug-image>}"
IMAGE="${2:?Missing stripped image, e.g. milvusdb/milvus:v2.6.15}"
IMAGE_NON_STRIP="${3:?Missing debug image, e.g. milvusdb/milvus:v2.6.15-debug}"

# Resolve to absolute path
COREDUMP="$(cd "$(dirname "$COREDUMP")" && pwd)/$(basename "$COREDUMP")"

if [ ! -f "$COREDUMP" ]; then
  echo "ERROR: coredump file not found: $COREDUMP"
  exit 1
fi

# Warn if stripped and debug images don't share the same base tag
# Expected: <image>:<tag> and <image>:<tag>-debug
STRIPPED_TAG="${IMAGE##*:}"
DEBUG_TAG="${IMAGE_NON_STRIP##*:}"
if [ "${DEBUG_TAG}" != "${STRIPPED_TAG}-debug" ]; then
  echo "WARNING: debug image tag '${DEBUG_TAG}' does not match '${STRIPPED_TAG}-debug'."
  echo "         Symbols may not align with the coredump addresses."
  echo ""
fi

echo "==> Pulling debug image: ${IMAGE_NON_STRIP}"
docker pull "${IMAGE_NON_STRIP}"

echo ""
echo "==> Launching GDB inside debug container..."
echo ""
echo "    Coredump:       ${COREDUMP}"
echo "    Stripped image: ${IMAGE}"
echo "    Debug image:    ${IMAGE_NON_STRIP}"
echo ""
echo "--- GDB will start. Useful commands: ---"
echo "    bt                        - backtrace of current thread"
echo "    thread apply all bt       - backtrace of all threads"
echo "    info threads              - list threads"
echo "    thread <n>                - switch to thread n"
echo "    info locals               - local variables"
echo "    print <var>               - inspect variable"
echo "    list                      - show source context"
echo "    info sharedlibrary        - check .so symbol status"
echo "----------------------------------------"
echo ""

docker run -it --rm \
  -v "${COREDUMP}:/tmp/core:ro" \
  --entrypoint "" \
  "${IMAGE_NON_STRIP}" \
  bash -c '
    # Locate the milvus binary
    MILVUS_BIN=$(find /milvus -name milvus -type f 2>/dev/null | head -1)
    if [ -z "$MILVUS_BIN" ]; then
      echo "ERROR: milvus binary not found in image"
      exit 1
    fi
    echo "Using binary: $MILVUS_BIN"

    # Install gdb if not present
    if ! command -v gdb &>/dev/null; then
      echo "Installing gdb..."
      apt-get update -qq && apt-get install -y -qq gdb >/dev/null 2>&1
    fi

    # Set library paths so GDB can resolve all .so symbols
    export LD_LIBRARY_PATH="/milvus/lib:${LD_LIBRARY_PATH:-}"

    # Launch GDB
    #   - solib-search-path: tells GDB where to find unstripped .so files
    #   - auto-loads symbols for milvus binary + all shared libraries
    gdb "$MILVUS_BIN" /tmp/core \
      -ex "set solib-search-path /milvus/lib" \
      -ex "set print pretty on" \
      -ex "bt"
  '
