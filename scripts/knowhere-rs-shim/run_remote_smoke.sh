#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Reuse the remote build/runtime environment so smoke execution sees the same
# library paths and writable state roots as standalone.
source "${SCRIPT_DIR}/remote_env.sh"

export MILVUS_URI="${MILVUS_URI:-http://127.0.0.1:19530}"

python3 "${SCRIPT_DIR}/smoke_hnsw.py"
