#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/remote_env.sh"

MANIFEST_PATH="${MILVUS_RS_STANDALONE_SUBSET_MANIFEST:-${SCRIPT_DIR}/standalone_test_subset.json}"
ARTIFACT_ROOT="${MILVUS_RS_STANDALONE_SUBSET_ROOT}"
ENDPOINT="${MILVUS_RS_STANDALONE_ENDPOINT}"
TOKEN="${MILVUS_RS_STANDALONE_TOKEN}"
MINIO_HOST="${MILVUS_RS_STANDALONE_MINIO_HOST}"
TIMEOUT="${MILVUS_RS_SUBSET_TEST_TIMEOUT}"
CASE_PATTERN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --manifest)
            MANIFEST_PATH="$2"
            shift 2
            ;;
        --artifact-root)
            ARTIFACT_ROOT="$2"
            shift 2
            ;;
        --endpoint)
            ENDPOINT="$2"
            shift 2
            ;;
        --token)
            TOKEN="$2"
            shift 2
            ;;
        --minio-host)
            MINIO_HOST="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --case-pattern)
            CASE_PATTERN="$2"
            shift 2
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

cmd=(
    python3 "${SCRIPT_DIR}/standalone_subset_lib.py" run-suite
    --manifest "${MANIFEST_PATH}"
    --root-dir "${ROOT_DIR}"
    --artifact-root "${ARTIFACT_ROOT}"
    --endpoint "${ENDPOINT}"
    --token "${TOKEN}"
    --minio-host "${MINIO_HOST}"
    --timeout "${TIMEOUT}"
    --suite python_client
)

if [[ -n "${CASE_PATTERN}" ]]; then
    cmd+=(--case-pattern "${CASE_PATTERN}")
fi

"${cmd[@]}"
