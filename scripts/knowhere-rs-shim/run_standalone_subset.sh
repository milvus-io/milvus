#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/remote_env.sh"

ARTIFACT_ROOT="${MILVUS_RS_STANDALONE_SUBSET_ROOT}"
MANIFEST_PATH="${MILVUS_RS_STANDALONE_SUBSET_MANIFEST:-${SCRIPT_DIR}/standalone_test_subset.json}"
ENDPOINT="${MILVUS_RS_STANDALONE_ENDPOINT}"
TOKEN="${MILVUS_RS_STANDALONE_TOKEN}"
MINIO_HOST="${MILVUS_RS_STANDALONE_MINIO_HOST}"
TIMEOUT="${MILVUS_RS_SUBSET_TEST_TIMEOUT}"
HEALTH_URL="${MILVUS_RS_STANDALONE_HEALTH_URL}"
CASE_PATTERN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --artifact-root)
            ARTIFACT_ROOT="$2"
            shift 2
            ;;
        --manifest)
            MANIFEST_PATH="$2"
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
        --health-url)
            HEALTH_URL="$2"
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

mkdir -p "${ARTIFACT_ROOT}"

if ! curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
    echo "standalone is not healthy: ${HEALTH_URL}" >&2
    exit 1
fi

overall_status=0

go_args=(--artifact-root "${ARTIFACT_ROOT}" --manifest "${MANIFEST_PATH}" --endpoint "${ENDPOINT}" --token "${TOKEN}" --minio-host "${MINIO_HOST}" --timeout "${TIMEOUT}")
python_args=("${go_args[@]}")
rest_args=("${go_args[@]}")

if [[ -n "${CASE_PATTERN}" ]]; then
    go_args+=(--case-pattern "${CASE_PATTERN}")
    python_args+=(--case-pattern "${CASE_PATTERN}")
    rest_args+=(--case-pattern "${CASE_PATTERN}")
fi

"${SCRIPT_DIR}/run_go_client_subset.sh" "${go_args[@]}" || overall_status=1
"${SCRIPT_DIR}/run_python_client_subset.sh" "${python_args[@]}" || overall_status=1
"${SCRIPT_DIR}/run_restful_subset.sh" "${rest_args[@]}" || overall_status=1

summary_json="${ARTIFACT_ROOT}/summary.json"
summary_md="${ARTIFACT_ROOT}/summary.md"

python3 "${SCRIPT_DIR}/collect_subset_results.py" "${ARTIFACT_ROOT}" \
    --summary-json "${summary_json}" \
    --summary-md "${summary_md}"

echo "SUMMARY_JSON=${summary_json}"
echo "SUMMARY_MD=${summary_md}"
exit "${overall_status}"
