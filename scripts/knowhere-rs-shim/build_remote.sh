#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -P "${SCRIPT_DIR}/../.." && pwd)"

mode="${1:-thirdparty}"
if [[ "${mode}" != "thirdparty" && "${mode}" != "core" ]]; then
    echo "Usage: $0 [thirdparty|core]"
    exit 1
fi

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/remote_env.sh"

cd "${ROOT_DIR}"

bash scripts/3rdparty_build.sh -o OFF -t Release

if [[ "${mode}" == "core" ]]; then
    bash scripts/core_build.sh -t Release
fi
