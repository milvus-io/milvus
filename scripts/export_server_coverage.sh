#!/usr/bin/env bash
# Run beside the server. stdout is exclusively the resulting coverage archive.
set -euo pipefail

root=${MILVUS_COVERAGE_DIR:?MILVUS_COVERAGE_DIR is required}
id=${1:?usage: coverage-export SNAPSHOT_ID [METADATA_DIRECTORY]}
metadata=${2:-$root}
tools=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ ! "$id" =~ ^[0-9a-f]{32}$ ]]; then
    echo 'Snapshot ID must contain 32 lowercase hexadecimal characters' >&2
    exit 1
fi
if [[ ! -s "$root/ready" ]]; then
    echo 'The coverage runtime is not ready' >&2
    exit 1
fi
pid=$(cat "$root/ready")
if [[ ! "$pid" =~ ^[0-9]+$ ]] || ! kill -0 "$pid" 2>/dev/null; then
    echo 'The recorded coverage process is no longer running' >&2
    exit 1
fi
run="$root/runs/$id"
if [[ -e "$run" ]] || ! (set -o noclobber; : > "$root/requests/$id") 2>/dev/null; then
    echo 'Snapshot ID has already been used' >&2
    exit 1
fi
for ((attempt=0; attempt<240; attempt++)); do
    if [[ -f "$run/error" ]]; then
        cat "$run/error" >&2
        exit 1
    fi
    [[ ! -f "$run/done.json" ]] || break
    sleep 0.5
done
if [[ ! -f "$run/done.json" ]]; then
    echo 'Timed out waiting for the server coverage snapshot' >&2
    exit 1
fi
if [[ ! -s "$metadata/build-info.txt" || ! -d "$metadata/gcno" ]]; then
    echo 'Build metadata is missing; package it from the same build as the server' >&2
    exit 1
fi
env -u LD_PRELOAD "$tools/coverage-covdata" textfmt -i="$run/go" -o="$run/go.out"
cp -a "$metadata/gcno/." "$run/cpp/"
cp "$metadata/build-info.txt" "$run/build-info.txt"
while IFS= read -r -d '' notes; do
    (
        cd "$(dirname "$notes")"
        env -u LD_PRELOAD "$tools/coverage-gcov" --json-format "$(basename "$notes")" >&2
    )
done < <(find "$run/cpp" -type f -name '*.gcno' -print0)
tar -C "$run" -czf - .
# Keep the snapshot for retrying transfers and auditing. The caller owns cleanup.
