#!/usr/bin/env bash
# Exercise exporter protocol failures without requiring a Milvus build.
set -euo pipefail
source_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/metadata/gcno/build"
cp "$source_root/scripts/export_server_coverage.sh" "$work/bin/coverage-export"
printf 'commit=test\n' > "$work/metadata/build-info.txt"
touch "$work/metadata/gcno/build/core.gcno"
cat > "$work/bin/coverage-covdata" <<'TOOL'
#!/usr/bin/env bash
set -eu
if [[ -n "${FAIL_COVDATA:-}" ]]; then exit 1; fi
for arg in "$@"; do
    case "$arg" in -o=*) printf 'mode: atomic\n' > "${arg#-o=}" ;; esac
done
TOOL
cat > "$work/bin/coverage-gcov" <<'TOOL'
#!/usr/bin/env bash
set -eu
printf 'gcov fixture\n' > core.gcov.json.gz
TOOL
chmod +x "$work/bin/"*

new_process() {
    export MILVUS_COVERAGE_DIR="$work/$1"
    mkdir -p "$MILVUS_COVERAGE_DIR/requests" "$MILVUS_COVERAGE_DIR/runs"
    printf '%s' "$$" > "$MILVUS_COVERAGE_DIR/ready"
}

respond() {
    local id=$1
    local mode=$2
    local run="$MILVUS_COVERAGE_DIR/runs/$id"
    for ((i=0; i<200; i++)); do
        if [[ -f "$MILVUS_COVERAGE_DIR/requests/$id" ]]; then
            mkdir -p "$run/go" "$run/cpp/build"
            if [[ "$mode" == error ]]; then
                printf 'native dump failed\n' > "$run/error"
            else
                printf 'counters\n' > "$run/cpp/build/core.gcda"
                printf '{}\n' > "$run/done.json"
            fi
            return
        fi
        sleep 0.02
    done
    echo 'Exporter did not issue a snapshot request' >&2
    return 1
}

id=0123456789abcdef0123456789abcdef
new_process success
respond "$id" success & responder=$!
"$work/bin/coverage-export" "$id" "$work/metadata" > "$work/success.tar.gz"
wait "$responder"
mkdir "$work/unpacked"
tar -xzf "$work/success.tar.gz" -C "$work/unpacked"
test -f "$work/unpacked/go.out"
test -f "$work/unpacked/cpp/build/core.gcda"
test -f "$work/unpacked/cpp/build/core.gcov.json.gz"
test -f "$MILVUS_COVERAGE_DIR/runs/$id/done.json"
if "$work/bin/coverage-export" "$id" "$work/metadata" > "$work/duplicate.out" 2> "$work/error"; then
    echo 'Accepted a reused snapshot ID' >&2; exit 1
fi

for scenario in error missing_metadata decoder_failure; do
    new_process "$scenario"
    metadata="$work/metadata"
    if [[ "$scenario" == missing_metadata ]]; then
        metadata="$work/wrong-metadata"
        mkdir -p "$metadata/gcno"
        printf 'commit=wrong\n' > "$metadata/build-info.txt"
    fi
    export FAIL_COVDATA=
    if [[ "$scenario" == decoder_failure ]]; then export FAIL_COVDATA=1; fi
    respond "$id" "$scenario" & responder=$!
    if "$work/bin/coverage-export" "$id" "$metadata" > "$work/$scenario.out" 2> "$work/error"; then
        echo "Accepted $scenario" >&2; exit 1
    fi
    wait "$responder"
    test ! -s "$work/$scenario.out"
    test -d "$MILVUS_COVERAGE_DIR/runs/$id"
done

if "$work/bin/coverage-export" ../escape "$work/metadata" > "$work/invalid.out" 2> "$work/error"; then
    echo 'Accepted an invalid snapshot ID' >&2; exit 1
fi
test ! -s "$work/invalid.out"
echo 'Server coverage exporter protocol tests passed'
