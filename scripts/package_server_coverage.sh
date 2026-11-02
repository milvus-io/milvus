#!/usr/bin/env bash
# Package metadata and matching decoders from a completed server coverage build.
set -euo pipefail

source_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${1:?usage: package_server_coverage.sh OUTPUT_DIRECTORY}
mkdir -p "$output"
output=$(cd "$output" && pwd)
cd "$source_root"

if ! go version -m bin/milvus | grep -F -- '-cover=true' >/dev/null; then
    echo 'The server must be built with USE_SERVER_COVERAGE=ON' >&2
    exit 1
fi
if ! go version -m bin/milvus | grep -E 'build[[:space:]]+-tags=.*servercoverage' >/dev/null; then
    echo 'The server coverage build tag is missing' >&2
    exit 1
fi
if ! nm -D lib/libmilvus_core.so | grep -F 'MilvusDumpCoverage' >/dev/null; then
    echo 'libmilvus_core.so does not contain the server coverage hook' >&2
    exit 1
fi
if [[ -e "$output/coverage" ]]; then
    echo 'Coverage metadata destination already exists; use a fresh output directory' >&2
    exit 1
fi
mkdir -p "$output/coverage/gcno" "$output/bin"
count=0
while IFS= read -r -d '' file; do
    destination="$output/coverage/gcno$file"
    mkdir -p "$(dirname "$destination")"
    cp "$file" "$destination"
    count=$((count + 1))
done < <(find "$source_root/cmake_build" -type f -name '*.gcno' -print0)
if [[ "$count" == 0 ]]; then
    echo 'The C++ build contains no coverage metadata (.gcno files)' >&2
    exit 1
fi
compiler=$(sed -n 's/^CMAKE_CXX_COMPILER:[^=]*=//p' cmake_build/CMakeCache.txt)
if [[ -z "$compiler" ]]; then
    echo 'Cannot determine the C++ compiler from CMakeCache.txt' >&2
    exit 1
fi
gcov_tool=$("$compiler" -print-prog-name=gcov)
gcov_tool=$(command -v "$gcov_tool")
if ! "$gcov_tool" --version | grep -F "$("$compiler" -dumpfullversion)" >/dev/null; then
    echo 'gcov must match the C++ compiler version' >&2
    exit 1
fi
cp "$gcov_tool" "$output/bin/coverage-gcov"
# Go 1.25+ builds some tools on demand. Use the selected server toolchain.
GOFLAGS= CGO_ENABLED=0 go build -o "$output/bin/coverage-covdata" cmd/covdata
cp scripts/export_server_coverage.sh "$output/bin/coverage-export"
chmod +x "$output/bin/coverage-export"
{
    printf 'commit=%s\n' "${GIT_COMMIT:-$(git rev-parse HEAD)}"
    printf 'source_root=%s\n' "$source_root"
    printf 'gcno_files=%s\n' "$count"
    printf 'working_tree_dirty=%s\n' "$(test -z "$(git status --porcelain)" && echo false || echo true)"
    go version
    "$compiler" --version
} > "$output/coverage/build-info.txt"
echo "Coverage metadata and tools written to $output" >&2
