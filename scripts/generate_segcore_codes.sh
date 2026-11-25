#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Regenerate pkg/util/merr/segcore_codes_gen.go from milvus-common's
# enum ErrorCode (the single source of truth for segcore error codes).
#
# Source resolution (first hit wins):
#   1. $MILVUS_COMMON_HEADER (explicit path to EasyAssert.h).
#   2. The milvus-common sources fetched by a core build (FetchContent, under
#      internal/core build directories), accepted only when the checked-out
#      commit matches the pin.
#   3. A shallow clone of the pinned milvus-common commit (covers CI gates such
#      as code-checker, which run before any core build exists).
#   4. The core build output header (internal/core/output/include/common/EasyAssert.h).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRAPPER="$ROOT/internal/core/thirdparty/milvus-common/CMakeLists.txt"

header="${MILVUS_COMMON_HEADER:-}"

pin="$(grep -oE 'MILVUS-COMMON-VERSION[[:space:]]+[0-9a-fA-F]{7,40}' "$WRAPPER" \
    | head -1 | awk '{print $2}')"
pin="${pin//[[:space:]]/}"
repo="$(grep -oE 'GIT_REPOSITORY[[:space:]]+"https://[^"]+"' "$WRAPPER" \
    | sed 's/.*"\(https[^"]*\)"/\1/' | head -1)"

# Trust a FetchContent build tree only when it is checked out at exactly the
# pinned commit; a tree left over from an older pin would silently shift the
# drift gate to a different milvus-common version.
find_build_tree_header() {
  local pin="$1" src_dir
  while IFS= read -r src_dir; do
    if [[ "$(git -C "$src_dir" rev-parse HEAD 2>/dev/null || true)" == "$pin" ]]; then
      printf '%s\n' "$src_dir/include/common/EasyAssert.h"
      return 0
    fi
  done < <(find "$ROOT/internal/core" "$ROOT/cmake_build" -maxdepth 4 -type d \
      -name 'milvus-common-src' 2>/dev/null)
}

# Shallow-clone the pinned commit so gates that run before a core build (e.g.
# the code-checker job) still read exactly the pinned revision.
fetch_pinned_header() {
  local pin="$1" repo="$2" dir
  [[ -n "$pin" && -n "$repo" ]] || return 0
  dir="${TMPDIR:-/tmp}/milvus-common-segcoregen/$pin"
  if [[ ! -f "$dir/include/common/EasyAssert.h" ]]; then
    rm -rf "$dir"
    mkdir -p "$dir"
    git init -q "$dir" || return 0
    git -C "$dir" remote add origin "$repo" || return 0
    git -C "$dir" fetch -q --depth 1 origin "$pin" || { rm -rf "$dir"; return 0; }
    git -C "$dir" checkout -q FETCH_HEAD || { rm -rf "$dir"; return 0; }
  fi
  printf '%s\n' "$dir/include/common/EasyAssert.h"
}

if [[ -z "$header" && -n "$pin" ]]; then
  header="$(find_build_tree_header "$pin")"
fi

if [[ -z "$header" ]]; then
  header="$(fetch_pinned_header "$pin" "$repo")"
fi

if [[ -z "$header" || ! -f "$header" ]]; then
  header="$ROOT/internal/core/output/include/common/EasyAssert.h"
fi

if [[ ! -f "$header" ]]; then
  echo "segcoregen: cannot locate milvus-common EasyAssert.h." >&2
  echo "  Set MILVUS_COMMON_HEADER=/path/to/EasyAssert.h, or build core first," >&2
  echo "  or check network access for cloning the pinned milvus-common commit." >&2
  exit 1
fi

header="$(cd "$(dirname "$header")" && pwd)/$(basename "$header")"
echo "segcoregen: using header $header"

cd "$ROOT/pkg"
go run ./util/merr/internal/segcoregen/main.go -header "$header" -out util/merr/segcore_codes_gen.go
gofmt -w util/merr/segcore_codes_gen.go
echo "segcoregen: wrote pkg/util/merr/segcore_codes_gen.go"
