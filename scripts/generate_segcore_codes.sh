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
#   2. The pinned milvus-common in the conan cache (ref from internal/core/conanfile.py).
#   3. The core build output header (internal/core/output/include/common/EasyAssert.h).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

header="${MILVUS_COMMON_HEADER:-}"

find_header_in_cache() {
  # Resolve the header from THIS ref's binary package folder (not the recipe
  # folder, and never another cached version): list the ref's package ids,
  # then ask for that package's cache path.
  local ref="$1" pid pkg_dir
  pid="$(conan list "$ref:*" 2>/dev/null | grep -oE '\b[a-f0-9]{40}\b' | head -1 || true)"
  [[ -n "$pid" ]] || return 0
  pkg_dir="$(conan cache path "$ref:$pid" 2>/dev/null || true)"
  [[ -n "$pkg_dir" ]] || return 0
  find "$pkg_dir" -path '*/common/EasyAssert.h' 2>/dev/null | head -1 || true
}

if [[ -z "$header" ]]; then
  ref="$(grep -oE 'milvus-common/[0-9][^"#]*' "$ROOT/internal/core/conanfile.py" | head -1 || true)"
  if [[ -n "$ref" ]]; then
    base="$(conan cache path "$ref" 2>/dev/null || true)"
    if [[ -n "$base" ]]; then
      header="$(find "$base" -path '*/common/EasyAssert.h' 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$header" || ! -f "$header" ]]; then
      header="$(find_header_in_cache "$ref")"
    fi
    # Cold conan cache (e.g. the CI code-checker job right after a pin bump,
    # when the conanfile-hash cache key misses): fetch the pinned package from
    # the configured remotes so the drift gate can still resolve the header.
    if [[ -z "$header" || ! -f "$header" ]]; then
      while read -r remote_name; do
        [[ -n "$remote_name" ]] || continue
        echo "segcoregen: conan cache miss for $ref, trying remote $remote_name ..." >&2
        if conan download "$ref" -r "$remote_name" >/dev/null 2>&1; then
          header="$(find_header_in_cache "$ref")"
          [[ -n "$header" && -f "$header" ]] && break
        fi
      done < <(conan remote list 2>/dev/null | sed -n 's/^\([^:]*\):.*/\1/p')
    fi
  fi
fi

if [[ -z "$header" || ! -f "$header" ]]; then
  header="$ROOT/internal/core/output/include/common/EasyAssert.h"
fi

if [[ ! -f "$header" ]]; then
  echo "segcoregen: cannot locate milvus-common EasyAssert.h." >&2
  echo "  Set MILVUS_COMMON_HEADER=/path/to/EasyAssert.h, or build core first," >&2
  echo "  or ensure the pinned milvus-common is in the conan cache." >&2
  exit 1
fi

header="$(cd "$(dirname "$header")" && pwd)/$(basename "$header")"
echo "segcoregen: using header $header"

cd "$ROOT/pkg"
go run ./util/merr/internal/segcoregen/main.go -header "$header" -out util/merr/segcore_codes_gen.go
gofmt -w util/merr/segcore_codes_gen.go
echo "segcoregen: wrote pkg/util/merr/segcore_codes_gen.go"
