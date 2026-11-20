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

set -e

mkdir -p "$GOPATH/bin" && cp -f "$PWD/bin/milvus" "$GOPATH/bin/milvus"
mkdir -p "$LIBRARY_PATH"
# Preserve linker and SONAME aliases instead of duplicating their library payloads.
cp -a "$PWD"/internal/core/output/lib/*.dylib* "$LIBRARY_PATH" 2>/dev/null || true
cp -a "$PWD"/internal/core/output/lib/*.so* "$LIBRARY_PATH" || true
cp -a "$PWD"/internal/core/output/lib64/*.so* "$LIBRARY_PATH" 2>/dev/null || true
cp -r "$PWD/internal/core/output/lib/ossl-modules" "$LIBRARY_PATH" 2>/dev/null || true
for marker in "$PWD/internal/core/output/lib/milvus-sanitizer" "$PWD/internal/core/output/lib64/milvus-sanitizer"; do
    if [[ -f "$marker" ]]; then
        cp "$marker" "$LIBRARY_PATH/"
    fi
done
for metadata in milvus-archer tsan-dependencies.json tsan.supp; do
    if [[ -f "$PWD/internal/core/output/lib/$metadata" ]]; then
        cp "$PWD/internal/core/output/lib/$metadata" "$LIBRARY_PATH/"
    fi
done
if [[ -d "$PWD/internal/core/output/lib/tsan-symbolizer" ]]; then
    cp -r "$PWD/internal/core/output/lib/tsan-symbolizer" "$LIBRARY_PATH/"
    cp "$PWD/internal/core/output/lib/llvm-symbolizer" "$LIBRARY_PATH/"
fi

PYUDF_WHEEL_DIR="$PWD/cmake_build/runtime/pyudf/wheels"
PYUDF_RUNTIME_DIR="$LIBRARY_PATH/pyudf"
shopt -s nullglob
PYUDF_WHEELS=("$PYUDF_WHEEL_DIR"/milvus_pyudf_runtime-*.whl)
shopt -u nullglob
if [[ ${#PYUDF_WHEELS[@]} -ne 1 ]]; then
    echo "ERROR: expected exactly one PyUDF runtime wheel, found ${#PYUDF_WHEELS[@]}" >&2
    exit 1
fi
rm -rf "$PYUDF_RUNTIME_DIR"
mkdir -p "$PYUDF_RUNTIME_DIR"
cp "${PYUDF_WHEELS[0]}" "$PYUDF_RUNTIME_DIR/"

# Ship the linked GCC runtimes; the runtime image may have an older compiler.
if LINKED_LIBRARIES=$(ldd ./bin/milvus); then
    ASAN_RUNTIME=
    for LIB_PATH in $(printf '%s\n' "$LINKED_LIBRARIES" | grep -E '(asan|tsan|atomic|libstdc\+\+|libgcc_s|libgomp|libgfortran|libquadmath)' | awk '{print $3}'); do
        if [[ ! "$LIB_PATH" -ef "$LIBRARY_PATH/${LIB_PATH##*/}" ]]; then
            cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null
        fi
        case "${LIB_PATH##*/}" in libasan.so.*) ASAN_RUNTIME=${LIB_PATH##*/} ;; esac
    done
    # A compiler switch leaves old SONAMEs in lib; retain only the linked ASan.
    for LIB_PATH in "$LIBRARY_PATH"/libasan.so.*; do
        if [[ "${LIB_PATH##*/}" != "$ASAN_RUNTIME" ]]; then
            rm -f -- "$LIB_PATH"
        fi
    done
fi

if [[ -f "$LIBRARY_PATH/milvus-archer" ]]; then
    python3 "$PWD/internal/core/milvus_tsan.py" verify "$LIBRARY_PATH"
    python3 "$PWD/internal/core/milvus_tsan.py" verify-runtime "$LIBRARY_PATH"
fi
