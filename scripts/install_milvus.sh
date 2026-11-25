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
shopt -s nullglob
svs_files=()
for lib_dir in "$PWD/internal/core/output/lib" "$PWD/internal/core/output/lib64"; do
    libraries=()
    for library in "$lib_dir"/*.so*; do
        if [[ ${library##*/} != libsvs_runtime.so* ]]; then
            libraries+=("$library")
        fi
    done
    if (( ${#libraries[@]} )); then
        cp -a "${libraries[@]}" "$LIBRARY_PATH/"
    fi
    # A cached native installation may be staged without its CMake build tree.
    if [[ -f "$lib_dir/libsvs_runtime.so" ]]; then
        svs_files=("$lib_dir"/libsvs_runtime.so* "$lib_dir/svs-notices.txt")
    fi
done
# In reused build trees, the install prefix may still contain a disabled or older SVS.
manifest="$PWD/cmake_build/install_manifest.txt"
if [[ -f "$manifest" ]]; then
    svs_files=()
    while IFS= read -r path || [[ -n "$path" ]]; do
        case "${path##*/}" in
            libsvs_runtime.so*|svs-notices.txt) svs_files+=("$path") ;;
        esac
    done < "$manifest"
fi
rm -f -- "$LIBRARY_PATH"/libsvs_runtime.so* "$LIBRARY_PATH/svs-notices.txt"
if (( ${#svs_files[@]} )); then
    cp -a "${svs_files[@]}" "$LIBRARY_PATH/"
    if [[ ! -f "$LIBRARY_PATH/svs-notices.txt" ]]; then
        echo "Missing SVS redistribution notices" >&2
        exit 1
    fi
fi
shopt -u nullglob
# The plan parser libraries exist for the C++ unit tests, which link them from
# internal/core/output/lib. Nothing at runtime loads them, so keep them out of
# the runtime directory, including copies left there by an older installer.
rm -f "$LIBRARY_PATH"/libmilvus-planparser*
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

for LIB_PATH in $(ldd ./bin/milvus | grep -E '(asan|tsan|atomic)' | awk '{print $3}'); do
    cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null
done

if [[ -f "$LIBRARY_PATH/milvus-archer" ]]; then
    python3 "$PWD/internal/core/milvus_tsan.py" verify "$LIBRARY_PATH"
    python3 "$PWD/internal/core/milvus_tsan.py" verify-runtime "$LIBRARY_PATH"
fi
