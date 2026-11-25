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
        svs_files=("$lib_dir"/libsvs_runtime.so*)
    fi
done
# In reused build trees, the install prefix may still contain a disabled or older SVS.
manifest="$PWD/cmake_build/install_manifest.txt"
if [[ -f "$manifest" ]]; then
    svs_files=()
    while IFS= read -r path || [[ -n "$path" ]]; do
        case "${path##*/}" in
            libsvs_runtime.so*) svs_files+=("$path") ;;
        esac
    done < "$manifest"
fi
rm -f -- "$LIBRARY_PATH"/libsvs_runtime.so*
if (( ${#svs_files[@]} )); then
    cp -a "${svs_files[@]}" "$LIBRARY_PATH/"
fi
shopt -u nullglob
cp -r "$PWD/internal/core/output/lib/ossl-modules" "$LIBRARY_PATH" 2>/dev/null || true

for LIB_PATH in $(ldd ./bin/milvus | grep -E '(asan|atomic)' | awk '{print $3}'); do
    cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null
done
