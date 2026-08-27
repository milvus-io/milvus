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

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
output_dir=${1:-"$repo_root/bin/cmek-fixtures"}
cpp_build_dir="$repo_root/cmake_build/cmek-fixtures"
go_command=${GO:-go}
build_jobs=${jobs:-2}

mkdir -p "$output_dir" "$cpp_build_dir"
cd "$repo_root"

"$go_command" build \
    -buildmode=plugin \
    -buildvcs=false \
    -tags dynamic,test \
    -o "$output_dir/libGoCipherPlugin.so" \
    ./tests/integration/cmek/pluginmock/go

"$go_command" build \
    -race \
    -buildmode=plugin \
    -buildvcs=false \
    -tags dynamic,test \
    -gcflags "all=-N -l" \
    -o "$output_dir/libGoCipherPluginRace.so" \
    ./tests/integration/cmek/pluginmock/go

cmake \
    -S "$repo_root/tests/integration/cmek/pluginmock/cpp" \
    -B "$cpp_build_dir" \
    -DMILVUS_SOURCE_DIR="$repo_root" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_LIBRARY_OUTPUT_DIRECTORY="$output_dir"
cmake --build "$cpp_build_dir" --parallel "$build_jobs"
"$cpp_build_dir/CipherPluginTest"

for artifact in \
    "$output_dir/libGoCipherPlugin.so" \
    "$output_dir/libGoCipherPluginRace.so" \
    "$output_dir/libCipherPlugin.so"; do
    if [ ! -s "$artifact" ]; then
        echo "CMEK fixture build did not produce $artifact" >&2
        exit 1
    fi
done
