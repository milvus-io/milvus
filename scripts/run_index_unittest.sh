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

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
index_test_binary="${INDEX_TEST_BINARY:-${repo_root}/cmake_build/unittest/index_tests}"

if [[ ! -x "${index_test_binary}" ]]; then
    printf 'index_tests binary not found or not executable: %s\n' "${index_test_binary}" >&2
    exit 1
fi

# Prefer the matching build-tree or install-tree library over another checkout.
binary_dir="$(cd -- "$(dirname -- "${index_test_binary}")" && pwd)"
export LD_LIBRARY_PATH="${binary_dir}/../src:${binary_dir}/../lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

lsan_suppressions="${repo_root}/internal/core/lsan_suppressions.txt"
if [[ -f "${lsan_suppressions}" ]]; then
    export LSAN_OPTIONS="${LSAN_OPTIONS:+${LSAN_OPTIONS}:}suppressions=${lsan_suppressions}"
fi

exec "${index_test_binary}" "$@"
