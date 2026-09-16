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

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT_DIR}/cmake_build/rust-tests}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
cd "${ROOT_DIR}/internal/core/thirdparty/tantivy/tantivy-binding"

# --lib compiles all library tests, catching stale call sites outside BM25.
# Use a separate, non-LTO test profile rather than relinking the Release graph.
# Run the self-contained BM25 suite: other suites download dictionaries and
# require writable /var/lib/milvus or /logs directories.
echo "Running Tantivy BM25 Rust unit tests"
exec cargo +1.89 test --locked --profile bm25-test --lib bm25_c::tests:: "$@"
