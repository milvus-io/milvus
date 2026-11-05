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

# Rewrite a private source copy and reuse the normal build flags and native SDK.
# The caller owns OUTPUT_DIRECTORY for the entire lifetime of its subprocesses.
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${1:?usage: build_failpoint_fixture.sh OUTPUT_DIRECTORY}
mkdir -p "$output/bin"
output=$(cd "$output" && pwd)
source_root="$output/source"
rm -f "$output/failpoint-fixture"
rm -rf "$source_root"
mkdir -p "$source_root"
cd "$repo_root"
# Copy only build inputs, never rewrite the developer's checkout. Native build
# products are shared read-only instead of copying the multi-gigabyte SDK.
tar --exclude='internal/core/output' --exclude='internal/core/thirdparty' \
    -cf - go.mod go.sum Makefile cmd internal pkg client configs scripts | \
    tar -xf - -C "$source_root"
ln -s "$repo_root/internal/core/output" "$source_root/internal/core/output"
ln -s "$repo_root/cmake_build" "$source_root/cmake_build"
ln -sfn "$repo_root/configs" "$output/configs"
# Resolve the tool from go.mod so rewriting and the runtime always match.
cd "$source_root"
failpoint_version=$(go list -m -f '{{.Version}}' github.com/pingcap/failpoint)
GOBIN="$output/bin" go install "github.com/pingcap/failpoint/failpoint-ctl@$failpoint_version"
"$output/bin/failpoint-ctl" enable \
    internal/streamingnode/server/wal/adaptor \
    internal/streamingnode/server/wal/interceptors/idempotency
make build-go INSTALL_PATH="$output/bin" GIT_COMMIT="$(git -C "$repo_root" rev-parse --short HEAD)"
test -s "$output/bin/milvus"
printf '%s\n' 'pingcap-instrumented' > "$output/failpoint-fixture"
