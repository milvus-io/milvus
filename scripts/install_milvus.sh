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
cp $PWD"/internal/core/output/lib/"*.dylib* "$LIBRARY_PATH" 2>/dev/null || true
cp $PWD"/internal/core/output/lib/"*.so* "$LIBRARY_PATH" || true
cp $PWD"/internal/core/output/lib64/"*.so* "$LIBRARY_PATH" 2>/dev/null || true

if [ "$USE_ASAN" == "ON" ]; then
    for LIB_PATH in $(ldconfig -p | grep -E '(asan|atomic)' | awk '{print $NF}'); do
        cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null
    done
fi
