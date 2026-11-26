#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd "$(dirname "$0")/.."
unset GOFIPS140 MILVUS_FIPS_ENABLED USE_ASAN USE_TSAN WITH_GPU

# Read the environment of a Make recipe, as seen by the Go toolchain.
build_settings() {
    make -s -f Makefile -f - "$@" show-fips-build-settings <<'MAKE'
show-fips-build-settings:
	@echo "$$GOFIPS140|$(MILVUS_GO_BUILD_TAGS)"
MAKE
}

check_settings() {
    local expected=$1 actual
    shift
    actual=$(build_settings "$@")
    if [[ "$actual" != "$expected" ]]; then
        echo "Expected '$expected', got '$actual' for: $*" >&2
        exit 1
    fi
}

default_tags=dynamic,sonic,with_jemalloc,bytedance_tango
check_settings "|$default_tags"
check_settings "v1.0.0|$default_tags,milvus_fips" MILVUS_FIPS_ENABLED=ON
check_settings "v1.0.0|dynamic,custom,use_asan,cuda,milvus_fips" \
    MILVUS_FIPS_ENABLED=ON GOFIPS140=inprocess MILVUS_GO_BUILD_TAGS=dynamic,custom USE_ASAN=ON WITH_GPU=ON

check_settings "v1.0.0|dynamic,custom,milvus_fips" \
    MILVUS_FIPS_ENABLED=ON 'MILVUS_GO_BUILD_TAGS=dynamic   custom milvus_fips'

# Embedded quotes change how Go parses the entire list, including the required tag.
for tags in "'dynamic' 'custom'" '"dynamic" "custom"'; do
    if result=$(build_settings MILVUS_FIPS_ENABLED=ON "MILVUS_GO_BUILD_TAGS=$tags" 2>&1); then
        echo "Expected a FIPS build with quoted tags to fail" >&2
        exit 1
    fi
    [[ "$result" == *"must not contain literal quote characters"* ]]
done

# Verify that the Go tool selects the guarded implementation with the final flags.
tags=$(build_settings MILVUS_FIPS_ENABLED=ON 'MILVUS_GO_BUILD_TAGS=dynamic custom')
selected=$(cd pkg && CGO_ENABLED=1 go list -tags "${tags#*|}" -f '{{join .CgoFiles " "}}' ./util/fips)
[[ "$selected" == openssl_enabled.go ]]

# External builds query the same selector used in the server recipe.
module=$(make -s --no-print-directory MILVUS_FIPS_ENABLED=ON GOFIPS140=inprocess print-fips-go-module)
[[ "$module" == v1.0.0 ]]
