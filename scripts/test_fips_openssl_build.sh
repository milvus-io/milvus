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

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
unset GOFIPS140 MILVUS_FIPS_ENABLED MAKEFLAGS MFLAGS USE_ASAN USE_TSAN WITH_GPU

# Inspect the tags actually passed to the Go compiler with caller-supplied tags.
plan=$(make -n build-go MILVUS_FIPS_ENABLED=ON 'MILVUS_GO_BUILD_TAGS=dynamic custom')
tags=$(sed -n 's/.*-tags "\([^"]*\)".*/\1/p' <<<"$plan")
if [[ "$tags" != dynamic,custom,milvus_fips ]]; then
    echo "Unexpected FIPS build tags '$tags'" >&2
    exit 1
fi

if result=$(make -n rpm MILVUS_FIPS_ENABLED=ON 2>&1); then
    echo "FIPS RPM unexpectedly accepted" >&2
    exit 1
fi
[[ "$result" == *"not supported by make rpm"* ]]

plan=$(make -n install MILVUS_FIPS_ENABLED=ON)
[[ "$plan" == *"/scripts/build_fips_provider.sh"* ]]
plan=$(make -n install)
[[ "$plan" != *"/scripts/build_fips_provider.sh"* ]]

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cd "$scratch"
conan=internal/core/output/lib/ossl-modules
provider=cmake_build/fips_provider/artifacts
module_config=fipsmodule-validated.cnf
wheels=cmake_build/runtime/pyudf/wheels
mkdir -p bin configs/ssl fake-bin "$conan" "$provider" "$wheels"
touch bin/milvus "$wheels/milvus_pyudf_runtime-fixture.whl"
printf '%s\n' conan >"$conan/fips.so"
printf '%s\n' legacy >"$conan/legacy.so"
printf '%s\n' validated >"$provider/fips.so"
printf '%s\n' pedantic >"$provider/$module_config"
printf '#!/bin/sh\nexit 0\n' >fake-bin/ldd
printf '#!/bin/sh\necho "$1" >> "$OPENSSL_TEST_LOG"\n' >fake-bin/openssl
chmod +x fake-bin/{ldd,openssl}
export PATH="$scratch/fake-bin:$PATH" OPENSSL_TEST_LOG="$scratch/openssl.log"
export GOPATH="$scratch/gopath" LIBRARY_PATH="$scratch/lib"

test_image_install() {
    local expected=$1 expected_status=${2:-0} image command status
    for image in ubuntu22.04 gpu/ubuntu22.04 amazonlinux2023 rockylinux9; do
        # Execute the actual Docker RUN command against the staged fixture.
        command=$(awk '/^RUN / {
            cmd=substr($0, 5)
            while (sub(/\\$/, "", cmd) && getline) cmd=cmd $0
            if (index(cmd, "openssl fipsinstall")) print cmd
        }' "$repo/build/docker/milvus/$image/Dockerfile")
        [[ -n "$command" ]] || { echo "Missing provider install in $image" >&2; exit 1; }
        : >"$OPENSSL_TEST_LOG"
        status=0
        sh -ec "${command//\/milvus\//.\/}" || status=$?
        [[ "$status" == "$expected_status" ]] || { echo "Unexpected fipsinstall status $status in $image" >&2; exit 1; }
        [[ "$(cat "$OPENSSL_TEST_LOG")" == "$expected" ]] || { echo "Unexpected fipsinstall in $image" >&2; exit 1; }
    done
}

# Reuse one directory to catch stale provider files when switching modes.
bash "$repo/scripts/install_milvus.sh"
for artifact in fips.so "$module_config"; do
    mv "$provider/$artifact" "$provider/$artifact.saved"
    if MILVUS_FIPS_ENABLED=ON bash "$repo/scripts/install_milvus.sh" >/dev/null 2>&1; then
        echo "FIPS install accepted a missing $artifact" >&2
        exit 1
    fi
    cmp "$conan/fips.so" lib/ossl-modules/fips.so
    mv "$provider/$artifact.saved" "$provider/$artifact"
done
MILVUS_FIPS_ENABLED=ON bash "$repo/scripts/install_milvus.sh"
cmp "$provider/fips.so" lib/ossl-modules/fips.so
cmp "$provider/$module_config" "configs/ssl/$module_config"
test ! -e lib/ossl-modules/legacy.so
test_image_install ""
mv lib/ossl-modules/fips.so lib/ossl-modules/fips.so.saved
test_image_install "" 1
mv lib/ossl-modules/fips.so.saved lib/ossl-modules/fips.so

MILVUS_FIPS_ENABLED=OFF bash "$repo/scripts/install_milvus.sh"
cmp "$conan/fips.so" lib/ossl-modules/fips.so
cmp "$conan/legacy.so" lib/ossl-modules/legacy.so
test ! -e "configs/ssl/$module_config"
test_image_install fipsinstall

MILVUS_FIPS_ENABLED=ON bash "$repo/scripts/install_milvus.sh"
rm -rf -- "$conan"
MILVUS_FIPS_ENABLED=OFF bash "$repo/scripts/install_milvus.sh"
test ! -e lib/ossl-modules/fips.so
test ! -e "configs/ssl/$module_config"
