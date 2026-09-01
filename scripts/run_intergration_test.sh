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

# run integration test
echo "Running integration test under ./tests/integration"

FILE_COVERAGE_INFO="it_coverage.txt"
BASEDIR=$(dirname "$0")
source $BASEDIR/setenv.sh

set -e

echo "mode: atomic" > ${FILE_COVERAGE_INFO}
echo "MILVUS_WORK_DIR: $MILVUS_WORK_DIR"
export MILVUS_INTEGRATION_CASE_TIMEOUT="${MILVUS_INTEGRATION_CASE_TIMEOUT:-20m}"

INTEGRATION_PACKAGE=""
if [ "${1:-}" = "--package" ]; then
    if [ -z "${2:-}" ]; then
        echo "--package requires a Go package pattern" >&2
        exit 2
    fi
    INTEGRATION_PACKAGE="$2"
    shift 2
fi
if [ "$#" -eq 0 ]; then
    TEST_CMD=(go test)
elif [ "$#" -eq 1 ]; then
    read -r -a TEST_CMD <<< "$1"
else
    TEST_CMD=("$@")
fi

TEST_ARGS=(
    "-gcflags=all=-N -l"
    -race
    -tags dynamic,test
    -v
    -failfast
    -count=1
    -buildvcs=false
    -coverpkg=./...
    -coverprofile=profile.out
    -covermode=atomic
    -caseTimeout=20m 
    -timeout=60m
)

function test_cmd() {
    local package_pattern="${INTEGRATION_PACKAGE:-./...}"
    local package_output
    if ! package_output=$(go list -tags dynamic,test "$package_pattern"); then
        return 1
    fi
    if [ -z "$package_output" ]; then
        echo "go list returned no packages for $package_pattern" >&2
        return 1
    fi
    mapfile -t PKGS <<< "$package_output"
    for pkg in "${PKGS[@]}"; do
        echo -e "-----------------------------------\nRunning test cases at $pkg ..." 
        "${TEST_CMD[@]}" "$pkg" "${TEST_ARGS[@]}"
        if [ -f profile.out ]; then
            # Skip the per-profile header to keep a single global "mode:" line
            # Skip the packages that are not covered by the test
            sed '1{/^mode:/d}' profile.out | grep -vE '(planparserv2/generated|mocks)' >> "${FILE_COVERAGE_INFO}" || [ $? -eq 1 ] 
            rm profile.out
        fi
        echo -e "-----------------------------------\n"
    done
}

beginTime=`date +%s`
printf "=== Running integration test ===\n\n"

for d in tests/integration; do
    pushd "$d"
    test_cmd 
    popd
done

endTime=`date +%s`
printf "=== Total time for go integration test: $(($endTime-$beginTime)) s==="
