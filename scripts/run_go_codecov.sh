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

FILE_COVERAGE_INFO="$PWD/go_coverage.txt"
FILE_COVERAGE_HTML="$PWD/go_coverage.html"


BASEDIR=$(dirname "$0")
source $BASEDIR/setenv.sh

set -e

echo "mode: atomic" > ${FILE_COVERAGE_INFO}

TEST_CMD=$@
if [ -z "$TEST_CMD" ]; then
   TEST_CMD="go test" 
fi

TEST_CMD_WITH_ARGS=(
    $TEST_CMD
    "-gcflags=all=-N -l"
    -race
    -tags dynamic,test
    -v
    -failfast
    -buildvcs=false
    -coverpkg=./...
    -coverprofile=profile.out
    -covermode=atomic
)

function test_cmd() {
    mapfile -t PKGS < <(go list -tags dynamic,test ./...)
    for pkg in "${PKGS[@]}"; do
        echo -e "-----------------------------------\nRunning test cases at $pkg ..." 
        "${TEST_CMD_WITH_ARGS[@]}" "$pkg"
        if [ -f profile.out ]; then
            # Skip the per-profile header to keep a single global "mode:" line
            # Skip the packages that are not covered by the test
            sed '1{/^mode:/d}' profile.out | grep -vE '(planparserv2/generated|mocks)' >> "${FILE_COVERAGE_INFO}" || [ $? -eq 1 ] 
            rm profile.out
        fi
        echo -e "-----------------------------------\n"
    done
}

export MILVUS_UT_WITHOUT_KAFKA=1 # kafka is not available in the CI environment, so skip the kafka tests

# starting the timer
beginTime=$(date +%s)
echo -e "=== Running go unittest ===\n\n"

for d in cmd/tools internal pkg client; do
    pushd "$d" 
    test_cmd 
    popd
done

endTime=$(date +%s)
echo -e "=== Total time for go unittest: $(($endTime-$beginTime)) s ==="

# generate html report
# go tool cover -html=./${FILE_COVERAGE_INFO} -o ./${FILE_COVERAGE_HTML}
# echo "Generate go coverage report to ${FILE_COVERAGE_HTML}"
