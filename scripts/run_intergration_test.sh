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
    -count=1
    -buildvcs=false
    -coverpkg=./...
    -coverprofile=profile.out
    -covermode=atomic
    -caseTimeout=20m 
    -timeout=60m
)

function test_cmd() {
    mapfile -t PKGS < <(go list -tags dynamic,test ./...)
    for pkg in "${PKGS[@]}"; do
        beginTime2=$(date +%s)
        echo -e "-----------------------------------\nRunning test cases at $pkg ..." 
        "${TEST_CMD_WITH_ARGS[@]}" "$pkg"
        if [ -f profile.out ]; then
            # Skip the per-profile header to keep a single global "mode:" line
            sed '1{/^mode:/d}' profile.out >> "${FILE_COVERAGE_INFO}"
            rm profile.out
        fi
        endTime2=$(date +%s)
        echo -e "-----------------------------------\n"
    done
}

beginTime=`date +%s`
printf "=== Running integration test ===\n\n"

for d in tests/integration; do
    pushd $d 
    test_cmd 
    popd
done

endTime=`date +%s`
printf "=== Total time for go integration test: $(($endTime-$beginTime)) s==="
