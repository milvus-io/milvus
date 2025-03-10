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

TEST_CMD=$@
if [ -z "$TEST_CMD" ]; then
   TEST_CMD="go test" 
fi

set -e
echo "mode: atomic" > ${FILE_COVERAGE_INFO}

# starting the timer
beginTime=`date +%s`

for d in $(go list ./tests/integration/...); do
    echo "Start to run integration test under \"$d\" pkg"
    if [[ $d == *"coordrecovery"* ]]; then
        echo "running coordrecovery"
        # simplified command to speed up coord init test since it is large.
        $TEST_CMD -tags dynamic,test -v -coverprofile=profile.out -covermode=atomic "$d" -caseTimeout=20m -timeout=30m
    elif [[ $d == *"import"* ]]; then
        go test -tags dynamic,test -v -coverprofile=profile.out -covermode=atomic "$d" -caseTimeout=20m -timeout=60m
    else
        $TEST_CMD -race -tags dynamic,test -v -coverpkg=./... -coverprofile=profile.out -covermode=atomic "$d" -caseTimeout=15m -timeout=30m
    fi
    if [ -f profile.out ]; then
        grep -v kafka profile.out | grep -v planparserv2/generated | grep -v mocks | sed '1d' >> ${FILE_COVERAGE_INFO}
        rm profile.out
    fi
done

endTime=`date +%s`

echo "Total time for go integration test:" $(($endTime-$beginTime)) "s"

