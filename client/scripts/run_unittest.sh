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

FILE_COVERAGE_INFO="go_coverage.txt"
FILE_COVERAGE_HTML="go_coverage.html"

BASEDIR=$(dirname "$0")

set -ex
echo "mode: atomic" > ${FILE_COVERAGE_INFO}

# run unittest
echo "Running unittest"

for d in $(go list ./... ); do
    go test -race -tags dynamic -v -coverpkg=./... -coverprofile=profile.out -covermode=atomic "$d"
    if [ -f profile.out ]; then
        cat profile.out | sed '1d' >> ${FILE_COVERAGE_INFO}
        rm profile.out
    fi
done