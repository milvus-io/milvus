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

set -ex
echo "mode: atomic" > ${FILE_COVERAGE_INFO}

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

export PKG_CONFIG_PATH="${PKG_CONFIG_PATH}:$ROOT_DIR/internal/core/output/lib/pkgconfig:$ROOT_DIR/internal/core/output/lib64/pkgconfig"
export RPATH=$(pkg-config --libs-only-L milvus_common | cut -c 3-)

# run unittest
echo "Running unittest under ./internal"
if [[ $(uname -s) == "Darwin" && "$(uname -m)" == "arm64" ]]; then
    APPLE_SILICON_FLAG="-tags dynamic"
fi
for d in $(go list ./internal/... | grep -v -e vendor -e kafka -e planparserv2/generated); do
    go test -race ${APPLE_SILICON_FLAG} -ldflags="-r $RPATH" -v -coverpkg=./... -coverprofile=profile.out -covermode=atomic "$d"
    if [ -f profile.out ]; then
        grep -v kafka profile.out | grep -v planparserv2/generated | sed '1d' >> ${FILE_COVERAGE_INFO}
        rm profile.out
    fi
done

# generate html report
go tool cover -html=./${FILE_COVERAGE_INFO} -o ./${FILE_COVERAGE_HTML}
echo "Generate go coverage report to ${FILE_COVERAGE_HTML}"
