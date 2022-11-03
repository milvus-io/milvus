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

# concurrency limit
THREAD_NUMBER=${THREAD_NUMBER:-15}
TMP_FIFO=$$.fifo


set -e -o pipefail
trap "rm -vf ${TMP_FIFO}" 0
echo "mode: atomic" > ${FILE_COVERAGE_INFO}

BASEDIR=$(dirname "$0")
source $BASEDIR/setenv.sh

function run_ut(){
    local go_file=${1}
    local profile_out="${go_file}_profile.out"
    go test -race ${APPLE_SILICON_FLAG} -v -coverpkg=./... -coverprofile=${profile_out} -covermode=atomic "${go_file}"
    if [ -f ${profile_out} ]; then
        grep -v kafka ${profile_out}| grep -v planparserv2/generated | grep -v mocks | sed '1d' >> ${FILE_COVERAGE_INFO}
        rm ${profile_out}
    fi
}

mkfifo ${TMP_FIFO}
exec 6<> ${TMP_FIFO}
rm -f ${TMP_FIFO}
# control concurrency limit 
for i in `seq ${THREAD_NUMBER}`
do  
    # put max concurrency into pipe
    echo  >&6
done 

# run unittest
echo "Running go unittest under ./internal"
# starting the timer
beginTime=`date +%s`
if [[ $(uname -s) == "Darwin" && "$(uname -m)" == "arm64" ]]; then
    APPLE_SILICON_FLAG="-tags dynamic"
fi

for d in $(go list ./internal/... | grep -v -e vendor -e kafka -e planparserv2/generated -e mocks); do

    # read line through file handles
    read -u 6
    {
        run_ut ${d}
        if [ $? -ne 0 ]; then
         exit -1
        fi
        echo >&6
    }&
done

wait
exec 6>&-
endTime=`date +%s`
echo "Total time for go unittest:" $(($endTime-$beginTime)) "s"
# generate html report
go tool cover -html=./${FILE_COVERAGE_INFO} -o ./${FILE_COVERAGE_HTML}
echo "Generate go coverage report to ${FILE_COVERAGE_HTML}"
# set +o pipefail