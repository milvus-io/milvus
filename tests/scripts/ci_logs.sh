#!/bin/bash

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

# Exit immediately for non zero status
set -e
# Check unset variables
set -u
# Print commands
set -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"

while (( "$#" )); do
  case "$1" in

    --release-name)
      RELEASE_NAME=$2
      shift 2
    ;;

    --artifacts-name)
      ARTIFACTS_NAME=$2
      shift 2
    ;;

    --log-dir)
      LOG_DIR=$2
      shift 2
    ;;

    -h|--help)
      { set +x; } 2>/dev/null
      HELP="
Usage:
  $0 [flags] [Arguments]
    
    --log-dir                  Log Path

    --artifacts-name           Artifacts Name

    --release-name             Milvus helm release name

    -h or --help                Print help information


Use \"$0  --help\" for more information about a given command.
"
      echo -e "${HELP}" ; exit 0
    ;;
    -*)
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS+=("$1")
      shift
      ;;
  esac
done

LOG_DIR=${LOG_DIR:-/ci-logs/}
RELEASE_NAME=${RELEASE_NAME:-milvus-testing}
RELEASE_LOG_DIR=/${RELEASE_NAME}
if [[ ! -d ${RELEASE_LOG_DIR} ]] ;then 
  mkdir -p ${RELEASE_LOG_DIR}
fi 
# Try to found logs file from mount disk /volume1/ci-logs
log_files=$(find ${LOG_DIR} -type f  -name "*${RELEASE_NAME}*" )
for log_file in ${log_files}
do 
 file_name=$(basename ${log_file})
  mv  ${log_file} ${RELEASE_LOG_DIR}/`echo ${file_name} | sed 's/jenkins.var.log.containers.//g' `
done 

tar -zcvf ${ARTIFACTS_NAME:-artifacts}.tar.gz ${RELEASE_LOG_DIR}/*
rm -rf ${RELEASE_LOG_DIR}

remain_log_files=$(find ${LOG_DIR} -type f  -name "*${RELEASE_NAME}*")

if [ -z "${remain_log_files:-}" ]; then
  echo "No remain log files"
else
  echo "Still have log files & Remove again"
  find ${LOG_DIR} -type f  -name "*${RELEASE_NAME}*" -exec rm -rf {} +
  echo "Check if any remain log files  after using rm to delete again "
  find ${LOG_DIR} -type f  -name "*${RELEASE_NAME}*"
fi