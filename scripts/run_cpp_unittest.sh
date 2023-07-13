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

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

MILVUS_CORE_DIR="${SCRIPTS_DIR}/../internal/core"
CORE_INSTALL_PREFIX="${MILVUS_CORE_DIR}/output"
UNITTEST_DIRS=("${CORE_INSTALL_PREFIX}/unittest")

# currently core will install target lib to "internal/core/output/lib"
if [ -d "${CORE_INSTALL_PREFIX}/lib" ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CORE_INSTALL_PREFIX}/lib
fi

# run unittest
for UNITTEST_DIR in "${UNITTEST_DIRS[@]}"; do
  if [ ! -d "${UNITTEST_DIR}" ]; then
    echo "The unittest folder does not exist!"
    exit 1
  fi

  echo "Running all unittest ..."
  ${UNITTEST_DIR}/all_tests
  if [ $? -ne 0 ]; then
      echo ${UNITTEST_DIR}/all_tests "run failed"
      exit 1
  fi
  if [ -f "${UNITTEST_DIR}/dynamic_simd_test" ]; then
      echo "Running dynamic simd test"
      ${UNITTEST_DIR}/dynamic_simd_test
      if [ $? -ne 0 ]; then 
        echo ${UNITTEST_DIR}/dynamic_simd_test "run failed"
        exit 1
      fi
  fi
done

# run cwrapper unittest
if [ -f ${CWRAPPER_UNITTEST} ];then
  echo "Running cwrapper unittest: ${CWRAPPER_UNITTEST}"
  ${CWRAPPER_UNITTEST}
  if [ $? -ne 0 ]; then
      echo ${CWRAPPER_UNITTEST} " run failed"
      exit 1
  fi
fi
