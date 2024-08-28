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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"

# Exit immediately for non zero status
set -e
# Check unset variables
set -u
# Print commands
set -x



MILVUS_HELM_RELEASE_NAME="${MILVUS_HELM_RELEASE_NAME:-milvus-testing}"
MILVUS_CLUSTER_ENABLED="${MILVUS_CLUSTER_ENABLED:-false}"
MILVUS_HELM_NAMESPACE="${MILVUS_HELM_NAMESPACE:-default}"
PARALLEL_NUM="${PARALLEL_NUM:-6}"
# Use service name instead of IP to test
MILVUS_SERVICE_NAME=$(echo "${MILVUS_HELM_RELEASE_NAME}-milvus.${MILVUS_HELM_NAMESPACE}" | tr -d '\n')
# MILVUS_SERVICE_HOST=$(kubectl get svc ${MILVUS_SERVICE_NAME}-milvus -n ${MILVUS_HELM_NAMESPACE} -o jsonpath='{.spec.clusterIP}')
MILVUS_SERVICE_PORT="19530"
# Minio service name
MINIO_SERVICE_NAME=$(echo "${MILVUS_HELM_RELEASE_NAME}-minio.${MILVUS_HELM_NAMESPACE}" | tr -d '\n')


# Shellcheck source=ci-util.sh
source "${ROOT}/tests/scripts/ci-util-4am.sh"


cd ${ROOT}/tests/python_client

# Print python3 version, python version 3.6.8 is more stable for test
python3 -V

# Pytest will try to get ${CI_LOG_PATH} from environment variables first,then use default path
export CI_LOG_PATH=/tmp/ci_logs/test

if [ ! -d "${CI_LOG_PATH}" ]; then
  # Create dir for ci log path when it does not exist
  mkdir -p ${CI_LOG_PATH}
fi

# skip pip install when DISABLE_PIP_INSTALL is set
DISABLE_PIP_INSTALL=${DISABLE_PIP_INSTALL:-false}
if [ "${DISABLE_PIP_INSTALL:-}" = "false" ]; then
  echo "prepare e2e test"
  install_pytest_requirements
fi




cd ${ROOT}/tests/python_client
# Run bulk insert test
# if MILVUS_HELM_RELEASE_NAME contains "msop", then it is one pod mode, skip the bulk insert test
if [[ "${MILVUS_HELM_RELEASE_NAME}" != *"msop"* ]]; then
  if [[ -n "${TEST_TIMEOUT:-}" ]]; then

    timeout  "${TEST_TIMEOUT}" pytest testcases/test_bulk_insert.py --timeout=300 -n 6 --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} \
                                      --html=${CI_LOG_PATH}/report_bulk_insert.html  --self-contained-html
  else
    pytest testcases/test_bulk_insert.py --timeout=300 -n 6 --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} \
                                      --html=${CI_LOG_PATH}/report_bulk_insert.html --self-contained-html
  fi
fi


# Run restful test v1

cd ${ROOT}/tests/restful_client

if [[ -n "${TEST_TIMEOUT:-}" ]]; then

  timeout  "${TEST_TIMEOUT}" pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} -v -x -m L0 -n 6 --timeout 180\
                                     --html=${CI_LOG_PATH}/report_restful.html  --self-contained-html
else
  pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} -v -x -m L0 -n 6 --timeout 180\
                                     --html=${CI_LOG_PATH}/report_restful.html --self-contained-html
fi

# Run restful test v2
cd ${ROOT}/tests/restful_client_v2

if [[ -n "${TEST_TIMEOUT:-}" ]]; then

  timeout  "${TEST_TIMEOUT}" pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} -v -x -m L0 -n 6 --timeout 180\
                                     --html=${CI_LOG_PATH}/report_restful.html  --self-contained-html
else
  pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} -v -x -m L0 -n 6 --timeout 180\
                                     --html=${CI_LOG_PATH}/report_restful.html --self-contained-html
fi


if [[ "${MILVUS_HELM_RELEASE_NAME}" != *"msop"* ]]; then
  if [[ -n "${TEST_TIMEOUT:-}" ]]; then

    timeout  "${TEST_TIMEOUT}" pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} -v -x -m BulkInsert -n 6 --timeout 180\
                                      --html=${CI_LOG_PATH}/report_restful.html  --self-contained-html
  else
    pytest testcases --endpoint http://${MILVUS_SERVICE_NAME}:${MILVUS_SERVICE_PORT} --minio_host ${MINIO_SERVICE_NAME} -v -x -m BulkInsert -n 6 --timeout 180\
                                      --html=${CI_LOG_PATH}/report_restful.html --self-contained-html
  fi
fi


cd ${ROOT}/tests/python_client


# Pytest is not able to have both --timeout & --workers, so do not add --timeout or --workers in the shell script
if [[ -n "${TEST_TIMEOUT:-}" ]]; then

  timeout  "${TEST_TIMEOUT}" pytest --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} \
                                     --html=${CI_LOG_PATH}/report.html  --self-contained-html ${@:-}
else
  pytest --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} \
                                     --html=${CI_LOG_PATH}/report.html --self-contained-html ${@:-}
fi

# # Run concurrent test with 5 processes
# if [[ -n "${TEST_TIMEOUT:-}" ]]; then

#   timeout  "${TEST_TIMEOUT}" pytest testcases/test_concurrent.py --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} --count 5 -n 5 \
#                                      --html=${CI_LOG_PATH}/report_concurrent.html  --self-contained-html
# else
#   pytest testcases/test_concurrent.py --host ${MILVUS_SERVICE_NAME} --port ${MILVUS_SERVICE_PORT} --count 5 -n 5 \
#                                      --html=${CI_LOG_PATH}/report_concurrent.html --self-contained-html
# fi
