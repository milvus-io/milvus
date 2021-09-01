#!/bin/bash

# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

set -e
set -x

MILVUS_HELM_RELEASE_NAME="${MILVUS_HELM_RELEASE_NAME:-milvus-testing}"
MILVUS_CLUSTER_ENABLED="${MILVUS_CLUSTER_ENABLED:-false}"
MILVUS_HELM_NAMESPACE="${MILVUS_HELM_NAMESPACE:-default}"
PARALLEL_NUM="${PARALLEL_NUM:-6}"
MILVUS_CLIENT="${MILVUS_CLIENT:-pymilvus}"

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"

if [[ "${TEST_ENV:-}" =~ ^kind*  ]]; then
  if [[ "${MILVUS_CLUSTER_ENABLED}" == "false" ]]; then
    MILVUS_LABELS="app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME},component=standalone"
  else
    MILVUS_LABELS="app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME},component=proxy"
  fi

  SERVICE_TYPE=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].spec.type}')

  if [[ "${SERVICE_TYPE}" == "LoadBalancer" ]]; then
    MILVUS_SERVICE_IP=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
    MILVUS_SERVICE_PORT=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].spec.ports[0].port}')
  elif [[ "${SERVICE_TYPE}" == "NodePort" ]]; then
    MILVUS_SERVICE_IP=$(kubectl get nodes --namespace "${MILVUS_HELM_NAMESPACE}" -o jsonpath='{.items[0].status.addresses[0].address}')
    MILVUS_SERVICE_PORT=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].spec.ports[0].nodePort}')
  else
    MILVUS_SERVICE_IP="127.0.0.1"
    POD_NAME=$(kubectl get pods --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].metadata.name}')
    MILVUS_SERVICE_PORT=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{.items[0].spec.ports[0].port}')
    kubectl --namespace "${MILVUS_HELM_NAMESPACE}" port-forward "${POD_NAME}" "${MILVUS_SERVICE_PORT}" &
    PORT_FORWARD_PID=$!
    trap "kill -TERM ${PORT_FORWARD_PID}" EXIT
  fi
fi

pushd "${ROOT}/tests/docker"
  if [[ "${TEST_ENV:-}" =~ ^kind*  ]]; then
    export PRE_EXIST_NETWORK="true"
    export PYTEST_NETWORK="kind"
  fi

  export MILVUS_SERVICE_IP="${MILVUS_SERVICE_IP:-127.0.0.1}"
  export MILVUS_SERVICE_PORT="${MILVUS_SERVICE_PORT:-19530}"

  if [[ "${MANUAL:-}" == "true" ]]; then
    docker-compose up -d
  else
    if [[ "${MILVUS_CLIENT}" == "pymilvus" ]]; then
      export MILVUS_PYTEST_WORKSPACE="/milvus/tests/python_client"
      docker-compose run --rm pytest /bin/bash -c "pytest -n ${PARALLEL_NUM} --host ${MILVUS_SERVICE_IP} --port ${MILVUS_SERVICE_PORT} \
                                                   --html=\${CI_LOG_PATH}/report.html --self-contained-html ${@:-}"
#    elif [[ "${MILVUS_CLIENT}" == "pymilvus-orm" ]]; then
#      export MILVUS_PYTEST_WORKSPACE="/milvus/tests20/python_client"
#      docker-compose run --rm pytest /bin/bash -c "pytest -n ${PARALLEL_NUM} --host ${MILVUS_SERVICE_IP} --port ${MILVUS_SERVICE_PORT} \
#                                               --html=\${CI_LOG_PATH}/report.html --self-contained-html ${@:-}"
    fi
  fi
popd
