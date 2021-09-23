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

MILVUS_HELM_REPO="https://github.com/milvus-io/milvus-helm.git"
MILVUS_HELM_RELEASE_NAME="${MILVUS_HELM_RELEASE_NAME:-milvus-testing}"
MILVUS_CLUSTER_ENABLED="${MILVUS_CLUSTER_ENABLED:-false}"
MILVUS_IMAGE_REPO="${MILVUS_IMAGE_REPO:-milvusdb/milvus}"
MILVUS_IMAGE_TAG="${MILVUS_IMAGE_TAG:-latest}"
MILVUS_HELM_NAMESPACE="${MILVUS_HELM_NAMESPACE:-default}"
MILVUS_INSTALL_TIMEOUT="${MILVUS_INSTALL_TIMEOUT:-500s}"

# Delete any previous Milvus cluster
echo "Deleting previous Milvus cluster with name=${MILVUS_HELM_RELEASE_NAME}"
if ! (helm uninstall -n "${MILVUS_HELM_NAMESPACE}" "${MILVUS_HELM_RELEASE_NAME}" > /dev/null 2>&1); then
  echo "No existing Milvus cluster with name ${MILVUS_HELM_RELEASE_NAME}. Continue..."
else
  MILVUS_LABELS="app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME}"
  kubectl delete pvc -n "${MILVUS_HELM_NAMESPACE}" $(kubectl get pvc -n "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS}" -o jsonpath='{range.items[*]}{.metadata.name} ') || true
fi

if [[ "${TEST_ENV}" == "kind-metallb" ]]; then
  MILVUS_SERVICE_TYPE="${MILVUS_SERVICE_TYPE:-LoadBalancer}"
else
  MILVUS_SERVICE_TYPE="${MILVUS_SERVICE_TYPE:-ClusterIP}"
fi


if [[ ! -d "${MILVUS_HELM_CHART_PATH:-}" ]]; then
  TMP_DIR="$(mktemp -d)"
  git clone --depth=1 -b "${MILVUS_HELM_BRANCH:-master}" "${MILVUS_HELM_REPO}" "${TMP_DIR}"
  MILVUS_HELM_CHART_PATH="${TMP_DIR}/charts/milvus"
fi

kubectl create namespace "${MILVUS_HELM_NAMESPACE}" > /dev/null 2>&1 || true

helm install --wait --timeout "${MILVUS_INSTALL_TIMEOUT}" \
                               --set image.all.repository="${MILVUS_IMAGE_REPO}" \
                               --set image.all.tag="${MILVUS_IMAGE_TAG}" \
                               --set image.all.pullPolicy="${MILVUS_PULL_POLICY:-Always}" \
                               --set cluster.enabled="${MILVUS_CLUSTER_ENABLED}" \
                               --set service.type="${MILVUS_SERVICE_TYPE}" \
                               --namespace "${MILVUS_HELM_NAMESPACE}" \
                               "${MILVUS_HELM_RELEASE_NAME}" \
                               ${@:-} "${MILVUS_HELM_CHART_PATH}"
