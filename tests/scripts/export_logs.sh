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

ARTIFACTS="${ARTIFACTS:-$(mktemp -d)}"

KIND_NAME="${1:-kind}"

mkdir -p "${ARTIFACTS}/${KIND_NAME}"

for node in `kind get nodes --name=kind | tr -s '\n' ' '`
do
  docker cp $node:/var/log "${ARTIFACTS}/${KIND_NAME}"
done

echo "Exported logs for cluster \"${KIND_NAME}\" to: \"${ARTIFACTS}/${KIND_NAME}\""

MILVUS_HELM_NAMESPACE="${MILVUS_HELM_NAMESPACE:-default}"
MILVUS_HELM_RELEASE_NAME="${MILVUS_HELM_RELEASE_NAME:-milvus-testing}"

ETCD_SERVICE_IP=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME},app.kubernetes.io/name=etcd" -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
ETCD_SERVICE_PORT=$(kubectl get service --namespace "${MILVUS_HELM_NAMESPACE}" -l "app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME},app.kubernetes.io/name=etcd" -o jsonpath='{.items[0].spec.ports[0].port}')
METRICS_DIRS="${ARTIFACTS:-$(mktemp -d)}/metrics"

mkdir -p "${METRICS_DIRS}"
curl -L -s "http://${ETCD_SERVICE_IP}:${ETCD_SERVICE_PORT}/metrics" > "${METRICS_DIRS}/etcd.log"
