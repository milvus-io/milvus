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

# Exit immediately for non zero status
set -e
# Print commands
set -x

while (( "$#" )); do
  case "$1" in

    --release-name)
      RELEASE_NAME=$2
      shift 2
    ;;
  
    -h|--help)
      { set +x; } 2>/dev/null
      HELP="
Usage:
  $0 [flags] [Arguments]

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

MILVUS_HELM_RELEASE_NAME="${MILVUS_HELM_RELEASE_NAME:-milvus-testing}"
MILVUS_HELM_NAMESPACE="${MILVUS_HELM_NAMESPACE:-default}"

if [[ -n "${RELEASE_NAME:-}" ]]; then
    MILVUS_HELM_RELEASE_NAME="${RELEASE_NAME}"
    # List pod list before uninstall 
    kubectl get pods -n ${MILVUS_HELM_NAMESPACE}  -o wide | grep "${MILVUS_HELM_RELEASE_NAME}-"

fi

# Uninstall Milvus Helm Release
# Do not exit with error when release not found so that the script can also be used to clean up related pvc even helm release has been uninstalled already
helm uninstall -n "${MILVUS_HELM_NAMESPACE}" "${MILVUS_HELM_RELEASE_NAME}" || true

MILVUS_LABELS1="app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME}"
MILVUS_LABELS2="release=${MILVUS_HELM_RELEASE_NAME}"

# Clean up pvc
kubectl delete pvc --wait -n "${MILVUS_HELM_NAMESPACE}" $(kubectl get pvc -n "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS1}" -o jsonpath='{range.items[*]}{.metadata.name} ') || true
kubectl delete pvc --wait -n "${MILVUS_HELM_NAMESPACE}" $(kubectl get pvc -n "${MILVUS_HELM_NAMESPACE}" -l "${MILVUS_LABELS2}" -o jsonpath='{range.items[*]}{.metadata.name} ') || true

# Add check & delete pvc again in case pvc need time to be deleted 
clean_label_pvc(){
    local label=${1?label expected as first argument.}

    for i in {1..10}
    do
        PVC=$(kubectl get pvc -n "${MILVUS_HELM_NAMESPACE}" -l "${label}" -o jsonpath='{range.items[*]}{.metadata.name} ')
        STATUS=$(echo ${PVC} | wc -w )
        echo "status is ${STATUS}"
        if [ $STATUS == 0 ]; then
            break
        else
            sleep 5
            echo "PVCs are ${PVC}"
            kubectl delete pvc --wait -n "${MILVUS_HELM_NAMESPACE}" ${PVC}
        fi
    done
}

echo "Check & Delete Persistent Volumes"
clean_label_pvc ${MILVUS_LABELS1}
clean_label_pvc ${MILVUS_LABELS2}