#!/bin/bash
set -e

release=${1:-"milvs-chaos"}
ns=${2:-"chaos-testing"}
bash uninstall_milvus.sh ${release} ${ns}|| true

echo "insatll cluster"
helm install --wait --debug --timeout 600s ${RELEASE_NAME:-$release} milvus/milvus \
                            --set image.all.repository=${REPOSITORY:-"milvusdb/milvus"} \
                            --set image.all.tag=${IMAGE_TAG:-"master-latest"} \
                            --set metrics.serviceMonitor.enabled=true \
                            -f ../cluster-values.yaml -n=${ns}