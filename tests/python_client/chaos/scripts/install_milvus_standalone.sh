#!/bin/bash
set -e

release=${1:-"milvs-chaos"}
ns=${2:-"chaos-testing"}
bash uninstall_milvus.sh ${release} ${ns}|| true
echo "insatll standalone"
helm install --wait --timeout 360s ${RELEASE_NAME:-$release} milvus/milvus \
                    --set image.all.repository=${REPOSITORY:-"milvusdb/milvus-dev"} \
                    --set image.all.tag=${IMAGE_TAG:-"master-latest"} \
                    -f ../standalone-values.yaml -n=${ns}