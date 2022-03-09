#!/bin/bash
set -e

release=${1:-"milvs-chaos"}
ns=${2:-"chaos-testing"}
pod=${3:-"querynode"}
node_num=${4:-1}
bash uninstall_milvus.sh ${release} ${ns}|| true

declare -A pod_map=(["querynode"]="queryNode" ["indexnode"]="indexNode" ["datanode"]="dataNode" ["proxy"]="proxy")
echo "insatll cluster"
helm install --wait --debug --timeout 360s ${RELEASE_NAME:-$release} milvus/milvus \
                            --set image.all.repository=${REPOSITORY:-"milvusdb/milvus-dev"} \
                            --set image.all.tag=${IMAGE_TAG:-"master-latest"} \
                            --set ${pod_map[${pod}]}.replicas=$node_num \
                            -f ../cluster-values.yaml -n=${ns}