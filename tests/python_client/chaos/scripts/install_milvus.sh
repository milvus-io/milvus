
release=${1:-"milvs-chaos"}
ns=${2:-"chaos-testing"}
bash uninstall_milvus.sh ${release} ${ns}|| true

helm repo add milvus https://milvus-io.github.io/milvus-helm/
helm repo update
helm install --wait --timeout 360s ${release} milvus/milvus -f ../cluster-values.yaml --set metrics.serviceMonitor.enabled=true -n=${ns}
