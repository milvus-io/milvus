
release=${1:-"milvs-chaos"}

bash uninstall_milvus.sh ${release} || true

helm install --wait --timeout 360s ${release} milvus/milvus --set service.type=NodePort -f ../cluster-values.yaml  -n=chaos-testing
