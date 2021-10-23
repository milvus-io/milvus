

bash uninstall_milvus.sh || true

helm install --wait --timeout 360s milvus-chaos milvus/milvus --set service.type=NodePort -f ../cluster-values.yaml  -n=chaos-testing
