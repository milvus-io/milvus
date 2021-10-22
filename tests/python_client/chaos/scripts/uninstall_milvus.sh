
set -e
helm uninstall milvus-chaos
kubectl delete pvc -l release=milvus-chaos
kubectl delete pvc -l app.kubernetes.io/instance=milvus-chaos
