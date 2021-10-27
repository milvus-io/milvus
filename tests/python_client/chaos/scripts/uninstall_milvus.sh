
set -e
release=${1:-"milvus-chaos"}
helm uninstall ${release}
kubectl delete pvc -l release=${release}
kubectl delete pvc -l app.kubernetes.io/instance=${release}
