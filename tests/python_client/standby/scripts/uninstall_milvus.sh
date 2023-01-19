
# Exit immediately for non zero status
set -e
release=${1:-"milvus-chaos"}
ns=${2:-"chaos-testing"}
helm uninstall ${release} -n=${ns}
kubectl delete pvc -l release=${release} -n=${ns}
kubectl delete pvc -l app.kubernetes.io/instance=${release} -n=${ns}
