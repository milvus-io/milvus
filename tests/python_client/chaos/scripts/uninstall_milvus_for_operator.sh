
# Exit immediately for non zero status
set -e
release=${1:-"milvus-chaos"}
ns=${2:-"chaos-testing"}
kubectl delete milvus ${release} -n=${ns} || echo "delete milvus ${release} failed"

# uninstall helm release
helm_release_list=('minio' 'etcd' 'kafka' 'pulsar')
for helm_release in ${helm_release_list[*]}; do
    echo "unistall helm release ${release}-${helm_release}"
    helm uninstall ${release}-${helm_release} -n=${ns} || echo "delete helm release ${release}-${helm_release} failed"
done
# delete pvc for storage
pvc_list=('minio')
for pvc in ${pvc_list[*]}; do
    echo "delete pvc with label release=${release}-${pvc}"
    kubectl delete pvc -l release=${release}-${pvc} -n=${ns} || echo "delete pvc with label release=${release}-${pvc} failed"
done

# delete pvc of etcd and message queue
pvc_list=('etcd' 'kafka' 'pulsar')
for pvc in ${pvc_list[*]}; do
    echo "delete pvc with label app.kubernetes.io/instance=${release}-${pvc}"
    kubectl delete pvc -l app.kubernetes.io/instance=${release}-${pvc} -n=${ns} || echo "delete pvc with label release=${release}-${pvc} failed"
done




